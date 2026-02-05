package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all connections; adjust for production!
	},
}

var (
	clients   = make(map[string]*Client)
	clientsMu sync.RWMutex
)

var ctx = context.Background()

type Hub struct {
	// Registered clients.
	clients map[*websocket.Conn]bool
	// Inbound messages from Redis.
	broadcast chan []byte
	// Register requests from the HTTP handler.
	register chan *websocket.Conn
	// Unregister requests from clients.
	unregister chan *websocket.Conn
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
		clients:    make(map[*websocket.Conn]bool),
	}
}

func InitRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Default Redis port
		Password: "",               // No password set by default
		DB:       0,                // Use default DB
	})

	// Verify connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Could not connect to Redis: %v", err))
	}

	return rdb
}

func ListenToRedis(ctx context.Context, rdb *redis.Client, hub *Hub) {
	pubsub := rdb.Subscribe(ctx, "Stream_Channel")
	ch := pubsub.Channel()

	for msg := range ch {
		// This sends the Python data directly to the Hub's broadcast loop
		HandleClientRead(*msg)
	}
}

func SendToRedis(data []byte, ctx context.Context, rdb *redis.Client) error {
	err := rdb.Publish(ctx, "Start_Stream", data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to redis: %v", err)
	}
	return nil
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			delete(h.clients, client)
		case message := <-h.broadcast:
			for client := range h.clients {
				// Use a non-blocking write or a goroutine to prevent
				// one slow user from stalling the entire Redis stream.
				go func(c *websocket.Conn, msg []byte) {
					err := c.WriteMessage(websocket.TextMessage, msg)
					if err != nil {
						h.unregister <- c
					}
				}(client, message)
			}
		}
	}
}

// Takes json and sends it to client
func SendToClient(client *Client, msg []byte) error {
	err := client.SafeWrite(websocket.TextMessage, msg)
	if err != nil {
		return err
	}
	log.Printf("sent to %s: %s", client.ID, string(msg))
	return nil
}

// Reads a message from the client and returns an error if there is one
func ReceiveFromClient(client *Client) ([]byte, error) {
	_, msg, err := client.Conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func StartRedisContainer() {
	path, err := exec.LookPath("docker")
	if err != nil {
		fmt.Println("Error: Docker executable not found in your system PATH.")
		fmt.Println("Check: Is Docker installed and is the terminal session refreshed?")
		return
	}
	fmt.Printf("Using Docker found at: %s\n", path)
	cmd := exec.Command(path, "start", "redis-server")

	if err := cmd.Run(); err != nil {
		fmt.Println("Container not running. Attempting to create/run...")

		// 3. Try to run (this works for both Windows and Linux)
		runCmd := exec.Command(path, "run", "-d", "--name", "redis-server", "-p", "6379:6379", "redis")
		if err := runCmd.Run(); err != nil {
			fmt.Printf("Critical Error: %v\n", err)
		} else {
			fmt.Println("Redis container is up!")
		}
	} else {
		fmt.Println("Redis container started!")
	}
}

// Stop the Redis container
func StopRedisContainer() {
	cmd := exec.Command("docker", "stop", "redis-server")
	err := cmd.Run()
	if err != nil {
		fmt.Println("Error stopping container:", err)
	} else {
		fmt.Println("Redis container stopped.")
	}
}

// Handles all websocket connections
func WebsocketConnectHandler(hub *Hub, openDB, balanceDB, priceDB, trackerDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		log.Println("Client ID missing")
		ws.Close()
		return
	}

	clientsMu.Lock()

	hub.register <- ws

	newClient := &Client{
		Conn: ws,
		ID:   clientID,
		Done: make(chan struct{}),
	}
	if _, exists := clients[clientID]; exists {
		log.Printf("Client %s already connected — replacing connection", clientID)
		DisconnectClient(clientID)
	}
	clients[clientID] = newClient
	clientsMu.Unlock()

	defer func() {
		hub.unregister <- ws // Unregister on disconnect
		DisconnectClient(clientID)
	}()

	log.Printf("Client connected: %s", clientID)

	// Start reader and writer goroutines
	if clientID == "PYTHON_CLIENT" {
		SendInitialPositions(openDB, trackerDB, clients, clientID, w, r)
	}

	if clientID == "STOCK_CLIENT" {
		SendOpenPositions(balanceDB, openDB, priceDB, trackerDB, clients)
		go HandleClientWrite(newClient, openDB, balanceDB, priceDB)
	}

	<-newClient.Done

}

func SendInitialPositions(openDB, trackerDB *sql.DB, clients map[string]*Client, clientID string, w http.ResponseWriter, r *http.Request) {
	var optionSymbols []OptionStreamRequest
	var stockSymbols []StockStreamRequest
	optionSymbols, stockSymbols = SendTrackerSymbols(trackerDB, openDB)
	if optionSymbols != nil {
		optionMsg, err := json.Marshal(optionSymbols)
		if err != nil {
			log.Printf("Error Marshalling: %v", err)
			return
		}

		err = SendToClient(clients[clientID], optionMsg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if stockSymbols != nil {
		stockMsg, err := json.Marshal(stockSymbols)
		if err != nil {
			log.Printf("Error Marshalling: %v", err)
			return
		}
		err = SendToClient(clients[clientID], stockMsg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func DisconnectClient(clientID string) {
	clientsMu.Lock()
	client, exists := clients[clientID]
	if !exists {
		log.Printf("Client %s was already removed", clientID)
		clientsMu.Unlock()
		return
	}
	delete(clients, clientID)
	clientsMu.Unlock()

	client.Conn.Close()
	client.Close() // safe: Once ensures it's only closed once
	log.Printf("Client disconnected: %s", clientID)
}

func ShutdownAllClients() {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for id, client := range clients {
		log.Printf("Closing connection for client: %s", id)
		client.Conn.Close()
		client.Close()
		select {
		case <-client.Done:
		default:
			close(client.Done)
		}
		delete(clients, id)
	}
}

// Receives and handles a websocket message from python client and sends data to frontend
func HandleClientRead(msg redis.Message) {
	var quotes map[string]MixedQuote
	payloadBytes := []byte(msg.Payload)
	fmt.Println(payloadBytes)
	if err := json.Unmarshal(payloadBytes, &quotes); err != nil {
		// If it’s not a quotes payload, skip or handle other message types here
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}
	optionPrices := make([]OptionPriceData, 0, len(quotes))
	stockPrices := make([]StockPriceData, 0, len(quotes))
	timestamp := time.Now().Unix()
	// Process each symbol
	for symbol, q := range quotes {
		switch {
		// Equity quote if BidSize/AskSize are present
		case q.BidSize != nil && q.AskSize != nil:
			stock := StockPriceData{
				Symbol:    symbol,
				Timestamp: timestamp,
				Mark:      q.Mark,
				BidPrice:  q.BidPrice,
				AskPrice:  q.AskPrice,
				LastPrice: q.LastPrice,
				BidSize:   *q.BidSize,
				AskSize:   *q.AskSize,
			}

			stockPrices = append(stockPrices, stock)

		// Option quote if IV or Greeks are present
		case q.IV != nil:
			option := OptionPriceData{
				Symbol:    symbol,
				Timestamp: timestamp,
				Bid:       q.BidPrice,
				Ask:       q.AskPrice,
				Mark:      q.Mark,
				Last:      q.LastPrice,
				High:      *q.HighPrice,
				IV:        *q.IV,
				Delta:     *q.Delta,
				Gamma:     *q.Gamma,
				Theta:     *q.Theta,
				Vega:      *q.Vega,
			}
			optionPrices = append(optionPrices, option)

		default:
			log.Printf("Unrecognized quote type for %s: %+v", symbol, q)
		}
	}
	fmt.Println("Received", stockPrices)
	fmt.Println("Received", optionPrices)
	// Send both payloads as two JSON arrays
	client := clients["STOCK_CLIENT"]
	if client == nil {
		log.Println("Stock Client is not connected")
		return
	}

	// Helper to marshal & send JSON
	send := func(v interface{}) error {
		msg, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return SendToClient(client, msg)
	}

	if err := send(optionPrices); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
		return
	}

	if err := send(stockPrices); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
		return
	}
}

// Incremently writes balance to the Frontend
func HandleClientWrite(client *Client, openDB, balanceDB, priceDB *sql.DB) {
	now := time.Now()
	wait := 15*time.Second - (time.Duration(now.Second()%15)*time.Second + time.Duration(now.Nanosecond()))
	timer := time.NewTimer(wait)

	select {
	case <-client.Done:
		log.Printf("handleClientWrite exiting for client %s", client.ID)
		DisconnectClient(client.ID)
		timer.Stop()
		return
	case t := <-timer.C:
		processWrite(t, client, balanceDB, openDB, priceDB)
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-client.Done:
			log.Printf("handleClientWrite exiting for client %s", client.ID)
			DisconnectClient(client.ID)
			return
		case t := <-ticker.C:
			processWrite(t, client, balanceDB, openDB, priceDB)
		}
	}
}

// Sends a message to the python streamer to start a subscription to a certain option ID
func StartOptionStream(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	price := r.URL.Query().Get("price")
	day := r.URL.Query().Get("day")
	month := r.URL.Query().Get("month")
	year := r.URL.Query().Get("year")
	optionType := r.URL.Query().Get("type")

	request := OptionStreamRequest{
		Symbol: symbol,
		Price:  price,
		Day:    day,
		Month:  month,
		Year:   year,
		Type:   optionType,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = SendToRedis(msg, context.Background(), rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Sends a message to the python streamer to start a subscription to a certain stock
func StartStockStream(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	request := StockStreamRequest{
		Symbol: symbol,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		return
	}
	err = SendToRedis(msg, context.Background(), rdb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}

// Write to a specific client the most recent balance
func processWrite(t time.Time, client *Client, balanceDB, openDB, priceDB *sql.DB) {
	var cash float64
	var balance float64

	date := TodayDate()

	balance = 10000.0
	cash = 10000.0

	query := `SELECT timestamp, balance, cash FROM Balance ORDER BY timestamp DESC LIMIT 1`
	row := balanceDB.QueryRow(query)

	var timestamp int64
	err := row.Scan(&timestamp, &balance, &cash)
	if err == sql.ErrNoRows {
		log.Printf("No rows in Balance; using default balance 10000")
	} else if err != nil {
		log.Printf("Failed to query table: %v", err)
		return
	}

	rows, err := openDB.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
	}
	defer rows.Close()

	tempPositionValue := 0.0

	for rows.Next() {
		var id string
		var amount int64
		if err := rows.Scan(&id, &amount); err != nil {
			continue
		}

		var mark float64
		if len(id) > 6 {
			var ts int64
			var sym string
			var b, a, l, h, iv, d, g, th, v float64
			err = priceDB.QueryRow(`SELECT * FROM Options WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1`, id).
				Scan(&ts, &sym, &mark, &b, &a, &l, &h, &iv, &d, &g, &th, &v)

			if err == nil {
				tempPositionValue += (mark * float64(amount) * 100)
			}
		} else {
			var ts, bs, as int64
			var sym string
			var b, a, l float64
			err = priceDB.QueryRow(`SELECT * FROM Stocks WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1`, id).
				Scan(&ts, &sym, &mark, &b, &a, &l, &bs, &as)

			if err == nil {
				tempPositionValue += (mark * float64(amount))
			}
		}

		if err != nil && err != sql.ErrNoRows {
			log.Printf("Price lookup failed for %s: %v", id, err)
		}
	}

	tempBalance := cash + tempPositionValue
	balance = tempBalance
	if err := rows.Err(); err != nil {
		log.Println("Rows iteration error:", err)
	}

	insertData := `INSERT OR REPLACE INTO Balance (timestamp, balance, cash) VALUES (?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, cash)
	if err != nil {
		log.Printf("Failed to insert initial balance into table %s: %v", date, err)
	}
	message := StockPriceData{
		Symbol:    "balance",
		Timestamp: t.Unix(),
		Mark:      balance,
	}

	msg, err := json.Marshal(message)
	if err != nil {
		return
	}
	err = SendToClient(clients["STOCK_CLIENT"], msg)
	if err != nil {
		errMsg := map[string]string{"error": err.Error()}
		msg, _ := json.Marshal(errMsg)
		_ = client.SafeWrite(websocket.TextMessage, msg)
		return
	}
}
