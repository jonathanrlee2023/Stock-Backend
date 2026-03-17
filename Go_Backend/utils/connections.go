package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type LivePrices struct {
	sync.RWMutex
	Prices map[string]MixedQuote
}

type OpenPositions struct {
	sync.RWMutex
	Positions map[string]OpenPositionDetails
}

type Balance struct {
	sync.RWMutex
	Balance float64
	Cash    float64
}

var GlobalPrices = &LivePrices{
	Prices: make(map[string]MixedQuote),
}

var GlobalOpenPositions = &OpenPositions{
	Positions: make(map[string]OpenPositionDetails),
}

var GlobalBalance = &Balance{
	Balance: 0,
	Cash:    0,
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all connections; adjust for production!
	},
}

var (
	Clients   = make(map[string]*Client)
	ClientsMu sync.RWMutex
)

var ctx = context.Background()

type Hub struct {
	// Registered clients.
	Clients map[*websocket.Conn]bool
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
		Clients:    make(map[*websocket.Conn]bool),
	}
}

func InitRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379", // Default Redis port
		Password: "",           // No password set by default
		DB:       0,            // Use default DB
	})

	// Verify connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Could not connect to Redis: %v", err))
	}

	return rdb
}

func ListenToRedis(ctx context.Context, rdb *redis.Client, hub *Hub, channel string) {
	pubsub := rdb.Subscribe(ctx, channel)
	ch := pubsub.Channel()
	switch channel {
	case "Stream_Channel":
		for msg := range ch {
			HandleClientRead(*msg)
		}
	case "Company_Channel":
		for msg := range ch {
			HandleCompanyRead(*msg)
		}

	case "One_Time_Data_Channel":
		for msg := range ch {
			HandleOptionRead(*msg)
		}
	}
}

func SendToRedis(data []byte, ctx context.Context, rdb *redis.Client, channel string) error {
	err := rdb.Publish(ctx, channel, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish to redis: %v", err)
	}
	return nil
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.Clients[client] = true
		case client := <-h.unregister:
			delete(h.Clients, client)
		case message := <-h.broadcast:
			for client := range h.Clients {
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
	if err := cmd.Run(); err == nil {
		fmt.Println("Redis container started!")
		return
	}

	fmt.Println("Container not starting. Cleaning up and recreating...")
	exec.Command(path, "rm", "-f", "redis-server").Run()

	// 3. Now try to run a fresh one
	runCmd := exec.Command(path, "run", "-d", "--name", "redis-server", "-p", "6380:6379", "redis")
	if err := runCmd.Run(); err != nil {
		// If it STILL fails, it's almost certainly a port conflict on 6379
		fmt.Printf("Critical Error: %v\n", err)
		fmt.Println("Check if port 6379 is already used by a local Redis installation.")
	} else {
		fmt.Println("Redis container is up and running!")
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
	select {
	case hub.register <- ws:
		log.Println("Hub registration successful")
	default:
		log.Println("Hub registration busy, retrying in background...")
		go func() { hub.register <- ws }()
	}

	ClientsMu.Lock()
	if oldClient, exists := Clients[clientID]; exists {
		log.Printf("Client %s already connected — replacing connection", clientID)
		// 1. Trigger the close
		oldClient.Conn.Close()
		select {
		case <-oldClient.Done:
		default:
			close(oldClient.Done)
		}
		delete(Clients, clientID)
		ClientsMu.Unlock()

		// 2. SMALL PAUSE: allow the old goroutine to exit the select loop
		time.Sleep(50 * time.Millisecond)

		ClientsMu.Lock()
	}

	newClient := &Client{
		Conn: ws,
		ID:   clientID,
		Done: make(chan struct{}),
	}
	Clients[clientID] = newClient
	ClientsMu.Unlock()

	defer func() {
		hub.unregister <- ws // Unregister on disconnect
		DisconnectClient(clientID)
	}()

	log.Printf("Client connected: %s", clientID)

	if clientID == "STOCK_CLIENT" {
		Clients[clientID].IsWriting = false
		SendOpenPositions(balanceDB, openDB, priceDB, trackerDB, Clients)
		go HandleClientWrite(newClient, openDB, balanceDB, priceDB)
	}

	<-newClient.Done

}

func DisconnectClient(clientID string) {
	ClientsMu.Lock()
	client, exists := Clients[clientID]
	if !exists {
		log.Printf("Client %s was already removed", clientID)
		ClientsMu.Unlock()
		return
	}
	delete(Clients, clientID)
	ClientsMu.Unlock()

	client.Conn.Close()
	client.Close() // safe: Once ensures it's only closed once
	log.Printf("Client disconnected: %s", clientID)
}

func ShutdownAllClients() {
	ClientsMu.Lock()
	defer ClientsMu.Unlock()

	for id, client := range Clients {
		log.Printf("Closing connection for client: %s", id)
		client.Conn.Close()
		client.Close()
		select {
		case <-client.Done:
		default:
			close(client.Done)
		}
		delete(Clients, id)
	}
}

func CompanyHandler(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	ticker := r.URL.Query().Get("ticker")
	company_request := Company_Request{Symbol: ticker}
	msg, err := json.Marshal(company_request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	SendToRedis(msg, context.Background(), rdb, "Request_Channel")
}

func HandleCompanyRead(msg redis.Message) {
	var company Company_Stats
	payloadBytes := []byte(msg.Payload)
	if err := json.Unmarshal(payloadBytes, &company); err != nil {
		// If it’s not a quotes payload, skip or handle other message types here
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}
	client := "STOCK_CLIENT"
	if Clients[client] == nil {
		return
	}

	if err := send(Clients[client], company); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = Clients[client].Conn.WriteMessage(websocket.TextMessage, errMsg)
		return
	}
}

func HandleOptionRead(msg redis.Message) {
	var option OptionExpiration
	payloadBytes := []byte(msg.Payload)
	if err := json.Unmarshal(payloadBytes, &option); err != nil {
		// If it’s not a quotes payload, skip or handle other message types here
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}

	client := "STOCK_CLIENT"
	if Clients[client] == nil {
		return
	}
	if err := send(Clients[client], option); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = Clients[client].Conn.WriteMessage(websocket.TextMessage, errMsg)
		return
	}
}

// Receives and handles a websocket message from python client and sends data to frontend
func HandleClientRead(msg redis.Message) {
	var quotes map[string]MixedQuote
	payloadBytes := []byte(msg.Payload)
	if err := json.Unmarshal(payloadBytes, &quotes); err != nil {
		// If it’s not a quotes payload, skip or handle other message types here
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}
	GlobalPrices.Lock()
	// Instead of replacing the whole map, we update existing keys
	// This preserves data if one broadcast only contains a subset of symbols
	maps.Copy(GlobalPrices.Prices, quotes)
	GlobalPrices.Unlock()
}

// Incremently writes balance to the Frontend
func HandleClientWrite(client *Client, openDB, balanceDB, priceDB *sql.DB) {
	client.Mu.Lock()
	// If already writing, just unlock and leave
	if client.IsWriting {
		client.Mu.Unlock()
		return
	}

	// Otherwise, set the flag and start the loop
	client.IsWriting = true
	client.Mu.Unlock()
	defer func() {
		client.Mu.Lock()
		client.IsWriting = false
		client.Mu.Unlock()
	}()
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
		ProcessWrite(t, client, balanceDB, openDB)
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
			ProcessWrite(t, client, balanceDB, openDB)
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

	err = SendToRedis(msg, context.Background(), rdb, "Request_Channel")
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
	err = SendToRedis(msg, context.Background(), rdb, "Request_Channel")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Write to a specific client the most recent balance
func ProcessWrite(t time.Time, client *Client, balanceDB, openDB *sql.DB) {
	// Don't write if we don't have any data
	if len(GlobalPrices.Prices) > 0 || GlobalBalance.Balance == 0 {
		var cash float64
		var balance float64

		balance = 10000.0
		cash = 10000.0

		balance, cash = GlobalBalance.Balance, GlobalBalance.Cash

		tempPositionValue := 0.0

		for id, details := range GlobalOpenPositions.Positions {
			amount := details.Amount

			GlobalPrices.RLock() // Lock for reading
			q, exists := GlobalPrices.Prices[id]
			GlobalPrices.RUnlock()
			var mark float64
			if exists {
				mark = q.Mark
			} else {
				log.Printf("Mark not found for %s", id)
				continue
			}
			if len(id) > 6 {
				tempPositionValue += (mark * float64(amount) * 100)
			} else {
				tempPositionValue += (mark * float64(amount))
			}
		}

		tempBalance := cash + tempPositionValue
		balance = tempBalance

		GlobalBalance.Lock()
		GlobalBalance.Balance = balance
		GlobalBalance.Cash = cash
		GlobalBalance.Unlock()
		now := time.Now().Unix()

		insertData := `INSERT OR IGNORE INTO Balance (timestamp, balance, cash) VALUES (?, ?, ?)`
		_, err := balanceDB.Exec(insertData, now, balance, cash)
		if err != nil {
			log.Printf("Failed to insert initial balance into table: %v", err)
		}
		stockPrices := make([]StockPriceData, 0, len(GlobalPrices.Prices))
		optionPrices := make([]OptionPriceData, 0, len(GlobalPrices.Prices))

		for symbol, q := range GlobalPrices.Prices {
			switch {
			// Equity quote if BidSize/AskSize are present
			case q.BidSize != nil && q.AskSize != nil:
				stock := StockPriceData{
					Symbol:    symbol,
					Timestamp: now,
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
					Timestamp: now,
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
		message := BalanceData{
			Balance:   balance,
			Timestamp: t.Unix(),
			Cash:      cash,
		}

		if len(optionPrices) > 0 {
			if err := send(client, optionPrices); err != nil {
				errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
				return
			}
		}
		if len(stockPrices) > 0 {
			if err := send(client, stockPrices); err != nil {
				errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
				return
			}
		}
		if err := send(client, message); err != nil {
			errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
			_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
			return
		}
	} else {
		log.Printf("No data yet")
	}
}

func send(client *Client, v interface{}) error {
	msg, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// fmt.Println("Sending to client:", string(msg))
	return SendToClient(client, msg)
}
