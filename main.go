package main

import (
	"Go-API/utils"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	ID   string // unique identifier for this client (e.g., user ID, session ID)
	Mu   sync.Mutex
	Done chan struct{}
}

func (c *Client) SafeWrite(messageType int, data []byte) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

var (
	clients   = make(map[string]*Client)
	clientsMu sync.RWMutex
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all connections; adjust for production!
	},
}

func main() {
	// Empty cache folder everytime API is turned on
	folderPath := []string{"./StockDataCache", "./TodayStockDataCache"}
	for _, value := range folderPath {
		err := utils.DeleteContents(value)
		if err != nil {
			fmt.Println("Error:", err)
		}
	}

	// start api
	startApiServer() // blocks forever
}

// Function that handles http permissions
func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func startApiServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", websocketConnectHandler)
	mux.HandleFunc("/startOptionStream", StartOptionStream)
	mux.HandleFunc("/startStockStream", StartStockStream)
	mux.HandleFunc("/dataReady", utils.DataReadyHandler)
	mux.HandleFunc("/newTracker", utils.NewTrackerHandler)
	mux.HandleFunc("/openPosition", utils.OpenPositionHandler)
	mux.HandleFunc("/closePosition", utils.ClosePositionHandler)
	mux.HandleFunc("/closeTracker", utils.RemoveTrackerHandler)

	handler := CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func websocketConnectHandler(w http.ResponseWriter, r *http.Request) {
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

	newClient := &Client{
		Conn: ws,
		ID:   clientID,
		Done: make(chan struct{}),
	}
	if oldConn, exists := clients[clientID]; exists {
		log.Printf("Client %s already connected — replacing connection", clientID)

		oldConn.Conn.Close()

		// Wait for old goroutines to finish by waiting for Done channel to close
		<-oldConn.Done

		// Now safe to delete old client
		clientsMu.Lock()
		delete(clients, clientID)
		clientsMu.Unlock()
	}
	clients[clientID] = newClient
	clientsMu.Unlock()

	log.Printf("Client connected: %s", clientID)

	defer func() {
		clientsMu.Lock()
		client, exists := clients[clientID]
		if exists {
			delete(clients, clientID)
			client.Conn.Close()
			select {
			case <-client.Done:
				// already closed
			default:
				close(client.Done)
			}
			log.Printf("Client disconnected: %s", clientID)
		} else {
			log.Printf("Client %s was already removed", clientID)
		}
		clientsMu.Unlock()
	}()

	// Start reader and writer goroutines
	if clientID == "PYTHON_CLIENT" {
		var symbols []string
		symbols = sendTrackerSymbols()
		for _, symbol := range symbols {
			request, err := ParseOptionString(symbol)
			if err != nil {
				log.Printf("Could not parse string")
				return
			}
			msg, err := json.Marshal(request)
			if err != nil {
				break
			}
			err = sendToClient("PYTHON_CLIENT", msg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	go handleClientRead(newClient)
	if clientID == "STOCK_CLIENT" {
		go handleClientWrite(newClient)
	}

	<-newClient.Done
}

func StartOptionStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	price := r.URL.Query().Get("price")
	day := r.URL.Query().Get("day")
	month := r.URL.Query().Get("month")
	year := r.URL.Query().Get("year")
	optionType := r.URL.Query().Get("type")

	request := utils.OptionStreamRequest{
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

	err = sendToClient("PYTHON_CLIENT", msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func StartStockStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	request := utils.StockStreamRequest{
		Symbol: symbol,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		return
	}
	err = sendToClient("PYTHON_CLIENT", msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}

func sendToClient(clientID string, msg []byte) error {
	client, ok := clients[clientID]
	if !ok {
		return fmt.Errorf("client %s not connected", clientID)
	}

	err := client.SafeWrite(websocket.TextMessage, msg)
	if err != nil {
		return err
	}
	log.Printf("sent to %s: %s", clientID, string(msg))
	return nil
}
func receiveFromClient(client *Client) ([]byte, error) {
	_, msg, err := client.Conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func handleClientRead(client *Client) {
	for {
		msg, err := receiveFromClient(client)
		if err != nil {
			log.Printf("Receive error for client %s: %v", client.ID, err)
			select {
			case <-client.Done:
				// already closed
			default:
				close(client.Done)
			}
			return
		}

		select {
		case <-client.Done:
			log.Printf("handleClientRead exiting for client %s", client.ID)
			return
		default:
		}

		var incoming struct {
			Type      string   `json:"type"`
			Filenames []string `json:"filenames"`
		}
		if err := json.Unmarshal(msg, &incoming); err != nil {
			log.Printf("Invalid message from client %s: %v", client.ID, err)
			continue
		}

		fmt.Printf("Received from %s: %+v\n", client.ID, incoming)

		if incoming.Type == "dataReady" {
			getRecentPrices(incoming.Filenames, client.Conn)
		}
	}
}

func handleClientWrite(client *Client) {
	// Example: Send incremental updates every 30 seconds
	now := time.Now()
	wait := 15*time.Second - (time.Duration(now.Second()%15)*time.Second + time.Duration(now.Nanosecond()))
	timer := time.NewTimer(wait)

	select {
	case <-client.Done:
		log.Printf("handleClientWrite exiting for client %s", client.ID)
		timer.Stop()
		return
	case t := <-timer.C:
		processWrite(t, client)
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-client.Done:
			log.Printf("handleClientWrite exiting for client %s", client.ID)
			return
		case t := <-ticker.C:
			processWrite(t, client)
		}
	}
}

func getRecentPrices(FileNames []string, ws *websocket.Conn) {
	for _, fileName := range FileNames {
		db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", fileName))
		if err != nil {
			log.Printf("Query failed recent prices: %v", err)
		}
		defer db.Close()
		if len(fileName) > 5 {
			row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

			var timestamp int64
			var bid, ask, last, high, iv, delta, gamma, theta, vega float64

			err := row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
			if err != nil {
				log.Printf("Query failed recent prices: %v", err)
				return
			}
			data := utils.OptionPriceData{
				Symbol:    fileName,
				Timestamp: timestamp,
				Bid:       bid,
				Ask:       ask,
				Mark:      math.Round(((ask+bid)/2)*100) / 100,
				Last:      last,
				High:      high,
				IV:        iv,
				Delta:     delta,
				Gamma:     gamma,
				Theta:     theta,
				Vega:      vega,
			}
			msg, err := json.Marshal(data)
			if err != nil {
				return
			}
			err = sendToClient("STOCK_CLIENT", msg)
			if err != nil {
				errMsg := map[string]string{"error": err.Error()}
				msg, _ := json.Marshal(errMsg)
				_ = ws.WriteMessage(websocket.TextMessage, msg)
				return
			}
		} else {
			row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

			var timestamp int64
			var bid, ask, last float64
			var askSize, bidSize int64

			err := row.Scan(&timestamp, &bid, &ask, &last, &askSize, &bidSize)
			if err != nil {
				log.Printf("Query failed recent prices: %v", err)
				return
			}
			data := utils.StockPriceData{
				Symbol:    fileName,
				Timestamp: timestamp,
				Mark:      math.Round(((ask+bid)/2)*100) / 100,
			}
			msg, err := json.Marshal(data)
			if err != nil {
				return
			}
			err = sendToClient("STOCK_CLIENT", msg)
			if err != nil {
				errMsg := map[string]string{"error": err.Error()}
				msg, _ := json.Marshal(errMsg)
				_ = ws.WriteMessage(websocket.TextMessage, msg)
				return
			}
		}
	}
}

func processWrite(t time.Time, client *Client) {
	openPositions := make(map[string]int)
	var realBalance float64

	balanceDB, err := sql.Open("sqlite", "Balance.db")
	if err != nil {
		log.Printf("Query failed process write balanceDB: %v", err)
		return
	}
	defer balanceDB.Close()
	for i := 0; i < 3; i++ {
		_, err = balanceDB.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS Balance (
			timestamp INTEGER PRIMARY KEY,
			balance FLOAT NOT NULL
		);`
	_, err = balanceDB.Exec(createTableSQL)
	if err != nil {
		log.Printf("Query failed process write balanceDB: %v", err)
		return
	}
	row := balanceDB.QueryRow("SELECT * FROM Balance ORDER BY timestamp DESC LIMIT 1")

	var timestamp int64

	err = row.Scan(&timestamp, &realBalance)
	if err == sql.ErrNoRows {
		realBalance = 10000
	} else if err != nil {
		log.Printf("Query failed process write balanceDB: %v", err)
		return
	}
	insertData := `INSERT OR REPLACE INTO Balance (timestamp, balance) VALUES (?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), realBalance)
	if err != nil {
		log.Printf("Query failed process write balanceDB: %v", err)
	}

	openDB, err := sql.Open("sqlite", "Open.db")
	if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer openDB.Close()
	for i := 0; i < 3; i++ {
		_, err = openDB.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}
	createTableSQL = `
		CREATE TABLE IF NOT EXISTS OpenPositions (
			id STRING PRIMARY KEY,
			price FLOAT NOT NULL,
			amount INTEGER NOT NULL
		);`
	_, err = openDB.Exec(createTableSQL)
	rows, err := openDB.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
	} else if err != nil {
		log.Fatal("Query failed process write openDB:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var price float64
		var amount int64

		err := rows.Scan(&id, &price, &amount)
		if err != nil {
			log.Println("Scan failed:", err)
			continue
		}

		openPositions[id] = int(amount)
	}

	for names, amount := range openPositions {
		db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", names))
		if err != nil {
			log.Printf("Query failed process write position file: %v", err)
			return
		}
		defer db.Close()
		for i := 0; i < 3; i++ {
			_, err = db.Exec("PRAGMA journal_mode=WAL;")
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			log.Fatal("Failed to enable WAL after retries:", err)
		}
		row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

		var timestamp int64
		var bid, ask, last, high, iv, delta, gamma, theta, vega float64

		err = row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
		if err != nil {
			log.Printf("Query failed process write position file: %v", err)
			return
		}
		mark := math.Round(((ask+bid)/2)*100) / 100
		realBalance += (mark * float64(amount)) * 100
	}
	if err := rows.Err(); err != nil {
		log.Println("Rows iteration error:", err)
	}
	message := utils.StockPriceData{
		Symbol:    "balance",
		Timestamp: t.Unix(),
		Mark:      realBalance,
	}

	msg, err := json.Marshal(message)
	if err != nil {
		return
	}
	err = sendToClient("STOCK_CLIENT", msg)
	if err != nil {
		errMsg := map[string]string{"error": err.Error()}
		msg, _ := json.Marshal(errMsg)
		_ = client.SafeWrite(websocket.TextMessage, msg)
		return
	}
}

func sendTrackerSymbols() []string {
	var symbols []string
	db, err := sql.Open("sqlite", "Tracker.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT id FROM Tracker")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string

		err := rows.Scan(&id)
		if err != nil {
			panic(err)
		}

		symbols = append(symbols, id)
	}
	return symbols
}

func ParseOptionString(s string) (utils.OptionStreamRequest, error) {
	var req utils.OptionStreamRequest

	// Split at underscore
	parts := []rune(s)
	underscoreIndex := -1
	for i, c := range parts {
		if c == '_' {
			underscoreIndex = i
			break
		}
	}

	if underscoreIndex == -1 || len(parts) < underscoreIndex+13 {
		return req, fmt.Errorf("invalid string format")
	}

	req.Symbol = string(parts[:underscoreIndex])

	req.Year = string(parts[underscoreIndex+1 : underscoreIndex+3])
	req.Month = string(parts[underscoreIndex+3 : underscoreIndex+5])
	req.Day = string(parts[underscoreIndex+5 : underscoreIndex+7])
	req.Type = string(parts[underscoreIndex+7])

	// Extract strike price
	priceStr := string(parts[underscoreIndex+8:])
	if len(priceStr) < 8 {
		return req, fmt.Errorf("invalid strike price length")
	}
	// convert to decimal
	priceInt, err := strconv.Atoi(priceStr)
	if err != nil {
		return req, fmt.Errorf("invalid price number: %v", err)
	}
	req.Price = fmt.Sprintf("%.2f", float64(priceInt)/1000.0)

	return req, nil
}
