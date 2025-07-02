package main

import (
	"Go-API/utils"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	ID   string // unique identifier for this client (e.g., user ID, session ID)
	Mu   sync.Mutex
	Done chan struct{}
	once sync.Once
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.Done)
	})
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
var ids []string

func main() {
	stop := make(chan os.Signal, 1)
	// Check if ctrl C is pressed
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	WriteEODData()
	// Endpoints for API
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

	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// Run server in a goroutine
	go func() {
		fmt.Println("Server is running on port 8080...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down gracefully...")

	shutdownAllClients()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Println("Server exited")
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

// Handles all websocket connections
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
	if _, exists := clients[clientID]; exists {
		log.Printf("Client %s already connected — replacing connection", clientID)
		DisconnectClient(clientID)
	}
	clients[clientID] = newClient
	clientsMu.Unlock()

	log.Printf("Client connected: %s", clientID)

	defer func() {
		DisconnectClient(clientID)
	}()

	// Start reader and writer goroutines
	if clientID == "PYTHON_CLIENT" {
		var symbols []string
		symbols = sendTrackerSymbols()
		if symbols != nil {
			for _, symbol := range symbols {
				if len(symbol) > 6 {
					request, err := ParseOptionString(symbol)
					if err != nil {
						log.Printf("Could not parse string")
						return
					}
					msg, err := json.Marshal(request)
					if err != nil {
						break
					}
					err = sendToClient(clients[clientID], msg)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
				} else {
					request := utils.StockStreamRequest{
						Symbol: symbol,
					}
					msg, err := json.Marshal(request)
					if err != nil {
						break
					}
					err = sendToClient(clients[clientID], msg)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
				}
			}
		}

	}

	go handleClientRead(newClient)
	if clientID == "STOCK_CLIENT" {
		sendOpenPositions()
		go handleClientWrite(newClient)
	}

	<-newClient.Done
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

func shutdownAllClients() {
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

// Sends a message to the python streamer to start a subscription to a certain option ID
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

	err = sendToClient(clients["PYTHON_CLIENT"], msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Sends a message to the python streamer to start a subscription to a certain stock
func StartStockStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	request := utils.StockStreamRequest{
		Symbol: symbol,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		return
	}
	err = sendToClient(clients["PYTHON_CLIENT"], msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}

// Takes json and sends it to client
func sendToClient(client *Client, msg []byte) error {
	err := client.SafeWrite(websocket.TextMessage, msg)
	if err != nil {
		return err
	}
	log.Printf("sent to %s: %s", client.ID, string(msg))
	return nil
}

// Reads a message from the client and returns an error if there is one
func receiveFromClient(client *Client) ([]byte, error) {
	_, msg, err := client.Conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// Receives and handles a websocket message
func handleClientRead(client *Client) {
	for {
		msg, err := receiveFromClient(client)
		if err != nil {
			log.Printf("Receive error for client %s: %v", client.ID, err)
			DisconnectClient(client.ID)
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

// Incremently writes balance to the Frontend
func handleClientWrite(client *Client) {
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
		processWrite(t, client)
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
			processWrite(t, client)
		}
	}
}

// Retrieve the most recent prices from an array of FileNames(tickers) and write to STOCK_CLIENT
func getRecentPrices(FileNames []string, ws *websocket.Conn) {
	for _, fileName := range FileNames {
		db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", fileName))
		if err != nil {
			log.Printf("Query failed recent 1 prices: %v", err)
		}
		for i := 0; i < 3; i++ {
			_, err = db.Exec("PRAGMA journal_mode=WAL;")
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		defer db.Close()
		if len(fileName) > 5 {
			row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

			var timestamp int64
			var bid, ask, last, high, iv, delta, gamma, theta, vega float64

			err := row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
			if err != nil {
				log.Printf("Query failed recent 2 prices: %v", err)
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
			err = sendToClient(clients["STOCK_CLIENT"], msg)
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
				log.Printf("Query failed recent 3 prices: %v", err)
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
			err = sendToClient(clients["STOCK_CLIENT"], msg)
			if err != nil {
				errMsg := map[string]string{"error": err.Error()}
				msg, _ := json.Marshal(errMsg)
				_ = ws.WriteMessage(websocket.TextMessage, msg)
				return
			}
		}
	}
}

// Write to a specific client the most recent balance
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
			balance REAL NOT NULL
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
			price REAL NOT NULL,
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
		if len(names) > 6 {
			var timestamp int64
			var bid, ask, last, high, iv, delta, gamma, theta, vega float64

			err = row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
			if err != nil {
				log.Printf("Query failed process write position file: %v", err)
				return
			}
			mark := math.Round(((ask+bid)/2)*100) / 100
			realBalance += (mark * float64(amount)) * 100
		} else {
			var timestamp, bidSize, askSize int64
			var bidPrice, askPrice, lastPrice float64
			err = row.Scan(&timestamp, &bidPrice, &askPrice, &lastPrice, &bidSize, &askSize)
			if err != nil {
				log.Printf("Query failed process write position file: %v", err)
				return
			}
			mark := math.Round(((askPrice+bidPrice)/2)*100) / 100
			realBalance += (mark * float64(amount))
		}
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
	err = sendToClient(clients["STOCK_CLIENT"], msg)
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
		log.Println("No Tracker")
		return nil
	}
	defer db.Close()

	rows, err := db.Query("SELECT id FROM Tracker")
	if err != nil {
		log.Println("No Tables in Tracker")
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var id string

		err := rows.Scan(&id)
		if err != nil {
			log.Println("No rows to read from")
			return nil
		}
		if len(id) > 6 {
			request, err := ParseOptionString(id)
			if err != nil {
				log.Println("Failed to parse")
			}
			expDate, err := ParseExpirationDate(request)
			if err != nil {
				log.Println("Failed to parse")
			}
			now := time.Now().UTC()
			if expDate.Before(now) {
				_, err := db.Exec("DELETE FROM Tracker WHERE id = ?", id)
				if err != nil {
					log.Println("Failed to delete expired tracker:", err)
				} else {
					log.Printf("Deleted expired tracker: %s (expired on %s)", id, expDate.Format(time.RFC3339))
				}
				continue
			}
			symbols = append(symbols, id)
		} else {
			symbols = append(symbols, id)
		}
	}
	return symbols
}

// Parses the full option id into each component
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

func ParseExpirationDate(req utils.OptionStreamRequest) (time.Time, error) {
	day, err := strconv.Atoi(req.Day)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day: %v", err)
	}
	month, err := strconv.Atoi(req.Month)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month: %v", err)
	}
	yearStr := req.Year
	var year int
	if len(yearStr) == 2 {
		// Interpret YY as 2000+YY if YY < 50, else 1900+YY
		yy, err := strconv.Atoi(yearStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year: %v", err)
		}
		if yy < 50 {
			year = 2000 + yy
		} else {
			year = 1900 + yy
		}
	} else if len(yearStr) == 4 {
		year, err = strconv.Atoi(yearStr)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year: %v", err)
		}
	} else {
		return time.Time{}, fmt.Errorf("invalid year length: %s", yearStr)
	}

	// Construct the time.Time object
	return time.Date(year, time.Month(month), day, 23, 59, 59, 0, time.UTC), nil
}

func sendOpenPositions() {
	openIDs := make(map[string]int64)
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
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS OpenPositions (
			id STRING PRIMARY KEY,
			price REAL NOT NULL,
			amount INTEGER NOT NULL
		);`
	_, err = openDB.Exec(createTableSQL)
	rows, err := openDB.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
		return
	} else if err != nil {
		log.Fatal("Query failed process write openDB:", err)
		return
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

		openIDs[id] = amount
	}
	msg := utils.OpenPositionsMessage{
		IDs: openIDs,
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println("Failed to marshal open positions:", err)
		return
	}

	err = sendToClient(clients["STOCK_CLIENT"], jsonData)
	if err != nil {
		errMsg := map[string]string{"error": err.Error()}
		msg, _ := json.Marshal(errMsg)
		_ = clients["STOCK_CLIENT"].SafeWrite(websocket.TextMessage, msg)
	}
}

func GetLastEntriesPerDay(db *sql.DB, fileName string) ([]utils.StockDbData, []utils.OptionDbData, error) {
	if len(fileName) > 8 {
		query := `
			SELECT timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega
			FROM prices
			JOIN (
				SELECT DATE(datetime(timestamp, 'unixepoch')) AS day, MAX(timestamp) AS latest_ts
				FROM prices
				GROUP BY day
			) AS daily_max
			ON DATE(datetime(timestamp, 'unixepoch')) = daily_max.day
			AND timestamp = daily_max.latest_ts
			ORDER BY timestamp;
		`

		rows, err := db.Query(query)
		if err != nil {
			return nil, nil, fmt.Errorf("query failed: %w", err)
		}
		defer rows.Close()
		var results []utils.OptionDbData
		for rows.Next() {
			var e utils.OptionDbData
			if err := rows.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.High, &e.Last, &e.IV, &e.Delta, &e.Gamma, &e.Theta, &e.Vega); err != nil {
				return nil, nil, fmt.Errorf("scan failed: %w", err)
			}
			results = append(results, e)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("rows iteration error: %w", err)

		}
		return nil, results, nil
	} else {
		query := `
			SELECT timestamp, bid_price, ask_price, last_price, bid_size, ask_size
			FROM prices
			JOIN (
				SELECT DATE(datetime(timestamp, 'unixepoch')) AS day, MAX(timestamp) AS latest_ts
				FROM prices
				GROUP BY day
			) AS daily_max
			ON DATE(datetime(timestamp, 'unixepoch')) = daily_max.day
			AND timestamp = daily_max.latest_ts
			ORDER BY timestamp;
		`

		rows, err := db.Query(query)
		if err != nil {
			return nil, nil, fmt.Errorf("query failed: %w", err)
		}
		defer rows.Close()
		var results []utils.StockDbData
		for rows.Next() {
			var e utils.StockDbData
			if err := rows.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.Last, &e.AskSize, &e.BidSize); err != nil {
				return nil, nil, fmt.Errorf("scan failed: %w", err)
			}
			results = append(results, e)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("rows iteration error: %w", err)

		}
		return results, nil, nil
	}
}

func WriteEODData() {
	dir := "." // current working directory

	excluded := map[string]struct{}{
		"Open.db":    {},
		"Close.db":   {},
		"Balance.db": {},
		"Tracker.db": {},
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(info.Name(), ".db") {
			return nil
		}

		if _, found := excluded[info.Name()]; found {
			fmt.Printf("Skipping excluded file: %s\n", info.Name())
			return nil
		}

		fmt.Printf("Processing database: %s\n", path)

		db, err := sql.Open("sqlite", path)
		if err != nil {
			log.Printf("Failed to open database %s: %v\n", path, err)
			return nil
		}
		defer db.Close()

		if len(path) > 8 {
			_, data, err := GetLastEntriesPerDay(db, path)
			fmt.Println(data)
			createTableSQL := `
				CREATE TABLE IF NOT EXISTS EODPrices (
					timestamp INTEGER PRIMARY KEY,
					bid_price REAL NOT NULL,
					ask_price REAL NOT NULL,
					last_price REAL NOT NULL,
					high_price REAL NOT NULL,
					iv REAL NOT NULL,
					delta REAL NOT NULL,
					gamma REAL NOT NULL,
					theta REAL NOT NULL,
					vega REAL NOT NULL
				);`

			if _, err := db.Exec(createTableSQL); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			insertSQL := `INSERT OR REPLACE INTO EODPrices (
                            timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
			stmt, err := db.Prepare(insertSQL)
			if err != nil {
				return fmt.Errorf("failed to prepare insert statement: %w", err)
			}
			defer stmt.Close()

			for _, entry := range data {
				_, err = stmt.Exec(entry.Timestamp, entry.Bid, entry.Ask, entry.Last, entry.High, entry.IV, entry.Delta, entry.Gamma, entry.Theta, entry.Vega)
				if err != nil {
					return fmt.Errorf("failed to insert entry %+v: %w", entry, err)
				}
			}
		} else {
			data, _, err := GetLastEntriesPerDay(db, path)
			fmt.Println(data)

			createTableSQL := `
				CREATE TABLE IF NOT EXISTS EODPrices (
					timestamp INTEGER PRIMARY KEY,
					bid_price REAL NOT NULL,
					ask_price REAL NOT NULL,
					last_price REAL NOT NULL,
					bid_size INTEGER NOT NULL,
					ask_size INTEGER NOT NULL
				);`

			if _, err := db.Exec(createTableSQL); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			insertSQL := `INSERT OR REPLACE INTO EODPrices (
                            timestamp, bid_price, ask_price, last_price, bid_size, ask_size
                        ) VALUES (?, ?, ?, ?, ?, ?)`
			stmt, err := db.Prepare(insertSQL)
			if err != nil {
				return fmt.Errorf("failed to prepare insert statement: %w", err)
			}
			defer stmt.Close()

			for _, entry := range data {
				_, err := stmt.Exec(entry.Timestamp, entry.Bid, entry.Ask, entry.Last, entry.BidSize, entry.AskSize)
				if err != nil {
					return fmt.Errorf("failed to insert entry %+v: %w", entry, err)
				}
			}

		}

		return nil
	})

	if err != nil {
		log.Fatalf("Failed walking folder: %v", err)
	}
}
