package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
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

// Handles all websocket connections
func WebsocketConnectHandler(w http.ResponseWriter, r *http.Request) {
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
		symbols = SendTrackerSymbols()

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
				err = SendToClient(clients[clientID], msg)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			} else {
				request := StockStreamRequest{
					Symbol: symbol,
				}
				msg, err := json.Marshal(request)
				if err != nil {
					break
				}
				err = SendToClient(clients[clientID], msg)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}

	}

	go HandleClientRead(newClient)
	if clientID == "STOCK_CLIENT" {
		SendOpenPositions(clients)
		go HandleClientWrite(newClient)
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
func HandleClientRead(client *Client) {
	for {
		msg, err := ReceiveFromClient(client)
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

		var quotes map[string]MixedQuote
		if err := json.Unmarshal(msg, &quotes); err != nil {
			// If it’s not a quotes payload, skip or handle other message types here
			log.Printf("Invalid quotes JSON from %s: %v", client.ID, err)
			continue
		}
		optionPrices := make([]OptionPriceData, 0, len(quotes))
		stockPrices := make([]StockPriceData, 0, len(quotes))
		timestamp := time.Now().Unix()
		// Process each symbol
		for symbol, q := range quotes {
			switch {
			// Equity quote if BidSize/AskSize are present
			case q.BidSize != nil && q.AskSize != nil:
				log.Printf(
					"Equity %s: bid=%.2f (size=%d) ask=%.2f (size=%d) last=%.2f",
					symbol, q.BidPrice, *q.BidSize, q.AskPrice, *q.AskSize, q.LastPrice,
				)
				stock := StockPriceData{
					Symbol:    symbol,
					Timestamp: timestamp,
					Mark:      math.Round(((q.AskPrice+q.BidPrice)/2)*100) / 100,
				}

				stockPrices = append(stockPrices, stock)

			// Option quote if IV or Greeks are present
			case q.IV != nil:
				log.Printf(
					"Option %s: bid=%.2f ask=%.2f last=%.2f high=%.2f IV=%.2f Δ=%.2f Γ=%.2f Θ=%.2f ν=%.2f",
					symbol, q.BidPrice, q.AskPrice, q.LastPrice, *q.HighPrice,
					*q.IV, *q.Delta, *q.Gamma, *q.Theta, *q.Vega,
				)
				option := OptionPriceData{
					Symbol:    symbol,
					Timestamp: timestamp,
					Bid:       q.BidPrice,
					Ask:       q.AskPrice,
					Mark:      math.Round(((q.AskPrice+q.BidPrice)/2)*100) / 100,
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
		// Send both payloads as two JSON arrays
		client := clients["STOCK_CLIENT"]
		if client == nil {
			log.Println("Stock Client is not connected")
			continue
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
}

// Incremently writes balance to the Frontend
func HandleClientWrite(client *Client) {
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

// Sends a message to the python streamer to start a subscription to a certain option ID
func StartOptionStream(w http.ResponseWriter, r *http.Request) {
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

	err = SendToClient(clients["PYTHON_CLIENT"], msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Sends a message to the python streamer to start a subscription to a certain stock
func StartStockStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	request := StockStreamRequest{
		Symbol: symbol,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		return
	}
	err = SendToClient(clients["PYTHON_CLIENT"], msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}

// Write to a specific client the most recent balance
func processWrite(t time.Time, client *Client) {
	openPositions := make(map[string]int)
	var realBalance float64
	var balance float64

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
		log.Printf("Failed to enable WAL after retries: %v", err)
	}
	date := TodayDate()

	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s" (
		timestamp INTEGER PRIMARY KEY,
		balance REAL NOT NULL,
		realBalance REAL NOT NULL	
	);`, date)
	_, err = balanceDB.Exec(createTableSQL)
	if err != nil {
		log.Printf("Failed to create today's table in Balance.db: %v", err)
		return
	}

	tables, err := getDateTables(balanceDB)
	if err != nil {
		log.Printf("Failed to get date tables in Balance.db: %v", err)
		return
	}

	// Sort tables in descending order (newest dates first)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i] > tables[j]
	})
	balance = 10000
	realBalance = 10000 // default fallback

	found := false
	for _, tbl := range tables {
		query := fmt.Sprintf(`SELECT timestamp, balance, realBalance FROM "%s" ORDER BY timestamp DESC LIMIT 1`, tbl)
		row := balanceDB.QueryRow(query)

		var timestamp int64
		err := row.Scan(&timestamp, &balance, &realBalance)
		if err == sql.ErrNoRows {
			continue // table exists but is empty — try earlier table
		} else if err != nil {
			log.Printf("Failed to query table %s: %v", tbl, err)
			continue
		}

		found = true
		break
	}

	if !found {
		log.Printf("No existing balances found; using default balance: %.2f", realBalance)
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
		log.Printf("Failed to enable WAL after retries: %v", err)
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
		log.Printf("Query failed process write openDB: %v", err)
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

	tempBalance := balance

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
			log.Printf("Failed to enable WAL after retries: %v", err)
		}
		date := TodayDate()
		query := fmt.Sprintf(`SELECT * FROM "%s" ORDER BY timestamp DESC LIMIT 1`, date)
		row := db.QueryRow(query)
		if len(names) > 6 {
			var timestamp int64
			var bid, ask, last, high, iv, delta, gamma, theta, vega float64

			err = row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
			if err != nil {
				log.Printf("Query failed process write position file: %v", err)
				return
			}
			mark := math.Round(((ask+bid)/2)*100) / 100
			tempBalance += (mark * float64(amount)) * 100
		} else {
			var timestamp, bidSize, askSize int64
			var bidPrice, askPrice, lastPrice float64
			err = row.Scan(&timestamp, &bidPrice, &askPrice, &lastPrice, &bidSize, &askSize)
			if err != nil {
				log.Printf("Query failed process write position file: %v", err)
				return
			}
			mark := math.Round(((askPrice+bidPrice)/2)*100) / 100
			tempBalance += (mark * float64(amount))
		}
	}
	realBalance = tempBalance
	if err := rows.Err(); err != nil {
		log.Println("Rows iteration error:", err)
	}

	insertData := fmt.Sprintf(`INSERT OR REPLACE INTO "%s" (timestamp, balance, realBalance) VALUES (?, ?, ?)`, date)
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, realBalance)
	if err != nil {
		log.Printf("Failed to insert initial balance into table %s: %v", date, err)
	}
	message := StockPriceData{
		Symbol:    "balance",
		Timestamp: t.Unix(),
		Mark:      realBalance,
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
