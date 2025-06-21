package main

import (
	"Go-API/utils"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	ID   string // unique identifier for this client (e.g., user ID, session ID)
	Mu   sync.Mutex
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
	mux.HandleFunc("/options", utils.OptionsHandler)
	mux.HandleFunc("/earningsCalender", utils.EarningsCalenderHandler)
	mux.HandleFunc("/stock", utils.StockHandler)
	mux.HandleFunc("/earningsVolatility", utils.EarningsVolatilityHandler)
	mux.HandleFunc("/todayStock", utils.TodayStockHandler)
	mux.HandleFunc("/economicData", utils.EconomicDataHandler)
	mux.HandleFunc("/impliedVolatility", utils.OptionVolatilityHandler)
	mux.HandleFunc("/combinedOptions", utils.CombinedOptionsHandler)
	mux.HandleFunc("/connect", websocketConnectHandler)
	mux.HandleFunc("/startOptionStream", StartOptionStream)
	mux.HandleFunc("/startStockStream", StartStockStream)
	mux.HandleFunc("/dataReady", utils.DataReadyHandler)
	mux.HandleFunc("/newTracker", utils.NewTrackerHandler)
	mux.HandleFunc("/newPosition", utils.OpenPositionHandler)

	handler := CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

// func startWebsocketConnection() {
// 	u := url.URL{Scheme: "ws", Host: "localhost:8765", Path: "/"}

// 	log.Printf("Connecting to %s...", u.String())

// 	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
// 	if err != nil {
// 		log.Fatal("Dial error:", err)
// 	}

// 	wsConn = c
// 	log.Println("WebSocket connected and ready.")

// 	for {
// 		_, message, err := wsConn.ReadMessage()
// 		if err != nil {
// 			log.Println("Read error:", err)
// 			return
// 		}
// 		log.Printf("Received: %s", message)
// 	}
// }

func websocketConnectHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer ws.Close()

	clientID := r.URL.Query().Get("id")

	clientsMu.Lock()
	if _, exists := clients[clientID]; exists {
		clientsMu.Unlock()
		_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"error":"client ID already connected"}`))
		return
	}
	clients[clientID] = &Client{Conn: ws, ID: clientID}
	clientsMu.Unlock()
	log.Printf("Client connected: %s", clientID)

	defer func() {
		clientsMu.Lock()
		delete(clients, clientID)
		clientsMu.Unlock()
		ws.Close()
		log.Printf("Client disconnected: %s", clientID)
	}()

	// Start reader and writer goroutines
	done := make(chan struct{})

	go handleClientRead(ws, clientID, done)
	if clientID == "STOCK_CLIENT" {
		go handleClientWrite(ws, done)
	}

	// Wait for either read or write to signal done
	<-done
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
func receiveFromClient(clientID string) ([]byte, error) {
	client, ok := clients[clientID]
	if !ok {
		return nil, fmt.Errorf("client %s not connected", clientID)
	}

	_, msg, err := client.Conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func handleClientRead(ws *websocket.Conn, clientID string, done chan struct{}) {
	defer close(done)
	for {
		msg, err := receiveFromClient(clientID)
		if err != nil {
			log.Println("Receive error:", err)
			break
		}
		var incoming struct {
			Type      string   `json:"type"`
			Filenames []string `json:"filenames"`
		}
		if err := json.Unmarshal(msg, &incoming); err != nil {
			log.Println("Invalid message:", err)
			continue
		}
		fmt.Println(incoming)
		if incoming.Type == "dataReady" {
			getRecentPrices(incoming.Filenames, ws)
		}
	}
}

func handleClientWrite(ws *websocket.Conn, done chan struct{}) {
	// Example: Send incremental updates every 30 seconds
	now := time.Now()
	wait := 30*time.Second - (time.Duration(now.Second()%30)*time.Second + time.Duration(now.Nanosecond()))
	timer := time.NewTimer(wait)

	select {
	case <-done:
		timer.Stop()
		return
	case t := <-timer.C:
		processWrite(t, ws, done)
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			processWrite(t, ws, done)
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
			err = sendToClient("OPTIONS_CLIENT", msg)
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

func processWrite(t time.Time, ws *websocket.Conn, done chan struct{}) {
	openPositions := make(map[string]int)
	var realBalance float64

	balanceDB, err := sql.Open("sqlite", "Balance.db")
	if err != nil {
		log.Printf("Query failed process write balanceDB: %v", err)
		return
	}
	defer balanceDB.Close()
	_, err = balanceDB.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		log.Fatal("Failed to enable WAL mode:", err)
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
	_, err = openDB.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		log.Fatal("Failed to enable WAL mode:", err)
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
		_, err = db.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			log.Fatal("Failed to enable WAL mode:", err)
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
		_ = ws.WriteMessage(websocket.TextMessage, msg)
		return
	}
}
