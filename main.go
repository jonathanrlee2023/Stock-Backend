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

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	ID   string // unique identifier for this client (e.g., user ID, session ID)
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
		w.Header().Set("Access-Control-Allow-Headers", "*")

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
	mux.HandleFunc("/dataReady", utils.PostHandler)

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
			for _, fileName := range incoming.Filenames {
				db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", fileName))
				if err != nil {
					log.Printf("Query failed: %v", err)
				}
				defer db.Close()
				if len(fileName) > 5 {
					row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

					var timestamp int64
					var bid, ask, last, high, iv, delta, gamma, theta, vega float64

					err := row.Scan(&timestamp, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
					if err != nil {
						log.Printf("Query failed: %v", err)
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
						log.Printf("Query failed: %v", err)
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
	}
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

	err := client.Conn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		return err
	}
	fmt.Println("sent to client" + string(msg))
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
