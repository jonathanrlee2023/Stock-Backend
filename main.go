package main

import (
	"Go-API/utils"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
)

type OptionStreamRequest struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
	Day    string `json:"day"`
	Month  string `json:"month"`
	Year   string `json:"year"`
	Type   string `json:"type"`
}

type StockStreamRequest struct {
	Symbol string `json:"symbol"`
}

var wsConn *websocket.Conn

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
	go startWebsocketConnection()

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
	mux.HandleFunc("/startOptionStream", StartOptionStream)
	mux.HandleFunc("/startStockStream", StartStockStream)
	mux.HandleFunc("/dataReady", utils.PostHandler)

	handler := CorsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func startWebsocketConnection() {
	u := url.URL{Scheme: "ws", Host: "localhost:8765", Path: "/"}

	log.Printf("Connecting to %s...", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("Dial error:", err)
	}

	wsConn = c
	log.Println("WebSocket connected and ready.")

	for {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			return
		}
		log.Printf("Received: %s", message)
	}
}

func SendToWebSocketOption(symbol, price, day, month, year, optionType string) error {
	if wsConn == nil {
		return fmt.Errorf("WebSocket connection not established")
	}

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
		return err
	}

	err = wsConn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		return err
	}

	return nil
}

func SendToWebSocketStock(symbol string) error {
	if wsConn == nil {
		return fmt.Errorf("WebSocket connection not established")
	}

	request := StockStreamRequest{
		Symbol: symbol,
	}

	msg, err := json.Marshal(request)
	if err != nil {
		return err
	}

	err = wsConn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		return err
	}

	return nil
}

func StartOptionStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	price := r.URL.Query().Get("price")
	day := r.URL.Query().Get("day")
	month := r.URL.Query().Get("month")
	year := r.URL.Query().Get("year")
	optionType := r.URL.Query().Get("type")

	err := SendToWebSocketOption(symbol, price, day, month, year, optionType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}

func StartStockStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")

	err := SendToWebSocketStock(symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Sent to WebSocket!")
}
