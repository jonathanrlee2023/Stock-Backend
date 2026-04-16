package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
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
			HandleRedisRead(*msg)
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
func WebsocketConnectHandler(hub *Hub, w http.ResponseWriter, r *http.Request) {
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
	UserID, err := getIDFromClient(clientID)
	if err != nil {
		log.Printf("Invalid client ID format: %s", clientID)
		return
	}

	ClientsMu.Lock()
	newClient := &Client{
        Conn:   ws,
        ID:     clientID,
        UserID: UserID,
        Done:   make(chan struct{}),
		send:   make(chan []byte, 256),
        Balance:       PortfolioBalances{Balances: make(map[int]*Balance)},
        PortfolioIDs:  Portfolio_IDs{IDs: make(map[int]string)},
        OpenPositions: OpenPositions{Positions: make(map[int]map[string]OpenPositionDetails)},
        IsWriting:     false,
    }
	if oldClient, exists := Clients[clientID]; exists {
		log.Printf("Replacing connection for: %s", clientID)
		close(oldClient.Done) 
		oldClient.Conn.Close() 
	}	

	Clients[clientID] = newClient
	ClientsMu.Unlock()

	go newClient.WritePump()

	defer func() {
		hub.unregister <- ws // Unregister on disconnect
		DisconnectClient(clientID, newClient)
	}()
	var clientFormat = regexp.MustCompile(`^STOCK_CLIENT_\d+$`)
	if clientFormat.MatchString(clientID) {
		SendOpenPositions(newClient, UserID)
		SendAllCached(clientID)
	}

	<-newClient.Done
}

func getIDFromClient(clientId string) (int, error) {
    parts := strings.Split(clientId, "_")
    
    idStr := parts[len(parts)-1]
    
    return strconv.Atoi(idStr)
}
func DisconnectClient(clientID string, caller *Client) {
    ClientsMu.Lock()
    
    // 1. Check if the client in the map is the same one calling Disconnect
    currentClient, exists := Clients[clientID]
    
    if exists && currentClient == caller {
        delete(Clients, clientID)
        log.Printf("Client disconnected and removed: %s", clientID)
    } else {
        log.Printf("Client %s removal skipped (already replaced or removed)", clientID)
    }
    ClientsMu.Unlock()

    caller.Conn.Close()
    caller.Close() 
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

func SendAllCached(clientID string) {
	client := Clients[clientID]
	client.OpenPositions.RLock()
	defer client.OpenPositions.RUnlock()
	GlobalCompanyCache.RLock()
	defer GlobalCompanyCache.RUnlock()
	GlobalOptionExpiration.RLock()
	defer GlobalOptionExpiration.RUnlock()
	GlobalCacheLimit.Lock()
	defer GlobalCacheLimit.Unlock()

	checked := make(map[string]struct{})
	for _, positions := range client.OpenPositions.Positions {
		for symbol := range positions {
			if _, ok := checked[symbol]; !ok {
				checked[symbol] = struct{}{}
				if stats, ok := GlobalCompanyCache.Stats[symbol]; ok {
					if err := send(client, stats); err != nil {
						errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
						_ = client.SafeWrite(websocket.TextMessage, errMsg)
						return
					}
				}
				if stats, ok := GlobalOptionExpiration.Stats[symbol]; ok {
					if err := send(client, stats); err != nil {
						errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
						_ = client.SafeWrite(websocket.TextMessage, errMsg)
						return
					} else {
					}
				}	
				GlobalCacheLimit.Queue = append(GlobalCacheLimit.Queue, symbol)
				if _, exists := GlobalCacheLimit.InQueue[symbol]; !exists {
					GlobalCacheLimit.InQueue[symbol] = struct{}{}
					GlobalCacheLimit.Queue = append(GlobalCacheLimit.Queue, symbol)
				}
			}
		}
	}	
}

func HandleCompanyRead(msg redis.Message) {
	var company CompanyStats
	payloadBytes := []byte(msg.Payload)
	if err := json.Unmarshal(payloadBytes, &company); err != nil {
		// If it’s not a quotes payload, skip or handle other message types here
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}
	GlobalCompanyCache.Lock()
	GlobalCompanyCache.Stats[company.Symbol] = company

	GlobalCacheLimit.Lock()
	if _, exists := GlobalCacheLimit.InQueue[company.Symbol]; !exists {
		GlobalCacheLimit.InQueue[company.Symbol] = struct{}{}
		GlobalCacheLimit.Queue = append(GlobalCacheLimit.Queue, company.Symbol)
		if len(GlobalCacheLimit.Queue) > GlobalCacheLimit.Limit {
			oldest := GlobalCacheLimit.Queue[0]
			GlobalCacheLimit.Queue = GlobalCacheLimit.Queue[1:]
			delete(GlobalCompanyCache.Stats, oldest)
		}
	}
	
	GlobalCompanyCache.Unlock()
	GlobalCacheLimit.Unlock()
	GlobalSubscriptionHub.RLock()
	defer GlobalSubscriptionHub.RUnlock()
	if clients, ok := GlobalSubscriptionHub.Topics[company.Symbol]; ok {
		for _, client := range clients {
			if err := send(client, company); err != nil {
				errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = client.SafeWrite(websocket.TextMessage, errMsg)
				return
			}
		}
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

	GlobalOptionExpiration.Lock()
	GlobalOptionExpiration.Stats[option.Symbol] = option

	GlobalCacheLimit.Lock()
	if _, exists := GlobalCacheLimit.InQueue[option.Symbol]; !exists {
		GlobalCacheLimit.InQueue[option.Symbol] = struct{}{}
		GlobalCacheLimit.Queue = append(GlobalCacheLimit.Queue, option.Symbol)
		if len(GlobalCacheLimit.Queue) > GlobalCacheLimit.Limit {
			oldest := GlobalCacheLimit.Queue[0]
			GlobalCacheLimit.Queue = GlobalCacheLimit.Queue[len(GlobalCacheLimit.Queue) - GlobalCacheLimit.Limit:]
			delete(GlobalCompanyCache.Stats, oldest)
		}
	}
	GlobalOptionExpiration.Unlock()

	GlobalCacheLimit.Unlock()
	GlobalSubscriptionHub.RLock()
	defer GlobalSubscriptionHub.RUnlock()
	if clients, ok := GlobalSubscriptionHub.Topics[option.Symbol]; ok {
		for _, client := range clients {
			if err := send(client, option); err != nil {
				errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = client.SafeWrite(websocket.TextMessage, errMsg)
				return
			}
		}
	}
}

// Receives and handles a websocket message from python client and sends data to frontend
func HandleRedisRead(msg redis.Message) {
	// start_time := time.Now()
	var quotes map[string]MixedQuote
	payloadBytes := []byte(msg.Payload)
	if err := json.Unmarshal(payloadBytes, &quotes); err != nil {
		log.Printf("Invalid quotes JSON from Python Client: %v", err)
		return
	}
	GlobalPrices.Lock()
	maps.Copy(GlobalPrices.Prices, quotes)
	GlobalPrices.Unlock()

	GlobalSubscriptionHub.RLock()
	activeTopics := maps.Clone(GlobalSubscriptionHub.Topics)
    GlobalSubscriptionHub.RUnlock()
	now := time.Now().Unix()

	type clientBatch struct {
        Stocks  []StockPriceData
        Options []OptionPriceData
    }
    batches := make(map[*Client]*clientBatch)
	for symbol, quote := range quotes {
		sepIdx := strings.IndexByte(symbol, ' ')
        var underlying string
        isOption := quote.IV != nil && sepIdx != -1
        if isOption {
            underlying = symbol[:sepIdx]
        }

		var targets []*Client
        
        // Direct Match
        if c, ok := activeTopics[symbol]; ok {
            targets = append(targets, c...)
        }
        
        // Underlying Match (Implied)
        if isOption {
            if c, ok := activeTopics[underlying]; ok {
                targets = append(targets, c...)
            }
        }

        if len(targets) == 0 {
            continue
        }
		if !isOption {
			stockQuote := StockPriceData{
				Symbol:    symbol,
				Timestamp: now,
				Mark:      quote.Mark,
				BidPrice:  quote.BidPrice,
				AskPrice:  quote.AskPrice,
				LastPrice: quote.LastPrice,
				BidSize:   *quote.BidSize,
				AskSize:   *quote.AskSize,
			}
			for _, c := range targets {
                if batches[c] == nil { batches[c] = &clientBatch{} }
                batches[c].Stocks = append(batches[c].Stocks, stockQuote)
            }
		} else {
			optionQuote := OptionPriceData{
				Symbol:    symbol,
				Timestamp: now,
				Bid:       quote.BidPrice,
				Ask:       quote.AskPrice,
				Mark:      quote.Mark,
				Last:      quote.LastPrice,
				High:      *quote.HighPrice,
				IV:        *quote.IV,
				Delta:     *quote.Delta,
				Gamma:     *quote.Gamma,
				Theta:     *quote.Theta,
				Vega:      *quote.Vega,
			}
			for _, c := range targets {
                if batches[c] == nil { batches[c] = &clientBatch{} }
                batches[c].Options = append(batches[c].Options, optionQuote)
            }
		}
	}
	for client, batch := range batches {
        payload, _ := json.Marshal(map[string]interface{}{
            "type":    "TICKER_UPDATE",
            "stocks":  batch.Stocks,
            "options": batch.Options,
        })
        client.EnqueueMessage(payload)
    }
	for _, client := range Clients {
		go ProcessWrite(time.Now(), client)
	}

	// log.Printf("HandleRedisRead took %v", time.Since(start_time))
}

func StartOptionStream(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	price := r.URL.Query().Get("price")
	day := r.URL.Query().Get("day")
	month := r.URL.Query().Get("month")
	year := r.URL.Query().Get("year")
	optionType := r.URL.Query().Get("type")
	clientID := r.URL.Query().Get("clientID")
	client := Clients[clientID]

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
	formattedSymbol := fmt.Sprintf("%-6s", strings.ToUpper(symbol))

    // 2. Format Date: YYMMDD
    formattedDate := fmt.Sprintf("%02s%02s%02s", year, month, day)

    // 3. Format Type: 'C' or 'P'
    formattedType := strings.ToUpper(string(optionType[0]))

    // 4. Format Price: Scaled by 1000, padded to 8 digits
    // Note: In a real app, convert price to float64 first to handle decimals
    formattedPrice := fmt.Sprintf("%08d", 147000) 

    // Combine into final ID
    optionID := fmt.Sprintf("%s%s%s%s", formattedSymbol, formattedDate, formattedType, formattedPrice)
	GlobalSubscriptionHub.Lock()
	if _, ok := GlobalSubscriptionHub.Topics[optionID]; !ok {
		GlobalSubscriptionHub.Topics[optionID] = make([]*Client, 0)
	}
	GlobalSubscriptionHub.Topics[optionID] = append(GlobalSubscriptionHub.Topics[optionID], client)
	GlobalSubscriptionHub.Unlock()
}

// Sends a message to the python streamer to start a subscription to a certain stock
func StartStockStream(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	clientID := r.URL.Query().Get("clientID")
	client := Clients[clientID]
	GlobalCompanyCache.Lock()
	defer GlobalCompanyCache.Unlock()
	if stats, ok := GlobalCompanyCache.Stats[symbol]; ok {
		if err := send(client, stats); err != nil {
			errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
			_ = client.SafeWrite(websocket.TextMessage, errMsg)
			return
		}
	}
	GlobalOptionExpiration.Lock()
	defer GlobalOptionExpiration.Unlock()
	if stats, ok := GlobalOptionExpiration.Stats[symbol]; ok {
		if err := send(client, stats); err != nil {
			errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
			_ = client.SafeWrite(websocket.TextMessage, errMsg)
			return
		}
		return
	}
	request := StockStreamRequest{
		Symbol: symbol,
	}

	GlobalSubscriptionHub.Lock()
	defer GlobalSubscriptionHub.Unlock()
	if _, ok := GlobalSubscriptionHub.Topics[symbol]; !ok {
		GlobalSubscriptionHub.Topics[symbol] = make([]*Client, 0)
	}
	GlobalSubscriptionHub.Topics[symbol] = append(GlobalSubscriptionHub.Topics[symbol], client)

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
func ProcessWrite(t time.Time, client *Client) {
	client.OpenPositions.RLock()
	defer client.OpenPositions.RUnlock()

	client.Balance.Lock()
	defer client.Balance.Unlock()

	GlobalPrices.RLock()
	defer GlobalPrices.RUnlock()
	if len(GlobalPrices.Prices) == 0 {
		return
	}

	userID := client.UserID
	dbSnapshot := make(map[int]BalanceData)
	for pid := range client.OpenPositions.Positions {
		var cash float64
		var balance float64

		if client.Balance.Balances[pid] == nil {
			continue
		}
		balance, cash = client.Balance.Balances[pid].Balance, client.Balance.Balances[pid].Cash

		if balance == 0.0 && cash == 0.0 {
			balance = 10000.0
			cash = 10000.0
		}

		tempPositionValue := 0.0
		for id, details := range client.OpenPositions.Positions[pid] {
			amount := details.Amount

			q, exists := GlobalPrices.Prices[id]
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

		client.Balance.Balances[pid].Balance = balance
		client.Balance.Balances[pid].Cash = cash

		message := BalanceData{
			Balance:   balance,
			Timestamp: t.Unix(),
			Cash:      cash,
			PortfolioID: pid,
		}
		dbSnapshot[pid] = message
		payload, err := json.Marshal(message)
		if err != nil {
			log.Printf("Failed to marshal balance data: %v", err)
			continue
		}
		client.EnqueueMessage(payload)
	}
	go WriteBalanceToDB(t.Unix(), userID, client, dbSnapshot)
}

func WriteBalanceToDB(now int64, userID int, client *Client, snapshot map[int]BalanceData) error {
	batch, err := GlobalDatabasePool.BalanceDB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return err
	}
	defer batch.Rollback()
	stmt, err := batch.Prepare(`
		INSERT OR IGNORE INTO Balance (timestamp, balance, cash, portfolio_id, user_id) 
		VALUES (?, ?, ?, ?, ?)
	`)

	if err != nil {
		batch.Rollback()
		log.Printf("Failed to prepare statement: %v", err)
		return err
	}
	defer stmt.Close()
	for id, data := range snapshot {
		_, err := stmt.Exec(now, data.Balance, data.Cash, id, userID)
		if err != nil {
			batch.Rollback()
			log.Printf("Failed to insert balance: %v", err)
			return err
		}
	}

	batch.Commit()
	return nil
}

func send(client *Client, v interface{}) error {
	msg, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return SendToClient(client, msg)
}
