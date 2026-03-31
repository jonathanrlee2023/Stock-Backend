package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// Sends open position and previous balance
func SendOpenPositions(balanceDB, openDB, priceDB, trackerDB *sql.DB, clients map[string]*Client) {
	openIDs := make(map[int]map[string]float64)
	var trackerIds []string
	GetPorfolioIDs(balanceDB)
	prevBalances := GetMostRecentBalance(balanceDB)

	rows, err := openDB.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer rows.Close()
	GlobalOpenPositions.Lock()
	defer GlobalOpenPositions.Unlock()

	for rows.Next() {
		var id string
		var price float64
		var amount float64
		var portfolio_id int

		err := rows.Scan(&id, &price, &amount, &portfolio_id)
		if err != nil {
			log.Println("Scan failed:", err)
			continue
		}
		if GlobalOpenPositions.Positions[portfolio_id] == nil {
			GlobalOpenPositions.Positions[portfolio_id] = make(map[string]OpenPositionDetails)
		}
		GlobalOpenPositions.Positions[portfolio_id][id] = OpenPositionDetails{Price: price, Amount: amount}

		if openIDs[portfolio_id] == nil {
			openIDs[portfolio_id] = make(map[string]float64)
		}
		openIDs[portfolio_id][id] = amount
	}

	rows, err = trackerDB.Query("SELECT * FROM Tracker")
	if err == sql.ErrNoRows {
		fmt.Println("No trackers Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id string

		err := rows.Scan(&id)
		if err != nil {
			log.Println("Scan failed:", err)
			continue
		}
		trackerIds = append(trackerIds, id)
	}
	fmt.Println(prevBalances)
	msg := OpenPositionsMessage{
		PrevBalance: prevBalances,
		OpenIDs:     openIDs,
		TrackerIDs:  trackerIds,
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println("Failed to marshal open positions:", err)
		return
	}
	client := clients["STOCK_CLIENT"]
	err = SendToClient(client, jsonData)
	if err != nil {
		errMsg := map[string]string{"error": err.Error()}
		msg, _ := json.Marshal(errMsg)
		_ = clients["STOCK_CLIENT"].SafeWrite(websocket.TextMessage, msg)
	}
	ProcessWrite(time.Now(), client, balanceDB, openDB)
}

// Send symbols from Tracker.db to python client
func SendTrackerSymbols(trackerDB, openDB *sql.DB) ([]OptionStreamRequest, []StockStreamRequest) {
	var optionSymbols []OptionStreamRequest
	var stockSymbols []StockStreamRequest
	var toDelete []string

	rows, err := trackerDB.Query("SELECT id FROM Tracker")
	if err != nil {
		log.Println("No Tables in Tracker")
		return nil, nil
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var option OptionStreamRequest

		err := rows.Scan(&id)
		if err != nil {
			log.Println("No rows to read from")
			return nil, nil
		}
		if len(id) > 6 {
			option, err = ParseOptionString(id)
			if err != nil {
				log.Println("Failed to parse")
			}
			expDate, err := ParseExpirationDate(option)
			if err != nil {
				log.Println("Failed to parse")
			}
			now := time.Now().UTC()
			if expDate.Before(now) {
				toDelete = append(toDelete, id)
				continue
			}
			optionSymbols = append(optionSymbols, option)
		} else {
			stock := StockStreamRequest{Symbol: id}
			stockSymbols = append(stockSymbols, stock)
		}
	}
	for _, id := range toDelete {
		log.Printf("Cleaning up expired option: %s", id)
		trackerDB.Exec("DELETE FROM Tracker WHERE id = ?", id)
		openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", id)
	}
	return optionSymbols, stockSymbols
}

func InitDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// Configure for high-concurrency and reliability
	db.SetMaxOpenConns(10) // Allow multiple readers
	db.SetMaxIdleConns(5)  // Keep a few connections ready
	db.SetConnMaxIdleTime(30 * time.Second)

	var lastErr error
	for i := 0; i < 5; i++ {
		_, lastErr = db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;")
		if lastErr == nil {
			return db, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("WAL/Timeout failed on %s: %v", path, lastErr)
}

func InitSchemas(openDB, balanceDB, closeDB, trackerDB *sql.DB) {
	schemas := []struct {
		db  *sql.DB
		sql string
	}{
		{openDB, `CREATE TABLE IF NOT EXISTS OpenPositions (id TEXT, price REAL, amount REAL, portfolio_id INTEGER, PRIMARY KEY (id, portfolio_id));`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Balance (timestamp INTEGER, balance REAL, cash REAL, portfolio_id INTEGER, PRIMARY KEY (timestamp, portfolio_id));`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Portfolios (portfolio_id INTEGER PRIMARY KEY, name TEXT);`},
		{closeDB, `CREATE TABLE IF NOT EXISTS ClosePositions (order_number INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, price REAL, amount REAL, pl REAL, portfolio_id INTEGER);`},
		{trackerDB, `CREATE TABLE IF NOT EXISTS Tracker (id TEXT PRIMARY KEY);`},
	}

	for _, s := range schemas {
		if _, err := s.db.Exec(s.sql); err != nil {
			log.Printf("Schema init error: %v", err)
		}
	}
}
