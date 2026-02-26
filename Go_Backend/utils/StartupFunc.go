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
	openIDs := make(map[string]int64)
	var trackerIds []string
	prevBalance := GetMostRecentBalance(balanceDB)

	rows, err := openDB.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
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

	msg := OpenPositionsMessage{
		PrevBalance: prevBalance,
		OpenIDs:     openIDs,
		TrackerIDs:  trackerIds,
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println("Failed to marshal open positions:", err)
		return
	}

	err = SendToClient(clients["STOCK_CLIENT"], jsonData)
	if err != nil {
		errMsg := map[string]string{"error": err.Error()}
		msg, _ := json.Marshal(errMsg)
		_ = clients["STOCK_CLIENT"].SafeWrite(websocket.TextMessage, msg)
	}
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
				log.Println("Ran")
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
		{openDB, `CREATE TABLE IF NOT EXISTS OpenPositions (id TEXT PRIMARY KEY, price REAL, amount INTEGER);`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Balance (timestamp INTEGER PRIMARY KEY, balance REAL, cash REAL);`},
		{closeDB, `CREATE TABLE IF NOT EXISTS ClosePositions (order_number INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, price REAL, amount INTEGER, pl REAL);`},
		{trackerDB, `CREATE TABLE IF NOT EXISTS Tracker (id TEXT PRIMARY KEY);`},
	}

	for _, s := range schemas {
		if _, err := s.db.Exec(s.sql); err != nil {
			log.Printf("Schema init error: %v", err)
		}
	}
}
