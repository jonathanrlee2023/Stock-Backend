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
func SendOpenPositions(balanceDB, openDB, priceDB, trackerDB *sql.DB, targetClient *Client, userID int) {
	openIDs := make(map[int]map[string]float64)
	var trackerIds []string
	GetPorfolioIDs(balanceDB, userID, targetClient)
	prevBalances := GetMostRecentBalance(balanceDB, userID)

	if targetClient.OpenPositions.Positions == nil {
        targetClient.OpenPositions.Positions = make(map[int]map[string]OpenPositionDetails)
    }

	rows, err := openDB.Query("SELECT id, price, amount, portfolio_id FROM OpenPositions WHERE user_id = ?", userID)
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer rows.Close()
	targetClient.OpenPositions.Lock()

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
		if targetClient.OpenPositions.Positions[portfolio_id] == nil {
			targetClient.OpenPositions.Positions[portfolio_id] = make(map[string]OpenPositionDetails)
		}
		targetClient.OpenPositions.Positions[portfolio_id][id] = OpenPositionDetails{Price: price, Amount: amount}

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
	targetClient.PortfolioIDs.Lock()
	msg := OpenPositionsMessage{
		PrevBalance: prevBalances,
		OpenIDs:     openIDs,
		TrackerIDs:  trackerIds,
		PortfolioNames: targetClient.PortfolioIDs.IDs,
	}
	targetClient.PortfolioIDs.Unlock()
	targetClient.OpenPositions.Unlock()

	fmt.Printf("Sending open positions data to client: %+v\n", msg)
	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println("Failed to marshal open positions:", err)
		return
	}
	if targetClient == nil {
        log.Println("Cannot send: target client is nil")
        return
    }

    err = SendToClient(targetClient, jsonData) 
    if err != nil {
        _ = targetClient.SafeWrite(websocket.TextMessage, jsonData) 
    }
    
    ProcessWrite(time.Now(), targetClient, balanceDB, openDB)
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
		{openDB, `CREATE TABLE IF NOT EXISTS OpenPositions_new (id TEXT, price REAL, amount REAL, portfolio_id INTEGER, user_id INTEGER, PRIMARY KEY (id, portfolio_id, user_id));`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Balance (timestamp INTEGER, balance REAL, cash REAL, portfolio_id INTEGER, user_id INTEGER, PRIMARY KEY (timestamp, portfolio_id, user_id));`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Users (
			user_id INTEGER PRIMARY KEY AUTOINCREMENT, 
			username TEXT UNIQUE, 
			password TEXT, 
			active INTEGER DEFAULT 1
		);`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Portfolios (
			portfolio_id INTEGER, 
			user_id INTEGER, 
			name TEXT, 
			PRIMARY KEY (portfolio_id, user_id)
		);`},
		{closeDB, `CREATE TABLE IF NOT EXISTS ClosePositions_new (order_number INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, price REAL, amount REAL, pl REAL, portfolio_id INTEGER, user_id INTEGER);`},
		{trackerDB, `CREATE TABLE IF NOT EXISTS Tracker (id TEXT PRIMARY KEY);`},
	}

	for _, s := range schemas {
		if _, err := s.db.Exec(s.sql); err != nil {
			log.Printf("Schema init error: %v", err)
		}
	}
}

func DeleteTable(balanceDB *sql.DB, tableName string) {
	_, err := balanceDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	if err != nil {
		log.Printf("Failed to delete table %s: %v", tableName, err)
	}
}

func MigrateDB(openDB, closeDB *sql.DB) error {
	// 1. MIGRATE OPEN POSITIONS (openDB)
	openTx, err := openDB.Begin()
	if err != nil {
		return err
	}

	openSteps := []string{
		`INSERT INTO OpenPositions_new (id, price, amount, portfolio_id, user_id)
		 SELECT id, price, amount, portfolio_id, 1 FROM OpenPositions;`,
		`DROP TABLE OpenPositions;`,
		`ALTER TABLE OpenPositions_new RENAME TO OpenPositions;`,
	}

	for _, q := range openSteps {
		if _, err := openTx.Exec(q); err != nil {
			openTx.Rollback()
			return fmt.Errorf("open migration failed: %v", err)
		}
	}
	if err := openTx.Commit(); err != nil {
		return err
	}

	// 2. MIGRATE CLOSE POSITIONS (closeDB)
	closeTx, err := closeDB.Begin()
	if err != nil {
		return err
	}

	closeSteps := []string{
		`INSERT INTO ClosePositions_new (order_number, id, price, amount, pl, portfolio_id, user_id)
		 SELECT order_number, id, price, amount, pl, portfolio_id, 1 FROM ClosePositions;`,
		`DROP TABLE ClosePositions;`,
		`ALTER TABLE ClosePositions_new RENAME TO ClosePositions;`,
	}

	for _, q := range closeSteps {
		if _, err := closeTx.Exec(q); err != nil {
			closeTx.Rollback()
			return fmt.Errorf("close migration failed: %v", err)
		}
	}
	
	return closeTx.Commit()
}