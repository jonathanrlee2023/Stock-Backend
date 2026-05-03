package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

// Sends open position and previous balance
func SendOpenPositions(targetClient *Client, userID int) {
	openIDs := make(map[int]map[string]float64)
	var trackerIds []string
	GetPorfolioIDs(userID, targetClient)
	prevBalances := GetMostRecentBalance(userID, targetClient)

	if targetClient.OpenPositions.Positions == nil {
        targetClient.OpenPositions.Positions = make(map[int]map[string]OpenPositionDetails)
    }

	rows, err := GlobalDatabasePool.OpenDB.Query("SELECT id, price, amount, portfolio_id FROM OpenPositions WHERE user_id = ?", userID)
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer rows.Close()
	targetClient.OpenPositions.Lock()
	GlobalSubscriptionHub.Lock()
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
		if len(id) > 8 {
			parts := strings.Split(id, "_")
			if len(parts) > 0 {
				id = parts[0]
			}
		}
		if _, ok := GlobalSubscriptionHub.Topics[id]; !ok {
			GlobalSubscriptionHub.Topics[id] = make([]*Client, 0)
		}
		GlobalSubscriptionHub.Topics[id] = append(GlobalSubscriptionHub.Topics[id], targetClient)
	}
	GlobalSubscriptionHub.Unlock()

	rows, err = GlobalDatabasePool.TrackerDB.Query("SELECT * FROM Tracker")
	if err == sql.ErrNoRows {
		fmt.Println("No trackers Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write trackerDB: %v", err)
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

	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println("Failed to marshal open positions:", err)
		return
	}
	if targetClient == nil {
        log.Println("Cannot send: target client is nil")
        return
    }

	targetClient.EnqueueMessage(jsonData)
    
    ProcessWrite(time.Now(), targetClient)
}

func SendCloseHistory(targetClient *Client, userID int) {
	rows, err := GlobalDatabasePool.CloseDB.Query("SELECT * FROM ClosePositions WHERE user_id = ?", userID)
	if err == sql.ErrNoRows {
		fmt.Println("No Close Positions Yet")
		return
	} else if err != nil {
		log.Printf("Query failed process write closeDB: %v", err)
		return
	}
	defer rows.Close()

	var closePositions []ClosePosition

	for rows.Next() {
		var id string
		var price float64
		var amount float64
		var pl float64
		var portfolio_id int
		var user_id int
		var timestamp int64

		err := rows.Scan(&id, &price, &amount, &pl, &portfolio_id, &user_id, &timestamp)
		if err != nil {
			log.Println("Scan failed:", err)
			continue
		}
		closePositions = append(closePositions, ClosePosition{ID: id, Price: price, Amount: amount, PL: pl, PortfolioID: portfolio_id, UserID: user_id, Timestamp: timestamp})
	}

	closeHistory := ClosePositionHistory{
		ClosePositions: closePositions,
	}
	jsonData, err := json.Marshal(closeHistory)
	if err != nil {
		log.Println("Failed to marshal close positions:", err)
		return
	}
	if targetClient == nil {
		log.Println("Cannot send: target client is nil")
		return
	}
	targetClient.EnqueueMessage(jsonData)
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
	for range 5 {
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
		{openDB, `CREATE TABLE IF NOT EXISTS OpenPositions_New (id TEXT, price REAL, amount REAL, portfolio_id INTEGER, user_id INTEGER, timestamp INTEGER, PRIMARY KEY (id, portfolio_id, user_id));`},
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
		{closeDB, `CREATE TABLE IF NOT EXISTS ClosePositions_New (order_number INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, price REAL, amount REAL, pl REAL, portfolio_id INTEGER, user_id INTEGER, timestamp INTEGER);`},
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
		`INSERT INTO OpenPositions_New (id, price, amount, portfolio_id, user_id, timestamp)
		 SELECT id, price, amount, portfolio_id, user_id, strftime('%s','now') FROM OpenPositions;`,
		`DROP TABLE OpenPositions;`,
		`ALTER TABLE OpenPositions_New RENAME TO OpenPositions;`,
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
		`INSERT INTO ClosePositions_New (order_number, id, price, amount, pl, portfolio_id, user_id, timestamp)
		 SELECT order_number, id, price, amount, pl, portfolio_id, user_id, strftime('%s','now') FROM ClosePositions;`,
		`DROP TABLE ClosePositions;`,
		`ALTER TABLE ClosePositions_New RENAME TO ClosePositions;`,
	}

	for _, q := range closeSteps {
		if _, err := closeTx.Exec(q); err != nil {
			closeTx.Rollback()
			return fmt.Errorf("close migration failed: %v", err)
		}
	}
	
	return closeTx.Commit()
}