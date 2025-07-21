package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

// Sends open position and previous balance
func SendOpenPositions(clients map[string]*Client) {
	openIDs := make(map[string]int64)
	var trackerIds []string
	prevBalance := GetMostRecentBalance()
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

	trackerDB, err := sql.Open("sqlite", "Tracker.db")
	if err != nil {
		log.Printf("Query failed process write openDB: %v", err)
		return
	}
	defer trackerDB.Close()
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
		CREATE TABLE IF NOT EXISTS Tracker (
			id STRING PRIMARY KEY,
			price REAL NOT NULL,
			amount INTEGER NOT NULL
		);`
	_, err = trackerDB.Exec(createTableSQL)
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
func SendTrackerSymbols() []string {
	var symbols []string
	fmt.Println(os.Getwd())
	trackerDB, err := sql.Open("sqlite", "Tracker.db")
	if err != nil {
		log.Println("No Tracker")
		return nil
	}
	for i := 0; i < 3; i++ {
		_, err = trackerDB.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Printf("Failed to enable WAL after retries: %v", err)
	}
	defer trackerDB.Close()

	openDB, err := sql.Open("sqlite", "Open.db")
	if err != nil {
		log.Println("No Tracker")
		return nil
	}
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
	defer openDB.Close()

	rows, err := trackerDB.Query("SELECT id FROM Tracker")
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
				log.Println("Ran")
				_, err := trackerDB.Exec("DELETE FROM Tracker WHERE id = ?", id)
				if err != nil {
					log.Println("Failed to delete expired tracker:", err)
				} else {
					log.Printf("Deleted expired tracker: %s (expired on %s)", id, expDate.Format(time.RFC3339))
				}
				_, err = openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", id)
				if err != nil {
					log.Println("Failed to delete expired symbol:", err)
				} else {
					log.Printf("Deleted expired symbol: %s (expired on %s)", id, expDate.Format(time.RFC3339))
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
