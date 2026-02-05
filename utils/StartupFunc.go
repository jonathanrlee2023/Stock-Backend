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
