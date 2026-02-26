package utils

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	_ "modernc.org/sqlite"
)

type PostData struct {
	FileNames []string `json:"filenames"`
}

type Tracker struct {
	ID string `json:"id"`
}
type Position struct {
	ID     string  `json:"id"`
	Price  float64 `json:"price"`
	Amount int64   `json:"amount"`
}

// Handles call from frontend to add a symbol to the tracker db
func NewTrackerHandler(trackerDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var newTracker Tracker

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &newTracker)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	insertData := `INSERT OR REPLACE INTO Tracker (id) VALUES (?)`
	_, err = trackerDB.Exec(insertData, newTracker.ID)
	if err != nil {
		log.Fatalf("Failed to write to table: %v", err)
	}
}

// Handles the removal of a symbol from the tracker db
func RemoveTrackerHandler(trackerDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var closeTracker Tracker

	if err := json.NewDecoder(r.Body).Decode(&closeTracker); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	_, err := trackerDB.Exec("DELETE FROM Tracker WHERE id = ?", closeTracker.ID)
	if err != nil {
		log.Printf("Error deleting tracker %s: %v", closeTracker.ID, err)
		http.Error(w, "Database delete failed", http.StatusInternalServerError)
	}
}

// Handles the creation of a position
func OpenPositionHandler(openDB, balanceDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var newPosition Position
	var extAmount int64
	var extPrice float64

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &newPosition)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err = openDB.QueryRow("SELECT price, amount FROM OpenPositions WHERE id = ?", newPosition.ID).Scan(&extPrice, &extAmount)
	updatedAmount := newPosition.Amount
	updatedPrice := newPosition.Price
	if err == nil {
		// FOUND: Average in the new shares
		totalCost := (extPrice * float64(extAmount)) + (newPosition.Price * float64(newPosition.Amount))
		updatedAmount = extAmount + newPosition.Amount
		updatedPrice = math.Round((totalCost/float64(updatedAmount))*100) / 100

		_, err = openDB.Exec("UPDATE OpenPositions SET price = ?, amount = ? WHERE id = ?", updatedPrice, updatedAmount, newPosition.ID)
	} else if err == sql.ErrNoRows {
		// NOT FOUND: Insert new
		_, err = openDB.Exec("INSERT INTO OpenPositions (id, price, amount) VALUES (?, ?, ?)", newPosition.ID, newPosition.Price, newPosition.Amount)
	}

	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}

	row := balanceDB.QueryRow(`SELECT timestamp, balance, cash FROM Balance ORDER BY timestamp DESC LIMIT 1`)

	var timestamp int64
	var balance float64
	var cash float64

	err = row.Scan(&timestamp, &balance, &cash)
	tradeCost := newPosition.Price * float64(newPosition.Amount)
	if len(newPosition.ID) > 6 {
		tradeCost *= 100 // Options contract multiplier
	}

	newCash := cash - tradeCost
	newBalance := balance

	insertData := `INSERT INTO Balance (timestamp, balance, cash) VALUES (?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), newBalance, newCash)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
}

// Handles the closing of a position
func ClosePositionHandler(openDB, closeDB, balanceDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var pl float64
	var closePosition Position
	var id string
	var price float64
	var amount int64

	if err := json.NewDecoder(r.Body).Decode(&closePosition); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS ClosePositions (
			order_number INTEGER PRIMARY KEY AUTOINCREMENT,
			id STRING NOT NULL,
			price REAL NOT NULL,
			amount INTEGER NOT NULL,
			pl REAL NOT NULL
		);`
	_, err := closeDB.Exec(createTableSQL)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}

	querySelect := "SELECT * FROM OpenPositions WHERE id = ?"

	err = openDB.QueryRow(querySelect, closePosition.ID).Scan(&id, &price, &amount)
	if err == sql.ErrNoRows {
		log.Println("No more positions to close")
	} else if err != nil {
		log.Printf("Failed to select FIFO position from %s: %v", closePosition.ID, err)
	}

	if closePosition.Amount > amount {
		http.Error(w, "Insufficient shares", 400)
		return
	}

	diff := closePosition.Price - price
	multiplier := 1.0
	if len(closePosition.ID) > 6 {
		multiplier = 100.0
	}
	// P/L = (SellPrice - BuyPrice) * Quantity * Multiplier
	pl = diff * float64(closePosition.Amount) * multiplier
	pl = math.Round(pl*100) / 100 // Clean cents
	remaining := amount - closePosition.Amount

	if remaining != 0 {
		insertData := `INSERT OR REPLACE INTO OpenPositions (id, price, amount) VALUES (?, ?, ?)`
		_, err = openDB.Exec(insertData, closePosition.ID, price, remaining)
		if err != nil {
			log.Printf("Failed to write to table: %v", err)
		}
	} else {
		_, err = openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", closePosition.ID)
		if err != nil {
			log.Printf("Failed to delete: %v", err)
			return
		}
	}
	insertData := `INSERT INTO ClosePositions (id, price, amount, pl) VALUES (?, ?, ?, ?)`
	_, err = closeDB.Exec(insertData, closePosition.ID, closePosition.Price, closePosition.Amount, pl)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}

	row := balanceDB.QueryRow(`SELECT timestamp, balance, cash FROM Balance ORDER BY timestamp DESC LIMIT 1`)

	var timestamp int64
	var balance float64
	var cash float64

	err = row.Scan(&timestamp, &balance, &cash)
	if err == sql.ErrNoRows {
		http.Error(w, "No Rows", http.StatusInternalServerError)
	} else if err != nil {
		http.Error(w, "Failed to query Balance", http.StatusInternalServerError)
		return
	}
	tradeCost := closePosition.Price * float64(closePosition.Amount)
	if len(closePosition.ID) > 6 {
		tradeCost *= 100 // Options contract multiplier
	}
	newCash := cash + tradeCost

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance, cash) VALUES (?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, newCash)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
}
