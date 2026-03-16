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
	Amount float64 `json:"amount"`
}

// NewTrackerHandler is an HTTP handler for creating a new tracker entry in the tracker db.
// It expects a POST request with a JSON body containing a single tracker struct.
// If the request is invalid, it will return a 400 error with a JSON body containing an error message.
// If the write to the database fails, it will return a 500 error with a JSON body containing an error message.
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

// Removes a symbol from the tracker db
//
// # Body should contain a single tracker struct in JSON format
//
// Returns 500 if database delete fails
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

// OpenPositionHandler is an HTTP handler for opening a new position in the openpositions db.
// It expects a POST request with a JSON body containing a single position struct.
// If the request is invalid, it will return a 400 error with a JSON body containing an error message.
// If the write to the database fails, it will return a 500 error with a JSON body containing an error message.
func OpenSharesPositionHandler(openDB, balanceDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var newPosition Position
	var extAmount float64
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

	GlobalOpenPositions.Lock()

	if _, ok := GlobalOpenPositions.Positions[newPosition.ID]; ok {
		extAmount = GlobalOpenPositions.Positions[newPosition.ID].Amount
		extPrice = GlobalOpenPositions.Positions[newPosition.ID].Price
		totalCost := (extPrice * float64(extAmount)) + (newPosition.Price * float64(newPosition.Amount))
		updatedAmount := extAmount + newPosition.Amount
		updatedPrice := math.Round((totalCost/float64(updatedAmount))*100) / 100

		_, err = openDB.Exec("UPDATE OpenPositions SET price = ?, amount = ? WHERE id = ?", updatedPrice, updatedAmount, newPosition.ID)
		GlobalOpenPositions.Positions[newPosition.ID] = OpenPositionDetails{
			Price:  updatedPrice,
			Amount: updatedAmount,
		}
	} else {
		_, err = openDB.Exec("INSERT INTO OpenPositions (id, price, amount) VALUES (?, ?, ?)", newPosition.ID, newPosition.Price, newPosition.Amount)
		GlobalOpenPositions.Positions[newPosition.ID] = OpenPositionDetails{
			Price:  newPosition.Price,
			Amount: newPosition.Amount,
		}
	}
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
		return
	}

	GlobalOpenPositions.Unlock()
	GlobalBalance.Lock()

	balance, cash := GlobalBalance.Balance, GlobalBalance.Cash
	tradeCost := newPosition.Price * float64(newPosition.Amount)
	if len(newPosition.ID) > 6 {
		tradeCost *= 100
	}

	newCash := cash - tradeCost
	newBalance := balance

	GlobalBalance.Balance = newBalance
	GlobalBalance.Cash = newCash
	GlobalBalance.Unlock()

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
	var price float64
	var amount float64

	if err := json.NewDecoder(r.Body).Decode(&closePosition); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	GlobalOpenPositions.Lock()
	if _, ok := GlobalOpenPositions.Positions[closePosition.ID]; !ok {
		GlobalOpenPositions.Unlock()
		http.Error(w, "Position not found", http.StatusBadRequest)
		return
	}
	price, amount = GlobalOpenPositions.Positions[closePosition.ID].Price, GlobalOpenPositions.Positions[closePosition.ID].Amount

	if closePosition.Amount > amount {
		http.Error(w, "Insufficient shares", 400)
		return
	}

	diff := closePosition.Price - price
	multiplier := 1.0
	if len(closePosition.ID) > 6 {
		multiplier = 100.0
	}

	pl = diff * float64(closePosition.Amount) * multiplier
	pl = math.Round(pl*100) / 100
	remaining := amount - closePosition.Amount
	if remaining != 0 {
		insertData := `INSERT OR REPLACE INTO OpenPositions (id, price, amount) VALUES (?, ?, ?)`
		_, err := openDB.Exec(insertData, closePosition.ID, price, remaining)
		if err != nil {
			log.Printf("Failed to write to table: %v", err)
		}
		GlobalOpenPositions.Positions[closePosition.ID] = OpenPositionDetails{
			Price:  price,
			Amount: remaining,
		}
	} else {
		_, err := openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", closePosition.ID)
		if err != nil {
			log.Printf("Failed to delete: %v", err)
			return
		}
		delete(GlobalOpenPositions.Positions, closePosition.ID)
	}
	GlobalOpenPositions.Unlock()

	insertData := `INSERT INTO ClosePositions (id, price, amount, pl) VALUES (?, ?, ?, ?)`
	_, err := closeDB.Exec(insertData, closePosition.ID, closePosition.Price, closePosition.Amount, pl)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
	GlobalBalance.Lock()
	balance := GlobalBalance.Balance
	cash := GlobalBalance.Cash
	tradeCost := closePosition.Price * float64(closePosition.Amount)
	if len(closePosition.ID) > 6 {
		tradeCost *= 100
	}
	newCash := cash + tradeCost
	GlobalBalance.Cash = newCash
	GlobalBalance.Unlock()

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance, cash) VALUES (?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, newCash)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
}
