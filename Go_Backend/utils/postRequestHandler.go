package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	_ "modernc.org/sqlite"
)

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

	if GlobalOpenPositions.Positions[newPosition.PortfolioID] == nil {
        GlobalOpenPositions.Positions[newPosition.PortfolioID] = make(map[string]OpenPositionDetails)
    }

	balance, cash := GlobalBalance.Balances[newPosition.PortfolioID].Balance, GlobalBalance.Balances[newPosition.PortfolioID].Cash


	if _, ok := GlobalOpenPositions.Positions[newPosition.PortfolioID][newPosition.ID]; ok {
		extAmount = GlobalOpenPositions.Positions[newPosition.PortfolioID][newPosition.ID].Amount
		extPrice = GlobalOpenPositions.Positions[newPosition.PortfolioID][newPosition.ID].Price
		totalCost := (extPrice * float64(extAmount)) + (newPosition.Price * float64(newPosition.Amount))
		updatedAmount := extAmount + newPosition.Amount
		updatedPrice := math.Round((totalCost/float64(updatedAmount))*100) / 100
		fmt.Println(extAmount, extPrice, totalCost, updatedAmount, updatedPrice)
		_, err = openDB.Exec("UPDATE OpenPositions SET price = ?, amount = ? WHERE id = ? AND portfolio_id = ?", updatedPrice, updatedAmount, newPosition.ID, newPosition.PortfolioID)
		GlobalOpenPositions.Positions[newPosition.PortfolioID][newPosition.ID] = OpenPositionDetails{
			Price:  updatedPrice,
			Amount: updatedAmount,
		}
	} else {
		_, err = openDB.Exec("INSERT INTO OpenPositions (id, price, amount, portfolio_id) VALUES (?, ?, ?, ?)", newPosition.ID, newPosition.Price, newPosition.Amount, newPosition.PortfolioID)
		GlobalOpenPositions.Positions[newPosition.PortfolioID][newPosition.ID] = OpenPositionDetails{
			Price:  newPosition.Price,
			Amount: newPosition.Amount,
		}
	}
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
		return
	}
	GlobalBalance.Lock()

	tradeCost := newPosition.Price * float64(newPosition.Amount)
	if len(newPosition.ID) > 6 {
		tradeCost *= 100
	}

	newCash := cash - tradeCost

	GlobalBalance.Balances[newPosition.PortfolioID].Cash = newCash

	insertData := `INSERT INTO Balance (timestamp, balance, cash, portfolio_id) VALUES (?, ?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, newCash, newPosition.PortfolioID)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
	GlobalOpenPositions.Unlock()
	GlobalBalance.Unlock()

	ProcessWrite(time.Now(), Clients["STOCK_CLIENT"], balanceDB, openDB)
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
	defer GlobalOpenPositions.Unlock()
	if _, ok := GlobalOpenPositions.Positions[closePosition.PortfolioID][closePosition.ID]; !ok {
		http.Error(w, "Position not found", http.StatusBadRequest)
		return
	}
	price, amount = GlobalOpenPositions.Positions[closePosition.PortfolioID][closePosition.ID].Price, GlobalOpenPositions.Positions[closePosition.PortfolioID][closePosition.ID].Amount
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
		insertData := `INSERT OR REPLACE INTO OpenPositions (id, price, amount, portfolio_id) VALUES (?, ?, ?, ?)`
		_, err := openDB.Exec(insertData, closePosition.ID, price, remaining, closePosition.PortfolioID)
		if err != nil {
			log.Printf("Failed to write to table: %v", err)
		}
		GlobalOpenPositions.Positions[closePosition.PortfolioID][closePosition.ID] = OpenPositionDetails{
			Price:  price,
			Amount: remaining,
		}
	} else {
		_, err := openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", closePosition.ID)
		if err != nil {
			log.Printf("Failed to delete: %v", err)
			return
		}
		delete(GlobalOpenPositions.Positions[closePosition.PortfolioID], closePosition.ID)
	}

	insertData := `INSERT INTO ClosePositions (id, price, amount, pl) VALUES (?, ?, ?, ?)`
	_, err := closeDB.Exec(insertData, closePosition.ID, closePosition.Price, closePosition.Amount, pl)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
	GlobalBalance.Lock()
	defer GlobalBalance.Unlock()
	balance := GlobalBalance.Balances[closePosition.PortfolioID].Balance
	cash := GlobalBalance.Balances[closePosition.PortfolioID].Cash
	tradeCost := closePosition.Price * float64(closePosition.Amount)
	if len(closePosition.ID) > 6 {
		tradeCost *= 100
	}
	newCash := cash + tradeCost
	GlobalBalance.Balances[closePosition.PortfolioID].Cash = newCash

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance, cash, portfolio_id) VALUES (?, ?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), balance, newCash, closePosition.PortfolioID)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}

	ProcessWrite(time.Now(), Clients["STOCK_CLIENT"], balanceDB, openDB)
}

func NewPortfolioHandler(balanceDB, openDB *sql.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var newPortfolio Portfolio
	if err := json.NewDecoder(r.Body).Decode(&newPortfolio); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	GlobalBalance.Lock()
	defer GlobalBalance.Unlock()
	GlobalPrices.RLock()
	defer GlobalPrices.RUnlock()
	GlobalOpenPositions.Lock()
	defer GlobalOpenPositions.Unlock()
	GlobalPortfolio_IDs.Lock()
	GlobalPortfolio_IDs.IDs = append(GlobalPortfolio_IDs.IDs, newPortfolio.ID)
	GlobalPortfolio_IDs.Unlock()
	GlobalOpenPositions.Positions[newPortfolio.ID] = make(map[string]OpenPositionDetails)

	cash := 10000.0

	batch, err := openDB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return 
	}
	stmt, err := batch.Prepare(`
		INSERT OR IGNORE INTO OpenPositions (id, price, amount, portfolio_id) 
		VALUES (?, ?, ?, ?)
	`)

	if err != nil {
		batch.Rollback()
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	for _, position := range newPortfolio.Positions {
		price := GlobalPrices.Prices[position.ID].Mark
		amount := position.Amount
		tradeCost := price * amount
		cash -= tradeCost
		GlobalOpenPositions.Positions[position.PortfolioID][position.ID] = OpenPositionDetails{
			Price:  price,
			Amount: amount,
		}
		_, err := stmt.Exec(position.ID, price, amount, position.PortfolioID)
		if err != nil {
			batch.Rollback() // Cancel everything if one insert fails
			log.Printf("Failed to insert balance: %v", err)
			return
		}
	}

	if err := batch.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	if GlobalBalance.Balances[newPortfolio.ID] == nil {
		GlobalBalance.Balances[newPortfolio.ID] = &Balance{
			Balance: 10000.0,
			Cash:    cash,
		}
	}
	
	insertData := `INSERT INTO Balance (timestamp, balance, cash, portfolio_id) VALUES (?, ?, ?, ?)`
	_, err = balanceDB.Exec(insertData, time.Now().Unix(), 10000.0, cash, newPortfolio.ID)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}

	insertData = `INSERT INTO Portfolios (portfolio_id, name) VALUES (?, ?)`
	_, err = balanceDB.Exec(insertData, newPortfolio.ID, newPortfolio.Name)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
}