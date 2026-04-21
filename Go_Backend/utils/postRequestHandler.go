package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

// NewTrackerHandler is an HTTP handler for creating a new tracker entry in the tracker db.
// It expects a POST request with a JSON body containing a single tracker struct.
// If the request is invalid, it will return a 400 error with a JSON body containing an error message.
// If the write to the database fails, it will return a 500 error with a JSON body containing an error message.
func NewTrackerHandler(w http.ResponseWriter, r *http.Request) {
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

	if newTracker.ID == "" {
		http.Error(w, "Tracker ID is required and must be a string", http.StatusBadRequest)
		return
	}

	insertData := `INSERT OR REPLACE INTO Tracker (id) VALUES (?)`
	_, err = GlobalDatabasePool.TrackerDB.Exec(insertData, newTracker.ID)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
}

// Removes a symbol from the tracker db
//
// # Body should contain a single tracker struct in JSON format
//
// Returns 500 if database delete fails
func RemoveTrackerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var closeTracker Tracker

	if err := json.NewDecoder(r.Body).Decode(&closeTracker); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	_, err := GlobalDatabasePool.TrackerDB.Exec("DELETE FROM Tracker WHERE id = ?", closeTracker.ID)
	if err != nil {
		log.Printf("Error deleting tracker %s: %v", closeTracker.ID, err)
		http.Error(w, "Database delete failed", http.StatusInternalServerError)
	}
}

// OpenPositionHandler is an HTTP handler for opening a new position in the openpositions db.
// It expects a POST request with a JSON body containing a single position struct.
// If the request is invalid, it will return a 400 error with a JSON body containing an error message.
// If the write to the database fails, it will return a 500 error with a JSON body containing an error message.
func OpenSharesPositionHandler(w http.ResponseWriter, r *http.Request) {
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
	client := Clients[newPosition.ClientID]
	userID := client.UserID
	client.OpenPositions.Lock()

	if client.OpenPositions.Positions[newPosition.PortfolioID] == nil {
        client.OpenPositions.Positions[newPosition.PortfolioID] = make(map[string]OpenPositionDetails)
    }

	balance, cash := client.Balance.Balances[newPosition.PortfolioID].Balance, client.Balance.Balances[newPosition.PortfolioID].Cash


	if _, ok := client.OpenPositions.Positions[newPosition.PortfolioID][newPosition.ID]; ok {
		extAmount = client.OpenPositions.Positions[newPosition.PortfolioID][newPosition.ID].Amount
		extPrice = client.OpenPositions.Positions[newPosition.PortfolioID][newPosition.ID].Price
		totalCost := (extPrice * float64(extAmount)) + (newPosition.Price * float64(newPosition.Amount))
		updatedAmount := extAmount + newPosition.Amount
		updatedPrice := math.Round((totalCost/float64(updatedAmount))*100) / 100
		_, err = GlobalDatabasePool.OpenDB.Exec("UPDATE OpenPositions SET price = ?, amount = ? WHERE id = ? AND portfolio_id = ? AND user_id = ?", updatedPrice, updatedAmount, newPosition.ID, newPosition.PortfolioID, userID)
		client.OpenPositions.Positions[newPosition.PortfolioID][newPosition.ID] = OpenPositionDetails{
			Price:  updatedPrice,
			Amount: updatedAmount,
		}
	} else {
		_, err = GlobalDatabasePool.OpenDB.Exec("INSERT INTO OpenPositions (id, price, amount, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)", newPosition.ID, newPosition.Price, newPosition.Amount, newPosition.PortfolioID, userID)
		client.OpenPositions.Positions[newPosition.PortfolioID][newPosition.ID] = OpenPositionDetails{
			Price:  newPosition.Price,
			Amount: newPosition.Amount,
		}
	}
	client.OpenPositions.Unlock()

	if err != nil {
		log.Printf("Failed to write to table: %v", err)
		return
	}
	client.Balance.Lock()

	tradeCost := newPosition.Price * float64(newPosition.Amount)
	if len(newPosition.ID) > 6 {
		tradeCost *= 100
	}

	newCash := cash - tradeCost

	client.Balance.Balances[newPosition.PortfolioID].Cash = newCash

	insertData := `INSERT INTO Balance (timestamp, balance, cash, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)`
	_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, time.Now().Unix(), balance, newCash, newPosition.PortfolioID, userID)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
	client.Balance.Unlock()

	ProcessWrite(time.Now(), client)
}

// Handles the closing of a position
func ClosePositionHandler(w http.ResponseWriter, r *http.Request) {
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
	client := Clients[closePosition.ClientID]
	userID := client.UserID
	client.OpenPositions.Lock()
	if _, ok := client.OpenPositions.Positions[closePosition.PortfolioID][closePosition.ID]; !ok {
		http.Error(w, "Position not found", http.StatusBadRequest)
		return
	}
	price, amount = client.OpenPositions.Positions[closePosition.PortfolioID][closePosition.ID].Price, client.OpenPositions.Positions[closePosition.PortfolioID][closePosition.ID].Amount
	if closePosition.Amount > amount {
		http.Error(w, "Insufficient shares", 400)
		client.OpenPositions.Unlock()
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
		insertData := `INSERT OR REPLACE INTO OpenPositions (id, price, amount, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)`
		_, err := GlobalDatabasePool.OpenDB.Exec(insertData, closePosition.ID, price, remaining, closePosition.PortfolioID, userID)
		if err != nil {
			log.Printf("Failed to write to table: %v", err)
		}
		client.OpenPositions.Positions[closePosition.PortfolioID][closePosition.ID] = OpenPositionDetails{
			Price:  price,
			Amount: remaining,
		}
	} else {
		_, err := GlobalDatabasePool.OpenDB.Exec("DELETE FROM OpenPositions WHERE id = ? AND user_id = ? AND portfolio_id = ?", closePosition.ID, userID, closePosition.PortfolioID)
		if err != nil {
			log.Printf("Failed to delete: %v", err)
			client.OpenPositions.Unlock()
			return
		}
		delete(client.OpenPositions.Positions[closePosition.PortfolioID], closePosition.ID)
	}
	client.OpenPositions.Unlock()

	insertData := `INSERT INTO ClosePositions (id, price, amount, pl, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := GlobalDatabasePool.CloseDB.Exec(insertData, closePosition.ID, closePosition.Price, closePosition.Amount, pl, closePosition.PortfolioID, userID)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
	client.Balance.Lock()
	balance := client.Balance.Balances[closePosition.PortfolioID].Balance
	cash := client.Balance.Balances[closePosition.PortfolioID].Cash
	tradeCost := closePosition.Price * float64(closePosition.Amount)
	if len(closePosition.ID) > 6 {
		tradeCost *= 100
	}
	newCash := cash + tradeCost
	client.Balance.Balances[closePosition.PortfolioID].Cash = newCash
	client.Balance.Unlock()

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance, cash, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)`
	_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, time.Now().Unix(), balance, newCash, closePosition.PortfolioID, userID)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
	ProcessWrite(time.Now(), client)
}

func NewPortfolioHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var newPortfolio Portfolio
	if err := json.NewDecoder(r.Body).Decode(&newPortfolio); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	var balance, cash float64
	id := newPortfolio.ID
	client := Clients[newPortfolio.Positions[0].ClientID]
	userID := client.UserID
	exists := false

	client.PortfolioIDs.Lock()
	if _, ok := client.PortfolioIDs.IDs[id]; ok {
		exists = true
	} else {
		client.PortfolioIDs.IDs[id] = newPortfolio.Name
	}
	client.PortfolioIDs.Unlock()

	client.Balance.Lock()
	defer client.Balance.Unlock()
	GlobalPrices.RLock()
	defer GlobalPrices.RUnlock()
	client.OpenPositions.Lock()
	defer client.OpenPositions.Unlock()
	

	if exists {
		err := GlobalDatabasePool.BalanceDB.QueryRow("SELECT balance, cash FROM Balance WHERE portfolio_id = ? AND user_id = ? ORDER BY timestamp DESC LIMIT 1", id, userID).Scan(&balance, &cash)
		if err != nil {
			log.Printf("Failed to read from table: %v", err)
			balance = 10000.0
			cash = 10000.0
		}
	} else {
		client.OpenPositions.Positions[id] = make(map[string]OpenPositionDetails)
		balance = 10000.0
		cash = 10000.0
	}

	batch, err := GlobalDatabasePool.OpenDB.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return 
	}
	stmt, err := batch.Prepare(`
		INSERT OR IGNORE INTO OpenPositions (id, price, amount, portfolio_id, user_id) 
		VALUES (?, ?, ?, ?, ?)
	`)
	defer batch.Rollback()

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
		client.OpenPositions.Positions[position.PortfolioID][position.ID] = OpenPositionDetails{
			Price:  price,
			Amount: amount,
		}
		_, err := stmt.Exec(position.ID, price, amount, position.PortfolioID, userID)
		if err != nil {
			batch.Rollback() // Cancel everything if one insert fails
			log.Printf("Failed to insert open position: %v", err)
			return
		}
	}

	if err := batch.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}
	if client.Balance.Balances[id] == nil {
		client.Balance.Balances[id] = &Balance{}
	}
	
	client.Balance.Balances[id].Balance = balance
	client.Balance.Balances[id].Cash = cash

	
	if !exists {
		insertData := `INSERT INTO Balance (timestamp, balance, cash, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)`
		_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, time.Now().Unix(), 10000.0, cash, id, userID)
		if err != nil {
			http.Error(w, "Failed to write balance", http.StatusInternalServerError)
		}

		insertData = `INSERT INTO Portfolios (portfolio_id, name, user_id) VALUES (?, ?, ?)`
		_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, id, newPortfolio.Name, userID)
		if err != nil {
			http.Error(w, "Failed to write balance", http.StatusInternalServerError)
		}
	} else {
		insertData := `INSERT INTO Balance (timestamp, balance, cash, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)`
		_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, time.Now().Unix(), balance, cash, id, userID)
		if err != nil {
			http.Error(w, "Failed to write balance", http.StatusInternalServerError)
		}
	}
}

func DeletePortfolioHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Only DELETE method is allowed", http.StatusMethodNotAllowed)
		return
	}

	id, err := strconv.Atoi(r.URL.Query().Get("id"))
	clientID := r.URL.Query().Get("client_id")
	if err != nil {
		http.Error(w, "Unable to parse id", http.StatusBadRequest)
		return
	}

	client := Clients[clientID]
	userID := client.UserID
	
	client.PortfolioIDs.Lock()
	defer client.PortfolioIDs.Unlock()
	client.OpenPositions.Lock()
	defer client.OpenPositions.Unlock()
	client.Balance.Lock()
	defer client.Balance.Unlock()
	if _, ok := client.PortfolioIDs.IDs[id]; !ok {
		http.Error(w, "Portfolio not found", http.StatusNotFound)
		return
	}

	_, err = GlobalDatabasePool.BalanceDB.Exec("DELETE FROM Balance WHERE portfolio_id = ? AND user_id = ?", id, userID)
	if err != nil {
		http.Error(w, "Failed to delete balance", http.StatusInternalServerError)
		return
	}
	_, err = GlobalDatabasePool.OpenDB.Exec("DELETE FROM OpenPositions WHERE portfolio_id = ? AND user_id = ?", id, userID)
	if err != nil {
		http.Error(w, "Failed to delete open positions", http.StatusInternalServerError)
		return
	}
	_, err = GlobalDatabasePool.BalanceDB.Exec("DELETE FROM Portfolios WHERE portfolio_id = ? AND user_id = ?", id, userID)
	if err != nil {
		http.Error(w, "Failed to delete portfolio", http.StatusInternalServerError)
		return
	}
	
	delete(client.PortfolioIDs.IDs, id)
	delete(client.OpenPositions.Positions, id)
	delete(client.Balance.Balances, id)
}


func StartBacktest(rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var backtestRequest BacktestRequest
	if err := json.NewDecoder(r.Body).Decode(&backtestRequest); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	msg, err := json.Marshal(backtestRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = SendToRedis(msg, context.Background(), rdb, "Request_Channel")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func LoginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var creds Credentials
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	var password string
	var userID int
	err := GlobalDatabasePool.BalanceDB.QueryRow("SELECT user_id, password FROM Users WHERE username = ?", creds.Username).Scan(&userID, &password)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "User not found", http.StatusNotFound)
		} else {
			http.Error(w, "Database error", http.StatusInternalServerError)
		}
		return
	}
	if !CheckPasswordHash(creds.Password, password) {
        http.Error(w, "Invalid password", http.StatusUnauthorized)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)

    response := map[string]interface{}{
        "user_id": userID,
        "message": "Login successful",
    }

    json.NewEncoder(w).Encode(response)
}

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var creds Credentials
	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	hashedPassword, err := HashPassword(creds.Password)
	if err != nil {
		http.Error(w, "Error hashing password", http.StatusInternalServerError)
		return
	}
	_, err = GlobalDatabasePool.BalanceDB.Exec("INSERT INTO Users (username, password) VALUES (?, ?)", creds.Username, hashedPassword)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	var userID int
	err = GlobalDatabasePool.BalanceDB.QueryRow("SELECT user_id FROM Users WHERE username = ?", creds.Username).Scan(&userID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	_, err = GlobalDatabasePool.BalanceDB.Exec("INSERT INTO Portfolios (portfolio_id, name, user_id) VALUES (?, ?, ?)", 1, "Main", userID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	_, err = GlobalDatabasePool.BalanceDB.Exec("INSERT INTO Balance (timestamp, balance, cash, portfolio_id, user_id) VALUES (?, ?, ?, ?, ?)", time.Now().Unix(), 10000, 10000, 1, userID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)

    response := map[string]interface{}{
        "user_id": userID,
        "message": "Login successful",
    }

    json.NewEncoder(w).Encode(response)
}

func HashPassword(password string) (string, error) {
	cost := bcrypt.DefaultCost
    if os.Getenv("GO_ENV") == "test" {
        cost = bcrypt.MinCost
    }
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	return string(bytes), err
}

func CheckPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}