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

func DataReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var fileNameData PostData

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &fileNameData)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	for _, fileName := range fileNameData.FileNames {
		db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", fileName))
		if err != nil {
			log.Printf("Query failed: %v", err)
		}
		defer db.Close()
		if len(fileName) > 5 {
			row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

			var timestamp int64
			var bid, ask, last, high, delta, gamma, theta, vega float64

			err := row.Scan(&timestamp, &bid, &ask, &last, &high, &delta, &gamma, &theta, &vega)
			if err != nil {
				log.Printf("Query failed: %v", err)
				http.Error(w, "Database query failed", http.StatusInternalServerError)
				return
			}
			fmt.Printf("Timestamp: %d | Bid: %.2f | Ask: %.2f | Last: %.2f | High: %.2f | Delta: %.4f | Gamma: %.4f | Theta: %.4f | Vega: %.4f\n",
				timestamp, bid, ask, last, high, delta, gamma, theta, vega)
		} else {
			row := db.QueryRow("SELECT * FROM prices ORDER BY timestamp DESC LIMIT 1")

			var timestamp int64
			var bid, ask, last float64
			var askSize, bidSize int64

			err := row.Scan(&timestamp, &bid, &ask, &last, &askSize, &bidSize)
			if err != nil {
				log.Printf("Query failed: %v", err)
				http.Error(w, "Database query failed", http.StatusInternalServerError)
				return
			}
			fmt.Printf("Timestamp: %d | Bid: %.2f | Ask: %.2f | Last: %.2f | Ask Size: %d | Bid Size: %d|\n",
				timestamp, bid, ask, last, askSize, bidSize)
		}
	}

	fmt.Fprintf(w, "Data has been read")
}

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

	db, err := sql.Open("sqlite", "./Tracker.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS Tracker (
			id STRING PRIMARY KEY
		);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	insertData := `INSERT OR REPLACE INTO Tracker (id) VALUES (?)`
	_, err = db.Exec(insertData, newTracker.ID)
	if err != nil {
		log.Fatalf("Failed to write to table: %v", err)
	}

	fmt.Fprintf(w, "Data has been read")
}
func RemoveTrackerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var closeTracker Tracker

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &closeTracker)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	db, err := sql.Open("sqlite", "./Tracker.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS Tracker (
			id STRING PRIMARY KEY
		);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("DELETE FROM Tracker WHERE id = ?", closeTracker.ID)
	if err != nil {
		log.Fatalf("Failed to delete from table: %v", err)
	}

	fmt.Fprintf(w, "Data has been deleted")
}

func OpenPositionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var newPosition Position

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

	openDb, err := sql.Open("sqlite", "./Open.db")
	if err != nil {
		http.Error(w, "Failed to open Open.db", http.StatusInternalServerError)
		return
	}
	defer openDb.Close()
	for i := 0; i < 3; i++ {
		_, err = openDb.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS OpenPositions (
			id STRING PRIMARY KEY,
			price FLOAT NOT NULL,
			amount INTEGER NOT NULL
		);`
	_, err = openDb.Exec(createTableSQL)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}
	rows, err := openDb.Query("SELECT * FROM OpenPositions")
	if err == sql.ErrNoRows {
		fmt.Println("No Open Positions Yet")
	} else if err != nil {
		log.Fatal("Query failed process write openDB:", err)
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
		if newPosition.ID == id {
			avg := ((float64(newPosition.Amount) * newPosition.Price) + (price * float64(amount))) / float64(amount+newPosition.Amount)
			newPosition.Amount += amount
			newPosition.Price = math.Round((avg * 100) / 100)
		}
	}

	insertData := `INSERT OR REPLACE INTO OpenPositions (id, price, amount) VALUES (?, ?, ?)`
	_, err = openDb.Exec(insertData, newPosition.ID, newPosition.Price, newPosition.Amount)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
	balanceDb, err := sql.Open("sqlite", "Balance.db")
	if err != nil {
		http.Error(w, "Failed to open Balance table", http.StatusInternalServerError)
	}
	for i := 0; i < 3; i++ {
		_, err = balanceDb.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}

	defer balanceDb.Close()

	createTableSQL = `
		CREATE TABLE IF NOT EXISTS Balance (
			timestamp INTEGER PRIMARY KEY,
			balance FLOAT NOT NULL
		);`
	_, err = balanceDb.Exec(createTableSQL)
	if err != nil {
		http.Error(w, "Failed to create Balance table", http.StatusInternalServerError)
		return
	}

	row := balanceDb.QueryRow("SELECT * FROM Balance ORDER BY timestamp DESC LIMIT 1")

	var timestamp int64
	var balance float64

	err = row.Scan(&timestamp, &balance)
	if err == sql.ErrNoRows {
		balance = 10000
	} else if err != nil {
		http.Error(w, "Failed to query Balance", http.StatusInternalServerError)
		return
	}
	balance = balance - (100 * (newPosition.Price * float64(newPosition.Amount)))

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance) VALUES (?, ?)`
	_, err = balanceDb.Exec(insertData, time.Now().Unix(), balance)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "Position Opened")
}

func ClosePositionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var closePosition Position

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &closePosition)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	closeDB, err := sql.Open("sqlite", "./Close.db")
	if err != nil {
		http.Error(w, "Failed to open Close.db", http.StatusInternalServerError)
		return
	}
	defer closeDB.Close()
	for i := 0; i < 3; i++ {
		_, err = closeDB.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS ClosePositions (
			id STRING PRIMARY KEY,
			price FLOAT NOT NULL,
			amount INTEGER NOT NULL,
			pl FLOAT NOT NULL
		);`
	_, err = closeDB.Exec(createTableSQL)
	if err != nil {
		log.Printf("Failed to create table: %v", err)
	}

	openDB, err := sql.Open("sqlite", "./Open.db")
	if err != nil {
		http.Error(w, "Failed to open Open.db", http.StatusInternalServerError)
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
		log.Fatal("Failed to enable WAL after retries:", err)
	}
	var openPrice float64
	var openAmount int

	row := openDB.QueryRow("SELECT price, amount FROM OpenPositions WHERE id = ?", closePosition.ID)
	err = row.Scan(&openPrice, &openAmount)
	if err == sql.ErrNoRows {
		http.Error(w, "No open position found with given ID", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Failed to query OpenPositions", http.StatusInternalServerError)
		return
	}

	pl := ((closePosition.Price - openPrice) * float64(closePosition.Amount)) * 100

	_, err = openDB.Exec("DELETE FROM OpenPositions WHERE id = ?", closePosition.ID)
	if err != nil {
		log.Printf("Failed to delete open position: %v", err)
	}

	insertData := `INSERT OR REPLACE INTO ClosePositions (id, price, amount, pl) VALUES (?, ?, ?, ?)`
	_, err = closeDB.Exec(insertData, closePosition.ID, closePosition.Price, closePosition.Amount, pl)
	if err != nil {
		log.Printf("Failed to write to table: %v", err)
	}
	balanceDb, err := sql.Open("sqlite", "Balance.db")
	if err != nil {
		http.Error(w, "Failed to open Balance table", http.StatusInternalServerError)
	}
	for i := 0; i < 3; i++ {
		_, err = balanceDb.Exec("PRAGMA journal_mode=WAL;")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		log.Fatal("Failed to enable WAL after retries:", err)
	}

	defer balanceDb.Close()

	row = balanceDb.QueryRow("SELECT * FROM Balance ORDER BY timestamp DESC LIMIT 1")

	var timestamp int64
	var balance float64

	err = row.Scan(&timestamp, &balance)
	if err == sql.ErrNoRows {
		http.Error(w, "No Rows", http.StatusInternalServerError)
	} else if err != nil {
		http.Error(w, "Failed to query Balance", http.StatusInternalServerError)
		return
	}
	balance = balance + (100 * (closePosition.Price * float64(closePosition.Amount)))

	insertData = `INSERT OR REPLACE INTO Balance (timestamp, balance) VALUES (?, ?)`
	_, err = balanceDb.Exec(insertData, time.Now().Unix(), balance)
	if err != nil {
		http.Error(w, "Failed to write balance", http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "Position Closed")
}
