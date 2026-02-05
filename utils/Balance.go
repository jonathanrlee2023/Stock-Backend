package utils

import (
	"database/sql"
	"log"
)

// Gets the previous day's last balance
func GetMostRecentBalance(balanceDB *sql.DB) float64 {
	query := `SELECT * FROM Balance ORDER BY timestamp DESC LIMIT 1;`
	row := balanceDB.QueryRow(query)
	var timestamp int64
	var cash, balance float64
	err := row.Scan(&timestamp, &balance, &cash)
	if err == sql.ErrNoRows {
		log.Println("No rows in EODPrices; using default balance 10000")
		return 10000
	} else if err != nil {
		log.Printf("Failed to read latest EOD balance: %v", err)
		return 10000
	}

	log.Printf("Found most recent EOD balance: %.2f", balance)
	return balance
}
