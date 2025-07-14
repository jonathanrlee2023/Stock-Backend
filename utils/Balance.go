package utils

import (
	"database/sql"
	"log"
)

// Gets the previous day's last balance
func GetMostRecentBalance() float64 {
	const balanceDBPath = "Balance.db"
	db, err := sql.Open("sqlite", balanceDBPath)
	if err != nil {
		log.Printf("Failed to open Balance.db: %v", err)
		return 10000
	}
	defer db.Close()

	query := `SELECT realBalance FROM EODPrices ORDER BY timestamp DESC LIMIT 1;`
	row := db.QueryRow(query)

	var realBalance float64
	err = row.Scan(&realBalance)
	if err == sql.ErrNoRows {
		log.Println("No rows in EODPrices; using default balance 10000")
		return 10000
	} else if err != nil {
		log.Printf("Failed to read latest EOD balance: %v", err)
		return 10000
	}

	log.Printf("Found most recent EOD balance: %.2f", realBalance)
	return realBalance
}
