package utils

import (
	"database/sql"
	"log"
	"time"
)

// Gets the previous day's last balance
func GetMostRecentBalance(balanceDB *sql.DB) float64 {
	var latestTs int64
	err := balanceDB.QueryRow("SELECT timestamp FROM Balance ORDER BY timestamp DESC LIMIT 1").Scan(&latestTs)
	if err == sql.ErrNoRows {
		return 10000 // Default if DB is empty
	}
	currentDate := time.Unix(latestTs, 0)

	// Get the very beginning of "Today" (00:00:00)
	beginningOfToday := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 0, 0, 0, 0, currentDate.Location())

	// The "End of Yesterday" is 1 second before today started
	endOfYesterday := beginningOfToday.Unix() - 1
	// The "Start of Yesterday" is 24 hours before today started
	startOfYesterday := beginningOfToday.AddDate(0, 0, -1).Unix()

	var balance float64
	query := `
        SELECT balance 
        FROM Balance 
        WHERE timestamp >= ? AND timestamp <= ? 
        ORDER BY timestamp DESC 
        LIMIT 1`

	err = balanceDB.QueryRow(query, startOfYesterday, endOfYesterday).Scan(&balance)

	if err == sql.ErrNoRows {
		log.Println("No balance found for the previous calendar day interval.")
		return 10000
	} else if err != nil {
		log.Printf("Error querying interval: %v", err)
		return 10000
	}

	return balance
}
