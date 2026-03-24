package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// Gets the previous day's last balance
func GetMostRecentBalance(balanceDB *sql.DB) float64 {
	var latestTs int64
	var todayBalance float64
	var todayCash float64
	err := balanceDB.QueryRow("SELECT timestamp, balance, cash FROM Balance ORDER BY timestamp DESC LIMIT 1").Scan(&latestTs, &todayBalance, &todayCash)
	if err == sql.ErrNoRows {
		GlobalBalance.Lock()
		GlobalBalance.Balance = 10000
		GlobalBalance.Cash = 10000
		GlobalBalance.Unlock()
		return 10000 // Default if DB is empty
	}
	currentDate := time.Unix(latestTs, 0)
	fmt.Println("Current Date: ", currentDate)
	beginningOfToday := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 0, 0, 0, 0, currentDate.Location())

	endOfYesterday := beginningOfToday.Unix() - 1
	startOfYesterday := beginningOfToday.AddDate(0, 0, -7).Unix()

	var balance float64
	var cash float64
	query := `
        SELECT balance
        FROM Balance 
        WHERE timestamp >= ? AND timestamp <= ? 
        ORDER BY timestamp DESC 
        LIMIT 1`

	err = balanceDB.QueryRow(query, startOfYesterday, endOfYesterday).Scan(&balance)

	if err == sql.ErrNoRows {
		log.Println("No balance found for the previous calendar day interval... Retrying")
		for i := 0; i < 5; i++ {
			startOfYesterday = startOfYesterday - 86400

			query := `
				SELECT balance
				FROM Balance 
				WHERE timestamp >= ? AND timestamp <= ? 
				ORDER BY timestamp DESC 
				LIMIT 1`

			err = balanceDB.QueryRow(query, startOfYesterday, endOfYesterday).Scan(&balance)

			if err == nil {
				return balance
			}
		}
		log.Println("Could not find balance for the previous calendar day interval after 5 attempts... Defaulting to 10000")
		return 10000
	} else if err != nil {
		log.Printf("Error querying interval: %v", err)
		return 10000
	}

	if balance == 0.0 && cash == 0.0 {
		balance = 10000.0
		cash = 10000.0
		fmt.Println("Resetting Balance and Cash")
	}

	GlobalBalance.Lock()
	GlobalBalance.Balance = todayBalance
	GlobalBalance.Cash = todayCash
	GlobalBalance.Unlock()

	message := BalanceData{
		Balance:   todayBalance,
		Timestamp: time.Now().Unix(),
		Cash:      todayCash,
	}
	client := Clients["STOCK_CLIENT"]
	if err := send(client, message); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
	}

	return balance
}
