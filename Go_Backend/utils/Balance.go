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
func GetMostRecentBalance(balanceDB *sql.DB) map[int]float64 {
	var latestTs int64
	var todayBalance float64
	var todayCash float64

	recentBalances := make(map[int]float64)

	GlobalPortfolio_IDs.Lock()
	GlobalBalance.Lock()
	defer GlobalPortfolio_IDs.Unlock()
	defer GlobalBalance.Unlock()
	GlobalPortfolio_IDs.IDs = []int{}
	found := false

	rows, err := balanceDB.Query("SELECT DISTINCT portfolio_id FROM Balance ORDER BY timestamp DESC")
	if err != nil {
		log.Println(err)
		return nil
	}
	defer rows.Close()
	fmt.Println("Reached")
	for rows.Next() {
		found = true
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Println(err)
			return nil
		}
		GlobalPortfolio_IDs.IDs = append(GlobalPortfolio_IDs.IDs, id)
		if _, b := GlobalBalance.Balances[id]; !b {
			GlobalBalance.Balances[id] = &Balance{} 
		}
	}
	if !found {
		GlobalBalance.Balances[1] = &Balance{}
		GlobalBalance.Balances[1].Balance = 10000
		GlobalBalance.Balances[1].Cash = 10000
		recentBalances[1] = 10000
		GlobalPortfolio_IDs.IDs = append(GlobalPortfolio_IDs.IDs, 1)
		fmt.Println(recentBalances)
		return recentBalances
	}

	if err = rows.Err(); err != nil {
		log.Println(err)
		return nil
	}
	for _, id := range GlobalPortfolio_IDs.IDs {
		err = balanceDB.QueryRow("SELECT timestamp, balance, cash FROM Balance WHERE portfolio_id = ? ORDER BY timestamp DESC LIMIT 1", id).Scan(&latestTs, &todayBalance, &todayCash)
		if err == sql.ErrNoRows {
			GlobalBalance.Balances[id] = &Balance{}
			GlobalBalance.Balances[id].Balance = 10000
			GlobalBalance.Balances[id].Cash = 10000
		}
		currentDate := time.Unix(latestTs, 0)
		beginningOfToday := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 0, 0, 0, 0, currentDate.Location())

		endOfYesterday := beginningOfToday.Unix() - 1
		startOfYesterday := beginningOfToday.AddDate(0, 0, -7).Unix()

		var balance float64
		query := `
			SELECT balance
			FROM Balance 
			WHERE timestamp >= ? AND timestamp <= ? AND portfolio_id = ? 
			ORDER BY timestamp DESC 
			LIMIT 1`

		err = balanceDB.QueryRow(query, startOfYesterday, endOfYesterday, id).Scan(&balance)

		if err == sql.ErrNoRows {
			log.Println("No balance found for the previous calendar day interval... Retrying")
			for i := 0; i < 5; i++ {
				startOfYesterday = startOfYesterday - 86400

				query := `
					SELECT balance
					FROM Balance 
					WHERE timestamp >= ? AND timestamp <= ? AND portfolio_id = ? 
					ORDER BY timestamp DESC 
					LIMIT 1`

				err = balanceDB.QueryRow(query, startOfYesterday, endOfYesterday, id).Scan(&balance)

				if err != sql.ErrNoRows {
					recentBalances[id] = balance
					break
				}
				if i == 4 && err == sql.ErrNoRows {
					recentBalances[id] = 10000
				}
			}
		} else if err != nil {
			log.Printf("Error querying interval: %v", err)
			recentBalances[id] = 10000
			GlobalBalance.Balances[id] = &Balance{}
			GlobalBalance.Balances[id].Balance = todayBalance
			GlobalBalance.Balances[id].Cash = todayCash
			return recentBalances
		}

		if balance == 0.0 {
			recentBalances[id] = 10000.0
		}
		if GlobalBalance.Balances[id] == nil {
			GlobalBalance.Balances[id] = &Balance{}
		}

		GlobalBalance.Balances[id].Balance = todayBalance
		GlobalBalance.Balances[id].Cash = todayCash
		
		recentBalances[id] = balance

		message := BalanceData{
			Balance:   todayBalance,
			Timestamp: time.Now().Unix(),
			Cash:      todayCash,
			PortfolioID: id,
		}
		client := Clients["STOCK_CLIENT"]
		if err := send(client, message); err != nil {
			errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
			_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
		}
	}

	fmt.Println("finished")

	return recentBalances
}
