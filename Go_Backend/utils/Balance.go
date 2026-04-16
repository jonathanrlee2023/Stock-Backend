package utils

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

// Gets the previous day's last balance
func GetMostRecentBalance(userID int, client *Client) map[int]float64 {
	var latestTs int64
	var todayBalance float64
	var todayCash float64

	recentBalances := make(map[int]float64)

	client.PortfolioIDs.Lock()
	client.Balance.Lock()
	defer client.PortfolioIDs.Unlock()
	defer client.Balance.Unlock()
	if client.PortfolioIDs.IDs == nil {
		client.PortfolioIDs.IDs = make(map[int]string)
	}
	if client.Balance.Balances == nil {
		client.Balance.Balances = make(map[int]*Balance)
	}
	
	if ok := client.PortfolioIDs.IDs[1]; ok != "Main" {
		client.Balance.Balances[1] = &Balance{}
		client.Balance.Balances[1].Balance = 10000
		client.Balance.Balances[1].Cash = 10000
		recentBalances[1] = 10000
		client.PortfolioIDs.IDs[1] = "Main"
		return recentBalances
	}

	for id := range client.PortfolioIDs.IDs {
		err := GlobalDatabasePool.BalanceDB.QueryRow("SELECT timestamp, balance, cash FROM Balance WHERE portfolio_id = ? AND user_id = ? ORDER BY timestamp DESC LIMIT 1", id, userID).Scan(&latestTs, &todayBalance, &todayCash)
		if err == sql.ErrNoRows {
			client.Balance.Balances[id] = &Balance{}
			client.Balance.Balances[id].Balance = 10000
			client.Balance.Balances[id].Cash = 10000
		}
		currentDate := time.Unix(latestTs, 0)
		beginningOfToday := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 0, 0, 0, 0, currentDate.Location())

		endOfYesterday := beginningOfToday.Unix() - 1
		startOfYesterday := beginningOfToday.AddDate(0, 0, -7).Unix()

		var balance float64
		query := `
			SELECT balance
			FROM Balance 
			WHERE timestamp >= ? AND timestamp <= ? AND portfolio_id = ? AND user_id = ?
			ORDER BY timestamp DESC 
			LIMIT 1`

		err = GlobalDatabasePool.BalanceDB.QueryRow(query, startOfYesterday, endOfYesterday, id, userID).Scan(&balance)

		if err == sql.ErrNoRows {
			log.Println("No balance found for the previous calendar day interval... Retrying")
			for i := 0; i < 5; i++ {
				startOfYesterday = startOfYesterday - 86400

				query := `
					SELECT balance
					FROM Balance 
					WHERE timestamp >= ? AND timestamp <= ? AND portfolio_id = ? AND user_id = ?
					ORDER BY timestamp DESC 
					LIMIT 1`

				err = GlobalDatabasePool.BalanceDB.QueryRow(query, startOfYesterday, endOfYesterday, id, userID).Scan(&balance)

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
			client.Balance.Balances[id] = &Balance{}
			client.Balance.Balances[id].Balance = todayBalance
			client.Balance.Balances[id].Cash = todayCash
			return recentBalances
		}

		if balance == 0.0 {
			recentBalances[id] = 10000.0
		}
		if client.Balance.Balances[id] == nil {
			client.Balance.Balances[id] = &Balance{}
		}

		client.Balance.Balances[id].Balance = todayBalance
		client.Balance.Balances[id].Cash = todayCash
		
		recentBalances[id] = balance

		message := BalanceData{
			Balance:   todayBalance,
			Timestamp: time.Now().Unix(),
			Cash:      todayCash,
			PortfolioID: id,
		}
		
		if client != nil {
			if err := send(client, message); err != nil {
				errMsg, _ := json.Marshal(map[string]string{"error": err.Error()})
				_ = client.Conn.WriteMessage(websocket.TextMessage, errMsg)
			}
		} else {
			log.Println("No client found for user ID:", userID)
		}
	}

	return recentBalances
}

func GetPorfolioIDs(userID int, client *Client) {
    if client == nil {
        log.Println("Error: GetPorfolioIDs called with nil client")
        return
    }

    found := false
    
    client.PortfolioIDs.Lock()
    defer client.PortfolioIDs.Unlock()  

    if client.PortfolioIDs.IDs == nil {
        client.PortfolioIDs.IDs = make(map[int]string)
    }
    if client.Balance.Balances == nil {
        client.Balance.Balances = make(map[int]*Balance)
    }

    rows, err := GlobalDatabasePool.BalanceDB.Query("SELECT portfolio_id, name FROM Portfolios WHERE user_id = ?", userID)
    if err != nil {
        log.Println(err)
        return
    }
    defer rows.Close()

    for rows.Next() {
        found = true
        var pid int
        var name string
        if err := rows.Scan(&pid, &name); err != nil {
            log.Println(err)
            return
        }
        
        client.PortfolioIDs.IDs[pid] = name
        
        if _, exists := client.Balance.Balances[pid]; !exists {
            client.Balance.Balances[pid] = &Balance{} 
        }
    }
	if !found {
		client.PortfolioIDs.IDs[1] = "Main"
		insertData := `INSERT INTO Portfolios (portfolio_id, name, user_id) VALUES (?, ?, ?)`
		_, err = GlobalDatabasePool.BalanceDB.Exec(insertData, 1, "Main", userID)
		if err != nil {
			log.Printf("Failed to write to table: %v", err)
		}
	}
}