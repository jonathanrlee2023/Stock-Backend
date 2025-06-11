package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	_ "modernc.org/sqlite"
)

type PostData struct {
	FileNames []string `json:"filenames"`
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
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
