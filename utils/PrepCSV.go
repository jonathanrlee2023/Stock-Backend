package utils

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
)

type OptionRow struct {
	Timestamp int64
	Mark      float64
	IV        float64
	Theta     float64
	Vega      float64
}

func PrepCSV(fileName string, featureRows []CSVOptionData) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	headers := []string{
		"symbol", "timestamp", "mark", "iv", "deltaIV", "accelIV", "smaIV",
		"smaIvSpike", "ivZScore", "theta", "vega", "futureReturn", "label",
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write each feature row
	for _, row := range featureRows {
		record := []string{
			row.Symbol,
			strconv.FormatInt(row.Timestamp, 10),
			strconv.FormatFloat(row.Mark, 'f', 6, 64),
			strconv.FormatFloat(row.IV, 'f', 6, 64),
			strconv.FormatFloat(row.DeltaIV, 'f', 6, 64),
			strconv.FormatFloat(row.AccelIV, 'f', 6, 64),
			strconv.FormatFloat(row.SmaIV, 'f', 6, 64),
			strconv.FormatFloat(row.SmaIvSpike, 'f', 6, 64),
			strconv.FormatFloat(row.IVZScore, 'f', 6, 64),
			strconv.FormatFloat(row.Theta, 'f', 6, 64),
			strconv.FormatFloat(row.Vega, 'f', 6, 64),
			strconv.FormatFloat(row.FutureReturn, 'f', 6, 64),
			strconv.Itoa(row.Label),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

func GetDataFromDB(ticker string) {
	db, err := sql.Open("sqlite", fmt.Sprintf("%s.db", ticker))
	if err != nil {
		log.Printf("Query failed process write %s: %v", ticker, err)
		return
	}
	defer db.Close()

	// Helper to get rows from either table
	fetchRows := func(table string) []OptionRow {
		query := fmt.Sprintf(`SELECT timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega FROM %s`, table)
		rows, err := db.Query(query)
		if err != nil {
			log.Printf("Query failed for table %s: %v", table, err)
			return nil
		}
		defer rows.Close()

		var data []OptionRow
		for rows.Next() {
			var ts int64
			var bid, ask, last, high, iv, delta, gamma, theta, vega float64

			if err := rows.Scan(&ts, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega); err != nil {
				log.Printf("Failed to scan from %s: %v", table, err)
				continue
			}
			mark := (bid + ask) / 2
			data = append(data, OptionRow{
				Timestamp: ts,
				Mark:      mark,
				IV:        iv,
				Theta:     theta,
				Vega:      vega,
			})
		}
		return data
	}

	// Fetch and merge both sets
	history := append(fetchRows("EODPrices"), fetchRows("SODPrices")...)

	// Sort combined history by timestamp
	sort.Slice(history, func(i, j int) bool {
		return history[i].Timestamp < history[j].Timestamp
	})

	var featureRows []CSVOptionData
	lookahead := 2
	smaWindow := 2
	zScoreWindow := 5

	for i := range history {
		if i < zScoreWindow || i < 2 || i+lookahead >= len(history) {
			continue
		}

		iv := history[i].IV
		markNow := history[i].Mark

		deltaIV := iv - history[i-1].IV
		accelIV := (iv - history[i-1].IV) - (history[i-1].IV - history[i-2].IV)

		// SMA
		var sma float64
		for j := i - smaWindow + 1; j <= i; j++ {
			sma += history[j].IV
		}
		sma /= float64(smaWindow)
		smaIvSpike := iv / sma

		// Z-score
		var sum, sumSq float64
		for j := i - zScoreWindow + 1; j <= i; j++ {
			sum += history[j].IV
			sumSq += history[j].IV * history[j].IV
		}
		mean := sum / float64(zScoreWindow)
		std := math.Sqrt((sumSq / float64(zScoreWindow)) - (mean * mean))
		ivZ := 0.0
		if std != 0 {
			ivZ = (iv - mean) / std
		}

		label := 0
		futureReturn := 0.0
		// Scan all future entries up to i + 3
		for j := i + 1; j <= i+3; j++ {
			futureReturn = (history[j].Mark - markNow) / markNow
			if futureReturn > 0.03 {
				label = 1
				break // No need to continue once a spike is found
			}
		}

		featureRow := CSVOptionData{
			Symbol:       ticker,
			Timestamp:    history[i].Timestamp,
			Mark:         markNow,
			IV:           iv,
			DeltaIV:      deltaIV,
			AccelIV:      accelIV,
			SmaIV:        sma,
			SmaIvSpike:   smaIvSpike,
			IVZScore:     ivZ,
			Theta:        history[i].Theta,
			Vega:         history[i].Vega,
			FutureReturn: futureReturn,
			Label:        label,
		}
		featureRows = append(featureRows, featureRow)
	}

	err = PrepCSV(fmt.Sprintf("%s_features.csv", ticker), featureRows)
	if err != nil {
		log.Printf("Failed to write CSV for %s: %v", ticker, err)
	} else {
		log.Printf("CSV written for %s with %d rows", ticker, len(featureRows))
	}
}
