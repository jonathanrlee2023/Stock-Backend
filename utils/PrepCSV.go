package utils

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
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

	rows, err := db.Query(`SELECT * FROM EODPrices`)

	if err == sql.ErrNoRows {
		fmt.Println("No EOD Prices Yet")
		return
	} else if err != nil {
		log.Fatal("Query failed process write openDB:", err)
		return
	}
	defer rows.Close()

	var history []OptionRow
	var featureRows []CSVOptionData
	lookahead := 1    // How far in the future to measure return
	smaWindow := 2    // Rolling average window
	zScoreWindow := 5 // Z-score history

	for rows.Next() {
		var ts int64
		var bid, ask, last, high, iv, delta, gamma, theta, vega float64

		err := rows.Scan(&ts, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega)
		if err != nil {
			log.Printf("Failed to scan: %v", err)
			continue
		}

		mark := (bid + ask) / 2

		row := OptionRow{
			Timestamp: ts,
			Mark:      mark,
			IV:        iv,
			Theta:     theta,
			Vega:      vega,
		}
		history = append(history, row)

		// We need at least 10 history rows to compute everything
		if len(history) < zScoreWindow+lookahead {
			continue
		}

		i := len(history) - 1
		iv = history[i].IV
		markNow := history[i].Mark

		// deltaIV
		deltaIV := iv - history[i-1].IV

		// accelIV
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

		// Future return (Mark_t+5 / Mark_t - 1)
		markFuture := history[i-lookahead+1].Mark
		futureReturn := (markFuture - markNow) / markNow

		// Label = 1 if return > 5%
		label := 0
		if futureReturn > 0.05 {
			label = 1
		}

		featureRow := CSVOptionData{
			Symbol:       ticker,
			Timestamp:    ts,
			Mark:         markNow,
			IV:           iv,
			DeltaIV:      deltaIV,
			AccelIV:      accelIV,
			SmaIV:        sma,
			SmaIvSpike:   smaIvSpike,
			IVZScore:     ivZ,
			Theta:        theta,
			Vega:         vega,
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
