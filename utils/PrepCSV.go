package utils

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type OptionRow struct {
	Timestamp int64
	Mark      float64
	IV        float64
	Delta     float64
	Gamma     float64
	Theta     float64
	Vega      float64
}

type EarningsDates map[string]string

func InitCSVData() {
	f, err := os.Open("earnings_dates.json")
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		log.Printf("Failed to read file: %v", err)
		return
	}

	var dates EarningsDates
	if err := json.Unmarshal(data, &dates); err != nil {
		log.Printf("Failed to parse JSON: %v", err)
		return
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("ReadDir error: %v", err)
		return
	}

	var dbFiles []string
	var callDB = regexp.MustCompile(`^[A-Z]+_\d{6}C\d+\.db$`)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := filepath.Ext(name)
		if ext != ".db" || !callDB.MatchString(name) {
			continue
		}

		// Remove the final ".db" and see if there's *another* ".db"
		base := strings.TrimSuffix(name, ext)
		if filepath.Ext(base) != ".db" && len(name) > 15 {
			underlyingTicker := extractTicker(name)

			edStr, ok := dates[underlyingTicker]
			if !ok {
				log.Printf("No earnings date for %s, skipping daysToEarnings", name)
			}
			var earningsDate time.Time
			if ok {
				earningsDate, err = time.Parse("2006-01-02", edStr)
				if err != nil {
					log.Printf("Invalid earnings date format for %s: %v", name, err)
					ok = false
				}
			}
			var daysToEarnings int64
			if ok {
				now := time.Now()
				diff := earningsDate.Sub(now).Hours() / 24
				daysToEarnings = int64(math.Ceil(diff))
			}

			if daysToEarnings > 0 {
				dbFiles = append(dbFiles, name)
			}
		}
	}

	for _, entry := range dbFiles {
		fmt.Println(entry)
		GetDataFromDB(entry, dates)
	}

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
		"smaIvSpike", "ivZScore", "theta", "vega", "futureReturn", "futurePrice", "label", "daysToEarnings",
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write each feature row
	for _, row := range featureRows {
		record := []string{
			row.Symbol,
			strconv.FormatInt(row.Timestamp, 10),
			strconv.FormatFloat(row.Mark, 'f', 2, 64),
			strconv.FormatFloat(row.IV, 'f', 6, 64),
			strconv.FormatFloat(row.DeltaIV, 'f', 6, 64),
			strconv.FormatFloat(row.AccelIV, 'f', 6, 64),
			strconv.FormatFloat(row.SmaIV, 'f', 6, 64),
			strconv.FormatFloat(row.SmaIvSpike, 'f', 6, 64),
			strconv.FormatFloat(row.IVZScore, 'f', 6, 64),
			strconv.FormatFloat(row.Theta, 'f', 6, 64),
			strconv.FormatFloat(row.Vega, 'f', 6, 64),
			strconv.FormatFloat(row.FutureReturn, 'f', 4, 64),
			strconv.FormatFloat(row.FuturePrice, 'f', 2, 64),
			strconv.Itoa(row.Label),
			strconv.FormatInt(row.DaysToEarnings, 10),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

func GetDataFromDB(ticker string, dates EarningsDates) {
	fmt.Println(ticker)
	callDB, err := sql.Open("sqlite", "file:"+ticker+"?mode=ro")
	if err != nil {
		log.Printf("open call DB %s failed: %v", ticker, err)
		return
	}
	defer callDB.Close()

	// 2) Derive & open put DB
	putID := flipCallPut(ticker)
	putDB, err := sql.Open("sqlite", "file:"+putID+"?mode=ro")
	if err != nil {
		log.Printf("open put DB %s failed: %v; continuing with call-only", putID, err)
	}
	if putDB != nil {
		defer putDB.Close()
	}

	underlyingTicker := extractTicker(ticker)

	edStr, ok := dates[underlyingTicker]
	if !ok {
		log.Printf("No earnings date for %s, skipping daysToEarnings", ticker)
	}
	var earningsDate time.Time
	if ok {
		earningsDate, err = time.Parse("2006-01-02", edStr)
		if err != nil {
			log.Printf("Invalid earnings date format for %s: %v", ticker, err)
			ok = false
		}
	}

	// Helper to get rows from either table

	// Fetch and merge both sets
	callRows := append(
		fetchRowsFromDB(callDB, "EODPrices"),
		fetchRowsFromDB(callDB, "SODPrices")...,
	)

	var putRows []OptionRow
	if putDB != nil {
		putRows = append(
			fetchRowsFromDB(putDB, "EODPrices"),
			fetchRowsFromDB(putDB, "SODPrices")...,
		)
	}

	merged := make(map[int64]OptionRow, len(callRows))
	for _, entry := range callRows {
		merged[entry.Timestamp] = entry
	}
	for _, entry := range putRows {
		if base, ok := merged[entry.Timestamp]; ok {
			// sum mark & Greeks
			base.Mark += entry.Mark
			base.Delta += entry.Delta
			base.Gamma += entry.Gamma
			base.Theta += entry.Theta
			base.Vega += entry.Vega
			merged[entry.Timestamp] = base
		} else {
			// no call at this timestamp? keep put-only row
			merged[entry.Timestamp] = entry
		}
	}

	history := make([]OptionRow, 0, len(merged))
	for _, row := range merged {
		history = append(history, row)
	}

	sort.Slice(history, func(i, j int) bool {
		return history[i].Timestamp < history[j].Timestamp
	})

	var featureRows []CSVOptionData
	lookahead := 2
	smaWindow := 2
	zScoreWindow := 5

	for i := range history {
		if i < zScoreWindow || i < 2 || i+lookahead+1 >= len(history) {
			continue
		}

		curr := history[i]
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
		futurePrice := 0.0
		// Scan all future entries up to i + 3
		for j := i + 1; j <= i+3; j++ {
			futureReturn = (history[j].Mark - markNow) / markNow
			if futureReturn > 0.02 {
				label = 1
				futurePrice = history[j].Mark
				break // No need to continue once a spike is found
			}
		}

		var daysToEarnings int64
		if ok {
			now := time.Unix(curr.Timestamp, 0)
			diff := earningsDate.Sub(now).Hours() / 24
			daysToEarnings = int64(math.Ceil(diff))
		}

		if daysToEarnings <= 0 {
			continue
		}

		featureRow := CSVOptionData{
			Symbol:         ticker,
			Timestamp:      history[i].Timestamp,
			Mark:           markNow,
			IV:             iv,
			DeltaIV:        deltaIV,
			AccelIV:        accelIV,
			SmaIV:          sma,
			SmaIvSpike:     smaIvSpike,
			IVZScore:       ivZ,
			Delta:          history[i].Delta,
			Gamma:          history[i].Gamma,
			Theta:          history[i].Theta,
			Vega:           history[i].Vega,
			FutureReturn:   futureReturn,
			FuturePrice:    futurePrice,
			Label:          label,
			DaysToEarnings: daysToEarnings,
		}
		featureRows = append(featureRows, featureRow)
	}

	err = PrepCSV(fmt.Sprintf("%s_features.csv", underlyingTicker), featureRows)
	if err != nil {
		log.Printf("Failed to write CSV for %s: %v", ticker, err)
	} else {
		log.Printf("CSV written for %s with %d rows", underlyingTicker, len(featureRows))
	}
}

func extractTicker(optionID string) string {
	// Split into at most 2 pieces
	parts := strings.SplitN(optionID, "_", 2)
	return parts[0]
}

func flipCallPut(optID string) string {
	// find the last 'C' or 'P' in the string
	idx := strings.LastIndexAny(optID, "CP")
	if idx < 0 {
		return optID
	}
	var other byte
	if optID[idx] == 'C' {
		other = 'P'
	} else {
		other = 'C'
	}
	// rebuild string with that one character swapped
	return optID[:idx] + string(other) + optID[idx+1:]
}

func fetchRowsFromDB(db *sql.DB, table string) []OptionRow {
	q := fmt.Sprintf(
		`SELECT timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega 
       FROM %s`, table)

	rows, err := db.Query(q)
	if err != nil {
		log.Printf("Query %s failed: %v", table, err)
		return nil
	}
	defer rows.Close()

	var out []OptionRow
	for rows.Next() {
		var ts int64
		var bid, ask, last, high, iv, delta, gamma, theta, vega float64
		if err := rows.Scan(&ts, &bid, &ask, &last, &high, &iv, &delta, &gamma, &theta, &vega); err != nil {
			log.Printf("Scan %s failed: %v", table, err)
			continue
		}
		mark := (bid + ask) / 2
		out = append(out, OptionRow{
			Timestamp: ts,
			Mark:      mark,
			IV:        iv,
			Delta:     delta,
			Gamma:     gamma,
			Theta:     theta,
			Vega:      vega,
		})
	}
	return out
}
