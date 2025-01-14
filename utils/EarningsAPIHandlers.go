package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func EarningsVolatilityHandler(w http.ResponseWriter, r *http.Request) {
	ticker := r.URL.Query().Get("ticker")
	earningsFileName := fmt.Sprintf("%searnings.json", ticker)
	earningsFilePath := filepath.Join("EarningsDataCache", earningsFileName)
	stockFileName := fmt.Sprintf("%s.json", ticker)
	stockFilePath := filepath.Join("StockDataCache", stockFileName)

	var stockResult StockResponse
	var earningsResult EarningsResponse
	// sleep for 0.20 seconds to assure file will be created and read
	time.Sleep(200 * time.Millisecond)

	if FileExists(earningsFilePath) && FileExists(stockFilePath) {
		// Read the file and decode the data
		stockFileData, err := os.ReadFile(stockFilePath)
		if err != nil {
			http.Error(w, "Error reading cached data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(stockFileData, &stockResult); err != nil {
			http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
			return
		}
		// Read the file and decode the data
		earningsFileData, err := os.ReadFile(earningsFilePath)
		if err != nil {
			http.Error(w, "Error reading cached data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(earningsFileData, &earningsResult); err != nil {
			http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "Error reading data", http.StatusInternalServerError)
	}

	returnedEarningsVolatility := CalculateEarningsVolatility(stockResult, earningsResult)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(returnedEarningsVolatility)
}

func EarningsCalenderHandler(w http.ResponseWriter, r *http.Request) {
	ticker := r.URL.Query().Get("symbol")
	alphaVantageApiKey := r.URL.Query().Get("apikey")

	// Define cache folder and file name
	cacheFolder := "EarningsDataCache"
	fileName := fmt.Sprintf("%searnings.json", ticker)
	filePath := filepath.Join(cacheFolder, fileName)

	// Ensure the cache folder exists
	if err := os.MkdirAll(cacheFolder, 0755); err != nil {
		http.Error(w, "Error creating cache folder", http.StatusInternalServerError)
		return
	}
	// Check if the file exists
	var result EarningsResponse
	if FileExists(filePath) {
		// Read the file and decode the data
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			http.Error(w, "Error reading cached data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(fileData, &result); err != nil {
			http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
			return
		}
	} else {
		// Fetch data from API
		apiURL := fmt.Sprintf("https://www.alphavantage.co/query?function=EARNINGS&symbol=%s&apikey=%s", ticker, alphaVantageApiKey)
		data, err := fetchAlphaVantageAPI(apiURL)
		if err != nil {
			http.Error(w, "Error fetching data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(data, &result); err != nil {
			http.Error(w, "Error parsing data", http.StatusInternalServerError)
			return
		}

		// Write the data to a file for caching
		fileData, err := json.Marshal(result)
		if err != nil {
			http.Error(w, "Error saving cached data", http.StatusInternalServerError)
			return
		}

		if err := os.WriteFile(filePath, fileData, 0644); err != nil {
			http.Error(w, "Error writing cached data", http.StatusInternalServerError)
			return
		}
	}

	earningsData := result.QuarterlyEarnings

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(earningsData)
}
