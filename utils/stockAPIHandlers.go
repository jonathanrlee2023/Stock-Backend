package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// handle stock api calls, calculate volatility, standard deviation, and most recent stock price
func StockHandler(w http.ResponseWriter, r *http.Request) {
	polygonApiKey := r.URL.Query().Get("apikey")
	ticker := r.URL.Query().Get("symbol")

	if polygonApiKey == "" || ticker == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	// format yesterday and two years ago date
	yesterday := time.Now().AddDate(0, 0, -1)
	twoYearsAgo := time.Now().AddDate(-2, 0, 0)
	year, month, day := yesterday.Date()
	yesterdayDate := fmt.Sprintf("%d-%02d-%02d", year, month, day)
	year, month, day = twoYearsAgo.Date()
	twoYearsAgoDate := fmt.Sprintf("%d-%02d-%02d", year, month, day)

	// Define cache folder and file name
	cacheFolder := "StockDataCache"
	fileName := fmt.Sprintf("%s.json", ticker)
	filePath := filepath.Join(cacheFolder, fileName)

	// Ensure the cache folder exists
	if err := os.MkdirAll(cacheFolder, 0755); err != nil {
		http.Error(w, "Error creating cache folder", http.StatusInternalServerError)
		return
	}

	// Check if the file exists
	var result StockResponse
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
		apiURL := fmt.Sprintf("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&apiKey=%s", ticker, twoYearsAgoDate, yesterdayDate, polygonApiKey)
		data, err := fetchDefaultAPI(apiURL)
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

	standardDev := StandardDev(result)

	volatility, yesterdayPrice := CalculateHistoricalVolatility(result, float64(result.ResultsCount))

	returnedStatistics := StockStatistics{Volatility: volatility, StdDev: standardDev, RecentClose: yesterdayPrice}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(returnedStatistics)
}

// Retrieves today's prices for a given stock
func TodayStockHandler(w http.ResponseWriter, r *http.Request) {
	alpacaKeyID := r.Header.Get("APCA-API-Key-ID")
	alpacaSecretKey := r.Header.Get("APCA-API-SECRET-KEY")

	if alpacaKeyID == "" || alpacaSecretKey == "" {
		http.Error(w, "Missing Alpaca API keys in headers", http.StatusBadRequest)
		return
	}

	symbol := r.URL.Query().Get("symbol")
	timeframe := r.URL.Query().Get("timeframe")

	if symbol == "" || timeframe == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	// sleep for 0.20 seconds to assure file will be created and read
	time.Sleep(200 * time.Millisecond)

	cacheFolder := "TodayStockDataCache"
	fileName := fmt.Sprintf("Today%s.json", symbol)
	filePath := filepath.Join("TodayStockDataCache", fileName)

	mostRecentWeekday := MostRecentWeekday(time.Now())
	year, month, day := mostRecentWeekday.Date()
	todayDate := fmt.Sprintf("%d-%02d-%02d", year, month, day)

	var stockResult AlpacaResponse
	// Ensure the cache folder exists
	if err := os.MkdirAll(cacheFolder, 0755); err != nil {
		http.Error(w, "Error creating cache folder", http.StatusInternalServerError)
		return
	}

	// Check if the file exists
	if FileExists(filePath) {
		// Read the file and decode the data
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			http.Error(w, "Error reading cached data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(fileData, &stockResult); err != nil {
			http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
			return
		}
	} else {
		// Fetch data from API
		baseURL := "https://data.alpaca.markets/v2/stocks/bars"

		// Build query parameters
		params := url.Values{}
		params.Add("symbols", symbol)
		params.Add("timeframe", timeframe)
		params.Add("start", fmt.Sprintf("%sT14:30:00Z", todayDate))
		params.Add("end", fmt.Sprintf("%sT22:30:00Z", todayDate))
		params.Add("limit", "200")
		params.Add("adjustment", "split")
		params.Add("feed", "sip")
		params.Add("sort", "asc")

		// Combine base URL with query parameters
		apiUrl := fmt.Sprintf("%s?%s", baseURL, params.Encode())

		data, err := fetchAlpacaAPIWithHeaders(apiUrl, alpacaKeyID, alpacaSecretKey)
		if err != nil {
			http.Error(w, "Error fetching data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(data, &stockResult); err != nil {
			http.Error(w, "Error parsing data", http.StatusInternalServerError)
			return
		}

		// Write the data to a file for caching
		fileData, err := json.Marshal(stockResult)
		if err != nil {
			http.Error(w, "Error saving cached data", http.StatusInternalServerError)
			return
		}

		if err := os.WriteFile(filePath, fileData, 0644); err != nil {
			http.Error(w, "Error writing cached data", http.StatusInternalServerError)
			return
		}
	}
	var pricesJson []CombinedStock

	symbolData := SlopeFunctions(stockResult, symbol)

	for x, value := range symbolData {
		pricesJson = append(pricesJson, CombinedStock{Price: value, Timestamp: x})
	}

	symbolJSON := StockSymbol{Symbol: pricesJson, Ticker: symbol}

	responseData, err := json.Marshal(symbolJSON)
	if err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseData)
}
