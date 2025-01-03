package main

import (
	"Go-API/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Empty cache folder everytime API is turned on
	folderPath := "./StockDataCache"
	err := utils.DeleteContents(folderPath)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Folder contents deleted successfully!")
	}
	// start api
	mux := http.NewServeMux()
	mux.HandleFunc("/options", optionsHandler)
	mux.HandleFunc("/earnings", earningsHandler)
	mux.HandleFunc("/stock", stockHandler)

	handler := corsMiddleware(mux)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func fetchAlpacaAPIWithHeaders(url, keyID, secretKey string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("APCA-API-KEY-ID", keyID)
	req.Header.Add("APCA-API-SECRET-KEY", secretKey)

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func fetchAlphaVantageAPI(url string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func fetchPolygonAPI(url string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("Content-Type", "application/json")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func optionsHandler(w http.ResponseWriter, r *http.Request) {
	alpacaKeyID := r.Header.Get("APCA-API-Key-ID")
	alpacaSecretKey := r.Header.Get("APCA-API-SECRET-KEY")

	if alpacaKeyID == "" || alpacaSecretKey == "" {
		http.Error(w, "Missing Alpaca API keys in headers", http.StatusBadRequest)
		return
	}

	symbol := r.URL.Query().Get("symbol")
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	timeframe := r.URL.Query().Get("timeframe")

	if symbol == "" || start == "" || end == "" || timeframe == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	optionSuffixes := []string{"241227P00590000", "241227C00600000"}

	var apiUrl []string
	var symbolJSON utils.OptionsSymbol

	for _, suffix := range optionSuffixes {
		apiUrl = append(apiUrl, fmt.Sprintf(
			"https://data.alpaca.markets/v1beta1/options/bars?symbols=%s%s&timeframe=%s&start=%s&end=%s&limit=1000&sort=desc",
			url.QueryEscape(symbol),
			suffix,
			url.QueryEscape(timeframe),
			url.QueryEscape(start),
			url.QueryEscape(end),
		))
	}

	for i, url := range apiUrl {
		symbolData := make(map[time.Time]float64)
		data, err := fetchAlpacaAPIWithHeaders(url, alpacaKeyID, alpacaSecretKey)
		if err != nil {
			http.Error(w, "Error fetching data", http.StatusInternalServerError)
			return
		}

		var result utils.AlpacaOptionsResponse
		var pricesJson []utils.CombinedOptions

		if err := json.Unmarshal(data, &result); err != nil {
			http.Error(w, "Error parsing data", http.StatusInternalServerError)
			return
		}

		fullKey := symbol + optionSuffixes[i]
		symbolData = utils.SlopeFunctions(result, fullKey)

		for x, value := range symbolData {
			pricesJson = append(pricesJson, utils.CombinedOptions{Price: value, Timestamp: x})
		}

		optionsJson := utils.OptionsPrices{Options: pricesJson}

		symbolJSON.Symbol = append(symbolJSON.Symbol, optionsJson)
	}
	responseData, err := json.Marshal(symbolJSON)
	if err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseData)
}

func earningsHandler(w http.ResponseWriter, r *http.Request) {
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
	var result utils.EarningsResponse
	if utils.FileExists(filePath) {
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
		fmt.Println("Read data from cache")
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

// handle stock api calls, calculate volatility, standard deviation, and most recent stock price
func stockHandler(w http.ResponseWriter, r *http.Request) {
	polygonApiKey := r.URL.Query().Get("apikey")
	ticker := r.URL.Query().Get("symbol")

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
	var result utils.StockResponse
	if utils.FileExists(filePath) {
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
		fmt.Println("Read data from cache")
	} else {
		// Fetch data from API
		apiURL := fmt.Sprintf("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&apiKey=%s", ticker, twoYearsAgoDate, yesterdayDate, polygonApiKey)
		data, err := fetchPolygonAPI(apiURL)
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

	standardDev := utils.StandardDev(result)

	volatility, yesterdayPrice := utils.CalculateHistoricalVolatility(result, float64(result.ResultsCount))

	returnedStatistics := utils.StockStatistics{Volatility: volatility, StdDev: standardDev, RecentClose: yesterdayPrice}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(returnedStatistics)
}

// func impliedVolatilityHandler(w http.ResponseWriter, r *http.Request) {
// 	ticker := r.URL.Query().Get("symbol")

// 	url := fmt.Sprintf("http://localhost:8080/options?symbol=%s&start=2024-12-20&end=2024-12-20&timeframe=10Min", ticker)
// 	data, err := fetchPolygonAPI(url)
// 	if err != nil {
// 		http.Error(w, "Error fetching data", http.StatusInternalServerError)
// 		return
// 	}

// 	var result utils.OptionsSymbol

// 	if err := json.Unmarshal(data, &result); err != nil {
// 		http.Error(w, "Error parsing data", http.StatusInternalServerError)
// 		return
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(returnedStdDev)
// }

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		// Handle preflight request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
