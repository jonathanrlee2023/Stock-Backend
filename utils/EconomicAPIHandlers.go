package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func EconomicDataHandler(w http.ResponseWriter, r *http.Request) {
	economicCategories := []string{"FEDERAL_FUNDS_RATE", "CPI"}
	alphaVantageApiKey := r.URL.Query().Get("apikey")

	today := time.Now()
	_, month, _ := today.Date()
	todayMonth := fmt.Sprintf("%02d", month)

	var result = make([]EconomicDataResponse, len(economicCategories))
	var economicResult EconomicDataResult
	var fileName string
	var filePath string

	// Define cache folder and file name
	cacheFolder := "EconomicData"
	// Ensure the cache folder exists
	if err := os.MkdirAll(cacheFolder, 0755); err != nil {
		http.Error(w, "Error creating cache folder", http.StatusInternalServerError)
		return
	}
	for x, economicCategory := range economicCategories {
		fileName = fmt.Sprintf("%s%sData.json", todayMonth, economicCategory)
		filePath = filepath.Join(cacheFolder, fileName)

		// Check if the file exists
		if FileExists(filePath) {
			// Read the file and decode the data
			fileData, err := os.ReadFile(filePath)
			if err != nil {
				http.Error(w, "Error reading cached data", http.StatusInternalServerError)
				return
			}

			if err := json.Unmarshal(fileData, &result[x]); err != nil {
				http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
				return
			}
		} else {
			apiURL := fmt.Sprintf("https://www.alphavantage.co/query?function=%s&apikey=%s", economicCategory, alphaVantageApiKey)
			data, err := fetchAlphaVantageAPI(apiURL)
			if err != nil {
				http.Error(w, "Error fetching data", http.StatusInternalServerError)
				return
			}

			if err := json.Unmarshal(data, &result[x]); err != nil {
				http.Error(w, "Error parsing data", http.StatusInternalServerError)
				return
			}

			// Write the data to a file for caching
			fileData, err := json.Marshal(result[x])
			if err != nil {
				http.Error(w, "Error saving cached data", http.StatusInternalServerError)
				return
			}

			if err := os.WriteFile(filePath, fileData, 0644); err != nil {
				http.Error(w, "Error writing cached data", http.StatusInternalServerError)
				return
			}
		}
	}

	var inflationResult float64

	floatValue1, err1 := strconv.ParseFloat(result[1].Data[0].Value, 64)
	if err1 != nil {
		http.Error(w, "Invalid data for current value", http.StatusInternalServerError)
		return
	}
	floatValue2, err2 := strconv.ParseFloat(result[1].Data[11].Value, 64)
	if err2 != nil {
		http.Error(w, "Invalid data for previous value", http.StatusInternalServerError)
		return
	}

	// Calculate inflation in percentage
	inflationResult = ((floatValue1 - floatValue2) / floatValue2) * 100

	economicResult = EconomicDataResult{Date: result[0].Data[0].Date, FFR: result[0].Data[0].Value, Inflation: inflationResult}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(economicResult)
}
