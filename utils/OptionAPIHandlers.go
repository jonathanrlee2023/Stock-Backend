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

func OptionsHandler(w http.ResponseWriter, r *http.Request) {
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
	typeOfOption := r.URL.Query().Get("type")

	if symbol == "" || start == "" || end == "" || timeframe == "" || typeOfOption == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	if typeOfOption == "Call" {
		typeOfOption = "C"
	} else if typeOfOption == "Put" {
		typeOfOption = "P"
	}

	// sleep for 0.20 seconds to assure file will be created and read
	time.Sleep(200 * time.Millisecond)

	fileName := fmt.Sprintf("%s.json", symbol)
	filePath := filepath.Join("StockDataCache", fileName)

	var stockResult StockResponse

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
		http.Error(w, "Error reading data", http.StatusInternalServerError)
	}

	year, month, day := NextWeekFriday()
	roundedPrice := RoundToNearestFive(stockResult.Results[stockResult.Count-1].C)
	formattedRoundedPrice := fmt.Sprintf("%03d", roundedPrice)

	optionSuffixes := fmt.Sprintf("%d%s%s%s00%s000", year-2000, month, day, typeOfOption, formattedRoundedPrice)

	var apiUrl string

	apiUrl = fmt.Sprintf(
		"https://data.alpaca.markets/v1beta1/options/bars?symbols=%s%s&timeframe=%s&start=%s&end=%s&limit=1000&sort=desc",
		url.QueryEscape(symbol),
		optionSuffixes,
		url.QueryEscape(timeframe),
		url.QueryEscape(start),
		url.QueryEscape(end),
	)
	symbolData := make(map[time.Time]float64)
	data, err := fetchAlpacaAPIWithHeaders(apiUrl, alpacaKeyID, alpacaSecretKey)
	if err != nil {
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}

	var result AlpacaResponse
	var pricesJson []CombinedOptions

	if err := json.Unmarshal(data, &result); err != nil {
		http.Error(w, "Error parsing data", http.StatusInternalServerError)
		return
	}

	fullKey := symbol + optionSuffixes
	symbolData = SlopeFunctions(result, fullKey)

	for x, value := range symbolData {
		pricesJson = append(pricesJson, CombinedOptions{Price: value, Timestamp: x})
	}

	symbolJSON := OptionsSymbol{Symbol: pricesJson, Ticker: symbol, Price: roundedPrice, ExpirationDate: fmt.Sprintf("%d-%s-%s", year, month, day)}

	responseData, err := json.Marshal(symbolJSON)
	if err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseData)
}

func OptionVolatilityHandler(w http.ResponseWriter, r *http.Request) {

}
