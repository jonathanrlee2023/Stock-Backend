package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	alpacaKeyID := r.Header.Get("APCA-API-Key-ID")
	alpacaSecretKey := r.Header.Get("APCA-API-SECRET-KEY")

	if alpacaKeyID == "" || alpacaSecretKey == "" {
		http.Error(w, "Missing Alpaca API keys in headers", http.StatusBadRequest)
		return
	}

	optionType := r.URL.Query().Get("type")
	ticker := r.URL.Query().Get("ticker")

	fileName := fmt.Sprintf("%s.json", ticker)
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

	mostRecentPrice := stockResult.Results[stockResult.Count-1].C
	roundedPrice := RoundToNearestFive(stockResult.Results[stockResult.Count-1].C)

	daysUntilFriday := int(time.Friday) - int(time.Now().Weekday())
	if daysUntilFriday <= 0 {
		daysUntilFriday += 7
	}

	yearsTillNextFriday := float64(daysUntilFriday) / 365.0

	today := time.Now()
	_, month, _ := today.Date()
	todayMonth := fmt.Sprintf("%02d", month)

	fileName = fmt.Sprintf("%sFEDERAL_FUNDS_RATEDATA.json", todayMonth)
	filePath = filepath.Join("EconomicData", fileName)

	var economicResult EconomicDataResponse

	if FileExists(filePath) {
		// Read the file and decode the data
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			http.Error(w, "Error reading cached data", http.StatusInternalServerError)
			return
		}

		if err := json.Unmarshal(fileData, &economicResult); err != nil {
			http.Error(w, "Error parsing cached data", http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "Error reading data", http.StatusInternalServerError)
	}

	riskFreeRateString := economicResult.Data[0].Value
	riskFreeRate, err := strconv.ParseFloat(riskFreeRateString, 64)
	if err != nil {
		http.Error(w, "Invalid data for current value", http.StatusInternalServerError)
		return
	}
	yesterday := MostRecentWeekday(time.Now().AddDate(0, 0, -1))
	year, month, day := yesterday.Date()
	yesterdayDate := fmt.Sprintf("%d-%02d-%02d", year, month, day)

	apiURL := fmt.Sprintf("http://localhost:8080/options?symbol=%s&start=%s&end=%s&timeframe=5Min&type=%s", ticker, yesterdayDate, yesterdayDate, optionType)
	data, err := fetchAlpacaAPIWithHeaders(apiURL, alpacaKeyID, alpacaSecretKey)
	if err != nil {
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}

	var result OptionsSymbol

	if err := json.Unmarshal(data, &result); err != nil {
		http.Error(w, "Error parsing data 1", http.StatusInternalServerError)
		return
	}

	symbolData := make(map[time.Time]float64)
	var timestamps []time.Time
	for _, value := range result.Symbol {
		symbolData[value.Timestamp] = value.Price
		timestamps = append(timestamps, value.Timestamp)
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].After(timestamps[j])
	})

	mostRecentOptionPrice := symbolData[timestamps[0]]

	if optionType == "Call" {
		optionType = "C"
	} else if optionType == "Put" {
		optionType = "P"
	}

	option := Option{S: mostRecentPrice, K: float64(roundedPrice), T: yearsTillNextFriday, R: riskFreeRate / 100.0, P: mostRecentOptionPrice, CP: optionType}

	apiURL = fmt.Sprintf("http://localhost:8080/stock?symbol=%s&apikey=X8531ZcJaqW6j7l9tG1PVFBZnwMNRs72", ticker)
	statisticsData, err := fetchDefaultAPI(apiURL)
	if err != nil {
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}

	var statisticsResult StockStatistics

	if err := json.Unmarshal(statisticsData, &statisticsResult); err != nil {
		http.Error(w, "Error parsing data 2", http.StatusInternalServerError)
		return
	}

	impliedVolatility, err := impliedVolatility(option, statisticsResult.Volatility/100.0)

	volatility := ImpliedVolatility{Volatility: impliedVolatility * 100}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(volatility)
}

func CombinedOptionsHandler(w http.ResponseWriter, r *http.Request) {
	alpacaKeyID := r.Header.Get("APCA-API-Key-ID")
	alpacaSecretKey := r.Header.Get("APCA-API-SECRET-KEY")

	if alpacaKeyID == "" || alpacaSecretKey == "" {
		http.Error(w, "Missing Alpaca API keys in headers", http.StatusBadRequest)
		return
	}

	symbol := r.URL.Query().Get("symbol")

	if symbol == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	var apiUrl string

	yesterday := MostRecentWeekday(time.Now().AddDate(0, 0, -1))
	year, month, day := yesterday.Date()
	yesterdayDate := fmt.Sprintf("%d-%02d-%02d", year, month, day)

	apiUrl = fmt.Sprintf(
		"http://localhost:8080/options?symbol=%s&start=%s&end=%s&timeframe=5Min&type=Call",
		symbol,
		yesterdayDate,
		yesterdayDate,
	)
	data, err := fetchAlpacaAPIWithHeaders(apiUrl, alpacaKeyID, alpacaSecretKey)
	if err != nil {
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}

	var callResult OptionsSymbol

	if err := json.Unmarshal(data, &callResult); err != nil {
		http.Error(w, "Error parsing data 1", http.StatusInternalServerError)
		return
	}

	callData := make(map[time.Time]float64)
	var callTimestamps []time.Time
	for _, value := range callResult.Symbol {
		callData[value.Timestamp] = value.Price
		callTimestamps = append(callTimestamps, value.Timestamp)
	}

	sort.Slice(callTimestamps, func(i, j int) bool {
		return callTimestamps[i].After(callTimestamps[j])
	})

	apiUrl = fmt.Sprintf(
		"http://localhost:8080/options?symbol=%s&start=%s&end=%s&timeframe=5Min&type=Put",
		symbol,
		yesterdayDate,
		yesterdayDate,
	)
	data, err = fetchAlpacaAPIWithHeaders(apiUrl, alpacaKeyID, alpacaSecretKey)
	if err != nil {
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}

	var putResult OptionsSymbol

	if err := json.Unmarshal(data, &putResult); err != nil {
		http.Error(w, "Error parsing data 1", http.StatusInternalServerError)
		return
	}

	putData := make(map[time.Time]float64)
	var putTimestamps []time.Time
	for _, value := range putResult.Symbol {
		putData[value.Timestamp] = value.Price
		putTimestamps = append(putTimestamps, value.Timestamp)
	}

	sort.Slice(putTimestamps, func(i, j int) bool {
		return putTimestamps[i].After(putTimestamps[j])
	})

	earliestCommon, latestCommon := FindCommonTimes(callTimestamps, putTimestamps)
	if earliestCommon == nil || latestCommon == nil {
		http.Error(w, "No common timestamps found", http.StatusBadRequest)
		return
	}

	combinedOptions := make(map[time.Time]float64)

	optionTime := *earliestCommon // Dereference the pointer to get the time.Time value

	for !optionTime.After(*latestCommon) {
		combinedOptions[optionTime] = putData[optionTime] + callData[optionTime]
		optionTime = optionTime.Add(5 * time.Minute)
	}

	var pricesJson []CombinedOptions
	for x, value := range combinedOptions {
		pricesJson = append(pricesJson, CombinedOptions{Price: value, Timestamp: x})
	}

	year2, month2, day2 := NextWeekFriday()
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

	roundedPrice := RoundToNearestFive(stockResult.Results[stockResult.Count-1].C)

	symbolJSON := OptionsSymbol{Symbol: pricesJson, Ticker: symbol, Price: roundedPrice, ExpirationDate: fmt.Sprintf("%d-%s-%s", year2, month2, day2)}
	responseData, err := json.Marshal(symbolJSON)
	if err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(responseData)
}
