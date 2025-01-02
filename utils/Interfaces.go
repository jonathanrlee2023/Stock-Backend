package utils

import "time"

type AlpacaOptionsResponse struct {
	Bars map[string][]struct {
		C  float64   `json:"c"`
		H  float64   `json:"h"`
		L  float64   `json:"l"`
		N  int       `json:"n"`
		O  float64   `json:"o"`
		T  time.Time `json:"t"`
		V  int       `json:"v"`
		Vw float64   `json:"vw"`
	} `json:"bars"`
	NextPageToken interface{} `json:"next_page_token"`
}

// Annual earnings data
type AnnualEarning struct {
	FiscalDateEnding string `json:"fiscalDateEnding"`
	ReportedEPS      string `json:"reportedEPS"`
}

// Quartely earnings data
type QuarterlyEarning struct {
	FiscalDateEnding   string `json:"fiscalDateEnding"`
	ReportedDate       string `json:"reportedDate"`
	ReportedEPS        string `json:"reportedEPS"`
	EstimatedEPS       string `json:"estimatedEPS"`
	Surprise           string `json:"surprise"`
	SurprisePercentage string `json:"surprisePercentage"`
	ReportTime         string `json:"reportTime"`
}

// Contains both quarterly and annual data
type EarningsResponse struct {
	Symbol            string             `json:"symbol"`
	AnnualEarnings    []AnnualEarning    `json:"annualEarnings"`
	QuarterlyEarnings []QuarterlyEarning `json:"quarterlyEarnings"`
}

type CombinedOptions struct {
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

type OptionsPrices struct {
	Options []CombinedOptions `json:"options"`
}

type OptionsSymbol struct {
	Symbol []OptionsPrices `json:"symbol"`
}

type StockResponse struct {
	Ticker       string `json:"ticker"`
	QueryCount   int    `json:"queryCount"`
	ResultsCount int    `json:"resultsCount"`
	Adjusted     bool   `json:"adjusted"`
	Results      []struct {
		V  float64 `json:"v"`
		Vw float64 `json:"vw"`
		O  float64 `json:"o"`
		C  float64 `json:"c"`
		H  float64 `json:"h"`
		L  float64 `json:"l"`
		T  int64   `json:"t"`
		N  int     `json:"n"`
	} `json:"results"`
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	Count     int    `json:"count"`
}

type StockStatistics struct {
	StdDev      float64 `json:"StandardDeviation"`
	Volatility  float64 `json:"Volatility"`
	RecentClose float64 `json:"RecentClose"`
}
