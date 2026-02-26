package utils

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn      *websocket.Conn
	ID        string // unique identifier for this client (e.g., user ID, session ID)
	Mu        sync.Mutex
	Done      chan struct{}
	once      sync.Once
	IsWriting bool
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.Done)
	})
}

func (c *Client) SafeWrite(messageType int, data []byte) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.Conn.WriteMessage(messageType, data)
}

type OptionStreamRequest struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
	Day    string `json:"day"`
	Month  string `json:"month"`
	Year   string `json:"year"`
	Type   string `json:"type"`
}

type StockStreamRequest struct {
	Symbol string `json:"symbol"`
}

type Company_Stats struct {
	Symbol           string   `json:"Symbol"`
	MarketCap        float64  `json:"MarketCap"`
	PEG              *float64 `json:"PEG"`
	Sloan            *float64 `json:"Sloan"`
	ROIC             *float64 `json:"ROIC"`
	HistGrowth       *float64 `json:"HistGrowth"`
	ForecastedGrowth *float64 `json:"ForecastedGrowth"`
	// Using pointers (*float64) for PEG fields because your Python code
	// specifically returns 'None' if growth is negative.
	TrailingPEG    *float64 `json:"TrailingPEG"`
	ForwardPEG     *float64 `json:"ForwardPEG"`
	IntrinsicPrice *float64 `json:"IntrinsicPrice"`
	// DividendPrice can also be None if the company has no dividend history.
	DividendPrice *float64 `json:"DividendPrice"`
	PriceAtReport *float64 `json:"PriceAtReport"`
	WACC          *float64 `json:"WACC"`
	FCFF          *float64 `json:"FCFF"`
	FCF           *float64 `json:"FCF"`
	NWC           *float64 `json:"NWC"`
	PriceTarget   *float64 `json:"PriceTarget"`
	StrongBuy     *int     `json:"StrongBuy"`
	Buy           *int     `json:"Buy"`
	Hold          *int     `json:"Hold"`
	StrongSell    *int     `json:"StrongSell"`
	Sell          *int     `json:"Sell"`
}

type OptionExpiration struct {
	Symbol       string         `json:"Symbol"`
	PriceHistory []Candle       `json:"PriceHistory"`
	Quote        StockPriceData `json:"Quote"`
	Call         []string       `json:"Call"`
	Put          []string       `json:"Put"`
}

type Candle struct {
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int     `json:"volume"`
	Timestamp int     `json:"timestamp"`
}
type Company_Request struct {
	Symbol string `json:"symbol"`
}
type OpenPositionsMessage struct {
	PrevBalance float64          `json:"prevBalance"`
	OpenIDs     map[string]int64 `json:"openIdList"`
	TrackerIDs  []string         `json:"trackerIdList"`
}
type OptionPriceData struct {
	Symbol    string  `json:"Symbol"`
	Timestamp int64   `json:"timestamp"`
	Bid       float64 `json:"Bid"`
	Ask       float64 `json:"Ask"`
	Mark      float64 `json:"Mark"`
	Last      float64 `json:"Last"`
	High      float64 `json:"High"`
	IV        float64 `json:"IV"`
	Delta     float64 `json:"Delta"`
	Gamma     float64 `json:"Damma"`
	Theta     float64 `json:"Theta"`
	Vega      float64 `json:"Vega"`
}

// MixedQuote represents either an equity quote (with Bid/Ask Size)
// or an option quote (with Greeks and IV). Absent fields stay nil.
type MixedQuote struct {
	BidPrice  float64 `json:"Bid Price"`
	AskPrice  float64 `json:"Ask Price"`
	LastPrice float64 `json:"Last Price"`
	Mark      float64 `json:"Mark"`
	Symbol    string  `json:"Symbol"`

	// Equity-only
	BidSize *int `json:"Bid Size,omitempty"`
	AskSize *int `json:"Ask Size,omitempty"`

	// Option-only
	HighPrice *float64 `json:"High Price,omitempty"`
	IV        *float64 `json:"IV,omitempty"`
	Delta     *float64 `json:"Delta,omitempty"`
	Gamma     *float64 `json:"Gamma,omitempty"`
	Theta     *float64 `json:"Theta,omitempty"`
	Vega      *float64 `json:"Vega,omitempty"`
}

type CSVOptionData struct {
	Symbol    string  `json:"symbol"`    // Option symbol or ticker
	Timestamp int64   `json:"timestamp"` // Unix timestamp (or could be string if using formatted time)
	Mark      float64 `json:"mark"`      // Straddle price at time t (current mark)

	IV      float64 `json:"iv"`      // Current implied volatility
	DeltaIV float64 `json:"deltaIV"` // IV_t - IV_t-1
	AccelIV float64 `json:"accelIV"` // (IV_t - IV_t-1) - (IV_t-1 - IV_t-2)

	SmaIV      float64 `json:"smaIV"`      // 5-period (or N-period) simple moving average of IV
	SmaIvSpike float64 `json:"smaIvSpike"` // IV / SMA — indicates IV spike
	IVZScore   float64 `json:"ivZScore"`   // (IV - mean) / stddev — how extreme IV is

	Delta float64 `json:"delta"`
	Gamma float64 `json:"gamma"`
	Theta float64 `json:"theta"` // (Optional) Theta at t (to track time decay)
	Vega  float64 `json:"vega"`  // (Optional) Vega at t (useful if later modeling delta_IV * vega)

	FutureReturn float64 `json:"futureReturn"` // (Mark_t+N - Mark_t) / Mark_t
	FuturePrice  float64 `json:"futurePrice"`  // (Mark_t+N - Mark_t) / Mark_t

	Label          int   `json:"label"` // 1 if futureReturn > threshold (e.g., 5%), else 0
	DaysToEarnings int64 `json:"daysToEarnings"`
}

type StockPriceData struct {
	BidPrice  float64 `json:"Bid Price"`
	AskPrice  float64 `json:"Ask Price"`
	LastPrice float64 `json:"Last Price"`
	Mark      float64 `json:"Mark"`
	Symbol    string  `json:"Symbol"`
	Timestamp int64   `json:"timestamp"`

	// Equity-only
	BidSize int `json:"Bid Size,omitempty"`
	AskSize int `json:"Ask Size,omitempty"`
}

type StockDbData struct {
	Timestamp int64   `json:"timestamp"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Last      float64 `json:"last"`
	AskSize   int64   `json:"askSize"`
	BidSize   int64   `json:"bidSize"`
}

type BalanceDbData struct {
	Timestamp   int64   `json:"timestamp"`
	Balance     float64 `json:"balance"`
	RealBalance float64 `json:"realBalance"`
}

type OptionDbData struct {
	Timestamp int64   `json:"timestamp"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Last      float64 `json:"last"`
	High      float64 `json:"high"`
	IV        float64 `json:"iv"`
	Delta     float64 `json:"delta"`
	Gamma     float64 `json:"gamma"`
	Theta     float64 `json:"theta"`
	Vega      float64 `json:"vega"`
}

type AlpacaResponse struct {
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

type OptionsSymbol struct {
	Ticker         string            `json:"ticker"`
	Price          int               `json:"price"`
	Symbol         []CombinedOptions `json:"symbol"`
	ExpirationDate string            `json:"expirationDate"`
}

type CombinedStock struct {
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

type StockSymbol struct {
	Ticker string          `json:"ticker"`
	Symbol []CombinedStock `json:"symbol"`
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

type EarningsVolatility struct {
	Ticker     string `json:"ticker"`
	Volatility []struct {
		ReportDate        string  `json:"reportedDate"`
		DollarDifference  float64 `json:"dollarDifference"`
		PercentDifference float64 `json:"percentDifference"`
	} `json:"volatility"`
}

type EconomicDataResponse struct {
	Name     string `json:"name"`
	Interval string `json:"interval"`
	Unit     string `json:"unit"`
	Data     []Data `json:"data"`
}

type Data struct {
	Date  string `json:"date"`
	Value string `json:"value"`
}

type EconomicDataResult struct {
	Date      string  `json:"date"`
	FFR       string  `json:"ffr"`
	Inflation float64 `json:"inflation"`
}

type ImpliedVolatility struct {
	Volatility float64 `json:"volatility"`
}
