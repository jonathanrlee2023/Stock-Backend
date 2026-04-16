package utils

import (
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn      *websocket.Conn
	ID        string // unique identifier for this client (e.g., user ID, session ID)
	Mu        sync.Mutex
	Done      chan struct{}
	send      chan []byte
	once      sync.Once
	UserID    int
	Balance   PortfolioBalances
	OpenPositions OpenPositions
	PortfolioIDs Portfolio_IDs
	IsWriting bool
}

type SubscriptionHub struct {
    Topics map[string][]*Client
    sync.RWMutex
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.Done)
	})
}
func (c *Client) EnqueueMessage(payload []byte) {
    select {
    case c.send <- payload:
    default:
        log.Printf("Client %s buffer full, dropping message", c.ID)
    }
}

func (c *Client) WritePump() {
    defer func() {
        c.Conn.Close()
    }()

    for {
        select {
        case message, ok := <-c.send:
            if !ok {
                c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }

            err := c.Conn.WriteMessage(websocket.TextMessage, message)
            if err != nil {
                log.Printf("WS write error for %s: %v", c.ID, err)
                return
            }
        case <-c.Done:
            return
        }
    }
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

type CompanyStats struct {
	Symbol           string   `json:"Symbol"`
	MarketCap        float64  `json:"MarketCap"`
	PEG              *float64 `json:"PEG"`
	Sloan            *float64 `json:"Sloan"`
	ROIC             *float64 `json:"ROIC"`
	HistGrowth       *float64 `json:"HistGrowth"`
	ForecastedGrowth *float64 `json:"ForecastedGrowth"`
	TrailingPEG    *float64 `json:"TrailingPEG"`
	ForwardPEG     *float64 `json:"ForwardPEG"`
	IntrinsicPrice *float64 `json:"IntrinsicPrice"`
	DividendPrice     *float64             `json:"DividendPrice"`
	PriceAtReport     *float64             `json:"PriceAtReport"`
	WACC              *float64             `json:"WACC"`
	FCFF              *float64             `json:"FCFF"`
	FCF               *float64             `json:"FCF"`
	FCFPerShare       *float64             `json:"FCFPerShare"`
	NWC               *float64             `json:"NWC"`
	PriceTarget       *float64             `json:"PriceTarget"`
	StrongBuy         *int                 `json:"StrongBuy"`
	Buy               *int                 `json:"Buy"`
	Hold              *int                 `json:"Hold"`
	StrongSell        *int                 `json:"StrongSell"`
	Sell              *int                 `json:"Sell"`
	EarningsDate      *string              `json:"EarningsDate"`
	Grade             *int                 `json:"Grade"`
	Sector            *string              `json:"Sector"`
	Industry          *string              `json:"Industry"`
	AnnualIncome      *[]IncomeStatement   `json:"AnnualIncome"`
	AnnualBalance     *[]BalanceSheet      `json:"AnnualBalance"`
	AnnualCash        *[]CashFlowStatement `json:"AnnualCash"`
	AnnualEarnings    *[]EarningsReport    `json:"AnnualEarnings"`
	QuarterlyIncome   *[]IncomeStatement   `json:"QuarterlyIncome"`
	QuarterlyBalance  *[]BalanceSheet      `json:"QuarterlyBalance"`
	QuarterlyCash     *[]CashFlowStatement `json:"QuarterlyCash"`
	QuarterlyEarnings *[]EarningsReport    `json:"QuarterlyEarnings"`
}

type OpenPositionDetails struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
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
	PrevBalance map[int]float64            `json:"prevBalance"`
	OpenIDs     map[int]map[string]float64 `json:"openIdList"`
	TrackerIDs  []string           `json:"trackerIdList"`
	PortfolioNames map[int]string `json:"portfolioNames"`
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
	Gamma     float64 `json:"Gamma"`
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

type BalanceData struct {
	Timestamp int64   `json:"timestamp"`
	Balance   float64 `json:"Balance"`
	Cash      float64 `json:"Cash"`
	PortfolioID int    `json:"PortfolioID"`
}

type EarningsVolatility struct {
	Ticker     string `json:"ticker"`
	Volatility []struct {
		ReportDate        string  `json:"reportedDate"`
		DollarDifference  float64 `json:"dollarDifference"`
		PercentDifference float64 `json:"percentDifference"`
	} `json:"volatility"`
}

type BalanceSheet struct {
	Date                   string   `json:"date"`
	ReportedCurrency       string   `json:"reportedCurrency"`
	TotalAssets            *float64 `json:"totalAssets"`
	TotalCurrentAssets     *float64 `json:"totalCurrentAssets"`
	CashAndCashEquivalents *float64 `json:"cashAndCashEquivalents"`
	CashAndShortTermInvest *float64 `json:"cashAndShortTermInvestments"`
	Inventory              *float64 `json:"inventory"`
	CurrentNetReceivables  *float64 `json:"currentNetReceivables"`
	TotalNonCurrentAssets  *float64 `json:"totalNonCurrentAssets"`
	PropertyPlantEquip     *float64 `json:"propertyPlantEquipment"`
	AccumulatedDeprec      *float64 `json:"accumulatedDepreciation"`
	IntangibleAssets       *float64 `json:"intangibleAssets"`
	IntangibleAssetsExcl   *float64 `json:"intangibleAssetsExcludingGoodwill"`
	Goodwill               *float64 `json:"goodwill"`
	Investments            *float64 `json:"investments"`
	LongTermInvestments    *float64 `json:"longTermInvestments"`
	ShortTermInvestments   *float64 `json:"shortTermInvestments"`
	OtherCurrentAssets     *float64 `json:"otherCurrentAssets"`
	OtherNonCurrentAssets  *float64 `json:"otherNonCurrentAssets"`
	TotalLiabilities       *float64 `json:"totalLiabilities"`
	TotalCurrentLiabil     *float64 `json:"totalCurrentLiabilities"`
	CurrentAccountsPayable *float64 `json:"currentAccountsPayable"`
	DeferredRevenue        *float64 `json:"deferredRevenue"`
	CurrentDebt            *float64 `json:"currentDebt"`
	ShortTermDebt          *float64 `json:"shortTermDebt"`
	TotalNonCurrentLiabil  *float64 `json:"totalNonCurrentLiabilities"`
	CapitalLeaseOblig      *float64 `json:"capitalLeaseObligations"`
	LongTermDebt           *float64 `json:"longTermDebt"`
	CurrentLongTermDebt    *float64 `json:"currentLongTermDebt"`
	LongTermDebtNonCurrent *float64 `json:"longTermDebtNonCurrent"`
	ShortLongTermDebt      *float64 `json:"shortLongTermDebt"`
	OtherCurrentLiabil     *float64 `json:"otherCurrentLiabilities"`
	OtherNonCurrentLiabil  *float64 `json:"otherNonCurrentLiabilities"`
	TotalShareholderEquity *float64 `json:"totalShareholderEquity"`
	TreasuryStock          *float64 `json:"treasuryStock"`
	RetainedEarnings       *float64 `json:"retainedEarnings"`
	CommonStock            *float64 `json:"commonStock"`
	CommonStockShares      *float64 `json:"commonStockSharesOutstanding"`

	Ticker     string `json:"ticker"`
	ReportType string `json:"report_type"`

	NWC      *float64 `json:"NWC"`
	DeltaNWC *float64 `json:"deltaNWC"`
	NWCRatio *float64 `json:"nwcRatio"`
	SymbolID float64  `json:"symbol_id"`
}

type IncomeStatement struct {
	Date             time.Time `json:"date"`
	ReportedCurrency string    `json:"reportedCurrency"`
	Ticker           string    `json:"ticker"`
	ReportType       string    `json:"report_type"`
	SymbolID         float64   `json:"symbol_id"`

	GrossProfit            *float64 `json:"grossProfit"`
	TotalRevenue           *float64 `json:"totalRevenue"`
	CostOfRevenue          *float64 `json:"costOfRevenue"`
	CostOfGoodsAndServicesSold *float64 `json:"costOfGoodsAndServicesSold"`
	OperatingIncome        *float64 `json:"operatingIncome"`
	SellingGeneralAdmin    *float64 `json:"sellingGeneralAndAdministrative"`
	ResearchAndDev         *float64 `json:"researchAndDevelopment"`
	OperatingExpenses      *float64 `json:"operatingExpenses"`
	InvestmentIncome       *float64 `json:"investmentIncome"`
	NetInterestIncome      *float64 `json:"netInterestIncome"`
	InterestIncome         *float64 `json:"interestIncome"`
	InterestExpense        *float64 `json:"interestExpense"`
	NonInterestIncome      *float64 `json:"nonInterestIncome"`
	OtherNonOperatingInc   *float64 `json:"otherNonOperatingIncome"`
	Depreciation           *float64 `json:"depreciation"`
	DepreciationAndAmort   *float64 `json:"depreciationAndAmortization"`
	IncomeBeforeTax        *float64 `json:"incomeBeforeTax"`
	IncomeTaxExpense       *float64 `json:"incomeTaxExpense"`
	InterestAndDebtExpense *float64 `json:"interestAndDebtExpense"`
	NetIncomeFromContOps   *float64 `json:"netIncomeFromContinuingOperations"`
	ComprehensiveIncome    *float64 `json:"comprehensiveIncome"`
	Ebit                   *float64 `json:"ebit"`
	Ebitda                 *float64 `json:"ebitda"`
	NetIncome              *float64 `json:"netIncome"`

	EffectiveTaxRate *float64 `json:"effectiveTaxRate"`
	RevGrowth        *float64 `json:"revGrowth"`
	EbitMargin       *float64 `json:"ebitMargin"`
	CapexPctRevenue  *float64 `json:"capexPctRevenue"`
	NwcPctRevenue    *float64 `json:"nwcPctRevenue"`
	DaPctRevenue     *float64 `json:"daPctRevenue"`
	EbitGrowth       *float64 `json:"ebitGrowth"`
	Roic             *float64 `json:"roic"`
}

type CashFlowStatement struct {
	Date             time.Time `json:"date"`
	ReportedCurrency string    `json:"reportedCurrency"`
	Ticker           string    `json:"ticker"`
	ReportType       string    `json:"report_type"`
	SymbolID         float64   `json:"symbol_id"`

	OperatingCashflow       *float64 `json:"operatingCashflow"`
	PaymentsForOperating    *float64 `json:"paymentsForOperatingActivities"`
	ProceedsFromOperating   *float64 `json:"proceedsFromOperatingActivities"`
	ChangeInOperatingAssets *float64 `json:"changeInOperatingAssets"`
	ChangeInOperatingLiab   *float64 `json:"changeInOperatingLiabilities"`
	DepreciationDepletion   *float64 `json:"depreciationDepletionAndAmortization"`
	ChangeInReceivables     *float64 `json:"changeInReceivables"`
	ChangeInInventory       *float64 `json:"changeInInventory"`
	StockBasedCompensation  *float64 `json:"stockBasedCompensation"`

	CapitalExpenditures     *float64 `json:"capitalExpenditures"`
	CashflowFromInvesting   *float64 `json:"cashflowFromInvestment"`
	CashflowFromFinancing   *float64 `json:"cashflowFromFinancing"`
	DividendPayout          *float64 `json:"dividendPayout"`
	DividendPayoutCommon    *float64 `json:"dividendPayoutCommonStock"`
	DividendPayoutPreferred *float64 `json:"dividendPayoutPreferredStock"`

	ProceedsFromRepurchase *float64 `json:"proceedsFromRepurchaseOfEquity"`
	PaymentsForRepurchase  *float64 `json:"paymentsForRepurchaseOfEquity"`
	ProceedsFromIssuance   *float64 `json:"proceedsFromIssuanceOfDebt"` // Note: often multiple issuance fields in APIs

	NetIncome            *float64 `json:"netIncome"`
	ProfitLoss           *float64 `json:"profitLoss"`
	ChangeInCashAndEquiv *float64 `json:"changeInCashAndCashEquivalents"`
	ChangeInExchangeRate *float64 `json:"changeInExchangeRate"`

	FCF          *float64 `json:"FCF"`
	FCFYoYGrowth *float64 `json:"FCF_yoy_growth"`
	FCFPerShare  *float64 `json:"FCF_per_share"`
	FCFF         *float64 `json:"FCFF"` // Free Cash Flow to Firm
}

type EarningsReport struct {
	Date         time.Time `json:"date"`
	Ticker       string    `json:"ticker"`
	ReportType   string    `json:"report_type"` // e.g., "quarterly"
	SymbolID     float64   `json:"symbol_id"`
	ReportedDate string    `json:"reportedDate"`
	ReportTime   string    `json:"reportTime"` // e.g., "before_market", "after_market"

	// EPS Data
	ReportedEPS  *float64 `json:"reportedEPS"`
	EstimatedEPS *float64 `json:"estimatedEPS"`

	// Surprise Data
	Surprise           *float64 `json:"surprise"`
	SurprisePercentage *float64 `json:"surprisePercentage"`
}

type PostData struct {
	FileNames []string `json:"filenames"`
}

type Tracker struct {
	ID string `json:"id"`
}
type Position struct {
	ID     string  `json:"id"`
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
	PortfolioID int `json:"portfolio_id"`
	ClientID string `json:"client_id"`
}

type Portfolio struct {
	ID int `json:"id"`
	Name string `json:"name"`
	Positions []Position `json:"positions"`
}

type LivePrices struct {
	sync.RWMutex
	Prices map[string]MixedQuote
}

type OpenPositions struct {
	sync.RWMutex
	Positions map[int]map[string]OpenPositionDetails
}

type Balance struct {
	Balance float64
	Cash    float64
}

type PortfolioBalances struct {
	sync.RWMutex
	Balances map[int]*Balance
}

type Portfolio_IDs struct {
	sync.RWMutex
	IDs map[int]string
}

type CompanyStatsCache struct {
	sync.RWMutex
	Stats map[string]CompanyStats
}

type OptionExpirationCache struct {
	sync.RWMutex
	Stats map[string]OptionExpiration
}

type CacheLimit struct {
	sync.RWMutex
	Limit int
	Queue []string
	InQueue map[string]struct{}
}

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type UserID struct {
	ClientID string
	ID int
}

type DatabasePool struct {
	BalanceDB *sql.DB
	OpenDB    *sql.DB
	CloseDB   *sql.DB
	TrackerDB *sql.DB
}