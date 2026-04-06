package utils

var GlobalPortfolio_IDs = &Portfolio_IDs{
	IDs: make(map[int]string),
}

var GlobalPrices = &LivePrices{
	Prices: make(map[string]MixedQuote),
}

var GlobalOpenPositions = &OpenPositions{
	Positions: make(map[int]map[string]OpenPositionDetails),
}

var GlobalBalance = &PortfolioBalances{
	Balances: make(map[int]*Balance),
}

var GlobalCompanyCache = &CompanyStatsCache{
	Stats: make(map[string]CompanyStats),
}

var GlobalOptionExpiration = &OptionExpirationCache{
	Stats: make(map[string]OptionExpiration),
}

var GlobalCacheLimit = &CacheLimit{
	Limit:   75,
	Queue:   []string{},
	InQueue: make(map[string]struct{}),
}