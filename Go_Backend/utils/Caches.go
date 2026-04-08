package utils

var GlobalPrices = &LivePrices{
	Prices: make(map[string]MixedQuote),
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

var GlobalUserID = &UserID{
	ID: 0,
}