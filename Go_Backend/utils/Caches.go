package utils

var GlobalPrices = &LivePrices{
	Prices: make(map[string]MixedQuote),
}

var GlobalCompanyCache = &CompanyStatsCache{
	Stats: make(map[string]CompanyStats),
}

var GlobalOptionExpiration = &OptionExpirationCache{
	Stats: make(map[string]InitialCompanyData),
}

var GlobalCacheLimit = &CacheLimit{
	Limit:   75,
	Queue:   []string{},
	InQueue: make(map[string]struct{}),
}

var GlobalSubscriptionHub = &SubscriptionHub{
	Topics: make(map[string][]*Client),
}

var GlobalMarketNews = &GlobalNews{
	GlobalNews: make(map[string]string),
}

var GlobalDatabasePool = &DatabasePool{
	BalanceDB: nil,
	OpenDB:    nil,
	CloseDB:   nil,
	TrackerDB: nil,
}