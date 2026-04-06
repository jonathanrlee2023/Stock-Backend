max_fiscal_lookup = {'income': {}, 'balance': {}, 'cash': {}, 'earnings': {}}
earnings_lookup = {}
latest_quote = {}
symbol_cache = {}
exchange_rate_cache = {}
last_checked_cache = {}
rate_limited = False

POPULAR_ETFS = {
    # Broad Market
    "SPY", "VOO", "IVV", "VTI", "QQQ", "DIA",
    
    # Sector Specific (SPDR Selectors)
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE",
    
    # Dividend & Value
    "SCHD", "VYM", "VIG", "VTV", "IWD",
    
    # Growth & Tech Specific
    "ARKK", "SMH", "SOXX", "IGV", "VUG",
    
    # International & Emerging
    "VEA", "VWO", "VXUS", "IEMG",
    
    # Fixed Income / Bonds
    "TLT", "BND", "AGG", "BIL", "SHY", "IEF", "LQD", "HYG"
}