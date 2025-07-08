from datetime import datetime, timedelta
import finnhub
import json

# Setup
api_client = finnhub.Client(api_key="d1mlkl1r01qlvnp378dgd1mlkl1r01qlvnp378e0")

# Get upcoming earnings for a ticker
tickers = ["AAPL", "MSFT", "TSLA"]
results = {}
today = datetime.today().strftime('%Y-%m-%d')
future = (datetime.today() + timedelta(days=90)).strftime('%Y-%m-%d')

tickers = ["AAPL", "MSFT", "TSLA"]
results = {}

for symbol in tickers:
    try:
        response = api_client.earnings_calendar(_from=today, to=future, symbol=symbol)
        events = response.get('earningsCalendar', [])
        if events:
            results[symbol] = events[0]['date']  # just the date string
        else:
            results[symbol] = None
    except Exception as e:
        print(f"Error for {symbol}: {e}")
        results[symbol] = None

with open("earnings_dates.json", "w") as f:
    json.dump(results, f, indent=2)

print("Saved earnings dates to earnings_dates.json")