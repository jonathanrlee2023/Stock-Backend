from datetime import datetime, timedelta
import time
import finnhub
import json

def getEarningsDate(tickers):
    api_client = finnhub.Client(api_key="d1mlkl1r01qlvnp378dgd1mlkl1r01qlvnp378e0")

    results = {}
    today = datetime.today().strftime('%Y-%m-%d')
    future = (datetime.today() + timedelta(days=90)).strftime('%Y-%m-%d')

    for symbol in tickers:
        try:
            time.sleep(1)
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