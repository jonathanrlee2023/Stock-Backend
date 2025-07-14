import json
from datetime import datetime, timedelta
import os
import time
from dotenv import load_dotenv
import finnhub

def updateEarningsDate(tickers):
    load_dotenv()  # Loads variables from .env into environment

    api_key = os.getenv("API_KEY")

    api_client = finnhub.Client(api_key=api_key)  # replace with actual key

    # Load existing data
    try:
        with open("earnings_dates.json", "r") as f:
            earnings_data = json.load(f)
    except FileNotFoundError:
        earnings_data = {}

    today = datetime.today().strftime('%Y-%m-%d')
    future = (datetime.today() + timedelta(days=90)).strftime('%Y-%m-%d')

    for symbol in tickers:
        try:
            time.sleep(1)
            response = api_client.earnings_calendar(_from=today, to=future, symbol=symbol)
            events = response.get('earningsCalendar', [])
            if events:
                earnings_data[symbol] = events[0]['date']
            else:
                earnings_data[symbol] = None
        except Exception as e:
            print(f"Error for {symbol}: {e}")
            earnings_data[symbol] = None

    # Save updated data
    with open("earnings_dates.json", "w") as f:
        json.dump(earnings_data, f, indent=2)

    print("Updated earnings_dates.json with latest data.")