import asyncio
import datetime
import json
import os
import re
import sqlite3
import time
from dotenv import load_dotenv
import finnhub

def next_friday_after_two_weeks():
    # 1. Start from today and jump ahead two weeks
    today = datetime.datetime.now().date()
    target = today + datetime.timedelta(weeks=2)

    # 2. Compute days until the next Friday (weekday() → Monday=0 … Sunday=6; Friday=4)
    days_until_friday = (4 - target.weekday() + 7) % 7

    # 3. If target is already Friday, days_until_friday == 0 → stays the same
    next_friday = target + datetime.timedelta(days=days_until_friday)

    # 4. Format as ‘YYYY-MM-DD’
    return next_friday.strftime("%Y-%m-%d")




async def updateEarningsDate(client, tickers):
    # Load existing data
    earnings_data = None
    try:
        with open("earnings_dates.json", "r") as f:
            earnings_data = json.load(f)
    except FileNotFoundError:
        earnings_data = {}
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    future = (datetime.datetime.today() + datetime.timedelta(days=90)).strftime('%Y-%m-%d')

    for symbol in tickers:
        try:
            await asyncio.sleep(1)            
            response = client.earnings_calendar(_from=today, to=future, symbol=symbol)
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

async def write_upcoming_earnings_symbols(tickers, client):
    print("Entered")
    load_dotenv()  # Loads variables from .env into environment

    api_key = os.getenv("API_KEY")
    finnhub_client = finnhub.Client(api_key=api_key)

    await updateEarningsDate(client=finnhub_client, tickers=tickers)
    makret_cap_dict = {}


    # Get today's date
    today = datetime.datetime.today()

    # Calculate date one week from now
    one_week_out = today + datetime.timedelta(days=14)

    # Format as YYYY-MM-DD
    _from = one_week_out.strftime('%Y-%m-%d')
    _to = _from  # same day if you want just one day of earnings

    response = finnhub_client.earnings_calendar(
        _from=_from,
        to=_to,
        symbol="",  # Leave empty for all symbols
        international=False
    )

    # The response is already a dict
    # You can directly parse or print it
    calendar = response.get("earningsCalendar", [])


    for entry in calendar:
        try:
            symbol = entry["symbol"]
            print(symbol)
            await asyncio.sleep(1)
            market_cap_response = (finnhub_client.company_profile2(symbol=symbol))
            market_cap = market_cap_response.get("marketCapitalization")
            makret_cap_dict[symbol] = market_cap
        except Exception as e:
            print(e)

    top_5 = dict(sorted(makret_cap_dict.items(), key=lambda item: item[1] or 0, reverse=True)[:5])

    for symbol, cap in top_5.items():
        if cap < 10000000000:
            continue
        conn = sqlite3.connect('Tracker.db')
        cursor = conn.cursor()

        # Insert the stock symbol first
        insert_table = 'INSERT OR REPLACE INTO Tracker (id) VALUES (?)'
        cursor.execute(insert_table, (symbol,))

        next_friday = next_friday_after_two_weeks()

        response = client.option_chains(symbol=symbol, strikeCount=1, fromDate=next_friday, toDate=next_friday).json()

        symbols = []

        # Loop over call and put maps
        for exp_map_key in ("callExpDateMap", "putExpDateMap"):
            exp_map = response.get(exp_map_key, {})
            for expiry_bucket in exp_map.values():
                for strike_level in expiry_bucket.values():
                    for contract in strike_level:
                        symbol = re.sub(r" +", "_", contract["symbol"])
                        symbols.append(symbol)

        print(symbols)

        # Insert both option IDs into the table
        for symbol in symbols:
            cursor.execute(insert_table, (symbol,))
            print(symbol)

        # Commit all inserts
        conn.commit()

        # Output
        await asyncio.sleep(1)