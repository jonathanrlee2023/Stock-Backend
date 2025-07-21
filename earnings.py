import asyncio
import datetime
import json
import os
import sqlite3
import time
from dotenv import load_dotenv
import finnhub

def get_next_option_expiration_on_or_after(client, symbol, start_date=None, weeks_out=2):
    """
    Returns the option expiration dict whose expirationDate is the earliest date
    at or after the Friday that is `weeks_out` weeks from `start_date`.
    """
    # 1. Normalize start_date
    if start_date is None:
        start_date = datetime.date.today()
    elif isinstance(start_date, datetime.datetime):
        start_date = start_date.date()

    # 2. Fetch and parse JSON
    response = client.option_expiration_chain(symbol)
    response.raise_for_status()
    data = response.json()

    # 3. Compute target Friday
    days_ahead = (4 - start_date.weekday()) % 7
    first_friday = start_date + datetime.timedelta(days=days_ahead)
    target_friday = first_friday + datetime.timedelta(weeks=weeks_out - 1)

    # 4. Build list of (date, entry) tuples
    entries = []
    for entry in data.get("expirationList", []):
        dt = datetime.datetime.strptime(entry["expirationDate"], "%Y-%m-%d").date()
        entries.append((dt, entry))

    # 5. Filter to dates >= target_friday
    future = [(dt, ent) for dt, ent in entries if dt >= target_friday]

    # 6. If none found, optionally fallback or return None
    if not future:
        return None

    # 7. Pick the earliest date in future
    closest_date, closest_entry = min(future, key=lambda pair: pair[0])

    print(closest_entry)

    return closest_entry


def format_option_id(symbol, strike, client, option_type="C", weeks_out=2):
    # 1. Get the dict entry
    expiration_entry = get_next_option_expiration_on_or_after(
        client, symbol=symbol, weeks_out=weeks_out
    )

    if not expiration_entry:
        raise ValueError("No expiration found on or after target Friday")

    # 2. Pull out the date string and parse it
    date_str = expiration_entry["expirationDate"]
    expiration_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    # 3. Format YYMMDD
    expiration_str = expiration_date.strftime("%y%m%d")

    # 4. Build the option ID
    strike_int      = int(round(strike * 1000))
    strike_formatted = f"{strike_int:08d}"

    return f"{symbol.upper()}_{expiration_str}{option_type.upper()}{strike_formatted}"


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

    top_10 = dict(sorted(makret_cap_dict.items(), key=lambda item: item[1] or 0, reverse=True)[:5])

    for symbol, cap in top_10.items():
        conn = sqlite3.connect('Tracker.db')
        cursor = conn.cursor()

        # Insert the stock symbol first
        insert_table = 'INSERT OR REPLACE INTO Tracker (id) VALUES (?)'
        cursor.execute(insert_table, (symbol,))

        # Get current stock price and round it appropriately
        quote = finnhub_client.quote(symbol=symbol)
        price = quote.get("c")
        rounded_price = round(price) if price < 100 else round(price / 5) * 5

        # Format the call and put option IDs
        call_id = format_option_id(symbol=symbol, strike=rounded_price, client=client, option_type="C" )
        put_id = format_option_id(symbol=symbol, strike=rounded_price, client=client, option_type="P")

        # Insert both option IDs into the table
        cursor.execute(insert_table, (call_id,))
        cursor.execute(insert_table, (put_id,))

        # Commit all inserts
        conn.commit()

        # Output
        print(f"{symbol}: ${format(cap * 1_000_000, ',')} Price: {price} Rounded Price: {rounded_price}")
        await asyncio.sleep(1)