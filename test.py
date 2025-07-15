import asyncio
import datetime
import sqlite3
import time
import finnhub

def get_next_option_expiration(start_date=None, weeks_out=2):
    if not start_date:
        start_date = datetime.datetime.today()
    # Find the next Friday
    days_ahead = (4 - start_date.weekday()) % 7
    first_friday = start_date + datetime.timedelta(days=days_ahead)
    # Add weeks to get target Friday
    target_friday = first_friday + datetime.timedelta(weeks=weeks_out - 1)
    return target_friday

def format_option_id(symbol, strike, option_type="C", weeks_out=2):
    expiration = get_next_option_expiration(weeks_out=weeks_out)
    expiration_str = expiration.strftime("%y%m%d")
    
    # Format strike price: multiply by 1000 and zero pad to 8 digits
    strike_int = int(round(strike * 1000))
    strike_formatted = f"{strike_int:08d}"
    
    return f"{symbol.upper()}_{expiration_str}{option_type.upper()}{strike_formatted}"

async def write_upcoming_earnings_symbols():
    makret_cap_dict = {}

    finnhub_client = finnhub.Client(api_key="d1mlkl1r01qlvnp378dgd1mlkl1r01qlvnp378e0")

    # Get today's date
    today = datetime.today()

    # Calculate date one week from now
    one_week_out = today + datetime.timedelta(days=7)

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
        symbol = entry["symbol"]
        await asyncio.sleep(1)
        market_cap_response = (finnhub_client.company_profile2(symbol=symbol))
        market_cap = market_cap_response.get("marketCapitalization")
        makret_cap_dict[symbol] = market_cap


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
        call_id = format_option_id(symbol=symbol, strike=rounded_price, option_type="C")
        put_id = format_option_id(symbol=symbol, strike=rounded_price, option_type="P")

        print(call_id)
        print(put_id)
        # Insert both option IDs into the table
        cursor.execute(insert_table, (call_id,))
        cursor.execute(insert_table, (put_id,))

        # Commit all inserts
        conn.commit()

        # Output
        print(f"{symbol}: ${format(cap * 1_000_000, ',')} Price: {price} Rounded Price: {rounded_price}")
        await asyncio.sleep(1)