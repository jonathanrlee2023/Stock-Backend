import datetime

import re

import orjson
import pytz

stock_labels = {
    '1': 'Bid Price',
    '2': 'Ask Price',
    '3': 'Last Price',
    '4': 'Bid Size',
    '5': 'Ask Size',
    '33': 'Mark'
}
file_names = []
new_data = {}
labeled_data = {}
option_ids = []

async def start_options_stream(ticker, price, day, month, year, type):
    ticker = str(ticker).strip().replace('\xa0', '')
    yy = str(year)[-2:]
    strike_int = int(float(price) * 1000)
    strike_str = str(strike_int).zfill(8)
    month_filled = month.zfill(2)
    day_filled = day.zfill(2)
    option_id = f'{yy}{month_filled}{day_filled}{type.upper()}{strike_str}'
    full_symbol = f"{ticker.ljust(6)}{option_id}"
    option_ids.append(full_symbol)
    file_names.append(full_symbol)
    new_data[full_symbol] = {}

async def start_stock_stream(streamer, ticker):
    caps_ticker = ticker.upper()
    request = streamer.level_one_equities(
        caps_ticker,
        "0,1,2,3,4,5,33",
        command="ADD"
    )

    await streamer.send_async(
        request
    )  

def receive_data(response):
    parsed = orjson.loads(response)
    # print(parsed)
    if 'data' in parsed:
        for item in parsed['data']:
            content = item.get("content", [])

            for quote in content:
                symbol = quote.get("key")
                symbol = re.sub(r'\s+', '_', symbol)
                if symbol not in new_data:
                    new_data[symbol] = {}         
                if symbol not in labeled_data:
                    labeled_data[symbol] = {}
                for key, value in quote.items():
                    if key in stock_labels:
                        labeled_data[symbol][stock_labels[key]] = value
                # Merge new data into stored state
                if symbol not in file_names:
                    file_names.append(symbol)
                new_data[symbol].update(labeled_data[symbol]) 

def parse_option(symbol):
    # Pattern: (Ticker)_(YY)(MM)(DD)(Type)(Strike)
    pattern = r"([A-Z]+)_(\d{2})(\d{2})(\d{2})([CP])(\d{8})"
    match = re.match(pattern, symbol)
    
    if match:
        ticker, year, month, day, option_type, strike_raw = match.groups()
        
        # Format the values
        full_year = f"20{year}"
        price = float(strike_raw) / 1000.0
        readable_type = "Call" if option_type == "C" else "Put"
        
        return {
            "ticker": ticker,
            "year": full_year,
            "month": month,
            "day": day,
            "type": readable_type,
            "price": price
        }
    return None

def is_weekday_business_hours_central():
    """
    Checks if the current time in the US/Central timezone is within regular business hours (8:30am-3:00pm CST, Monday-Friday).

    Returns a boolean indicating whether the current time is within business hours or not.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    central = pytz.timezone('US/Central')
    now_central = now.astimezone(central)

    if now_central.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False

    start = now_central.replace(hour=8, minute=30, second=0, microsecond=0)
    end = now_central.replace(hour=15, minute=0, second=0, microsecond=0)

    return start <= now_central <= end

def is_market_closed():
    """
    Checks if the current time in the US/Central timezone is outside regular business hours (8:30am-3:00pm CST, Monday-Friday) and returns a boolean indicating whether the market is closed or not.

    Returns a boolean indicating whether the market is closed or not.
    """
    now = datetime.datetime.now()
    weekday = now.weekday()  # Monday is 0, Friday is 4, Sunday is 6
    current_time = now.time()
    
    # 1. Friday after 7 PM (19:00)
    if weekday == 4 and current_time >= datetime.time(19, 0):
        return True
    
    # 2. Saturday (All day)
    if weekday == 5:
        return True
    
    # 3. Sunday before 7 PM (19:00)
    if weekday == 6 and current_time < datetime.time(19, 0):
        return True
    
    # Otherwise, the market is open
    return False
