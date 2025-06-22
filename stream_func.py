import json
import os
import re
from filelock import FileLock

option_labels = {
    '1': 'Description',
    '2': 'Bid Price',
    '3': 'Ask Price',
    '4': 'Last Price',
    '5': 'High Price',
    '10': 'IV',
    '28': 'Delta',
    '29': 'Gamma',
    '30': 'Theta',
    '31': 'Vega'
}

stock_labels = {
    '1': 'Bid Price',
    '2': 'Ask Price',
    '3': 'Last Price',
    '4': 'Bid Size',
    '5': 'Ask Size'
}
ticker = None
option_price = None
c_or_p = None
option_id = None
file_names = []
new_data = {}
labeled_data = {}

async def start_options_stream(streamer, ticker, price, day, month, year, type):
    strike_int = int(float(price) * 1000)
    strike_str = str(strike_int).zfill(8)
    month_filled = month.zfill(2)
    option_id = f'{year}{month_filled}{day}{type.upper()}{strike_str}'
    request = streamer.level_one_options(
        f"{ticker.ljust(6)}{option_id}",
        "1,2,3,4,5,10,28,29,30,31",
        command='ADD'
    )

    await streamer.send_async(request)    

async def start_stock_stream(streamer, ticker):
    caps_ticker = ticker.upper()
    request = streamer.level_one_equities(
        caps_ticker,
        "0,1,2,3,4,5",
        command="ADD"
    )
    print(request)

    await streamer.send_async(
        request
    )  

def receive_data(response):
    parsed = json.loads(response)
    print(parsed)
    if 'data' in parsed:
        for item in parsed['data']:
            content = item.get("content", [])

            for quote in content:
                symbol = quote.get("key")
                print(symbol)
                if '  ' in symbol:
                    symbol = re.sub(r'\s+', '_', symbol)
                if symbol not in new_data:
                    new_data[symbol] = {}         
                if symbol not in labeled_data:
                    labeled_data[symbol] = {}
                if len(symbol) > 5:           
                    for key, value in quote.items():
                        if key in option_labels:
                            labeled_data[symbol][option_labels[key]] = value
                else:
                    for key, value in quote.items():
                        if key in stock_labels:
                            labeled_data[symbol][stock_labels[key]] = value
                # Merge new data into stored state
                if symbol not in file_names:
                    file_names.append(symbol)
                    print(f'{symbol}.db')
                new_data[symbol].update(labeled_data[symbol]) 