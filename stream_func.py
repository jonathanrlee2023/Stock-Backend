import json
import os
from filelock import FileLock

option_labels = {
    '0': 'Symbol',
    '1': 'Description',
    '2': 'Bid Price',
    '3': 'Ask Price',
    '4': 'Last Price',
    '5': 'High Price',
    '28': 'Delta',
    '29': 'Gamma',
    '30': 'Theta',
    '31': 'Vega'
}

stock_labels = {
    '0': 'Symbol',
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

def start_options_stream(streamer):
    global ticker
    while True:
        ticker = input("Enter ticker you want to see options for or 'Done' if you are done: ")
        if ticker == 'Done':
            break
        while True:
            c_or_p = input("Enter 'C' for call and 'P' for put or 'Done' if you are done: ")
            if c_or_p == 'Done':
                break
            option_price = input('Enter option price: ')
            strike_int = int(float(option_price) * 1000)
            strike_str = str(strike_int).zfill(8)
            day = input('Enter day of expiration: ')
            month = input('Enter month of expiration: ')
            year = input('Enter last two digits of year of expiration: ')
            month_filled = month.zfill(2)
            option_id = f'{year}{month_filled}{day}{c_or_p.upper()}{strike_str}'
            streamer.send(streamer.level_one_options(f"{ticker}  {option_id}", "1,2,3,4,5,28,29,30,31", command='ADD'))
            file_path = f"{ticker}_{option_id}.json"
            file_names.append(file_path)
            lock = FileLock(f'{file_path}.lock')
            with lock:
                if not os.path.exists(file_path):
                    with open(file_path, "w") as f:
                        json.dump({}, f)

def start_stock_stream(streamer):
    global ticker
    while True:
        ticker = input("Enter ticker or 'Done' if you are done: ")
        if ticker == 'Done':
            break
        caps_ticker = ticker.upper()
        streamer.send(streamer.level_one_equities(f"{caps_ticker}", "1,2,3,4,5", command='ADD'))
        file_path = f"{caps_ticker}.json"
        file_names.append(file_path)
        lock = FileLock(f'{file_path}.lock')
        with lock:
            if not os.path.exists(file_path):
                with open(file_path, "w") as f:
                    json.dump({}, f)

def receive_data(response):
    parsed = json.loads(response)
    
    if 'data' in parsed:
        for item in parsed['data']:
            content = item.get("content", [])
            

            for quote in content:
                symbol = quote.get("key")
                if '  ' in symbol:
                    symbol = symbol.replace('  ', '_')
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
                new_data[symbol].update(labeled_data[symbol]) 