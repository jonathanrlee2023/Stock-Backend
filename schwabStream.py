import json
import schwabdev
from dotenv import load_dotenv
import os
import time
from datetime import datetime, timedelta


load_dotenv()  # loads variables from .env into environment

appKey = os.getenv("appKey")
appSecret = os.getenv("appSecret")

field_labels = {
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

client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

streamer = client.stream

ticker = None
option_price = None
c_or_p = None
option_id = None
option_ids = []
new_data = {}
labeled_data = {}

def start_stream():
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
            full_option_id = f"{ticker}_{option_id}"
            option_ids.append(full_option_id)
            file_path = f"{ticker}_{option_id}.json"
            if not os.path.exists(file_path):
                with open(file_path, "w") as f:
                    json.dump({full_option_id: {}}, f)

def receive_data(response):
    parsed = json.loads(response)
    
    if 'data' in parsed:
        for item in parsed['data']:
            content = item.get("content", [])
            

            for quote in content:
                symbol = quote.get("key")
                symbol = symbol.replace('  ', '_')
                if symbol not in new_data:
                    new_data[symbol] = {}         
                if symbol not in labeled_data:
                    labeled_data[symbol] = {}           
                for key, value in quote.items():
                    if key in field_labels:
                        labeled_data[symbol][field_labels[key]] = value
                # Merge new data into stored state
                new_data[symbol].update(labeled_data[symbol])                
try:
    print("Starting stream...")
    streamer.start_auto(receiver=receive_data)
    
    start_stream()

    # Keep the script alive
    while True:
        time.sleep(60)
        for id in option_ids:
            existing_data = None
            timestamp = int(time.time())
            with open(f"{id}.json", "r") as f:
                existing_data = json.load(f)
            existing_data[id][timestamp] = new_data[id]
            with open(f'{id}.json', "w") as f:
                json.dump(existing_data, f)
except Exception as e:
    print("Stream error:", e)
finally:
    print("Cleaning up...")
    streamer.stop()
