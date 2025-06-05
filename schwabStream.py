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
old_data = {}
new_data = {}


def receive_data(response):
    parsed = json.loads(response)
    
    if 'data' in parsed:
        for item in parsed['data']:
            content = item.get("content", [])

            for quote in content:
                symbol = quote.get("key")
                symbol = symbol.replace('  ', '_')
                with open(f"{symbol}.json", "r") as f:
                    old_data = json.load(f)
                if symbol not in old_data:
                    old_data[symbol] = []                    

                # Merge new data into stored state
                new_data.update(quote)

                # Print full updated state for the symbol
                print(f"\n📈 {symbol} snapshot:")
                for field, value in new_data[symbol].items():
                    if field != "key":
                        print(f"  Field {field}: {value}")
                timestamp = int(time.time())
                if timestamp % 60 == 0:
                    old_data[symbol].append(new_data)


    elif 'response' in parsed:
        print("\n System/Response Message:")
        print(parsed['response'])

    else:
        print("\n Unknown Message Type:")
        print(parsed)
try:
    print("Starting stream...")
    streamer.start_auto(receiver=receive_data)
    print("Sending Level One quote request for AAPL")
    while ticker != 'Done':
        ticker = input('Enter ticker you want to see options for:')
        while True:
            c_or_p = input("Enter 'C' for call and 'P' for put or 'Done' if you are done")
            if c_or_p == 'Done':
                break
            option_price = input('Enter option price')
            strike_int = int(float(option_price) * 1000)
            strike_str = str(strike_int).zfill(8)
            option_id = f'250613{c_or_p.upper()}{strike_str}'
            streamer.send(streamer.level_one_options(f"{ticker}  250613{c_or_p.upper()}{strike_str}", "1,2,3,4,5,28,29,30,31", command='ADD'))
            file_path = f"{ticker}_{option_id}.json"
            if not os.path.exists(file_path):
                with open(file_path, "w") as f:
                    json.dump({}, f)


    # Keep the script alive
    while True:
        time.sleep(10)
except Exception as e:
    print("Stream error:", e)
finally:
    print("Cleaning up...")
    streamer.stop()