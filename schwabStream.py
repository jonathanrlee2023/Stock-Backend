import json
import sqlite3
import pytz
import redis
import requests
import schwabdev
from dotenv import load_dotenv
import os
import time
import stream_func
import datetime
import asyncio
import websockets

stream_started = False
stream_lock = asyncio.Lock()
tickers = []
r = redis.Redis(host='localhost', port=6379, db=0)

async def listen_for_messages(streamer):
    global stream_started
    pubsub = r.pubsub()
    pubsub.subscribe('Start_Stream')
    try:
        if r.ping():
            print("✅ Redis Connection Successful!")
    except redis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")

    while True:
        message = pubsub.get_message(ignore_subscribe_messages=True)
        if not stream_started:
            async with stream_lock:
                if not stream_started:
                    print("Starting streamer…")
                    streamer.start(receiver=stream_func.receive_data)
                    stream_started = True
        if message:
            data = json.loads(message["data"])
            if isinstance(data, list) and data:
                first = data[0]

                # OptionStreamRequest array
                if "price" in first:
                    for opt in data:
                        symbol      = opt["symbol"]
                        price       = opt["price"]
                        day         = opt["day"]
                        month       = opt["month"]
                        year        = opt["year"]
                        option_type = opt["type"]

                        if symbol not in streamer.subscriptions.get("LEVELONE_OPTIONS", {}):
                            asyncio.create_task(
                                stream_func.start_options_stream(
                                    streamer=streamer,
                                    ticker=symbol,
                                    price=price,
                                    day=day,
                                    month=month,
                                    year=year,
                                    type=option_type,
                                )
                            )
                            # Track the underlying ticker (before the “_”)
                            base = symbol.split("_", 1)[0]
                            if base not in tickers:
                                tickers.append(base)

                    await r.publish("Stream_Channel", json.dumps({"Status": "Started"
                                                                  "Symbol: {}".format(symbol)}))

                # StockStreamRequest array
                else:
                    for stk in data:
                        symbol = stk["symbol"]

                        if symbol not in streamer.subscriptions.get("LEVELONE_EQUITIES", {}):
                            asyncio.create_task(
                                stream_func.start_stock_stream(
                                    streamer=streamer,
                                    ticker=symbol,
                                )
                            )
                            if symbol not in tickers:
                                tickers.append(symbol)

                    await r.publish("Stream_Channel", json.dumps({"Status": "Started"
                                                                  "Symbol: {}".format(symbol)}))

            else:
                # Fallback for a single‐item dict (old behavior) or unexpected shape
                symbol = data.get("symbol")
                print("Handling single request:", data)

                try:
                    if "price" in data:
                        price = data["price"]
                        day = data["day"]
                        month = data["month"]
                        year = data["year"]
                        option_type = data["type"]
                        if symbol not in streamer.subscriptions.get('LEVELONE_OPTIONS', {}):
                            asyncio.create_task(
                                stream_func.start_options_stream(
                                    streamer=streamer,
                                    ticker=symbol,
                                    price=price,
                                    day=day,
                                    month=month,
                                    year=year,
                                    type=option_type
                                )
                            )
                            stripped_ticker = symbol.split("_", 1)[0]
                            if stripped_ticker not in tickers:
                                tickers.append(stripped_ticker)
                    else:
                        if symbol not in streamer.subscriptions.get('LEVELONE_EQUITIES', {}):
                            asyncio.create_task(
                                stream_func.start_stock_stream(
                                    streamer=streamer,
                                    ticker=symbol,
                                )
                            )
                            if symbol not in tickers:
                                tickers.append(symbol)

                    await r.publish("Stream_Channel", json.dumps({"Status": "Started"
                                                                  "Symbol: {}".format(symbol)}))

                except requests.exceptions.ReadTimeout:
                    print("Timeout connecting to Schwab API.")
                except Exception as e:
                    print("Stream handling error:", e)

async def write_to_db():
    conn = sqlite3.connect(f'PriceData.db')
    cursor = conn.cursor()      

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Options (
            timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
            bid_price REAL, ask_price REAL, last_price REAL, high_price REAL,
            iv REAL, delta REAL, gamma REAL, theta REAL, vega REAL,
            PRIMARY KEY (timestamp, symbol)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Stocks (
            timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
            bid_price REAL, ask_price REAL, last_price REAL, bid_size INTEGER, ask_size INTEGER,
            PRIMARY KEY (timestamp, symbol)
        )
    """)
    conn.commit()            

    while True:
        await asyncio.sleep(60 - time.time() % 60)
        timestamp = int(time.time())
        snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names}

        option_records = []
        stock_records = []

        for name, data in snapshot.items():
            try:
                if len(name) > 8: # Option logic
                    option_records.append((
                        timestamp, name, data.get("Mark"), data.get("Bid Price"),
                        data.get("Ask Price"), data.get("Last Price"), data.get("High Price"),
                        data.get("IV"), data.get("Delta"), data.get("Gamma"),
                        data.get("Theta"), data.get("Vega")
                    ))
                else: # Stock logic
                    stock_records.append((
                        timestamp, name, data.get("Mark"), data.get("Bid Price"),
                        data.get("Ask Price"), data.get("Last Price"), 
                        data.get("Bid Size"), data.get("Ask Size")
                    ))
            except KeyError as e:
                print(f"Skipping {name}: Missing key {e}")

        if option_records:
            cursor.executemany(
                "INSERT OR REPLACE INTO Options VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", 
                option_records
            )
        
        if stock_records:
            cursor.executemany(
                "INSERT OR REPLACE INTO Stocks VALUES (?,?,?,?,?,?,?,?)", 
                stock_records
            )

        conn.commit()
        print(f"Snapshot saved at {timestamp}")

    # conn.close()  # Ideally handled via a shutdown signal
async def broadcast_to_redis():
    new_data = {
    "NVDA": {
        "Symbol": "NVDA",
        "Bid Price": 135.22,
        "Ask Price": 135.25,
        "Last Price": 135.24,
        "Bid Size": 1200,
        "Ask Size": 800,
        "Mark": 135.23
    },
    "NVDA_250221C00130000": {
        "Symbol": "NVDA_250221C00130000",
        "Bid Price": 8.45,
        "Ask Price": 8.60,
        "Last Price": 8.55,
        "High Price": 9.10,
        "IV": 42.5,
        "Delta": 0.6521,
        "Gamma": 0.012,
        "Theta": -0.045,
        "Vega": 0.12,
        "Mark": 8.52
    }
}
    try:
        if r.ping():
            print("✅ Redis Connection Successful!")
    except redis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")
    if not is_weekday_business_hours_central():
        print("Stream is closed.")
        r.publish("Stream_Channel", json.dumps(new_data))
    while True:
        await asyncio.sleep(15 - time.time() % 15)
        timestamp = int(time.time())        

        if stream_func.file_names:
            snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names}
            # Publish to Go!
            r.publish("Stream_Channel", json.dumps(snapshot))
        else:
            continue

def is_weekday_business_hours_central():
    now = datetime.datetime.now(datetime.timezone.utc)

    central = pytz.timezone('US/Central')
    now_central = now.astimezone(central)

    if now_central.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False

    start = now_central.replace(hour=8, minute=30, second=0, microsecond=0)
    end = now_central.replace(hour=15, minute=0, second=0, microsecond=0)

    return start <= now_central <= end

async def main():
    load_dotenv()  # loads variables from .env into environment

    appKey = os.getenv("appKey")
    appSecret = os.getenv("appSecret")


    client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

    streamer = client.stream

    try:
        
        # Start background tasks
        print("Starting background tasks...")
        tasks = [
            asyncio.create_task(broadcast_to_redis()),
            asyncio.create_task(listen_for_messages(streamer)),
            asyncio.create_task(write_to_db()),
        ]

        # await asyncio.sleep(10) 

        # tasks.append(asyncio.create_task(earnings.write_upcoming_earnings_symbols(tickers=tickers, client=client)))

        # try:
        #     while True:
        #         if not is_weekday_business_hours_central() or is_weekday_business_hours_central(None):
        #             print("Not in trading hours")
        #             print("Shutting down: after 3PM Central.")
        #             for task in tasks:
        #                 task.cancel()
        #             break
        #         await asyncio.sleep(300)
        # except asyncio.CancelledError:
        #     print("Tasks were cancelled.")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

asyncio.run(main())
