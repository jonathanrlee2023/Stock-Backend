import json
import sqlite3
import aiosqlite
import pytz
from redis import asyncio as aioredis
import requests
import schwabdev
from dotenv import load_dotenv
import os
import time
from companyClass import Company
import stream_func
import datetime
import asyncio
import websockets
from stream_func import parse_option

tasks_started = False
stream_started = False
stream_lock = asyncio.Lock()
tickers = []
r = aioredis.Redis(host='localhost', port=6380, db=0)

async def listen_for_messages(streamer):
    global stream_started
    print("Listening for messages…")
    pubsub = r.pubsub()
    await pubsub.subscribe('Start_Stream')
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")

    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True)
        if not stream_started:
            async with stream_lock:
                if not stream_started:
                    print("Starting streamer…")
                    streamer.start(receiver=stream_func.receive_data)
                    stream_started = True
        if message is not None:
            data = json.loads(message["data"])
            
            # 🛑 FILTER: If this is just a confirmation message, IGNORE IT
            if "Status" in data:
                continue
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

                    await r.publish("Start_Stream", json.dumps({"Status": "Started",
                                                                  "Symbol": symbol}))

                # StockStreamRequest array
                else:
                    symbol = data.get("symbol")
                    if not symbol:
                        print("Skipping malformed or empty request:", data)
                        continue
                    
                    print("Handling single request:", data)
                    # ... (rest of your single request logic)
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

                    # Added a comma after "Started" and made "Symbol" its own key
                    await r.publish("Start_Stream", json.dumps({"Status": "Started",
                                                                "Symbol": symbol}))

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

                    await r.publish("Start_Stream", json.dumps({"Status": "Started",
                                                                  "Symbol": symbol}))

                except requests.exceptions.ReadTimeout:
                    print("Timeout connecting to Schwab API.")
                except Exception as e:
                    print("Stream handling error:", e)

async def write_to_db():
    async with aiosqlite.connect('PriceData.db') as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS Options (
                timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
                bid_price REAL, ask_price REAL, last_price REAL, high_price REAL,
                iv REAL, delta REAL, gamma REAL, theta REAL, vega REAL,
                PRIMARY KEY (timestamp, symbol)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS Stocks (
                timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
                bid_price REAL, ask_price REAL, last_price REAL, bid_size INTEGER, ask_size INTEGER,
                PRIMARY KEY (timestamp, symbol)
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_options_symbol ON Options (symbol, timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON Stocks (symbol, timestamp)")
        await db.commit()

        while True:
            wait_time = 15 - (time.time() % 15)
            if wait_time < 0.1: wait_time = 15
            await asyncio.sleep(wait_time)
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
                await db.executemany(
                    "INSERT OR REPLACE INTO Options VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", 
                    option_records
                )
            
            if stock_records:
                await db.executemany(
                    "INSERT OR REPLACE INTO Stocks VALUES (?,?,?,?,?,?,?,?)", 
                    stock_records
                )

            await db.commit()
            print("Wrote to DB")
    
    
async def broadcast_to_redis():
    print("Called")
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")
    if not is_weekday_business_hours_central():
        print("Stream is closed.")
    while True:
        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1: wait_time = 15
        await asyncio.sleep(wait_time)

        if stream_func.file_names:
            snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names}
            await r.publish("Stream_Channel", json.dumps(snapshot))

async def update_tickers_from_db(streamer):
    async with aiosqlite.connect("Tracker.db") as db:
        async with db.execute("SELECT id FROM tracker") as cursor:
            rows = await cursor.fetchall()
            ticker_list = [row[0] for row in rows]
            
    if ticker_list:
        for ticker in ticker_list:
            if len(ticker) > 8: # Option logic
                parsed = parse_option(ticker)
                if parsed:
                    asyncio.create_task(
                                stream_func.start_options_stream(
                                    streamer=streamer,
                                    ticker=parsed["ticker"],
                                    price=parsed["price"],
                                    day=parsed["day"],
                                    month=parsed["month"],
                                    year=parsed["year"],
                                    type=parsed["type"],
                                )
                            )
            else: # Stock logic
                asyncio.create_task(
                            stream_func.start_stock_stream(
                                streamer=streamer,
                                ticker=ticker
                            )
                        )

        print(f"Subscribing to: {ticker_list}")
        
    else:
        print("No tickers found in tracker.db")

async def handleCompany(streamer):
    pubsub = r.pubsub()
    await pubsub.subscribe('Company_Channel')
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True)
        if message is not None:
            data = json.loads(message["data"])
            company = Company(ticker=data["ticker"], streamer=streamer)
            await r.publish("Start_Stream", json.dumps(company.final_report))
            


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
    global tasks_started
    if tasks_started:
        return
    tasks_started = True
    load_dotenv()
    appKey = os.getenv("appKey")
    appSecret = os.getenv("appSecret")

    client = schwabdev.Client(app_key=appKey, app_secret=appSecret)
    streamer = client.stream

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(update_tickers_from_db(streamer)),
        asyncio.create_task(broadcast_to_redis()),
        asyncio.create_task(listen_for_messages(streamer)),
        asyncio.create_task(write_to_db()),
    ]

    # CRITICAL: This keeps the script alive and running all tasks
    await asyncio.gather(*tasks)

asyncio.run(main())
