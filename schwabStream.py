import json
import sqlite3
import requests
import schwabdev
from dotenv import load_dotenv
import os
import time
import stream_func
import sys
from datetime import datetime, timedelta
from filelock import FileLock
import asyncio
import websockets
import traceback

stream_started = False
stream_lock = asyncio.Lock()

async def listen_for_messages(websocket, streamer):
    global stream_started

    async for message in websocket:
        print("Received message")
        data = json.loads(message)
        symbol = data["symbol"]

        if len(data) > 1:
            price = data["price"]
            day = data["day"]
            month = data["month"]
            year = data["year"]
            option_type = data["type"]

        print(f"Handling stream request: {data}")

        async with stream_lock:
            if not stream_started:
                print("Starting streamer...")
                streamer.start(receiver=stream_func.receive_data)
                stream_started = True

        await websocket.send(f"Streaming started for {symbol}")
        try:
            if len(data) > 1:
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
            elif len(data) == 1:
                asyncio.create_task(
                    stream_func.start_stock_stream(
                        streamer=streamer,
                        ticker=symbol,
                    )
                )
        except requests.exceptions.ReadTimeout:
            print("Timeout while connecting to Schwab API.")
        except Exception as e:
            print("Stream handling error:", e)

async def write_to_db(websocket):
    while True:
        await asyncio.sleep(30 - time.time() % 30)
        timestamp = int(time.time())        
        
        if len(stream_func.file_names) != 0:
            for name in stream_func.file_names:
                conn = sqlite3.connect(f'{name}.db')
                cursor = conn.cursor()
                timestamp = int(time.time())
                data = stream_func.new_data[name]
                print(name)
                if len(name) > 8:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS prices (
                            timestamp INTEGER PRIMARY KEY,
                            bid_price FLOAT NOT NULL,
                            ask_price FLOAT NOT NULL,
                            last_price FLOAT NOT NULL,
                            high_price FLOAT NOT NULL,
                            delta FLOAT NOT NULL,
                            gamma FLOAT NOT NULL,
                            theta FLOAT NOT NULL,
                            vega FLOAT NOT NULL
                    )
                    """)
                    cursor.execute("""
                        INSERT INTO prices (
                            timestamp, bid_price, ask_price, last_price, high_price, delta, gamma, theta, vega
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        timestamp,
                        data["Bid Price"],
                        data["Ask Price"],
                        data["Last Price"],
                        data["High Price"],
                        data["Delta"],
                        data["Gamma"],
                        data["Theta"],
                        data["Vega"]
                    ))
                else:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS prices (
                            timestamp INTEGER PRIMARY KEY AUTOINCREMENT,
                            bid_price FLOAT NOT NULL,
                            ask_price FLOAT NOT NULL,
                            last_price FLOAT NOT NULL,
                            bid_size INTEGER NOT NULL,
                            ask_size INTEGER NOT NULL
                    )
                    """)
                    cursor.execute("""
                        INSERT INTO prices (
                            timestamp, bid_price, ask_price, last_price, bid_size, ask_size
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        timestamp,
                        data["Bid Price"],
                        data["Ask Price"],
                        data["Last Price"],
                        data["Bid Size"],
                        data["Ask Size"]
                    ))
                conn.commit()
                conn.close()
            await websocket.send(json.dumps({
                "type": "dataReady",
                "filenames": stream_func.file_names
            }))

        else:
            continue

async def main():
    load_dotenv()  # loads variables from .env into environment

    appKey = os.getenv("appKey")
    appSecret = os.getenv("appSecret")

    client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

    streamer = client.stream

    uri = "ws://localhost:8080/connect?id=PYTHON_CLIENT"

    async with websockets.connect(uri) as websocket:
        print("Connected to Websocket")
        await asyncio.gather(
            listen_for_messages(websocket, streamer),
            write_to_db(websocket)
        )
        await asyncio.Future()

asyncio.run(main())
