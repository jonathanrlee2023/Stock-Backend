import json
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

url = 'http://localhost:8080/dataReady'
stream_started = False
stream_lock = asyncio.Lock()

def make_handler(streamer):
    async def handler(websocket):
        global stream_started

        async for message in websocket:
            data = json.loads(message)
            symbol = data["symbol"]
            price = data["price"]
            day = data["day"]
            month = data["month"]
            year = data["year"]
            option_type = data["type"]

            print(f"Received: {data}")
            async with stream_lock:
                if not stream_started:
                    print("Starting stream now...")
                    streamer.start(receiver=stream_func.receive_data)
                    stream_started = True

            await websocket.send(f"Streaming started for {symbol}")
            try:
                # Inside the handler
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
            except requests.exceptions.ReadTimeout:
                print("Timeout while connecting to Schwab API. Retrying...")
                # Optionally: retry here
            except Exception as e:
                print("Unhandled stream error:", e)

    return handler

async def main():
    load_dotenv()  # loads variables from .env into environment

    appKey = os.getenv("appKey")
    appSecret = os.getenv("appSecret")

    client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

    streamer = client.stream

    handler = make_handler(streamer)
 
    async with websockets.serve(handler, "0.0.0.0", 8765):
        print("WebSocket server running...")
        try:
            while True:
                await asyncio.sleep(30 - time.time() % 30)
                timestamp = int(time.time())        
                
                if len(stream_func.file_names) != 0:
                    for names in stream_func.file_names:
                        existing_data = None
                        timestamp = int(time.time())
                        lock = FileLock(f'{names}.lock')
                        with lock:
                            if not os.path.exists(names):
                                with open(names, "w") as f:
                                    json.dump({
                                        'Data': {},
                                        'Latest': ''
                                    }, f)
                            with open(names, "r") as f:
                                existing_data = json.load(f)
                            existing_data['Data'][timestamp] = stream_func.new_data[names.replace('.json','')]
                            existing_data['Latest'] = str(timestamp)
                            with open(names, "w") as f:
                                json.dump(existing_data, f)
                    data = {
                        "filenames": stream_func.file_names
                    }
                    response = requests.post(url=url, json=data)
                    print(response)
                else:
                    continue
        except Exception as e:
            print("Exception type:", type(e))
            print("Exception:", e)
            traceback.print_exc()
        finally:
            print("Cleaning up...")
        await asyncio.Future()

asyncio.run(main())
