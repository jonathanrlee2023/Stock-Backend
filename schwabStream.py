import json
import schwabdev
from dotenv import load_dotenv
import os
import time
import stream_func
from datetime import datetime, timedelta


load_dotenv()  # loads variables from .env into environment

appKey = os.getenv("appKey")
appSecret = os.getenv("appSecret")


client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

streamer = client.stream 

try:
    print("Starting stream...")
    streamer.start(receiver=stream_func.receive_data)
    
    stream_func.start_options_stream(streamer=streamer)

    # Keep the script alive
    while True:
        now = time.time()
        sleep_seconds = 60 - (now % 60)
        time.sleep(sleep_seconds)

        timestamp = int(time.time())        
        for names in stream_func.file_names:
            existing_data = None
            timestamp = int(time.time())
            with open(f"{names}.json", "r") as f:
                existing_data = json.load(f)
            existing_data[timestamp] = stream_func.new_data[names]
            with open(f'{names}.json', "w") as f:
                json.dump(existing_data, f)
except Exception as e:
    print("Stream error:", e)
finally:
    print("Cleaning up...")
    streamer.stop()
