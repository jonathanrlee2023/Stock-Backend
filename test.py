import asyncio
import json
from schwabdev import Client
from dotenv import load_dotenv
import os

load_dotenv()

appKey = os.getenv("appKey")
appSecret = os.getenv("appSecret")

client = Client(app_key=appKey, app_secret=appSecret)

def receiver(data):
    print("RECEIVED DATA:", json.dumps(data, indent=2))

async def main():
    streamer = client.stream
    streamer.start(receiver=receiver)

    await asyncio.sleep(2)  # Wait for LOGIN response

    sub_request = streamer.level_one_equities("AAPL", "0,1,2,3,4,5", command='ADD')
    print("Sending sub request:", sub_request)

    await streamer.send_async(sub_request)

    await asyncio.sleep(10)  # Keep process alive to receive updates

asyncio.run(main())
