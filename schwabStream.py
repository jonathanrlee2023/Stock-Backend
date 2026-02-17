import schwabdev
from dotenv import load_dotenv
import os
import asyncio
import asyncFunc

tasks_started = False

async def main():
    global tasks_started
    if tasks_started:
        return
    tasks_started = True
    load_dotenv()
    appKey = os.getenv("appKey")
    appSecret = os.getenv("appSecret")
    alpha_vantage_api_key = os.getenv("AlphaVantageKey")
    rate_api_key = os.getenv("ExchangeRateKey")

    client = schwabdev.Client(app_key=appKey, app_secret=appSecret)
    streamer = client.stream

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(asyncFunc.update_tickers_from_db(streamer)),
        asyncio.create_task(asyncFunc.broadcast_to_redis()),
        asyncio.create_task(asyncFunc.listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client)),
        asyncio.create_task(asyncFunc.write_to_db()),
    ]

    # CRITICAL: This keeps the script alive and running all tasks
    await asyncio.gather(*tasks)

asyncio.run(main())
