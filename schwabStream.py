from pprint import pprint

import schwabdev
from dotenv import load_dotenv
import os
import asyncio
import asyncFunc
import stream_func

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

    db_connection = await asyncFunc.init_db()

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(asyncFunc.update_tickers_from_db(streamer)),
        asyncio.create_task(asyncFunc.broadcast_to_redis()),
        asyncio.create_task(asyncFunc.listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client, db_connection)),
        asyncio.create_task(asyncFunc.write_to_db(db_connection)),
        asyncio.create_task(asyncFunc.stream_options(client)),
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\nShutdown signal received...")
    except Exception as e:
        print(f"\nTask failed with error: {e}")
    finally:
        # This part ensures the script actually exits
        print("Closing database and cleaning up tasks...")
        await db_connection.close()
        
        # Cancel all remaining tasks
        current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in current_tasks:
            task.cancel()
        
        await asyncio.gather(*current_tasks, return_exceptions=True)
        print("Cleanup complete. Goodbye!")

asyncio.run(main())
