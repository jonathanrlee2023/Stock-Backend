from pprint import pprint

import schwabdev
from dotenv import load_dotenv
import os
import asyncio
import asyncFunc
from sqlalchemy.ext.asyncio import create_async_engine


tasks_started = False
# def delete_table(db_name, table_name):
#         conn = sqlite3.connect(db_name)
#         cursor = conn.cursor()
        
#         # Using a f-string for the table name is okay if YOU define it, 
#         # but be careful of SQL injection with user input!
#         cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
#         conn.commit()
#         conn.close()
#         print(f"Table '{table_name}' has been deleted.")
ENGINES = {
        "income": create_async_engine("sqlite+aiosqlite:///income_statement.db"),
        "balance": create_async_engine("sqlite+aiosqlite:///balance_sheet.db"),
        "cash": create_async_engine("sqlite+aiosqlite:///cash_flow.db"),
        "earnings": create_async_engine("sqlite+aiosqlite:///earnings.db"),
        "overview": create_async_engine("sqlite+aiosqlite:///company_overviews.db"),
        "raw_income": create_async_engine("sqlite+aiosqlite:///RAW_INCOME_STATEMENT.db"),
        "raw_balance": create_async_engine("sqlite+aiosqlite:///RAW_BALANCE_SHEET.db"),
        "raw_cash": create_async_engine("sqlite+aiosqlite:///RAW_CASH_FLOW.db"),
        "raw_earnings": create_async_engine("sqlite+aiosqlite:///RAW_EARNINGS.db")
    }

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
        asyncio.create_task(asyncFunc.listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client, db_connection, ENGINES)),
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

        dispose_tasks = [engine.dispose() for engine in ENGINES.values()]
        await asyncio.gather(*dispose_tasks)
        
        # Cancel all remaining tasks
        current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in current_tasks:
            task.cancel()
        
        await asyncio.gather(*current_tasks, return_exceptions=True)
        print("Cleanup complete. Goodbye!")

asyncio.run(main())
