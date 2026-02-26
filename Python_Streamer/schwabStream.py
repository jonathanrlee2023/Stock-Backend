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
def create_engines(db_dir):
    # Ensure the path is absolute so SQLAlchemy doesn't get lost
    base_path = os.path.abspath(db_dir)
    
    db_names = {
        "income": "income_statement.db",
        "balance": "balance_sheet.db",
        "cash": "cash_flow.db",
        "earnings": "earnings.db",
        "overview": "company_overviews.db",
        "raw_income": "RAW_INCOME_STATEMENT.db",
        "raw_balance": "RAW_BALANCE_SHEET.db",
        "raw_cash": "RAW_CASH_FLOW.db",
        "raw_earnings": "RAW_EARNINGS.db"
    }
    
    # Build engines dynamically using the shared DB_DIR
    return {
        key: create_async_engine(f"sqlite+aiosqlite:///{os.path.join(base_path, name)}")
        for key, name in db_names.items()
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
    db_dir = os.getenv("DB_DIR", "../Database")

    engines = create_engines(db_dir)

    client = schwabdev.Client(app_key=appKey, app_secret=appSecret, tokens_db='/app/Database/tokens.json')

    streamer = schwabdev.Stream(client)

    db_connection = await asyncFunc.init_db()

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(asyncFunc.update_tickers_from_db(streamer)),
        asyncio.create_task(asyncFunc.broadcast_to_redis()),
        asyncio.create_task(asyncFunc.listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client, db_connection, engines=engines)),
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

        dispose_tasks = [engine.dispose() for engine in engines.values()]
        await asyncio.gather(*dispose_tasks)
        
        # Cancel all remaining tasks
        current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in current_tasks:
            task.cancel()
        
        await asyncio.gather(*current_tasks, return_exceptions=True)
        print("Cleanup complete. Goodbye!")

asyncio.run(main())
