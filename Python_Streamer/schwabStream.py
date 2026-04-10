from concurrent.futures import ProcessPoolExecutor

import httpx

from secureAPIKey import SecureAPIKey
import schwabdev
from dotenv import load_dotenv
import os
import asyncio
import asyncFunc
from appState import app_state
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
        "RAW_INCOME_STATEMENT": "RAW_INCOME_STATEMENT.db",
        "RAW_BALANCE_SHEET": "RAW_BALANCE_SHEET.db",
        "RAW_CASH_FLOW": "RAW_CASH_FLOW.db",
        "RAW_EARNINGS": "RAW_EARNINGS.db",
    }

    # Build engines dynamically using the shared DB_DIR
    return {
        key: create_async_engine(f"sqlite+aiosqlite:///{os.path.join(base_path, name)}")
        for key, name in db_names.items()
    }


async def init_app_state(db_dir):
    app_state.engines = create_engines(db_dir)
    app_state.price_db = await asyncFunc.init_db()
    app_state.earnings_db = await asyncFunc.init_earnings_db()
    app_state.tracker_db = await asyncFunc.init_tracker_db()
    app_state.last_checked_db = await asyncFunc.init_last_checked_db()

    await asyncFunc.init_financial_db()


async def verify_db_conn():
    print("Verifying database connections...")
    connection_ready = False
    retries = 0

    while not connection_ready and retries < 5:
        try:
            # We run a tiny dummy query to see if the engine responds
            await app_state.price_db.execute("SELECT 1")
            await app_state.earnings_db.execute("SELECT 1")
            await app_state.tracker_db.execute("SELECT 1")

            connection_ready = True
            print("Databases are online and responding.")
        except Exception as e:
            retries += 1
            print(f"Waiting for DB... attempt {retries}/5 (Error: {e})")
            await asyncio.sleep(1)

    if not connection_ready:
        print("CRITICAL: Database failed to stabilize. Exiting.")
        return


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

    api_manager = SecureAPIKey(alpha_vantage_api_key)

    await init_app_state(db_dir)
    app_state.cpu_executor = ProcessPoolExecutor(max_workers=4)
    app_state.client = schwabdev.Client(app_key=appKey, app_secret=appSecret, tokens_db="/app/Database/tokens.json")
    app_state.streamer = schwabdev.Stream(app_state.client)
    app_state.httpx_client = httpx.AsyncClient(
        timeout=httpx.Timeout(5.0), limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
    )

    await verify_db_conn()

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(asyncFunc.update_tickers_from_db(api_manager, rate_api_key)),
        asyncio.create_task(asyncFunc.broadcast_to_redis()),
        asyncio.create_task(asyncFunc.listen_for_messages(api_manager, rate_api_key)),
        asyncio.create_task(asyncFunc.write_to_db()),
        asyncio.create_task(asyncFunc.stream_options()),
        asyncio.create_task(asyncFunc.get_earnings_dates(api_manager)),
        asyncio.create_task(asyncFunc.get_recent_quote_time()),
        asyncio.create_task(asyncFunc.last_checked_cacher()),
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

        await app_state.price_db.close()
        await app_state.earnings_db.close()
        await app_state.tracker_db.close()
        for engine in app_state.engines.values():
            await engine.dispose()

        app_state.client.close()
        app_state.streamer.close()
        app_state.httpx_client.close()
        app_state.cpu_executor.shutdown()

        # Cancel all remaining tasks
        current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in current_tasks:
            task.cancel()

        await asyncio.gather(*current_tasks, return_exceptions=True)
        print("Cleanup complete. Goodbye!")


asyncio.run(main())
