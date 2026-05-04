import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _pkg in ("core", "company", "streaming", "backtest"):
    _d = _ROOT / _pkg
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

from concurrent.futures import ProcessPoolExecutor
from abc import ABC, abstractmethod

import httpx
import finnhub
from streaming.marketNewsClass import MarketNews
from streaming.dataloader import DataLoader
from core.secureAPIKey import SecureAPIKey
import schwabdev
from dotenv import load_dotenv
import os
import asyncio
import streaming.asyncFunc as asyncFunc
import streaming.startupFunc as startupFunc
from core.appState import app_state
from pprint import pprint
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
    }

    # Build engines dynamically using the shared DB_DIR
    return {
        key: create_async_engine(f"sqlite+aiosqlite:///{os.path.join(base_path, name)}")
        for key, name in db_names.items()
    }


async def init_app_state(db_dir):
    app_state.engines = create_engines(db_dir)
    app_state.price_db = await startupFunc.init_db()
    app_state.earnings_db = await startupFunc.init_earnings_db()
    app_state.tracker_db = await startupFunc.init_tracker_db()
    app_state.last_checked_db = await startupFunc.init_last_checked_db()

    await startupFunc.init_financial_db()


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


class StackFactory(ABC):
    @abstractmethod
    async def build(
        self,
        *,
        db_dir,
        app_key,
        app_secret,
        finnhub_api_key,
        schwab_tokens_db,
    ):
        raise NotImplementedError


class ProductionStackFactory(StackFactory):
    async def build(
        self,
        *,
        db_dir,
        app_key,
        app_secret,
        finnhub_api_key,
        schwab_tokens_db,
    ):
        await init_app_state(db_dir)
        schwab_client = schwabdev.Client(
            app_key=app_key,
            app_secret=app_secret,
            tokens_db=schwab_tokens_db,
        )
        app_state.schwab_client = schwab_client
        app_state.streamer = schwabdev.Stream(app_state.schwab_client)
        app_state.httpx_client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        app_state.finnhub_client = finnhub.Client(api_key=finnhub_api_key)
        app_state.data_loader = DataLoader(schwab_client)
        app_state.market_news = MarketNews()


class TestStackFactory(StackFactory):
    def __init__(self, *, httpx_client=None, finnhub_client=None, data_loader=None, market_news=None):
        self.httpx_client = httpx_client
        self.finnhub_client = finnhub_client
        self.data_loader = data_loader
        self.market_news = market_news

    async def build(
        self,
        *,
        db_dir,
        app_key,
        app_secret,
        finnhub_api_key,
        schwab_tokens_db,
    ):
        await init_app_state(db_dir)
        app_state.schwab_client = None
        app_state.streamer = None
        app_state.httpx_client = self.httpx_client
        app_state.finnhub_client = self.finnhub_client
        app_state.data_loader = self.data_loader
        app_state.market_news = self.market_news


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
    finnhub_api_key = os.getenv("FinnhubApiKey")
    schwab_tokens_db = os.getenv("SCHWAB_TOKENS_DB", "/app/Database/tokens.json")

    api_manager = SecureAPIKey(alpha_vantage_api_key)
    stack_factory = ProductionStackFactory()
    await stack_factory.build(
        db_dir=db_dir,
        app_key=appKey,
        app_secret=appSecret,
        finnhub_api_key=finnhub_api_key,
        schwab_tokens_db=schwab_tokens_db,
    )

    await verify_db_conn()

    print("Starting background tasks...")
    tasks = [
        asyncio.create_task(asyncFunc.update_tickers_from_db(api_manager, rate_api_key)),
        asyncio.create_task(asyncFunc.broadcast_to_redis()),
        asyncio.create_task(asyncFunc.listen_for_messages(api_manager, rate_api_key)),
        asyncio.create_task(asyncFunc.write_to_db()),
        asyncio.create_task(asyncFunc.stream_options()),
        asyncio.create_task(startupFunc.get_earnings_dates(api_manager)),
        asyncio.create_task(startupFunc.get_recent_quote_time()),
        asyncio.create_task(startupFunc.last_checked_cacher()),
        asyncio.create_task(startupFunc.get_global_market_news()),
        asyncio.create_task(asyncFunc.date_refresher_task()),
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

        app_state.schwab_client.close()
        app_state.streamer.close()
        app_state.httpx_client.close()

        # Cancel all remaining tasks
        current_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in current_tasks:
            task.cancel()

        await asyncio.gather(*current_tasks, return_exceptions=True)
        print("Cleanup complete. Goodbye!")


asyncio.run(main())
