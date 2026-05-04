import asyncio
from datetime import date
import io
import os
import sqlite3
import aiosqlite
import orjson
import pandas as pd
from sqlalchemy import text
from core.appState import app_state
from core.cache import max_fiscal_lookup, symbol_cache, latest_quote
from asyncFunc import get_symbol_id, r
from core.schemas import financial_schemas
from redis import asyncio as aioredis

db_dir = os.getenv("DB_DIR", "../Database")
today = date.today()
today_str = today.isoformat()

async def get_global_market_news():
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")
    try:
        while True:
            market_news = app_state.market_news
            news = await market_news.get_market_news()
            await r.publish("Global_News_Channel", orjson.dumps(news))

            await asyncio.sleep(3600)
    except Exception as e:
        print(f"Error fetching market news: {e}")
    
async def init_caches():
    await asyncio.gather(cache_all_max_dates(), get_recent_quote_time(), cache_symbol_ids())


async def cache_all_max_dates():
    global max_fiscal_lookup
    financial_tables = {"income", "balance", "cash", "earnings"}

    async def fetch_table_max(name):
        async with app_state.engines[name].connect() as conn:
            query = text(f"SELECT symbol_id, MAX(date) FROM {name} GROUP BY symbol_id")
            result = await conn.execute(query)

            return name, dict(result.fetchall())

    tasks = [fetch_table_max(table) for table in financial_tables if table in app_state.engines]

    results = await asyncio.gather(*tasks)

    for table, data in results:
        max_fiscal_lookup[table] = data

async def get_recent_quote_time():
    try:
        priceDB = app_state.price_db
        global latest_quote
        async with priceDB.execute("SELECT symbol_id, MAX(timestamp) FROM HistoricalStocks GROUP BY symbol_id") as cursor:
            rows = await cursor.fetchall()
            latest_quote.update(dict(rows))

        return
    except Exception as e:
        print(f"Error fetching latest quote time: {e}")


async def cache_symbol_ids():
    try:
        global symbol_cache
        async with app_state.price_db.execute("SELECT symbol, symbol_id FROM Symbols") as cursor:
            result = await cursor.fetchall()
            symbol_cache.update(dict(result))

        return
    except Exception as e:
        print(f"Error caching symbol IDs: {e}")

async def get_earnings_dates(api_key):
    """
    Fetches the earnings calendar for the next 3 months.

    If the latest report date is older than today, fetches the latest earnings calendar
    from Alpha Vantage and saves it to the database. Otherwise, loads the earnings calendar
    from the database.

    Parameters
    ----------
    api_key: str
        The API key to use for the Alpha Vantage request.

    Returns
    -------
    None
    """
    global earnings_lookup
    global today
    try:
        db_path = os.path.join(db_dir, "EarningsDates.db")
        should_fetch = False

        latest_report = await get_last_update_date()
        if latest_report is None:
            should_fetch = True
        else:
            if today_str > latest_report:
                should_fetch = True
        await cache_all_max_dates()
        if should_fetch:
            client = app_state.httpx_client
            await api_key.lock_key()
            try:
                url = f"https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={api_key.key}"
                response = await client.get(url)
            except Exception as e:
                print(f"Error fetching earnings calendar: {e}")
                return
            finally:
                api_key.unlock_key()
            if response.status_code == 200:
                csv_data = io.StringIO(response.text)
                df = pd.read_csv(csv_data)
                df.columns = [c.strip().lower() for c in df.columns]

                df["symbol_id"] = df["symbol"].str.upper().map(symbol_cache)

                missing_symbols = df[df["symbol_id"].isna()]["symbol"].unique().tolist()
                if missing_symbols:
                    new_ids = await asyncio.gather(*(get_symbol_id(s) for s in missing_symbols))

                    new_mappings = {s: i for s, i in zip(missing_symbols, new_ids) if i is not None}
                    symbol_cache.update(new_mappings)

                    df["symbol_id"] = df["symbol"].str.upper().map(symbol_cache)

                earnings_df = df.dropna(subset=["symbol_id"]).copy()
                earnings_df["symbol_id"] = earnings_df["symbol_id"].astype(int)
                earnings_lookup = {
                    row.symbol_id: (row.reportdate, row.fiscaldateending) for row in earnings_df.itertuples(index=False)
                }
                os.makedirs(db_dir, exist_ok=True)

                def save_to_db():
                    with sqlite3.connect(db_path) as conn:
                        earnings_df.to_sql("earnings_calendar", conn, if_exists="replace", index=False)

                await asyncio.to_thread(save_to_db)
            else:
                print(f"Failed to fetch data: {response.status_code}")
                return
        else:

            def load_from_db():
                with sqlite3.connect(db_path) as conn:
                    return pd.read_sql("SELECT * FROM earnings_calendar", conn)

            earnings_df = await asyncio.to_thread(load_from_db)
            earnings_lookup = {
                row.symbol_id: (row.reportdate, row.fiscaldateending) for row in earnings_df.itertuples(index=False)
            }
            return

    except Exception as e:
        print(f"Error fetching earnings calendar: {e}")

async def get_last_update_date():
    db = app_state.earnings_db
    async with db.execute("SELECT MAX(reportdate) FROM earnings_calendar") as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None
    
async def last_checked_cacher():
    db = app_state.last_checked_db
    global last_checked_cache
    async with db.execute("SELECT symbol_id, last_checked_date FROM last_checked") as cursor:
        rows = await cursor.fetchall()
        last_checked_cache = dict(rows)


async def init_earnings_db():
    db_path = os.path.join(db_dir, "EarningsDates.db")
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("""
            CREATE TABLE IF NOT EXISTS earnings_calendar (
                symbol TEXT,
                name TEXT,
                reportdate TEXT,
                fiscaldateending TEXT,
                estimate REAL,
                currency TEXT,
                timeoftheday TEXT,
                symbol_id INTEGER,
                UNIQUE(symbol_id, reportdate) ON CONFLICT REPLACE
            )
        """)
    await db.commit()

    return db

async def init_tracker_db():
    db_path = os.path.join(db_dir, "Tracker.db")
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.commit()

    return db

async def init_db():
    db_path = os.path.join(db_dir, "PriceData.db")
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("""
        CREATE TABLE IF NOT EXISTS Options (
            timestamp INTEGER NOT NULL, symbol_id INTEGER NOT NULL, mark REAL,
            bid_price REAL, ask_price REAL, last_price REAL, high_price REAL,
            iv REAL, delta REAL, gamma REAL, theta REAL, vega REAL,
            PRIMARY KEY (timestamp, symbol_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS Stocks (
            timestamp INTEGER NOT NULL, symbol_id INTEGER NOT NULL, mark REAL,
            bid_price REAL, ask_price REAL, last_price REAL, bid_size INTEGER, ask_size INTEGER,
            PRIMARY KEY (timestamp, symbol_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS HistoricalStocks (
            timestamp INTEGER NOT NULL, symbol_id INTEGER NOT NULL, open REAL, high REAL, low REAL, close REAL, volume INTEGER,
            PRIMARY KEY (timestamp, symbol_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS Symbols (
            symbol_id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT UNIQUE
        )
    """)

    await db.execute("CREATE INDEX IF NOT EXISTS idx_options_symbol ON Options (symbol_id, timestamp)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON Stocks (symbol_id, timestamp)")
    await db.commit()

    return db


async def init_last_checked_db():
    db_path = os.path.join(db_dir, "LastChecked.db")
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.execute("""
        CREATE TABLE IF NOT EXISTS last_checked (
            symbol_id INTEGER PRIMARY KEY, last_checked_date DATE
        )
    """)
    await db.commit()

    return db


async def init_financial_db():
    for table, schema in financial_schemas.items():
        engine = app_state.engines.get(table)
        if not engine:
            print(f"⚠️ Warning: No engine found for table '{table}'")
            continue

        async with engine.begin() as conn:
            try:
                for statement in schema.split(";"):
                    if statement.strip():
                        await conn.execute(text(statement))
            except Exception as e:
                print(f"❌ Failed to initialize {table}: {e}")