import asyncio
import io
import sqlite3
import pandas as pd
from operator import itemgetter
import os
import time
import orjson
from redis import asyncio as aioredis
import aiosqlite
import requests
from sqlalchemy import text
import datetime
from appState import app_state
from dataloader import DataLoader
import stream_func
from cache import latest_quote, earnings_lookup, max_fiscal_lookup, symbol_cache, last_checked_cache, POPULAR_ETFS
from schemas import financial_schemas

stream_started = False
stream_lock = asyncio.Lock()
is_weekday_business_hours_central = stream_func.is_weekday_business_hours_central()
is_market_closed = stream_func.is_market_closed()
option_attributes = [
    "askPrice",
    "bidPrice",
    "lastPrice",
    "highPrice",
    "volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "mark",
]
parsed_labels = ["Ask Price", "Bid Price", "Last Price", "High Price", "IV", "Delta", "Gamma", "Theta", "Vega", "Mark"]
get_option_stats = itemgetter(*option_attributes)
r = aioredis.Redis(host="redis", port=6379, db=0)
db_dir = os.getenv("DB_DIR", "../Database")
alpha_vantage_semaphore = asyncio.Semaphore(1)
schwab_api_semaphore = asyncio.Semaphore(5)

today = datetime.date.today()
today_str = today.isoformat()


async def date_refresher_task():
    global today, today_str
    while True:
        # Update the globals
        now = datetime.date.today()
        if now != today:
            today = now
            today_str = now.isoformat()

        await asyncio.sleep(3600)


async def listen_for_messages(alpha_vantage_api_key, rate_api_key):
    """
    Listens for incoming messages from the Redis channel 'Request_Channel'.
    When a message is received, it is parsed and checked for validity.
    If valid, it is passed to the streamer to start a new stream.

    NOTE: This function will only start a new stream if the market is currently open.
    If the market is closed, the stream will not be started until the market reopens.

    :param streamer: The streamer to use when starting new streams.
    :param alpha_vantage_api_key: The Alpha Vantage API key to use when retrieving company data.
    :param rate_api_key: The ExchangeRate API key to use when retrieving currency exchange rates.
    :param client: The Schwab API client to use when retrieving company data.
    """
    global stream_started
    option_ids = stream_func.option_ids
    streamer = app_state.streamer
    pubsub = r.pubsub()
    await pubsub.subscribe("Request_Channel")
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")

    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True)
        if not stream_started:
            async with stream_lock:
                if not stream_started:
                    streamer.start(receiver=stream_func.receive_data)
                    stream_started = True
        if message is not None:
            data = orjson.loads(message["data"])

            if "Status" in data:
                continue
            else:
                symbol = data.get("symbol")
                try:
                    if "price" in data:
                        price = data["price"]
                        day = data["day"]
                        month = data["month"]
                        year = data["year"]
                        option_type = data["type"]
                        if symbol not in option_ids:
                            if not is_market_closed:
                                asyncio.create_task(
                                    stream_func.start_options_stream(
                                        ticker=symbol, price=price, day=day, month=month, year=year, type=option_type
                                    )
                                )
                    else:
                        if symbol not in streamer.subscriptions.get("LEVELONE_EQUITIES", {}):
                            if not is_market_closed:
                                asyncio.create_task(
                                    stream_func.start_stock_stream(
                                        ticker=symbol,
                                    )
                                )
                        await asyncio.gather(
                            get_options_and_initial_quotes(symbol),
                            handleCompany(alpha_vantage_api_key, rate_api_key, symbol),
                        )
                except requests.exceptions.ReadTimeout:
                    print("Timeout connecting to Schwab API.")
                except Exception as e:
                    print("Stream handling error:", e)


async def get_symbol_id(symbol):
    """Helper to get ID from cache or DB, creating it if necessary."""
    if symbol in symbol_cache:
        return symbol_cache[symbol]

    db = app_state.price_db
    async with db.execute("SELECT symbol_id FROM Symbols WHERE symbol = ?", (symbol,)) as cursor:
        row = await cursor.fetchone()
        if row:
            symbol_cache[symbol] = row[0]
            return row[0]

    cursor = await db.execute("INSERT INTO Symbols (symbol) VALUES (?)", (symbol,))
    new_id = cursor.lastrowid

    await db.commit()

    symbol_cache[symbol] = new_id
    return new_id


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


async def write_to_db():
    """
    Write data to the database. This function is designed to run in a loop and be called
    every 15 seconds. It will create the tables and indices if they do not exist,
    and then write the data in the snapshot to the database.

    :return: None
    """
    db = app_state.price_db
    async with db.execute("SELECT symbol, symbol_id FROM Symbols") as cursor:
        async for row in cursor:
            symbol_cache[row[0]] = row[1]

    while True:
        write_interval = 300
        wait_time = write_interval - (time.time() % write_interval)
        if wait_time < 0.1:
            wait_time = write_interval
        await asyncio.sleep(wait_time)
        timestamp = int(time.time())

        snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names if name in stream_func.new_data}

        option_records = []
        stock_records = []

        for name, data in snapshot.items():
            try:
                if name in symbol_cache:
                    s_id = symbol_cache[name]
                else:
                    s_id = await get_symbol_id(name)
                if len(name) > 8:  # Option logic
                    option_records.append(
                        (
                            timestamp,
                            s_id,
                            data.get("Mark"),
                            data.get("Bid Price"),
                            data.get("Ask Price"),
                            data.get("Last Price"),
                            data.get("High Price"),
                            data.get("IV"),
                            data.get("Delta"),
                            data.get("Gamma"),
                            data.get("Theta"),
                            data.get("Vega"),
                        )
                    )
                else:  # Stock logic
                    stock_records.append(
                        (
                            timestamp,
                            s_id,
                            data.get("Mark"),
                            data.get("Bid Price"),
                            data.get("Ask Price"),
                            data.get("Last Price"),
                            data.get("Bid Size"),
                            data.get("Ask Size"),
                        )
                    )
            except KeyError as e:
                print(f"Skipping {name}: Missing key {e}")

        if option_records:
            await db.executemany("INSERT OR IGNORE INTO Options VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", option_records)

        if stock_records:
            await db.executemany("INSERT OR IGNORE INTO Stocks VALUES (?,?,?,?,?,?,?,?)", stock_records)

        await db.commit()


async def stream_options():
    """
    Streams options from the Schwab API into the SQLite database.

    Fetches a batch of options from the API and processes them in parallel.
    For each option, fetches the current quote and stores it in the database.

    Continuously fetches options until the list of option IDs is empty.
    Then, waits for the next 15 second interval to fetch more options.

    :param client: The Schwab API client to use when fetching options.
    """
    option_ids = stream_func.option_ids
    new_data = stream_func.new_data
    client = app_state.client
    LIMIT = 250

    while True:
        if not option_ids:
            await asyncio.sleep(5)
            continue

        # start_fetch = time.perf_counter()

        try:
            if len(option_ids) <= LIMIT:
                response = await asyncio.to_thread(client.quotes, option_ids)
                # GUARD: Verify response is valid and not empty
                if response.status_code != 200 or not response.content:
                    print(f"API Error: Status {response.status_code} | Length: {len(response.content)}")
                    await asyncio.sleep(5)
                    continue
                all_responses = [response.content]
            else:
                batches = [option_ids[i : i + LIMIT] for i in range(0, len(option_ids), LIMIT)]
                tasks = [asyncio.to_thread(client.quotes, b) for b in batches]
                responses = await asyncio.gather(*tasks)

                all_responses = [r.content for r in responses if r.status_code == 200 and r.content]

                if not all_responses:
                    continue
        except Exception as e:
            print(f"Network exception caught: {e}")
            await asyncio.sleep(5)
            continue

        # fetch_done = time.perf_counter()

        local_fetcher = get_option_stats
        local_new_data = new_data

        for raw_content in all_responses:
            batch_data = orjson.loads(raw_content)

            for symbol, details in batch_data.items():
                try:
                    t = local_new_data[symbol]
                except KeyError:
                    t = local_new_data[symbol] = {}

                try:
                    v = local_fetcher(details["quote"])
                    # Fastest way to assign: Multiple assignment
                    (
                        t["Bid Price"],
                        t["Ask Price"],
                        t["Last Price"],
                        t["High Price"],
                        t["IV"],
                        t["Delta"],
                        t["Gamma"],
                        t["Theta"],
                        t["Vega"],
                        t["Mark"],
                    ) = v
                except KeyError:
                    continue

        # end_time = time.perf_counter()

        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1:
            wait_time = 15
        await asyncio.sleep(wait_time)


async def broadcast_to_redis():
    """
    Broadcasts the current snapshot of data to the Redis channel named "Stream_Channel".
    This function will sleep for 15 seconds minus the current time modulo 15 seconds
    and then broadcast the snapshot. This is done to ensure that Redis receives updates
    at a consistent interval.
    """
    file_names = stream_func.file_names
    while True:
        try:
            current_data = stream_func.new_data.copy()
            file_names = list(stream_func.file_names)

            if file_names:
                snapshot = {name: current_data[name] for name in file_names if name in current_data}
                if snapshot:
                    await r.publish("Stream_Channel", orjson.dumps(snapshot))

            wait_time = 15 - (time.time() % 15)
            if wait_time < 0.1:
                wait_time = 15
            await asyncio.sleep(wait_time)
        except aioredis.ConnectionError:
            print("❌ Connection lost. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")
            await asyncio.sleep(1)  # Prevent tight-looping on errors


async def init_tracker_db():
    db_path = os.path.join(db_dir, "Tracker.db")
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.commit()

    return db


async def update_tickers_from_db(api_manager, rate_api_key):
    """
    Updates the list of tickers to subscribe to from the database.

    Retrieves all tickers from the database and subscribes to them.

    If the ticker is an option, it is parsed into its constituent parts
    and added to the list of options to subscribe to. If the ticker is
    a stock, it is added to the list of stocks to subscribe to.

    Parameters
    ----------
    streamer : Streamer
        The streamer object to use for subscribing to the tickers.

    Returns
    -------
    None
    """
    db = app_state.tracker_db
    async with db.execute("SELECT id FROM tracker") as cursor:
        rows = await cursor.fetchall()
        ticker_list = [row[0] for row in rows]

    if not ticker_list:
        print("No tickers found in tracker.db")
        return
    tasks = []
    companies = []
    valid_tickers = []
    for ticker in ticker_list:
        if len(ticker) > 8:
            parsed = stream_func.parse_option(ticker)
            if parsed:
                tasks.append(
                    stream_func.start_options_stream(
                        ticker=parsed["ticker"],
                        price=parsed["price"],
                        day=parsed["day"],
                        month=parsed["month"],
                        year=parsed["year"],
                        type=parsed["type"],
                    )
                )
                valid_tickers.append(ticker)
        else:
            tasks.append(stream_func.start_stock_stream(ticker))
            companies.append(ticker)
            valid_tickers.append(ticker)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for ticker, res in zip(valid_tickers, results):
        if isinstance(res, Exception):
            print(f"Failed to subscribe to {ticker}: {res}")

    if not companies:
        return
    tasks = []
    schwab_tasks = [throttled_get_options_and_initial_quotes(c) for c in companies]
    await asyncio.gather(*schwab_tasks, return_exceptions=True)

    # Phase 3: Alpha Vantage fundamentals (slow, serialized)
    av_tasks = [throttled_handle_company(api_manager, rate_api_key, c) for c in companies]
    await asyncio.gather(*av_tasks, return_exceptions=True)


async def throttled_handle_company(api_manager, rate_api_key, ticker):
    async with alpha_vantage_semaphore:
        print(f"📡 Fetching company data for {ticker}...")
        await handleCompany(api_manager, rate_api_key, ticker)
        await asyncio.sleep(0.1)
        return


async def throttled_get_options_and_initial_quotes(ticker):
    async with schwab_api_semaphore:
        print(f"📡 Fetching initial data for {ticker}...")
        await get_options_and_initial_quotes(ticker)

        return


async def handleCompany(api_key, rate_key, ticker):
    """
    Handles a company request by fetching the company's data and publishing the final report to the Redis channel.

    Parameters
    ----------
    client : schwabdev.Client
        The client instance to use for fetching the company's data.
    api_key : SecureAPIKey
        The Alpha Vantage API key to use for fetching the company's data.
    rate_key : str
        The ExchangeRate API key to use for fetching the company's data.
    ticker : str
        The ticker symbol of the company to fetch data for.

    Returns
    -------
    None
    """
    if ticker in POPULAR_ETFS:
        print(f"⚠️ Skipping {ticker} as it's a popular ETF with no financials.")
        return
    try:
        from companyClass import Company

        company = await Company.create(ticker=ticker, api_key=api_key, rate_api_key=rate_key)
        if company is None:
            print("Company data not fetched properly")
            return

        await r.publish("Company_Channel", orjson.dumps(company.final_report))
    except Exception as e:
        print(f"Error publishing company data for {ticker}: {e}")

    del company


async def get_options_and_initial_quotes(ticker):
    """
    Fetches the one time data for a given ticker and publishes it to the "One_Time_Data_Channel" Redis channel.

    Parameters
    ----------
    ticker : str
        The ticker symbol of the company to fetch data for.
    client : schwabdev.Client
        The client instance to use for fetching the company's data.

    Returns
    -------
    None
    """
    client = app_state.client
    if symbol_cache.get(ticker) is None:
        s_id = await get_symbol_id(ticker)
    else:
        s_id = symbol_cache[ticker]
    option_ids = stream_func.option_ids
    file_names = stream_func.file_names
    call_option_id_list = []
    put_option_id_list = []

    loader = DataLoader(ticker=ticker, symbol_id=s_id, connection=client)
    try:
        tasks = [
            asyncio.to_thread(loader.get_quote),
            loader.get_recent_price_history(),
            asyncio.to_thread(loader.get_option_expirations),
        ]

        quote, price_history, (call_option_id_list, put_option_id_list) = await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Error fetching initial data for {ticker}: {e}")
        return

    all_options = call_option_id_list + put_option_id_list
    option_ids.extend(all_options)
    file_names.extend(all_options)

    quote_dict = {
        "Symbol": ticker,
        "timestamp": int(quote["quoteTime"] // 1000),
        "BidPrice": quote["bidPrice"],
        "AskPrice": quote["askPrice"],
        "LastPrice": quote["lastPrice"],
        "Mark": quote["mark"],
    }

    payload = {
        "Symbol": ticker,
        "Quote": quote_dict,
        "PriceHistory": price_history,
        "Call": call_option_id_list,
        "Put": put_option_id_list,
    }

    await r.publish("One_Time_Data_Channel", orjson.dumps(payload))


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


async def get_last_update_date():
    db = app_state.earnings_db
    async with db.execute("SELECT MAX(reportdate) FROM earnings_calendar") as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None


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


def get_furthest_date_for_stock(symbol_id):
    """
    Returns the latest scheduled earnings date from RAM.
    Falling back to the DB is no longer necessary if the cache is loaded.
    """
    global earnings_lookup

    ticker_data = earnings_lookup.get(symbol_id)
    if ticker_data:
        return ticker_data[0]

    return None


async def check_for_new_earnings(symbol_id, table) -> bool:
    """
    Checks if there are new earnings available for a specific ticker.

    Args:
        symbol_id (int): The ID of the ticker symbol in the database.
        table (str): The table name in the earnings database.

    Returns:
        bool: True if there are new earnings available, False otherwise.
    """
    global earnings_lookup
    global max_fiscal_lookup
    # start = time.perf_counter()
    global today
    global last_checked_cache

    if symbol_id in last_checked_cache:
        last_checked_date = last_checked_cache[symbol_id]

        if isinstance(last_checked_date, str):
            last_checked_date = datetime.date.fromisoformat(last_checked_date)
        else:
            last_checked_date = last_checked_date

        days_since_check = (today - last_checked_date).days

        if days_since_check < 7:
            return False

    last_checked_cache[symbol_id] = today
    try:
        await app_state.last_checked_db.execute(
            "INSERT OR REPLACE INTO last_checked (symbol_id, last_checked_date) VALUES (?, ?)",
            (symbol_id, today.isoformat()),
        )
        await app_state.last_checked_db.commit()
    except Exception as e:
        print(f"Error updating last checked date for symbol_id {symbol_id}: {e}")
    last_local_fiscal = max_fiscal_lookup[table].get(symbol_id, None)

    if last_local_fiscal is None:
        return True
    ticker_data = earnings_lookup.get(symbol_id)

    if not ticker_data:
        return False

    report_date_str, next_fiscal_date = ticker_data
    if next_fiscal_date > (last_local_fiscal or "1900-01-01"):
        if today_str > report_date_str:  # String comparison is faster than parsing
            return True

    return False


async def init_caches():
    await asyncio.gather(cache_all_max_dates(), get_recent_quote_time(), cache_symbol_ids())


async def cache_all_max_dates():
    global max_fiscal_lookup
    financial_tables = {
        "balance": "RAW_BALANCE_SHEET",
        "cash": "RAW_CASH_FLOW",
        "earnings": "RAW_EARNINGS",
        "income": "RAW_INCOME_STATEMENT",
    }

    async def fetch_table_max(table, db_name):
        async with app_state.engines[db_name].connect() as conn:
            query = text(f"SELECT symbol_id, MAX(date) FROM {table} GROUP BY symbol_id")
            result = await conn.execute(query)

            return table, dict(result.fetchall())

    tasks = [fetch_table_max(table, db_name) for table, db_name in financial_tables.items()]

    results = await asyncio.gather(*tasks)

    for table, data in results:
        max_fiscal_lookup[table] = data


async def get_recent_quote_time():
    priceDB = app_state.price_db
    global latest_quote
    async with priceDB.execute("SELECT symbol_id, MAX(timestamp) FROM HistoricalStocks GROUP BY symbol_id") as cursor:
        rows = await cursor.fetchall()
        latest_quote.update(dict(rows))

    return


async def cache_symbol_ids():
    global symbol_cache
    async with app_state.price_db.execute("SELECT symbol, symbol_id FROM Symbols") as cursor:
        result = await cursor.fetchall()
        symbol_cache.update(dict(result))

    return


async def last_checked_cacher():
    db = app_state.last_checked_db
    global last_checked_cache
    async with db.execute("SELECT symbol_id, last_checked_date FROM last_checked") as cursor:
        rows = await cursor.fetchall()
        last_checked_cache = dict(rows)
