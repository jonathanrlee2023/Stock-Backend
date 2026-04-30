import asyncio
from operator import itemgetter
import os
import time
import orjson
from redis import asyncio as aioredis
import requests
import datetime
from backtesting import run_backtest
from appState import app_state
import stream_func
from dbUtils import get_symbol_id
from cache import earnings_lookup, max_fiscal_lookup, symbol_cache, last_checked_cache, r, POPULAR_ETFS

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
db_dir = os.getenv("DB_DIR", "../Database")
alpha_vantage_semaphore = asyncio.Semaphore(1)
schwab_api_semaphore = asyncio.Semaphore(5)

today = datetime.date.today()
today_str = today.isoformat()


async def date_refresher_task():
    global today, today_str, is_weekday_business_hours_central, is_market_closed
    while True:
        # Update the globals
        now = datetime.date.today()
        if now != today:
            today = now
            today_str = now.isoformat()
            is_weekday_business_hours_central = stream_func.is_weekday_business_hours_central()
            is_market_closed = stream_func.is_market_closed()

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
            if "UserPortfolio" in data:
                await run_backtest(data["UserPortfolio"], data["BenchmarkPortfolio"], data["DaysAgo"], data["ClientID"])
            elif "Status" in data:
                continue
            else:
                symbol = data.get("symbol")
                get_options_data = data.get("getOptionData")
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
                        tasks = []
                        if symbol not in streamer.subscriptions.get("LEVELONE_EQUITIES", {}):
                            if not is_market_closed:
                                tasks.append(stream_func.start_stock_stream(ticker=symbol))
                            tasks.append(handleCompany(alpha_vantage_api_key, rate_api_key, symbol))

                        tasks.append(get_options_and_initial_quotes(symbol, get_options_data))
                        results = await asyncio.gather(*tasks, return_exceptions=True)

                        for i, res in enumerate(results):
                            if isinstance(res, Exception):
                                print(f"Task {i} failed for {symbol}: {res}")
                except requests.exceptions.ReadTimeout:
                    print("Timeout connecting to Schwab API.")
                except Exception as e:
                    print("Stream handling error:", e)


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
    client = app_state.schwab_client
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
            print(parsed)
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
    schwab_tasks = [throttled_get_options_and_initial_quotes(c, get_options="No") for c in companies]
    await asyncio.gather(*schwab_tasks, return_exceptions=True)

    # Phase 3: Alpha Vantage fundamentals (slow, serialized)
    av_tasks = [throttled_handle_company(api_manager, rate_api_key, c) for c in companies]
    await asyncio.gather(*av_tasks, return_exceptions=True)


async def throttled_handle_company(api_manager, rate_api_key, ticker):
    async with alpha_vantage_semaphore:
        await handleCompany(api_manager, rate_api_key, ticker)
        await asyncio.sleep(0.1)
        return


async def throttled_get_options_and_initial_quotes(ticker, get_options):
    async with schwab_api_semaphore:
        await get_options_and_initial_quotes(ticker, get_options=get_options)
        await asyncio.sleep(1)
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
    print(f"📡 Fetching company data for {ticker}...")

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


async def get_options_and_initial_quotes(ticker, get_options):
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
    print(f"📡 Fetching initial data for {ticker}...")

    if symbol_cache.get(ticker) is None:
        s_id = await get_symbol_id(ticker)
    else:
        s_id = symbol_cache[ticker]
    option_ids = stream_func.option_ids
    file_names = stream_func.file_names
    call_option_id_list = []
    put_option_id_list = []

    loader = app_state.data_loader
    try:
        market_news = app_state.market_news
        news = await market_news.get_company_news(ticker)
    except Exception as e:
        print(f"Error fetching market news for {ticker}: {e}")
    try:
        tasks = [
            asyncio.to_thread(loader.get_quote, ticker),
            loader.get_recent_price_history(ticker=ticker, s_id=s_id),
            asyncio.to_thread(loader.get_option_expirations, ticker),
        ]

        quote, price_history, (call_option_id_list, put_option_id_list) = await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Error fetching initial data for {ticker}: {e}")
        return

    all_options = call_option_id_list + put_option_id_list
    if get_options == "Yes":
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
        "News": news,
    }

    await r.publish("One_Time_Data_Channel", orjson.dumps(payload))

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


