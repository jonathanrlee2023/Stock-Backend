import asyncio
import json
from operator import itemgetter
import time
import orjson
from redis import asyncio as aioredis
import aiosqlite
import requests
from companyClass import Company
from dataloader import DataLoader
import stream_func


stream_started = False
stream_lock = asyncio.Lock()
is_weekday_business_hours_central = stream_func.is_weekday_business_hours_central()
is_market_closed = stream_func.is_market_closed()
option_attributes = ['askPrice', 'bidPrice', 'lastPrice', 'highPrice', 'volatility', 'delta', 'gamma', 'theta', 'vega', 'mark']
parsed_labels = ['Ask Price',  'Bid Price', 'Last Price', 'High Price', 'IV', 'Delta', 'Gamma', 'Theta', 'Vega', 'Mark']
get_option_stats = itemgetter(*option_attributes)
symbol_cache = {}
r = aioredis.Redis(host='localhost', port=6380, db=0)

async def listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client, db):
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
    pubsub = r.pubsub()
    await pubsub.subscribe('Request_Channel')
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
            data = json.loads(message["data"])
            
            # 🛑 FILTER: If this is just a confirmation message, IGNORE IT
            if "Status" in data:
                continue
            if isinstance(data, list) and data:
                first = data[0]

                # OptionStreamRequest array
                if "price" in first:
                    for opt in data:
                        symbol      = opt["symbol"]
                        price       = opt["price"]
                        day         = opt["day"]
                        month       = opt["month"]
                        year        = opt["year"]
                        option_type = opt["type"]

                        if symbol not in option_ids:
                            if not is_weekday_business_hours_central:
                                asyncio.create_task(
                                    stream_func.start_options_stream(
                                        ticker=symbol,
                                        price=price,
                                        day=day,
                                        month=month,
                                        year=year,
                                        type=option_type,
                                    )
                                )
                        else:
                            print(f"Stream for {symbol} {option_type} at ${price} on {month}/{day}/{year} already started.")

                # StockStreamRequest array
                else:
                    symbol = data.get("symbol")
                    if not symbol:
                        print("Skipping malformed or empty request:", data)
                        continue
                    
                    # ... (rest of your single request logic)
                    for stk in data:
                        symbol = stk["symbol"]

                        if symbol not in streamer.subscriptions.get("LEVELONE_EQUITIES", {}):
                            if not is_weekday_business_hours_central:
                                asyncio.create_task(
                                    stream_func.start_stock_stream(
                                        streamer=streamer,
                                        ticker=symbol,
                                    )
                                )
                        else:
                            print(f"Stream for {symbol} already started.")  
                        await asyncio.gather(
                            get_options_and_initial_quotes(symbol, client, db),
                            handleCompany(client, alpha_vantage_api_key, rate_api_key, symbol)
                        )

            else:
                # Fallback for a single‐item dict (old behavior) or unexpected shape
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
                                        ticker=symbol,
                                        price=price,
                                        day=day,
                                        month=month,
                                        year=year,
                                        type=option_type
                                    )
                                )                            
                        else: 
                            print(f"Stream for {symbol} {option_type} at ${price} on {month}/{day}/{year} already started.")
                    else:
                        if symbol not in streamer.subscriptions.get('LEVELONE_EQUITIES', {}):
                            if not is_market_closed:
                                asyncio.create_task(
                                    stream_func.start_stock_stream(
                                        streamer=streamer,
                                        ticker=symbol,
                                    )
                                )
                        else:
                            print(f"Stream for {symbol} already started.")
                        await asyncio.gather(
                            get_options_and_initial_quotes(symbol, client, db),
                            handleCompany(client, alpha_vantage_api_key, rate_api_key, symbol)
                        )
                except requests.exceptions.ReadTimeout:
                    print("Timeout connecting to Schwab API.")
                except Exception as e:
                    print("Stream handling error:", e)

async def get_symbol_id(db, symbol):
    """Helper to get ID from cache or DB, creating it if necessary."""
    # 1. Check RAM Cache first
    if symbol in symbol_cache:
        return symbol_cache[symbol]

    # 2. Check DB if not in cache
    async with db.execute("SELECT symbol_id FROM Symbols WHERE symbol = ?", (symbol,)) as cursor:
        row = await cursor.fetchone()
        if row:
            symbol_cache[symbol] = row[0]
            return row[0]

    # 3. If totally new, insert it
    # We use the cursor returned by execute to get the lastrowid
    cursor = await db.execute("INSERT INTO Symbols (symbol) VALUES (?)", (symbol,))
    new_id = cursor.lastrowid 
    
    # Optional but recommended: commit new symbols immediately 
    # so other tasks can see them
    await db.commit()
    
    symbol_cache[symbol] = new_id
    return new_id

async def init_db():
    db = await aiosqlite.connect('PriceData.db')
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

async def write_to_db(db):
    """
    Write data to the database. This function is designed to run in a loop and be called
    every 15 seconds. It will create the tables and indices if they do not exist,
    and then write the data in the snapshot to the database.

    :return: None
    """
    async with db.execute("SELECT symbol, symbol_id FROM Symbols") as cursor:
        async for row in cursor:
            symbol_cache[row[0]] = row[1]

    while True:
        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1: wait_time = 15
        await asyncio.sleep(wait_time)
        timestamp = int(time.time())    
        
        snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names}

        option_records = []
        stock_records = []

        for name, data in snapshot.items():
            try:
                if name in symbol_cache:
                    s_id = symbol_cache[name]
                else:
                    # Only await if it's a brand new symbol we haven't seen since startup
                    s_id = await get_symbol_id(db, name)
                if len(name) > 8: # Option logic
                    option_records.append((
                        timestamp, s_id, data.get("Mark"), data.get("Bid Price"),
                        data.get("Ask Price"), data.get("Last Price"), data.get("High Price"),
                        data.get("IV"), data.get("Delta"), data.get("Gamma"),
                        data.get("Theta"), data.get("Vega")
                    ))
                else: # Stock logic
                    stock_records.append((
                        timestamp, s_id, data.get("Mark"), data.get("Bid Price"),
                        data.get("Ask Price"), data.get("Last Price"), 
                        data.get("Bid Size"), data.get("Ask Size")
                    ))
            except KeyError as e:
                print(f"Skipping {name}: Missing key {e}")


        if option_records:
            await db.executemany(
                "INSERT OR IGNORE INTO Options VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", 
                option_records
            )
        
        if stock_records:
            await db.executemany(
                "INSERT OR IGNORE INTO Stocks VALUES (?,?,?,?,?,?,?,?)", 
                stock_records
            )

        await db.commit()

async def stream_options(client):
    """
    Streams options data from the client and updates the local data storage.
    
    Parameters
    ----------
    client : schwabdev.Client
        The client instance to use for fetching the options data.
    
    Returns
    -------
    None
    """
        
    option_ids = stream_func.option_ids
    new_data = stream_func.new_data
    while True:
        if not option_ids:
            wait_time = 5 - (time.time() % 5)
            if wait_time < 0.1: wait_time = 5
            await asyncio.sleep(wait_time)
            continue
        response = client.quotes(option_ids)

        start_time = time.perf_counter()
        data = orjson.loads(response.content) 
            
        local_fetcher = get_option_stats
        local_new_data = new_data
        
        for symbol, details in data.items():
            try:
                # EAFP: Assume the symbol exists. This is faster than 'if symbol in'
                t = local_new_data[symbol]
            except KeyError:
                # Only happens once per symbol
                t = local_new_data[symbol] = {}
            try:
                v = local_fetcher(details['quote'])
                
                # Manual unrolling: Faster than zip() because it avoids iterator overhead
                t['Bid Price']   = v[0]
                t['Ask Price']   = v[1]
                t['Last Price']  = v[2]
                t['High Price']  = v[3]
                t['IV']          = v[4]
                t['Delta']       = v[5]
                t['Gamma']       = v[6]
                t['Theta']       = v[7]
                t['Vega']        = v[8]
                t['Mark']        = v[9]
            except KeyError:
                continue
            
        print(f"Extraction & Renaming: {time.perf_counter() - start_time:.6f}s")
        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1: wait_time = 15
        await asyncio.sleep(wait_time)
    

    
    
async def broadcast_to_redis():
    """
    Broadcasts the current snapshot of data to the Redis channel named "Stream_Channel".
    This function will sleep for 15 seconds minus the current time modulo 15 seconds
    and then broadcast the snapshot. This is done to ensure that Redis receives updates
    at a consistent interval.
    """
    new_data = stream_func.new_data
    file_names = stream_func.file_names 
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")
    if not is_weekday_business_hours_central:
        print("Stream is closed.")
    while True:
        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1: wait_time = 15
        await asyncio.sleep(wait_time)

        if stream_func.file_names:
            snapshot = {name: new_data[name] for name in file_names}
            await r.publish("Stream_Channel", json.dumps(snapshot))

async def update_tickers_from_db(streamer):
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
    
    async with aiosqlite.connect("Tracker.db") as db:
        async with db.execute("SELECT id FROM tracker") as cursor:
            rows = await cursor.fetchall()
            ticker_list = [row[0] for row in rows]
            
    if ticker_list:
        for ticker in ticker_list:
            if len(ticker) > 8: # Option logic
                parsed = stream_func.parse_option(ticker)
                if parsed:
                    asyncio.create_task(
                                stream_func.start_options_stream(
                                    ticker=parsed["ticker"],
                                    price=parsed["price"],
                                    day=parsed["day"],
                                    month=parsed["month"],
                                    year=parsed["year"],
                                    type=parsed["type"],
                                )
                            )
            else: # Stock logic
                asyncio.create_task(
                            stream_func.start_stock_stream(
                                streamer=streamer,
                                ticker=ticker
                            )
                        )

        print(f"Subscribing to: {ticker_list}")
        
    else:
        print("No tickers found in tracker.db")

async def handleCompany(client, api_key, rate_key, ticker):
    """
    Handles a company request by fetching the company's data and publishing the final report to the Redis channel.

    Parameters
    ----------
    client : schwabdev.Client
        The client instance to use for fetching the company's data.
    api_key : str
        The Alpha Vantage API key to use for fetching the company's data.
    rate_key : str
        The ExchangeRate API key to use for fetching the company's data.
    ticker : str
        The ticker symbol of the company to fetch data for.

    Returns
    -------
    None
    """
    company = await Company.create(ticker=ticker, api_key=api_key, rate_api_key=rate_key, client=client)
    if company is None:
        print("Company data not fetched properly")
        return
    await r.publish("Company_Channel", json.dumps(company.final_report))

async def get_options_and_initial_quotes(ticker, client, db):
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
    s_id = await get_symbol_id(db, ticker)
    option_ids = stream_func.option_ids
    call_option_id_list = []
    put_option_id_list = []
    
    loader = DataLoader(ticker=ticker, connection=client)

    quote = loader.get_quote()

    async with db.execute(
        "SELECT timestamp, open, high, low, close, volume FROM HistoricalStocks WHERE symbol_id = ? ORDER BY timestamp ASC", 
        (s_id,)
    ) as cursor:
        rows = await cursor.fetchall()

    if rows:
        price_history = [
            {"timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} 
            for r in rows
        ]
        print(f"Loaded {ticker} history from Cache (DB)")
    else:
        price_history = loader.get_price_history() # Assuming this returns a list of dicts

        if price_history:
            history_records = [
                (item['timestamp'], s_id, item['open'], item['high'], item['low'], item['close'], item['volume'])
                for item in price_history
            ]
            
            await db.executemany(
                "INSERT OR IGNORE INTO HistoricalStocks VALUES (?, ?, ?, ?, ?, ?, ?)",
                history_records
            )
            await db.commit()
            print(f"Saved {ticker} history to DB")

    call_option_id_list, put_option_id_list = loader.get_option_expirations()
    option_ids.extend(call_option_id_list)
    option_ids.extend(put_option_id_list)
    quote_dict = {
            "Symbol": ticker,
            "timestamp": int(quote['quoteTime'] // 1000),
            "BidPrice": quote['bidPrice'],
            "AskPrice": quote['askPrice'],
            "LastPrice": quote['lastPrice'],
            "Mark": quote['mark']
        }
    
    await r.publish("One_Time_Data_Channel", json.dumps({"Symbol": ticker, "Quote": quote_dict, "PriceHistory": price_history, "Call": call_option_id_list, "Put": put_option_id_list}))