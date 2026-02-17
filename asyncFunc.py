import asyncio
import json
import time
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
r = aioredis.Redis(host='localhost', port=6380, db=0)

async def listen_for_messages(streamer, alpha_vantage_api_key, rate_api_key, client):
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
    print("Listening for messages…")
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
                    print("Starting streamer…")
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

                        if symbol not in streamer.subscriptions.get("LEVELONE_OPTIONS", {}):
                            if not is_weekday_business_hours_central:
                                asyncio.create_task(
                                    stream_func.start_options_stream(
                                        streamer=streamer,
                                        ticker=symbol,
                                        price=price,
                                        day=day,
                                        month=month,
                                        year=year,
                                        type=option_type,
                                    )
                                )

                                print(f"Started Option Stream Request for: {symbol} {option_type} at ${price} on {month}/{day}/{year}")
                        else:
                            print(f"Stream for {symbol} {option_type} at ${price} on {month}/{day}/{year} already started.")

                # StockStreamRequest array
                else:
                    symbol = data.get("symbol")
                    if not symbol:
                        print("Skipping malformed or empty request:", data)
                        continue
                    
                    print("Handling single request:", data)
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
                            

                                print(f"Started Stock Stream Request for: {symbol}") 
                        else:
                            print(f"Stream for {symbol} already started.")  
                        await asyncio.gather(
                            get_option_expiration_chain(symbol, client),
                            handleCompany(client, alpha_vantage_api_key, rate_api_key, symbol)
                        )

            else:
                # Fallback for a single‐item dict (old behavior) or unexpected shape
                symbol = data.get("symbol")
                print("Handling single request:", data)

                try:
                    if "price" in data:
                        price = data["price"]
                        day = data["day"]
                        month = data["month"]
                        year = data["year"]
                        option_type = data["type"]
                        if symbol not in streamer.subscriptions.get('LEVELONE_OPTIONS', {}):
                            if not is_market_closed():
                                asyncio.create_task(
                                    stream_func.start_options_stream(
                                        streamer=streamer,
                                        ticker=symbol,
                                        price=price,
                                        day=day,
                                        month=month,
                                        year=year,
                                        type=option_type
                                    )
                                )
                            
                                print(f"Started Option Stream Request for: {symbol} {option_type} at ${price} on {month}/{day}/{year}")
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
                                print(f"Started Stream Request for: {symbol}")
                        else:
                            print(f"Stream for {symbol} already started.")
                        await asyncio.gather(
                            get_option_expiration_chain(symbol, client),
                            handleCompany(client, alpha_vantage_api_key, rate_api_key, symbol)
                        )
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
    async with aiosqlite.connect('PriceData.db') as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS Options (
                timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
                bid_price REAL, ask_price REAL, last_price REAL, high_price REAL,
                iv REAL, delta REAL, gamma REAL, theta REAL, vega REAL,
                PRIMARY KEY (timestamp, symbol)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS Stocks (
                timestamp INTEGER NOT NULL, symbol TEXT NOT NULL, mark REAL,
                bid_price REAL, ask_price REAL, last_price REAL, bid_size INTEGER, ask_size INTEGER,
                PRIMARY KEY (timestamp, symbol)
            )
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_options_symbol ON Options (symbol, timestamp)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_stocks_symbol ON Stocks (symbol, timestamp)")
        await db.commit()

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
                    if len(name) > 8: # Option logic
                        option_records.append((
                            timestamp, name, data.get("Mark"), data.get("Bid Price"),
                            data.get("Ask Price"), data.get("Last Price"), data.get("High Price"),
                            data.get("IV"), data.get("Delta"), data.get("Gamma"),
                            data.get("Theta"), data.get("Vega")
                        ))
                    else: # Stock logic
                        stock_records.append((
                            timestamp, name, data.get("Mark"), data.get("Bid Price"),
                            data.get("Ask Price"), data.get("Last Price"), 
                            data.get("Bid Size"), data.get("Ask Size")
                        ))
                except KeyError as e:
                    print(f"Skipping {name}: Missing key {e}")


            if option_records:
                await db.executemany(
                    "INSERT OR REPLACE INTO Options VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", 
                    option_records
                )
            
            if stock_records:
                await db.executemany(
                    "INSERT OR REPLACE INTO Stocks VALUES (?,?,?,?,?,?,?,?)", 
                    stock_records
                )

            await db.commit()
            print("Wrote to DB")
    
    
async def broadcast_to_redis():
    """
    Broadcasts the current snapshot of data to the Redis channel named "Stream_Channel".
    This function will sleep for 15 seconds minus the current time modulo 15 seconds
    and then broadcast the snapshot. This is done to ensure that Redis receives updates
    at a consistent interval.
    """
    print("Called")
    try:
        if await r.ping():
            print("✅ Redis Connection Successful!")
    except aioredis.ConnectionError:
        print("❌ Redis Connection Failed. Is the Docker container running?")
    if not is_weekday_business_hours_central():
        print("Stream is closed.")
    while True:
        wait_time = 15 - (time.time() % 15)
        if wait_time < 0.1: wait_time = 15
        await asyncio.sleep(wait_time)

        if stream_func.file_names:
            snapshot = {name: stream_func.new_data[name] for name in stream_func.file_names}
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
                                    streamer=streamer,
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

async def get_option_expiration_chain(ticker, client):
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
    call_option_id_list = []
    put_option_id_list = []
    
    loader = DataLoader(ticker=ticker, connection=client)

    quote = loader.get_quote()

    # print(f"quote {quote}")
    price_history = loader.get_price_history()
    # print(f"price history {price_history[0]}")

    call_option_id_list, put_option_id_list = loader.get_option_expirations()
    quote_dict = {
            "Symbol": ticker,
            "timestamp": int(quote['quoteTime'] // 1000),
            "BidPrice": quote['bidPrice'],
            "AskPrice": quote['askPrice'],
            "LastPrice": quote['lastPrice'],
            "Mark": quote['mark']
        }
    
    # print(quote_dict)
    await r.publish("One_Time_Data_Channel", json.dumps({"Symbol": ticker, "Quote": quote_dict, "PriceHistory": price_history, "Call": call_option_id_list, "Put": put_option_id_list}))