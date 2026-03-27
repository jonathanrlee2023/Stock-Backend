import asyncio
from datetime import datetime, timedelta
from cache import latest_quote
import time
import schwabdev
import os
from dotenv import load_dotenv
from dbState import db_state


class DataLoader:
    def __init__(self, ticker, symbol_id, connection, fiscalDate=None):
        self.fiscalDate = fiscalDate
        if connection is not None:
            self.connection = connection
        else:
            self.connection = self.establish_connection()
        self.ticker = ticker
        self.symbol_id = symbol_id

    def establish_connection(self):
        load_dotenv()  # loads variables from .env into environment

        appKey = os.getenv("appKey")
        appSecret = os.getenv("appSecret")
        if not appKey or not appSecret:
            raise ValueError("appKey and appSecret must be set in the environment variables.")
        
        client = schwabdev.Client(app_key=appKey, app_secret=appSecret)
        return client
    
    async def load_data(self):
        db = db_state.price_db
        start = time.perf_counter()
        try:
            if self.fiscalDate is None:
                return None
            start_ms = int(self.fiscalDate * 1000)
            end_ms = start_ms + (7 * 86400 * 1000)
            async with db.execute(
                f"SELECT close FROM HistoricalStocks WHERE symbol_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC LIMIT 1", 
                (self.symbol_id, start_ms, end_ms)
            ) as cursor:
                rows = await cursor.fetchall()
            if rows:
                return rows[0][0]
        
            def fetch_api():
                return self.connection.price_history(
                    symbol=self.ticker,
                    periodType="year",
                    frequencyType="daily",
                    startDate=start_ms,
                    endDate=end_ms
                )

            response = await asyncio.to_thread(fetch_api)
            
            if response.status_code == 200:
                quote = response.json()
                if 'candles' in quote and quote['candles']:
                    print(f"API Success: {time.perf_counter() - start:.4f}s")
                    return quote['candles'][0]['close']
        except Exception as e:
            print(f"Schwab API Error: {e}")
        return None
    
    async def get_price_history(self):
        s_id = self.symbol_id
        priceDB = db_state.price_db
        try:
            async with priceDB.execute(
                "SELECT timestamp, open, high, low, close, volume FROM HistoricalStocks WHERE symbol_id = ? ORDER BY timestamp ASC", 
                (s_id,)
            ) as cursor:
                rows = await cursor.fetchall()

            if rows:
                price_history = [
                    {"timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} 
                    for r in rows
                ]
                print(f"Loaded {self.ticker} history from Cache (DB)")
                return price_history
        except Exception as e:
            print(f"Price Database Error: {e}")
        try:
            start_ms = self.get_unix_timestamp_5_years_ago()
            # One year ago in milliseconds
            one_year_ago_ms = int((time.time() - (365 * 24 * 60 * 60)) * 1000)

            response = self.connection.price_history(
                symbol=self.ticker,
                periodType="year",
                frequencyType="daily",
                startDate=start_ms,
            )
            
            if response.status_code != 200:
                return None
                
            quote = response.json()
            raw_candles = quote.get('candles', [])
            
            if not raw_candles:
                return None

            filtered_candles = []
            # We use a counter for the "every 5th" logic
            skip_counter = 0

            # Iterate through candles (Schwab returns them Chronological: Oldest -> Newest)
            for candle in raw_candles:
                if 'datetime' in candle:
                    candle['timestamp'] = candle.pop('datetime')
                
                if candle['timestamp'] >= one_year_ago_ms:
                    filtered_candles.append(candle)
                else:
                    # OLD: Only keep every 5th candle
                    if skip_counter % 5 == 0:
                        filtered_candles.append(candle)
                    skip_counter += 1

            return filtered_candles

        except Exception as e:
            print(f"Schwab API Error: {e}")
        return None
    
    async def get_recent_price_history(self):
        global latest_quote
        start_ms = 0
        today_ms = int(time.time() * 1000)
        raw_candles = []
        needs_update = False
        s_id = self.symbol_id
        
        priceDB = db_state.price_db
        start_ms = latest_quote.get(s_id, 0)
        if start_ms == 0: 
            result = await priceDB.execute(
                "SELECT timestamp, symbol_id FROM HistoricalStocks WHERE symbol_id = ? ORDER BY timestamp DESC LIMIT 1", 
                (self.symbol_id,)
            )

            # 2. Extract the row
            row = await result.fetchone()
            if row:
                start_ms = row[0]
            else:
                print("No rows found") 
                raw_candles = await self.get_price_history()           
        date_start = datetime.fromtimestamp(start_ms / 1000.0).date()
        date_today = datetime.fromtimestamp(today_ms / 1000.0).date()

        if date_start == date_today:
            raw_candles = await self.get_price_history()
        else:
            print(f"Updating {self.ticker} history from {date_start} to {date_today}")
            needs_update = True
        
        try:
            if needs_update:
                response = self.connection.price_history(
                    symbol=self.ticker,
                    periodType="year",
                    frequencyType="daily",
                    startDate=start_ms,
                )
                
                if response.status_code != 200:
                    print(f"API Error: Status {response.status_code}")
                    return None
                    
                quotes = response.json()
                new_candles = quotes.get('candles', [])
                latest_quote[s_id] = quotes.get('previousCloseDate', 0)
                
                if not new_candles:
                    return None                
                
                raw_candles.extend(new_candles)

        except Exception as e:
            print(f"Schwab API Error: {e}")
        try:
            history_records = [
                (item.get('timestamp') or item.get('datetime'), s_id, item['open'], item['high'], item['low'], item['close'], item['volume'])
                for item in raw_candles
            ]
            await priceDB.executemany(
                "INSERT OR IGNORE INTO HistoricalStocks VALUES (?, ?, ?, ?, ?, ?, ?)",
                history_records
            )
            await priceDB.commit()
            print(f"Saved {self.ticker} history to DB")

        except Exception as e:
            print(f"Price Database Error: {e}")
        return await self.get_price_history()
    
    def get_quote(self):
        response = self.connection.quote(symbol_id=self.ticker).json()
        data = response[self.ticker]['quote']
        return data
    
    def get_option_expirations(self):
        call_option_id_list = []
        put_option_id_list = []
        fromDate = datetime.now()
        toDate = fromDate + timedelta(days=7)
        response = self.connection.option_chains(symbol=self.ticker, strikeCount=5, optionType="ALL", toDate=toDate).json()
        call_data = response['callExpDateMap']
        put_data = response['putExpDateMap']
        for date, contract in call_data.items():
            for strike, options_list in contract.items():
                # options_list is a list, e.g., [{ 'id': '...', 'symbol': '...' }]
                if options_list: 
                    call_option_id_list.append(options_list[0]['symbol'])
        for date, contract in put_data.items():
            for strike, options_list in contract.items():
                if options_list:
                    put_option_id_list.append(options_list[0]['symbol'])

        return call_option_id_list, put_option_id_list

    
    def get_unix_timestamp_5_years_ago(self):
        now = datetime.now()
        
        try:
            # Subtract 5 years from the current year
            five_years_ago = now.replace(year=now.year - 5)
        except ValueError:
            # This handles the edge case of February 29th.
            # If today is Feb 29 and 5 years ago wasn't a leap year, 
            # we fall back to Feb 28th.
            five_years_ago = now.replace(year=now.year - 5, day=now.day - 1)
            
        # Convert to Unix timestamp (integer)
        return int(time.mktime(five_years_ago.timetuple()))