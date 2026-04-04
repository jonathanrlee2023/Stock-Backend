import asyncio
from datetime import datetime, timedelta
from cache import latest_quote
import time
import schwabdev
import os
from dotenv import load_dotenv
from appState import app_state
from cache import last_checked_cache


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
        db = app_state.price_db
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
        priceDB = app_state.price_db
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

            response = await asyncio.to_thread(self.connection.price_history,
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
        
        priceDB = app_state.price_db
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
                start_ms += 86400000
                if self.is_data_fresh(start_ms):
                    return await self.get_price_history()
                response = self.connection.price_history(
                    symbol=self.ticker,
                    periodType="year",
                    frequencyType="daily",
                    startDate=start_ms,
                )
                
                if response.status_code != 200:
                    print(f"API Error: Status {response.status_code} {response.content}")
                    return await self.get_price_history()
                    
                quotes = response.json()
                new_candles = quotes.get('candles', [])
                latest_quote[s_id] = quotes.get('previousCloseDate', 0)
                if not new_candles:
                    return await self.get_price_history()                
                
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
    
    def is_data_fresh(self, last_db_ts):
        if not last_db_ts:
            return False
            
        global last_checked_cache
        if self.symbol_id in last_checked_cache:
            last_checked_date = last_checked_cache[self.symbol_id]
            
            days_since_check = (today - last_checked_date).days
            
            if days_since_check < 7:
                return False
        last_date = datetime.fromtimestamp(last_db_ts / 1000.0).date()
        now = datetime.now()   
        today = now.date()     
        # 1. If last entry is today, we are fresh (for Daily bars)
        if last_date >= today:
            return True
            
        # 2. Weekend Logic: If today is Saturday(5) or Sunday(6)
        # and we have Friday's data, we are fresh.
        weekday = now.weekday()
        if weekday == 5: # Saturday
            return last_date >= (today - timedelta(days=1))
        if weekday == 6: # Sunday
            return last_date >= (today - timedelta(days=2))
            
        # 3. Monday Morning Logic: If it's before market open (9:30 AM ET)
        # and we have Friday's data, we are fresh.
        if weekday == 0 and now.hour < 10: # Rough check for pre-market
            return last_date >= (today - timedelta(days=3))

        return False
    
    def get_quote(self):
        response = self.connection.quote(self.ticker).json()
        return response.get(self.ticker, {}).get('quote', {})
    
    def get_option_expirations(self):
        fromDate = datetime.now()
        toDate = fromDate + timedelta(days=7)

        response = self.connection.option_chains(symbol=self.ticker, strikeCount=5, optionType="ALL", toDate=toDate).json()
        
        def extract_symbols(exp_map):
            symbols = []
            for date_map in exp_map.values():
                for strike_list in date_map.values():
                    if strike_list:
                        symbols.append(strike_list[0]['symbol'])
            return symbols

        call_ids = extract_symbols(response.get('callExpDateMap', {}))
        put_ids = extract_symbols(response.get('putExpDateMap', {}))

        return call_ids, put_ids

    
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