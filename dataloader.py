import datetime
from pprint import pprint
import time
import schwabdev
import os
from dotenv import load_dotenv


class DataLoader:
    def __init__(self, ticker, connection, fiscalDate=None):
        self.fiscalDate = fiscalDate
        if connection is not None:
            self.connection = connection
        else:
            self.connection = self.establish_connection()
        self.ticker = ticker

    def establish_connection(self):
        load_dotenv()  # loads variables from .env into environment

        appKey = os.getenv("appKey")
        appSecret = os.getenv("appSecret")
        if not appKey or not appSecret:
            raise ValueError("appKey and appSecret must be set in the environment variables.")
        
        client = schwabdev.Client(app_key=appKey, app_secret=appSecret)
        return client
    
    def load_data(self):
        try:
            if self.fiscalDate is None:
                return None
            start_ms = int(self.fiscalDate * 1000)
            end_ms = start_ms + (2 * 86400 * 1000)
            
            response = self.connection.price_history(
                symbol=self.ticker,
                periodType="year",
                frequencyType="daily",
                startDate=start_ms,
                endDate=end_ms
            )
            
            # Ensure the response is valid before calling .json()
            if response.status_code != 200:
                return None
                
            quote = response.json()
            if 'candles' in quote and quote['candles']:
                return quote['candles'][0]['close']
        except Exception as e:
            print(f"Schwab API Error: {e}")
        return None
    
    def get_price_history(self):
        try:
            start_ms = self.get_unix_timestamp_5_years_ago()
            
            response = self.connection.price_history(
                symbol=self.ticker,
                periodType="year",
                frequencyType="daily",
                startDate=start_ms,
            )
            
            # Ensure the response is valid before calling .json()
            if response.status_code != 200:
                return None
                
            quote = response.json()
            if 'candles' in quote and quote['candles']:
                for candle in quote.get('candles', []):
                    if 'datetime' in candle:
                        candle['timestamp'] = candle.pop('datetime')
                return quote['candles']
        except Exception as e:
            print(f"Schwab API Error: {e}")
        return None
    
    def get_quote(self):
        response = self.connection.quote(symbol_id=self.ticker).json()
        data = response[self.ticker]['quote']
        return data
    
    def get_option_expirations(self):
        call_option_id_list = []
        put_option_id_list = []
        fromDate = datetime.datetime.now()
        toDate = fromDate + datetime.timedelta(days=7)
        response = self.connection.option_chains(symbol=self.ticker, strikeCount=3, optionType="ALL", toDate=toDate).json()
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
        now = datetime.datetime.now()
        
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