from pprint import pprint
import schwabdev
import os
from dotenv import load_dotenv


class DataLoader:
    def __init__(self, ticker, fiscalDate, connection, quote=None):
        self.fiscalDate = fiscalDate
        if connection is not None:
            self.connection = connection
        else:
            self.connection = self.establish_connection()
        self.ticker = ticker
        if quote is not None:
            self.quote = quote
        else:
            self.quote = self.load_data()
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