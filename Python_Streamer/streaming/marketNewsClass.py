from os import getenv
from core.appState import app_state
from pprint import pprint
from datetime import datetime, timedelta

class MarketNews:
    def __init__(self):
        self.api_key = getenv("GNewsKey")
        self.finnhub_client = app_state.finnhub_client
    
    async def get_market_news(self):
        url_dict = {}
        response = self.finnhub_client.general_news('top news', min_id=0)
        for article in response:
            if article['category'] == "top news":
                url_dict[article['headline']] = article['url']
                
        return url_dict

    async def get_company_news(self, ticker):
        url_dict = {}
        to = self.get_today_date()
        response = self.finnhub_client.company_news(ticker, _from=to, to=to)
        for article in response:
            url_dict[article['headline']] = article['url']
                
        return url_dict

    def get_today_date(self):
        # Get current date/time
        today = datetime.now()
        # Format both as YYYY-MM-DD
        today_str = today.strftime('%Y-%m-%d')
        
        return today_str


