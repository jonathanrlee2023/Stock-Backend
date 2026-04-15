import pandas as pd
import asyncio

from sqlalchemy import text
from FinancialDataSource import FinancialDataSource
from appState import app_state
from cache import exchange_rate_cache

class CompanyDataFetcher(FinancialDataSource):
    def __init__(self, ticker, symbol_id, api_key, rate_api_key, data, client):
        self.ticker = ticker
        self.symbol_id = symbol_id
        self.api_key = api_key
        self.rate_api_key = rate_api_key
        self.client = client
        self.data = data

    async def fetch_fundamentals(self, category):
        CATEGORY_MAP = {
            "income": "INCOME_STATEMENT",
            "balance": "BALANCE_SHEET",
            "cash": "CASH_FLOW",
            "earnings": "EARNINGS",
        }
        if category == "overview":
            return await self.get_company_overviews()     

        api_query = CATEGORY_MAP.get(category, None)
        raw_data = await self._get_api_response(api_query)
        if not raw_data:
            return False

        # 2. Parsing Logic (Encapsulated)
        annual_df, quarterly_df = self._parse_category_data(category, raw_data)

        # 3. Storage
        self.data[category]["annual"] = annual_df
        self.data[category]["quarterly"] = quarterly_df

        return True

    async def _get_api_response(self, category):
        """Handles the locking, fetching, and rate limit errors."""
        await self.api_key.lock_key()
        try:
            if self.api_key.rate_limited:
                return None

            url = f"https://www.alphavantage.co/query?function={category}&symbol={self.ticker}&apikey={self.api_key.key}"
            response = await self.client.get(url)
            data = response.json()

            if any(key in data for key in ["Error Message", "Information"]):
                print(f"API Error for {self.ticker}: RATE LIMIT EXCEEDED")
                self.api_key.rate_limit()
                return None
            
            return data
        except Exception as e:
            print(f"Fetch Error: {e}")
            return None
        finally:
            await asyncio.sleep(1)
            self.api_key.unlock_key()

    def _parse_category_data(self, category, data):
        """Determines which keys to use based on category."""
        if category == "earnings":
            a_key, q_key = "annualEarnings", "quarterlyEarnings"
        else:
            a_key, q_key = "annualReports", "quarterlyReports"

        annual_df = pd.DataFrame(data.get(a_key, []))
        quarterly_df = pd.DataFrame(data.get(q_key, []))

        # Common cleaning
        annual_df = self._prepare_df(annual_df, "annual")
        quarterly_df = self._prepare_df(quarterly_df, "quarterly")

        return annual_df, quarterly_df

    def _prepare_df(self, df, report_type):
        """Standardizes columns and cleans data."""
        if df.empty:
            return df
        
        df = df.rename(columns={"fiscalDateEnding": "date"})
        df["ticker"] = self.ticker
        df["report_type"] = report_type
        
        # Clean numeric cols (except specific ones)
        return self._clean_financial_df(df, date_col="date")

    def _clean_financial_df(self, df, date_col, scale=1):
        exclude = [date_col, "reportedCurrency", "ticker", "report_type"]
        cols = [c for c in df.columns if c not in exclude]

        for col in cols:
            df[col] = (
                df[col]
                .replace("None", pd.NA)
                .astype(str)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )

        df[cols] = df[cols] / scale
        return df

    async def get_company_overviews(self):
        ticker = self.ticker

        # 2. Fetch from API
        client = app_state.httpx_client
        await self.api_key.lock_key()
        try:
            url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={self.api_key.key}"
            if self.api_key.rate_limited:
                self.api_key.unlock_key()
                return False
            response = await client.get(url)
            data = response.json()

            if "Error Message" in data or "Information" in data:
                print(f"API Error for {ticker}: RATE LIMIT EXCEEDED")
                self.api_key.rate_limit()
                return False

            if not data:
                print(f"No data found for {ticker}")
                return False
        except Exception as e:
            print(f"API Error for {ticker}: {e}")
            self.api_key.unlock_key()
            return False
        finally:
            await asyncio.sleep(1)
            self.api_key.unlock_key()

        new_df = pd.DataFrame([data])
        start_idx = new_df.columns.get_loc("LatestQuarter") + 1

        cols_to_fix = new_df.columns[start_idx:]

        new_df[cols_to_fix] = new_df[cols_to_fix].apply(pd.to_numeric, errors="coerce")

        # Load the newly saved data into the class attribute
        self.data["overview"] = new_df
        return True

    async def replace_with_usd(self):
        # API key for the exchange rate service
        """
        Replaces all non-USD currency values in the memory dictionary with their USD equivalents.

        This method queries the exchange rate API service to retrieve the latest conversion rates.

        It then iterates through the memory dictionary and applies the conversion rates to the numeric columns of the dataframes.

        The 'reportedCurrency' column is updated to 'USD' for all dataframes.

        Parameters

        ----------
        self : Company
            The Company object containing the data to be standardized.

        Returns
        -------
        None

        Notes
        -----
        This method assumes that the 'reportedCurrency' column exists in all dataframes and that the conversion rates are available in the exchange rate API service.

        The method does not handle errors in the event that the API service is unavailable or returns invalid data.
        """
        global exchange_rate_cache
        rate_url = f"https://v6.exchangerate-api.com/v6/{self.rate_api_key}/latest/USD"

        if exchange_rate_cache:
            rates_dict = exchange_rate_cache
        else:
            client = app_state.httpx_client

            resp = await client.get(rate_url)
            rates_dict = resp.json().get("conversion_rates", {})

            exchange_rate_cache = rates_dict

        # We iterate through our memory dictionary
        for category in ["income", "balance", "cash"]:
            for freq in ["annual", "quarterly"]:
                df = self.data[category][freq]
                if df.empty:
                    continue

                # Create conversion factors: 1.0 for USD, specific rate for others
                # This handles companies like Toyota (JPY) or ASML (EUR)
                conversion_factors = df["reportedCurrency"].map(rates_dict).fillna(1)

                # Identify numeric columns only
                protected_cols = ["ticker", "report_type", "date", "reportedCurrency"]
                numeric_cols = df.select_dtypes(include=["number"]).columns
                cols_to_convert = [c for c in numeric_cols if c not in protected_cols]

                # Vectorized conversion (Fast)
                df[cols_to_convert] = df[cols_to_convert].div(conversion_factors, axis=0).round(4)
                df["reportedCurrency"] = "USD"
