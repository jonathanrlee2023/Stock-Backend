import asyncio
import os
from pathlib import Path
from pprint import pprint
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import time
import pandas as pd
import numpy as np
import httpx

from dbState import db_state
import asyncFunc
from dataloader import DataLoader

pd.set_option('future.no_silent_downcasting', True)

RISK_FREE_RATE = 0.0375
MARKET_RISK_PREMIUM = 0.055
# Levered Beta averages for Robinhood/Morningstar Categories
# Benchmarked for 2024-2026 market conditions
SECTOR_BETAS = {
    # CYCLICAL (High Economic Sensitivity)
    "CONSUMER CYCLICAL": 1.15,      # AMZN, TSLA, NKE
    "FINANCIAL SERVICES": 1.05,     # JPM, GS, V
    "BASIC MATERIALS": 1.05,        # LIN, FCX, SHW
    "REAL ESTATE": 0.85,            # PLD, AMT (Sensitive to rates)

    # SENSITIVE (Follows the broader market)
    "TECHNOLOGY": 1.25,             # NVDA, MSFT, AAPL, ORCL
    "COMMUNICATION SERVICES": 1.15, # GOOGL, META, NFLX
    "INDUSTRIALS": 1.02,            # NOC, GE, UPS, BA
    "ENERGY": 0.90,                 # XOM, CVX

    # DEFENSIVE (Resistant to economic downturns)
    "HEALTHCARE": 0.85,             # UNH, JNJ, PFE
    "CONSUMER DEFENSIVE": 0.65,     # WMT, KO, PG, COST
    "UTILITIES": 0.55               # NEE, DUK
}



DEFENSIVE = ["HEALTHCARE", "CONSUMER DEFENSIVE", "UTILITIES"]
SENSITIVE = ["TECHNOLOGY", "COMMUNICATION SERVICES", "INDUSTRIALS", "ENERGY"]
CYCLICAL = ["CONSUMER CYCLICAL", "FINANCIAL SERVICES", "BASIC MATERIALS", "REAL ESTATE"]
class Company:
    def __init__(self, ticker, api_key, rate_api_key, client):
        # 1. Minimal setup: only assign non-I/O variables
        self.ticker = ticker
        print(self.ticker)
        self.api_key = api_key
        self.rate_api_key = rate_api_key
        self.client = client
        self.engines = db_state.engines

        self.data = {
            "income": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "balance": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "cash": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "earnings": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "overview": pd.DataFrame() # Overviews are usually 1 row
        }
        
        # Placeholders for data that will be loaded asynchronously
        self.income_df = None
        self.balance_df = None
        self.cash_df = None
        self.company_overview = None


    @classmethod
    async def create(cls, ticker, api_key, rate_api_key, client):
        """Asynchronous factory to create and fully initialize the instance."""
        self = cls(ticker, api_key, rate_api_key, client)
        self.symbol_id = await asyncFunc.get_symbol_id(self.ticker)

        await self.load_all_from_dbs()
        
        # 2. Run async I/O tasks
        # These will check the DBs, fetch if missing, and load into DataFrames
        if self.data["overview"].empty or self.data["income"]["annual"].empty or self.data["balance"]["annual"].empty or self.data["cash"]["annual"].empty:
            await asyncio.gather(
                self.get_fundamentals(),
                self.get_company_overviews()
            )
        

        await self.replace_with_usd()
        self.reorder_data()

        # Load the specific annual data needed for DCF calculations
        self.income_df = self.data["income"]["annual"]
        self.balance_df = self.data["balance"]["annual"]
        self.cash_df = self.data["cash"]["annual"]
        self.company_overview = self.data["overview"]

        if self.income_df.empty or self.balance_df.empty or self.cash_df.empty or self.company_overview.empty:
            print(f"--- Initialization Aborted for {ticker}: No data available ---")
            return None # Or handle as an error
        self.price_at_report = await self.get_current_price()


        # 3. Perform the Math (Sync tasks)
        self.fcf, self.fcff, self.nwc = self.calc_fcf()
        self.wacc = self.calc_wacc()
        self.calc_forecast_metrics()

        self.market_cap = self.company_overview["MarketCapitalization"].values[0]
        self.intrinsic_price, self.dividend_price = self.fcff_forecast()
        self.return_on_invested_capital = self.roic()
        self.peg = self.peg_ratio()
        self.sloan = self.sloan_ratio()
        self.hist_growth, self.forecasted_growth, self.trailing_peg, self.forward_peg = self.analyze_peg()

        self.earnings_date = await asyncFunc.get_furthest_date_for_stock(symbol=self.ticker)


        # Extract values from the loaded overview
        self._setup_analyst_ratings()
        def safe_float(val):
            try:
                if val is None or pd.isna(val):
                    return None # Or None, if your frontend prefers null over 0
                return float(val)
            except:
                return None

        # Helper to safely convert numpy/none to int
        def safe_int(val):
            try:
                if val is None or pd.isna(val):
                    return None
                return int(val)
            except:
                return None

        self.final_report = {
            "Symbol": str(self.ticker),
            "MarketCap": safe_int(self.market_cap),
            "PEG": safe_float(self.peg),
            "Sloan": safe_float(self.sloan),
            "ROIC": safe_float(self.return_on_invested_capital),
            "HistGrowth": safe_float(self.hist_growth),
            "ForecastedGrowth": safe_float(self.forecasted_growth),
            "TrailingPEG": safe_float(self.trailing_peg),
            "ForwardPEG": safe_float(self.forward_peg),
            "IntrinsicPrice": safe_float(self.intrinsic_price),
            "DividendPrice": safe_float(self.dividend_price),
            "PriceAtReport": safe_float(self.price_at_report),
            "WACC": safe_float(self.wacc),
            "FCFF": safe_float(self.fcff),
            "FCF": safe_float(self.fcf),
            "NWC": safe_float(self.nwc),
            "PriceTarget": safe_float(self.price_target),
            "StrongBuy": safe_int(self.strong_buy),
            "Buy": safe_int(self.buy),
            "Hold": safe_int(self.hold),
            "StrongSell": safe_int(self.strong_sell),
            "Sell": safe_int(self.sell),
            "EarningsDate": self.earnings_date,
        }

        await self.save_all_to_db()
        return self


    def _setup_analyst_ratings(self):
        self.price_target = float(self.company_overview["AnalystTargetPrice"].values[0])
        self.strong_buy = float(self.company_overview["AnalystRatingStrongBuy"].values[0])
        self.buy = float(self.company_overview["AnalystRatingBuy"].values[0])
        self.hold = float(self.company_overview["AnalystRatingHold"].values[0])
        self.strong_sell = float(self.company_overview["AnalystRatingStrongSell"].values[0])
        self.sell = float(self.company_overview["AnalystRatingSell"].values[0])

        self.total_ratings = self.strong_buy + self.buy + self.strong_sell + self.hold + self.sell

        self.percent_buy = (self.buy + self.strong_buy) / self.total_ratings
        self.percent_hold = self.hold / self.total_ratings
        self.percent_sell = (self.sell + self.strong_sell) / self.total_ratings

    async def _read_sql_to_df(self, table_name, engine, report_type):
        """Reads data from a specific fundamental DB."""
        async with engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT * FROM {table_name} WHERE ticker = :t AND report_type = :r"),
                {"t": self.ticker, "r": report_type}
            )
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    async def get_current_price(self) -> float:
        """Asynchronously fetches price data using the DataLoader."""
        self.unix_timestamp() # Sets self.timestamp

        loader = DataLoader(
                ticker=self.ticker, 
                fiscalDate=self.timestamp, 
                connection=self.client
            )
        
        # Corrected wrapper: Just call the synchronous method normally
        def sync_fetch():
            return loader.load_data() 
        
        # Run the wrapper in a thread pool
        price = await asyncio.to_thread(sync_fetch)
        
        return price
    def unix_timestamp(self):
        """
        Convert the latest fiscal date ending from the income statement to a Unix timestamp

        Parameters
        ----------
        None

        Returns
        -------
        int
            The Unix timestamp of the latest fiscal date ending
        """
        income_df = self.income_df
        timestamp = str(income_df["date"].values[-1]).split(' ')[0]        
        self.timestamp =  int(time.mktime(time.strptime(timestamp, '%Y-%m-%d')))

    async def get_fundamentals(self):
        """
        Asynchronously fetches the annual and quarterly financial data for a given ticker,
        which includes the income statement, balance sheet, cash flow, and earnings.
        The data is fetched from Alpha Vantage's API and stored in the instance's data dictionary.
        The function will return True if all data is fetched successfully, and False otherwise.

        Parameters
        ----------
        None

        Returns
        -------
        bool
            True if all data is fetched successfully, False otherwise
        """
        categories = ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS"]
        ticker = self.ticker
        symbol_id = self.symbol_id

        async with httpx.AsyncClient() as client:
            for category in categories:
                if category == "INCOME_STATEMENT": table_name = "income"
                elif category == "BALANCE_SHEET": table_name = "balance"
                elif category == "CASH_FLOW": table_name = "cash"
                elif category == "EARNINGS": table_name = "earnings"
                # 1. Define the specific DB for this category
                retrieve_new_data = await asyncFunc.check_for_new_earnings(ticker, symbol_id, table_name)

                engine = self.engines[table_name]
                # 2. Check if this ticker already has data in this specific DB
                # Note: We check a table named 'data' inside that specific DB
                already_exists = await self._check_db_for_ticker(table_name, engine, ticker)
                raw_engine = self.engines[f"RAW_{category}"]
                
                if retrieve_new_data or not already_exists:
                    raw_exists = await self._check_db_for_ticker(table_name, raw_engine, ticker)

                    if not raw_exists or retrieve_new_data:
                        url = f"https://www.alphavantage.co/query?function={category}&symbol={ticker}&apikey={self.api_key}"
                        response = await client.get(url)
                        data = response.json()

                        if "Error Message" in data or "Information" in data:
                            print(f"API Limit/Error for {ticker}: {data.get('Information', 'Unknown Error')}")
                            return False

                        try:
                            # 3. Process Data
                            if category != "EARNINGS":
                                annual_df = pd.DataFrame(data["annualReports"])
                                quarterly_df = pd.DataFrame(data["quarterlyReports"])
                                
                                annual_df = annual_df.rename(columns={'fiscalDateEnding': 'date'})
                                quarterly_df = quarterly_df.rename(columns={'fiscalDateEnding': 'date'})
                                
                                # Clean and Scale (passing the correct date column name)
                                annual_df = self._clean_financial_df(annual_df, date_col='date', scale=1_000_000)
                                quarterly_df = self._clean_financial_df(quarterly_df, date_col='date', scale=1_000_000)
                                
                            else:
                                annual_df = pd.DataFrame(data["annualEarnings"])
                                quarterly_df = pd.DataFrame(data["quarterlyEarnings"])

                                annual_df = annual_df.rename(columns={'fiscalDateEnding': 'date'})
                                quarterly_df = quarterly_df.rename(columns={'fiscalDateEnding': 'date'})
                                
                            self.data[table_name]["annual"] = annual_df
                            self.data[table_name]["quarterly"] = quarterly_df
                            # 4. Merge Annual and Quarterly into one DataFrame for the DB
                            # We standardized the columns so they stack perfectly

                            for df, r_type in [(annual_df, 'annual'), (quarterly_df, 'quarterly')]:
                                df['ticker'] = ticker
                                df['report_type'] = r_type
                            combined_df = pd.concat([annual_df, quarterly_df], ignore_index=True, sort=False)
                            
                            # Ensure 'fiscalDateEnding' is renamed to 'date' if not already done
                            if 'fiscalDateEnding' in combined_df.columns:
                                combined_df = combined_df.rename(columns={'fiscalDateEnding': 'date'})
                            
                            async with raw_engine.connect() as conn:
                                try:
                                    await self._upsert_to_database(raw_engine, combined_df, table_name=table_name)

                                except Exception as e:
                                    print(f"Database table check failed for RAW_{category}.db: {e}")
                                await conn.execute(text(f"DELETE FROM {table_name} WHERE ticker = :t"), {"t": self.ticker})
                            await asyncio.sleep(1.05) # Alpha Vantage Rate Limit
                            
                        except Exception as e:
                            print(f"Error processing {category} for {ticker}: {e}")
                            return False

                    else:
                        quarterly_df = await self._read_sql_to_df(table_name, engine=engine, report_type="quarterly")
                        annual_df = await self._read_sql_to_df(table_name, engine=engine, report_type="annual")
                        self.data[table_name]["annual"] = annual_df
                        self.data[table_name]["quarterly"] = quarterly_df                
            else:
                await self.load_all_from_dbs()
        return True
    async def load_all_from_dbs(self):
        """
        Checks all 5 databases and loads existing data into self.data structure.
        Separates quarterly and annual data for financials.
        """
        ticker = self.ticker
        symbol_id = self.symbol_id
        db_dir = os.getenv("DB_DIR", "/app/Database")
        
        # Map dictionary keys to their specific database filenames
        db_map = {
            "income": "income_statement.db",
            "balance": "balance_sheet.db",
            "cash": "cash_flow.db",
            "earnings": "earnings.db"
        }

        # 1. Load the 4 Financial/Time-Series Databases
        for cat_key, db_file in db_map.items():
            full_path = os.path.join(db_dir, db_file)
            retrieve_new_data = await asyncFunc.check_for_new_earnings(ticker, symbol_id, cat_key)
            if retrieve_new_data:
                continue

            if not os.path.exists(full_path):
                print(f"File not found: {full_path}") # Debug print
                continue
                
            engine = self.engines[cat_key]
            async with engine.connect() as conn:
                try:
                    check_sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{cat_key}'"
                    table_check = await conn.execute(text(check_sql))
                    if table_check.fetchone():
                        result = await conn.execute(
                            text(f"SELECT * FROM {cat_key} WHERE ticker = :t"), {"t": ticker}
                        )
                        df = pd.DataFrame(result.fetchall(), columns=result.keys())
                        
                        if not df.empty:
                            # Separate Quarterly and Annual into the internal dict
                            # Use .copy() to ensure they are independent DataFrames in memory
                            self.data[cat_key]["annual"] = df[df["report_type"] == "annual"].copy()
                            self.data[cat_key]["quarterly"] = df[df["report_type"] == "quarterly"].copy()
                except Exception as e:
                    print(f"Database table check failed for {db_file}: {e}")

        # 2. Load the Company Overview (The 5th Database)
        overview_db = "company_overviews.db"
        full_path = os.path.join(db_dir, overview_db)

        if os.path.exists(full_path):
            engine = self.engines["overview"]
            async with engine.connect() as conn:
                try:
                    # Note: Overview uses 'Symbol' instead of 'ticker' per your current schema
                    result = await conn.execute(
                        text(f"SELECT * FROM Overview WHERE Symbol = :t"), {"t": ticker}
                    )
                    overview_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    if not overview_df.empty:
                        self.data["overview"] = overview_df
                except Exception:
                    pass
    async def _check_db_for_ticker(self, table_name, engine, ticker):
        async with engine.connect() as conn:
            try:
                check_sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                table_check = await conn.execute(text(check_sql))
                if table_check.fetchone() is None:
                    return False
                result = await conn.execute(
                    text(f"SELECT 1 FROM {table_name} WHERE ticker = :ticker LIMIT 1"),
                    {"ticker": ticker}
                )
                return result.fetchone() is not None
            except:
                return False

    async def _save_df_to_sql(self, engine, df, table_name):
        # We wrap the sync to_sql. 
        # Using if_exists='append' allows multiple tickers to live in the same DB.
        def sync_save(connection):
            df.to_sql(table_name, connection, if_exists="append", index=False)
        
        async with engine.begin() as conn:
            await conn.run_sync(sync_save)

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
        overview_engine = self.engines["overview"]
        ticker = self.ticker
        api_key = self.api_key


        # 1. Check if the ticker already exists in the database
        async with overview_engine.connect() as conn:
            try:
                # Alpha Vantage uses 'Symbol' (Capitalized) in the JSON response
                result = await conn.execute(
                    text(f"SELECT 1 FROM Overview WHERE Symbol = :symbol LIMIT 1"),
                    {"symbol": ticker}
                )
                if result.fetchone():
                    # Data exists, load it into the object and return
                    # We'll need a helper to load this into self.company_overview
                    await self._load_overview_from_db(overview_engine=overview_engine)
                    return True
            except Exception:
                # Table doesn't exist yet, proceed to fetch
                pass

        # 2. Fetch from API
        async with httpx.AsyncClient() as client:
            url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
            response = await client.get(url)
            data = response.json()

            if "Error Message" in data or "Information" in data:
                print(f"API Error for {ticker}: RATE LIMIT EXCEEDED")
                return False
            
            if not data:
                print(f"No data found for {ticker}")
                return False

            # 3. Process into DataFrame
            new_df = pd.DataFrame([data])
            start_idx = new_df.columns.get_loc("LatestQuarter") + 1

            # 2. Get the list of columns that need to be numeric
            cols_to_fix = new_df.columns[start_idx:]

            # 3. Batch convert using to_numeric with 'coerce'
            # 'coerce' turns non-numeric strings (like "None") into NaN so the float conversion works
            new_df[cols_to_fix] = new_df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
            
        # 4. Save to SQL (Asynchronously)
        def sync_save(sync_conn):
                # Using append because this is a master list of all tickers searched
                new_df.to_sql("Overview", sync_conn, if_exists="append", index=False)

        async with overview_engine.begin() as conn:
            await conn.run_sync(sync_save)
        
        # Load the newly saved data into the class attribute
        self.data["overview"] = new_df
        return True

    async def _load_overview_from_db(self, overview_engine):
        """Helper to populate self.company_overview from the DB"""
        async with overview_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT * FROM Overview WHERE Symbol = :symbol"),
                {"symbol": self.ticker}
            )
            row = result.fetchone()
            if row:
                df = pd.DataFrame([row._asdict()])
                start_idx = df.columns.get_loc("LatestQuarter") + 1

                # 2. Get the list of columns that need to be numeric
                cols_to_fix = df.columns[start_idx:]

                # 3. Batch convert using to_numeric with 'coerce'
                # 'coerce' turns non-numeric strings (like "None") into NaN so the float conversion works
                df[cols_to_fix] = df[cols_to_fix].apply(pd.to_numeric, errors='coerce')
                self.company_overview = df

    def calc_fcf(self) -> tuple[float, float, float]:
        """
        Calculate the Free Cash Flow (FCF) and Free Cash Flow to the Firm (FCFF) for a given ticker.

        The FCF is calculated as the operating cash flow minus capital expenditures.

        The FCF to the Firm (FCFF) is calculated as the EBIT times (1 - effective tax rate) plus depreciation and amortization, minus capital expenditures and the change in NWC.

        Parameters
        ----------
        ticker : str
            The ticker symbol of the company.

        Returns
        -------
        None
        """
        ticker = self.ticker
        
        cash_df = self.cash_df
        income_df = self.income_df
        cash_df["FCF"] = (cash_df["operatingCashflow"] - cash_df["capitalExpenditures"]).round(2)
        cash_df["FCF_YoY_Growth"] = (
        cash_df["FCF"].pct_change()
            .replace([np.inf, -np.inf], np.nan)
            * 100
        ).round(2)

        balance_df = self.balance_df
        cash_df["FCF_Per_Share"] = (cash_df["FCF"] / balance_df["commonStockSharesOutstanding"]).round(2)

        income_df["effectiveTaxRate"] = (
            income_df["incomeTaxExpense"] / income_df["incomeBeforeTax"]
        ).clip(0, 0.35).round(4)  
        
        balance_df["inventory"] = balance_df["inventory"].fillna(0)

        # 1. Define Operating Current Assets (Total Current Assets minus Cash)
        cash_total = balance_df["cashAndShortTermInvestments"].fillna(0)
        # If the aggregate column is 0 or missing, assume we need to sum the parts
        mask = cash_total == 0
        cash_total[mask] = (
            balance_df.loc[mask, "cashAndCashEquivalentsAtCarryingValue"].fillna(0) +
            balance_df.loc[mask, "shortTermInvestments"].fillna(0)
        )

        operating_current_assets = balance_df["totalCurrentAssets"] - cash_total

        # 2. Define Operating Current Liabilities (Total Current Liabilities minus Debt)
        operating_current_liabilities = (
            balance_df["totalCurrentLiabilities"] 
            - balance_df["shortTermDebt"].fillna(0)
            - balance_df["currentDebt"].fillna(0) # Check your specific CSV header for debt
        )

        # 3. Calculate NWC Ratio
        balance_df["NWC"] = operating_current_assets - operating_current_liabilities

        balance_df["deltaNWC"] = balance_df["NWC"].diff()

        if 'ebit' not in income_df.columns or income_df['ebit'].isnull().any():
            # Method: Net Income + Interest + Taxes
            income_df['ebit'] = (
                income_df['netIncome'] + 
                income_df['interestExpense'].fillna(0) + 
                income_df['incomeTaxExpense'].fillna(0)
            )
        
        cash_df["FCFF"] = (income_df["ebit"] * (1 - income_df["effectiveTaxRate"]) + income_df["depreciationAndAmortization"] - cash_df["capitalExpenditures"] -  balance_df["deltaNWC"]).round(2)


        self.cash_df = cash_df
        self.income_df = income_df
        self.balance_df = balance_df

        return cash_df['FCF'].iloc[-1], cash_df['FCFF'].iloc[-1], balance_df['deltaNWC'].iloc[-1]

    def reorder_data(self):
        """
        Sorts all DataFrames in the memory dictionary by date.
        Calculations like DCF and ROIC rely on chronological order.
        """
        # Categories mapped to our dictionary keys
        categories = ["income", "balance", "cash", "earnings"]
        
        for category in categories:
            # Check both annual and quarterly frequencies
            for freq in ["annual", "quarterly"]:
                df = self.data.get(category, {}).get(freq)
                
                if df is not None and not df.empty:
                    # 1. Ensure the date column is in datetime format
                    # We use 'date' because we standardized it during fetch
                    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                    
                    # 2. Sort ascending (Oldest to Newest)
                    # This ensures iloc[-1] is the most recent period for calculations
                    df.sort_values("date", ascending=True, inplace=True)
                    
                    # 3. Reset index to keep iloc clean
                    df.reset_index(drop=True, inplace=True)
                    
    def calc_wacc(self) -> float:
        """
        Calculate the Weighted Average Cost of Capital (WACC) for a given ticker.

        The WACC is calculated as the weighted average of the cost of equity and the cost of debt.

        The cost of equity is calculated as the risk-free rate plus beta times the market risk premium.

        The cost of debt is calculated as the interest expense divided by the average debt.

        The post-tax cost of debt is calculated as the cost of debt times (1 - effective tax rate).

        The total debt is calculated as the short long-term debt.

        The equity is calculated as the market capitalization divided by 1,000,000.

        The WACC is calculated as the weighted average of the cost of equity and the cost of debt.

        Parameters
        ----------
        ticker : str
            The ticker symbol of the company.

        Returns
        -------
        None
        """
        ticker = self.ticker
        
        income_df = self.income_df
        balance_df = self.balance_df

        recent_income = income_df.iloc[-1]
        recent_balance = balance_df.tail(2)

        company_overview = self.company_overview

        beta = company_overview["Beta"].values[0]
        sector = company_overview["Sector"].values[0]

        if abs(SECTOR_BETAS[sector] - beta) > 0.4:
            beta = (SECTOR_BETAS[sector] + beta) / 2.0


        cost_of_equity = RISK_FREE_RATE + beta * MARKET_RISK_PREMIUM
        company_overview["CostOfEquity"] = cost_of_equity

        interest_expense = abs(income_df["interestExpense"].dropna().iloc[-1])
        if pd.isna(interest_expense):
            interest_expense = recent_income["ebit"] - recent_income["incomeBeforeTax"]

        average_debt = recent_balance["shortLongTermDebtTotal"].mean(skipna=True)
        
        effective_tax_rate = recent_income["effectiveTaxRate"]
        
        cost_of_debt = min(interest_expense / average_debt, 0.15)
        
        post_tax_cost_of_debt = cost_of_debt * (1 - effective_tax_rate)

        total_debt = recent_balance.iloc[-1]["shortLongTermDebtTotal"]
        equity = company_overview["MarketCapitalization"].values[0] / 1_000_000

        wacc = ((equity / (equity + total_debt)) * cost_of_equity + (total_debt / (equity + total_debt)) * post_tax_cost_of_debt) * 100

        company_overview["WACC"] = wacc

        self.company_overview = company_overview

        return wacc.round(4)

    def calc_forecast_metrics(self):
        """
        Calculate forecast metrics for a given ticker.

        Parameters
        ----------
        ticker : str
            Ticker symbol

        Returns
        -------
        None
        """

        income_df = self.income_df
        cash_df = self.cash_df
        balance_df = self.balance_df
        
        income_df["revGrowth"] = income_df["totalRevenue"].pct_change().replace([np.inf, -np.inf], np.nan).round(2)
        income_df["ebitMargin"] = (
            income_df["ebit"] / income_df["totalRevenue"]
        ).round(4)
        income_df["capexPctRevenue"] = (
            cash_df["capitalExpenditures"].abs() / income_df["totalRevenue"]
        ).round(4)

        income_df["nwcPctRevenue"] = (
            balance_df["deltaNWC"] / income_df["totalRevenue"]
        ).round(4)

        income_df["daPctRevenue"] = (
            cash_df["depreciationDepletionAndAmortization"]
            / income_df["totalRevenue"]
        )
        if 'ebit' not in income_df.columns or income_df['ebit'].isnull().any():
            # Method: Net Income + Interest + Taxes
            income_df['ebit'] = (
                income_df['netIncome'] + 
                income_df['interestExpense'].fillna(0) + 
                income_df['incomeTaxExpense'].fillna(0)
            )
        income_df["ebitGrowth"] = income_df["ebit"].pct_change().round(4)


        
        balance_df["nwcRatio"] = balance_df["NWC"] / income_df["totalRevenue"]

        self.income_df = income_df
        self.balance_df = balance_df

        return

    def dividend_model(self):
        company_overview = self.company_overview
        terminal_growth = self.terminal_growth
        dividend_per_share = company_overview["DividendPerShare"].values[0]
        cost_of_equity = company_overview["CostOfEquity"].values[0]
        intrinsic_price = (dividend_per_share * (1+terminal_growth)) / (cost_of_equity - terminal_growth)
        company_overview["dividendPrice"] = round(float(intrinsic_price), 2)
        
        self.company_overview = company_overview
        return intrinsic_price
    
    def calculate_3_stage_growth(self, start_growth, terminal_growth, years):
        # Stage 1: Years 1-3 (High Growth)
        stage1 = np.linspace(start_growth, start_growth * 0.75, 3)
        
        # Stage 2: Years 4-10 (Linear Decay)
        # We have 7 years left to get from start_growth down to terminal_growth
        stage2 = np.linspace(start_growth * 0.70, terminal_growth, years - 3)
        
        # Combine them into a single 10-year path
        growth_path = np.concatenate([stage1, stage2])
        
        return growth_path

    def fcff_forecast(self): 
        """
        Run the 10-year projection for a given ticker, using the Free Cash Flow to Firm (FCFF) method.

        Parameters
        ----------
        ticker : str
            Ticker symbol

        Returns
        -------
        intrinsic_price : float
            The intrinsic price of the stock
        dividend_price : float
            The dividend price of the stock if applicable
        """
        ticker = self.ticker
        dividend_price = None
        # 1. LOAD DATA
        income_df = self.income_df
        balance_df = self.balance_df
        company_overview = self.company_overview

        market_cap = company_overview["MarketCapitalization"].values[0]

        # 2. GET WACC
        wacc_val = company_overview["WACC"].values[0]
        wacc = wacc_val / 100 if wacc_val > 1 else wacc_val
        years = 15 if market_cap > 1_000_000_000 else 10

        # 3. CALCULATE HISTORICAL AVERAGES (The "Means")
        avg_ebit_margin = income_df["ebitMargin"].tail(5).mean(skipna=True)
        avg_revenue = income_df["totalRevenue"].tail(5).mean(skipna=True)
        avg_rev_growth = income_df["revGrowth"].tail(5).mean(skipna=True)
        avg_long_term_rev_growth = income_df["revGrowth"].tail(15).mean(skipna=True)
        avg_tax_rate = income_df["effectiveTaxRate"].tail(5).mean(skipna=True)
        avg_nwc_ratio = balance_df["nwcRatio"].tail(5).mean(skipna=True)

        std_rev_growth = income_df["revGrowth"].tail(5).std()

        # 4. STARTING VALUES & CYCLE DETECTION
        revenue_0 = income_df["totalRevenue"].iloc[-1]
        ebit_margin_0 = income_df["ebitMargin"].iloc[-1]

        sector = company_overview["Sector"].values[0]
        industry = company_overview["Industry"].values[0]

        # Detect if we are in a "Bust" (Mean Revert the Start)
        is_down_cycle = (ebit_margin_0 < 0) or (ebit_margin_0 < avg_ebit_margin * 0.5)

        if is_down_cycle:
            revenue_0 = avg_revenue 
            ebit_margin_0 = avg_ebit_margin
            start_growth = 0.05 # Conservative mid-cycle recovery
        else:
            # Determine current growth blend (Revenue + EBIT)
            if 'ebitGrowth' not in income_df.columns:
                income_df["ebitGrowth"] = income_df["ebit"].pct_change().fillna(0).replace([np.inf, -np.inf], 0)
            
            ebit_growth_avg = income_df["ebitGrowth"].tail(5).mean(skipna=True)
            actual_growth = (avg_rev_growth * 0.7) + (ebit_growth_avg * 0.3)
            actual_growth = min(actual_growth, avg_rev_growth)
            
            # Cap start growth between 5% and 40% (for hyper-growth)
            start_growth = max(min(actual_growth, 0.40), 0.05)

        self.start_growth = start_growth
        avg_ebit_growth_long_term = income_df["ebitGrowth"].tail(15).mean(skipna=True)
        
        long_term_growth = (avg_long_term_rev_growth * 0.7) + (avg_ebit_growth_long_term * 0.3)
        # 5. CREATE THE MEAN REVERSION GLIDE PATHS
        # Terminal targets
        if sector in DEFENSIVE: terminal_growth = long_term_growth
        elif sector in SENSITIVE: terminal_growth = max(long_term_growth / 2.0, 0.05)
        else: terminal_growth = (long_term_growth + 0.02) / 2
        while terminal_growth > start_growth or terminal_growth > 0.06:
            terminal_growth = terminal_growth * 0.9
        self.terminal_growth = terminal_growth

        # See if has dividends
        dividend_per_share = company_overview["DividendPerShare"].values[0]
        if industry == "BANKS - DIVERSIFIED" or not pd.isna(dividend_per_share):
            # Dividend model
            dividend_price = self.dividend_model()

        # Margin Glide Path: Move from current to 5-year average
        # (Or use current if margin is expanding and improving)
        if ebit_margin_0 > avg_ebit_margin * 1.2:
            terminal_ebit_margin = (ebit_margin_0 + avg_ebit_margin) / 2
        else:
            terminal_ebit_margin = avg_ebit_margin

        if start_growth - terminal_growth > 0.25:
            growth_path = self.calculate_3_stage_growth(start_growth, terminal_growth, years)
        else:
            growth_path = np.linspace(start_growth, terminal_growth, years)
        margin_path = np.linspace(ebit_margin_0, terminal_ebit_margin, years)
        tax_path = np.linspace(income_df["effectiveTaxRate"].iloc[-1], avg_tax_rate, years)
        tax_path = np.clip(tax_path, 0.10, 0.35) # Keep tax between 10% and 35%

        
        # 6. RUN THE 10-YEAR PROJECTION
        forecast = []
        revenue = revenue_0
        prev_nwc = revenue_0 * avg_nwc_ratio
        
        capex_pct = income_df["capexPctRevenue"].iloc[-1]
        da_pct = income_df["daPctRevenue"].iloc[-1]
        
        for t in range(years):
            revenue *= (1 + growth_path[t])
            ebit = revenue * margin_path[t]
            nopat = ebit * (1 - tax_path[t])
            
            # Reinvestment Logic
            da = revenue * da_pct
            if t < (years - 1):
                capex = revenue * capex_pct
            else:
                capex = da * 1.1 # Terminal Year Steady State Reinvestment
                
            current_nwc = revenue * avg_nwc_ratio
            delta_nwc = current_nwc - prev_nwc
            prev_nwc = current_nwc
            
            fcff = nopat + da - capex - delta_nwc
            forecast.append({"Year": t + 1, "FCFF": fcff})

        # 7. VALUATION
        forecast_df = pd.DataFrame(forecast)
        forecast_df["PV_FCFF"] = forecast_df["FCFF"] / ((1 + wacc) ** forecast_df["Year"])

        # Terminal Value Safety
        terminal_fcff = forecast_df["FCFF"].iloc[-1] * (1 + terminal_growth)
        terminal_fcff = max(terminal_fcff, revenue * 0.05) # Floor at 5% of Rev

        # Choice of Valuation Method    
        # Switch to Multiple for Growth/Tech
        if start_growth > 0.12 or sector == "TECHNOLOGY":
            target_multiple = 20.0 if start_growth < 0.25 else 25.0
            terminal_value = terminal_fcff * target_multiple
        else:
            denom = max(wacc - terminal_growth, 0.01)
            terminal_value = terminal_fcff / denom

        # Final Calculation
        pv_terminal = terminal_value / ((1 + wacc) ** years)
        enterprise_value = forecast_df["PV_FCFF"].sum() + pv_terminal
        
        # Net Debt
        liquid_assets = (balance_df["cashAndCashEquivalentsAtCarryingValue"].iloc[-1] + 
                        balance_df["shortTermInvestments"].fillna(0).iloc[-1])
        total_debt = (balance_df["shortLongTermDebtTotal"].fillna(0).iloc[-1] + 
                    balance_df["longTermDebt"].fillna(0).iloc[-1])
        net_debt = total_debt - liquid_assets
        
        equity_value = max(enterprise_value - net_debt, 0)
        shares = balance_df["commonStockSharesOutstanding"].iloc[-1]
        intrinsic_price = equity_value / shares if shares > 0 else 0
        
        # Save back to CSV
        company_overview["IntrinsicPrice"] = round(float(intrinsic_price), 2)
        
        self.balance_df = balance_df
        self.income_df = income_df
        self.company_overview = company_overview
        
        
        return intrinsic_price, dividend_price

    def roic(self):
        """
        Calculate the Return on Invested Capital (ROIC) for a given ticker.

        The ROIC is calculated as the Net Operating Profit After Tax (NOPAT) divided by the Invested Capital.

        The Invested Capital is calculated as the Debt + Equity - Cash.

        The NOPAT is calculated as the EBIT times (1 - effective tax rate).

        Parameters
        ----------
        ticker : str
            Ticker symbol

        Returns
        -------
        float
            ROIC value
        """
        ticker = self.ticker
        balance_df = self.balance_df
        income_df = self.income_df
        
        if "ROIC" in income_df.columns:
            if income_df["ROIC"].iloc[-1] > 0:
                Roic = income_df["ROIC"].iloc[-1]
                return Roic
        # Calculate Invested Capital (Debt + Equity - Cash)
        # Note: You need to decide if you use 'Total Assets - Current Liabilities' or the financing approach below
        invested_capital = (
            balance_df["totalShareholderEquity"] + 
            balance_df["shortLongTermDebtTotal"].fillna(0) - 
            balance_df["cashAndCashEquivalentsAtCarryingValue"]
        )

        # Calculate NOPAT
        nopat = income_df["ebit"] * (1 - income_df["effectiveTaxRate"])

        # ROIC
        income_df["ROIC"] = (nopat / invested_capital).round(4)
        self.income_df = income_df
        return income_df["ROIC"].iloc[-1]

    def peg_ratio(self):        
        peg_ratio = self.company_overview["PEGRatio"].iloc[0]
        # print(f"PEG Ratio for {self.ticker}: {peg_ratio}")
        return peg_ratio

    def sloan_ratio(self):
        """
        Calculate the Sloan Ratio for a given ticker.

        The Sloan Ratio is calculated as (Net Income - Free Cash Flow) / Total Assets.

        Parameters
        ----------
        ticker : str
            The ticker symbol of the company.

        Returns
        -------
        float
            The Sloan Ratio for the given ticker.
        """
        ticker = self.ticker
        company_overview = self.company_overview
        if "sloanRatio" in company_overview.columns:
            val = company_overview["sloanRatio"].iloc[0]
            if pd.notna(val): # This handles None, NaN, and Null
                if val > 0:
                    # print(f"Sloan Ratio for {ticker} already calculated: {val}")
                    return val
        income_df = self.income_df
        balance_df = self.balance_df
        cash_df = self.cash_df

        net_income = income_df["netIncome"].iloc[-1]
        fcf = cash_df["FCF"].iloc[-1]
        total_assets = balance_df["totalAssets"].iloc[-1]

        sloan_ratio = (net_income - fcf) / total_assets

        company_overview["sloanRatio"] = sloan_ratio.round(4)
        self.company_overview = company_overview
        # print(f"Sloan Ratio for {ticker}: {sloan_ratio}")
        return sloan_ratio


    def analyze_peg(self) -> tuple[float, float, float | None, float | None]:
        """
        Analyze the PEG ratio for a given ticker.

        The PEG ratio is a measure of the value of a company in terms of earnings growth. It is calculated as the price/earnings ratio divided by the earnings growth rate.

        This method will calculate the historical growth rate, projected growth rate, trailing PEG, and forward PEG for the given ticker.

        Parameters
        ----------
        ticker : str
            The ticker symbol of the company.

        Returns
        -------
        tuple[float, float, float | None, float | None]
            A tuple containing the historical growth rate, projected growth rate, trailing PEG, and forward PEG. If the projected growth rate is less than 0%, the forward PEG will be None.

        """
        company_overview = self.company_overview
        income_df = self.income_df
        cash_df = self.cash_df
        ticker = self.ticker

        # 0. Get the Growth Ratios
        try:
            historical_growth = income_df["revGrowth"].tail(10).mean(skipna=True)
            if pd.isna(cash_df["dividendPayout"].iloc[-1]):
                retention_ratio = 1
            else:
                dividend_payout_ratio = cash_df["dividendPayout"].iloc[-1] / income_df["netIncome"].iloc[-1]
                retention_ratio = (1 - dividend_payout_ratio)
            projected_growth = retention_ratio * income_df["ROIC"].iloc[-1]
        except (ValueError, IndexError):
            print("--- PEG Analysis Failed: Missing Growth Data ---")
            return
        # 1. Get the P/E Ratios
        try:
            trailing_pe = float(company_overview["TrailingPE"].values[0])
            forward_pe = float(company_overview["ForwardPE"].values[0])
        except (ValueError, IndexError):
            print("--- PEG Analysis Failed: Missing P/E Data ---")
            return

        # print(f"Historical Growth: {historical_growth:.3f}")
        # print(f"Projected Growth: {projected_growth:.3f}")
        # print(f"Trailing P/E: {trailing_pe}")
        # print(f"Forward P/E: {forward_pe}")
        # 2. Convert Growth to Integers (e.g., 0.15 -> 15.0)
        hist_g_int = historical_growth * 100
        proj_g_int = projected_growth * 100

        # 3. Calculate Ratios
        if hist_g_int > 0:
            trailing_peg = trailing_pe / hist_g_int
        else:
            trailing_peg = None

        if proj_g_int > 0:
            forward_peg = forward_pe / proj_g_int
        else:
            forward_peg = None
            print("--- Forward PEG Analysis Failed: Projected Growth is less than 0% ---")

        return historical_growth, projected_growth, trailing_peg, forward_peg

    async def replace_with_usd(self):
        # API key for the exchange rate service
        rate_url = f"https://v6.exchangerate-api.com/v6/{self.rate_api_key}/latest/USD"
        
        async with httpx.AsyncClient() as client:
            resp = await client.get(rate_url)
            rates_dict = resp.json().get("conversion_rates", {})

        # We iterate through our memory dictionary
        for category in ["income", "balance", "cash"]:
            for freq in ["annual", "quarterly"]:
                df = self.data[category][freq]
                if df.empty: continue

                # Create conversion factors: 1.0 for USD, specific rate for others
                # This handles companies like Toyota (JPY) or ASML (EUR)
                conversion_factors = df['reportedCurrency'].map(rates_dict).fillna(1)

                # Identify numeric columns only
                protected_cols = ['ticker', 'report_type', 'date', 'reportedCurrency']
                numeric_cols = df.select_dtypes(include=['number']).columns
                cols_to_convert = [c for c in numeric_cols if c not in protected_cols]

                # Vectorized conversion (Fast)
                df[cols_to_convert] = df[cols_to_convert].div(conversion_factors, axis=0).round(4)
                df['reportedCurrency'] = 'USD'
                
        # print(f"Currency standardization complete for {self.ticker}")

    
    async def save_all_to_db(self):
        """
        Final Flush: Persists data from the memory dictionary into 5 separate DB files.
        Handles 'Extra Columns' in annual data via automatic schema migration.
        """
        db_map = ["income", "balance", "cash", "earnings"]

        # 1. Save the 4 Financial Databases
        for cat_key in db_map:
            # Create a copy to avoid modifying the original memory dictionary directly
            annual_df = self.data[cat_key]["annual"].copy()
            quarterly_df = self.data[cat_key]["quarterly"].copy()
            
            df_to_save = pd.concat([annual_df, quarterly_df], ignore_index=True)
            if df_to_save.empty:
                continue

            # Add the ID so the upsert logic can use it
            df_to_save["symbol_id"] = self.symbol_id

            engine = self.engines[cat_key]
            # Ensure we use symbol_id as the primary key column for the delete step
            await self._upsert_to_database(engine, df_to_save, cat_key, pk_col="symbol_id")

        # 2. Save the Company Overview Database (The 5th DB)
        if not self.data["overview"].empty:
            overview_df = self.data["overview"].copy()
            
            # Inject calculated metrics
            overview_df["intrinsic_price"] = self.intrinsic_price
            overview_df["wacc"] = self.wacc
            overview_df["roic"] = self.return_on_invested_capital
            overview_df["timestamp"] = self.timestamp
            
            # NEW: Inject the integer ID
            overview_df["symbol_id"] = self.symbol_id
            
            overview_engine = self.engines["overview"]
            
            # CHANGE: Use symbol_id as the pk_col here too for a unified system
            await self._upsert_to_database(overview_engine, overview_df, "Overview", pk_col="symbol_id")

    async def _upsert_to_database(self, engine, df, table_name, pk_col="symbol_id"):
        """
        Handles Schema Evolution and prevents duplicates by using symbol_id.
        Ensures the transition from ticker strings to integer IDs is seamless.
        """
        # Ensure the integer ID is present in the DataFrame before saving
        if pk_col == "symbol_id":
            df["symbol_id"] = self.symbol_id

        async with engine.begin() as conn:
            # 1. Check if the table exists
            table_exists_res = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name=:t"), 
                {"t": table_name}
            )
            table_exists = table_exists_res.fetchone() is not None
            
            if table_exists:
                # 2. Schema Migration: Check for missing columns
                existing_cols_res = await conn.execute(text(f"PRAGMA table_info({table_name})"))
                existing_cols_info = existing_cols_res.fetchall()
                existing_cols_lower = [row[1].lower() for row in existing_cols_info]

                for col in df.columns:
                    if col.lower() not in existing_cols_lower:
                        # Assign correct SQLite types
                        if col.lower() == "symbol_id":
                            dtype = "INTEGER"
                        elif col.lower() in ["reportedcurrency", "report_type", "ticker", "symbol"]:
                            dtype = "TEXT"
                        else:
                            dtype = "REAL"
                        
                        try:
                            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}"))
                            print(f"Migration: Added column {col} ({dtype}) to {table_name}")
                        except Exception as e:
                            print(f"Column {col} add failed (likely case conflict): {e}")

                # 3. Clean Slate: Remove old entries for this company
                # We delete by both symbol_id AND ticker during the migration phase 
                # to ensure no duplicate rows exist under different identifiers.
                delete_query = f"DELETE FROM {table_name} WHERE symbol_id = :sid"
                params = {"sid": self.symbol_id}
                
                if "ticker" in existing_cols_lower:
                    delete_query += " OR ticker = :t"
                    params["t"] = self.ticker
                elif "symbol" in existing_cols_lower: # For the Overview table
                    delete_query += " OR Symbol = :t"
                    params["t"] = self.ticker

                await conn.execute(text(delete_query), params)
            
            # 4. Final Flush: Save the DataFrame
            def sync_save(sync_conn):
                # Using 'append' because we manually handled the 'Clean Slate' above
                df.to_sql(table_name, sync_conn, if_exists="append", index=False)
            
            await conn.run_sync(sync_save)