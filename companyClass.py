import asyncio
import glob
import os
from pathlib import Path
from pprint import pprint
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import time
import aiosqlite
import pandas as pd
import numpy as np
import requests
import httpx

from dataloader import DataLoader

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
    def __init__(self, ticker, api_key, rate_api_key, streamer):
        # 1. Minimal setup: only assign non-I/O variables
        self.ticker = ticker
        self.api_key = api_key
        self.rate_api_key = rate_api_key
        self.streamer = streamer

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
    async def create(cls, ticker, api_key, rate_api_key, streamer):
        """Asynchronous factory to create and fully initialize the instance."""
        self = cls(ticker, api_key, rate_api_key, streamer)
        await self.load_all_from_dbs()
        
        # 2. Run async I/O tasks
        # These will check the DBs, fetch if missing, and load into DataFrames
        if self.data["overview"].empty:
            await asyncio.gather(
                self.get_fundamentals(),
                self.get_company_overviews()
            )

        await self.replace_with_usd()
        self.reorder_df()
        
        # Load the specific annual data needed for DCF calculations
        self.price_at_report = await self.get_current_price()
        self.income_df = self.data["income"]["annual"]
        self.balance_df = self.data["balance"]["annual"]
        self.cash_df = self.data["cash"]["annual"]
        self.company_overview = self.data["overview"]

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


        # Extract values from the loaded overview
        self._setup_analyst_ratings()
        self.final_report = {
            "market_cap": self.market_cap,
            "peg": self.peg,
            "sloan": self.sloan,
            "roic": self.return_on_invested_capital,
            "hist_growth": self.hist_growth,
            "forecasted_growth": self.forecasted_growth,
            "trailing_peg": self.trailing_peg,
            "forward_peg": self.forward_peg,
            "intrinsic_price": self.intrinsic_price,
            "dividend_price": self.dividend_price,
            "price_at_report": self.price_at_report,
            "wacc": self.wacc,
            "fcff": self.fcff,
            "fcf": self.fcf,
            "nwc": self.nwc,
            "price_target": self.price_target,
            "strong_buy": self.strong_buy,
            "buy": self.buy,
            "hold": self.hold,
            "strong_sell": self.strong_sell,
            "sell": self.sell
        }

        
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

    async def _read_sql_to_df(self, table_name, db_file, report_type):
        """Reads data from a specific fundamental DB."""
        local_engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")
        async with local_engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT * FROM {table_name} WHERE ticker = :t AND report_type = :r"),
                {"t": self.ticker, "r": report_type}
            )
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        await local_engine.dispose()
        return df

    async def get_current_price(self) -> float:
        """Asynchronously fetches price data using the DataLoader."""
        self.unix_timestamp() # Sets self.timestamp
        
        # Define a small wrapper function for the blocking code
        def sync_fetch():
            loader = DataLoader(
                ticker=self.ticker, 
                fiscalDate=self.timestamp, 
                connection=self.streamer
            )
            return loader.quote

        # Run the Schwab API request in a thread pool
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
        # We define the 4 categories as requested
        categories = ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS"]
        ticker = self.ticker
        is_new_data_added = False

        async with httpx.AsyncClient() as client:
            for category in categories:
                if category == "INCOME_STATEMENT": table_name = "income"
                elif category == "BALANCE_SHEET": table_name = "balance"
                elif category == "CASH_FLOW": table_name = "cash"
                elif category == "EARNINGS": table_name = "earnings"
                # 1. Define the specific DB for this category
                db_name = f"{category.lower()}.db"
                engine = create_async_engine(f"sqlite+aiosqlite:///{db_name}")

                # 2. Check if this ticker already has data in this specific DB
                # Note: We check a table named 'data' inside that specific DB
                already_exists = await self._check_db_for_ticker(table_name,engine, ticker)
                
                if not already_exists:
                    print(f"Fetching {category} for {ticker}...")
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
                            
                            # Add our Primary Key columns
                            annual_df['ticker'] = ticker
                            annual_df['report_type'] = 'annual'
                            
                            quarterly_df['ticker'] = ticker
                            quarterly_df['report_type'] = 'quarterly'
                            
                            # Clean and Scale (passing the correct date column name)
                            annual_df = self._clean_financial_df(annual_df, date_col='fiscalDateEnding', scale=1_000_000)
                            quarterly_df = self._clean_financial_df(quarterly_df, date_col='fiscalDateEnding', scale=1_000_000)
                        else:
                            annual_df = pd.DataFrame(data["annualEarnings"])
                            quarterly_df = pd.DataFrame(data["quarterlyEarnings"])
                            
                            annual_df['ticker'] = ticker
                            annual_df['report_type'] = 'annual'
                            quarterly_df['ticker'] = ticker
                            quarterly_df['report_type'] = 'quarterly'
                            
                            # Earnings uses 'fiscalDateEnding' for annual and 'reportedDate' for quarterly
                            # Let's standardize the column name to 'date' to match your PK requirement
                            annual_df = annual_df.rename(columns={'fiscalDateEnding': 'date'})
                            quarterly_df = quarterly_df.rename(columns={'reportedDate': 'date'})

                        # 4. Merge Annual and Quarterly into one DataFrame for the DB
                        # We standardized the columns so they stack perfectly
                        combined_df = pd.concat([annual_df, quarterly_df], ignore_index=True)
                        
                        # Ensure 'fiscalDateEnding' is renamed to 'date' if not already done
                        if 'fiscalDateEnding' in combined_df.columns:
                            combined_df = combined_df.rename(columns={'fiscalDateEnding': 'date'})

                        # 5. Save to the specific Database file
                        await self._save_df_to_sql(engine, combined_df, "data")
                        
                        is_new_data_added = True
                        await asyncio.sleep(1.01) # Alpha Vantage Rate Limit

                    except Exception as e:
                        print(f"Error processing {category} for {ticker}: {e}")
                        return False
                
                await engine.dispose() # Clean up connection for this specific DB file
            else:
                await self.load_all_from_dbs()
        return True
    async def load_all_from_dbs(self):
        """
        Checks all 5 databases and loads existing data into self.data structure.
        Separates quarterly and annual data for financials.
        """
        ticker = self.ticker
        
        # Map dictionary keys to their specific database filenames
        db_map = {
            "income": "income_statement.db",
            "balance": "balance_sheet.db",
            "cash": "cash_flow.db",
            "earnings": "earnings.db"
        }

        # 1. Load the 4 Financial/Time-Series Databases
        for cat_key, db_file in db_map.items():
            if not os.path.exists(db_file):
                continue
                
            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")
            async with engine.connect() as conn:
                try:
                    result = await conn.execute(
                        text(f"SELECT * FROM {cat_key} WHERE ticker = :t"), {"t": ticker}
                    )
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    
                    if not df.empty:
                        # Separate Quarterly and Annual into the internal dict
                        # Use .copy() to ensure they are independent DataFrames in memory
                        self.data[cat_key]["annual"] = df[df["report_type"] == "annual"].copy()
                        self.data[cat_key]["quarterly"] = df[df["report_type"] == "quarterly"].copy()
                        print(f"Loaded {cat_key} data from DB for {ticker}")
                except Exception as e:
                    print(f"Database table check failed for {db_file}: {e}")
            await engine.dispose()

        # 2. Load the Company Overview (The 5th Database)
        overview_db = "company_overviews.db"
        if os.path.exists(overview_db):
            engine = create_async_engine(f"sqlite+aiosqlite:///{overview_db}")
            async with engine.connect() as conn:
                try:
                    # Note: Overview uses 'Symbol' instead of 'ticker' per your current schema
                    result = await conn.execute(
                        text(f"SELECT * FROM Overview WHERE Symbol = :t"), {"t": ticker}
                    )
                    overview_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    if not overview_df.empty:
                        self.data["overview"] = overview_df
                        print(f"Loaded Overview from DB for {ticker}")
                except Exception:
                    pass
            await engine.dispose()
    async def _check_db_for_ticker(self, table_name, engine, ticker):
        async with engine.connect() as conn:
            try:
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
        OVERVIEW_DB_URL = "sqlite+aiosqlite:///company_overviews.db"
        overview_engine = create_async_engine(OVERVIEW_DB_URL)
        ticker = self.ticker
        api_key = self.api_key
        table_name = "data"

        # 1. Check if the ticker already exists in the database
        async with overview_engine.connect() as conn:
            try:
                # Alpha Vantage uses 'Symbol' (Capitalized) in the JSON response
                result = await conn.execute(
                    text(f"SELECT 1 FROM {table_name} WHERE Symbol = :symbol LIMIT 1"),
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
            
        # 4. Save to SQL (Asynchronously)
        def sync_save(sync_conn):
                # Using append because this is a master list of all tickers searched
                new_df.to_sql(table_name, sync_conn, if_exists="append", index=False)

        async with overview_engine.begin() as conn:
            await conn.run_sync(sync_save)
        
        # Load the newly saved data into the class attribute
        self.company_overview = new_df
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
                self.company_overview = pd.DataFrame([row._asdict()])

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
        print(
            f"FCF: {cash_df['FCF'].iloc[-1]}, "
            f"FCFF: {cash_df['FCFF'].iloc[-1]}, "
            f"ΔNWC: {balance_df['deltaNWC'].iloc[-1]}"
        )


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
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    
                    # 2. Sort ascending (Oldest to Newest)
                    # This ensures iloc[-1] is the most recent period for calculations
                    df.sort_values("date", ascending=True, inplace=True)
                    
                    # 3. Reset index to keep iloc clean
                    df.reset_index(drop=True, inplace=True)
                    
        print(f"Data reordered chronologically for {self.ticker}")

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
        print(company_overview["Beta"])

        beta = company_overview["Beta"].values[0]
        sector = company_overview["Sector"].values[0]
        print(f"Sector: {sector} for {ticker}")
        if abs(SECTOR_BETAS[sector] - beta) > 0.4:
            beta = (SECTOR_BETAS[sector] + beta) / 2.0
            # beta = SECTOR_BETAS[sector]
        print(f"Beta: {beta} for {ticker}", beta)

        cost_of_equity = RISK_FREE_RATE + beta * MARKET_RISK_PREMIUM
        print(f"Cost of Equity: {cost_of_equity} for {ticker}", cost_of_equity)
        company_overview["CostOfEquity"] = cost_of_equity

        interest_expense = abs(income_df["interestExpense"].dropna().iloc[-1])
        if pd.isna(interest_expense):
            interest_expense = recent_income["ebit"] - recent_income["incomeBeforeTax"]
        print(f"Interest Expense: {interest_expense} for {ticker}", interest_expense)

        average_debt = recent_balance["shortLongTermDebtTotal"].mean()
        print(f"Average Debt: {average_debt} for {ticker}", average_debt)
        
        effective_tax_rate = recent_income["effectiveTaxRate"]
        print(f"Effective Tax Rate: {effective_tax_rate} for {ticker}", effective_tax_rate)
        
        cost_of_debt = min(interest_expense / average_debt, 0.15)
        print(f"Cost of Debt: {cost_of_debt} for {ticker}", cost_of_debt)
        
        post_tax_cost_of_debt = cost_of_debt * (1 - effective_tax_rate)
        print(f"Post Tax Cost of Debt: {post_tax_cost_of_debt} for {ticker}", post_tax_cost_of_debt)

        total_debt = recent_balance.iloc[-1]["shortLongTermDebtTotal"]
        print(f"Total Debt: {total_debt} for {ticker}", total_debt)
        equity = company_overview["MarketCapitalization"].values[0] / 1_000_000

        wacc = ((equity / (equity + total_debt)) * cost_of_equity + (total_debt / (equity + total_debt)) * post_tax_cost_of_debt) * 100
        print(f"WACC: {wacc.round(4)} for {ticker}")

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
        print("--- Using Dividend Model ---")
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
        avg_ebit_margin = income_df["ebitMargin"].tail(5).mean()
        avg_revenue = income_df["totalRevenue"].tail(5).mean()
        avg_rev_growth = income_df["revGrowth"].tail(5).mean()
        avg_long_term_rev_growth = income_df["revGrowth"].tail(15).mean()
        avg_tax_rate = income_df["effectiveTaxRate"].tail(5).mean()
        avg_nwc_ratio = balance_df["nwcRatio"].tail(5).mean()

        std_rev_growth = income_df["revGrowth"].tail(5).std()
        print(f"--- Standard Deviation of Revenue Growth: {std_rev_growth:.4f} ---")

        # 4. STARTING VALUES & CYCLE DETECTION
        revenue_0 = income_df["totalRevenue"].iloc[-1]
        ebit_margin_0 = income_df["ebitMargin"].iloc[-1]

        sector = company_overview["Sector"].values[0]
        industry = company_overview["Industry"].values[0]

        # Detect if we are in a "Bust" (Mean Revert the Start)
        is_down_cycle = (ebit_margin_0 < 0) or (ebit_margin_0 < avg_ebit_margin * 0.5)

        if is_down_cycle:
            print(f"--- Warning: {ticker} detected in Down-Cycle. Normalizing starting point. ---")
            revenue_0 = avg_revenue 
            ebit_margin_0 = avg_ebit_margin
            start_growth = 0.05 # Conservative mid-cycle recovery
        else:
            # Determine current growth blend (Revenue + EBIT)
            if 'ebitGrowth' not in income_df.columns:
                income_df["ebitGrowth"] = income_df["ebit"].pct_change().fillna(0).replace([np.inf, -np.inf], 0)
            
            ebit_growth_avg = income_df["ebitGrowth"].tail(5).mean()
            actual_growth = (avg_rev_growth * 0.7) + (ebit_growth_avg * 0.3)
            actual_growth = min(actual_growth, avg_rev_growth)
            
            # Cap start growth between 5% and 40% (for hyper-growth)
            start_growth = max(min(actual_growth, 0.40), 0.05)

        print(f"--- Starting Growth: {start_growth:.1%}")
        self.start_growth = start_growth
        avg_ebit_growth_long_term = income_df["ebitGrowth"].tail(15).mean()
        print(f"--- Average Long-Term EBIT Growth: {avg_ebit_growth_long_term:.3%}")
        print(f"--- Average Long-Term Revenue Growth: {avg_long_term_rev_growth:.3%}")
        
        long_term_growth = (avg_long_term_rev_growth * 0.7) + (avg_ebit_growth_long_term * 0.3)
        print(f"--- Long-Term Growth: {long_term_growth:.1%}")
        # 5. CREATE THE MEAN REVERSION GLIDE PATHS
        # Terminal targets
        if sector in DEFENSIVE: terminal_growth = long_term_growth
        elif sector in SENSITIVE: terminal_growth = max(long_term_growth / 2.0, 0.05)
        else: terminal_growth = (long_term_growth + 0.02) / 2
        while terminal_growth > start_growth or terminal_growth > 0.06:
            terminal_growth = terminal_growth * 0.9
        print(f"--- Terminal Growth Rate for {ticker}: {terminal_growth:.1%} ---")
        self.terminal_growth = terminal_growth

        # See if has dividends
        dividend_per_share = company_overview["DividendPerShare"].values[0]
        if industry == "BANKS - DIVERSIFIED" or not np.isnan(dividend_per_share):
            # Dividend model
            dividend_price = self.dividend_model()

        # Margin Glide Path: Move from current to 5-year average
        # (Or use current if margin is expanding and improving)
        if ebit_margin_0 > avg_ebit_margin * 1.2:
            terminal_ebit_margin = (ebit_margin_0 + avg_ebit_margin) / 2
            print(f"--- Using Margin Expansion Target: {terminal_ebit_margin:.1%} ---")
        else:
            terminal_ebit_margin = avg_ebit_margin

        if start_growth - terminal_growth > 0.25:
            print(f"--- Using 3-Stage Growth Path ---")
            growth_path = self.calculate_3_stage_growth(start_growth, terminal_growth, years)
        else:
            growth_path = np.linspace(start_growth, terminal_growth, years)
        margin_path = np.linspace(ebit_margin_0, terminal_ebit_margin, years)
        tax_path = np.linspace(income_df["effectiveTaxRate"].iloc[-1], avg_tax_rate, years)
        tax_path = np.clip(tax_path, 0.10, 0.35) # Keep tax between 10% and 35%

        print(f"--- Growth Glide Path: {growth_path} ---")
        
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
            print(f"--- Using Exit Multiple: {target_multiple}x ---")
        else:
            denom = max(wacc - terminal_growth, 0.01)
            terminal_value = terminal_fcff / denom
            print("--- Using Gordon Growth Method ---")

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
        ticker = self.ticker
        balance_df = self.balance_df
        income_df = self.income_df
        
        if "ROIC" in income_df.columns:
            if income_df["ROIC"].iloc[-1] > 0:
                print(f"ROIC for {ticker} already calculated.")
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
            if company_overview["sloanRatio"].iloc[0] > 0:
                print(f"Sloan Ratio for {ticker} already calculated")
                sloan_ratio = company_overview["sloanRatio"].iloc[0]
                return sloan_ratio
        income_df = self.income_df
        balance_df = self.balance_df
        cash_df = self.cash_df

        net_income = income_df["netIncome"].iloc[-1]
        fcf = cash_df["FCF"].iloc[-1]
        total_assets = balance_df["totalAssets"].iloc[-1]

        sloan_ratio = (net_income - fcf) / total_assets

        company_overview["sloanRatio"] = sloan_ratio.round(4)
        self.company_overview = company_overview
        return sloan_ratio


    def analyze_peg(self) -> tuple[float, float, float | None, float | None]:
        company_overview = self.company_overview
        income_df = self.income_df
        cash_df = self.cash_df
        ticker = self.ticker

        # 0. Get the Growth Ratios
        try:
            historical_growth = income_df["revGrowth"].tail(10).mean()
            if np.isnan(cash_df["dividendPayout"].iloc[-1]):
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

        print(f"Historical Growth: {historical_growth:.3f}")
        print(f"Projected Growth: {projected_growth:.3f}")
        print(f"Trailing P/E: {trailing_pe}")
        print(f"Forward P/E: {forward_pe}")
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
                
        print(f"Currency standardization complete for {self.ticker}")

    
    async def save_all_to_db(self):
        """
        Final Flush: Persists data from the memory dictionary into 5 separate DB files.
        Handles 'Extra Columns' in annual data via automatic schema migration.
        """
        # Map dictionary keys to their specific database filenames
        db_map = {
            "income": "income_statement.db",
            "balance": "balance_sheet.db",
            "cash": "cash_flow.db",
            "earnings": "earnings.db"
        }

        # 1. Save the 4 Financial Databases
        for cat_key, db_file in db_map.items():
            annual_df = self.data[cat_key]["annual"]
            quarterly_df = self.data[cat_key]["quarterly"]
            
            # Combine into one DF for storage
            df_to_save = pd.concat([annual_df, quarterly_df], ignore_index=True)
            if df_to_save.empty:
                continue

            engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")
            await self._upsert_to_database(engine, df_to_save, cat_key)
            await engine.dispose()

        # 2. Save the Company Overview Database (The 5th DB)
        # This includes your final target price, WACC, and other calculated metrics
        if not self.data["overview"].empty:
            # Update overview DF with the final calculated class attributes
            self.data["overview"]["intrinsic_price"] = self.intrinsic_price
            self.data["overview"]["wacc"] = self.wacc
            self.data["overview"]["roic"] = self.return_on_invested_capital
            self.data["overview"]["timestamp"] = self.timestamp
            
            overview_engine = create_async_engine("sqlite+aiosqlite:///company_overviews.db")
            # For overviews, the PK is 'Symbol' (Capitalized)
            await self._upsert_to_database(overview_engine, self.data["overview"], "Overview", pk_col="Symbol")
            await overview_engine.dispose()

    async def _upsert_to_database(self, engine, df, table_name, pk_col="ticker"):
        """Helper to handle Schema Evolution and prevent duplicates."""
        async with engine.begin() as conn:
            # A. Schema Migration: Check for new columns (e.g., your annual calc columns)
            existing_cols_res = await conn.execute(text(f"PRAGMA table_info({table_name})"))
            existing_cols = [row[1] for row in existing_cols_res.fetchall()]
            
            if existing_cols:
                for col in df.columns:
                    if col not in existing_cols:
                        if col == "reportedCurrency" or col == "report_type" or col == "reportedCurrency":
                            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT"))
                        # SQLite doesn't support multiple columns in one ALTER, so we loop
                        else: 
                            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} REAL"))

            # B. Clean Slate: Remove old data for this ticker before appending the new 'Enriched' version
            # This prevents duplicate rows when you re-run the same stock.
            await conn.execute(text(f"DELETE FROM {table_name} WHERE {pk_col} = :t"), {"t": self.ticker})
            
            # C. Save
            def sync_save(sync_conn):
                df.to_sql(table_name, sync_conn, if_exists="append", index=False)
            
            await conn.run_sync(sync_save)