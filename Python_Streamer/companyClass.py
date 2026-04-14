import asyncio
from sqlalchemy import text
import time
import pandas as pd
import numpy as np

from appState import app_state
from cache import exchange_rate_cache, max_fiscal_lookup
from asyncFunc import get_symbol_id, get_furthest_date_for_stock
from dataloader import DataLoader
from companyFinancialCalc import CompanyFinancialCalculator, ValuationResults
from companyDB import CompanyDBHandler
from companyFetch import CompanyDataFetcher

class Company:
    def __init__(self, ticker, api_key, rate_api_key):
        self.ticker = ticker
        self.api_key = api_key
        self.rate_api_key = rate_api_key

        self.data = {
            "income": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "balance": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "cash": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "earnings": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
            "overview": pd.DataFrame(),  # Overviews are usually 1 row
        }

    @classmethod
    async def create(cls, ticker, api_key, rate_api_key):
        """Asynchronous factory to create and fully initialize the instance."""
        self = cls(ticker, api_key, rate_api_key)
        self.symbol_id = await get_symbol_id(self.ticker)

        db_handler = CompanyDBHandler(app_state.engines, self.symbol_id, ticker)

        await db_handler.load_all_from_dbs()

        self.data = db_handler.data

        categories = ["income", "balance", "cash", "earnings"]

        api_handler = CompanyDataFetcher(ticker, self.symbol_id, api_key, rate_api_key, app_state.httpx_client)

        for cat in categories:
            if self.data[cat]["annual"].empty:
                print(f"--- {cat.capitalize()} data missing for {ticker}. Fetching from API... ---")
                await api_handler.fetch_fundamentals(category=cat)
                
                self.data[cat] = api_handler.data[cat]

        if self.data["overview"].empty:
            print(f"--- Overview missing for {ticker}. Fetching from API... ---")
            await api_handler.fetch_fundamentals(category="overview")
            self.data["overview"] = api_handler.data["overview"]

        await api_handler.replace_with_usd()

        self.reorder_data()

        if self.data["overview"].empty or self.data["income"]["annual"].empty:
            print(f"--- Initialization Aborted for {ticker}: No data available ---")
            return None
        self.get_current_price = await self.get_current_price() 
        financial_calculator = CompanyFinancialCalculator(self.ticker, self.data, price_at_report=self.price_at_report)

        self.valuation_results = financial_calculator.run_valuation()
        if self.valuation_results is None:
            print(f"--- Aborting {ticker}: Valuation math failed and returned None ---")
            return None

        self.income_df = self.data["income"]["annual"]
        self.quarterly_income_df = self.data["income"]["quarterly"]
        self.quarterly_balance_df = self.data["balance"]["quarterly"]
        self.quarterly_cash_df = self.data["cash"]["quarterly"]
        self.balance_df = self.data["balance"]["annual"]
        self.cash_df = self.data["cash"]["annual"]
        self.earnings_df = self.data["earnings"]["annual"]
        self.quarterly_earnings_df = self.data["earnings"]["quarterly"]
        self.company_overview = self.data["overview"]
        

        self.market_cap = self.company_overview["MarketCapitalization"].values[0]
        self.fcf, self.fcff, self.fcf_per_share, self.nwc = financial_calculator.calc_fcf()
        self.wacc = self.valuation_results.wacc
        self.intrinsic_price = self.valuation_results.intrinsic_price
        self.dividend_price = self.valuation_results.dividend_price
        self.peg = self.company_overview["PEGRatio"].iloc[0]
        self.return_on_invested_capital = financial_calculator.roic()
        self.sloan = financial_calculator.sloan_ratio()
        self.hist_growth, self.forecasted_growth, self.trailing_peg, self.forward_peg = financial_calculator.analyze_peg()

        if self.peg is None:
            try:
                self.eps_growth = financial_calculator.calc_eps_growth()
                if self.eps_growth <= 0:
                    self.eps_growth = 0.0
                else:
                    self.peg = financial_calculator.trailing_pe / self.eps_growth
            except Exception as e:
                print("Exception in setting PEG: ", e)

        self.sector = self.company_overview["Sector"].values[0]
        self.industry = self.company_overview["Industry"].values[0]

        self.earnings_date = get_furthest_date_for_stock(symbol_id=self.symbol_id)

        # Extract values from the loaded overview
        self._setup_analyst_ratings()

        await db_handler.save_all_to_db()

        def safe_float(val):
            try:
                if val is None or pd.isna(val):
                    return None
                return float(val)
            except Exception as e:
                print("Exception in converting to float: ", e)
                return None

        def safe_int(val):
            try:
                if val is None or pd.isna(val):
                    return None
                return int(val)
            except Exception as e:
                print("Exception in converting to int: ", e)
                return None

        annual_income = self.prepare_df_for_go(self.income_df)
        annual_balance = self.prepare_df_for_go(self.balance_df)
        annual_cash = self.prepare_df_for_go(self.cash_df)
        annual_earnings = self.prepare_df_for_go(self.earnings_df)
        quarterly_income = self.prepare_df_for_go(self.quarterly_income_df)
        quarterly_balance = self.prepare_df_for_go(self.quarterly_balance_df)
        quarterly_cash = self.prepare_df_for_go(self.quarterly_cash_df)
        quarterly_earnings = self.prepare_df_for_go(self.quarterly_earnings_df)

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
            "FCFPerShare": safe_float(self.fcf_per_share),
            "NWC": safe_float(self.nwc),
            "PriceTarget": safe_float(self.price_target),
            "StrongBuy": safe_int(self.strong_buy),
            "Buy": safe_int(self.buy),
            "Hold": safe_int(self.hold),
            "StrongSell": safe_int(self.strong_sell),
            "Sell": safe_int(self.sell),
            "EarningsDate": self.earnings_date,
            "Grade": self.grade_stock(),
            "Sector": self.sector,
            "Industry": self.industry,
            "AnnualIncome": annual_income,
            "AnnualBalance": annual_balance,
            "AnnualCash": annual_cash,
            "AnnualEarnings": annual_earnings,
            "QuarterlyIncome": quarterly_income,
            "QuarterlyBalance": quarterly_balance,
            "QuarterlyCash": quarterly_cash,
            "QuarterlyEarnings": quarterly_earnings,
        }

        return self
    
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

    async def get_current_price(self) -> float:
        """Asynchronously fetches price data using the DataLoader."""
        self.unix_timestamp()  # Sets self.timestamp
        try:
            loader = DataLoader(
                ticker=self.ticker, symbol_id=self.symbol_id, fiscalDate=self.timestamp, connection=app_state.schwab_client
            )   
            self.price_at_report = await loader.load_data()
        except Exception as e:
            print(f"Error fetching current price for {self.ticker}: {str(e)}")

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
        income_df = self.data["income"]["annual"]
        timestamp = str(income_df["date"].values[-1]).split(" ")[0]
        self.timestamp = int(time.mktime(time.strptime(timestamp, "%Y-%m-%d")))



    def prepare_df_for_go(self, df: pd.DataFrame):
        export_df = df.copy()

        # 1. Identify and force numeric conversion for all non-metadata columns
        # We exclude common string/date columns so they don't get 'coerced' to NaN
        metadata_cols = ["date", "ticker", "report_type", "reportTime", "reportedDate", "symbol_id", "reportedCurrency"]

        for col in export_df.columns:
            if col not in metadata_cols:
                # errors='coerce' turns strings like "N/A" or "-" into np.nan
                export_df[col] = pd.to_numeric(export_df[col], errors="coerce")

        # 2. Date Formatting
        if "date" in export_df.columns:
            export_df["date"] = pd.to_datetime(export_df["date"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # 3. CRITICAL: Now that everything is numeric or NaN, swap NaNs for None
        # We use a double-pass to catch both numpy and pandas null variants
        export_df = export_df.replace({np.nan: None, np.inf: None, -np.inf: None})
        export_df = export_df.where(pd.notnull(export_df), None)

        return export_df.to_dict(orient="records")

    def grade_stock(self):
        """
        Grades a stock based on 6 key metrics.

        1. Capital Efficiency: ROIC vs WACC (Weight: 15)
        2. Valuation: PEG Ratio (Weight: 15)
        3. Earnings Quality: Sloan Ratio (Weight: 15)
        4. Margin of safety: Price vs Intrinsic (Weight: 20)
        5. Analyst Sentiment (Weight: 15)
        6. Cash Flow Strength: FCF (Weight: 15)

        Returns a score between 0 and 100. Higher scores indicate better performance.
        """
        score = 0
        try:
            # 1. Capital Efficiency: ROIC vs WACC (Weight: 15)
            # Ideally ROIC > WACC. If ROIC is 2x WACC, it's an elite performer.
            roic = self.return_on_invested_capital
            wacc = self.wacc
            if roic > (wacc * 2):
                score += 15
            elif roic > wacc:
                score += 10
            elif roic > 0:
                score += 5

            # 2. Valuation: PEG Ratio (Weight: 15)
            # Lower is better. < 1.0 is undervalued, > 2.0 is overvalued.
            peg = self.peg
            if peg is not None:
                if 0 < peg <= 1.0:
                    score += 15
                elif 1.0 < peg <= 1.5:
                    score += 10
                elif 1.5 < peg <= 2.0:
                    score += 5

            # 3. Earnings Quality: Sloan Ratio (Weight: 15)
            # -10% to 10% is the safe zone. Outside that indicates accrual risk.
            sloan = self.sloan
            if -0.10 <= sloan <= 0.10:
                score += 15
            elif -0.20 <= sloan <= 0.20:
                score += 7

            # 4. Margin of Safety: Price vs Intrinsic (Weight: 20)
            intrinsic = self.intrinsic_price
            current = self.price_at_report
            if intrinsic > (current * 1.3):
                score += 15  # 30% Margin of Safety
            elif intrinsic > current:
                score += 10

            # 5. Analyst Sentiment (Weight: 15)
            # Ratio of Buys to Sells
            buys = self.strong_buy + self.buy
            sells = self.strong_sell + self.sell
            if buys > (sells * 3) and buys > 5:
                score += 15
            elif buys > sells:
                score += 8

            # 6. Cash Flow Strength: FCF (Weight: 15)
            # Positive FCF is mandatory for a good grade
            if self.fcf > 0:
                score += 15

            hist = self.hist_growth
            fore = self.forecasted_growth
            if fore >= (hist * 0.75) and fore > 0:
                score += 10
            elif fore > 0:
                score += 5
        except Exception as e:
            print("Error Grading Stock: ", e)

        return int(score)

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
                {"t": self.ticker, "r": report_type},
            )
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    