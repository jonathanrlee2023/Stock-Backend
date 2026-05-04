import asyncio
from sqlalchemy import text
import time
import pandas as pd
import numpy as np

from core.appState import app_state
from streaming.asyncFunc import get_symbol_id, get_furthest_date_for_stock
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
        return await CompanyBuilder(ticker, api_key, rate_api_key).build()
    
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
        loader = app_state.data_loader
        try:
            self.price_at_report = await loader.load_data(ticker=self.ticker, symbol_id=self.symbol_id, fiscalDate=self.timestamp)
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
        Grades a stock based on 8 key metrics, with both positive and negative scoring.
        Final score is clamped between 0 and 100.
        Metric Details:
        ---------------
        1. Capital Efficiency (ROIC vs WACC)
        - ROIC and WACC must be in consistent units (both as percentages).
        - ROIC > 2x WACC: +15 (elite capital allocator)
        - ROIC > WACC:    +10 (creating shareholder value)
        - ROIC > 0:        +5 (at least profitable on capital)
        - ROIC < 0:       -10 (actively destroying capital)

        2. Valuation (PEG Ratio)
        - Negative PEG indicates declining earnings and is penalized.
        - 0 < PEG <= 1.0:  +15 (undervalued relative to growth)
        - 1.0 < PEG <= 1.5: +10 (fairly valued)
        - 1.5 < PEG <= 2.0:  +5 (mildly overvalued)
        - PEG < 0:          -10 (earnings declining)

        3. Earnings Quality (Sloan Ratio)
        - Measures accrual component of earnings. High absolute values
            indicate earnings are not backed by cash flow.
        - |sloan| <= 0.10:  +15 (high quality earnings)
        - |sloan| <= 0.20:   +7 (acceptable)
        - |sloan| <= 0.25:   -5 (elevated accrual risk)
        - |sloan| >  0.25:  -10 (aggressive accrual manipulation territory)

        4. Margin of Safety (Intrinsic vs Current Price)
        - Based on DCF-derived intrinsic price vs price at last fiscal report.
        - intrinsic > 1.5x current:  +20 (deep value, 50% margin of safety)
        - intrinsic > 1.3x current:  +15 (strong margin of safety, 30%)
        - intrinsic > current:        +8 (trading below intrinsic)
        - intrinsic < 0.7x current:  -5  (trading at 30%+ premium to intrinsic)

        5. Analyst Sentiment
        - Normalized against structural sell-side bullishness bias.
            Raw buy/sell counts are insufficient — buy% relative to total is used.
        - buy% > 70% and buys > 5: +15 (exceptional conviction)
        - buy% > 55%:               +8 (above baseline bullishness)
        - buy% < 40%:               -5 (genuine negative consensus)

        6. Free Cash Flow Yield (FCF / Market Cap)
        - FCF values are stored in millions; market cap in raw dollars.
        - FCF yield > 5%:  +15 (strong cash generation)
        - FCF yield > 2%:   +8 (acceptable)
        - FCF yield > 0%:   +3 (positive but thin)
        - FCF yield <= 0%:  -5 (cash burn)

        7. Growth Sustainability (Forecasted vs Historical)
        - Rewards both maintenance of growth rate AND absolute growth level.
        - Forecasted growth >= 75% of historical AND fore > 10%: +10
        - Forecasted growth >= 75% of historical:                 +7
        - Forecasted growth > 0%:                                 +3

        8. Leverage (Debt-to-Equity)
        - High leverage amplifies downside and increases insolvency risk.
        - D/E < 0.3:  +10 (conservatively financed)
        - D/E < 1.0:   +5 (manageable)
        - D/E > 3.0:  -10 (dangerously leveraged)

        Returns:
            int: Score between 0 and 100 inclusive.
        """
        score = 0
        try:
            # 1. Capital Efficiency: ROIC vs WACC (Weight: 15)
            # Ideally ROIC > WACC. If ROIC is 2x WACC, it's an elite performer.
            roic_pct = self.return_on_invested_capital * 100
            wacc_pct = self.wacc  # already a percent

            if roic_pct > (wacc_pct * 2):
                score += 15
            elif roic_pct > wacc_pct:
                score += 10
            elif roic_pct > 0:
                score += 5
            
            if roic_pct < 0:
                score -= 10

            # 2. Valuation: PEG Ratio (Weight: 15)
            # Lower is better. < 1.0 is undervalued, > 2.0 is overvalued.
            peg = self.peg
            if peg is not None:
                if peg < 0:
                    score -= 10
                elif 0 < peg <= 1.0:
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

            if sloan > 0.25 or sloan < -0.25:
                score -= 10
            elif sloan > 0.20 or sloan < -0.20:
                score -= 5

            # 4. Margin of Safety: Price vs Intrinsic (Weight: 20)
            intrinsic = self.intrinsic_price
            current = self.price_at_report
            if intrinsic > (current * 1.5):   # 50% margin of safety — deep value
                score += 20
            elif intrinsic > (current * 1.3):  # 30% margin
                score += 15
            elif intrinsic > current:
                score += 8
            elif intrinsic < (current * 0.7):  # Trading at 30%+ premium to intrinsic
                score -= 5

            # 5. Analyst Sentiment (Weight: 15)
            # Ratio of Buys to Sells
            buys = self.strong_buy + self.buy
            sells = self.strong_sell + self.sell
            total = buys + sells + self.hold
            if total > 0:
                buy_pct = buys / total
                # 70%+ buy rate is genuinely exceptional given structural bias
                if buy_pct > 0.70 and buys > 5:
                    score += 15
                elif buy_pct > 0.55:
                    score += 8
                # Below 40% buy rate is a genuine red flag
                elif buy_pct < 0.40:
                    score -= 5

            # 6. Cash Flow Strength: FCF (Weight: 15)
            if self.market_cap and self.market_cap > 0:
                fcf_yield = (self.fcf * 1_000_000) / self.market_cap  # fcf is in millions
                if fcf_yield > 0.05:  # >5% FCF yield is strong
                    score += 15
                elif fcf_yield > 0.02:
                    score += 8
                elif fcf_yield > 0:
                    score += 3
                else:
                    score -= 5  # Negative FCF is a penalty, not neutral

            hist = self.hist_growth
            fore = self.forecasted_growth
            if fore > 0 and hist > 0:
                maintenance_ratio = fore / hist
                if maintenance_ratio >= 0.75 and fore > 0.10:  # Maintaining AND growing fast
                    score += 10
                elif maintenance_ratio >= 0.75:
                    score += 7
                elif fore > 0:
                    score += 3

            debt_to_equity = self.balance_df["shortLongTermDebtTotal"].iloc[-1] / \
                 self.balance_df["totalShareholderEquity"].iloc[-1]

            if debt_to_equity < 0.3:
                score += 10
            elif debt_to_equity < 1.0:
                score += 5
            elif debt_to_equity > 3.0:
                score -= 10
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


class CompanyBuilder:
    def __init__(self, ticker, api_key, rate_api_key):
        self.ticker = ticker
        self.api_key = api_key
        self.rate_api_key = rate_api_key
        self.company = None
        self.db_handler = None
        self.financial_calculator = None

    async def build(self):
        await self.initialize_company()
        await self.load_from_db()
        await self.fetch_missing_fundamentals()
        if not self.has_required_data():
            return None
        await self.run_valuation()
        if self.company.valuation_results is None:
            return None
        self.populate_derived_fields()
        await self.persist()
        self.finalize_report()
        return self.company

    async def initialize_company(self):
        self.company = Company(self.ticker, self.api_key, self.rate_api_key)
        self.company.symbol_id = await get_symbol_id(self.company.ticker)
        self.db_handler = CompanyDBHandler(app_state.engines, self.company.symbol_id, self.ticker, data=self.company.data)
        return self

    async def load_from_db(self):
        await self.db_handler.load_all_from_dbs()
        self.company.data = self.db_handler.data
        return self

    async def fetch_missing_fundamentals(self):
        categories = ["income", "balance", "cash", "earnings"]
        api_handler = CompanyDataFetcher(
            self.ticker,
            self.company.symbol_id,
            self.api_key,
            self.rate_api_key,
            data=self.company.data,
            client=app_state.httpx_client,
        )

        for cat in categories:
            if self.company.data[cat]["annual"].empty:
                print(f"--- {cat.capitalize()} data missing for {self.ticker}. Fetching from API... ---")
                await api_handler.fetch_fundamentals(category=cat)
                self.company.data[cat] = api_handler.data[cat]

        if self.company.data["overview"].empty:
            print(f"--- Overview missing for {self.ticker}. Fetching from API... ---")
            await api_handler.fetch_fundamentals(category="overview")
            self.company.data["overview"] = api_handler.data["overview"]

        await api_handler.replace_with_usd()
        self.company.reorder_data()
        return self

    def has_required_data(self):
        if self.company.data["overview"].empty or self.company.data["income"]["annual"].empty:
            print(f"--- Initialization Aborted for {self.ticker}: No data available ---")
            return False
        return True

    async def run_valuation(self):
        await self.company.get_current_price()
        self.financial_calculator = CompanyFinancialCalculator(
            self.company.ticker,
            self.company.data,
            price_at_report=self.company.price_at_report,
        )
        self.company.valuation_results = self.financial_calculator.run_valuation()
        if self.company.valuation_results is None:
            print(f"--- Aborting {self.ticker}: Valuation math failed and returned None ---")
        return self

    def populate_derived_fields(self):
        company = self.company
        financial_calculator = self.financial_calculator

        company.income_df = company.data["income"]["annual"]
        company.quarterly_income_df = company.data["income"]["quarterly"]
        company.quarterly_balance_df = company.data["balance"]["quarterly"]
        company.quarterly_cash_df = company.data["cash"]["quarterly"]
        company.balance_df = company.data["balance"]["annual"]
        company.cash_df = company.data["cash"]["annual"]
        company.earnings_df = company.data["earnings"]["annual"]
        company.quarterly_earnings_df = company.data["earnings"]["quarterly"]
        company.company_overview = company.data["overview"]

        company.market_cap = company.company_overview["MarketCapitalization"].values[0]
        company.fcf = company.valuation_results.fcf
        company.fcff = company.valuation_results.fcff
        company.fcf_per_share = company.valuation_results.fcf_per_share
        company.nwc = company.valuation_results.delta_nwc
        company.wacc = company.valuation_results.wacc
        company.intrinsic_price = company.valuation_results.intrinsic_price
        company.dividend_price = company.valuation_results.dividend_price
        company.peg = company.company_overview["PEGRatio"].iloc[0]
        company.return_on_invested_capital = financial_calculator.roic()
        company.sloan = financial_calculator.sloan_ratio()
        company.hist_growth, company.forecasted_growth, company.trailing_peg, company.forward_peg = financial_calculator.analyze_peg()

        if company.peg is None:
            try:
                company.eps_growth = financial_calculator.calc_eps_growth()
                if company.eps_growth <= 0:
                    company.eps_growth = 0.0
                else:
                    company.peg = financial_calculator.trailing_pe / company.eps_growth
            except Exception as e:
                print("Exception in setting PEG: ", e)

        company.sector = company.company_overview["Sector"].values[0]
        company.industry = company.company_overview["Industry"].values[0]
        company.earnings_date = get_furthest_date_for_stock(symbol_id=company.symbol_id)
        company._setup_analyst_ratings()
        company.grade = company.grade_stock()
        return self

    async def persist(self):
        await self.db_handler.save_all_to_db()
        return self

    @staticmethod
    def _safe_float(val):
        try:
            if val is None or pd.isna(val):
                return None
            return float(val)
        except Exception as e:
            print("Exception in converting to float: ", e)
            return None

    @staticmethod
    def _safe_int(val):
        try:
            if val is None or pd.isna(val):
                return None
            return int(val)
        except Exception as e:
            print("Exception in converting to int: ", e)
            return None

    def finalize_report(self):
        company = self.company
        annual_income = company.prepare_df_for_go(company.income_df)
        annual_balance = company.prepare_df_for_go(company.balance_df)
        annual_cash = company.prepare_df_for_go(company.cash_df)
        annual_earnings = company.prepare_df_for_go(company.earnings_df)
        quarterly_income = company.prepare_df_for_go(company.quarterly_income_df)
        quarterly_balance = company.prepare_df_for_go(company.quarterly_balance_df)
        quarterly_cash = company.prepare_df_for_go(company.quarterly_cash_df)
        quarterly_earnings = company.prepare_df_for_go(company.quarterly_earnings_df)

        company.final_report = {
            "Symbol": str(company.ticker),
            "MarketCap": self._safe_int(company.market_cap),
            "PEG": self._safe_float(company.peg),
            "Sloan": self._safe_float(company.sloan),
            "ROIC": self._safe_float(company.return_on_invested_capital),
            "HistGrowth": self._safe_float(company.hist_growth),
            "ForecastedGrowth": self._safe_float(company.forecasted_growth),
            "TrailingPEG": self._safe_float(company.trailing_peg),
            "ForwardPEG": self._safe_float(company.forward_peg),
            "IntrinsicPrice": self._safe_float(company.intrinsic_price),
            "DividendPrice": self._safe_float(company.dividend_price),
            "PriceAtReport": self._safe_float(company.price_at_report),
            "WACC": self._safe_float(company.wacc),
            "FCFF": self._safe_float(company.fcff),
            "FCF": self._safe_float(company.fcf),
            "FCFPerShare": self._safe_float(company.fcf_per_share),
            "NWC": self._safe_float(company.nwc),
            "PriceTarget": self._safe_float(company.price_target),
            "StrongBuy": self._safe_int(company.strong_buy),
            "Buy": self._safe_int(company.buy),
            "Hold": self._safe_int(company.hold),
            "StrongSell": self._safe_int(company.strong_sell),
            "Sell": self._safe_int(company.sell),
            "EarningsDate": company.earnings_date,
            "Grade": company.grade,
            "Sector": company.sector,
            "Industry": company.industry,
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

    