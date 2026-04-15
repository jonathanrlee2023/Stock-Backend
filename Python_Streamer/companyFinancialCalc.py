import time

from attr import dataclass
import numpy as np
import pandas as pd
from appState import app_state
from dataloader import DataLoader


RISK_FREE_RATE = 0.0375
MARKET_RISK_PREMIUM = 0.055
# Levered Beta averages for Robinhood/Morningstar Categories
# Benchmarked for 2024-2026 market conditions
SECTOR_BETAS = {
    # CYCLICAL (High Economic Sensitivity)
    "CONSUMER CYCLICAL": 1.15,  # AMZN, TSLA, NKE
    "FINANCIAL SERVICES": 1.05,  # JPM, GS, V
    "BASIC MATERIALS": 1.05,  # LIN, FCX, SHW
    "REAL ESTATE": 0.85,  # PLD, AMT (Sensitive to rates)
    # SENSITIVE (Follows the broader market)
    "TECHNOLOGY": 1.25,  # NVDA, MSFT, AAPL, ORCL
    "COMMUNICATION SERVICES": 1.15,  # GOOGL, META, NFLX
    "INDUSTRIALS": 1.02,  # NOC, GE, UPS, BA
    "ENERGY": 0.90,  # XOM, CVX
    # DEFENSIVE (Resistant to economic downturns)
    "HEALTHCARE": 0.85,  # UNH, JNJ, PFE
    "CONSUMER DEFENSIVE": 0.65,  # WMT, KO, PG, COST
    "UTILITIES": 0.55,  # NEE, DUK
}

DEFENSIVE = ["HEALTHCARE", "CONSUMER DEFENSIVE", "UTILITIES"]
SENSITIVE = ["TECHNOLOGY", "COMMUNICATION SERVICES", "INDUSTRIALS", "ENERGY"]
CYCLICAL = ["CONSUMER CYCLICAL", "FINANCIAL SERVICES", "BASIC MATERIALS", "REAL ESTATE"]

@dataclass
class ValuationResults:
    intrinsic_price: float
    dividend_price: float
    wacc: float
    roic: float
    fcf: float
    fcff: float
    fcf_per_share: float
    delta_nwc: float
class CompanyFinancialCalculator:
    def __init__(self, ticker, data, price_at_report=None):
        self.ticker = ticker
        self.income_df = data["income"]["annual"]
        self.balance_df = data["balance"]["annual"]
        self.cash_df = data["cash"]["annual"]
        self.earnings_df = data["earnings"]["annual"]
        self.company_overview = data["overview"]
        self.price_at_report = price_at_report

    def run_valuation(self) -> ValuationResults:
        """The 'Orchestrator' that returns a clean object."""
        fcf, fcff, fcf_per_share, delta_nwc = self.calc_fcf()
        wacc = self.calc_wacc()
        roic = self.roic()
        intrinsic, div_price = self.fcff_forecast()

        self.company_overview["ROIC"] = roic
        self.company_overview["WACC"] = wacc
        self.company_overview["IntrinsicPrice"] = intrinsic
        self.company_overview["DividendPrice"] = div_price
        
        return ValuationResults(
            intrinsic_price=intrinsic,
            dividend_price=div_price,
            wacc=wacc,
            roic=roic,
            fcf=fcf,
            fcff=fcff,
            fcf_per_share=fcf_per_share,
            delta_nwc=delta_nwc
        )
    
    
    
    def calc_fcf(self) -> tuple[float, float, float, float]:
        """
        Calculate the Free Cash Flow (FCF) and Free Cash Flow to the Firm (FCFF) for a given ticker.

        The FCF is calculated as the operating cash flow minus capital expenditures.

        The FCF to the Firm (FCFF) is calculated as the EBIT times (1 - effective tax rate) plus depreciation and amortization, minus capital expenditures and the change in NWC.


        Returns
        -------
        tuple
            A tuple containing the FCF, FCFF, FCF per share, and change in NWC for the most recent fiscal year.
        """
        ticker = self.ticker

        cash_df = self.cash_df
        income_df = self.income_df
        try:
            cash_df["FCF"] = (cash_df["operatingCashflow"] - cash_df["capitalExpenditures"]).round(2)
            cash_df["FCF_YoY_Growth"] = (cash_df["FCF"].pct_change().replace([np.inf, -np.inf], np.nan) * 100).round(2)

            balance_df = self.balance_df
            cash_df["FCF_Per_Share"] = (cash_df["FCF"] / balance_df["commonStockSharesOutstanding"]).round(2)

            income_df["effectiveTaxRate"] = (
                (income_df["incomeTaxExpense"] / income_df["incomeBeforeTax"]).clip(0, 0.35).round(4)
            )

            balance_df["inventory"] = balance_df["inventory"].fillna(0)

            # 1. Define Operating Current Assets (Total Current Assets minus Cash)
            cash_total = balance_df["cashAndShortTermInvestments"].fillna(0)
            # If the aggregate column is 0 or missing, assume we need to sum the parts
            mask = cash_total == 0
            cash_total[mask] = balance_df.loc[mask, "cashAndCashEquivalentsAtCarryingValue"].fillna(0) + balance_df.loc[
                mask, "shortTermInvestments"
            ].fillna(0)

            operating_current_assets = balance_df["totalCurrentAssets"] - cash_total

            # 2. Define Operating Current Liabilities (Total Current Liabilities minus Debt)
            operating_current_liabilities = (
                balance_df["totalCurrentLiabilities"]
                - balance_df["shortTermDebt"].fillna(0)
                - balance_df["currentDebt"].fillna(0)  # Check your specific CSV header for debt
            )

            # 3. Calculate NWC Ratio
            balance_df["NWC"] = operating_current_assets - operating_current_liabilities

            balance_df["deltaNWC"] = balance_df["NWC"].diff()

            if "ebit" not in income_df.columns or income_df["ebit"].isnull().any():
                # Method: Net Income + Interest + Taxes
                income_df["ebit"] = (
                    income_df["netIncome"]
                    + income_df["interestExpense"].fillna(0)
                    + income_df["incomeTaxExpense"].fillna(0)
                )

            cash_df["FCFF"] = (
                income_df["ebit"] * (1 - income_df["effectiveTaxRate"])
                + income_df["depreciationAndAmortization"]
                - cash_df["capitalExpenditures"]
                - balance_df["deltaNWC"]
            ).round(2)
        except Exception as e:
            print(f"Error calculating FCF for {ticker}: {str(e)}")
            print(f"Data: {cash_df.head()}, {income_df.head()}, {balance_df.head()}")

        self.cash_df = cash_df
        self.income_df = income_df
        self.balance_df = balance_df

        return (
            cash_df["FCF"].iloc[-1],
            cash_df["FCFF"].iloc[-1],
            cash_df["FCF_Per_Share"].iloc[-1],
            balance_df["deltaNWC"].iloc[-1],
        )

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

        try:
            beta = company_overview["Beta"].values[0]
            total_debt = recent_balance.iloc[-1]["shortLongTermDebtTotal"]
            equity = company_overview["MarketCapitalization"].values[0] / 1_000_000
            effective_tax_rate = recent_income["effectiveTaxRate"]
            sector = company_overview["Sector"].values[0]

            if pd.isna(beta) or beta is None:
                # Fallback to Sector Beta (Unlevered)
                unlevered_beta = SECTOR_BETAS.get(sector, 1.0)  # Default to 1.0 if sector missing

                # Re-lever it using the company's capital structure
                debt_to_equity = total_debt / equity if equity != 0 else 0
                beta = unlevered_beta * (1 + (1 - effective_tax_rate) * debt_to_equity)

            if abs(SECTOR_BETAS[sector] - beta) > 0.4:
                beta = (SECTOR_BETAS[sector] + beta) / 2.0

            cost_of_equity = RISK_FREE_RATE + beta * MARKET_RISK_PREMIUM
            company_overview["CostOfEquity"] = cost_of_equity

            interest_expense = abs(income_df["interestExpense"].dropna().iloc[-1])
            if pd.isna(interest_expense):
                interest_expense = recent_income["ebit"] - recent_income["incomeBeforeTax"]

            average_debt = recent_balance["shortLongTermDebtTotal"].mean(skipna=True)

            cost_of_debt = min(interest_expense / average_debt, 0.15)

            post_tax_cost_of_debt = cost_of_debt * (1 - effective_tax_rate)

            wacc = (
                (equity / (equity + total_debt)) * cost_of_equity
                + (total_debt / (equity + total_debt)) * post_tax_cost_of_debt
            ) * 100

            company_overview["WACC"] = wacc

        except Exception as e:
            print(f"Error calculating WACC for {ticker}: {str(e)}")
            print(f"Data: {income_df.head()}, {balance_df.head()}")

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

        try:
            income_df["revGrowth"] = income_df["totalRevenue"].pct_change().replace([np.inf, -np.inf], np.nan).round(2)
            income_df["ebitMargin"] = (income_df["ebit"] / income_df["totalRevenue"]).round(4)
            income_df["capexPctRevenue"] = (cash_df["capitalExpenditures"].abs() / income_df["totalRevenue"]).round(4)

            income_df["nwcPctRevenue"] = (balance_df["deltaNWC"] / income_df["totalRevenue"]).round(4)

            income_df["daPctRevenue"] = cash_df["depreciationDepletionAndAmortization"] / income_df["totalRevenue"]
            if "ebit" not in income_df.columns or income_df["ebit"].isnull().any():
                # Method: Net Income + Interest + Taxes
                income_df["ebit"] = (
                    income_df["netIncome"]
                    + income_df["interestExpense"].fillna(0)
                    + income_df["incomeTaxExpense"].fillna(0)
                )
            income_df["ebitGrowth"] = income_df["ebit"].pct_change().round(4)

            balance_df["nwcRatio"] = balance_df["NWC"] / income_df["totalRevenue"]
        except Exception as e:
            print(f"Error calculating forecast metrics for {self.ticker}: {str(e)}")
            print(f"Data: {income_df.head()}, {balance_df.head()}, {cash_df.head()}")


        self.income_df = income_df
        self.balance_df = balance_df

        return

    def dividend_model(self):
        company_overview = self.company_overview
        terminal_growth = self.terminal_growth
        dividend_per_share = company_overview["DividendPerShare"].values[0]
        cost_of_equity = company_overview["CostOfEquity"].values[0]
        intrinsic_price = (dividend_per_share * (1 + terminal_growth)) / (cost_of_equity - terminal_growth)
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
        intrinsic_price = 0.0
        dividend_price = None
        # 1. LOAD DATA
        income_df = self.income_df
        balance_df = self.balance_df
        company_overview = self.company_overview

        try:
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
                start_growth = 0.05  # Conservative mid-cycle recovery
            else:
                # Determine current growth blend (Revenue + EBIT)
                if "ebitGrowth" not in income_df.columns:
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
            if sector in DEFENSIVE:
                terminal_growth = long_term_growth
            elif sector in SENSITIVE:
                terminal_growth = max(long_term_growth / 2.0, 0.05)
            else:
                terminal_growth = (long_term_growth + 0.02) / 2
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
            tax_path = np.clip(tax_path, 0.10, 0.35)  # Keep tax between 10% and 35%

            # 6. RUN THE 10-YEAR PROJECTION
            forecast = []
            revenue = revenue_0
            prev_nwc = revenue_0 * avg_nwc_ratio

            capex_pct = income_df["capexPctRevenue"].iloc[-1]
            da_pct = income_df["daPctRevenue"].iloc[-1]

            for t in range(years):
                revenue *= 1 + growth_path[t]
                ebit = revenue * margin_path[t]
                nopat = ebit * (1 - tax_path[t])

                # Reinvestment Logic
                da = revenue * da_pct
                if t < (years - 1):
                    capex = revenue * capex_pct
                else:
                    capex = da * 1.1  # Terminal Year Steady State Reinvestment

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
            terminal_fcff = max(terminal_fcff, revenue * 0.05)  # Floor at 5% of Rev

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
            liquid_assets = (
                balance_df["cashAndCashEquivalentsAtCarryingValue"].iloc[-1]
                + balance_df["shortTermInvestments"].fillna(0).iloc[-1]
            )
            total_debt = (
                balance_df["shortLongTermDebtTotal"].fillna(0).iloc[-1] + balance_df["longTermDebt"].fillna(0).iloc[-1]
            )
            net_debt = total_debt - liquid_assets

            equity_value = max(enterprise_value - net_debt, 0)
            shares = balance_df["commonStockSharesOutstanding"].iloc[-1]
            intrinsic_price = equity_value / shares if shares > 0 else 0

            # Save back to CSV
            company_overview["IntrinsicPrice"] = round(float(intrinsic_price), 2)
        except Exception as e:
            print(f"Error calculating intrinsic price for {ticker}: {e}")
        
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

        try:
            if "ROIC" in income_df.columns:
                if income_df["ROIC"].iloc[-1] > 0:
                    Roic = income_df["ROIC"].iloc[-1]
                    return Roic
            # Calculate Invested Capital (Debt + Equity - Cash)
            # Note: You need to decide if you use 'Total Assets - Current Liabilities' or the financing approach below
            invested_capital = (
                balance_df["totalShareholderEquity"]
                + balance_df["shortLongTermDebtTotal"].fillna(0)
                - balance_df["cashAndCashEquivalentsAtCarryingValue"]
            )

            # Calculate NOPAT
            nopat = income_df["ebit"] * (1 - income_df["effectiveTaxRate"])

            # ROIC
            income_df["ROIC"] = (nopat / invested_capital).round(4)
        except Exception as e:
            print(f"Error calculating ROIC for {ticker}: {e}")
        self.income_df = income_df
        return income_df["ROIC"].iloc[-1]

    def calc_eps_growth(self) -> float:
        income_df = self.income_df
        company_overview = self.company_overview
        if len(income_df) < 4:
            return 0.0

        eps_series = income_df["netIncome"] / company_overview["SharesOutstanding"].values[0]

        current_eps = eps_series.iloc[-1]
        initial_eps = eps_series.iloc[-4]

        if current_eps <= 0 or initial_eps <= 0:
            return 0.0

        try:
            cagr = (current_eps / initial_eps) ** (1 / 3) - 1
            return cagr * 100
        except Exception:
            return 0.0

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
        try:
            if "sloanRatio" in company_overview.columns:
                val = company_overview["sloanRatio"].iloc[0]
                if pd.notna(val):  # This handles None, NaN, and Null
                    if val > 0:
                        return val
            income_df = self.income_df
            balance_df = self.balance_df
            cash_df = self.cash_df

            net_income = income_df["netIncome"].iloc[-1]
            fcf = cash_df["FCF"].iloc[-1]
            total_assets = balance_df["totalAssets"].iloc[-1]

            sloan_ratio = (net_income - fcf) / total_assets

            company_overview["sloanRatio"] = sloan_ratio.round(4)
        except Exception as e:
            print(f"Error calculating Sloan Ratio for {ticker}: {e}")
        self.company_overview = company_overview
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

        # 0. Get the Growth Ratios
        try:
            historical_growth = income_df["revGrowth"].tail(10).mean(skipna=True)
            if pd.isna(cash_df["dividendPayout"].iloc[-1]):
                retention_ratio = 1
            else:
                dividend_payout_ratio = cash_df["dividendPayout"].iloc[-1] / income_df["netIncome"].iloc[-1]
                retention_ratio = 1 - dividend_payout_ratio
            projected_growth = retention_ratio * income_df["ROIC"].iloc[-1]
        except (ValueError, IndexError):
            print("--- PEG Analysis Failed: Missing Growth Data ---")
            return
        # 1. Get the P/E Ratios
        try:
            trailing_pe = float(company_overview["TrailingPE"].values[0])
            forward_pe = float(company_overview["ForwardPE"].values[0])
        except Exception as e:
            print("--- PEG Analysis Failed: Missing P/E Data ---", e)
            total_net_income = income_df["netIncome"].tail(4).sum()
            shares_outstanding = company_overview["SharesOutstanding"].values[0]

            trailing_eps = total_net_income / shares_outstanding
            self.trailing_pe = self.price_at_report / trailing_eps
            trailing_pe = self.trailing_pe
            forward_pe = None

        try:
            hist_g_int = historical_growth * 100
            proj_g_int = projected_growth * 100
            if hist_g_int > 0 and trailing_pe is not None:
                trailing_peg = trailing_pe / hist_g_int
            else:
                trailing_peg = None

            if proj_g_int > 0 and forward_pe is not None:
                forward_peg = forward_pe / proj_g_int
            else:
                forward_peg = None
                print("--- Forward PEG Analysis Failed: Projected Growth is less than 0% or no Forward P/E ---")

        except (ValueError, IndexError):
            return
        return historical_growth, projected_growth, trailing_peg, forward_peg