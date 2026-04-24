import numpy as np
import pytest
import pandas as pd
from companyFinancialCalc import CompanyFinancialCalculator
from companyClass import Company


@pytest.mark.skip(reason="just testing the import")
def test_example():
    assert True


def test_prepare_df_for_go_formatting():
    comp = Company("TEST", "key", "key")
    # Create a dataframe with common financial "messy" data
    df = pd.DataFrame(
        {
            "date": ["2023-12-31", "2023-09-30"],
            "totalRevenue": ["150000000", "None"],  # String numbers and "None"
            "netIncome": [50000000.0, float("nan")],  # Floats and NaNs
            "reportedCurrency": ["USD", "USD"],  # Metadata should stay as string
        }
    )

    result = comp.prepare_df_for_go(df)

    # Assertions
    assert result[0]["totalRevenue"] == 150000000.0
    assert result[1]["totalRevenue"] is None
    assert result[1]["netIncome"] is None
    assert "T00:00:00Z" in result[0]["date"]  # Ensure Go-compatible date format


def test_grade_stock_elite():
    comp = Company("AAPL", "mock_key", "mock_rate_key")

    # 1. Capital Efficiency: ROIC (30%) > 2x WACC (10%) -> +15
    comp.return_on_invested_capital = 0.30 
    comp.wacc = 10.0  

    # 2. Valuation: PEG 0.5 -> +15
    comp.peg = 0.5  

    # 3. Earnings Quality: Sloan 0.05 -> +15
    comp.sloan = 0.05  

    # 4. Margin of Safety: Intrinsic (160) > 1.5x Current (100) -> +20
    comp.intrinsic_price = 160  
    comp.price_at_report = 100  

    # 5. Analyst Sentiment: 10 buys, 0 sells, 0 holds (100% buy) -> +15
    comp.strong_buy, comp.buy = 5, 5
    comp.strong_sell, comp.sell, comp.hold = 0, 0, 0

    # 6. FCF Yield: 100M FCF / 1B Market Cap = 10% -> +15
    comp.fcf = 100  # in millions
    comp.market_cap = 1_000_000_000  

    # 7. Growth Sustainability: Fore (15%) is 100% of Hist (15%) -> +10
    comp.hist_growth = 0.15
    comp.forecasted_growth = 0.15

    # 8. Leverage: D/E 0.1 -> +10
    comp.balance_df = pd.DataFrame({
        "shortLongTermDebtTotal": [100],
        "totalShareholderEquity": [1000]
    })

    # Total expected: 15+15+15+20+15+15+10+10 = 115 (Function returns int)
    score = comp.grade_stock()
    assert score >= 100


def test_fcff_forecast_down_cycle_recovery():
    """
    Validates down-cycle detection and recovery assumptions in fcff_forecast.

    Scenario: Revenue declining + EBIT margin collapsing to negative.
        avg_revenue = (140+135+125+110+90) / 5 = 120
        current_revenue = 90
        revenue_gap = (120 - 90) / 120 = 0.25
        start_growth = min(0.05 + (0.25 * 0.5), 0.15) = 0.15

    Key behaviors under test:
        - is_down_cycle triggers (ebit_margin_0 < 0)
        - avg_ebit_margin (0.126) used instead of current (-0.05)
        - start_growth scales with revenue trough depth, capped at 15%
        - terminal_growth stays below start_growth and below 6%
    """
    data = {
        "income": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
        "balance": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
        "cash": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
        "earnings": {"annual": pd.DataFrame(), "quarterly": pd.DataFrame()},
        "overview": pd.DataFrame()
    }

    data["income"]["annual"] = pd.DataFrame(
        {
            "totalRevenue":     [140,    135,    125,    110,    90   ],
            "ebitMargin":       [0.20,   0.20,   0.18,   0.10,  -0.05],
            "revGrowth":        [0.10,  -0.036, -0.074, -0.120, -0.182],
            "ebit":             [28.0,   27.0,   22.5,   11.0,  -4.5 ],
            "ebitGrowth":       [0.10,  -0.036, -0.167, -0.511, -1.409],
            "effectiveTaxRate": [0.21]  * 5,
            "capexPctRevenue":  [0.05]  * 5,
            "daPctRevenue":     [0.04]  * 5,
            "nwcPctRevenue":    [0.01]  * 5,
        }
    )

    data["balance"]["annual"] = pd.DataFrame(
        {
            "nwcRatio":                              [0.10] * 5,
            "cashAndCashEquivalentsAtCarryingValue": [50]   * 5,
            "shortTermInvestments":                  [0]    * 5,
            "shortLongTermDebtTotal":                [10]   * 5,
            "commonStockSharesOutstanding":          [10]   * 5,
            "totalShareholderEquity":                [100]  * 5,
        }
    )

    data["overview"] = pd.DataFrame(
        {
            "MarketCapitalization": [500_000_000],
            "WACC":                 [8.0],
            "CostOfEquity":         [10.0],
            "Sector":               ["TECHNOLOGY"],
            "Industry":             ["SOFTWARE"],
            "DividendPerShare":     [np.nan],
        }
    )

    comp = CompanyFinancialCalculator("TEST", data, price_at_report=100.0)
    price, div = comp.fcff_forecast()

    # Basic sanity
    assert price > 0, "Intrinsic price must be positive"
    assert div is None, "Dividend model should not trigger on NaN dividend"

    # revenue_gap = (120 - 90) / 120 = 0.25
    # start_growth = min(0.05 + 0.25 * 0.5, 0.15) = 0.15
    assert comp.start_growth == pytest.approx(0.15, abs=1e-6), (
        f"Expected start_growth=0.15 from revenue_gap=0.25, got {comp.start_growth}"
    )

    # terminal_growth must satisfy both constraints
    assert comp.terminal_growth < comp.start_growth, (
        "terminal_growth must be below start_growth"
    )
    assert comp.terminal_growth <= 0.06, (
        "terminal_growth must not exceed 6% absolute cap"
    )
    assert comp.terminal_growth > 0, (
        "terminal_growth should be positive for TECHNOLOGY sector"
    )


# def test_growth_blending_logic():
#     comp = Company("GROWTH_CO", "key", "key")

#     # 1. Income Data: 10% Revenue Growth vs 0% EBIT Growth
#     # actual_growth = (0.10 * 0.7) + (0.00 * 0.3) = 0.07
#     comp.income_df = pd.DataFrame(
#         {
#             "totalRevenue": [100, 110, 121, 133, 146],  # 10% growth
#             "ebit": [20, 20, 20, 20, 20],  # 0% growth
#             "ebitMargin": [0.2, 0.18, 0.16, 0.15, 0.14],
#             "revGrowth": [0.1] * 5,
#             "ebitGrowth": [0.0] * 5,
#             "effectiveTaxRate": [0.21] * 5,
#             "capexPctRevenue": [0.05] * 5,
#             "daPctRevenue": [0.04] * 5,
#         }
#     )

#     # 2. Balance Sheet (Needs 5 rows to match Income Statement length)
#     comp.balance_df = pd.DataFrame(
#         {
#             "nwcRatio": [0.1] * 5,
#             "cashAndCashEquivalentsAtCarryingValue": [50] * 5,
#             "shortTermInvestments": [0] * 5,
#             "shortLongTermDebtTotal": [10] * 5,
#             "longTermDebt": [40] * 5,
#             "commonStockSharesOutstanding": [10] * 5,
#         }
#     )

#     # 3. Overview (Only needs 1 row)
#     comp.company_overview = pd.DataFrame(
#         {
#             "MarketCapitalization": [500_000_000],  # < 1B = 10 year forecast
#             "WACC": [8.0],
#             "CostOfEquity": [10.0],
#             "Sector": ["TECHNOLOGY"],
#             "Industry": ["SOFTWARE"],
#             "DividendPerShare": [np.nan],  # Use NaN to skip the dividend model crash
#         }
#     )

#     # Run
#     comp.fcff_forecast()

#     # Assert the blended growth is 7%
#     assert 0.069 < comp.start_growth < 0.071
