import numpy as np
import pytest
import pandas as pd
from companyClass import Company


@pytest.mark.skip(reason="just testing the import")
def test_example():
    assert True

def test_prepare_df_for_go_formatting():
    comp = Company("TEST", "key", "key")
    # Create a dataframe with common financial "messy" data
    df = pd.DataFrame({
        "date": ["2023-12-31", "2023-09-30"],
        "totalRevenue": ["150000000", "None"], # String numbers and "None"
        "netIncome": [50000000.0, float('nan')], # Floats and NaNs
        "reportedCurrency": ["USD", "USD"]       # Metadata should stay as string
    })
    
    result = comp.prepare_df_for_go(df)
    
    # Assertions
    assert result[0]["totalRevenue"] == 150000000.0
    assert result[1]["totalRevenue"] is None
    assert result[1]["netIncome"] is None
    assert "T00:00:00Z" in result[0]["date"] # Ensure Go-compatible date format

def test_grade_stock_calculation():
    comp = Company("AAPL", "mock_key", "mock_rate_key")
    
    # Manually set metrics to simulate a "perfect" stock
    comp.return_on_invested_capital = 0.30  # High ROIC
    comp.wacc = 0.10                        # 10% WACC
    comp.peg = 0.5                          # Very undervalued
    comp.sloan = 0.02                       # High quality
    comp.intrinsic_price = 200              # Worth 200
    comp.price_at_report = 100              # Trading at 100
    comp.strong_buy, comp.buy = 10, 5       # Analyst support
    comp.strong_sell, comp.sell, comp.hold = 0, 0, 1
    comp.fcf = 1000000                      # Positive cash flow
    
    # Test high grade
    score = comp.grade_stock()
    assert score > 80

def test_fcff_forecast_down_cycle_recovery():
    comp = Company("RECOVERY_CO", "key", "key")
    
    # 1. Mock income_df (The "Bust" scenario)
    comp.income_df = pd.DataFrame({
        "totalRevenue": [100, 110, 120, 130, 140],
        "ebitMargin": [0.20, 0.20, 0.20, 0.20, -0.10], 
        "revGrowth": [0.1] * 5,
        "ebit": [20, 22, 24, 26, -14],
        "ebitGrowth": [0.1, 0.1, 0.1, 0.1, -1.5],
        "effectiveTaxRate": [0.21] * 5,
        "capexPctRevenue": [0.05] * 5,
        "daPctRevenue": [0.04] * 5
    })

    comp.balance_df = pd.DataFrame({
        "nwcRatio": [0.1] * 5,
        "cashAndCashEquivalentsAtCarryingValue": [50] * 5,
        "shortTermInvestments": [0] * 5,
        "shortLongTermDebtTotal": [10] * 5,
        "longTermDebt": [40] * 5,
        "commonStockSharesOutstanding": [10] * 5
    })

    # 3. Mock company_overview (All columns must be length 1)
    comp.company_overview = pd.DataFrame({
        "MarketCapitalization": [500_000_000],
        "WACC": [8.0],
        "CostOfEquity": [10.0], # Added this! (Usually higher than WACC)
        "Sector": ["TECHNOLOGY"],
        "Industry": ["SOFTWARE"],
        "DividendPerShare": [0]
    })

    # Run the forecast
    price, div = comp.fcff_forecast()
    
    # Assertions
    assert price > 0
    # The code should have detected the down cycle and used avg_ebit_margin (0.20) 
    # instead of current (-0.10)
    assert comp.start_growth == 0.05

def test_growth_blending_logic():
    comp = Company("GROWTH_CO", "key", "key")
    
    # 1. Income Data: 10% Revenue Growth vs 0% EBIT Growth
    # actual_growth = (0.10 * 0.7) + (0.00 * 0.3) = 0.07
    comp.income_df = pd.DataFrame({
        "totalRevenue": [100, 110, 121, 133, 146], # 10% growth
        "ebit": [20, 20, 20, 20, 20],              # 0% growth
        "ebitMargin": [0.2, 0.18, 0.16, 0.15, 0.14],
        "revGrowth": [0.1] * 5,
        "ebitGrowth": [0.0] * 5,
        "effectiveTaxRate": [0.21] * 5,
        "capexPctRevenue": [0.05] * 5,
        "daPctRevenue": [0.04] * 5
    })
    
    # 2. Balance Sheet (Needs 5 rows to match Income Statement length)
    comp.balance_df = pd.DataFrame({
        "nwcRatio": [0.1] * 5,
        "cashAndCashEquivalentsAtCarryingValue": [50] * 5,
        "shortTermInvestments": [0] * 5,
        "shortLongTermDebtTotal": [10] * 5,
        "longTermDebt": [40] * 5,
        "commonStockSharesOutstanding": [10] * 5
    })
    
    # 3. Overview (Only needs 1 row)
    comp.company_overview = pd.DataFrame({
        "MarketCapitalization": [500_000_000], # < 1B = 10 year forecast
        "WACC": [8.0],
        "CostOfEquity": [10.0],
        "Sector": ["TECHNOLOGY"],
        "Industry": ["SOFTWARE"],
        "DividendPerShare": [np.nan] # Use NaN to skip the dividend model crash
    })
    
    # Run
    comp.fcff_forecast()
    
    # Assert the blended growth is 7%
    assert 0.069 < comp.start_growth < 0.071