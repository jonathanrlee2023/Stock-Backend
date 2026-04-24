import orjson
import pandas as pd
from appState import app_state
from backtest_helper import compute_strategy_stats
from cache import symbol_cache, r

async def get_symbol_id(symbol):
    """Helper to get ID from cache or DB, creating it if necessary."""
    if symbol in symbol_cache:
        return symbol_cache[symbol]

    db = app_state.price_db
    async with db.execute("SELECT symbol_id FROM Symbols WHERE symbol = ?", (symbol,)) as cursor:
        row = await cursor.fetchone()
        if row:
            symbol_cache[symbol] = row[0]
            return row[0]

    cursor = await db.execute("INSERT INTO Symbols (symbol) VALUES (?)", (symbol,))
    new_id = cursor.lastrowid

    await db.commit()

    symbol_cache[symbol] = new_id
    return new_id
async def run_backtest(user_portfolio, benchmark, days_ago, client_id):
    initial_capital = 10_000
    data_loader = app_state.data_loader
    possible_cols = ['datetime', 'timestamp', 'time', 'Date']


    async def calculate_equity_curve(portfolio_dict):
        prices = {}
        for stock, weight in portfolio_dict.items():
            try:
                if stock == "Cash": 
                    continue
                s_id = await get_symbol_id(stock)
                df = await data_loader.load_backtesting_data(stock, s_id, days_ago)
                if df.empty:
                    print(f"Warning: No data found for {stock}. Skipping.")
                    continue
                time_col = next((c for c in possible_cols if c in df.columns), None)

                if time_col is None:
                    raise KeyError(f"Could not find a time column for {stock}. Found: {df.columns.tolist()}")
            except Exception as e:
                print(f"Error processing {stock}: {e}")
                continue

            # Convert to datetime objects and set as index
            try:
                df[time_col] = pd.to_datetime(df[time_col])
                prices[stock] = df.set_index(time_col)["close"]
            except Exception as e:
                print(f"Error processing {stock}: {e}")
        try:
            price_matrix = pd.DataFrame(prices).sort_index()

            stocks_only = {s: v for s, v in portfolio_dict.items() if s != "Cash"}
            weights = pd.Series(stocks_only)
            allocated_dollars = weights * initial_capital

            ipo_prices = price_matrix.apply(lambda col: col.dropna().iloc[0])
            
            shares_held = allocated_dollars / ipo_prices

            position_values = price_matrix.ffill() * shares_held

            pre_ipo_mask = position_values.isna()
            
            sidelined_cash = (pre_ipo_mask * allocated_dollars).sum(axis=1)

            portfolio_base_cash = initial_capital * portfolio_dict.get("Cash", 0)
            
            total_equity = (
                position_values.fillna(0).sum(axis=1) + 
                sidelined_cash + 
                portfolio_base_cash
            )

            return total_equity.reset_index(name="Capital").rename(columns={"index": "datetime"})
        except Exception as e:
            print(f"Error calculating price curve: {e}")

    # Execute for both User and Benchmark
    try:
        test_df = await calculate_equity_curve(user_portfolio)
        bench_df = await calculate_equity_curve(benchmark)
    except Exception as e:
        print(f"Error calculating equity curve: {e}")
        return

    try:
        stats = compute_strategy_stats(test_df)

        test_df["datetime"] = test_df["datetime"].astype(str)
        bench_df["datetime"] = bench_df["datetime"].astype(str)

        payload = {"User": test_df.to_dict("records"), "Benchmark": bench_df.to_dict("records"), "Stats": stats, "ClientID": client_id}

        await r.publish("Backtest_Channel", orjson.dumps(payload))

    except Exception as e:
        print(f"Error computing stats: {e}")