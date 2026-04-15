import asyncio
import asyncFunc
import pandas as pd
from cache import max_fiscal_lookup
from sqlalchemy import text


class CompanyDBHandler:
    def __init__(self, engines: dict, symbol_id: int, ticker: str, data: dict):
        # DIP: Inject the engines needed rather than reaching for app_state
        self.engines = engines
        self.symbol_id = symbol_id
        self.ticker = ticker
        self.data = data

    async def _check_db_for_ticker(self, table_name, engine, ticker):
        async with engine.connect() as conn:
            try:
                check_sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                table_check = await conn.execute(text(check_sql))
                if table_check.fetchone() is None:
                    return False
                result = await conn.execute(
                    text(f"SELECT 1 FROM {table_name} WHERE ticker = :ticker LIMIT 1"), {"ticker": ticker}
                )
                return result.fetchone() is not None
            except Exception as e:
                print(f"Exception in checking DB for ticker {ticker}: {e}")
                return False

    async def _save_df_to_sql(self, engine, df, table_name):
        # We wrap the sync to_sql.
        # Using if_exists='append' allows multiple tickers to live in the same DB.
        def sync_save(connection):
            df.to_sql(table_name, connection, if_exists="append", index=False)

        async with engine.begin() as conn:
            await conn.run_sync(sync_save)

    async def load_all_from_dbs(self):
        """
        Checks all 5 databases and loads existing data into self.data structure.
        Separates quarterly and annual data for financials.
        """
        symbol_id = self.symbol_id

        # 1. Parallelize the fetches
        # Fetching from 5 files at once is faster than one by one
        tasks = []
        db_keys = ["income", "balance", "cash", "earnings", "overview"]

        for key in db_keys:
            # Check if we actually need new data before bothering the DB
            if key not in self.engines:
                print(f"⚠️ Warning: Engine for '{key}' not found in engines. Skipping.")
                continue
            if key != "overview":
                if await asyncFunc.check_for_new_earnings(symbol_id, key):
                    continue
            tasks.append(self._fetch_category(key))

        await asyncio.gather(*tasks)

    async def _fetch_category(self, key):
        engine = self.engines[key]
        async with engine.connect() as conn:
            # Querying by symbol_id hits the INDEX immediately
            result = await conn.execute(text(f"SELECT * FROM {key} WHERE symbol_id = :sid"), {"sid": self.symbol_id})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            if df.empty:
                return

            if key == "overview":
                self.data["overview"] = df
            else:
                # Vectorized separation is faster than manual loops
                self.data[key]["annual"] = df[df["report_type"] == "annual"]
                self.data[key]["quarterly"] = df[df["report_type"] == "quarterly"]

    async def save_all_to_db(self):
        """
        Final Flush: Persists data from the memory dictionary into 5 separate DB files.
        Handles 'Extra Columns' in annual data via automatic schema migration.
        """
        global max_fiscal_lookup
        db_map = ["income", "balance", "cash", "earnings"]

        # 1. Save the 4 Financial Databases
        for cat_key in db_map:
            try:
                annual_df = self.data[cat_key]["annual"]
                quarterly_df = self.data[cat_key]["quarterly"]

                latest_memory_date = pd.to_datetime(quarterly_df["date"]).max()
                cached_date = max_fiscal_lookup[cat_key].get(self.symbol_id)

                if cached_date and latest_memory_date <= pd.to_datetime(cached_date):
                    continue

                if not annual_df.empty or not quarterly_df.empty:
                    df = pd.concat([annual_df, quarterly_df], ignore_index=True)
                    df["symbol_id"] = self.symbol_id
                    await self._upsert_to_database(self.engines[cat_key], df, cat_key)
            except Exception as e:
                print(f"--- Saving {cat_key} to DB Failed: {e} ---")
                print(f"DataFrame head for {cat_key}:\n{self.data[cat_key]['annual'].head()}\n{self.data[cat_key]['quarterly'].head()}")

        if not self.data["overview"].empty:
            overview_df = self.data["overview"]
            overview_df["symbol_id"] = self.symbol_id

            await self._upsert_to_database(self.engines["overview"], overview_df, "overview")

    async def _upsert_to_database(self, engine, df, table_name):
        """
        Handles Schema Evolution and prevents duplicates by using symbol_id.
        Ensures the transition from ticker strings to integer IDs is seamless.
        """
        async with engine.begin() as conn:
            await conn.execute(text(f"DELETE FROM {table_name} WHERE symbol_id = :sid"), {"sid": self.symbol_id})

            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    table_name, sync_conn, if_exists="append", index=False, method="multi", chunksize=500
                )
            )

            await conn.commit()