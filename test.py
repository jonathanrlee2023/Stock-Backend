import sqlite3
import time
import finnhub

makret_cap_dict = {}

finnhub_client = finnhub.Client(api_key="d1mlkl1r01qlvnp378dgd1mlkl1r01qlvnp378e0")

response = finnhub_client.earnings_calendar(
    _from="2025-07-21",
    to="2025-07-25",
    symbol="",  # Leave empty for all symbols
    international=False
)

# The response is already a dict
# You can directly parse or print it
calendar = response.get("earningsCalendar", [])


for entry in calendar:
    symbol = entry["symbol"]
    time.sleep(1)
    market_cap_response = (finnhub_client.company_profile2(symbol=symbol))
    market_cap = market_cap_response.get("marketCapitalization")
    makret_cap_dict[symbol] = market_cap


top_10 = dict(sorted(makret_cap_dict.items(), key=lambda item: item[1] or 0, reverse=True)[:25])

for symbol, cap in top_10.items():
    conn = sqlite3.connect('Tracker.db')
    cursor = conn.cursor()
    insert_table = 'INSERT OR REPLACE INTO Tracker (id) VALUES (?)'
    cursor.execute(insert_table, (symbol,))
    conn.commit()
    conn.close()
    quote = finnhub_client.quote(symbol=symbol)
    price = quote.get("c")
    time.sleep(1)
    print(f"{symbol}: ${format(cap * 1_000_000, ",")} Price: {price}")