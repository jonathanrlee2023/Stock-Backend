from core.cache import symbol_cache
from core.appState import app_state

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