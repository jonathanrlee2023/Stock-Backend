from datetime import datetime, date, timedelta
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Optional

def find_long_db_files(root_dir: str, min_length: int = 15):
    root = Path(root_dir)
    for path in root.rglob('*.db'):
        # path.name includes the extension; use path.stem to exclude it
        if len(path.name) > min_length:
            yield path


PATTERN = re.compile(r'^([A-Za-z]+)[_\s]+(\d{6}[CP]\d+)$')

def parse_expiration(path: Path) -> date: # type: ignore
    """
    Given a Path like
      Path("GOOGL_250725C00185000.db")  or
      Path("GOOGL 250725C00185000")
    returns the expiration date as a datetime.date.

    Raises ValueError if the filename doesn't match the expected pattern.
    """
    # 1) isolate just the stem (drops any .db or other extension)
    stem = path.stem

    # 2) apply regex
    m = PATTERN.match(stem)
    if not m:
        raise ValueError(f"Filename '{stem}' does not match SYMBOL[ _]YYMMDD… pattern")

    symbol, option_id = m.groups()

    # 3) slice off the YYMMDD portion and parse
    date_str = option_id[:6]           # e.g. "250725"
    return datetime.strptime(date_str, "%y%m%d").date()


def load_expirations(path: str) -> dict[str, Optional[datetime.date]]:
    with open(path, 'r') as f:
        raw = json.load(f)

    expirations: dict[str, Optional[datetime.date]] = {}
    for symbol, date_str in raw.items():
        if date_str is None:
            expirations[symbol] = None
        else:
            expirations[symbol] = datetime.strptime(date_str, "%Y-%m-%d").date()
    return expirations

def is_within_three_weeks(target: date | datetime,  # type: ignore
                          reference: date | datetime = None) -> bool: # type: ignore
    """
    Returns True if target is between reference (default: today) 
    and 3 weeks after reference (inclusive).
    """
    if reference is None:
        # use date if target is a date, else datetime.now()
        reference = date.today() if isinstance(target, date) else datetime.now()
    
    # normalize to date for pure-date comparisons
    if isinstance(target, datetime):
        target_dt = target
        ref_dt    = reference
    else:
        target_dt = datetime.combine(target, datetime.min.time())
        ref_dt    = datetime.combine(reference, datetime.min.time())
    
    three_weeks_later = ref_dt + timedelta(weeks=3)
    return ref_dt <= target_dt <= three_weeks_later

# Regex to capture an optional symbol prefix and the 6-digit date + rest
_PATTERN = re.compile(r'''
    ^                                  # start
    (?:(?P<symbol>[A-Za-z]+)_)?        # optional SYMBOL_ 
    (?P<option_id>\d{6}[CP]\d+)        # 6-digit YYMMDD + C/P + strike digits
    $                                  # end
''', re.VERBOSE)

def bump_one_week(input_id: str) -> str:
    """
    Return a new option ID with its expiration date
    moved one week (7 days) later.

    Examples:
      bump_one_week("250718P01290000")       → "250725P01290000"
      bump_one_week("NFLX_250718P01290000")  → "NFLX_250725P01290000"
    """
    m = _PATTERN.match(input_id)
    if not m:
        raise ValueError(f"Bad option ID format: {input_id!r}")

    symbol = m.group('symbol') or ""
    opt   = m.group('option_id')

    # parse old date and add 7 days
    old_date = datetime.strptime(opt[:6], "%y%m%d").date()
    new_date = old_date + timedelta(weeks=1)
    new_date_code = new_date.strftime("%y%m%d")

    # rebuild the ID
    new_opt = new_date_code + opt[6:]
    return f"{symbol + '_' if symbol else ''}{new_opt}"

def delete_db_file(path: Path) -> None:
    """
    Delete the file at `path`.  
    Raises FileNotFoundError if it doesn’t exist.
    """
    path.unlink()
    print(f"Deleted {path.name}")



if __name__ == '__main__':

    earnings = load_expirations("earnings_dates.json")
    conn = sqlite3.connect('Tracker.db')
    cursor = conn.cursor()

    for db_path in find_long_db_files('.', min_length=15):
        date = parse_expiration(db_path)
        symbol = db_path.stem.split("_", 1)[0]
        earn_date = earnings.get(symbol)

        if earn_date is not None and date < earn_date and is_within_three_weeks(target=earn_date, reference=date):
            print(f"{db_path.name}: expiration {date} is before earnings on {earn_date}")
            old_id = db_path.stem
            new_id = bump_one_week(old_id)

            # 4) delete old tracker row
            cursor.execute("DELETE FROM Tracker WHERE id = ?", (old_id,))

            # 5) insert or replace new ID
            cursor.execute(
                "INSERT OR REPLACE INTO Tracker (id) VALUES (?)",
                (new_id,)
            )

            conn.commit()

            # 6) delete the original .db file
            delete_db_file(db_path)

    conn.close()

