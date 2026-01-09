import requests
from typing import List, Optional, Tuple

from db import (
    create_db,
    upsert_prices_eod,
    get_connection,
)
from api import STOCKDATA_API_KEY, STOCKDATA_BASE_URL

TICKERS = ["ORCL", "UNH", "FDS"]
MAX_QUARTERS = 28


def get_recent_quarters_for_ticker(ticker: str, limit: int) -> List[str]:
    """
    Pull distinct quarter-end dates from holdings for a ticker.
    Returns most recent `limit` dates as YYYY-MM-DD strings.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT quarter
        FROM holdings
        WHERE ticker = ?
        ORDER BY date(quarter) DESC
        LIMIT ?
    """, (ticker.upper(), limit))

    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def fetch_close_on_date(ticker: str, date_str: str) -> Optional[Tuple[str, float]]:
    """
    Fetch EOD close for ticker on a specific date.
    API returns nearest previous trading day if market was closed.
    Returns (price_date, close) or None.
    """
    url = f"{STOCKDATA_BASE_URL}/data/eod"
    params = {
        "symbols": ticker.upper(),
        "date": date_str,
        "api_token": STOCKDATA_API_KEY,
    }

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()

    data = r.json().get("data", [])
    if not data:
        return None

    bar = data[0]
    return str(bar.get("date"))[:10], float(bar.get("close"))


def run_update_prices(
    tickers: List[str] = None,
    max_quarters: int = MAX_QUARTERS
) -> int:
    """
    Populates prices_eod for the most recent max_quarters quarter dates per ticker.
    """
    create_db()
    tickers = tickers or TICKERS

    total_calls = 0

    for ticker in tickers:
        quarters = get_recent_quarters_for_ticker(ticker, max_quarters)
        print(f"{ticker}: fetching prices for {len(quarters)} quarter dates")

        rows = []
        for q in quarters:
            result = fetch_close_on_date(ticker, q)
            total_calls += 1
            if result is None:
                continue

            price_date, close = result
            rows.append((ticker.upper(), price_date, float(close)))

        changed = upsert_prices_eod(rows)
        print(f"{ticker}: stored/updated {changed} prices")

    print(f"TOTAL API CALLS USED: {total_calls}")
    return total_calls


if __name__ == "__main__":
    run_update_prices()