from datetime import date, timedelta
from prices import fetch_eod_quarterly
from db import create_db, upsert_prices_eod

TICKERS = ["ORCL", "UNH", "FDS"]


def today_str():
    return date.today().isoformat()


def start_5y_str():
    return (date.today() - timedelta(days=5 * 365)).isoformat()


if __name__ == "__main__":
    create_db()

    date_from = start_5y_str()
    date_to = today_str()

    for t in TICKERS:
        data = fetch_eod_quarterly(t, date_from=date_from, date_to=date_to, sort="asc")

        rows = []
        for bar in data:
            d = str(bar.get("date", ""))[:10]
            c = bar.get("close", None)
            if d and c is not None:
                rows.append((t.upper(), d, float(c)))

        changed = upsert_prices_eod(rows)
        print(f"{t}: quarters fetched={len(data)} stored/updated={changed}")
