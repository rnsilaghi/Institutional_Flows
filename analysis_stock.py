import sqlite3
import pandas as pd
from typing import List
from db import DB_PATH, get_prices_eod_for_ticker


def _quarter_end(dt: pd.Timestamp) -> pd.Timestamp:
    return dt.to_period("Q").end_time.normalize()


def _build_quarter_closes(px_daily: pd.DataFrame) -> pd.DataFrame:
    """
    px_daily: columns = ["date", "close"]
    Returns: ["quarter", "close_q"] where quarter is calendar quarter-end (YYYY-MM-DD)
    close_q = last available trading close on or before quarter-end.
    """
    px = px_daily.copy()
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values("date")
    px["quarter"] = px["date"].dt.to_period("Q").dt.end_time.dt.normalize()
    # Last trading day in each quarter is the quarter close
    q = px.groupby("quarter", as_index=False).last()[["quarter", "close"]]
    q = q.rename(columns={"close": "close_q"})
    q["quarter"] = q["quarter"].dt.strftime("%Y-%m-%d")
    return q


def _get_net_exposure_all_quarters(tickers: List[str]) -> pd.DataFrame:
    """
    We compute net exposure change per ticker+quarter across ALL quarters in holdings,
    then later we filter to quarters that have prices.
    """
    tickers = [t.upper() for t in tickers]
    conn = sqlite3.connect(DB_PATH)

    placeholders = ",".join(["?"] * len(tickers))

    query = f"""
    WITH base AS (
        SELECT
            manager,
            ticker,
            quarter,
            CAST(value_k AS REAL) AS qty_proxy
        FROM holdings
        WHERE ticker IN ({placeholders})
          AND value_k IS NOT NULL
    ),
    ordered AS (
        SELECT
            manager,
            ticker,
            quarter,
            qty_proxy,
            LAG(qty_proxy) OVER (
                PARTITION BY manager, ticker
                ORDER BY date(quarter)
            ) AS prev_qty_proxy
        FROM base
    ),
    deltas AS (
        SELECT
            ticker,
            quarter,
            (qty_proxy - prev_qty_proxy) AS delta_qty_proxy
        FROM ordered
        WHERE prev_qty_proxy IS NOT NULL
    )
    SELECT
        ticker,
        quarter,
        SUM(delta_qty_proxy) AS net_exposure_change
    FROM deltas
    GROUP BY ticker, quarter
    ORDER BY ticker, date(quarter)
    """

    expo = pd.read_sql(query, conn, params=tickers)
    conn.close()
    return expo


def compute_exposure_vs_next_q_return(tickers: List[str]) -> pd.DataFrame:
    """
    Returns a DataFrame with ticker, quarter, net_exposure_change, close_q, close_next_q, and price_return_next_q
    Only for quarters where we have price data for both this quarter and next quarter.
    """
    expo = _get_net_exposure_all_quarters(tickers)
    if expo.empty:
        return pd.DataFrame()

    frames = []
    for t in [x.upper() for x in tickers]:
        px_rows = get_prices_eod_for_ticker(t)
        if not px_rows:
            continue

        px_daily = pd.DataFrame(px_rows, columns=["date", "close"])
        if px_daily.empty:
            continue

        qpx = _build_quarter_closes(px_daily)
        qpx["ticker"] = t

        # next-quarter return
        qpx = qpx.sort_values("quarter").reset_index(drop=True)
        qpx["close_next_q"] = qpx["close_q"].shift(-1)
        qpx["price_return_next_q"] = (qpx["close_next_q"] / qpx["close_q"]) - 1

        m = expo[expo["ticker"] == t].merge(
            qpx[["ticker", "quarter", "close_q", "close_next_q", "price_return_next_q"]],
            on=["ticker", "quarter"],
            how="inner"
        )
        
        m = m.dropna(subset=["close_next_q"]).reset_index(drop=True)
        frames.append(m)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()