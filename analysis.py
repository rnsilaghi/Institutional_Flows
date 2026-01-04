import sqlite3
import pandas as pd
from db import DB_PATH

N_DELTAS = 12


def infer_trades_per_manager(n_deltas: int = N_DELTAS):
    """
    - Cap to most recent (n_deltas + 1) quarters per manager+ticker
    - Drop first (baseline) by requiring prev_qty_proxy IS NOT NULL
    """
    keep_obs = n_deltas + 1
    conn = sqlite3.connect(DB_PATH)

    query = f"""
    WITH base AS (
        SELECT
            manager,
            ticker,
            quarter,
            SUBSTR(filed_at, 1, 10) AS filed_date,
            shares,
            value_k,
            CASE
                WHEN shares IS NOT NULL THEN CAST(shares AS REAL)
                ELSE CAST(value_k AS REAL)
            END AS qty_proxy,
            CASE
                WHEN shares IS NOT NULL THEN 'shares'
                ELSE 'value_k'
            END AS qty_source
        FROM holdings
        WHERE value_k IS NOT NULL
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY manager, ticker
                ORDER BY date(quarter) DESC
            ) AS rn
        FROM base
    ),
    capped AS (
        SELECT * FROM ranked WHERE rn <= {keep_obs}
    ),
    ordered AS (
        SELECT
            *,
            LAG(qty_proxy) OVER (
                PARTITION BY manager, ticker
                ORDER BY date(quarter)
            ) AS prev_qty_proxy
        FROM capped
    )
    SELECT
        manager,
        ticker,
        quarter,
        filed_date,
        value_k,
        prev_qty_proxy,
        qty_proxy,
        (qty_proxy - prev_qty_proxy) AS delta_qty_proxy,
        CASE
            WHEN (qty_proxy - prev_qty_proxy) > 0 THEN 'BUY'
            WHEN (qty_proxy - prev_qty_proxy) < 0 THEN 'SELL'
            ELSE 'HOLD'
        END AS action,
        qty_source
    FROM ordered
    WHERE prev_qty_proxy IS NOT NULL
    ORDER BY ticker, manager, date(quarter)
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df
