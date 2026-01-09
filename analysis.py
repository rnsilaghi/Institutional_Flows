import sqlite3
import pandas as pd
from db import DB_PATH

N_DELTAS = 12


def infer_trades_per_manager(n_deltas: int = N_DELTAS):
    keep_obs = n_deltas + 1 #because we need to discard the first quarter
    conn = sqlite3.connect(DB_PATH)

    query = f"""
    WITH ranked AS (
        SELECT
            manager,
            ticker,
            quarter,
            filed_date,
            value_k AS qty_proxy,
            ROW_NUMBER() OVER (
                PARTITION BY manager, ticker
                ORDER BY date(quarter) DESC
            ) AS rn
        FROM holdings
        WHERE value_k IS NOT NULL
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
        prev_qty_proxy,
        qty_proxy,
        (qty_proxy - prev_qty_proxy) AS delta_qty_proxy,
        CASE
            WHEN (qty_proxy - prev_qty_proxy) > 0 THEN 'BUY'
            WHEN (qty_proxy - prev_qty_proxy) < 0 THEN 'SELL'
            ELSE 'HOLD'
        END AS action
    FROM ordered
    WHERE prev_qty_proxy IS NOT NULL
    ORDER BY ticker, manager, date(quarter)
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df