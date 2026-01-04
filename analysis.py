import sqlite3
import pandas as pd
from db import DB_PATH



def infer_trades():
    conn = sqlite3.connect(DB_PATH)

    query = """
    WITH ordered AS (
        SELECT
            manager,
            ticker,
            quarter,
            shares,
            LAG(shares) OVER (
                PARTITION BY manager, ticker
                ORDER BY quarter
            ) AS prev_shares
        FROM holdings
    )
    SELECT
        manager,
        ticker,
        quarter,
        prev_shares,
        shares,
        shares - prev_shares AS delta_shares,
        CASE
            WHEN prev_shares IS NULL AND shares > 0 THEN 'ENTRY'
            WHEN prev_shares > 0 AND shares = 0 THEN 'EXIT'
            WHEN shares - prev_shares > 0 THEN 'BUY'
            WHEN shares - prev_shares < 0 THEN 'SELL'
            ELSE 'HOLD'
        END AS action
    FROM ordered
    WHERE prev_shares IS NOT NULL
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df
