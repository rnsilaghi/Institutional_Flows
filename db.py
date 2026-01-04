import sqlite3
import os

# Absolute path to the directory this file lives in (Game Theory folder)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Database lives directly in Game Theory folder
DB_PATH = os.path.join(BASE_DIR, "my_db.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS holdings (
        manager TEXT,
        quarter TEXT,
        ticker TEXT,
        shares INTEGER,
        market_value REAL
    )
    """)

    conn.commit()
    conn.close()


def insert_holdings(rows):
    if not rows:
        return

    conn = get_connection()
    cur = conn.cursor()

    cur.executemany("""
        INSERT INTO holdings
        (manager, quarter, ticker, shares, market_value)
        VALUES (?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()
