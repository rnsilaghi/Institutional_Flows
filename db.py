import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "my_db.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_db():
    conn = get_connection()
    cur = conn.cursor()

    # Holdings Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS holdings (
        accession_no TEXT NOT NULL,
        manager      TEXT NOT NULL,
        quarter      TEXT NOT NULL,
        ticker       TEXT NOT NULL,
        value_k      REAL NOT NULL,
        filed_date   TEXT,          -- YYYY-MM-DD (clean)
        PRIMARY KEY (accession_no, manager, ticker)
    )
    """)

    # Backfill Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingest_checkpoint (
        ticker TEXT PRIMARY KEY,
        last_filed_at TEXT
    )
    """)

    # Price Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prices_eod (
        ticker TEXT NOT NULL,
        date   TEXT NOT NULL,   -- YYYY-MM-DD
        close  REAL NOT NULL,
        PRIMARY KEY (ticker, date)
    )
    """)

    conn.commit()
    conn.close()


def insert_holdings(rows):
    """
    rows: (accession_no, manager, quarter, ticker, value_k, filed_date)
    """
    if not rows:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    before = conn.total_changes

    cur.executemany("""
        INSERT OR IGNORE INTO holdings
        (accession_no, manager, quarter, ticker, value_k, filed_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    inserted = conn.total_changes - before
    conn.close()
    return inserted


def get_backfill_checkpoint(ticker: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT last_filed_at FROM ingest_checkpoint WHERE ticker = ?", (ticker.upper(),))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_backfill_checkpoint(ticker: str, last_filed_at: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ingest_checkpoint (ticker, last_filed_at)
        VALUES (?, ?)
        ON CONFLICT(ticker) DO UPDATE SET last_filed_at = excluded.last_filed_at
    """, (ticker.upper(), last_filed_at))
    conn.commit()
    conn.close()


def upsert_prices_eod(rows):
    """
    rows: (ticker, date, close)
    """
    if not rows:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    before = conn.total_changes

    cur.executemany("""
        INSERT INTO prices_eod (ticker, date, close)
        VALUES (?, ?, ?)
        ON CONFLICT(ticker, date) DO UPDATE SET
            close = excluded.close
    """, rows)

    conn.commit()
    changed = conn.total_changes - before
    conn.close()
    return changed


def get_prices_eod_for_ticker(ticker: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT date, close
        FROM prices_eod
        WHERE ticker = ?
        ORDER BY date ASC
    """, (ticker.upper(),))
    rows = cur.fetchall()
    conn.close()
    return rows
