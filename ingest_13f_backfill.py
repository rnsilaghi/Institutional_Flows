from sec_edgar import get_13f_filings_for_ticker_backfill, extract_holdings
from db import create_db, insert_holdings, get_backfill_checkpoint, set_backfill_checkpoint

TICKERS = ["ORCL", "UNH", "FDS"]


if __name__ == "__main__":
    create_db()

    for ticker in TICKERS:
        checkpoint = get_backfill_checkpoint(ticker)

        filings = get_13f_filings_for_ticker_backfill(
            ticker=ticker,
            limit=200,
            end_checkpoint_filed_at=checkpoint,
            years=5
        )

        rows = extract_holdings(filings, ticker)
        inserted = insert_holdings(rows)

        filed_ats = [f.get("filedAt") for f in filings if f.get("filedAt")]
        if filed_ats:
            oldest = min(filed_ats)
            set_backfill_checkpoint(ticker, oldest)

        print(
            f"{ticker}: filings fetched={len(filings)} rows extracted={len(rows)} inserted={inserted} "
            f"checkpoint(oldest_so_far)={checkpoint}"
        )
