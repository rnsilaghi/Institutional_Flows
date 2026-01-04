from sec_edgar import get_13f_filings_for_ticker, extract_holdings
from db import create_db, insert_holdings
from analysis import infer_trades

TICKERS = ["ORCL", "UNH", "FDS"]

if __name__ == "__main__":
    create_db()

    for ticker in TICKERS:
        filings = get_13f_filings_for_ticker(ticker)
        rows = extract_holdings(filings, ticker)
        insert_holdings(rows)

    trades = infer_trades()

    trades.to_csv("inferred_13f_trades.txt", sep="|", index=False)
    print("Saved inferred trades to inferred_13f_trades.txt")

