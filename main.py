from db import create_db
from analysis import infer_trades_per_manager

TICKERS = ["ORCL", "UNH", "FDS"]


def write_pretty_txt_by_ticker(df, ticker: str):
    out_path = f"{ticker}_trades.txt"
    sub = df[df["ticker"] == ticker].copy()
    sub = sub.sort_values(["manager", "quarter"])

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== " + ticker + " (per-manager position changes; qty_source=shares or value_k) ===\n\n")

        for manager, g in sub.groupby("manager", sort=True):
            f.write(f"MANAGER: {manager}\n")
            f.write("-" * (9 + len(manager)) + "\n")
            f.write("quarter | filed_date | action | value_k | prev_qty | qty | delta | qty_source\n")

            for _, row in g.iterrows():
                line = (
                    f"{row['quarter']} | "
                    f"{row['filed_date']} | "
                    f"{row['action']} | "
                    f"{row['value_k']:.0f} | "
                    f"{row['prev_qty_proxy']:.0f} | "
                    f"{row['qty_proxy']:.0f} | "
                    f"{row['delta_qty_proxy']:.0f} | "
                    f"{row['qty_source']}\n"
                )
                f.write(line)

            f.write("\n")

    print(f"Saved {out_path}")


if __name__ == "__main__":
    create_db()

    # Analysis-only: uses existing DB holdings (no SEC calls)
    trades = infer_trades_per_manager()

    for ticker in TICKERS:
        write_pretty_txt_by_ticker(trades, ticker)
