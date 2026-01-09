import os
import pandas as pd

from db import create_db, insert_holdings, get_backfill_checkpoint, set_backfill_checkpoint
from sec_edgar import get_13f_filings_for_ticker_backfill, extract_holdings
from analysis import infer_trades_per_manager
from analysis_stock import compute_exposure_vs_next_q_return
from stats_tests import run_stats
from update_prices import run_update_prices
from plots import save_all_plots

TICKERS = ["ORCL", "UNH", "FDS"]

# knobs (edit here, not in main.py)
RUN_SEC_INGEST = True
YEARS_13F_WINDOW = 5
PRICE_QUARTERS_PER_TICKER = 28


def project_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


BASE_DIR = project_dir()


def write_trades_txt_by_ticker(df: pd.DataFrame, ticker: str):
    out_path = os.path.join(BASE_DIR, f"{ticker}_trades.txt")

    sub = df[df["ticker"] == ticker].copy()
    sub = sub.sort_values(["manager", "quarter"])

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== " + ticker + " (per-manager position changes; qty_proxy=value_k) ===\n\n")

        for manager, g in sub.groupby("manager", sort=True):
            f.write(f"MANAGER: {manager}\n")
            f.write("-" * (9 + len(manager)) + "\n")
            f.write("quarter | filed_date | action | prev_qty | qty | delta\n")

            for _, row in g.iterrows():
                f.write(
                    f"{row['quarter']} | "
                    f"{row['filed_date']} | "
                    f"{row['action']} | "
                    f"{row['prev_qty_proxy']:.0f} | "
                    f"{row['qty_proxy']:.0f} | "
                    f"{row['delta_qty_proxy']:.0f}\n"
                )
            f.write("\n")

    print(f"Saved {out_path}")


def write_exposure_summary_txt(df: pd.DataFrame, filename="exposure_vs_next_q_return.txt"):
    path = os.path.join(BASE_DIR, filename)

    with open(path, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("No matched rows. Likely: missing next-quarter close in prices.\n")
            return

        for ticker, g in df.groupby("ticker", sort=True):
            f.write(f"=== {ticker} — Net 13F Exposure vs Next-Quarter Return ===\n\n")
            f.write("quarter | net_exposure | next_q_return | signal\n")
            f.write("-" * 56 + "\n")

            for _, row in g.iterrows():
                net_exp = float(row["net_exposure_change"])
                ret = float(row["price_return_next_q"])

                if net_exp == 0 or ret == 0:
                    signal = "NEUTRAL"
                elif (net_exp > 0 and ret > 0) or (net_exp < 0 and ret < 0):
                    signal = "MATCH"
                else:
                    signal = "MISMATCH"

                f.write(
                    f"{row['quarter']} | "
                    f"{net_exp:>12,.0f} | "
                    f"{ret:>8.2%} | "
                    f"{signal}\n"
                )
            f.write("\n\n")

    print(f"Saved {path}")


def write_stats_txt(stats: dict, filename="stats_summary.txt"):
    path = os.path.join(BASE_DIR, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write("=== Statistical Tests: Net Exposure vs Next-Quarter Returns ===\n\n")

        f.write("Pearson correlation:\n")
        f.write(f"  r = {stats['pearson']['r']:.3f}\n")
        f.write(f"  p-value = {stats['pearson']['p_value']:.4f}\n\n")

        f.write("Spearman rank correlation:\n")
        f.write(f"  rho = {stats['spearman']['r']:.3f}\n")
        f.write(f"  p-value = {stats['spearman']['p_value']:.4f}\n\n")

        f.write("Regression: return = alpha + beta * exposure\n")
        f.write(f"  beta = {stats['regression']['beta']:.10f}\n")
        f.write(f"  t-stat = {stats['regression']['t_stat']:.2f}\n")
        f.write(f"  p-value = {stats['regression']['p_value']:.4f}\n")
        f.write(f"  R^2 = {stats['regression']['r_squared']:.4f}\n")
        f.write(f"  N = {stats['regression']['n_obs']}\n\n")

        f.write("Directional accuracy:\n")
        f.write(f"  hit rate = {stats['directional']['hit_rate']:.2%}\n")
        f.write(f"  hits = {stats['directional']['hits']} / {stats['directional']['n_obs']}\n")
        f.write(f"  binomial p-value = {float(stats['directional']['p_value']):.4f}\n")

    print(f"Saved {path}")


def sec_ingest():
    if not RUN_SEC_INGEST:
        print("SEC ingest skipped (RUN_SEC_INGEST=False).")
        return

    for ticker in TICKERS:
        checkpoint = get_backfill_checkpoint(ticker)

        filings = get_13f_filings_for_ticker_backfill(
            ticker=ticker,
            limit=200,
            end_checkpoint_filed_at=checkpoint,
            years=YEARS_13F_WINDOW
        )

        rows = extract_holdings(filings, ticker)
        inserted = insert_holdings(rows)

        filed_ats = [f.get("filedAt") for f in filings if f.get("filedAt")]
        if filed_ats:
            oldest = min(filed_ats)
            set_backfill_checkpoint(ticker, oldest)

        print(f"{ticker}: filings={len(filings)} rows={len(rows)} inserted={inserted} checkpoint={checkpoint}")


def run_pipeline():
    create_db()

    # 1) SEC -> holdings
    sec_ingest()

    # 2) prices
    run_update_prices(tickers=TICKERS, max_quarters=PRICE_QUARTERS_PER_TICKER)

    # 3) per-manager summaries
    trades = infer_trades_per_manager()
    for t in TICKERS:
        write_trades_txt_by_ticker(trades, t)

    # 4) factor dataset
    df = compute_exposure_vs_next_q_return(TICKERS)
    print("Merged rows:", len(df))

    csv_path = os.path.join(BASE_DIR, "exposure_vs_next_q_return.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path}")

    write_exposure_summary_txt(df)

    # 5) stats
    stats = run_stats(df)
    write_stats_txt(stats)

    # 6) plots
    save_all_plots(df, BASE_DIR)
