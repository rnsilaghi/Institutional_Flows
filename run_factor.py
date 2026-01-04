from db import create_db
from analysis_stock import compute_exposure_vs_next_q_return

TICKERS = ["ORCL", "UNH", "FDS"]

if __name__ == "__main__":
    create_db()
    df = compute_exposure_vs_next_q_return(TICKERS)

    print(df.head(25))
    df.to_csv("exposure_vs_next_q_return.csv", index=False)
    print("Saved exposure_vs_next_q_return.csv")
