import pandas as pd
from typing import List
from analysis import infer_trades_per_manager
from db import get_prices_eod_for_ticker


def compute_exposure_vs_next_q_return(tickers: List[str]) -> pd.DataFrame:
    trades = infer_trades_per_manager()

    expo = (
        trades[trades["ticker"].isin([t.upper() for t in tickers])]
        .groupby(["ticker", "quarter"], as_index=False)["delta_qty_proxy"]
        .sum()
        .rename(columns={"delta_qty_proxy": "net_exposure_change"})
    )

    frames = []
    for t in tickers:
        t = t.upper()
        px_rows = get_prices_eod_for_ticker(t)
        if not px_rows:
            continue

        px = pd.DataFrame(px_rows, columns=["quarter", "close_q"])
        px["ticker"] = t
        px = px.sort_values(["ticker", "quarter"]).reset_index(drop=True)

        px["close_next_q"] = px["close_q"].shift(-1)
        px["price_return_next_q"] = (px["close_next_q"] / px["close_q"]) - 1

        m = expo[expo["ticker"] == t].merge(px, on=["ticker", "quarter"], how="inner")
        m = m.dropna(subset=["close_next_q"]).reset_index(drop=True)
        frames.append(m)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
