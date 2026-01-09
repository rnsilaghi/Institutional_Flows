import os
import pandas as pd
import matplotlib.pyplot as plt


def save_all_plots(df: pd.DataFrame, out_dir: str):
    if df.empty:
        print("plots: df empty, skipping.")
        return

    # 1) Scatter
    plt.figure()
    plt.scatter(df["net_exposure_change"], df["price_return_next_q"])
    plt.xlabel("Net Exposure Change")
    plt.ylabel("Next-Quarter Return")
    plt.title("Net 13F Exposure vs Next-Quarter Return")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "scatter_exposure_vs_next_q_return.png"), dpi=150)
    plt.close()

    # 2) Net exposure time series
    expo_ts = (
        df.groupby("quarter", as_index=False)["net_exposure_change"]
          .sum()
          .sort_values("quarter")
    )
    plt.figure()
    plt.plot(expo_ts["quarter"], expo_ts["net_exposure_change"])
    plt.xlabel("Quarter")
    plt.ylabel("Net Exposure Change")
    plt.title("Net 13F Exposure Over Time")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "timeseries_net_exposure.png"), dpi=150)
    plt.close()

    # 3) Z-scored overlay
    expo_ts["exposure_z"] = (
        (expo_ts["net_exposure_change"] - expo_ts["net_exposure_change"].mean())
        / expo_ts["net_exposure_change"].std()
    )

    rets_ts = (
        df.groupby("quarter", as_index=False)["price_return_next_q"]
          .mean()
          .sort_values("quarter")
    )

    plt.figure()
    plt.plot(expo_ts["quarter"], expo_ts["exposure_z"], label="Exposure (z-score)")
    plt.plot(rets_ts["quarter"], rets_ts["price_return_next_q"], label="Next-Quarter Return")
    plt.xlabel("Quarter")
    plt.title("Z-scored Exposure vs Next-Quarter Return")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "overlay_exposure_z_vs_next_q_return.png"), dpi=150)
    plt.close()

    print("Saved plots (.png)")