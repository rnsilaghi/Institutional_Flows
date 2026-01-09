import numpy as np
import pandas as pd
from scipy import stats
import statsmodels.api as sm


def run_stats(df: pd.DataFrame):
    """
    df must contain:
      - net_exposure_change
      - price_return_next_q
    """
    x = df["net_exposure_change"].astype(float).values
    y = df["price_return_next_q"].astype(float).values

    results = {}

    # Correlation (Pearson)
    pearson_r, pearson_p = stats.pearsonr(x, y)
    results["pearson"] = {
        "r": pearson_r,
        "p_value": pearson_p
    }

    # Correlation (Spearman) 
    spearman_r, spearman_p = stats.spearmanr(x, y)
    results["spearman"] = {
        "r": spearman_r,
        "p_value": spearman_p
    }

    # Regression 
    X = sm.add_constant(x)
    model = sm.OLS(y, X).fit()
    results["regression"] = {
        "beta": model.params[1],
        "t_stat": model.tvalues[1],
        "p_value": model.pvalues[1],
        "r_squared": model.rsquared,
        "n_obs": int(model.nobs)
    }

    # Directional accuracy
    signs_match = np.sign(x) == np.sign(y)
    hits = np.sum(signs_match)
    n = len(signs_match)

    # Binomial test vs 50%
    p_binom = stats.binomtest(hits, n, 0.5, alternative="greater").pvalue

    results["directional"] = {
    "hit_rate": hits / n if n > 0 else float("nan"),
    "hits": hits,
    "n_obs": n,
    "p_value": float(p_binom)
}


    return results