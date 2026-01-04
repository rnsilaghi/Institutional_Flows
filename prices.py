import requests
from typing import List, Dict, Optional
from api import STOCKDATA_API_KEY, STOCKDATA_BASE_URL


def fetch_eod_quarterly(
    ticker: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sort: str = "asc",
) -> List[Dict]:
    if not STOCKDATA_API_KEY or STOCKDATA_API_KEY.startswith("YOUR_"):
        raise ValueError("STOCKDATA_API_KEY missing or invalid")

    url = f"{STOCKDATA_BASE_URL}/data/eod"
    params = {
        "api_token": STOCKDATA_API_KEY,
        "symbols": ticker.upper(),
        "interval": "quarter",
        "sort": sort,
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])
