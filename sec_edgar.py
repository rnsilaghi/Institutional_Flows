import requests
from typing import List, Dict
from api import SEC_API_KEY, SEC_BASE_URL


def get_13f_filings_for_ticker(ticker: str, size: int = 200) -> List[Dict]:
    if not SEC_API_KEY or SEC_API_KEY.startswith("YOUR_"):
        raise ValueError("SEC_API_KEY missing or invalid")

    url = f"{SEC_BASE_URL}?token={SEC_API_KEY}"

    payload = {
        "query": f'formType:"13F-HR" AND holdings.ticker:{ticker}',
        "from": "0",
        "size": str(size),
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
    return data.get("filings", [])


def extract_holdings(filings: List[Dict], ticker: str):
    rows = []

    for f in filings:
        manager = f.get("companyName")
        quarter = f.get("periodOfReport")

        for h in f.get("holdings", []):
            if h.get("ticker") == ticker:
                rows.append((
                    manager,
                    quarter,
                    ticker,
                    h.get("shares"),
                    h.get("value")  # USD thousands
                ))

    return rows
