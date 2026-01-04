import requests
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from api import SEC_API_KEY, SEC_BASE_URL


def _safe_int(x):
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_filed_at(x: Optional[str]) -> Optional[datetime]:
    if not x:
        return None
    try:
        return datetime.fromisoformat(x)
    except Exception:
        try:
            return datetime.strptime(x[:10], "%Y-%m-%d")
        except Exception:
            return None


def get_13f_filings_for_ticker_backfill(
    ticker: str,
    limit: int = 200,
    end_checkpoint_filed_at: Optional[str] = None,
    years: int = 5,
) -> List[Dict]:
    """
    BACKFILL MODE:
    Pull filings OLDER than end_checkpoint_filed_at, but within last `years` years.

    Query: filedAt:[start_date TO end_date] (inclusive),
    then strict filter: filedAt < checkpoint (so we don't re-pull boundary).
    """
    if not SEC_API_KEY or SEC_API_KEY.startswith("YOUR_"):
        raise ValueError("SEC_API_KEY missing or invalid")

    url = f"{SEC_BASE_URL}?token={SEC_API_KEY}"

    today = datetime.utcnow().date()
    start_date = today - timedelta(days=years * 365)
    start_str = start_date.strftime("%Y-%m-%d")

    if end_checkpoint_filed_at:
        end_str = end_checkpoint_filed_at[:10]
        checkpoint_dt = _parse_filed_at(end_checkpoint_filed_at)
    else:
        end_str = today.strftime("%Y-%m-%d")
        checkpoint_dt = None

    payload = {
        "query": (
            f'formType:"13F-HR" '
            f'AND filedAt:[{start_str} TO {end_str}] '
            f'AND holdings.ticker:{ticker}'
        ),
        "from": "0",
        "size": str(limit),
        "sort": [{"filedAt": {"order": "desc"}}]
    }

    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    filings = r.json().get("filings", [])

    if checkpoint_dt:
        filtered = []
        for f in filings:
            f_dt = _parse_filed_at(f.get("filedAt"))
            if f_dt and f_dt < checkpoint_dt:
                filtered.append(f)
        filings = filtered

    return filings


def extract_holdings(filings: List[Dict], ticker: str) -> List[Tuple]:
    """
    (accession_no, manager, quarter, ticker, shares, value_k, filed_at)
    """
    rows = []
    for f in filings:
        manager = f.get("companyName")
        quarter = f.get("periodOfReport")
        filed_at = f.get("filedAt")

        accession_no = f.get("accessionNo") or f.get("accessionNumber") or f.get("id") or f.get("linkToHtml")
        if not accession_no or not manager or not quarter:
            continue

        holdings = f.get("holdings") or []
        if not holdings:
            continue

        for h in holdings:
            if (h.get("ticker") or "").upper() != ticker.upper():
                continue

            shares = _safe_int(h.get("shares") or h.get("sshPrnamt"))
            value_k = _safe_float(h.get("value") or h.get("marketValue") or h.get("valueK"))
            if value_k is None:
                continue

            rows.append((
                str(accession_no),
                str(manager),
                str(quarter),
                ticker.upper(),
                shares,
                value_k,
                filed_at
            ))

    return rows
