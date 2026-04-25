#!/usr/bin/env python3
"""
Congressional trade data provider — CapitolTrades.com.

Fetches disclosed equity trades for U.S. House and Senate members from
CapitolTrades' public BFF (backend-for-frontend) JSON API:

  https://bff.capitoltrades.com/trades

CapitolTrades aggregates official STOCK Act PTR filings from both chambers.
Disclosures typically lag actual trades by ~24-48 hours plus any reporting
delay by the member.

Normalized trade dict shape:
{
  'chamber':         'House' | 'Senate',
  'member':          'Nancy Pelosi',
  'party':           'D' | 'R' | 'I' | None,
  'state':           str | None,
  'district':        str | None,                     # not on this feed
  'ticker':          str,
  'asset':           str,                            # company / asset name
  'transaction':     'purchase' | 'sale' | 'exchange' | 'other',
  'raw_type':        original feed string,
  'amount_range':    '$1,001 - $15,000',
  'amount_min':      float,
  'amount_max':      float,
  'transaction_date': 'YYYY-MM-DD',
  'disclosure_date':  'YYYY-MM-DD',
  'days_to_disclose': int | None,
  'ptr_url':         str | None,
  'is_priority':     bool,                           # Pelosi (and configurable high-signal names)
}
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

CAPITOL_TRADES_URL = "https://bff.capitoltrades.com/trades"

# CapitolTrades' BFF rejects requests without a browser-ish User-Agent.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

# Names we always flag as priority. Lowercased substring match against member name.
PRIORITY_MEMBERS = {
    "pelosi",
}

# Feed "txType" strings → normalized transaction bucket
_TX_MAP = {
    "buy":            "purchase",
    "purchase":       "purchase",
    "receive":        "purchase",
    "sell":           "sale",
    "sale":           "sale",
    "sell (full)":    "sale",
    "sell (partial)": "sale",
    "sale_full":      "sale",
    "sale_partial":   "sale",
    "exchange":       "exchange",
}

# Range patterns we may see in size/sizeRange:
#   "$1,001 - $15,000"
#   "1K–15K"   (em-dash)
#   "15K-50K"
#   "1M-5M"
_AMOUNT_TOKEN_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([kKmMbB]?)")


def _parse_amount(amount: Optional[str]) -> tuple[float, float]:
    """Parse a size range like '$1,001 - $15,000' or '15K–50K' into (min, max)."""
    if not amount:
        return 0.0, 0.0
    cleaned = amount.replace(",", "").replace("$", "")
    tokens = _AMOUNT_TOKEN_RE.findall(cleaned)
    if not tokens:
        return 0.0, 0.0

    def to_dollars(num: str, suffix: str) -> float:
        try:
            n = float(num)
        except ValueError:
            return 0.0
        s = suffix.lower()
        if s == "k":
            n *= 1_000
        elif s == "m":
            n *= 1_000_000
        elif s == "b":
            n *= 1_000_000_000
        return n

    if len(tokens) >= 2:
        return to_dollars(*tokens[0]), to_dollars(*tokens[1])
    v = to_dollars(*tokens[0])
    return v, v


def _parse_date(value: Optional[str]) -> Optional[str]:
    """Normalize various date formats to ISO 'YYYY-MM-DD'."""
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    # CapitolTrades returns ISO 'YYYY-MM-DD' but tolerate variants for safety.
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value[:len(fmt)] if "T" in value and "T" in fmt else value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Last resort: take the first 10 chars and try ISO again
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _normalize_type(value: Optional[str]) -> tuple[str, str]:
    raw = (value or "").strip()
    return _TX_MAP.get(raw.lower(), "other"), raw


def _normalize_party(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    p = value.lower()
    if "democrat" in p:
        return "D"
    if "republican" in p:
        return "R"
    if "independent" in p:
        return "I"
    return None


def _is_priority(member_name: str) -> bool:
    name = (member_name or "").lower()
    return any(p in name for p in PRIORITY_MEMBERS)


def _days_between(start_iso: Optional[str], end_iso: Optional[str]) -> Optional[int]:
    if not start_iso or not end_iso:
        return None
    try:
        a = datetime.strptime(start_iso, "%Y-%m-%d")
        b = datetime.strptime(end_iso, "%Y-%m-%d")
        return (b - a).days
    except ValueError:
        return None


def _normalize_trade(row: dict) -> dict:
    politician = row.get("politician") or {}
    asset      = row.get("asset") or {}

    chamber_raw = (politician.get("chamber") or "").lower()
    chamber     = "Senate" if chamber_raw == "senate" else "House"

    full_name = (politician.get("fullName") or
                 f"{politician.get('firstName') or ''} {politician.get('lastName') or ''}".strip())

    transaction, raw_type = _normalize_type(row.get("txType") or row.get("txTypeExtended"))

    tx_date  = _parse_date(row.get("txDate"))
    dis_date = _parse_date(row.get("filingDate") or row.get("pubDate"))

    size_str   = row.get("sizeRange") or row.get("size") or ""
    lo, hi     = _parse_amount(size_str)
    ticker     = ((asset.get("assetTicker") or "").strip().upper()) or "—"

    return {
        "chamber":          chamber,
        "member":           full_name,
        "party":            _normalize_party(politician.get("party")),
        "state":            politician.get("stateId") or politician.get("state"),
        "district":         None,
        "ticker":           ticker,
        "asset":            (asset.get("assetName") or "").strip(),
        "transaction":      transaction,
        "raw_type":         raw_type,
        "amount_range":     size_str,
        "amount_min":       lo,
        "amount_max":       hi,
        "transaction_date": tx_date,
        "disclosure_date":  dis_date,
        "days_to_disclose": _days_between(tx_date, dis_date),
        "ptr_url":          None,
        "is_priority":      _is_priority(full_name),
    }


def _fetch_pages(max_pages: int = 4, page_size: int = 96) -> list[dict]:
    """
    Pull recent disclosures from CapitolTrades, sorted newest first.

    96 × 4 = ~384 trades, which comfortably covers >7 days at typical volumes.
    Server-side filters are non-trivial; we sort by -pubDate and slice locally.
    """
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    trades: list[dict] = []
    last_page_seen = 0

    for page in range(1, max_pages + 1):
        try:
            resp = requests.get(
                CAPITOL_TRADES_URL,
                params={"page": page, "pageSize": page_size, "sortBy": "-pubDate"},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.error("[capitoltrades] page %d failed: %s", page, e)
            break

        rows = payload.get("data") or []
        if not rows:
            break

        for r in rows:
            trades.append(_normalize_trade(r))

        last_page_seen = page
        meta_paging = (payload.get("meta") or {}).get("paging") or {}
        total_pages = meta_paging.get("totalPages")
        if total_pages and page >= total_pages:
            break

    logger.info("[capitoltrades] fetched %d trades across %d pages", len(trades), last_page_seen)
    return trades


def fetch_recent_trades(
    lookback_days: int = 7,
    basis: str = "disclosure",
) -> dict:
    """
    Fetch and normalize trades from CapitolTrades, filtered to the last N days.

    Args:
        lookback_days: Window size.
        basis: 'disclosure' filters by disclosure_date — what newly became public.
               'transaction' filters by actual trade date — surfaces older trades disclosed late.

    Returns:
        {
          'lookback_days':    int,
          'basis':            'disclosure' | 'transaction',
          'cutoff_date':      'YYYY-MM-DD',
          'counts':           { 'house': int, 'senate': int, 'priority': int, 'total': int },
          'priority_trades':  [ ... normalized trades for Pelosi etc, newest first ],
          'all_trades':       [ ... every trade in window, sorted by amount_max desc ],
        }
    """
    if basis not in ("disclosure", "transaction"):
        raise ValueError("basis must be 'disclosure' or 'transaction'")

    cutoff     = (datetime.utcnow() - timedelta(days=lookback_days)).date()
    cutoff_iso = cutoff.strftime("%Y-%m-%d")
    date_field = "disclosure_date" if basis == "disclosure" else "transaction_date"

    all_trades = _fetch_pages()

    in_window = []
    for t in all_trades:
        d = t.get(date_field)
        if not d:
            continue
        try:
            if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff:
                in_window.append(t)
        except ValueError:
            continue

    # Priority section: newest first, then by size
    priority = [t for t in in_window if t["is_priority"]]
    priority.sort(key=lambda t: (t.get(date_field) or "", t.get("amount_max", 0)), reverse=True)

    # Everything else: by size desc, then date
    all_sorted = sorted(
        in_window,
        key=lambda t: (t.get("amount_max", 0), t.get(date_field) or ""),
        reverse=True,
    )

    counts = {
        "house":    sum(1 for t in in_window if t["chamber"] == "House"),
        "senate":   sum(1 for t in in_window if t["chamber"] == "Senate"),
        "priority": len(priority),
        "total":    len(in_window),
    }

    return {
        "lookback_days":   lookback_days,
        "basis":           basis,
        "cutoff_date":     cutoff_iso,
        "counts":          counts,
        "priority_trades": priority,
        "all_trades":      all_sorted,
    }


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data = fetch_recent_trades(lookback_days=7)
    print(json.dumps({
        "counts":          data["counts"],
        "cutoff_date":     data["cutoff_date"],
        "priority_sample": data["priority_trades"][:5],
        "top_sample":      data["all_trades"][:5],
    }, indent=2, default=str))
