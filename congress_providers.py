#!/usr/bin/env python3
"""
Congressional trade data providers.

Pulls disclosed equity trades for members of the U.S. House and Senate from
the free, public Stock Watcher S3 JSON feeds:

  - House:  https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json
  - Senate: https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json

Both feeds are built by scraping official STOCK Act PTR filings. There is a
~24–48 hour lag behind actual disclosures.

Normalized trade dict shape:
{
  'chamber':         'House' | 'Senate',
  'member':          'Nancy Pelosi',
  'party':           'D' | 'R' | 'I' | None,         # not in source feeds (v1: None)
  'state':           str | None,
  'district':        str | None,                     # House only
  'ticker':          str,
  'asset':           str,                            # long description
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

HOUSE_FEED_URL  = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
SENATE_FEED_URL = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"

# Names we always flag as priority. Lowercased substring match against member name.
PRIORITY_MEMBERS = {
    "pelosi",
}

# Feed "type" strings → normalized transaction bucket
_TX_MAP = {
    "purchase":        "purchase",
    "p":               "purchase",
    "sale":            "sale",
    "sale_full":       "sale",
    "sale (full)":     "sale",
    "sale_partial":    "sale",
    "sale (partial)":  "sale",
    "s":               "sale",
    "s (partial)":     "sale",
    "exchange":        "exchange",
    "e":               "exchange",
}

# Typical Stock Watcher ranges — parse both "$1,001 - $15,000" and "$1,001 -"
_AMOUNT_RE = re.compile(r"\$?([\d,]+)(?:\s*-\s*\$?([\d,]+))?")


def _parse_amount(amount: Optional[str]) -> tuple[float, float]:
    """Return (min, max) dollar values parsed from a range string. Missing → (0.0, 0.0)."""
    if not amount:
        return 0.0, 0.0
    m = _AMOUNT_RE.search(amount)
    if not m:
        return 0.0, 0.0
    lo = float(m.group(1).replace(",", "")) if m.group(1) else 0.0
    hi = float(m.group(2).replace(",", "")) if m.group(2) else lo
    return lo, hi


def _parse_date(value: Optional[str]) -> Optional[str]:
    """
    Feeds use either 'YYYY-MM-DD' or 'MM/DD/YYYY'. Normalize to ISO.
    Returns None if unparseable.
    """
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _normalize_type(value: Optional[str]) -> tuple[str, str]:
    raw = (value or "").strip()
    key = raw.lower().replace("  ", " ")
    return _TX_MAP.get(key, "other"), raw


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


# ---------------------------------------------------------------------------
# House
# ---------------------------------------------------------------------------

def _fetch_house() -> list[dict]:
    logger.info("Fetching House disclosures from Stock Watcher...")
    try:
        resp = requests.get(HOUSE_FEED_URL, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error("[house] Failed to fetch feed: %s", e)
        return []

    trades = []
    for row in raw:
        transaction, raw_type = _normalize_type(row.get("type"))
        tx_date  = _parse_date(row.get("transaction_date"))
        dis_date = _parse_date(row.get("disclosure_date"))
        lo, hi   = _parse_amount(row.get("amount"))
        member   = (row.get("representative") or "").strip()
        ticker   = (row.get("ticker") or "").strip().upper() or "—"

        trades.append({
            "chamber":          "House",
            "member":           member,
            "party":            None,
            "state":            (row.get("state") or None),
            "district":         (row.get("district") or None),
            "ticker":           ticker,
            "asset":            (row.get("asset_description") or "").strip(),
            "transaction":      transaction,
            "raw_type":         raw_type,
            "amount_range":     (row.get("amount") or "").strip(),
            "amount_min":       lo,
            "amount_max":       hi,
            "transaction_date": tx_date,
            "disclosure_date":  dis_date,
            "days_to_disclose": _days_between(tx_date, dis_date),
            "ptr_url":          row.get("ptr_link"),
            "is_priority":      _is_priority(member),
        })
    logger.info("[house] Parsed %d trades", len(trades))
    return trades


# ---------------------------------------------------------------------------
# Senate
# ---------------------------------------------------------------------------

def _fetch_senate() -> list[dict]:
    logger.info("Fetching Senate disclosures from Stock Watcher...")
    try:
        resp = requests.get(SENATE_FEED_URL, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        logger.error("[senate] Failed to fetch feed: %s", e)
        return []

    trades = []
    for row in raw:
        transaction, raw_type = _normalize_type(row.get("type"))
        tx_date  = _parse_date(row.get("transaction_date"))
        dis_date = _parse_date(row.get("disclosure_date"))
        lo, hi   = _parse_amount(row.get("amount"))
        member   = (row.get("senator") or "").strip()
        ticker   = (row.get("ticker") or "").strip().upper() or "—"

        trades.append({
            "chamber":          "Senate",
            "member":           member,
            "party":            None,
            "state":            None,
            "district":         None,
            "ticker":           ticker,
            "asset":            (row.get("asset_description") or "").strip(),
            "transaction":      transaction,
            "raw_type":         raw_type,
            "amount_range":     (row.get("amount") or "").strip(),
            "amount_min":       lo,
            "amount_max":       hi,
            "transaction_date": tx_date,
            "disclosure_date":  dis_date,
            "days_to_disclose": _days_between(tx_date, dis_date),
            "ptr_url":          row.get("ptr_link"),
            "is_priority":      _is_priority(member),
        })
    logger.info("[senate] Parsed %d trades", len(trades))
    return trades


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_recent_trades(
    lookback_days: int = 7,
    basis: str = "disclosure",
) -> dict:
    """
    Fetch and normalize trades from both chambers, filtered to the last N days.

    Args:
        lookback_days: Window size.
        basis: 'disclosure' (default) filters by disclosure_date — reflects what
               has newly become public. 'transaction' filters by actual trade date,
               which can surface older trades disclosed late.

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

    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).date()
    cutoff_iso = cutoff.strftime("%Y-%m-%d")
    date_field = "disclosure_date" if basis == "disclosure" else "transaction_date"

    all_trades = _fetch_house() + _fetch_senate()

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

    # Priority section: Pelosi first, newest first
    priority = [t for t in in_window if t["is_priority"]]
    priority.sort(key=lambda t: (t.get(date_field) or "", t.get("amount_max", 0)), reverse=True)

    # Everything else: sorted by trade size (amount_max) desc, then date
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
