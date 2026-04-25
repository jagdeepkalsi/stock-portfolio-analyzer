#!/usr/bin/env python3
"""
Market trends data layer.

Pulls daily + weekly performance for:
  - Major indexes (SPY, QQQ, IWM, DIA)
  - 11 SPDR sector ETFs
  - Macro tape (volatility, treasuries, dollar, oil, gold, bitcoin) via ETF proxies

Source: Finnhub REST API (free tier supports 60 req/min for US ETFs).

For each symbol we try /stock/candle first to get ~2 months of daily closes,
which gives us 1D / 1W / 1M % changes from a single source. If /candle is
unavailable on the caller's tier, we fall back to /quote — that still gives
1D from c vs pc, with 1W and 1M reported as None.

Macro instruments (^VIX, ^TNX, futures, BTC-USD) aren't on Finnhub's free
tier, so we substitute liquid ETF proxies that move closely with them:
  VIXY  ≈ VIX volatility
  IEF   ≈ 7-10Y Treasuries (inversely correlated with 10Y yield)
  UUP   ≈ US Dollar Index
  USO   ≈ WTI crude oil
  GLD   ≈ gold
  IBIT  ≈ bitcoin

Returns a render-ready dict (see fetch_market_trends docstring).
"""

import logging
import time
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"


INDEXES = [
    ("SPY",  "S&P 500"),
    ("QQQ",  "Nasdaq 100"),
    ("IWM",  "Russell 2000"),
    ("DIA",  "Dow Jones"),
]

SECTORS = [
    ("XLK",  "Technology"),
    ("XLF",  "Financials"),
    ("XLV",  "Health Care"),
    ("XLY",  "Consumer Discretionary"),
    ("XLP",  "Consumer Staples"),
    ("XLE",  "Energy"),
    ("XLI",  "Industrials"),
    ("XLB",  "Materials"),
    ("XLRE", "Real Estate"),
    ("XLU",  "Utilities"),
    ("XLC",  "Communication Services"),
]

MACRO = [
    ("VIXY", "Volatility (VIXY proxy)"),
    ("IEF",  "Treasuries 7-10Y (IEF)"),
    ("UUP",  "US Dollar (UUP proxy)"),
    ("USO",  "Crude Oil (USO proxy)"),
    ("GLD",  "Gold (GLD)"),
    ("IBIT", "Bitcoin (IBIT proxy)"),
]


def _pct_change(closes: list[float], periods: int) -> Optional[float]:
    """Return % change from N sessions ago to the latest close, or None."""
    if not closes or len(closes) <= periods:
        return None
    try:
        latest = float(closes[-1])
        prior  = float(closes[-1 - periods])
        if prior == 0:
            return None
        return (latest / prior - 1.0) * 100.0
    except (ValueError, TypeError):
        return None


def _get(path: str, params: dict, api_key: str) -> Optional[dict]:
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/{path}",
            params={**params, "token": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        # 403 on /stock/candle means the caller's plan doesn't include candles —
        # we expect this on free tier and fall back to /quote silently at info-level.
        if e.response is not None and e.response.status_code == 403 and path == "stock/candle":
            logger.info("[market_trends] %s 403 (no candle access) for %s", path, params.get("symbol"))
        else:
            logger.error("[market_trends] %s failed for %s: %s", path, params.get("symbol"), e)
        return None
    except Exception as e:
        logger.error("[market_trends] %s failed for %s: %s", path, params.get("symbol"), e)
        return None


def _fetch_symbol(symbol: str, label: str, api_key: str) -> Optional[dict]:
    """
    Pull daily closes for one symbol and compute perf snapshots.

    Tries /stock/candle first (gives 1D / 1W / 1M from one call).
    Falls back to /quote (1D only) if candle is unavailable.
    """
    now_ts        = int(time.time())
    sixty_days_ago = now_ts - 60 * 86400

    candle = _get(
        "stock/candle",
        {"symbol": symbol, "resolution": "D", "from": sixty_days_ago, "to": now_ts},
        api_key,
    )
    if candle and candle.get("s") == "ok" and candle.get("c"):
        closes = candle["c"]
        if len(closes) >= 2:
            price = float(closes[-1])
            return {
                "symbol":    symbol,
                "label":     label,
                "price":     price,
                "change_1d": _pct_change(closes, 1),
                "change_1w": _pct_change(closes, 5),
                "change_1m": _pct_change(closes, 21),
            }

    # Fallback: /quote gives current + previous close (1D only)
    quote = _get("quote", {"symbol": symbol}, api_key)
    if not quote or not quote.get("c"):
        logger.warning("[market_trends] No data for %s", symbol)
        return None

    price      = float(quote["c"])
    prev_close = float(quote.get("pc") or 0)
    change_1d  = ((price / prev_close - 1.0) * 100.0) if prev_close else None

    return {
        "symbol":    symbol,
        "label":     label,
        "price":     price,
        "change_1d": change_1d,
        "change_1w": None,
        "change_1m": None,
    }


def _fetch_all(pairs: list[tuple[str, str]], api_key: str) -> list[dict]:
    rows = []
    for symbol, label in pairs:
        row = _fetch_symbol(symbol, label, api_key)
        if row:
            rows.append(row)
    return rows


def _top_movers(rows: list[dict], key: str, n: int = 3) -> dict:
    """Top N winners and losers by the given key (e.g. 'change_1w')."""
    clean = [r for r in rows if r.get(key) is not None]
    clean.sort(key=lambda r: r[key], reverse=True)
    return {
        "winners": clean[:n],
        "losers":  list(reversed(clean[-n:])) if len(clean) >= n else list(reversed(clean)),
    }


def _build_highlights(indexes: list[dict], sectors: list[dict], macro: list[dict]) -> list[str]:
    """Plain-English bullets describing what moved this week. Deterministic rules."""
    bullets: list[str] = []

    spy = next((r for r in indexes if r["symbol"] == "SPY"), None)
    qqq = next((r for r in indexes if r["symbol"] == "QQQ"), None)
    iwm = next((r for r in indexes if r["symbol"] == "IWM"), None)

    if spy and spy.get("change_1d") is not None:
        direction = "higher" if spy["change_1d"] >= 0 else "lower"
        bullets.append(
            f"S&P 500 closed {direction} on the day ({spy['change_1d']:+.2f}%), "
            f"{spy['change_1w']:+.2f}% on the week." if spy.get("change_1w") is not None
            else f"S&P 500 closed {direction} on the day ({spy['change_1d']:+.2f}%)."
        )

    if spy and iwm and spy.get("change_1w") is not None and iwm.get("change_1w") is not None:
        spread = spy["change_1w"] - iwm["change_1w"]
        if abs(spread) >= 1.5:
            leader = "large caps" if spread > 0 else "small caps"
            bullets.append(
                f"{leader.capitalize()} leading this week — SPY {spy['change_1w']:+.2f}% "
                f"vs IWM {iwm['change_1w']:+.2f}% ({spread:+.2f}pp spread)."
            )

    if spy and qqq and spy.get("change_1w") is not None and qqq.get("change_1w") is not None:
        spread = qqq["change_1w"] - spy["change_1w"]
        if abs(spread) >= 1.0:
            leader = "Tech outperforming" if spread > 0 else "Tech underperforming"
            bullets.append(
                f"{leader} — QQQ {qqq['change_1w']:+.2f}% vs SPY {spy['change_1w']:+.2f}% this week."
            )

    sector_movers = _top_movers(sectors, "change_1w", n=1)
    if sector_movers["winners"] and sector_movers["losers"]:
        w = sector_movers["winners"][0]
        l = sector_movers["losers"][0]
        if w.get("change_1w") is not None and l.get("change_1w") is not None:
            bullets.append(
                f"Sector leader: {w['label']} ({w['change_1w']:+.2f}% / 1W). "
                f"Laggard: {l['label']} ({l['change_1w']:+.2f}% / 1W)."
            )

    vixy = next((r for r in macro if r["symbol"] == "VIXY"), None)
    if vixy and vixy.get("change_1w") is not None:
        if vixy["change_1w"] >= 10:
            bullets.append(f"Volatility up sharply — VIXY {vixy['change_1w']:+.1f}% this week.")
        elif vixy["change_1w"] <= -10:
            bullets.append(f"Volatility falling — VIXY {vixy['change_1w']:+.1f}% this week.")

    ief = next((r for r in macro if r["symbol"] == "IEF"), None)
    if ief and ief.get("change_1w") is not None and abs(ief["change_1w"]) >= 1.5:
        # IEF up = yields down, and vice versa
        direction = "falling" if ief["change_1w"] > 0 else "rising"
        bullets.append(
            f"Treasury yields {direction} — IEF {ief['change_1w']:+.2f}% this week."
        )

    ibit = next((r for r in macro if r["symbol"] == "IBIT"), None)
    if ibit and ibit.get("change_1w") is not None and abs(ibit["change_1w"]) >= 5:
        bullets.append(f"Bitcoin {ibit['change_1w']:+.2f}% this week (via IBIT).")

    uso = next((r for r in macro if r["symbol"] == "USO"), None)
    if uso and uso.get("change_1w") is not None and abs(uso["change_1w"]) >= 4:
        bullets.append(f"Crude oil {uso['change_1w']:+.2f}% this week (via USO).")

    return bullets[:6]


def fetch_market_trends(api_key: str) -> dict:
    """
    Fetch everything and return a render-ready dict.

    Args:
        api_key: Finnhub API key (required).

    Shape:
    {
      'generated_at':          ISO string,
      'generated_at_display':  'April 24, 2026',
      'indexes':   [ {symbol, label, price, change_1d, change_1w, change_1m}, ... ],
      'sectors':   [ ... same shape ... ],
      'macro':     [ ... same shape ... ],
      'sector_movers': { 'winners': [...3], 'losers': [...3] },  # by 1W
      'highlights': [ 'bullet string', ... ],
    }
    """
    if not api_key:
        raise ValueError("fetch_market_trends requires a Finnhub api_key")

    now = datetime.now()

    logger.info("Fetching index data...")
    indexes = _fetch_all(INDEXES, api_key)

    logger.info("Fetching sector data...")
    sectors = _fetch_all(SECTORS, api_key)

    logger.info("Fetching macro tape...")
    macro = _fetch_all(MACRO, api_key)

    sector_movers = _top_movers(sectors, "change_1w", n=3)
    highlights    = _build_highlights(indexes, sectors, macro)

    return {
        "generated_at":         now.isoformat(),
        "generated_at_display": now.strftime("%B %d, %Y"),
        "indexes":              indexes,
        "sectors":              sectors,
        "macro":                macro,
        "sector_movers":        sector_movers,
        "highlights":           highlights,
    }


if __name__ == "__main__":
    import json
    import os

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    key = os.environ.get("FINNHUB_API_KEY")
    if not key:
        print("Set FINNHUB_API_KEY in your environment to test locally.")
        raise SystemExit(1)

    data = fetch_market_trends(key)
    print(json.dumps(data, indent=2, default=str))
