#!/usr/bin/env python3
"""
Market trends data layer.

Pulls daily + weekly performance for:
  - Major indexes (SPY, QQQ, IWM, DIA)
  - 11 SPDR sector ETFs
  - Macro tape (VIX, 10Y yield, dollar, oil, gold, bitcoin)

Source: yfinance (free, no API key). We pull one month of daily bars per
symbol and compute 1D / 1W / 1M % changes from the close series, so every
period comes from the same source with the same timestamp convention.

Returns a MarketTrendsData dict suitable for email/JSON rendering.
"""

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


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
    ("^VIX",   "VIX (volatility)"),
    ("^TNX",   "10Y Treasury yield"),
    ("DX-Y.NYB", "US Dollar Index"),
    ("CL=F",   "Crude Oil (WTI)"),
    ("GC=F",   "Gold"),
    ("BTC-USD", "Bitcoin"),
]


def _pct_change(series, periods: int) -> Optional[float]:
    """Return % change from N trading sessions ago to the last close, or None."""
    if series is None or len(series) <= periods:
        return None
    try:
        latest = float(series.iloc[-1])
        prior  = float(series.iloc[-1 - periods])
        if prior == 0:
            return None
        return (latest / prior - 1.0) * 100.0
    except Exception:
        return None


def _fetch_symbol(symbol: str, label: str) -> Optional[dict]:
    """
    Pull ~1 month of daily closes for one symbol and compute perf snapshots.

    Returns: {symbol, label, price, change_1d, change_1w, change_1m}
    """
    try:
        import yfinance as yf

        ticker = yf.Ticker(symbol)
        # 1mo of daily bars is enough for 1D / 1W (5 sessions) / 1M (~21 sessions)
        hist = ticker.history(period="2mo", interval="1d", auto_adjust=False)

        if hist is None or hist.empty or "Close" not in hist.columns:
            logger.warning("[market_trends] No history for %s", symbol)
            return None

        closes = hist["Close"].dropna()
        if len(closes) < 2:
            logger.warning("[market_trends] Insufficient history for %s", symbol)
            return None

        price = float(closes.iloc[-1])
        return {
            "symbol":    symbol,
            "label":     label,
            "price":     price,
            "change_1d": _pct_change(closes, 1),
            "change_1w": _pct_change(closes, 5),
            "change_1m": _pct_change(closes, 21),
        }

    except Exception as e:
        logger.error("[market_trends] Error fetching %s: %s", symbol, e)
        return None


def _fetch_all(pairs: list[tuple[str, str]]) -> list[dict]:
    rows = []
    for symbol, label in pairs:
        row = _fetch_symbol(symbol, label)
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
    """
    Plain-English bullets describing what moved this week. Deterministic rules,
    no LLM. Kept short — 3 to 6 bullets.
    """
    bullets: list[str] = []

    # Index tape (daily)
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

    # Large-cap vs small-cap divergence
    if spy and iwm and spy.get("change_1w") is not None and iwm.get("change_1w") is not None:
        spread = spy["change_1w"] - iwm["change_1w"]
        if abs(spread) >= 1.5:
            leader = "large caps" if spread > 0 else "small caps"
            bullets.append(
                f"{leader.capitalize()} leading this week — SPY {spy['change_1w']:+.2f}% "
                f"vs IWM {iwm['change_1w']:+.2f}% ({spread:+.2f}pp spread)."
            )

    # Tech vs broad market
    if spy and qqq and spy.get("change_1w") is not None and qqq.get("change_1w") is not None:
        spread = qqq["change_1w"] - spy["change_1w"]
        if abs(spread) >= 1.0:
            leader = "Tech outperforming" if spread > 0 else "Tech underperforming"
            bullets.append(
                f"{leader} — QQQ {qqq['change_1w']:+.2f}% vs SPY {spy['change_1w']:+.2f}% this week."
            )

    # Sector rotation
    sector_movers = _top_movers(sectors, "change_1w", n=1)
    if sector_movers["winners"] and sector_movers["losers"]:
        w = sector_movers["winners"][0]
        l = sector_movers["losers"][0]
        if w.get("change_1w") is not None and l.get("change_1w") is not None:
            bullets.append(
                f"Sector leader: {w['label']} ({w['change_1w']:+.2f}% / 1W). "
                f"Laggard: {l['label']} ({l['change_1w']:+.2f}% / 1W)."
            )

    # Volatility
    vix = next((r for r in macro if r["symbol"] == "^VIX"), None)
    if vix and vix.get("change_1w") is not None:
        if vix["change_1w"] >= 10:
            bullets.append(f"Volatility up sharply — VIX {vix['change_1w']:+.1f}% this week, now {vix['price']:.2f}.")
        elif vix["change_1w"] <= -10:
            bullets.append(f"Volatility falling — VIX {vix['change_1w']:+.1f}% this week, now {vix['price']:.2f}.")

    # Yields
    tnx = next((r for r in macro if r["symbol"] == "^TNX"), None)
    if tnx and tnx.get("change_1w") is not None and abs(tnx["change_1w"]) >= 3:
        direction = "rising" if tnx["change_1w"] > 0 else "falling"
        bullets.append(
            f"10Y Treasury yield {direction} — {tnx['change_1w']:+.2f}% this week, now {tnx['price']:.2f}%."
        )

    # Crypto / commodity callouts (only if meaningful)
    btc = next((r for r in macro if r["symbol"] == "BTC-USD"), None)
    if btc and btc.get("change_1w") is not None and abs(btc["change_1w"]) >= 5:
        bullets.append(f"Bitcoin {btc['change_1w']:+.2f}% this week, now ${btc['price']:,.0f}.")

    oil = next((r for r in macro if r["symbol"] == "CL=F"), None)
    if oil and oil.get("change_1w") is not None and abs(oil["change_1w"]) >= 4:
        bullets.append(f"Crude oil {oil['change_1w']:+.2f}% this week, now ${oil['price']:.2f}.")

    return bullets[:6]


def fetch_market_trends() -> dict:
    """
    Fetch everything and return a render-ready dict.

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
    now = datetime.now()

    logger.info("Fetching index data...")
    indexes = _fetch_all(INDEXES)

    logger.info("Fetching sector data...")
    sectors = _fetch_all(SECTORS)

    logger.info("Fetching macro tape...")
    macro = _fetch_all(MACRO)

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data = fetch_market_trends()
    print(json.dumps(data, indent=2, default=str))
