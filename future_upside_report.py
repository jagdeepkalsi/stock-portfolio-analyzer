#!/usr/bin/env python3
"""
Future Upside Watchlist report.

Builds a local HTML + JSON report that:
  1. Scores future-facing themes using ETF performance.
  2. Screens a curated universe for beaten-down upside candidates.
  3. Adds current stock data and a compact options snapshot for the top names.

Run:
    python future_upside_report.py --dry
    python future_upside_report.py --display-limit 50
    python future_upside_report.py --scan-limit 25 --display-limit 10
    python future_upside_report.py --market-provider polygon --display-limit 50
    python future_upside_report.py --skip-options
"""

import argparse
import csv
import html
import io
import json
import logging
import math
import os
import statistics
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from congress_providers import fetch_member_trades, fetch_recent_trades

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_WATCHLIST = "future_watchlist.json"
DEFAULT_OUT_DIR = "out"
DEFAULT_CONGRESS_MEMBERS = [
    "Nancy Pelosi",
    "Josh Gottheimer",
    "Dan Crenshaw",
    "Ro Khanna",
]
CONGRESS_MEMBER_ALIASES = {
    "nancy pelosi": ["nancy pelosi"],
    "josh gottheimer": ["josh gottheimer"],
    "dan crenshaw": ["dan crenshaw", "daniel crenshaw"],
    "ro khanna": ["ro khanna", "rohit khanna"],
}
CAPITOL_TRADES_POLITICIAN_IDS = {
    "nancy pelosi": "P000197",
    "josh gottheimer": "G000583",
    "dan crenshaw": "C001120",
    "ro khanna": "K000389",
}
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FINNHUB_BASE = "https://finnhub.io/api/v1"
POLYGON_BASE = "https://api.polygon.io"
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}
ACTIVE_MARKET_PROVIDER = "auto"
REPORT_TIMEZONE = ZoneInfo("America/Los_Angeles")


class MarketDataError(RuntimeError):
    """Raised when the configured market data source cannot serve the report."""


@dataclass
class ThemeScore:
    name: str
    description: str
    sector_etfs: list[str]
    score: float
    change_1w: float | None
    change_1m: float | None
    status: str


@dataclass
class OptionSnapshot:
    expiration: str | None = None
    atm_strike: float | None = None
    atm_last: float | None = None
    atm_bid: float | None = None
    atm_ask: float | None = None
    atm_iv: float | None = None
    atm_open_interest: int | None = None
    atm_volume: int | None = None
    otm_strike: float | None = None
    otm_last: float | None = None
    otm_bid: float | None = None
    otm_ask: float | None = None
    otm_iv: float | None = None
    otm_open_interest: int | None = None
    otm_volume: int | None = None
    breakeven: float | None = None
    breakeven_upside_pct: float | None = None
    note: str = ""


@dataclass
class CompanyScore:
    symbol: str
    company_name: str
    theme: str
    sector: str
    industry: str
    price: float | None
    day_change_pct: float | None
    change_5d_pct: float | None
    change_1m_pct: float | None
    week_52_low: float | None
    week_52_high: float | None
    pct_above_low: float | None
    pct_below_high: float | None
    market_cap: float | None
    avg_volume: float | None
    volume: float | None
    relative_volume: float | None
    target_mean_price: float | None
    target_upside_pct: float | None
    beta: float | None
    score: float
    setup: str
    risk: str
    signals: list[str]
    option: OptionSnapshot


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _pct(now: float | None, prior: float | None) -> float | None:
    if now is None or prior is None or prior == 0:
        return None
    return (now / prior - 1.0) * 100.0


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    return f"${value:,.2f}"


def _fmt_range(lo: float | None, hi: float | None, fallback: str = "-") -> str:
    lo = _safe_float(lo)
    hi = _safe_float(hi)
    if lo is None and hi is None:
        return fallback or "-"
    if lo is None:
        return f"up to {_fmt_money(hi)}"
    if hi is None or lo == hi:
        return _fmt_money(lo)
    return f"{_fmt_money(lo)} - {_fmt_money(hi)}"


def _fmt_num(value: float | int | None, decimals: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:,.{decimals}f}"


def _fmt_pct(value: float | None, signed: bool = False) -> str:
    if value is None:
        return "-"
    sign = "+" if signed and value >= 0 else ""
    return f"{sign}{value:.1f}%"


def _esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _detail_filename(symbol: str) -> str:
    safe = "".join(ch for ch in symbol.upper() if ch.isalnum() or ch in ("-", "."))
    return f"{safe or 'symbol'}.html"


def _iso_from_ts(ts: int | float | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(float(ts)).date().isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def _cell_pct(value: float | None, signed: bool = True) -> str:
    if value is None:
        return '<td class="num muted">-</td>'
    cls = "pos" if value >= 0 else "neg"
    return f'<td class="num {cls}">{_fmt_pct(value, signed=signed)}</td>'


def load_watchlist(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def flatten_universe(watchlist: dict) -> dict[str, dict]:
    universe: dict[str, dict] = {}
    for theme in watchlist.get("themes", []):
        for symbol in theme.get("symbols", []):
            symbol = symbol.upper().strip()
            if not symbol:
                continue
            existing = universe.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "themes": [],
                    "sector_etfs": set(),
                },
            )
            existing["themes"].append(theme["name"])
            existing["sector_etfs"].update(theme.get("sector_etfs", []))

    for row in universe.values():
        row["sector_etfs"] = sorted(row["sector_etfs"])
    return universe


def resolve_market_provider(requested: str) -> str:
    requested = (requested or "auto").lower().strip()
    if requested not in {"auto", "polygon", "finnhub", "yahoo"}:
        raise MarketDataError("--market-provider must be one of: auto, polygon, finnhub, yahoo")

    has_polygon = bool(os.getenv("POLYGON_API_KEY"))
    has_finnhub = bool(os.getenv("FINNHUB_API_KEY"))
    if requested == "polygon" and not has_polygon:
        raise MarketDataError(
            "POLYGON_API_KEY is not set. Add it to .env, or use --market-provider finnhub/yahoo."
        )
    if requested == "finnhub" and not has_finnhub:
        raise MarketDataError(
            "FINNHUB_API_KEY is not set. Add it to .env, or run with --market-provider yahoo "
            "after Yahoo rate limits cool down."
        )
    if requested == "auto":
        if has_polygon:
            return "polygon"
        return "finnhub" if has_finnhub else "yahoo"
    return requested


def validate_market_provider(provider: str) -> None:
    if provider == "polygon":
        probe = polygon_chart("QQQ", range_="3mo")
        if len(probe.get("closes") or []) < 20:
            raise MarketDataError(
                "Polygon did not return usable daily OHLC history. Check POLYGON_API_KEY in .env."
            )
        return

    if provider == "finnhub":
        probe = finnhub_chart("QQQ", range_="3mo")
        if not probe.get("closes"):
            raise MarketDataError(
                "Finnhub did not return usable quote/history data. Check FINNHUB_API_KEY in .env."
            )
        return

    try:
        resp = requests.get(
            YAHOO_CHART_URL.format(symbol="QQQ"),
            params={"range": "5d", "interval": "1d"},
            headers=HTTP_HEADERS,
            timeout=12,
        )
        if resp.status_code == 429:
            raise MarketDataError(
                "Yahoo Finance is rate-limiting this Mac/IP right now (HTTP 429). "
                "Add FINNHUB_API_KEY to .env and rerun with --market-provider finnhub, "
                "or wait and retry Yahoo later with a larger --pause."
            )
        resp.raise_for_status()
    except MarketDataError:
        raise
    except Exception as e:
        raise MarketDataError(f"Yahoo Finance probe failed: {e}") from e


def yahoo_chart(symbol: str, range_: str = "1y", interval: str = "1d") -> dict:
    try:
        resp = requests.get(
            YAHOO_CHART_URL.format(symbol=symbol),
            params={"range": range_, "interval": interval},
            headers=HTTP_HEADERS,
            timeout=12,
        )
        if resp.status_code == 429:
            raise MarketDataError(
                "Yahoo Finance is rate-limiting requests (HTTP 429). "
                "Use --market-provider finnhub with FINNHUB_API_KEY, or retry later."
            )
        resp.raise_for_status()
        data = resp.json()
    except MarketDataError:
        raise
    except Exception as e:
        logger.warning("Chart fetch failed for %s: %s", symbol, e)
        return {}

    try:
        result = (data.get("chart", {}).get("result") or [])[0]
    except (IndexError, AttributeError):
        return {}

    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    raw_closes = [_safe_float(v) for v in quote.get("close", [])]
    raw_volumes = [_safe_float(v) for v in quote.get("volume", [])]
    raw_timestamps = result.get("timestamp") or []
    history = []
    for idx, close in enumerate(raw_closes):
        if close is None:
            continue
        history.append(
            {
                "date": _iso_from_ts(raw_timestamps[idx] if idx < len(raw_timestamps) else None),
                "close": close,
                "volume": raw_volumes[idx] if idx < len(raw_volumes) else None,
            }
        )
    closes = [row["close"] for row in history]
    volumes = [row["volume"] for row in history if row.get("volume") is not None]
    dates = [row["date"] for row in history]

    return {
        "meta": result.get("meta") or {},
        "closes": closes,
        "volumes": volumes,
        "dates": dates,
        "history": history,
    }


def stooq_chart(symbol: str, range_: str = "1y") -> dict:
    """Fetch daily historical prices from Stooq as a no-key fallback."""
    api_key = os.getenv("STOOQ_API_KEY")
    if not api_key:
        return {}
    days = 95 if range_ == "3mo" else 370
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=days)
    stooq_symbol = symbol.lower().replace("-", ".")
    if not stooq_symbol.endswith(".us"):
        stooq_symbol = f"{stooq_symbol}.us"

    try:
        resp = requests.get(
            "https://stooq.com/q/d/l/",
            params={
                "s": stooq_symbol,
                "d1": from_date.strftime("%Y%m%d"),
                "d2": to_date.strftime("%Y%m%d"),
                "i": "d",
                "apikey": api_key,
            },
            headers=HTTP_HEADERS,
            timeout=12,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.info("Stooq daily history fetch failed for %s: %s", symbol, e)
        return {}

    history = []
    try:
        for row in csv.DictReader(io.StringIO(resp.text)):
            close = _safe_float(row.get("Close"))
            if close is None:
                continue
            history.append(
                {
                    "date": row.get("Date") or "",
                    "close": close,
                    "volume": _safe_float(row.get("Volume")),
                }
            )
    except csv.Error as e:
        logger.info("Stooq daily history parse failed for %s: %s", symbol, e)
        return {}

    history = [row for row in history if row.get("date")]
    if not history:
        return {}
    closes = [row["close"] for row in history]
    volumes = [row["volume"] for row in history if row.get("volume") is not None]
    dates = [row["date"] for row in history]
    return {
        "meta": {
            "regularMarketPrice": closes[-1],
            "chartPreviousClose": closes[-2] if len(closes) > 1 else None,
        },
        "closes": closes,
        "volumes": volumes,
        "dates": dates,
        "history": history,
    }


def polygon_chart(symbol: str, range_: str = "1y") -> dict:
    """Fetch adjusted daily OHLCV bars from Polygon."""
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        return {}

    days = 95 if range_ == "3mo" else 370
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=days)
    polygon_symbol = symbol.upper().replace("-", ".")

    try:
        resp = requests.get(
            f"{POLYGON_BASE}/v2/aggs/ticker/{polygon_symbol}/range/1/day/"
            f"{from_date.isoformat()}/{to_date.isoformat()}",
            params={
                "adjusted": "true",
                "sort": "asc",
                "limit": 50000,
                "apiKey": api_key,
            },
            headers=HTTP_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Polygon daily history fetch failed for %s: %s", symbol, e)
        return {}

    rows = data.get("results") or []
    history = []
    for row in rows:
        close = _safe_float(row.get("c"))
        if close is None:
            continue
        ts = _safe_float(row.get("t"))
        date_text = datetime.fromtimestamp(ts / 1000).date().isoformat() if ts else ""
        history.append(
            {
                "date": date_text,
                "close": close,
                "volume": _safe_float(row.get("v")),
                "open": _safe_float(row.get("o")),
                "high": _safe_float(row.get("h")),
                "low": _safe_float(row.get("l")),
            }
        )

    history = [row for row in history if row.get("date")]
    if not history:
        return {}

    closes = [row["close"] for row in history]
    volumes = [row["volume"] for row in history if row.get("volume") is not None]
    dates = [row["date"] for row in history]
    return {
        "meta": {
            "regularMarketPrice": closes[-1],
            "chartPreviousClose": closes[-2] if len(closes) > 1 else None,
            "regularMarketVolume": volumes[-1] if volumes else None,
        },
        "closes": closes,
        "volumes": volumes,
        "dates": dates,
        "history": history,
    }


def finnhub_metadata(symbol: str) -> dict[str, Any]:
    """Fetch Finnhub quote/profile metrics without relying on candle history."""
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return {}

    meta: dict[str, Any] = {}
    try:
        quote = requests.get(
            f"{FINNHUB_BASE}/quote",
            params={"symbol": symbol, "token": api_key},
            timeout=12,
        )
        quote.raise_for_status()
        q = quote.json()
        current = _safe_float(q.get("c"))
        previous = _safe_float(q.get("pc"))
        if current:
            meta["regularMarketPrice"] = current
        if previous:
            meta["chartPreviousClose"] = previous
        if _safe_float(q.get("dp")) is not None:
            meta["regularMarketChangePercent"] = _safe_float(q.get("dp"))
    except Exception as e:
        logger.info("Finnhub quote metadata fetch failed for %s: %s", symbol, e)

    try:
        metric = requests.get(
            f"{FINNHUB_BASE}/stock/metric",
            params={"symbol": symbol, "metric": "all", "token": api_key},
            timeout=12,
        )
        metric.raise_for_status()
        metrics = metric.json().get("metric") or {}
        meta["fiftyTwoWeekHigh"] = _safe_float(metrics.get("52WeekHigh"))
        meta["fiftyTwoWeekLow"] = _safe_float(metrics.get("52WeekLow"))
    except Exception:
        pass

    try:
        profile = requests.get(
            f"{FINNHUB_BASE}/stock/profile2",
            params={"symbol": symbol, "token": api_key},
            timeout=12,
        )
        profile.raise_for_status()
        p = profile.json()
        if p.get("name"):
            meta["longName"] = p["name"]
        if p.get("marketCapitalization"):
            meta["marketCap"] = _safe_float(p["marketCapitalization"]) * 1_000_000
        if p.get("finnhubIndustry"):
            meta["industry"] = p["finnhubIndustry"]
    except Exception:
        pass

    return {key: value for key, value in meta.items() if value is not None}


def finnhub_chart(symbol: str, range_: str = "1y") -> dict:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return {}

    now_ts = int(time.time())
    days = 95 if range_ == "3mo" else 370
    from_ts = now_ts - days * 86400
    closes: list[float] = []
    volumes: list[float] = []
    dates: list[str] = []
    history: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}

    try:
        candle = requests.get(
            f"{FINNHUB_BASE}/stock/candle",
            params={
                "symbol": symbol,
                "resolution": "D",
                "from": from_ts,
                "to": now_ts,
                "token": api_key,
            },
            timeout=12,
        )
        if candle.status_code != 403:
            candle.raise_for_status()
            candle_data = candle.json()
            if candle_data.get("s") == "ok":
                raw_closes = [_safe_float(v) for v in candle_data.get("c", [])]
                raw_volumes = [_safe_float(v) for v in candle_data.get("v", [])]
                raw_timestamps = candle_data.get("t", [])
                history = []
                for idx, close in enumerate(raw_closes):
                    if close is None:
                        continue
                    history.append(
                        {
                            "date": _iso_from_ts(raw_timestamps[idx] if idx < len(raw_timestamps) else None),
                            "close": close,
                            "volume": raw_volumes[idx] if idx < len(raw_volumes) else None,
                        }
                    )
                closes = [row["close"] for row in history]
                volumes = [row["volume"] for row in history if row.get("volume") is not None]
                dates = [row["date"] for row in history]
    except Exception as e:
        logger.info("Finnhub candle fetch failed for %s: %s", symbol, e)

    if len(history) < 20:
        fallback = stooq_chart(symbol, range_=range_)
        if len(fallback.get("history") or []) >= 20:
            logger.info("Using Stooq daily history fallback for %s", symbol)
            closes = fallback["closes"]
            volumes = fallback["volumes"]
            dates = fallback["dates"]
            history = fallback["history"]
            meta.update(fallback.get("meta") or {})

    try:
        quote = requests.get(
            f"{FINNHUB_BASE}/quote",
            params={"symbol": symbol, "token": api_key},
            timeout=12,
        )
        quote.raise_for_status()
        q = quote.json()
        current = _safe_float(q.get("c"))
        previous = _safe_float(q.get("pc"))
        if current:
            meta["regularMarketPrice"] = current
            if not closes:
                closes = [v for v in [previous, current] if v is not None]
                dates = [datetime.now().date().isoformat()] * len(closes)
                history = [{"date": date, "close": close, "volume": None} for date, close in zip(dates, closes)]
        if previous:
            meta["chartPreviousClose"] = previous
    except Exception as e:
        logger.warning("Finnhub quote fetch failed for %s: %s", symbol, e)

    try:
        metric = requests.get(
            f"{FINNHUB_BASE}/stock/metric",
            params={"symbol": symbol, "metric": "all", "token": api_key},
            timeout=12,
        )
        metric.raise_for_status()
        metrics = metric.json().get("metric") or {}
        meta["fiftyTwoWeekHigh"] = _safe_float(metrics.get("52WeekHigh"))
        meta["fiftyTwoWeekLow"] = _safe_float(metrics.get("52WeekLow"))
        avg_vol = _safe_float(metrics.get("10DayAverageTradingVolume"))
        if avg_vol and not volumes:
            volumes = [avg_vol * 1_000_000]
    except Exception:
        pass

    try:
        profile = requests.get(
            f"{FINNHUB_BASE}/stock/profile2",
            params={"symbol": symbol, "token": api_key},
            timeout=12,
        )
        profile.raise_for_status()
        p = profile.json()
        if p.get("name"):
            meta["longName"] = p["name"]
        if p.get("marketCapitalization"):
            meta["marketCap"] = _safe_float(p["marketCapitalization"]) * 1_000_000
        if p.get("finnhubIndustry"):
            meta["industry"] = p["finnhubIndustry"]
    except Exception:
        pass

    if not closes and not meta.get("regularMarketPrice"):
        return {}
    return {
        "meta": meta,
        "closes": closes,
        "volumes": volumes,
        "dates": dates,
        "history": history,
    }


def history_perf(symbol: str, range_: str = "1y") -> dict:
    if ACTIVE_MARKET_PROVIDER == "polygon":
        chart = polygon_chart(symbol, range_=range_)
        if chart.get("history") and os.getenv("FINNHUB_API_KEY"):
            chart["meta"] = {
                **(chart.get("meta") or {}),
                **finnhub_metadata(symbol),
            }
    elif ACTIVE_MARKET_PROVIDER == "finnhub":
        chart = finnhub_chart(symbol, range_=range_)
    else:
        chart = yahoo_chart(symbol, range_=range_)
    closes = chart.get("closes") or []
    if not closes:
        return {}

    latest = closes[-1]
    return {
        "meta": chart.get("meta") or {},
        "closes": closes,
        "volumes": chart.get("volumes") or [],
        "dates": chart.get("dates") or [],
        "history": chart.get("history") or [],
        "latest": latest,
        "change_5d_pct": _pct(latest, closes[-6]) if len(closes) > 5 else None,
        "change_1w_pct": _pct(latest, closes[-6]) if len(closes) > 5 else None,
        "change_1m_pct": _pct(latest, closes[-22]) if len(closes) > 21 else None,
        "change_2m_pct": _pct(latest, closes[0]) if len(closes) > 1 else None,
    }


def score_themes(watchlist: dict, pause: float = 0.1) -> list[ThemeScore]:
    cache: dict[str, dict] = {}

    def etf_perf(symbol: str) -> dict:
        if symbol not in cache:
            cache[symbol] = history_perf(symbol, range_="3mo")
            time.sleep(pause)
        return cache[symbol]

    scores: list[ThemeScore] = []
    for theme in watchlist.get("themes", []):
        perfs = [etf_perf(symbol) for symbol in theme.get("sector_etfs", [])]
        w1 = [p["change_1w_pct"] for p in perfs if p.get("change_1w_pct") is not None]
        m1 = [p["change_1m_pct"] for p in perfs if p.get("change_1m_pct") is not None]
        avg_1w = statistics.mean(w1) if w1 else None
        avg_1m = statistics.mean(m1) if m1 else None

        raw = 50.0
        if avg_1w is not None:
            raw += max(-12, min(12, avg_1w * 2.0))
        if avg_1m is not None:
            raw += max(-16, min(16, avg_1m * 1.2))

        if avg_1w is not None and avg_1m is not None:
            if avg_1w > 0 and avg_1m < 0:
                raw += 8
                status = "Reversal"
            elif avg_1w > 0 and avg_1m > 0:
                raw += 6
                status = "Momentum"
            elif avg_1w < 0 and avg_1m < 0:
                raw -= 5
                status = "Weak"
            else:
                status = "Mixed"
        else:
            status = "Limited data"

        scores.append(
            ThemeScore(
                name=theme["name"],
                description=theme.get("description", ""),
                sector_etfs=theme.get("sector_etfs", []),
                score=round(max(0, min(100, raw)), 1),
                change_1w=round(avg_1w, 2) if avg_1w is not None else None,
                change_1m=round(avg_1m, 2) if avg_1m is not None else None,
                status=status,
            )
        )

    scores.sort(key=lambda r: r.score, reverse=True)
    return scores


def _info_value(info: dict, *keys: str) -> Any:
    for key in keys:
        value = info.get(key)
        if value not in (None, "", "None"):
            return value
    return None


def _setup_from(score: float, pct_above_low: float | None, pct_below_high: float | None,
                target_upside_pct: float | None, change_5d_pct: float | None) -> str:
    if target_upside_pct is not None and target_upside_pct >= 45:
        return "Target upside"
    if pct_above_low is not None and pct_above_low <= 25 and change_5d_pct is not None and change_5d_pct > 0:
        return "Oversold turn"
    if pct_below_high is not None and pct_below_high <= 12 and change_5d_pct is not None and change_5d_pct > 2:
        return "Breakout"
    if score >= 72:
        return "Strong setup"
    return "Watchlist"


def _risk_from(market_cap: float | None, beta: float | None, price: float | None,
               pct_above_low: float | None) -> str:
    risk_points = 0
    if market_cap is not None and market_cap < 2_000_000_000:
        risk_points += 2
    elif market_cap is not None and market_cap < 10_000_000_000:
        risk_points += 1
    if beta is not None and beta >= 2:
        risk_points += 2
    elif beta is not None and beta >= 1.4:
        risk_points += 1
    if price is not None and price < 5:
        risk_points += 2
    if pct_above_low is not None and pct_above_low <= 10:
        risk_points += 1
    if risk_points >= 4:
        return "Very high"
    if risk_points >= 2:
        return "High"
    return "Moderate"


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _black_scholes_call(spot: float, strike: float, days: int, volatility: float,
                        risk_free_rate: float = 0.045) -> float | None:
    if spot <= 0 or strike <= 0 or days <= 0 or volatility <= 0:
        return None
    t = days / 365.0
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * volatility * volatility) * t) / (
        volatility * math.sqrt(t)
    )
    d2 = d1 - volatility * math.sqrt(t)
    return spot * _norm_cdf(d1) - strike * math.exp(-risk_free_rate * t) * _norm_cdf(d2)


def _realized_volatility(closes: list[float], lookback: int = 60) -> float | None:
    clean = [v for v in closes if v and v > 0]
    if len(clean) < 10:
        return None
    window = clean[-lookback:]
    returns = [math.log(window[i] / window[i - 1]) for i in range(1, len(window))]
    if len(returns) < 5:
        return None
    return statistics.stdev(returns) * math.sqrt(252)


def _round_strike(price: float) -> float:
    if price < 10:
        step = 0.5
    elif price < 50:
        step = 1
    elif price < 200:
        step = 2.5
    else:
        step = 5
    return round(price / step) * step


def _spread_from_model(value: float | None) -> tuple[float | None, float | None]:
    if value is None:
        return None, None
    width = max(0.05, value * 0.08)
    return max(0.01, round(value - width, 2)), round(value + width, 2)


def score_company(symbol: str, universe_row: dict, theme_scores: dict[str, ThemeScore]) -> CompanyScore | None:
    perf = history_perf(symbol, range_="1y")
    meta = perf.get("meta") or {}
    closes = perf.get("closes") or []
    volumes = perf.get("volumes") or []

    price = _safe_float(meta.get("regularMarketPrice")) or perf.get("latest")
    previous_close = _safe_float(meta.get("chartPreviousClose"))
    if closes and len(closes) > 1:
        latest_close = _safe_float(closes[-1])
        if previous_close is None or (price is not None and latest_close is not None and abs(price - latest_close) < 0.01):
            previous_close = closes[-2]
    day_change_pct = _pct(price, previous_close)

    week_52_low = _safe_float(meta.get("fiftyTwoWeekLow")) or (min(closes) if closes else None)
    week_52_high = _safe_float(meta.get("fiftyTwoWeekHigh")) or (max(closes) if closes else None)
    pct_above_low = _pct(price, week_52_low)
    pct_below_high = _pct(week_52_high, price)

    target_mean_price = None
    target_upside_pct = _pct(target_mean_price, price)

    volume = _safe_float(meta.get("regularMarketVolume")) or (volumes[-1] if volumes else None)
    recent_volumes = volumes[-30:] if len(volumes) >= 5 else volumes
    avg_volume = statistics.mean(recent_volumes) if recent_volumes else None
    relative_volume = (volume / avg_volume) if volume is not None and avg_volume else None

    market_cap = _safe_float(meta.get("marketCap"))
    beta = None
    company_name = str(meta.get("longName") or meta.get("shortName") or symbol)
    sector = "Future Themes"
    industry = str(meta.get("industry") or ", ".join(universe_row["themes"][:2]))

    best_theme = max(
        universe_row["themes"],
        key=lambda name: theme_scores.get(name, ThemeScore(name, "", [], 50, None, None, "")).score,
    )
    theme_score = theme_scores.get(best_theme)

    score = 35.0
    signals: list[str] = []

    if theme_score:
        score += (theme_score.score - 50) * 0.35
        signals.append(f"{theme_score.status} theme")

    if pct_above_low is not None:
        if pct_above_low <= 15:
            score += 16
            signals.append("Near 52w low")
        elif pct_above_low <= 35:
            score += 10
            signals.append("Discounted range")
        elif pct_above_low <= 60:
            score += 4

    if pct_below_high is not None:
        if pct_below_high >= 40:
            score += 8
            signals.append("Well below high")
        elif pct_below_high >= 20:
            score += 4

    if target_upside_pct is not None:
        if target_upside_pct >= 60:
            score += 18
            signals.append("Large target upside")
        elif target_upside_pct >= 30:
            score += 12
            signals.append("Target upside")
        elif target_upside_pct >= 15:
            score += 6
        elif target_upside_pct < -5:
            score -= 8

    change_5d_pct = perf.get("change_5d_pct")
    change_1m_pct = perf.get("change_1m_pct")
    if change_5d_pct is not None:
        if change_5d_pct > 3:
            score += 8
            signals.append("5d strength")
        elif change_5d_pct > 0:
            score += 4
        elif change_5d_pct < -8:
            score -= 5

    if change_1m_pct is not None:
        if -20 <= change_1m_pct <= -3:
            score += 7
            signals.append("Pullback")
        elif change_1m_pct > 8:
            score += 6
            signals.append("1m momentum")
        elif change_1m_pct < -35:
            score -= 10
            signals.append("Falling knife risk")

    if relative_volume is not None:
        if relative_volume >= 2:
            score += 7
            signals.append("Volume spike")
        elif relative_volume >= 1.25:
            score += 4

    if market_cap is not None:
        if market_cap < 1_000_000_000:
            score -= 8
        elif market_cap < 5_000_000_000:
            score -= 3
        elif market_cap > 50_000_000_000:
            score += 2

    if price is not None and price < 2:
        score -= 12

    score = round(max(0, min(100, score)), 1)
    setup = _setup_from(score, pct_above_low, pct_below_high, target_upside_pct, change_5d_pct)
    risk = _risk_from(market_cap, beta, price, pct_above_low)

    if not signals:
        signals.append("Needs confirmation")

    return CompanyScore(
        symbol=symbol,
        company_name=company_name,
        theme=best_theme,
        sector=sector,
        industry=industry,
        price=round(price, 2) if price is not None else None,
        day_change_pct=round(day_change_pct, 2) if day_change_pct is not None else None,
        change_5d_pct=round(change_5d_pct, 2) if change_5d_pct is not None else None,
        change_1m_pct=round(change_1m_pct, 2) if change_1m_pct is not None else None,
        week_52_low=round(week_52_low, 2) if week_52_low is not None else None,
        week_52_high=round(week_52_high, 2) if week_52_high is not None else None,
        pct_above_low=round(pct_above_low, 1) if pct_above_low is not None else None,
        pct_below_high=round(pct_below_high, 1) if pct_below_high is not None else None,
        market_cap=market_cap,
        avg_volume=avg_volume,
        volume=volume,
        relative_volume=round(relative_volume, 2) if relative_volume is not None else None,
        target_mean_price=round(target_mean_price, 2) if target_mean_price is not None else None,
        target_upside_pct=round(target_upside_pct, 1) if target_upside_pct is not None else None,
        beta=round(beta, 2) if beta is not None else None,
        score=score,
        setup=setup,
        risk=risk,
        signals=signals[:5],
        option=OptionSnapshot(note="Not fetched"),
    )


def add_option_snapshot(company: CompanyScore) -> None:
    if company.price is None:
        company.option = OptionSnapshot(note="No stock price")
        return
    # Avoid a second market-data burst after the main scan, especially on Finnhub's
    # rolling free-tier limits. If we do not have fresh closes here, fall back to a
    # conservative default volatility below.
    closes = []
    if ACTIVE_MARKET_PROVIDER != "finnhub":
        perf = history_perf(company.symbol, range_="1y")
        closes = perf.get("closes") or []
    volatility = _realized_volatility(closes) or 0.65
    volatility = max(0.2, min(2.5, volatility))

    days = 45
    expiration = (datetime.now().date() + timedelta(days=days)).isoformat()
    atm_strike = _round_strike(company.price)
    otm_strike = _round_strike(company.price * 1.1)
    if otm_strike <= atm_strike:
        otm_strike = atm_strike + (0.5 if atm_strike < 10 else 1 if atm_strike < 50 else 2.5)

    atm_model = _black_scholes_call(company.price, atm_strike, days, volatility)
    otm_model = _black_scholes_call(company.price, otm_strike, days, volatility)
    atm_bid, atm_ask = _spread_from_model(atm_model)
    otm_bid, otm_ask = _spread_from_model(otm_model)
    breakeven = (otm_strike + otm_ask) if otm_ask is not None else None

    company.option = OptionSnapshot(
        expiration=expiration,
        atm_strike=atm_strike,
        atm_last=round(atm_model, 2) if atm_model is not None else None,
        atm_bid=atm_bid,
        atm_ask=atm_ask,
        atm_iv=round(volatility, 3),
        atm_open_interest=None,
        atm_volume=None,
        otm_strike=otm_strike,
        otm_last=round(otm_model, 2) if otm_model is not None else None,
        otm_bid=otm_bid,
        otm_ask=otm_ask,
        otm_iv=round(volatility, 3),
        otm_open_interest=None,
        otm_volume=None,
        breakeven=round(breakeven, 2) if breakeven is not None else None,
        breakeven_upside_pct=round(_pct(breakeven, company.price), 1) if breakeven is not None else None,
        note="Modeled from 60d realized volatility",
    )


def build_congress_watchlist(member_names: list[str], lookback_days: int = 365) -> dict:
    watched = [name.strip() for name in member_names if name.strip()]
    watched_lower = [name.lower() for name in watched]
    empty = {
        "lookback_days": lookback_days,
        "portfolio_lookback_days": 1095,
        "members": [
            {
                "name": name,
                "trade_count": 0,
                "purchase_count": 0,
                "sale_count": 0,
                "estimated_buy_max": 0,
                "estimated_sell_max": 0,
                "top_tickers": [],
                "inferred_portfolio": [],
                "recent_trades": [],
            }
            for name in watched
        ],
        "all_recent": [],
        "error": "",
    }

    if not watched:
        return empty

    try:
        data = fetch_recent_trades(lookback_days=lookback_days, basis="disclosure")
    except Exception as e:
        logger.warning("Congressional trade fetch failed: %s", e)
        empty["error"] = str(e)
        return empty

    trades = data.get("all_trades", [])
    member_rows = []
    all_recent = []

    for display_name, needle in zip(watched, watched_lower):
        aliases = CONGRESS_MEMBER_ALIASES.get(needle, [needle])
        matches = [
            trade for trade in trades
            if any(alias in (trade.get("member") or "").lower() for alias in aliases)
            and trade.get("ticker")
            and trade.get("ticker") != "—"
        ]
        matches.sort(key=lambda t: (t.get("disclosure_date") or "", t.get("amount_max") or 0), reverse=True)

        history_matches = matches
        politician_id = CAPITOL_TRADES_POLITICIAN_IDS.get(needle)
        if politician_id:
            try:
                member_history = fetch_member_trades(
                    politician_id=politician_id,
                    lookback_days=1095,
                    basis="disclosure",
                    max_pages=12,
                )
                history_matches = [
                    trade for trade in member_history.get("all_trades", [])
                    if trade.get("ticker") and trade.get("ticker") != "—"
                ]
            except Exception as e:
                logger.warning("Member trade history fetch failed for %s: %s", display_name, e)

        ticker_stats: dict[str, dict] = {}
        buy_max = 0.0
        sell_max = 0.0
        purchases = 0
        sales = 0

        for trade in history_matches:
            ticker = trade["ticker"]
            stats = ticker_stats.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "asset": trade.get("asset") or ticker,
                    "count": 0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "buy_max": 0.0,
                    "sell_max": 0.0,
                    "net_buy_max": 0.0,
                    "last_disclosure": "",
                    "last_transaction": "",
                },
            )
            stats["count"] += 1
            stats["last_disclosure"] = max(stats["last_disclosure"], trade.get("disclosure_date") or "")
            stats["last_transaction"] = max(stats["last_transaction"], trade.get("transaction_date") or "")
            amount = _safe_float(trade.get("amount_max")) or 0.0
            if trade.get("transaction") == "purchase":
                purchases += 1
                buy_max += amount
                stats["buy_count"] += 1
                stats["buy_max"] += amount
            elif trade.get("transaction") == "sale":
                sales += 1
                sell_max += amount
                stats["sell_count"] += 1
                stats["sell_max"] += amount
            stats["net_buy_max"] = stats["buy_max"] - stats["sell_max"]

        top_tickers = sorted(
            ticker_stats.values(),
            key=lambda r: (r["buy_max"] + r["sell_max"], r["count"], r["last_disclosure"]),
            reverse=True,
        )[:8]
        inferred_portfolio = sorted(
            [row for row in ticker_stats.values() if row["net_buy_max"] > 0],
            key=lambda r: (r["net_buy_max"], r["last_disclosure"]),
            reverse=True,
        )[:8]

        recent = matches[:10]
        all_recent.extend({**trade, "tracked_member": display_name} for trade in recent[:5])
        member_rows.append(
            {
                "name": display_name,
                "trade_count": len(history_matches),
                "recent_trade_count": len(matches),
                "purchase_count": purchases,
                "sale_count": sales,
                "estimated_buy_max": buy_max,
                "estimated_sell_max": sell_max,
                "top_tickers": top_tickers,
                "inferred_portfolio": inferred_portfolio,
                "recent_trades": recent,
            }
        )

    all_recent.sort(key=lambda t: (t.get("disclosure_date") or "", t.get("amount_max") or 0), reverse=True)
    return {
        "lookback_days": lookback_days,
        "portfolio_lookback_days": 1095,
        "members": member_rows,
        "all_recent": all_recent[:20],
        "error": "",
    }


def fetch_company_news(symbol: str, lookback_days: int = 30, limit: int = 10) -> list[dict[str, Any]]:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return []
    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=lookback_days)
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/company-news",
            params={
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
                "token": api_key,
            },
            timeout=12,
        )
        resp.raise_for_status()
        articles = resp.json()
    except Exception as e:
        logger.warning("Company news fetch failed for %s: %s", symbol, e)
        return []

    cleaned = []
    for article in articles if isinstance(articles, list) else []:
        headline = article.get("headline") or ""
        url = article.get("url") or ""
        if not headline or not url:
            continue
        cleaned.append(
            {
                "headline": headline,
                "summary": article.get("summary") or "",
                "source": article.get("source") or "News",
                "url": url,
                "datetime": _safe_int(article.get("datetime")) or 0,
                "date": _iso_from_ts(article.get("datetime")),
            }
        )
    cleaned.sort(key=lambda row: row.get("datetime") or 0, reverse=True)
    return cleaned[:limit]


def fetch_earnings_events(symbol: str, quarters: int = 4) -> list[dict[str, Any]]:
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        return []
    to_date = datetime.now().date() + timedelta(days=3)
    from_date = to_date - timedelta(days=540)
    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/calendar/earnings",
            params={
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
                "token": api_key,
            },
            timeout=12,
        )
        resp.raise_for_status()
        rows = (resp.json() or {}).get("earningsCalendar") or []
    except Exception as e:
        logger.warning("Earnings calendar fetch failed for %s: %s", symbol, e)
        return []

    events = []
    for row in rows:
        report_date = row.get("date")
        if not report_date:
            continue
        events.append(
            {
                "date": report_date,
                "quarter": row.get("quarter") or "",
                "year": row.get("year") or "",
                "eps_actual": _safe_float(row.get("epsActual")),
                "eps_estimate": _safe_float(row.get("epsEstimate")),
                "revenue_actual": _safe_float(row.get("revenueActual")),
                "revenue_estimate": _safe_float(row.get("revenueEstimate")),
                "hour": row.get("hour") or "",
            }
        )
    events.sort(key=lambda row: row["date"], reverse=True)
    return events[:quarters]


def _history_window(history: list[dict[str, Any]], months: int = 6) -> list[dict[str, Any]]:
    if not history:
        return []
    cutoff = datetime.now().date() - timedelta(days=months * 31)
    rows = []
    for row in history:
        date_text = row.get("date") or ""
        try:
            row_date = datetime.fromisoformat(date_text).date()
        except ValueError:
            continue
        if row_date >= cutoff:
            rows.append(
                {
                    "date": date_text,
                    "close": round(row["close"], 2) if row.get("close") is not None else None,
                    "volume": row.get("volume"),
                }
            )
    return rows


def _close_around(history: list[dict[str, Any]], event_date: str, offset: int) -> tuple[str, float | None]:
    dated = [(row.get("date") or "", _safe_float(row.get("close"))) for row in history if row.get("date")]
    dated = [(date, close) for date, close in dated if close is not None]
    if not dated:
        return "", None
    dates = [date for date, _ in dated]
    idx = 0
    for i, date_text in enumerate(dates):
        if date_text <= event_date:
            idx = i
        if date_text > event_date:
            break
    target = max(0, min(len(dated) - 1, idx + offset))
    return dated[target]


def enrich_earnings_reactions(
    events: list[dict[str, Any]],
    history: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched = []
    for event in events:
        date = event["date"]
        pre5_date, pre5 = _close_around(history, date, -5)
        prev_date, prev = _close_around(history, date, -1)
        next_date, nxt = _close_around(history, date, 1)
        post5_date, post5 = _close_around(history, date, 5)
        enriched.append(
            {
                **event,
                "pre_5d_date": pre5_date,
                "pre_5d_close": pre5,
                "prev_close_date": prev_date,
                "prev_close": prev,
                "next_close_date": next_date,
                "next_close": nxt,
                "post_5d_date": post5_date,
                "post_5d_close": post5,
                "pre_5d_pct": round(_pct(prev, pre5), 2) if pre5 is not None and prev is not None else None,
                "next_day_pct": round(_pct(nxt, prev), 2) if prev is not None and nxt is not None else None,
                "post_5d_pct": round(_pct(post5, prev), 2) if prev is not None and post5 is not None else None,
            }
        )
    return enriched


def build_company_detail(company: CompanyScore, pause: float = 0.15) -> dict[str, Any]:
    perf = history_perf(company.symbol, range_="1y")
    history = _history_window(perf.get("history") or [], months=6)
    time.sleep(pause)
    news = fetch_company_news(company.symbol)
    time.sleep(pause)
    earnings = enrich_earnings_reactions(fetch_earnings_events(company.symbol), perf.get("history") or [])
    return {
        "symbol": company.symbol,
        "company_name": company.company_name,
        "theme": company.theme,
        "price": company.price,
        "score": company.score,
        "setup": company.setup,
        "risk": company.risk,
        "history": history,
        "news": news,
        "earnings_reactions": earnings,
        "detail_path": f"details/{_detail_filename(company.symbol)}",
    }


def parse_display_limit(value: Any, default: int | None = 50) -> int | None:
    if value is None or value == "":
        return default
    if isinstance(value, str) and value.strip().lower() in {"all", "none", "null", "*"}:
        return None
    parsed = int(value)
    if parsed < 1:
        raise ValueError("display limit must be at least 1, or 'all'")
    return parsed


def build_report(
    watchlist_path: str,
    display_limit: int | None,
    scan_limit: int | None,
    skip_options: bool,
    skip_detail_pages: bool,
    pause: float,
    congress_members: list[str],
    congress_lookback_days: int,
    market_provider: str,
) -> dict:
    global ACTIVE_MARKET_PROVIDER

    ACTIVE_MARKET_PROVIDER = resolve_market_provider(market_provider)
    logger.info("Using %s market data provider", ACTIVE_MARKET_PROVIDER)
    validate_market_provider(ACTIVE_MARKET_PROVIDER)

    watchlist = load_watchlist(watchlist_path)
    universe = flatten_universe(watchlist)
    universe_items = sorted(universe.items())
    total_symbols = len(universe_items)
    if scan_limit is not None:
        universe_items = universe_items[:scan_limit]

    logger.info("Scoring %d themes...", len(watchlist.get("themes", [])))
    themes = score_themes(watchlist, pause=pause)
    theme_map = {theme.name: theme for theme in themes}

    logger.info("Scoring %d companies...", len(universe_items))
    companies: list[CompanyScore] = []
    for idx, (symbol, row) in enumerate(universe_items, start=1):
        logger.info("[%d/%d] %s", idx, len(universe_items), symbol)
        scored = score_company(symbol, row, theme_map)
        if scored and scored.price is not None:
            companies.append(scored)
        time.sleep(pause)

    companies.sort(key=lambda r: r.score, reverse=True)
    top_companies = companies if display_limit is None else companies[:display_limit]

    if not skip_options:
        logger.info("Fetching option snapshots for top %d...", len(top_companies))
        for idx, company in enumerate(top_companies, start=1):
            logger.info("[options %d/%d] %s", idx, len(top_companies), company.symbol)
            add_option_snapshot(company)
            time.sleep(pause)

    logger.info("Building congressional watchlist section...")
    congress = build_congress_watchlist(
        member_names=congress_members,
        lookback_days=congress_lookback_days,
    )

    company_details = []
    if not skip_detail_pages:
        logger.info("Building detail pages for top %d...", len(top_companies))
        for idx, company in enumerate(top_companies, start=1):
            logger.info("[detail %d/%d] %s", idx, len(top_companies), company.symbol)
            detail = build_company_detail(company, pause=pause)
            company_details.append(detail)
            time.sleep(pause)

    now = datetime.now(REPORT_TIMEZONE)
    return {
        "generated_at": now.isoformat(),
        "generated_at_display": now.strftime("%B %d, %Y %I:%M %p %Z"),
        "watchlist_path": watchlist_path,
        "total_symbols": total_symbols,
        "scanned_symbols": len(universe_items),
        "scan_limit": scan_limit,
        "display_limit": display_limit if display_limit is not None else "all",
        "limit": display_limit if display_limit is not None else "all",
        "themes": [asdict(theme) for theme in themes],
        "companies": [asdict(company) for company in top_companies],
        "company_details": company_details,
        "congress": congress,
        "notes": [
            "Scores are a screening aid, not investment advice.",
            f"Stock prices and history come from the configured {ACTIVE_MARKET_PROVIDER} market data provider.",
            "For strongest historical scoring, use Polygon daily OHLCV with POLYGON_API_KEY.",
            "Options are modeled 45-day call estimates using Black-Scholes and 60-day realized volatility; they are not live bid/ask quotes.",
            "Congressional trades are public disclosures and can lag actual transaction dates.",
        ],
    }


def _theme_card(theme: dict) -> str:
    return f"""
      <div class="theme-card">
        <div class="theme-score">{theme["score"]:.1f}</div>
        <div>
          <h3>{theme["name"]}</h3>
          <p>{theme["description"]}</p>
          <div class="meta">
            <span>{theme["status"]}</span>
            <span>1W {_fmt_pct(theme["change_1w"], signed=True)}</span>
            <span>1M {_fmt_pct(theme["change_1m"], signed=True)}</span>
            <span>{", ".join(theme["sector_etfs"])}</span>
          </div>
        </div>
      </div>
    """


def _company_row(row: dict, rank: int) -> str:
    option = row.get("option") or {}
    signals = "".join(f"<span>{_esc(signal)}</span>" for signal in row.get("signals", []))
    option_text = "-"
    if option.get("expiration"):
        option_text = (
            f'{option.get("expiration")} model<br>'
            f'ATM ${_fmt_num(option.get("atm_strike"))} '
            f'({_fmt_num(option.get("atm_bid"))}/{_fmt_num(option.get("atm_ask"))})<br>'
            f'OTM ${_fmt_num(option.get("otm_strike"))} '
            f'BE {_fmt_pct(option.get("breakeven_upside_pct"))}'
        )
    elif option.get("note"):
        option_text = option["note"]

    risk_cls = "risk-high" if row["risk"] in ("High", "Very high") else "risk-med"
    detail_path = row.get("detail_path")
    symbol_html = _esc(row["symbol"])
    company_html = _esc(row["company_name"])
    if detail_path:
        company_cell = (
            f'<a class="company-link" href="{_esc(detail_path)}"><strong>{symbol_html}</strong>'
            f'<div class="sub">{company_html}</div></a>'
        )
    else:
        company_cell = f'<strong>{symbol_html}</strong><div class="sub">{company_html}</div>'

    return f"""
      <tr>
        <td class="rank">{rank}</td>
        <td>{company_cell}</td>
        <td>
          {_esc(row["theme"])}
          <div class="sub">{_esc(row["industry"])}</div>
        </td>
        <td class="num score">{row["score"]:.1f}</td>
        <td>{_esc(row["setup"])}<div class="chips">{signals}</div></td>
        <td class="{risk_cls}">{row["risk"]}</td>
        <td class="num">${_fmt_num(row["price"])}</td>
        {_cell_pct(row["day_change_pct"])}
        {_cell_pct(row["change_5d_pct"])}
        {_cell_pct(row["change_1m_pct"])}
        <td class="num">{_fmt_pct(row["pct_above_low"])}</td>
        <td class="num">{_fmt_pct(row["pct_below_high"])}</td>
        <td class="num">{_fmt_pct(row["target_upside_pct"], signed=True)}</td>
        <td class="num">{_fmt_num(row["relative_volume"])}</td>
        <td class="num">{_fmt_money(row["market_cap"])}</td>
        <td class="option">{option_text}</td>
      </tr>
    """


def _tx_badge(transaction: str) -> str:
    label = {
        "purchase": "BUY",
        "sale": "SELL",
        "exchange": "EXCH",
    }.get(transaction, (transaction or "OTHER").upper())
    cls = {
        "purchase": "tx-buy",
        "sale": "tx-sell",
        "exchange": "tx-other",
    }.get(transaction, "tx-other")
    return f'<span class="tx {cls}">{label}</span>'


def _congress_member_card(member: dict) -> str:
    tickers = member.get("top_tickers") or []
    ticker_html = "".join(
        f'<span>{row["ticker"]} {row["count"]}x</span>'
        for row in tickers[:6]
    ) or '<span>No matching disclosures</span>'
    portfolio = member.get("inferred_portfolio") or []
    portfolio_html = "".join(
        f'<span title="{_esc(row.get("asset") or row["ticker"])}">{row["ticker"]} {_fmt_money(row.get("net_buy_max"))}</span>'
        for row in portfolio[:6]
    ) or '<span>No net disclosed buys</span>'

    return f"""
      <div class="member-card">
        <h3>{member["name"]}</h3>
        <div class="member-stats">
          <span>{member["trade_count"]} trades</span>
          <span>{member["purchase_count"]} buys</span>
          <span>{member["sale_count"]} sells</span>
        </div>
        <div class="member-money">
          <div><strong>{_fmt_money(member["estimated_buy_max"])}</strong><span>max disclosed buys</span></div>
          <div><strong>{_fmt_money(member["estimated_sell_max"])}</strong><span>max disclosed sells</span></div>
        </div>
        <div class="mini-label">Inferred disclosed portfolio</div>
        <div class="chips portfolio-chips">{portfolio_html}</div>
        <div class="mini-label">Most active disclosed tickers</div>
        <div class="chips">{ticker_html}</div>
      </div>
    """


def _congress_trade_row(trade: dict) -> str:
    return f"""
      <tr>
        <td><strong>{trade.get("tracked_member") or trade.get("member") or "-"}</strong><div class="sub">{trade.get("chamber") or ""} {trade.get("state") or ""}</div></td>
        <td><strong>{trade.get("ticker") or "-"}</strong><div class="sub">{trade.get("asset") or ""}</div></td>
        <td>{_tx_badge(trade.get("transaction") or "")}</td>
        <td class="num">{_fmt_range(trade.get("amount_min"), trade.get("amount_max"), trade.get("amount_range") or "-")}</td>
        <td>{trade.get("transaction_date") or "-"}</td>
        <td>{trade.get("disclosure_date") or "-"}<div class="sub">{trade.get("days_to_disclose")}d delay</div></td>
      </tr>
    """


def _render_congress_section(congress: dict) -> str:
    error = congress.get("error")
    member_cards = "\n".join(_congress_member_card(member) for member in congress.get("members", []))
    rows = "\n".join(_congress_trade_row(trade) for trade in congress.get("all_recent", []))
    if not rows:
        msg = f"Congressional feed unavailable: {error}" if error else "No matching congressional disclosures found."
        rows = f'<tr><td colspan="6" class="muted" style="text-align:center;padding:18px;">{msg}</td></tr>'

    return f"""
    <section>
      <h2>Congressional Watchlist</h2>
      <p class="section-copy">
        Recent table uses disclosures over the last {congress.get("lookback_days", 365)} days.
        Card portfolios are inferred from up to {congress.get("portfolio_lookback_days", 1095)} days of public trade disclosures;
        they are not guaranteed current holdings.
      </p>
      <div class="members">{member_cards}</div>
      <h2 class="subhead">Recent Tracked Trades</h2>
      <div class="table-scroll compact">
        <table>
          <thead>
            <tr>
              <th>Member</th>
              <th>Ticker</th>
              <th>Action</th>
              <th>Size</th>
              <th>Traded</th>
              <th>Disclosed</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>
    """


def render_html(report: dict) -> str:
    showing_label = (
        f"Showing all {len(report['companies'])}"
        if report.get("display_limit") == "all"
        else f"Showing top {len(report['companies'])}"
    )
    theme_cards = "\n".join(_theme_card(theme) for theme in report["themes"])
    detail_paths = {
        detail["symbol"]: detail.get("detail_path")
        for detail in report.get("company_details", [])
        if detail.get("symbol") and detail.get("detail_path")
    }
    company_rows = []
    for row in report["companies"]:
        row = dict(row)
        row["detail_path"] = detail_paths.get(row["symbol"])
        company_rows.append(row)
    rows = "\n".join(_company_row(row, idx) for idx, row in enumerate(company_rows, start=1))
    congress_section = _render_congress_section(report.get("congress") or {})
    best = report["companies"][:5]
    best_items = "".join(
        f'<li><strong>{row["symbol"]}</strong> {row["setup"]}, score {row["score"]:.1f}, '
        f'target upside {_fmt_pct(row["target_upside_pct"], signed=True)}</li>'
        for row in best
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Future Upside Watchlist - {report["generated_at_display"]}</title>
  <style>
    :root {{
      --bg: #f5f7fa;
      --panel: #ffffff;
      --ink: #162033;
      --muted: #667085;
      --line: #d9e0ea;
      --green: #147a4b;
      --red: #b42318;
      --amber: #9a6700;
      --blue: #2457a6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }}
    .wrap {{ max-width: 1480px; margin: 0 auto; padding: 24px; }}
    header {{
      background: #111827;
      color: white;
      padding: 22px 26px;
      border-radius: 8px 8px 0 0;
    }}
    h1 {{ margin: 0; font-size: 26px; letter-spacing: 0; }}
    header p {{ margin: 6px 0 0; color: #c7d2fe; }}
    .header-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }}
    .header-meta span {{
      border: 1px solid rgba(199, 210, 254, .35);
      border-radius: 999px;
      padding: 4px 10px;
      color: #e0e7ff;
      background: rgba(255, 255, 255, .08);
      font-size: 12px;
      white-space: nowrap;
    }}
    section {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-top: 0;
      padding: 22px 24px;
    }}
    section + section {{ border-top: 1px solid var(--line); }}
    h2 {{ margin: 0 0 14px; font-size: 18px; }}
    .summary {{
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 18px;
    }}
    .note {{
      background: #eef4ff;
      border-left: 4px solid var(--blue);
      padding: 12px 14px;
      border-radius: 4px;
    }}
    .note ul {{ margin: 0; padding-left: 18px; }}
    .themes {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 12px;
    }}
    .members {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .theme-card {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      display: grid;
      grid-template-columns: 72px 1fr;
      gap: 14px;
      min-height: 126px;
    }}
    .member-card {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-height: 162px;
    }}
    .theme-score {{
      align-self: start;
      justify-self: start;
      background: #e8f1ff;
      color: #174ea6;
      border: 1px solid #bed7ff;
      border-radius: 6px;
      padding: 10px 8px;
      min-width: 62px;
      text-align: center;
      font-size: 20px;
      font-weight: 750;
      font-variant-numeric: tabular-nums;
    }}
    .theme-card h3 {{ margin: 0 0 4px; font-size: 15px; }}
    .member-card h3 {{ margin: 0 0 8px; font-size: 15px; }}
    .theme-card p {{ margin: 0 0 9px; color: var(--muted); }}
    .section-copy {{ margin: -4px 0 14px; color: var(--muted); }}
    .subhead {{ margin-top: 18px; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .member-stats {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }}
    .member-stats span {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 7px;
      color: #344054;
      background: #f8fafc;
      font-size: 12px;
    }}
    .member-money {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-bottom: 10px;
    }}
    .member-money div {{
      border-left: 3px solid #d0d5dd;
      padding-left: 8px;
    }}
    .member-money strong {{ display: block; font-variant-numeric: tabular-nums; }}
    .member-money span {{ display: block; color: var(--muted); font-size: 12px; }}
    .mini-label {{
      margin: 9px 0 5px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 750;
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .portfolio-chips span {{ border-color: #b7dfc5; background: #ecfdf3; color: #067647; }}
    .meta span, .chips span {{
      display: inline-block;
      border: 1px solid var(--line);
      background: #f8fafc;
      color: #344054;
      border-radius: 999px;
      padding: 2px 7px;
      font-size: 12px;
      white-space: nowrap;
    }}
    .table-scroll {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }}
    a.company-link {{ color: inherit; text-decoration: none; display: block; }}
    a.company-link strong {{ color: #174ea6; text-decoration: underline; text-underline-offset: 2px; }}
    a.company-link:hover strong {{ color: #0b3a7a; }}
    .table-scroll.compact table {{ min-width: 880px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1380px; background: white; }}
    th {{
      position: sticky;
      top: 0;
      background: #f8fafc;
      color: #475467;
      text-align: left;
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .02em;
    }}
    td {{ padding: 10px; border-bottom: 1px solid #edf1f6; vertical-align: top; }}
    tr:hover {{ background: #fbfdff; }}
    .rank {{ color: var(--muted); width: 44px; text-align: right; }}
    .sub {{ color: var(--muted); font-size: 12px; margin-top: 2px; max-width: 260px; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
    .score {{ color: #174ea6; font-weight: 750; }}
    .pos {{ color: var(--green); }}
    .neg {{ color: var(--red); }}
    .muted {{ color: var(--muted); }}
    .risk-high {{ color: var(--red); font-weight: 650; white-space: nowrap; }}
    .risk-med {{ color: var(--amber); font-weight: 650; white-space: nowrap; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 5px; max-width: 240px; }}
    .option {{ min-width: 170px; color: #344054; font-size: 12px; }}
    .tx {{
      display: inline-block;
      border-radius: 999px;
      color: white;
      padding: 2px 8px;
      font-size: 11px;
      font-weight: 750;
    }}
    .tx-buy {{ background: var(--green); }}
    .tx-sell {{ background: var(--red); }}
    .tx-other {{ background: #667085; }}
    footer {{
      background: #ffffff;
      color: var(--muted);
      border: 1px solid var(--line);
      border-top: 0;
      border-radius: 0 0 8px 8px;
      padding: 16px 24px;
      font-size: 12px;
    }}
    @media (max-width: 760px) {{
      .wrap {{ padding: 12px; }}
      header, section, footer {{ padding-left: 16px; padding-right: 16px; }}
      .summary {{ grid-template-columns: 1fr; }}
      .theme-card {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <h1>Future Upside Watchlist</h1>
      <p>Generated {report["generated_at_display"]}</p>
      <div class="header-meta">
        <span>Scanned {report["scanned_symbols"]} of {report.get("total_symbols", report["scanned_symbols"])} symbols</span>
        <span>{showing_label}</span>
        <span>Provider {ACTIVE_MARKET_PROVIDER}</span>
      </div>
    </header>

    <section>
      <div class="summary">
        <div>
          <h2>Best Setups</h2>
          <ul>{best_items}</ul>
        </div>
        <div class="note">
          <ul>
            <li>Scores favor future-facing themes, discounted prices, target upside, volume, and improving momentum.</li>
            <li>Options are modeled calls near the money and roughly 10% out of the money, not live chain quotes.</li>
            <li>This is a screening report only, not investment advice.</li>
          </ul>
        </div>
      </div>
    </section>

    <section>
      <h2>Theme Ranking</h2>
      <div class="themes">{theme_cards}</div>
    </section>

    <section>
      <h2>Top Companies</h2>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Company</th>
              <th>Theme</th>
              <th>Score</th>
              <th>Setup</th>
              <th>Risk</th>
              <th>Price</th>
              <th>1D</th>
              <th>5D</th>
              <th>1M</th>
              <th>Above Low</th>
              <th>Below High</th>
              <th>Target Upside</th>
              <th>Rel Vol</th>
              <th>Market Cap</th>
              <th>Options Snapshot</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>

    {congress_section}

    <footer>
      Sources: configured market-data provider for price history, Finnhub when available for news/profile/earnings, and public congressional disclosures.
      Option values are model estimates using historical volatility, not live quotes.
      Review liquidity, bid/ask spreads, company filings, and your risk limits before trading.
    </footer>
  </div>
</body>
</html>"""


def _history_svg(history: list[dict[str, Any]]) -> str:
    points = [(row.get("date") or "", _safe_float(row.get("close"))) for row in history]
    points = [(date, close) for date, close in points if close is not None]
    if len(points) < 2:
        return '<div class="empty">Price history unavailable.</div>'
    if len(points) < 20:
        return (
            '<div class="empty">Only quote-level price data was available for this symbol '
            f'({len(points)} points). Daily candles were not returned by the configured data source.</div>'
        )

    width = 900
    height = 320
    pad_x = 48
    pad_y = 32
    closes = [close for _, close in points]
    lo = min(closes)
    hi = max(closes)
    span = hi - lo or 1.0

    def xy(idx: int, close: float) -> tuple[float, float]:
        x = pad_x + idx * ((width - pad_x * 2) / (len(points) - 1))
        y = pad_y + (hi - close) * ((height - pad_y * 2) / span)
        return x, y

    line = " ".join(f"{x:.1f},{y:.1f}" for idx, (_, close) in enumerate(points) for x, y in [xy(idx, close)])
    area = f"{pad_x},{height - pad_y} {line} {width - pad_x},{height - pad_y}"
    first_date, first_close = points[0]
    last_date, last_close = points[-1]
    change = _pct(last_close, first_close)
    change_cls = "#147a4b" if (change or 0) >= 0 else "#b42318"
    return f"""
      <svg class="price-chart" viewBox="0 0 {width} {height}" role="img" aria-label="Six month price history">
        <line x1="{pad_x}" y1="{height - pad_y}" x2="{width - pad_x}" y2="{height - pad_y}" class="axis"/>
        <line x1="{pad_x}" y1="{pad_y}" x2="{pad_x}" y2="{height - pad_y}" class="axis"/>
        <polygon points="{area}" class="area"/>
        <polyline points="{line}" class="line"/>
        <text x="{pad_x}" y="22" class="chart-label">{_esc(first_date)} ${_fmt_num(first_close)}</text>
        <text x="{width - pad_x}" y="22" text-anchor="end" class="chart-label">{_esc(last_date)} ${_fmt_num(last_close)}</text>
        <text x="{width - pad_x}" y="{height - 8}" text-anchor="end" fill="{change_cls}" class="chart-change">6M {_fmt_pct(change, signed=True)}</text>
        <text x="{pad_x}" y="{height - 8}" class="chart-label">Range ${_fmt_num(lo)} - ${_fmt_num(hi)}</text>
      </svg>
    """


def _news_item(article: dict) -> str:
    summary = article.get("summary") or ""
    if len(summary) > 220:
        summary = summary[:217].rstrip() + "..."
    return f"""
      <a class="news-item" href="{_esc(article.get("url"))}" target="_blank" rel="noopener">
        <div class="news-date">{_esc(article.get("date") or "")} · {_esc(article.get("source") or "News")}</div>
        <strong>{_esc(article.get("headline") or "")}</strong>
        <p>{_esc(summary)}</p>
      </a>
    """


def _earnings_row(row: dict) -> str:
    eps_surprise = None
    if row.get("eps_actual") is not None and row.get("eps_estimate") not in (None, 0):
        eps_surprise = _pct(row.get("eps_actual"), row.get("eps_estimate"))
    return f"""
      <tr>
        <td><strong>{_esc(row.get("date"))}</strong><div class="sub">{_esc(row.get("hour") or "")}</div></td>
        <td>{_esc(str(row.get("quarter") or ""))} {_esc(str(row.get("year") or ""))}</td>
        <td class="num">{_fmt_num(row.get("eps_actual"))}</td>
        <td class="num">{_fmt_num(row.get("eps_estimate"))}</td>
        <td class="num">{_fmt_pct(eps_surprise, signed=True)}</td>
        {_cell_pct(row.get("pre_5d_pct"))}
        {_cell_pct(row.get("next_day_pct"))}
        {_cell_pct(row.get("post_5d_pct"))}
      </tr>
    """


def render_company_detail_html(detail: dict, generated_at_display: str) -> str:
    history = detail.get("history") or []
    news = detail.get("news") or []
    earnings = detail.get("earnings_reactions") or []
    news_html = "\n".join(_news_item(article) for article in news[:10])
    if not news_html:
        news_html = '<div class="empty">No recent company news returned by the provider.</div>'
    earnings_rows = "\n".join(_earnings_row(row) for row in earnings)
    if not earnings_rows:
        earnings_rows = '<tr><td colspan="8" class="empty">No recent earnings calendar data returned by the provider.</td></tr>'

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_esc(detail["symbol"])} Detail - Future Upside Watchlist</title>
  <style>
    :root {{
      --bg: #f5f7fa;
      --panel: #ffffff;
      --ink: #162033;
      --muted: #667085;
      --line: #d9e0ea;
      --green: #147a4b;
      --red: #b42318;
      --blue: #2457a6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 24px; }}
    header {{
      background: #111827;
      color: white;
      padding: 22px 26px;
      border-radius: 8px 8px 0 0;
    }}
    header a {{ color: #c7d2fe; text-decoration: none; }}
    h1 {{ margin: 5px 0 0; font-size: 26px; letter-spacing: 0; }}
    header p {{ margin: 6px 0 0; color: #c7d2fe; }}
    .grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.45fr) minmax(320px, .85fr);
      gap: 18px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-top: 0;
      padding: 22px 24px;
    }}
    .panel {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      background: white;
    }}
    .panel + .panel {{ margin-top: 18px; }}
    h2 {{ margin: 0 0 12px; font-size: 18px; }}
    .stats {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .stats span {{
      border: 1px solid var(--line);
      background: #f8fafc;
      border-radius: 999px;
      padding: 4px 9px;
      color: #344054;
      font-size: 12px;
    }}
    .price-chart {{ display: block; width: 100%; height: auto; min-height: 280px; }}
    .axis {{ stroke: #d0d5dd; stroke-width: 1; }}
    .area {{ fill: #e8f1ff; opacity: .75; }}
    .line {{ fill: none; stroke: var(--blue); stroke-width: 3; stroke-linejoin: round; stroke-linecap: round; }}
    .chart-label {{ fill: var(--muted); font-size: 13px; }}
    .chart-change {{ font-size: 14px; font-weight: 750; }}
    .news-list {{ display: grid; gap: 10px; }}
    .news-item {{
      display: block;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      color: inherit;
      text-decoration: none;
      background: #fbfdff;
    }}
    .news-item:hover strong {{ color: var(--blue); text-decoration: underline; text-underline-offset: 2px; }}
    .news-date {{ color: var(--muted); font-size: 12px; margin-bottom: 4px; }}
    .news-item p {{ margin: 5px 0 0; color: var(--muted); font-size: 13px; }}
    .table-scroll {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 820px; }}
    th {{
      background: #f8fafc;
      color: #475467;
      text-align: left;
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .02em;
    }}
    td {{ padding: 10px; border-bottom: 1px solid #edf1f6; vertical-align: top; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
    .pos {{ color: var(--green); }}
    .neg {{ color: var(--red); }}
    .sub {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
    .empty {{ color: var(--muted); padding: 14px; text-align: center; }}
    footer {{
      background: #ffffff;
      color: var(--muted);
      border: 1px solid var(--line);
      border-top: 0;
      border-radius: 0 0 8px 8px;
      padding: 16px 24px;
      font-size: 12px;
    }}
    @media (max-width: 920px) {{
      .wrap {{ padding: 12px; }}
      .grid {{ grid-template-columns: 1fr; padding: 16px; }}
      header, footer {{ padding-left: 16px; padding-right: 16px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <a href="../future_upside_{datetime.now().strftime("%Y%m%d")}.html">Back to watchlist</a>
      <h1>{_esc(detail["symbol"])} - {_esc(detail["company_name"])}</h1>
      <p>{_esc(detail["theme"])} · ${_fmt_num(detail.get("price"))} · score {_fmt_num(detail.get("score"), 1)} · {_esc(detail.get("setup"))} · {_esc(detail.get("risk"))} risk</p>
    </header>
    <main class="grid">
      <div>
        <section class="panel">
          <h2>Six-Month Price History</h2>
          {_history_svg(history)}
          <div class="stats">
            <span>{len(history)} trading points</span>
            <span>Generated {generated_at_display}</span>
          </div>
        </section>
        <section class="panel">
          <h2>Earnings Reaction - Last Four Quarters</h2>
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Quarter</th>
                  <th>EPS</th>
                  <th>Estimate</th>
                  <th>Surprise</th>
                  <th>Pre 5D</th>
                  <th>Next Day</th>
                  <th>Post 5D</th>
                </tr>
              </thead>
              <tbody>{earnings_rows}</tbody>
            </table>
          </div>
        </section>
      </div>
      <aside>
        <section class="panel">
          <h2>Latest News</h2>
          <div class="news-list">{news_html}</div>
        </section>
      </aside>
    </main>
    <footer>
      News and earnings calendar data use Finnhub when available. Earnings reaction percentages compare closes before and after reported earnings dates and are a research aid, not a forecast.
    </footer>
  </div>
</body>
</html>"""


def write_outputs(report: dict, out_dir: str) -> tuple[Path, Path]:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    details_path = path / "details"
    details_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    html_path = path / f"future_upside_{stamp}.html"
    json_path = path / f"future_upside_{stamp}.json"
    html_path.write_text(render_html(report), encoding="utf-8")
    for detail in report.get("company_details", []):
        detail_file = details_path / _detail_filename(detail["symbol"])
        detail_file.write_text(
            render_company_detail_html(detail, report["generated_at_display"]),
            encoding="utf-8",
        )
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return html_path, json_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Future Upside Watchlist HTML report")
    parser.add_argument("--watchlist", default=DEFAULT_WATCHLIST, help="Path to future_watchlist.json")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory")
    parser.add_argument(
        "--display-limit",
        "--limit",
        dest="display_limit",
        type=parse_display_limit,
        default=50,
        help="Number of ranked companies to show, or 'all'. --limit is kept as a backward-compatible alias.",
    )
    parser.add_argument(
        "--scan-limit",
        type=int,
        default=None,
        help="Maximum number of watchlist symbols to fetch and score before ranking.",
    )
    parser.add_argument("--skip-options", action="store_true", help="Skip option chain fetches")
    parser.add_argument("--skip-detail-pages", action="store_true", help="Skip per-company detail pages")
    parser.add_argument("--pause", type=float, default=0.15, help="Pause between market-data calls")
    parser.add_argument(
        "--market-provider",
        choices=["auto", "polygon", "finnhub", "yahoo"],
        default="auto",
        help="Market data source. auto uses Polygon when POLYGON_API_KEY is set, then Finnhub, otherwise Yahoo.",
    )
    parser.add_argument("--dry", action="store_true", help="Alias for local output mode")
    parser.add_argument(
        "--congress-members",
        default=",".join(DEFAULT_CONGRESS_MEMBERS),
        help="Comma-separated congressional members to track",
    )
    parser.add_argument(
        "--congress-lookback-days",
        type=int,
        default=365,
        help="Disclosure lookback window for congressional watchlist",
    )
    args = parser.parse_args(argv)

    if args.scan_limit is not None and args.scan_limit < 1:
        raise SystemExit("--scan-limit must be at least 1")

    try:
        report = build_report(
            watchlist_path=args.watchlist,
            display_limit=args.display_limit,
            scan_limit=args.scan_limit,
            skip_options=args.skip_options,
            skip_detail_pages=args.skip_detail_pages,
            pause=args.pause,
            congress_members=[name.strip() for name in args.congress_members.split(",")],
            congress_lookback_days=args.congress_lookback_days,
            market_provider=args.market_provider,
        )
    except MarketDataError as e:
        logger.error("%s", e)
        return 1
    html_path, json_path = write_outputs(report, args.out_dir)
    logger.info("Wrote %s", html_path)
    logger.info("Wrote %s", json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
