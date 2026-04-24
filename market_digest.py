#!/usr/bin/env python3
"""
Daily Market Pulse digest.

A brand-new email (separate from the portfolio summary and pre-market news
digest) with two sections:

  1. Market Trends — daily + weekly view across indexes, sectors, and macro tape.
  2. Congressional Trades — recent House + Senate disclosures, Pelosi prioritized.

Run locally:
    python market_digest.py          # builds digest, sends email if configured
    python market_digest.py --dry    # builds digest, writes HTML + JSON to ./out/, skips email

Config (all optional; reads portfolio.json if present):
    settings.market_digest = {
        "enabled":          true,
        "send_email":       true,
        "congress_lookback_days": 7,
        "congress_basis":   "disclosure",       # or "transaction"
        "max_priority_trades": 25,
        "max_other_trades":    50
    }
"""

import argparse
import json
import logging
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

from congress_providers import fetch_recent_trades
from market_trends import fetch_market_trends

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "enabled":                True,
    "send_email":             True,
    "congress_lookback_days": 7,
    "congress_basis":         "disclosure",
    "max_priority_trades":    25,
    "max_other_trades":       50,
}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def load_portfolio_config(path: str = "portfolio.json") -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("%s not found — using defaults", path)
        return {"settings": {}}


def load_digest_config(portfolio: dict) -> dict:
    cfg = {**DEFAULT_CONFIG}
    cfg.update(portfolio.get("settings", {}).get("market_digest", {}) or {})
    return cfg


def build_digest(portfolio: dict, cfg: dict) -> dict:
    """Run both data layers and package their results into a single dict."""
    logger.info("Building market trends section...")
    trends = fetch_market_trends()

    logger.info("Building congress section (lookback %dd, basis=%s)...",
                cfg["congress_lookback_days"], cfg["congress_basis"])
    congress = fetch_recent_trades(
        lookback_days=cfg["congress_lookback_days"],
        basis=cfg["congress_basis"],
    )

    congress["priority_trades"] = congress["priority_trades"][: cfg["max_priority_trades"]]
    # "Other" list = everything, but we'll slice priority out for readability in the table
    priority_keys = {(t["chamber"], t["member"], t["ticker"], t["transaction_date"])
                     for t in congress["priority_trades"]}
    other = [t for t in congress["all_trades"]
             if (t["chamber"], t["member"], t["ticker"], t["transaction_date"]) not in priority_keys]
    congress["other_trades"] = other[: cfg["max_other_trades"]]

    now = datetime.now()
    return {
        "generated_at":          now.isoformat(),
        "generated_at_display":  now.strftime("%B %d, %Y · %I:%M %p"),
        "trends":                trends,
        "congress":              congress,
    }


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _pct_cell(value) -> str:
    """Colored % cell. None → em-dash, positive green, negative red."""
    if value is None:
        return '<td style="text-align:right;color:#999;">—</td>'
    color = "#1b8754" if value >= 0 else "#c0392b"
    sign  = "+" if value >= 0 else ""
    return (f'<td style="text-align:right;color:{color};font-variant-numeric:tabular-nums;">'
            f'{sign}{value:.2f}%</td>')


def _price_cell(value) -> str:
    if value is None:
        return '<td style="text-align:right;color:#999;">—</td>'
    return f'<td style="text-align:right;font-variant-numeric:tabular-nums;">{value:,.2f}</td>'


def _dollar_range(row: dict) -> str:
    lo, hi = row.get("amount_min") or 0, row.get("amount_max") or 0
    if lo == 0 and hi == 0:
        return row.get("amount_range") or "—"
    if lo == hi:
        return f"${lo:,.0f}"
    return f"${lo:,.0f} – ${hi:,.0f}"


def _tx_badge(transaction: str) -> str:
    colors = {
        "purchase": ("#1b8754", "BUY"),
        "sale":     ("#c0392b", "SELL"),
        "exchange": ("#7a5901", "EXCH"),
        "other":    ("#555",    "OTHER"),
    }
    color, label = colors.get(transaction, ("#555", transaction.upper() or "—"))
    return (f'<span style="display:inline-block;padding:2px 8px;border-radius:10px;'
            f'background:{color};color:#fff;font-size:11px;font-weight:600;">{label}</span>')


def _table(headers: list[str], rows_html: list[str]) -> str:
    head = "".join(
        f'<th style="text-align:left;padding:8px 10px;border-bottom:2px solid #ddd;'
        f'font-size:12px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">{h}</th>'
        for h in headers
    )
    body = "".join(rows_html) or (
        f'<tr><td colspan="{len(headers)}" style="padding:14px;color:#888;text-align:center;">'
        f'No data.</td></tr>'
    )
    return (f'<table style="width:100%;border-collapse:collapse;font-size:14px;">'
            f'<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>')


def _perf_row(row: dict) -> str:
    return (
        f'<tr>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;"><strong>{row["label"]}</strong>'
        f' <span style="color:#888;">({row["symbol"]})</span></td>'
        f'{_price_cell(row.get("price"))}'
        f'{_pct_cell(row.get("change_1d"))}'
        f'{_pct_cell(row.get("change_1w"))}'
        f'{_pct_cell(row.get("change_1m"))}'
        f'</tr>'
    )


def _trade_row(t: dict, highlight: bool = False) -> str:
    bg = "background:#fff8e1;" if highlight else ""
    state_suffix = f" · {t['state']}" if t.get("state") else ""
    tx_date      = t.get("transaction_date") or "—"
    dis_date     = t.get("disclosure_date") or "—"
    days         = t.get("days_to_disclose")
    days_span    = (f' <span style="color:#888;">({days}d)</span>'
                    if days is not None else "")
    return (
        f'<tr style="{bg}">'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;">'
        f'<strong>{t["member"] or "—"}</strong>'
        f'<span style="color:#888;font-size:12px;"> · {t["chamber"]}{state_suffix}</span>'
        f'</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;font-weight:600;">{t["ticker"]}</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;">{_tx_badge(t["transaction"])}</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;text-align:right;'
        f'font-variant-numeric:tabular-nums;">{_dollar_range(t)}</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;font-size:13px;">{tx_date}</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid #eee;color:#555;font-size:13px;">{dis_date}{days_span}</td>'
        f'</tr>'
    )


def render_email_html(digest: dict) -> str:
    trends   = digest["trends"]
    congress = digest["congress"]

    # Highlights bullets
    highlight_html = (
        "<ul style='margin:0;padding-left:20px;line-height:1.6;'>"
        + "".join(f"<li>{h}</li>" for h in trends["highlights"])
        + "</ul>"
    ) if trends["highlights"] else '<p style="color:#888;">No notable moves today.</p>'

    perf_headers = ["Instrument", "Last", "1D", "1W", "1M"]
    index_rows   = [_perf_row(r) for r in trends["indexes"]]
    sector_rows  = [_perf_row(r) for r in trends["sectors"]]
    macro_rows   = [_perf_row(r) for r in trends["macro"]]

    priority_rows = [_trade_row(t, highlight=True) for t in congress["priority_trades"]]
    other_rows    = [_trade_row(t) for t in congress["other_trades"]]

    trade_headers = ["Member", "Ticker", "Action", "Size", "Traded", "Disclosed"]

    counts = congress["counts"]
    congress_summary = (
        f'{counts["total"]} trades disclosed in the last {congress["lookback_days"]} days '
        f'· {counts["house"]} House · {counts["senate"]} Senate '
        f'· {counts["priority"]} priority'
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Market Pulse — {digest["generated_at_display"]}</title>
</head>
<body style="margin:0;padding:0;background:#f4f6f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#222;">
  <div style="max-width:820px;margin:24px auto;padding:0 16px;">

    <div style="background:#111;color:#fff;padding:20px 24px;border-radius:8px 8px 0 0;">
      <div style="font-size:22px;font-weight:700;letter-spacing:-0.3px;">Market Pulse</div>
      <div style="font-size:13px;color:#bbb;margin-top:4px;">{digest["generated_at_display"]}</div>
    </div>

    <div style="background:#fff;padding:24px;border-radius:0 0 8px 8px;box-shadow:0 1px 2px rgba(0,0,0,0.05);">

      <!-- Section 1: Market Trends -->
      <h2 style="margin:0 0 8px;font-size:18px;">1 · What's moving markets</h2>
      <div style="background:#f8f9fb;border-left:3px solid #2b6cb0;padding:12px 14px;margin:10px 0 20px;border-radius:4px;">
        {highlight_html}
      </div>

      <h3 style="margin:20px 0 6px;font-size:14px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">Major indexes</h3>
      {_table(perf_headers, index_rows)}

      <h3 style="margin:24px 0 6px;font-size:14px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">Sector rotation (SPDR ETFs)</h3>
      {_table(perf_headers, sector_rows)}

      <h3 style="margin:24px 0 6px;font-size:14px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">Macro tape</h3>
      {_table(perf_headers, macro_rows)}

      <hr style="border:none;border-top:1px solid #eee;margin:32px 0;">

      <!-- Section 2: Congressional Trades -->
      <h2 style="margin:0 0 8px;font-size:18px;">2 · Congressional trades</h2>
      <div style="color:#666;font-size:13px;margin-bottom:12px;">{congress_summary}</div>

      <h3 style="margin:16px 0 6px;font-size:14px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">Priority (Pelosi + watchlist)</h3>
      {_table(trade_headers, priority_rows)}

      <h3 style="margin:24px 0 6px;font-size:14px;color:#555;text-transform:uppercase;letter-spacing:0.5px;">All other disclosures (by size)</h3>
      {_table(trade_headers, other_rows)}

      <p style="color:#999;font-size:12px;margin-top:28px;">
        Sources: yfinance · house-stock-watcher · senate-stock-watcher.
        Congressional disclosures typically lag actual trades by weeks. Not investment advice.
      </p>

    </div>
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

def send_email(html: str, portfolio: dict, subject_date: str) -> bool:
    email_settings = portfolio.get("settings", {}).get("email_settings", {})
    recipient      = email_settings.get("recipient")

    if not recipient or recipient == "your-email@example.com":
        logger.warning("Recipient not configured in portfolio.json email_settings.recipient")
        return False

    smtp_user     = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        logger.warning("SMTP_USER and/or SMTP_PASSWORD not set in environment")
        return False

    msg = MIMEMultipart("alternative")
    msg["From"]    = smtp_user
    msg["To"]      = recipient
    msg["Subject"] = f"[Market Pulse] {subject_date}"

    msg.attach(MIMEText("Market Pulse — view in an HTML-capable email client.", "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        server = smtplib.SMTP(
            email_settings.get("smtp_server", "smtp.gmail.com"),
            email_settings.get("smtp_port", 587),
        )
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        logger.info("Market Pulse email sent to %s", recipient)
        return True
    except Exception as e:
        logger.error("Failed to send email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _write_outputs(digest: dict, html: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    (out_dir / f"market_pulse_{stamp}.html").write_text(html, encoding="utf-8")
    (out_dir / f"market_pulse_{stamp}.json").write_text(
        json.dumps(digest, indent=2, default=str), encoding="utf-8"
    )
    logger.info("Wrote dry-run outputs to %s/", out_dir)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily Market Pulse digest")
    parser.add_argument("--dry", action="store_true",
                        help="Build digest and write HTML+JSON to ./out/, skip email.")
    parser.add_argument("--config", default="portfolio.json",
                        help="Path to portfolio.json (default: portfolio.json)")
    args = parser.parse_args(argv)

    portfolio = load_portfolio_config(args.config)
    cfg       = load_digest_config(portfolio)

    if not cfg["enabled"] and not args.dry:
        logger.info("market_digest disabled in config; exiting.")
        return 0

    digest = build_digest(portfolio, cfg)
    html   = render_email_html(digest)

    if args.dry or not cfg["send_email"]:
        _write_outputs(digest, html, Path("out"))
        return 0

    ok = send_email(html, portfolio, digest["generated_at_display"].split(" · ")[0])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
