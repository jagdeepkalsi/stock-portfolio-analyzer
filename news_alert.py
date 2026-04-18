#!/usr/bin/env python3
"""
Pre-market news alert generator.

Fetches, scores, and emails a curated digest of financial news covering:
  - Company-specific news for every stock in your portfolio
  - Market-wide news (general business + M&A/merger feed)

Run locally:
    python news_alert.py

Schedule (cron, Mon-Fri 7:30 AM ET):
    30 7 * * 1-5  cd /path/to/app && python news_alert.py >> logs/news_alert.log 2>&1

Enable in portfolio.json:
    "settings": {
        "news_alerts": {
            "enabled": true,
            "send_email": true,
            ...
        }
    }
"""

import json
import logging
import os
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from dotenv import load_dotenv

from news_providers import get_news_provider
from news_scorer import NewsDeduplicator, NewsScorer

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default config (merged with portfolio.json settings)
# ---------------------------------------------------------------------------

DEFAULT_NEWS_CONFIG = {
    "enabled": False,
    "send_email": True,
    "lookback_days": 1,
    "min_score_threshold": 3,
    "max_articles_per_symbol": 5,
    "max_market_articles": 10,
    "dedup_window_hours": 24,
    "include_market_news": True,
    "market_news_categories": ["general", "merger"],
    "keyword_overrides": {},
}


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

class NewsAlertGenerator:
    """
    Orchestrates news fetching, scoring, and digest building for the local runner.
    The Lambda version (LambdaNewsAlertGenerator in lambda_function.py) shares
    the same logic but reads config from S3 / Secrets Manager.
    """

    def __init__(self, portfolio_file: str = 'portfolio.json'):
        self.portfolio_file = portfolio_file
        self.portfolio      = self._load_portfolio()
        self.symbol_map     = self._load_symbol_map()   # {symbol: company_name}
        self.news_cfg       = self._load_news_config()
        self.provider       = self._init_provider()
        self.scorer         = NewsScorer(self.news_cfg)
        self.deduper        = NewsDeduplicator(
            window_hours=self.news_cfg.get('dedup_window_hours', 24)
        )

    # ── Config loading ───────────────────────────────────────────────────────

    def _load_portfolio(self) -> dict:
        try:
            with open(self.portfolio_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("portfolio.json not found — using defaults")
            return {"settings": {}}

    def _load_symbol_map(self) -> dict:
        """
        Return {symbol: company_name} for every unique holding in the portfolio.
        Company names are populated later from the news provider data (set to
        symbol as a fallback if not available).
        """
        holdings_file = self.portfolio.get('settings', {}).get('holdings_file', 'holdings.csv')
        symbol_map = {}
        try:
            df = pd.read_csv(holdings_file)
            for symbol in df['symbol'].unique():
                symbol_map[str(symbol).strip().upper()] = str(symbol).strip().upper()
        except FileNotFoundError:
            logger.warning("Holdings file '%s' not found — no company news will be fetched", holdings_file)
        except Exception as e:
            logger.warning("Could not read holdings file: %s", e)
        return symbol_map

    def _load_news_config(self) -> dict:
        cfg = {**DEFAULT_NEWS_CONFIG}
        user_cfg = self.portfolio.get('settings', {}).get('news_alerts', {})
        cfg.update(user_cfg)
        return cfg

    def _init_provider(self):
        finnhub_key = os.getenv('FINNHUB_API_KEY')
        return get_news_provider('finnhub', finnhub_key=finnhub_key)

    # ── Data fetching ────────────────────────────────────────────────────────

    def fetch_all_news(self, lookback_days: int = 1) -> dict:
        """
        Fetch company news for all holdings + market/macro news.

        Returns:
            {
                'company_news': {SYMBOL: [article, ...]},
                'market_news': [article, ...]
            }
        """
        today     = datetime.now().date()
        from_date = (today - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')

        company_news = {}
        for symbol in sorted(self.symbol_map.keys()):
            logger.info("Fetching news for %s", symbol)
            articles = self.provider.get_company_news(symbol, from_date, to_date)
            company_news[symbol] = articles
            logger.info("  → %d articles", len(articles))

        market_news = []
        if self.news_cfg.get('include_market_news', True):
            for category in self.news_cfg.get('market_news_categories', ['general', 'merger']):
                logger.info("Fetching market news (category=%s)", category)
                articles = self.provider.get_market_news(category)
                market_news.extend(articles)
                logger.info("  → %d articles", len(articles))

        total = sum(len(v) for v in company_news.values()) + len(market_news)
        logger.info("Total articles fetched: %d", total)
        return {'company_news': company_news, 'market_news': market_news}

    # ── Scoring & filtering ──────────────────────────────────────────────────

    def score_and_rank_news(self, raw: dict) -> dict:
        """
        1. Tag portfolio impact on every article (including market news).
        2. Score each article.
        3. Filter below min_score_threshold.
        4. Deduplicate across all feeds (company articles take priority).
        5. Sort each symbol's list by score desc, then datetime desc.
        6. Apply per-symbol and market article caps.

        Returns same shape as fetch_all_news() but with scored + filtered articles.
        """
        portfolio_symbols = set(self.symbol_map.keys())
        threshold = self.news_cfg.get('min_score_threshold', 3)
        max_per_symbol = self.news_cfg.get('max_articles_per_symbol', 5)
        max_market = self.news_cfg.get('max_market_articles', 10)

        # --- Tag + score company news first (they win dedup ties) ---
        scored_company = {}
        all_for_dedup = []

        for symbol, articles in raw['company_news'].items():
            scored = []
            for a in articles:
                self.scorer.tag_portfolio_impact(a, portfolio_symbols)
                a['score'] = self.scorer.score(a)
                a['impact_tags'] = self.scorer.extract_impact_tags(a)
                if a['score'] >= threshold:
                    scored.append(a)
            scored_company[symbol] = scored
            all_for_dedup.extend(scored)

        # --- Tag + score market news ---
        scored_market = []
        for a in raw['market_news']:
            self.scorer.tag_portfolio_impact(a, portfolio_symbols)
            a['score'] = self.scorer.score(a)
            a['impact_tags'] = self.scorer.extract_impact_tags(a)
            if a['score'] >= threshold:
                scored_market.append(a)
        all_for_dedup.extend(scored_market)

        # --- Deduplicate across all feeds ---
        deduped_ids = {a['id'] for a in self.deduper.filter(all_for_dedup)}

        # --- Filter, sort, cap company news ---
        final_company = {}
        for symbol, articles in scored_company.items():
            filtered = [a for a in articles if a.get('id') in deduped_ids]
            filtered.sort(key=lambda a: (a['score'], a.get('datetime', 0)), reverse=True)
            final_company[symbol] = filtered[:max_per_symbol]

        # --- Filter, sort, cap market news ---
        filtered_market = [a for a in scored_market if a.get('id') in deduped_ids]
        filtered_market.sort(key=lambda a: (a['score'], a.get('datetime', 0)), reverse=True)
        final_market = filtered_market[:max_market]

        return {'company_news': final_company, 'market_news': final_market}

    # ── DigestData builder ───────────────────────────────────────────────────

    def build_digest_data(self, scored: dict) -> dict:
        """
        Build a pure JSON-serializable DigestData dict from scored news.

        This is the single handoff point between the data layer and any renderer
        (email, web API, CLI --json, future mobile app). No rendering logic here.
        """
        now = datetime.now()
        now_ts = time.time()

        def age_display(unix_ts: int) -> str:
            age_sec = now_ts - unix_ts
            if age_sec < 3600:
                return f"{int(age_sec / 60)}m ago"
            if age_sec < 86400:
                return f"{int(age_sec / 3600)}h ago"
            return f"{int(age_sec / 86400)}d ago"

        def score_tier(score: int) -> str:
            if score >= 8:  return 'critical'
            if score >= 5:  return 'high'
            if score >= 3:  return 'medium'
            return 'low'

        def serialize_article(a: dict, symbol=None) -> dict:
            return {
                'id':               a.get('id'),
                'symbol':           symbol,
                'score':            a.get('score', 0),
                'score_tier':       score_tier(a.get('score', 0)),
                'headline':         a.get('headline', ''),
                'summary':          (a.get('summary') or '')[:300],
                'source':           a.get('source', ''),
                'url':              a.get('url', ''),
                'image':            a.get('image', ''),
                'age_display':      age_display(a.get('datetime', 0)),
                'impact_tags':      a.get('impact_tags', []),
                'portfolio_impact': a.get('portfolio_impact', []),
                'is_portfolio_impact': bool(a.get('portfolio_impact')),
            }

        # Company news section (only symbols with at least 1 article)
        company_sections = []
        for symbol in sorted(
            scored['company_news'].keys(),
            key=lambda s: max((a.get('score', 0) for a in scored['company_news'][s]), default=0),
            reverse=True,
        ):
            articles = scored['company_news'][symbol]
            if not articles:
                continue
            company_sections.append({
                'symbol':       symbol,
                'company_name': self.symbol_map.get(symbol, symbol),
                'articles':     [serialize_article(a, symbol=symbol) for a in articles],
            })

        market_articles = [serialize_article(a) for a in scored['market_news']]

        # Symbols with at least one critical/high score article (score >= 8)
        high_impact_symbols = [
            s['symbol'] for s in company_sections
            if any(a['score'] >= 8 for a in s['articles'])
        ]
        # Also flag if a market article directly mentions a portfolio holding
        for a in market_articles:
            if a.get('score', 0) >= 8 and a.get('portfolio_impact'):
                for sym in a['portfolio_impact']:
                    if sym not in high_impact_symbols:
                        high_impact_symbols.append(sym)

        total_shown = sum(len(s['articles']) for s in company_sections) + len(market_articles)

        return {
            'generated_at':         now.isoformat(),
            'generated_at_display': now.strftime('%B %d, %Y · %-I:%M %p ET'),
            'stats': {
                'articles_shown': total_shown,
            },
            'high_impact_symbols': high_impact_symbols,
            'company_news':        company_sections,
            'market_news':         market_articles,
        }

    # ── Runner ───────────────────────────────────────────────────────────────

    def run(self):
        if not self.news_cfg.get('enabled', False):
            logger.info("News alerts disabled in portfolio.json (settings.news_alerts.enabled=false)")
            return

        lookback = self.news_cfg.get('lookback_days', 1)
        logger.info("Starting news alert (lookback=%d day(s))", lookback)

        raw    = self.fetch_all_news(lookback_days=lookback)
        scored = self.score_and_rank_news(raw)
        digest = self.build_digest_data(scored)

        # Print a quick console summary
        total = digest['stats']['articles_shown']
        high  = digest['high_impact_symbols']
        logger.info("Digest built: %d articles shown", total)
        if high:
            logger.info("HIGH IMPACT symbols: %s", ', '.join(high))

        html = render_email_html(digest)

        if self.news_cfg.get('send_email', True):
            send_news_email(html, self.portfolio)
        else:
            logger.info("Email disabled (send_email=false). Set send_email=true in news_alerts config to send.")


# ---------------------------------------------------------------------------
# Renderers — consume DigestData, produce output
# ---------------------------------------------------------------------------

def render_json(digest: dict) -> str:
    """Return the DigestData as a formatted JSON string. Used by web endpoint or --json flag."""
    return json.dumps(digest, indent=2)


def render_email_html(digest: dict) -> str:
    """
    Produce a standalone HTML email body from a DigestData dict.
    Matches the visual style of the existing portfolio summary email.
    """
    high_impact   = digest.get('high_impact_symbols', [])
    company_news  = digest.get('company_news', [])
    market_news   = digest.get('market_news', [])
    generated_at  = digest.get('generated_at_display', '')
    total_shown   = digest.get('stats', {}).get('articles_shown', 0)

    score_colors = {
        'critical': '#dc3545',
        'high':     '#fd7e14',
        'medium':   '#e6a817',
        'low':      '#6c757d',
    }
    score_text_colors = {
        'critical': '#ffffff',
        'high':     '#ffffff',
        'medium':   '#212529',
        'low':      '#ffffff',
    }

    def badge(score: int, tier: str) -> str:
        bg  = score_colors.get(tier, '#6c757d')
        fg  = score_text_colors.get(tier, '#ffffff')
        return (
            f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
            f'background:{bg};color:{fg};font-size:11px;font-weight:700;'
            f'min-width:24px;text-align:center;">{score}</span>'
        )

    def tag_pill(tag: str) -> str:
        return (
            f'<span style="display:inline-block;padding:1px 6px;border-radius:3px;'
            f'background:#e9ecef;color:#495057;font-size:10px;margin:1px;">{tag}</span>'
        )

    def portfolio_pill(symbols: list) -> str:
        if not symbols:
            return ''
        syms = ' · '.join(symbols)
        return (
            f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
            f'background:#fff3cd;color:#856404;font-size:11px;font-weight:600;'
            f'margin-right:6px;">★ PORTFOLIO: {syms}</span>'
        )

    def render_article(a: dict) -> str:
        b    = badge(a['score'], a['score_tier'])
        tags = ''.join(tag_pill(t) for t in a.get('impact_tags', []))
        ptag = portfolio_pill(a.get('portfolio_impact', []))
        summary = a.get('summary', '')
        summary_html = f'<div style="color:#555;font-size:12px;margin:3px 0 4px 0;">{summary}</div>' if summary else ''
        headline_html = (
            f'<a href="{a["url"]}" style="color:#1a0dab;text-decoration:none;font-size:14px;font-weight:500;">'
            f'{a["headline"]}</a>'
            if a.get('url') else
            f'<span style="font-size:14px;font-weight:500;">{a["headline"]}</span>'
        )
        meta = f'<span style="color:#888;font-size:11px;">{a.get("source","")} · {a.get("age_display","")}</span>'
        return f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;vertical-align:top;">
            <div style="display:flex;align-items:flex-start;gap:8px;">
              <div style="flex-shrink:0;margin-top:2px;">{b}</div>
              <div>
                {ptag}{headline_html}
                {summary_html}
                {meta}&nbsp;&nbsp;{tags}
              </div>
            </div>
          </td>
        </tr>"""

    # ── High-impact banner ──────────────────────────────────────────────────
    banner_html = ''
    if high_impact:
        syms_html = ''.join(
            f'<span style="display:inline-block;padding:3px 10px;border-radius:4px;'
            f'background:#dc3545;color:#fff;font-weight:700;font-size:13px;margin:2px;">'
            f'{s}</span> '
            for s in high_impact
        )
        banner_html = f"""
    <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;
                padding:12px 16px;margin-bottom:20px;">
      <strong style="color:#856404;">HIGH IMPACT ALERT</strong>&nbsp;&nbsp;{syms_html}
    </div>"""

    # ── Company news sections ───────────────────────────────────────────────
    company_html = ''
    if company_news:
        company_html = '<h2 style="color:#2c3e50;border-bottom:2px solid #3498db;padding-bottom:6px;">Company News</h2>'
        for section in company_news:
            symbol  = section['symbol']
            name    = section['company_name']
            articles = section['articles']
            rows = ''.join(render_article(a) for a in articles)
            company_html += f"""
    <div style="margin-bottom:20px;">
      <div style="background:#2c3e50;color:#fff;padding:8px 14px;border-radius:5px 5px 0 0;
                  font-weight:700;font-size:14px;">{symbol} &nbsp;·&nbsp; {name}</div>
      <div style="border:1px solid #ddd;border-top:none;border-radius:0 0 5px 5px;padding:0 12px;">
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
      </div>
    </div>"""

    # ── Market & macro section ──────────────────────────────────────────────
    market_html = ''
    if market_news:
        rows = ''.join(render_article(a) for a in market_news)
        market_html = f"""
    <h2 style="color:#2c3e50;border-bottom:2px solid #27ae60;padding-bottom:6px;">Market &amp; Macro</h2>
    <div style="margin-bottom:20px;">
      <div style="border:1px solid #ddd;border-radius:5px;padding:0 12px;">
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
      </div>
    </div>"""

    # ── Footer ──────────────────────────────────────────────────────────────
    footer_html = f"""
    <div style="border-top:1px solid #ddd;margin-top:20px;padding-top:12px;
                color:#888;font-size:11px;text-align:center;">
      Generated {generated_at} &nbsp;·&nbsp; {total_shown} articles shown
      &nbsp;·&nbsp; News data provided by
      <a href="https://finnhub.io" style="color:#888;">Finnhub</a>
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Portfolio News Digest</title>
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
             background:#f5f5f5;margin:0;padding:20px;">
  <div style="max-width:700px;margin:0 auto;background:#fff;border-radius:8px;
              box-shadow:0 2px 8px rgba(0,0,0,0.1);padding:24px;">

    <div style="background:linear-gradient(135deg,#2c3e50,#3498db);color:#fff;
                padding:16px 20px;border-radius:6px;margin-bottom:20px;">
      <h1 style="margin:0;font-size:20px;">Pre-Market News Digest</h1>
      <p style="margin:4px 0 0 0;opacity:0.85;font-size:13px;">{generated_at}</p>
    </div>

    {banner_html}
    {company_html}
    {market_html}
    {footer_html}

  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

def send_news_email(html: str, portfolio_config: dict) -> bool:
    """
    Send the news digest HTML email via SMTP.

    Accepts the full portfolio config dict (same format as portfolio.json)
    so the web layer can call this function directly with its own config.
    """
    try:
        email_settings = portfolio_config.get('settings', {}).get('email_settings', {})
        recipient      = email_settings.get('recipient')

        if not recipient or recipient == 'your-email@example.com':
            logger.warning("Email recipient not configured in portfolio.json email_settings.recipient")
            return False

        smtp_user     = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')

        if not smtp_user or not smtp_password:
            logger.warning("SMTP_USER and/or SMTP_PASSWORD environment variables not set")
            return False

        msg = MIMEMultipart('alternative')
        msg['From']    = smtp_user
        msg['To']      = recipient
        msg['Subject'] = f"[Pre-Market Alert] Portfolio News Digest - {datetime.now().strftime('%B %d, %Y')}"

        plain = "Portfolio news digest — view this email in an HTML-capable client."
        msg.attach(MIMEText(plain, 'plain'))
        msg.attach(MIMEText(html, 'html'))

        server = smtplib.SMTP(
            email_settings.get('smtp_server', 'smtp.gmail.com'),
            email_settings.get('smtp_port', 587),
        )
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()

        logger.info("News digest email sent to %s", recipient)
        return True

    except Exception as e:
        logger.error("Failed to send news digest email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    generator = NewsAlertGenerator()
    generator.run()


if __name__ == "__main__":
    main()
