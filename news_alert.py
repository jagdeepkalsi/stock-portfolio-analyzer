#!/usr/bin/env python3
"""
Pre-market news alert generator.

Fetches, scores, and emails a curated 3-section digest:
  1. Top 10 Market News     — best stories from general/merger feeds
  2. Top 10 Portfolio Impact — any story across all feeds mentioning your holdings
  3. Top 5 Holdings by Value — 3 stories each, ranked by shares × purchase_price,
                               shown side-by-side in a 2-column grid

Sources: Finnhub · Alpha Vantage News Sentiment · MarketWatch RSS

Run locally:
    python news_alert.py

Schedule (cron, Mon-Fri 7:30 AM ET):
    30 7 * * 1-5  cd /path/to/app && python news_alert.py >> logs/news_alert.log 2>&1

Enable in portfolio.json → settings.news_alerts.enabled: true
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

from news_providers import AlphaVantageNewsProvider, get_news_providers
from news_scorer import NewsDeduplicator, NewsScorer

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_NEWS_CONFIG = {
    "enabled":                  False,
    "send_email":               True,
    "providers":                ["finnhub", "alpha_vantage", "marketwatch_rss"],
    "lookback_days":            1,
    "min_score_threshold":      3,
    "max_market_articles":      10,
    "max_portfolio_articles":   10,
    "max_company_articles":     3,
    "max_top_companies":        5,
    "dedup_window_hours":       24,
    "keyword_overrides":        {},
}


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

class NewsAlertGenerator:
    """
    Orchestrates news fetching, scoring, and digest building for the local runner.
    """

    def __init__(self, portfolio_file: str = 'portfolio.json'):
        self.portfolio_file = portfolio_file
        self.portfolio      = self._load_portfolio()
        self.holdings_data  = self._load_holdings_data()   # {sym: {company_name, position_value}}
        self.news_cfg       = self._load_news_config()
        self.providers      = self._init_providers()
        self.scorer         = NewsScorer(self.news_cfg)
        # Dedup window must cover the full fetch window so no valid articles are
        # discarded as "too old". lookback_days=1 means articles can be up to ~48h
        # old by run time, so we take max(configured_window, lookback*24 + 12h buffer).
        _lookback_hours = self.news_cfg.get('lookback_days', 1) * 24 + 12
        _dedup_window   = max(self.news_cfg.get('dedup_window_hours', 24), _lookback_hours)
        self.deduper        = NewsDeduplicator(window_hours=_dedup_window)

    # ── Config loading ───────────────────────────────────────────────────────

    def _load_portfolio(self) -> dict:
        try:
            with open(self.portfolio_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("portfolio.json not found — using defaults")
            return {"settings": {}}

    def _load_holdings_data(self) -> dict:
        """
        Returns {symbol: {'company_name': str, 'position_value': float}}
        position_value = sum(shares × purchase_price) across all lots.
        Used to rank companies by portfolio weight.
        """
        holdings_file = self.portfolio.get('settings', {}).get('holdings_file', 'holdings.csv')
        data = {}
        try:
            df = pd.read_csv(holdings_file)
            for _, row in df.iterrows():
                sym = str(row['symbol']).strip().upper()
                val = float(row['shares']) * float(row['purchase_price'])
                if sym not in data:
                    data[sym] = {'company_name': sym, 'position_value': 0.0}
                data[sym]['position_value'] += val
        except FileNotFoundError:
            logger.warning("Holdings file not found — company sections will be empty")
        except Exception as e:
            logger.warning("Could not read holdings: %s", e)
        return data

    def _load_news_config(self) -> dict:
        cfg = {**DEFAULT_NEWS_CONFIG}
        user_cfg = self.portfolio.get('settings', {}).get('news_alerts', {})
        cfg.update(user_cfg)
        return cfg

    def _init_providers(self) -> list:
        return get_news_providers(
            self.news_cfg.get('providers', ['finnhub', 'alpha_vantage', 'marketwatch_rss']),
            finnhub_key=os.getenv('FINNHUB_API_KEY'),
            alpha_vantage_key=os.getenv('ALPHA_VANTAGE_API_KEY'),
        )

    # ── Data fetching ────────────────────────────────────────────────────────

    def fetch_all_news(self, lookback_days: int = 1) -> dict:
        """
        Fetch from all providers and return raw articles.

        Returns:
            {
                'company_news': {SYMBOL: [article, ...]},
                'market_news':  [article, ...]
            }
        """
        today     = datetime.now().date()
        from_date = (today - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        to_date   = today.strftime('%Y-%m-%d')
        symbols   = list(self.holdings_data.keys())

        company_news: dict = {sym: [] for sym in symbols}
        market_news:  list = []

        for provider in self.providers:
            pname = type(provider).__name__

            # Prime Alpha Vantage cache with all symbols in one API call
            if isinstance(provider, AlphaVantageNewsProvider):
                logger.info("Alpha Vantage: priming batch cache for %s", symbols)
                provider.prime_cache(symbols, from_date)

            # Company news
            for symbol in symbols:
                articles = provider.get_company_news(symbol, from_date, to_date)
                if articles:
                    logger.info("%s: %d articles for %s", pname, len(articles), symbol)
                company_news[symbol].extend(articles)

            # Market news (Finnhub: general + merger; MarketWatch: one RSS call)
            if hasattr(provider, 'get_market_news'):
                if pname == 'FinnhubNewsProvider':
                    for category in ['general', 'merger']:
                        articles = provider.get_market_news(category)
                        logger.info("%s market(%s): %d articles", pname, category, len(articles))
                        market_news.extend(articles)
                else:
                    articles = provider.get_market_news()
                    if articles:
                        logger.info("%s market: %d articles", pname, len(articles))
                    market_news.extend(articles)

        total = sum(len(v) for v in company_news.values()) + len(market_news)
        logger.info("Total raw articles: %d", total)
        return {'company_news': company_news, 'market_news': market_news}

    # ── Scoring & filtering ──────────────────────────────────────────────────

    def score_and_rank_news(self, raw: dict) -> dict:
        """
        Score, tag, deduplicate, and return all articles.

        Returns:
            {
                'company_news':    {SYM: [scored articles, pre-cap]},
                'market_news':     [scored market articles, pre-cap],
                'all_scored_flat': [all scored + deduped articles, sorted by score]
            }
        Caps (top 10 / top 10 / top 3) are applied in build_digest_data().
        """
        portfolio_symbols = set(self.holdings_data.keys())
        threshold = self.news_cfg.get('min_score_threshold', 3)

        # --- Score company news first (wins dedup ties) ---
        scored_company: dict = {}
        all_for_dedup:  list = []

        for symbol, articles in raw['company_news'].items():
            scored = []
            for a in articles:
                self.scorer.tag_portfolio_impact(a, portfolio_symbols)
                a['score']       = self.scorer.score(a)
                a['impact_tags'] = self.scorer.extract_impact_tags(a)
                if a['score'] >= threshold:
                    scored.append(a)
            scored_company[symbol] = scored
            all_for_dedup.extend(scored)

        # --- Score market news ---
        scored_market: list = []
        for a in raw['market_news']:
            self.scorer.tag_portfolio_impact(a, portfolio_symbols)
            a['score']       = self.scorer.score(a)
            a['impact_tags'] = self.scorer.extract_impact_tags(a)
            if a['score'] >= threshold:
                scored_market.append(a)
        all_for_dedup.extend(scored_market)

        # --- Per-company dedup: remove duplicates within each symbol's own feed only ---
        # We use independent dedupers so cross-symbol sharing doesn't drop valid articles.
        # A story about both AAPL and MSFT should appear in BOTH company sections.
        final_company: dict = {}
        _lookback_hours = self.news_cfg.get('lookback_days', 1) * 24 + 12
        _dedup_window   = max(self.news_cfg.get('dedup_window_hours', 24), _lookback_hours)
        for sym, articles in scored_company.items():
            sym_deduper = NewsDeduplicator(window_hours=_dedup_window)
            kept = sym_deduper.filter(articles)
            kept.sort(key=lambda a: (a['score'], a.get('datetime', 0)), reverse=True)
            final_company[sym] = kept

        # --- Cross-feed dedup: used for market news and portfolio-impact section ---
        # Company articles go in first so they win ties against market feed duplicates.
        cross_feed_input = list(all_for_dedup)   # already company-first order
        deduped_flat = self.deduper.filter(cross_feed_input)

        s = self.deduper.stats
        logger.info(
            "Dedup stats — ID: %d, URL: %d, Jaccard: %d, Too old: %d",
            s['id'], s['url'], s['jaccard'], s['old']
        )

        deduped_ids = {a['id'] for a in deduped_flat}

        final_market = [a for a in scored_market if a.get('id') in deduped_ids]
        final_market.sort(key=lambda a: (a['score'], a.get('datetime', 0)), reverse=True)

        # --- Combined flat list sorted by score (for portfolio-impact section) ---
        all_flat = sorted(deduped_flat, key=lambda a: (a['score'], a.get('datetime', 0)), reverse=True)

        return {
            'company_news':    final_company,
            'market_news':     final_market,
            'all_scored_flat': all_flat,
        }

    # ── DigestData builder ───────────────────────────────────────────────────

    def build_digest_data(self, scored: dict) -> dict:
        """
        Build a pure JSON-serializable DigestData dict with 3 sections.
        This is the single handoff point between the data layer and any renderer.
        """
        now    = datetime.now()
        now_ts = time.time()

        def age_display(unix_ts: int) -> str:
            age_sec = now_ts - unix_ts
            if age_sec < 3600:  return f"{int(age_sec / 60)}m ago"
            if age_sec < 86400: return f"{int(age_sec / 3600)}h ago"
            return f"{int(age_sec / 86400)}d ago"

        def score_tier(score: int) -> str:
            if score >= 8: return 'critical'
            if score >= 5: return 'high'
            if score >= 3: return 'medium'
            return 'low'

        def serialize_article(a: dict, symbol=None) -> dict:
            return {
                'id':                a.get('id'),
                'symbol':            symbol,
                'score':             a.get('score', 0),
                'score_tier':        score_tier(a.get('score', 0)),
                'headline':          a.get('headline', ''),
                'summary':           (a.get('summary') or '')[:300],
                'source':            a.get('source', ''),
                'url':               a.get('url', ''),
                'age_display':       age_display(a.get('datetime', 0)),
                'impact_tags':       a.get('impact_tags', []),
                'portfolio_impact':  a.get('portfolio_impact', []),
                'is_portfolio_impact': bool(a.get('portfolio_impact')),
                'av_sentiment_score': a.get('av_sentiment_score', 0.0),
            }

        # ── Section 1: Top 10 market news ───────────────────────────────────
        market_section = [
            serialize_article(a) for a in scored['market_news'][:self.news_cfg.get('max_market_articles', 10)]
        ]

        # ── Section 2: Top 10 portfolio-impact stories (any feed) ───────────
        max_pi = self.news_cfg.get('max_portfolio_articles', 10)
        pi_articles = [
            a for a in scored['all_scored_flat']
            if a.get('portfolio_impact')
        ][:max_pi]
        portfolio_impact_section = [
            serialize_article(a, symbol=a.get('symbol')) for a in pi_articles
        ]

        # ── Section 3: Top 5 holdings by position value, 3 articles each ────
        max_companies = self.news_cfg.get('max_top_companies', 5)
        max_per_co    = self.news_cfg.get('max_company_articles', 3)

        ranked_symbols = sorted(
            self.holdings_data.keys(),
            key=lambda s: self.holdings_data[s]['position_value'],
            reverse=True,
        )[:max_companies]

        top_companies = []
        for rank, sym in enumerate(ranked_symbols, start=1):
            hd = self.holdings_data[sym]
            co_articles = scored['company_news'].get(sym, [])[:max_per_co]
            top_companies.append({
                'symbol':         sym,
                'company_name':   hd['company_name'],
                'position_value': round(hd['position_value'], 2),
                'rank':           rank,
                'articles':       [serialize_article(a, symbol=sym) for a in co_articles],
            })

        # ── High-impact banner symbols ───────────────────────────────────────
        high_impact_symbols = []
        for sym in ranked_symbols:
            if any(a['score'] >= 8 for a in scored['company_news'].get(sym, [])):
                high_impact_symbols.append(sym)
        for a in scored['market_news']:
            if a.get('score', 0) >= 8 and a.get('portfolio_impact'):
                for s in a['portfolio_impact']:
                    if s not in high_impact_symbols:
                        high_impact_symbols.append(s)

        total_shown = len(market_section) + len(portfolio_impact_section) + sum(
            len(c['articles']) for c in top_companies
        )

        try:
            generated_at_display = now.strftime('%B %-d, %Y · %-I:%M %p ET')
        except ValueError:
            generated_at_display = now.strftime('%B %d, %Y · %I:%M %p ET')

        return {
            'generated_at':         now.isoformat(),
            'generated_at_display': generated_at_display,
            'stats':                {'articles_shown': total_shown},
            'high_impact_symbols':  high_impact_symbols,
            'market_news':          market_section,
            'portfolio_impact_news': portfolio_impact_section,
            'top_companies':        top_companies,
        }

    # ── Runner ───────────────────────────────────────────────────────────────

    def run(self):
        if not self.news_cfg.get('enabled', False):
            logger.info("News alerts disabled (set settings.news_alerts.enabled=true in portfolio.json)")
            return

        lookback = self.news_cfg.get('lookback_days', 1)
        logger.info("Starting news alert (lookback=%d day(s), providers=%s)",
                    lookback, self.news_cfg.get('providers'))

        raw    = self.fetch_all_news(lookback_days=lookback)
        scored = self.score_and_rank_news(raw)
        digest = self.build_digest_data(scored)

        total = digest['stats']['articles_shown']
        high  = digest['high_impact_symbols']
        logger.info("Digest: %d articles shown | Sections: Market=%d, Portfolio Impact=%d, Holdings=%d",
                    total,
                    len(digest['market_news']),
                    len(digest['portfolio_impact_news']),
                    sum(len(c['articles']) for c in digest['top_companies']))
        if high:
            logger.info("HIGH IMPACT: %s", ', '.join(high))

        html = render_email_html(digest)

        if self.news_cfg.get('send_email', True):
            send_news_email(html, self.portfolio)
        else:
            logger.info("Email disabled (send_email=false in news_alerts config)")


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def render_json(digest: dict) -> str:
    """Return DigestData as formatted JSON. Used by web endpoints or --json flag."""
    return json.dumps(digest, indent=2)


def render_email_html(digest: dict) -> str:
    """
    Produce a standalone HTML email body from DigestData.
    Three sections: Market News · Portfolio Impact · Top Holdings (2-col grid)
    """
    high_impact          = digest.get('high_impact_symbols', [])
    market_news          = digest.get('market_news', [])
    portfolio_impact     = digest.get('portfolio_impact_news', [])
    top_companies        = digest.get('top_companies', [])
    generated_at         = digest.get('generated_at_display', '')
    total_shown          = digest.get('stats', {}).get('articles_shown', 0)

    score_colors = {'critical': '#dc3545', 'high': '#fd7e14', 'medium': '#e6a817', 'low': '#6c757d'}
    score_text   = {'critical': '#fff',    'high': '#fff',    'medium': '#212529', 'low': '#fff'}

    def badge(score: int, tier: str) -> str:
        bg = score_colors.get(tier, '#6c757d')
        fg = score_text.get(tier, '#fff')
        return (f'<span style="display:inline-block;padding:2px 7px;border-radius:4px;'
                f'background:{bg};color:{fg};font-size:11px;font-weight:700;'
                f'min-width:24px;text-align:center;">{score}</span>')

    def tag_pill(tag: str) -> str:
        # Sentiment tags get distinct colours
        if tag == 'BULLISH':
            return ('<span style="display:inline-block;padding:1px 6px;border-radius:3px;'
                    'background:#d4edda;color:#155724;font-size:10px;margin:1px;">▲ BULLISH</span>')
        if tag == 'BEARISH':
            return ('<span style="display:inline-block;padding:1px 6px;border-radius:3px;'
                    'background:#f8d7da;color:#721c24;font-size:10px;margin:1px;">▼ BEARISH</span>')
        return (f'<span style="display:inline-block;padding:1px 6px;border-radius:3px;'
                f'background:#e9ecef;color:#495057;font-size:10px;margin:1px;">{tag}</span>')

    def portfolio_pill(symbols: list) -> str:
        if not symbols:
            return ''
        return (f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
                f'background:#fff3cd;color:#856404;font-size:11px;font-weight:600;'
                f'margin-right:6px;">★ {" · ".join(symbols)}</span>')

    def article_row(a: dict) -> str:
        b       = badge(a['score'], a['score_tier'])
        tags    = ''.join(tag_pill(t) for t in a.get('impact_tags', []))
        ptag    = portfolio_pill(a.get('portfolio_impact', []))
        summary = a.get('summary', '')
        sum_html = (f'<div style="color:#555;font-size:12px;margin:3px 0 4px 0;">{summary}</div>'
                    if summary else '')
        hl = (f'<a href="{a["url"]}" style="color:#1a0dab;text-decoration:none;'
              f'font-size:14px;font-weight:500;">{a["headline"]}</a>'
              if a.get('url') else
              f'<span style="font-size:14px;font-weight:500;">{a["headline"]}</span>')
        meta = f'<span style="color:#888;font-size:11px;">{a.get("source","")} · {a.get("age_display","")}</span>'
        return f"""
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;vertical-align:top;">
            <div style="display:flex;align-items:flex-start;gap:8px;">
              <div style="flex-shrink:0;margin-top:2px;">{b}</div>
              <div style="flex:1;">{ptag}{hl}{sum_html}{meta}&nbsp;&nbsp;{tags}</div>
            </div>
          </td>
        </tr>"""

    def section_card(header_html: str, articles: list, header_color: str = '#2c3e50') -> str:
        if not articles:
            return ''
        rows = ''.join(article_row(a) for a in articles)
        return f"""
    <div style="margin-bottom:24px;">
      <div style="background:{header_color};color:#fff;padding:10px 16px;
                  border-radius:6px 6px 0 0;font-weight:700;font-size:14px;">{header_html}</div>
      <div style="border:1px solid #ddd;border-top:none;border-radius:0 0 6px 6px;padding:0 14px;">
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
      </div>
    </div>"""

    # ── HIGH IMPACT BANNER ──────────────────────────────────────────────────
    banner_html = ''
    if high_impact:
        pills = ''.join(
            f'<span style="display:inline-block;padding:3px 10px;border-radius:4px;'
            f'background:#dc3545;color:#fff;font-weight:700;font-size:13px;margin:2px;">{s}</span> '
            for s in high_impact
        )
        banner_html = f"""
    <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;
                padding:12px 16px;margin-bottom:20px;">
      <strong style="color:#856404;">⚡ HIGH IMPACT ALERT &nbsp;</strong>{pills}
    </div>"""

    # ── SECTION 1: MARKET NEWS ──────────────────────────────────────────────
    s1 = section_card(
        '🌐 &nbsp;Market &amp; Macro — Top Stories',
        market_news,
        header_color='#27ae60',
    )

    # ── SECTION 2: PORTFOLIO IMPACT ─────────────────────────────────────────
    s2 = section_card(
        '★ &nbsp;Portfolio Impact — Stories Mentioning Your Holdings',
        portfolio_impact,
        header_color='#2980b9',
    )

    # ── SECTION 3: TOP HOLDINGS GRID (2 columns) ────────────────────────────
    def company_card_html(co: dict) -> str:
        sym   = co['symbol']
        name  = co['company_name']
        rank  = co['rank']
        val   = co['position_value']
        arts  = co['articles']
        if not arts:
            rows_html = '<p style="color:#888;font-size:12px;padding:8px 0;">No stories today.</p>'
        else:
            rows_html = f'<table style="width:100%;border-collapse:collapse;">{"".join(article_row(a) for a in arts)}</table>'
        return f"""
        <div style="background:#2c3e50;color:#fff;padding:8px 12px;border-radius:5px 5px 0 0;
                    font-size:13px;font-weight:700;">
          {sym} &nbsp;<span style="font-weight:400;opacity:0.8;">#{rank} · ${val:,.0f}</span>
          <span style="float:right;opacity:0.7;font-size:11px;">{name}</span>
        </div>
        <div style="border:1px solid #ddd;border-top:none;border-radius:0 0 5px 5px;
                    padding:0 10px;min-height:60px;">{rows_html}</div>"""

    grid_rows_html = ''
    pairs = [top_companies[i:i+2] for i in range(0, len(top_companies), 2)]
    for pair in pairs:
        if len(pair) == 2:
            left, right = pair
            grid_rows_html += f"""
      <tr>
        <td style="width:50%;padding:6px 8px 6px 0;vertical-align:top;">
          {company_card_html(left)}
        </td>
        <td style="width:50%;padding:6px 0 6px 8px;vertical-align:top;">
          {company_card_html(right)}
        </td>
      </tr>"""
        else:
            solo = pair[0]
            grid_rows_html += f"""
      <tr>
        <td colspan="2" style="padding:6px 0;vertical-align:top;">
          {company_card_html(solo)}
        </td>
      </tr>"""

    s3 = ''
    if top_companies:
        s3 = f"""
    <div style="margin-bottom:24px;">
      <h2 style="color:#2c3e50;border-bottom:2px solid #8e44ad;padding-bottom:6px;
                 font-size:16px;margin-bottom:12px;">
        📊 &nbsp;Top Holdings — Latest News
      </h2>
      <table style="width:100%;border-collapse:collapse;">{grid_rows_html}</table>
    </div>"""

    # ── FOOTER ──────────────────────────────────────────────────────────────
    footer = f"""
    <div style="border-top:1px solid #ddd;margin-top:20px;padding-top:12px;
                color:#888;font-size:11px;text-align:center;">
      Generated {generated_at} &nbsp;·&nbsp; {total_shown} articles shown
      &nbsp;·&nbsp; Sources: Finnhub · Alpha Vantage · MarketWatch
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
  <div style="max-width:740px;margin:0 auto;background:#fff;border-radius:8px;
              box-shadow:0 2px 8px rgba(0,0,0,0.1);padding:24px;">

    <div style="background:linear-gradient(135deg,#2c3e50,#3498db);color:#fff;
                padding:16px 20px;border-radius:6px;margin-bottom:20px;">
      <h1 style="margin:0;font-size:20px;">📰 Pre-Market News Digest</h1>
      <p style="margin:4px 0 0 0;opacity:0.85;font-size:13px;">{generated_at}</p>
    </div>

    {banner_html}
    {s1}
    {s2}
    {s3}
    {footer}

  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

def send_news_email(html: str, portfolio_config: dict) -> bool:
    try:
        email_settings = portfolio_config.get('settings', {}).get('email_settings', {})
        recipient      = email_settings.get('recipient')

        if not recipient or recipient == 'your-email@example.com':
            logger.warning("Recipient not configured in portfolio.json email_settings.recipient")
            return False

        smtp_user     = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')

        if not smtp_user or not smtp_password:
            logger.warning("SMTP_USER and/or SMTP_PASSWORD not set")
            return False

        msg = MIMEMultipart('alternative')
        msg['From']    = smtp_user
        msg['To']      = recipient
        msg['Subject'] = f"[Pre-Market Alert] Portfolio News Digest - {datetime.now().strftime('%B %d, %Y')}"

        msg.attach(MIMEText("Portfolio news digest — view in an HTML-capable client.", 'plain'))
        msg.attach(MIMEText(html, 'html'))

        server = smtplib.SMTP(
            email_settings.get('smtp_server', 'smtp.gmail.com'),
            email_settings.get('smtp_port', 587),
        )
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()

        logger.info("Email sent to %s", recipient)
        return True

    except Exception as e:
        logger.error("Failed to send email: %s", e)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    NewsAlertGenerator().run()


if __name__ == "__main__":
    main()
