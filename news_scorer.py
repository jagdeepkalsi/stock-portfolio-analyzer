#!/usr/bin/env python3
"""
News scoring and deduplication.

NewsScorer:    Assigns an impact score (0-100) to each article using keyword
               matching, recency, and portfolio relevance.

NewsDeduplicator: Removes duplicate articles across company and market feeds
                  using Finnhub article IDs and headline similarity.
"""

import time
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# NewsScorer
# ---------------------------------------------------------------------------

class NewsScorer:
    """
    Scores articles by financial impact using a keyword-weighted additive model.

    Scoring layers (in order):
      1. Keyword scan — headline + summary searched case-insensitively; each
         matching keyword adds its assigned points (multiple keywords accumulate).
      2. Recency bonus — articles < 3 hours old receive +2.
      3. Portfolio boost — articles that mention one of your holdings get +3.
      4. Noise cap — if any LOW_NOISE_KEYWORD matches, score is hard-capped at 2
         regardless of other matches.
      Final score is clamped to [0, 100].
    """

    # keyword (lowercase) → base score points
    HIGH_IMPACT_KEYWORDS = {
        # M&A
        'acqui':            8,
        'merger':           8,
        'takeover':         8,
        'buyout':           8,
        'acquisition':      8,
        'acquires':         8,
        'acquired by':      8,

        # Earnings
        'earnings beat':    9,
        'earnings miss':    9,
        'eps beat':         8,
        'eps miss':         8,
        'beat estimates':   8,
        'missed estimates': 8,
        'beat expectations':8,
        'missed expectations':8,
        'guidance raised':  9,
        'guidance cut':     9,
        'raises guidance':  9,
        'cuts guidance':    9,
        'raised guidance':  9,
        'revenue beat':     7,
        'revenue miss':     7,
        'profit warning':   9,

        # Analyst actions
        'upgrade':          6,
        'downgrade':        6,
        'price target raised':6,
        'price target cut': 6,
        'price target increase':6,
        'price target decrease':6,
        'outperform':       4,
        'underperform':     4,
        'buy rating':       5,
        'sell rating':      5,
        'overweight':       4,
        'underweight':      4,
        'initiates coverage':5,

        # FDA / Regulatory
        'fda approval':     9,
        'fda approved':     9,
        'fda rejected':     9,
        'fda rejection':    9,
        'fda clearance':    8,
        'regulatory approval':8,
        'regulatory action':7,
        'sec investigation':9,
        'sec probe':        8,
        'doj investigation':8,

        # Bankruptcy / Crisis
        'bankrupt':         10,
        'chapter 11':       10,
        'chapter 7':        10,
        'insolvency':       9,
        'default':          8,
        'debt restructur':  8,
        'fraud':            9,
        'investigation':    5,
        'accounting irregularities':10,
        'restatement':      8,

        # Executive changes
        'ceo resign':       7,
        'ceo fired':        7,
        'ceo depart':       7,
        'cfo resign':       6,
        'cfo depart':       6,
        'cto resign':       5,
        'executive depart': 5,
        'board resign':     5,
        'steps down':       5,

        # Dividends / Capital returns
        'dividend cut':     7,
        'dividend increase':6,
        'dividend raise':   6,
        'special dividend': 6,
        'share buyback':    5,
        'stock repurchase': 5,
        'buyback program':  5,

        # Macro / Federal Reserve
        'federal reserve':  6,
        'fed rate':         6,
        'interest rate':    5,
        'rate hike':        7,
        'rate cut':         7,
        'quantitative easing':6,
        'quantitative tightening':6,
        'inflation':        5,
        'cpi':              5,
        'recession':        7,
        'market crash':     8,
        'market rally':     4,
        'tariff':           5,
        'trade war':        6,
        'sanctions':        6,
    }

    # If any of these appear in the text, score is capped at 2 (noise suppression)
    LOW_NOISE_KEYWORDS = [
        'preview',
        'weekly roundup',
        'daily brief',
        'morning brief',
        'afternoon brief',
        'market wrap',
        'weekly recap',
        'analyst note',
        'morning note',
        'sponsored',
        'what to watch',
        'next week',
        'earnings season kicks',
        'stocks to watch',
    ]

    # Tag label → keywords that trigger it (used by extract_impact_tags)
    TAG_KEYWORDS = {
        'M&A':            ['acqui', 'merger', 'takeover', 'buyout'],
        'EARNINGS BEAT':  ['earnings beat', 'eps beat', 'beat estimates', 'beat expectations'],
        'EARNINGS MISS':  ['earnings miss', 'eps miss', 'missed estimates', 'missed expectations', 'profit warning'],
        'GUIDANCE UP':    ['guidance raised', 'raises guidance', 'raised guidance'],
        'GUIDANCE DOWN':  ['guidance cut', 'cuts guidance'],
        'UPGRADE':        ['upgrade', 'buy rating', 'overweight', 'outperform', 'initiates coverage'],
        'DOWNGRADE':      ['downgrade', 'sell rating', 'underweight', 'underperform'],
        'FDA':            ['fda approval', 'fda approved', 'fda rejected', 'fda rejection', 'fda clearance', 'regulatory approval'],
        'BANKRUPTCY':     ['bankrupt', 'chapter 11', 'chapter 7', 'insolvency', 'default', 'debt restructur'],
        'FRAUD/LEGAL':    ['fraud', 'sec investigation', 'sec probe', 'doj investigation', 'accounting irregularities', 'restatement'],
        'CEO CHANGE':     ['ceo resign', 'ceo fired', 'ceo depart', 'steps down'],
        'EXEC CHANGE':    ['cfo resign', 'cfo depart', 'cto resign', 'executive depart', 'board resign'],
        'DIVIDEND':       ['dividend cut', 'dividend increase', 'dividend raise', 'special dividend'],
        'BUYBACK':        ['share buyback', 'stock repurchase', 'buyback program'],
        'FED/MACRO':      ['federal reserve', 'fed rate', 'interest rate', 'rate hike', 'rate cut', 'inflation', 'cpi', 'recession', 'tariff', 'trade war'],
    }

    def __init__(self, config: dict = None):
        cfg = config or {}
        self.max_articles_per_symbol = cfg.get('max_articles_per_symbol', 5)
        self.max_market_articles     = cfg.get('max_market_articles', 10)
        # Allow per-user keyword score overrides via portfolio.json
        custom = cfg.get('keyword_overrides', {})
        self.keywords = {**self.HIGH_IMPACT_KEYWORDS, **custom}

    def score(self, article: dict) -> int:
        """
        Return an integer impact score 0–100 for the given article.
        Does not mutate the article dict.
        """
        text = (
            article.get('headline', '') + ' ' + article.get('summary', '')
        ).lower()

        total = 0

        # Layer 1: keyword scan
        for kw, pts in self.keywords.items():
            if kw in text:
                total += pts

        # Layer 2: noise cap (applied before recency/portfolio bonuses)
        for noise_kw in self.LOW_NOISE_KEYWORDS:
            if noise_kw in text:
                return min(total, 2)

        # Layer 3: recency bonus
        age_hours = (time.time() - article.get('datetime', 0)) / 3600
        if age_hours < 3:
            total += 2

        # Layer 4: portfolio boost (portfolio_impact must already be set)
        if article.get('portfolio_impact'):
            total += 3

        # Layer 5: Alpha Vantage sentiment bonus
        av_score = article.get('av_sentiment_score', 0.0)
        if abs(av_score) >= 0.15:   # threshold: Somewhat Bullish / Somewhat Bearish
            total += 2

        return min(total, 100)

    def extract_impact_tags(self, article: dict) -> list:
        """
        Return a list of human-readable impact label strings for the article.
        E.g. ["EARNINGS BEAT", "GUIDANCE UP", "BULLISH"]
        """
        text = (
            article.get('headline', '') + ' ' + article.get('summary', '')
        ).lower()

        tags = []
        for tag, keywords in self.TAG_KEYWORDS.items():
            for kw in keywords:
                if kw in text:
                    tags.append(tag)
                    break

        # Alpha Vantage sentiment tags (added regardless of keyword matches)
        av_score = article.get('av_sentiment_score', 0.0)
        if av_score >= 0.15:
            tags.append('BULLISH')
        elif av_score <= -0.15:
            tags.append('BEARISH')

        return tags

    def tag_portfolio_impact(self, article: dict, portfolio_symbols: set) -> list:
        """
        Scan headline + summary + 'related' field for ticker symbols that are
        in the user's portfolio.

        Returns a list of matching symbols (e.g. ["AAPL", "MSFT"]).
        Stores the result as article['portfolio_impact'] (mutates in place).

        Note: Finnhub's 'related' field is empty "" on general/merger feeds,
        so text scanning is the primary detection method.
        """
        text = (
            article.get('headline', '') + ' ' +
            article.get('summary', '') + ' ' +
            article.get('related', '')
        ).upper()

        matches = []
        for sym in portfolio_symbols:
            # Match whole-word symbol to avoid false positives (e.g. "IT" in "Twitter")
            # Simple check: symbol surrounded by non-alpha characters or at string boundary
            import re
            if re.search(r'(?<![A-Z])' + re.escape(sym) + r'(?![A-Z])', text):
                matches.append(sym)

        article['portfolio_impact'] = matches
        return matches


# ---------------------------------------------------------------------------
# NewsDeduplicator
# ---------------------------------------------------------------------------

class NewsDeduplicator:
    """
    Removes duplicate articles across company and market feeds.

    Two articles are considered duplicates if:
      - They share the same Finnhub article id, OR
      - Their headline token-Jaccard similarity exceeds 0.7
        (catches wire reformats of the same story)

    First occurrence wins. Since fetch_all_news() processes company news
    before market news, company-tagged articles take priority.
    """

    def __init__(self, window_hours: int = 24):
        self.window_hours = window_hours
        self._seen_ids:       set  = set()
        self._seen_urls:      set  = set()   # normalized URL dedup (cross-provider)
        self._seen_headlines: list = []
        self.stats = {'id': 0, 'url': 0, 'jaccard': 0, 'old': 0}

    def filter(self, articles: list) -> list:
        """
        Accept a flat list of articles. Return deduplicated list preserving order.

        Three dedup layers (first occurrence wins):
          1. Exact article ID  — same Finnhub/AV id seen twice
          2. Normalized URL    — same story on different domains (strips query params)
          3. Headline Jaccard  — near-identical headline (≥ 0.70), catches wire reformats
        """
        result = []
        cutoff = time.time() - (self.window_hours * 3600)

        for article in articles:
            # Drop articles older than the dedup window
            if article.get('datetime', 0) < cutoff:
                self.stats['old'] += 1
                continue

            article_id = article.get('id')
            url        = article.get('url', '')
            headline   = article.get('headline', '')
            norm_url   = self._normalize_url(url)

            # Level 1: exact id dedup
            if article_id and article_id in self._seen_ids:
                self.stats['id'] += 1
                continue

            # Level 2: normalized URL dedup
            if norm_url and norm_url in self._seen_urls:
                self.stats['url'] += 1
                continue

            # Level 3: headline similarity dedup
            if any(self._jaccard(headline, seen) >= 0.7 for seen in self._seen_headlines):
                self.stats['jaccard'] += 1
                continue

            # Accept this article
            if article_id:
                self._seen_ids.add(article_id)
            if norm_url:
                self._seen_urls.add(norm_url)
            self._seen_headlines.append(headline)
            result.append(article)

        return result

    def _normalize_url(self, url: str) -> str:
        """Strip query params and fragment; return 'netloc/path' for comparison."""
        if not url:
            return ''
        try:
            p = urlparse(url)
            return f"{p.netloc}{p.path}".rstrip('/')
        except Exception:
            return ''

    def _jaccard(self, a: str, b: str) -> float:
        """Token-level Jaccard similarity. No external libraries."""
        sa = set(a.lower().split())
        sb = set(b.lower().split())
        if not sa or not sb:
            return 0.0
        return len(sa & sb) / len(sa | sb)
