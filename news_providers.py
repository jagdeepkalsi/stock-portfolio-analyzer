#!/usr/bin/env python3
"""
News data provider abstraction layer.

Mirrors the pattern in data_providers.py. To add a new news source,
subclass NewsProvider and register it in get_news_providers().

Supported providers:
  - "finnhub":        Finnhub news API (FINNHUB_API_KEY, 60 req/min free)
  - "alpha_vantage":  Alpha Vantage NEWS_SENTIMENT (ALPHA_VANTAGE_API_KEY, 25 req/day free)
                      One batch call covers all portfolio symbols. Adds per-ticker
                      sentiment scores (av_sentiment_score key) to each article.
  - "marketwatch_rss": MarketWatch top stories RSS feed (no key, unlimited, stdlib only)

Article dict shape returned by all providers:
{
    'id':                int | str,  # Finnhub: int, AV/RSS: str hash
    'symbol':            str | None, # None for market-wide news
    'category':          str,
    'datetime':          int,        # Unix timestamp
    'headline':          str,
    'summary':           str,
    'source':            str,
    'url':               str,
    'image':             str,
    'related':           str,
    'av_sentiment_score': float,     # Alpha Vantage only; 0.0 otherwise
}
"""

import hashlib
import logging
import time
import urllib.request
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class NewsProvider(ABC):
    """Common interface every news provider must implement."""

    @abstractmethod
    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list:
        """
        Fetch news articles for a single stock symbol.

        Args:
            symbol:    Ticker symbol (e.g. "AAPL")
            from_date: Start date string "YYYY-MM-DD"
            to_date:   End date string "YYYY-MM-DD"

        Returns:
            List of article dicts. Empty list on failure — never raises.
        """

    @abstractmethod
    def get_market_news(self, category: str = 'general') -> list:
        """
        Fetch market-wide news.

        Args:
            category: One of 'general', 'forex', 'crypto', 'merger'

        Returns:
            List of article dicts. Empty list on failure — never raises.
        """


def _blank_article() -> dict:
    """Return a blank article dict with all expected keys at safe defaults."""
    return {
        'id': None,
        'symbol': None,
        'category': 'company',
        'datetime': 0,
        'headline': '',
        'summary': '',
        'source': '',
        'url': '',
        'image': '',
        'related': '',
        'av_sentiment_score': 0.0,
    }


# ---------------------------------------------------------------------------
# Finnhub provider
# ---------------------------------------------------------------------------

class FinnhubNewsProvider(NewsProvider):
    """
    Uses the Finnhub REST API for news data.
    Reuses the same API key as FinnhubProvider in data_providers.py.

    Free tier: 60 requests/minute.
    request_delay of 0.12s between calls keeps a 20-symbol portfolio
    well under that limit (~2.6s total for all company + market calls).
    """

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_key: str, request_delay: float = 0.12):
        self.api_key = api_key
        self.request_delay = request_delay

    def _get(self, endpoint: str, params: dict) -> list:
        params['token'] = self.api_key
        url = f"{self.BASE_URL}{endpoint}"
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            logger.warning("Finnhub news: unexpected response for %s: %s", endpoint, data)
            return []
        except requests.RequestException as e:
            logger.warning("Finnhub news request failed for %s: %s", endpoint, e)
            return []

    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list:
        time.sleep(self.request_delay)
        articles = self._get('/company-news', {
            'symbol': symbol,
            'from': from_date,
            'to': to_date,
        })
        for a in articles:
            a['symbol'] = symbol
            a.setdefault('av_sentiment_score', 0.0)
        return articles

    def get_market_news(self, category: str = 'general') -> list:
        time.sleep(self.request_delay)
        articles = self._get('/news', {'category': category})
        for a in articles:
            a.setdefault('symbol', None)
            a.setdefault('av_sentiment_score', 0.0)
        return articles


# ---------------------------------------------------------------------------
# Alpha Vantage News Sentiment provider
# ---------------------------------------------------------------------------

class AlphaVantageNewsProvider(NewsProvider):
    """
    Uses the Alpha Vantage NEWS_SENTIMENT endpoint.

    Key features:
    - One batch call covers ALL portfolio symbols (comma-separated tickers param)
    - Returns per-ticker sentiment scores (-0.35 Bearish … +0.35 Bullish)
    - Score is injected as 'av_sentiment_score' for the scorer to use

    Free tier: 25 requests/day — one batch call per run is all we need.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._cache: list | None = None  # articles fetched for all symbols at once

    def _fetch_batch(self, symbols: list, from_date: str) -> list:
        """
        Fetch NEWS_SENTIMENT for all symbols in a single API call.
        Results are cached so subsequent per-symbol calls are free.
        """
        if self._cache is not None:
            return self._cache

        # AV expects time_from as "YYYYMMDDTHHMM"
        try:
            dt = datetime.strptime(from_date, '%Y-%m-%d')
            time_from = dt.strftime('%Y%m%dT0000')
        except ValueError:
            time_from = None

        params = {
            'function': 'NEWS_SENTIMENT',
            'tickers':  ','.join(symbols),
            'limit':    '50',
            'apikey':   self.api_key,
        }
        if time_from:
            params['time_from'] = time_from

        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if 'feed' not in data:
                logger.warning("Alpha Vantage news: unexpected response: %s", str(data)[:200])
                self._cache = []
                return []

            articles = []
            for item in data.get('feed', []):
                a = _blank_article()

                # Parse timestamp "YYYYMMDDTHHMMSS" → Unix
                try:
                    ts_str = item.get('time_published', '')
                    dt_obj = datetime.strptime(ts_str, '%Y%m%dT%H%M%S')
                    a['datetime'] = int(dt_obj.replace(tzinfo=timezone.utc).timestamp())
                except (ValueError, TypeError):
                    a['datetime'] = 0

                a['headline'] = item.get('title', '')
                a['summary']  = item.get('summary', '')
                a['url']      = item.get('url', '')
                a['source']   = item.get('source', 'Alpha Vantage')
                a['image']    = item.get('banner_image', '')
                a['category'] = 'company'
                a['id']       = 'av_' + hashlib.md5(a['url'].encode()).hexdigest()[:12]

                # Extract the highest-magnitude sentiment across all tickers
                best_score = 0.0
                best_symbol = None
                for ts in item.get('ticker_sentiment', []):
                    try:
                        score = float(ts.get('ticker_sentiment_score', 0))
                        if abs(score) > abs(best_score):
                            best_score = score
                            best_symbol = ts.get('ticker', '').upper()
                    except (ValueError, TypeError):
                        pass

                a['av_sentiment_score'] = best_score
                a['symbol']  = best_symbol
                a['related'] = best_symbol or ''

                articles.append(a)

            self._cache = articles
            return articles

        except requests.RequestException as e:
            logger.warning("Alpha Vantage news request failed: %s", e)
            self._cache = []
            return []

    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list:
        """
        Returns articles from the batch where the best-sentiment ticker matches symbol.
        The batch is fetched only once regardless of how many symbols are in the portfolio.
        """
        # symbols list is needed for the batch call; we use symbol as a proxy here
        # The caller (NewsAlertGenerator) calls this once per symbol with the same from_date
        # _fetch_batch caches so only the first call hits the network
        batch = self._fetch_batch([symbol], from_date)
        return [a for a in batch if a.get('symbol') == symbol.upper()]

    def get_market_news(self, category: str = 'general') -> list:
        """Alpha Vantage has no market-wide feed — return empty."""
        return []

    def prime_cache(self, symbols: list, from_date: str):
        """
        Call this once before iterating per-symbol to load all articles in one API call.
        The result is cached; subsequent get_company_news() calls just filter the cache.
        """
        self._fetch_batch(symbols, from_date)


# ---------------------------------------------------------------------------
# MarketWatch RSS provider
# ---------------------------------------------------------------------------

class MarketWatchRSSProvider(NewsProvider):
    """
    Fetches MarketWatch top stories via their public RSS feed.
    No API key required. Uses only stdlib (xml.etree + urllib).
    Provides market-wide editorial coverage distinct from Finnhub's wire feeds.
    """

    RSS_URL = "https://feeds.content.dowjones.io/public/rss/mw_topstories"
    # Dublin Core namespace used by MarketWatch RSS for author field
    DC_NS = 'http://purl.org/dc/elements/1.1/'

    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list:
        """RSS has no ticker filter — company news returns empty."""
        return []

    def get_market_news(self, category: str = 'general') -> list:
        """Fetch and parse the MarketWatch RSS feed."""
        try:
            req = urllib.request.Request(
                self.RSS_URL,
                headers={'User-Agent': 'portfolio-news-alert/1.0'}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()

            root = ET.fromstring(raw)
            channel = root.find('channel')
            if channel is None:
                return []

            articles = []
            for item in channel.findall('item'):
                a = _blank_article()

                a['headline'] = (item.findtext('title') or '').strip()
                a['summary']  = (item.findtext('description') or '').strip()
                a['url']      = (item.findtext('link') or '').strip()
                a['source']   = 'MarketWatch'
                a['category'] = 'general'
                a['symbol']   = None

                # Stable ID from guid or URL
                guid = item.findtext('guid') or a['url']
                a['id'] = 'mw_' + hashlib.md5(guid.encode()).hexdigest()[:12]

                # Parse RFC-2822 pubDate → Unix timestamp
                pub_date = item.findtext('pubDate') or ''
                try:
                    dt_obj = parsedate_to_datetime(pub_date)
                    a['datetime'] = int(dt_obj.timestamp())
                except Exception:
                    a['datetime'] = int(time.time())

                # Author from dc:creator if present
                creator = item.findtext(f'{{{self.DC_NS}}}creator')
                if creator:
                    a['source'] = f"MarketWatch / {creator.strip()}"

                # Image from media:content if present (ignore namespace complexity)
                for child in item:
                    if 'content' in child.tag and child.get('medium') == 'image':
                        a['image'] = child.get('url', '')
                        break

                if a['headline']:
                    articles.append(a)

            logger.info("MarketWatch RSS: fetched %d articles", len(articles))
            return articles

        except Exception as e:
            logger.warning("MarketWatch RSS fetch failed: %s", e)
            return []


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_news_providers(
    provider_names: list,
    finnhub_key: str = None,
    alpha_vantage_key: str = None,
) -> list:
    """
    Return a list of configured NewsProvider instances for the requested names.

    Providers with missing credentials are skipped with a warning (not raised),
    so a missing optional key never breaks the whole run.

    Args:
        provider_names:    e.g. ["finnhub", "alpha_vantage", "marketwatch_rss"]
        finnhub_key:       Required for "finnhub"
        alpha_vantage_key: Required for "alpha_vantage"

    Returns:
        List of NewsProvider instances in the requested order.
    """
    providers = []
    for name in provider_names:
        if name == 'finnhub':
            if not finnhub_key:
                logger.warning("Skipping finnhub provider: FINNHUB_API_KEY not set")
                continue
            providers.append(FinnhubNewsProvider(api_key=finnhub_key))

        elif name == 'alpha_vantage':
            if not alpha_vantage_key:
                logger.warning("Skipping alpha_vantage provider: ALPHA_VANTAGE_API_KEY not set")
                continue
            providers.append(AlphaVantageNewsProvider(api_key=alpha_vantage_key))

        elif name == 'marketwatch_rss':
            providers.append(MarketWatchRSSProvider())

        else:
            logger.warning("Unknown news provider '%s' — skipping", name)

    if not providers:
        raise ValueError(
            "No news providers could be initialized. "
            "Check your API keys and provider names in portfolio.json."
        )

    return providers


# Keep old single-provider factory for backward compatibility with existing tests
def get_news_provider(provider_name: str = 'finnhub', finnhub_key: str = None) -> NewsProvider:
    return get_news_providers([provider_name], finnhub_key=finnhub_key)[0]
