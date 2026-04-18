#!/usr/bin/env python3
"""
News data provider abstraction layer.

Mirrors the pattern in data_providers.py. To add a new news source,
subclass NewsProvider and register it in get_news_provider().

Currently supported:
  - "finnhub": Finnhub news API (reuses existing FINNHUB_API_KEY, free tier: 60 req/min)

Article dict shape returned by all providers:
{
    'id':        int,
    'symbol':    str | None,   # None for market-wide news
    'category':  str,
    'datetime':  int,          # Unix timestamp
    'headline':  str,
    'summary':   str,
    'source':    str,
    'url':       str,
    'image':     str,
    'related':   str,
}
"""

import logging
import time
from abc import ABC, abstractmethod

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
        """
        Make a GET request to the Finnhub API.
        Returns the parsed JSON list, or [] on any error.
        """
        params['token'] = self.api_key
        url = f"{self.BASE_URL}{endpoint}"
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            # Some endpoints return a dict on error
            logger.warning("Finnhub news: unexpected response shape for %s: %s", endpoint, data)
            return []
        except requests.RequestException as e:
            logger.warning("Finnhub news request failed for %s: %s", endpoint, e)
            return []

    def get_company_news(self, symbol: str, from_date: str, to_date: str) -> list:
        """
        Fetch company-specific news from /company-news.
        Injects 'symbol' key into every article for downstream use.
        """
        time.sleep(self.request_delay)
        articles = self._get('/company-news', {
            'symbol': symbol,
            'from': from_date,
            'to': to_date,
        })
        for article in articles:
            article['symbol'] = symbol
        return articles

    def get_market_news(self, category: str = 'general') -> list:
        """
        Fetch market-wide news from /news.
        Sets symbol=None on each article.
        """
        time.sleep(self.request_delay)
        articles = self._get('/news', {'category': category})
        for article in articles:
            article.setdefault('symbol', None)
        return articles


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_news_provider(provider_name: str = 'finnhub', finnhub_key: str = None) -> NewsProvider:
    """
    Return a configured NewsProvider instance.

    Args:
        provider_name: Provider identifier string (currently only 'finnhub')
        finnhub_key:   Finnhub API key (required when provider_name='finnhub')

    Raises:
        ValueError: If provider_name is unknown or required credentials are missing.
    """
    if provider_name == 'finnhub':
        if not finnhub_key:
            raise ValueError(
                "FINNHUB_API_KEY is required for the finnhub news provider. "
                "Set it in your .env file or as an environment variable."
            )
        return FinnhubNewsProvider(api_key=finnhub_key)

    raise ValueError(
        f"Unknown news provider: '{provider_name}'. "
        "Supported providers: ['finnhub']"
    )
