#!/usr/bin/env python3
"""
Stock data provider abstraction layer.

To switch providers, set the DATA_PROVIDER environment variable:
  - "finnhub"        : Finnhub (default, free tier: 60 req/min, requires API key)
  - "yfinance"       : Yahoo Finance (free, no API key, but prone to rate limiting)
  - "alpha_vantage"  : Alpha Vantage (requires API key, better for production)

All providers return the same dict shape from get_stock_data():
{
    'symbol':         str,
    'current_price':  float,
    'previous_close': float,
    'change':         float,   # current_price - previous_close
    'change_percent': float,   # daily % change
    'volume':         int,
    'company_name':   str,
    'sector':         str
}
"""

import logging
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class StockDataProvider(ABC):
    """Common interface every provider must implement."""

    @abstractmethod
    def get_stock_data(self, symbol: str) -> dict | None:
        """
        Fetch quote + company info for a single symbol.
        Returns the standard dict above, or None if the data cannot be fetched.
        """


# ---------------------------------------------------------------------------
# Yahoo Finance provider (yfinance) — free, no API key
# ---------------------------------------------------------------------------

class YFinanceProvider(StockDataProvider):
    """
    Uses the yfinance library to pull data from Yahoo Finance.
    Free, no rate-limit key required, supports virtually all US and
    international tickers.  Not suitable for intraday precision or
    real-time latency < 15 min.
    """

    def get_stock_data(self, symbol: str) -> dict | None:
        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)

            # fast_info is a lightweight endpoint — price + volume only
            fast = ticker.fast_info
            current_price = fast.last_price
            prev_close    = fast.previous_close

            if not current_price:
                logger.warning(f"[yfinance] No price data for {symbol}")
                return None

            prev_close    = prev_close or current_price
            change        = current_price - prev_close
            change_pct    = (change / prev_close * 100) if prev_close else 0
            volume        = int(fast.last_volume or 0)

            # info gives company name + sector (slightly slower, cached by yfinance)
            info         = ticker.info
            company_name = info.get('longName') or info.get('shortName') or symbol
            sector       = info.get('sector') or 'Unknown'

            logger.info(f"[yfinance] {symbol}: ${current_price:.2f} ({change_pct:+.2f}%)")
            return {
                'symbol':         symbol,
                'current_price':  float(current_price),
                'previous_close': float(prev_close),
                'change':         float(change),
                'change_percent': float(change_pct),
                'volume':         volume,
                'company_name':   company_name,
                'sector':         sector,
            }

        except Exception as e:
            logger.error(f"[yfinance] Error fetching {symbol}: {e}")
            return None


# ---------------------------------------------------------------------------
# Alpha Vantage provider — paid/free tier, reliable for production
# ---------------------------------------------------------------------------

class AlphaVantageProvider(StockDataProvider):
    """
    Uses the Alpha Vantage REST API.
    Free tier: 25 requests/day, 5/min.
    Premium tiers unlock higher limits and real-time data.

    Requires an API key passed to the constructor.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str, request_delay: float = 0.5):
        """
        Args:
            api_key:       Your Alpha Vantage API key.
            request_delay: Seconds to sleep between successive calls
                           (default 0.5 s keeps free tier under 5 req/min).
        """
        if not api_key:
            raise ValueError("AlphaVantageProvider requires a non-empty api_key")
        self.api_key      = api_key
        self.request_delay = request_delay

    def _get(self, params: dict) -> dict:
        import requests as _requests
        resp = _requests.get(self.BASE_URL, params={**params, 'apikey': self.api_key}, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def get_stock_data(self, symbol: str) -> dict | None:
        try:
            # --- quote ---
            data = self._get({'function': 'GLOBAL_QUOTE', 'symbol': symbol})

            if 'Error Message' in data:
                logger.error(f"[alpha_vantage] API error for {symbol}: {data['Error Message']}")
                return None
            if 'Note' in data:
                logger.warning(f"[alpha_vantage] Rate limit hit for {symbol}: {data['Note']}")
                return None

            quote = data.get('Global Quote', {})
            if not quote:
                logger.warning(f"[alpha_vantage] No quote data for {symbol}")
                return None

            current_price = float(quote.get('05. price', 0))
            prev_close    = float(quote.get('08. previous close', 0))
            volume        = int(quote.get('06. volume', 0))
            change        = current_price - prev_close
            change_pct    = (change / prev_close * 100) if prev_close else 0

            # --- company overview ---
            company_name = symbol
            sector       = 'Unknown'
            try:
                time.sleep(self.request_delay)
                overview = self._get({'function': 'OVERVIEW', 'symbol': symbol})
                if overview and 'Sector' in overview:
                    sector       = overview.get('Sector', 'Unknown')
                    company_name = overview.get('Name', symbol)
            except Exception as e:
                logger.warning(f"[alpha_vantage] Could not fetch overview for {symbol}: {e}")

            logger.info(f"[alpha_vantage] {symbol}: ${current_price:.2f} ({change_pct:+.2f}%)")
            return {
                'symbol':         symbol,
                'current_price':  current_price,
                'previous_close': prev_close,
                'change':         change,
                'change_percent': change_pct,
                'volume':         volume,
                'company_name':   company_name,
                'sector':         sector,
            }

        except Exception as e:
            logger.error(f"[alpha_vantage] Error fetching {symbol}: {e}")
            return None


# ---------------------------------------------------------------------------
# Finnhub provider — free tier: 60 req/min, reliable, requires API key
# ---------------------------------------------------------------------------

class FinnhubProvider(StockDataProvider):
    """
    Uses the Finnhub REST API.
    Free tier: 60 API calls/minute — comfortably handles large portfolios.
    Requires a free API key from https://finnhub.io

    Two calls per symbol:
      - /quote        → price, prev close, change, volume
      - /stock/profile2 → company name, sector
    """

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FinnhubProvider requires a non-empty api_key")
        self.api_key = api_key

    def _get(self, endpoint: str, params: dict) -> dict:
        import requests as _requests
        resp = _requests.get(
            f"{self.BASE_URL}/{endpoint}",
            params={**params, 'token': self.api_key},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()

    def get_stock_data(self, symbol: str) -> dict | None:
        try:
            # --- quote ---
            quote = self._get('quote', {'symbol': symbol})

            current_price = float(quote.get('c') or 0)
            prev_close    = float(quote.get('pc') or 0)

            if not current_price:
                logger.warning(f"[finnhub] No price data for {symbol}")
                return None

            prev_close  = prev_close or current_price
            change      = current_price - prev_close
            change_pct  = (change / prev_close * 100) if prev_close else 0
            volume      = int(quote.get('v') or 0)

            # --- company profile ---
            company_name = symbol
            sector       = 'Unknown'
            try:
                profile = self._get('stock/profile2', {'symbol': symbol})
                company_name = profile.get('name') or symbol
                sector       = profile.get('finnhubIndustry') or 'Unknown'
            except Exception as e:
                logger.warning(f"[finnhub] Could not fetch profile for {symbol}: {e}")

            logger.info(f"[finnhub] {symbol}: ${current_price:.2f} ({change_pct:+.2f}%)")
            return {
                'symbol':         symbol,
                'current_price':  current_price,
                'previous_close': prev_close,
                'change':         change,
                'change_percent': change_pct,
                'volume':         volume,
                'company_name':   company_name,
                'sector':         sector,
            }

        except Exception as e:
            logger.error(f"[finnhub] Error fetching {symbol}: {e}")
            return None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_provider(
    provider_name: str = 'finnhub',
    alpha_vantage_key: str = None,
    finnhub_key: str = None,
) -> StockDataProvider:
    """
    Return the correct provider instance.

    Usage:
        provider = get_provider(os.getenv('DATA_PROVIDER', 'finnhub'), finnhub_key=key)
        data = provider.get_stock_data('AAPL')

    Args:
        provider_name:     'finnhub' (default), 'yfinance', or 'alpha_vantage'
        finnhub_key:       Required when provider_name == 'finnhub'
        alpha_vantage_key: Required when provider_name == 'alpha_vantage'
    """
    name = (provider_name or 'finnhub').lower().strip()

    if name == 'finnhub':
        logger.info("Using Finnhub data provider")
        return FinnhubProvider(api_key=finnhub_key)

    if name == 'yfinance':
        logger.info("Using YFinance data provider")
        return YFinanceProvider()

    if name == 'alpha_vantage':
        logger.info("Using Alpha Vantage data provider")
        return AlphaVantageProvider(api_key=alpha_vantage_key)

    raise ValueError(
        f"Unknown DATA_PROVIDER '{provider_name}'. "
        "Valid options: 'finnhub', 'yfinance', 'alpha_vantage'"
    )
