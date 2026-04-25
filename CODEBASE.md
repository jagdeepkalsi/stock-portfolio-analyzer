# Stock Portfolio Analyzer — Codebase Reference

> Use this file at the start of a new session to get up to speed quickly.
> Working directory: `/Users/jagdeepkalsi/stock-portfolio-analyzer/.claude/worktrees/sleepy-gates`

---

## What This Project Does

Three independent emails sent to **jagdeep.kalsi@gmail.com**:

| Email | Script | Schedule | Purpose |
|-------|--------|----------|---------|
| Portfolio Summary | `portfolio_analyzer.py` | 2:00 PM ET (weekdays) | Total value, unrealized gains, sector allocation, benchmark vs S&P/NASDAQ |
| Pre-Market News Digest | `news_alert.py` | 7:30 AM ET (weekdays) | 3-section news digest: market news, portfolio impact stories, per-holding news |
| Market Pulse | `market_digest.py` | daily | Market trend snapshot (daily+weekly) + Congressional trades (Pelosi priority) |

Both run locally via cron **or** on AWS Lambda + EventBridge (same code, different entry points).

---

## File Map

```
portfolio_analyzer.py     Local runner — portfolio summary email
lambda_function.py        AWS Lambda handlers for BOTH portfolio summary and news alert
data_providers.py         Stock price provider abstraction (Finnhub / yfinance / Alpha Vantage)
news_providers.py         News provider abstraction (Finnhub / Alpha Vantage / MarketWatch RSS)
news_scorer.py            Article scoring, impact tagging, deduplication
news_alert.py             News alert orchestrator + HTML email renderer
market_trends.py          Index/sector/macro performance via yfinance (1D/1W/1M)
congress_providers.py     House + Senate Stock Watcher feed adapters
market_digest.py          Market Pulse orchestrator + HTML email renderer
holdings.csv              Portfolio holdings (gitignored — real data)
portfolio.json            Runtime config (gitignored — real data)
.env                      API keys + SMTP credentials (gitignored)
cloudformation-template.yaml  AWS infrastructure (2 Lambdas, 2 EventBridge rules, S3, Secrets Manager)
deploy.sh                 Build + deploy to AWS Lambda
requirements.txt          Local dependencies
requirements-lambda.txt   Lambda dependencies
```

---

## Config: `portfolio.json`

```json
{
  "settings": {
    "holdings_file": "holdings.csv",
    "send_email": false,
    "email_settings": {
      "recipient": "jagdeep.kalsi@gmail.com",
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587
    },
    "news_alerts": {
      "enabled": true,
      "send_email": true,
      "providers": ["finnhub", "alpha_vantage", "marketwatch_rss"],
      "lookback_days": 1,
      "min_score_threshold": 3,
      "max_market_articles": 10,
      "max_portfolio_articles": 10,
      "max_company_articles": 3,
      "max_top_companies": 5,
      "dedup_window_hours": 24,
      "keyword_overrides": {}
    }
  }
}
```

**Key flags:**
- `send_email: false` on the top-level settings = portfolio summary email is disabled locally (Lambda handles it)
- `news_alerts.enabled: true` = news alert is active
- `news_alerts.send_email: true` = sends the news email

---

## Config: `.env`

```
DATA_PROVIDER=finnhub
FINNHUB_API_KEY=d7hcf0hr01qhiu0b52lgd7hcf0hr01qhiu0b52m0
SMTP_USER=jagdeep.kalsi@gmail.com
SMTP_PASSWORD=qrtv jtuw ewdm vutq
ALPHA_VANTAGE_API_KEY=          ← NOT YET SET — add this to enable AV news + sentiment
```

---

## Holdings: `holdings.csv`

```
account_name,account_type,symbol,shares,purchase_price,purchase_date
Fidelity_401k,401k,AAPL,10,150.00,2023-01-15
Fidelity_401k,401k,MSFT,8,300.00,2023-01-15
Schwab_Brokerage,brokerage,GOOGL,5,2500.00,2023-01-15
Schwab_Brokerage,brokerage,TSLA,3,800.00,2023-01-15
```

**Position values** (shares × purchase_price, used to rank companies in news digest):
- GOOGL: $12,500 → #1
- MSFT:  $2,400  → #2
- TSLA:  $2,400  → #3
- AAPL:  $1,500  → #4

---

## Module: `data_providers.py`

Pluggable stock price layer. Selected via `DATA_PROVIDER` env var.

```python
class StockDataProvider(ABC):
    def get_stock_data(self, symbol: str) -> dict | None
    # Returns: {symbol, current_price, previous_close, change, change_percent, volume, company_name, sector}

class FinnhubProvider(StockDataProvider)   # default, 60 req/min free
class YFinanceProvider(StockDataProvider)  # free, no key, rate-limited
class AlphaVantageProvider(StockDataProvider)  # 25 req/day free

def get_provider(name, alpha_vantage_key=None, finnhub_key=None) -> StockDataProvider
```

---

## Module: `news_providers.py`

Pluggable news layer. Three providers, all implement the same interface.

```python
class NewsProvider(ABC):
    def get_company_news(self, symbol, from_date, to_date) -> list[dict]
    def get_market_news(self, category='general') -> list[dict]

# Article dict shape:
# {id, symbol, category, datetime (unix), headline, summary, source, url, image, related, av_sentiment_score}

class FinnhubNewsProvider(NewsProvider)
    # /company-news  → per-ticker, 60 req/min free
    # /news?category=general|merger  → market-wide

class AlphaVantageNewsProvider(NewsProvider)
    # NEWS_SENTIMENT endpoint
    # One batch call for ALL symbols at once: ?tickers=AAPL,MSFT,GOOGL,TSLA
    # Sets av_sentiment_score on each article (-0.35 Bearish … +0.35 Bullish)
    # Call prime_cache(symbols, from_date) before iterating per-symbol to use 1 API call total
    # 25 req/day free — 1 call covers whole portfolio ✓
    # ⚠️  ALPHA_VANTAGE_API_KEY not in .env yet

class MarketWatchRSSProvider(NewsProvider)
    # https://feeds.content.dowjones.io/public/rss/mw_topstories
    # No API key, no new dependencies (stdlib xml.etree + urllib)
    # get_company_news() always returns [] (no ticker filter in RSS)
    # Updates every 60 seconds

def get_news_providers(provider_names, finnhub_key, alpha_vantage_key) -> list[NewsProvider]
    # Missing keys → provider skipped with warning (never raises)

def get_news_provider(name, finnhub_key) -> NewsProvider
    # Legacy single-provider factory (backward compat)
```

---

## Module: `news_scorer.py`

Scoring, tagging, and deduplication.

```python
class NewsScorer:
    # Scoring layers (additive, applied in order):
    #   1. Keyword scan   — ~60 keywords × point values (M&A=8, earnings beat=9, upgrade=6, recession=7, ...)
    #   2. Noise cap      — if "preview/roundup/sponsored/what to watch" → cap score at 2
    #   3. Recency bonus  — article < 3h old → +2
    #   4. Portfolio boost — article mentions a holding → +3
    #   5. Sentiment bonus — |av_sentiment_score| >= 0.15 → +2
    #   Final: min(total, 100)

    def score(article) -> int                              # 0–100
    def extract_impact_tags(article) -> list[str]
        # Tags: M&A, EARNINGS BEAT, EARNINGS MISS, GUIDANCE UP, GUIDANCE DOWN,
        #       UPGRADE, DOWNGRADE, FDA, BANKRUPTCY, FRAUD/LEGAL, CEO CHANGE,
        #       EXEC CHANGE, DIVIDEND, BUYBACK, FED/MACRO, BULLISH, BEARISH
    def tag_portfolio_impact(article, portfolio_symbols) -> list[str]
        # Scans headline+summary+related for ticker symbols
        # Mutates article['portfolio_impact'] = [matching symbols]

class NewsDeduplicator:
    # Three layers (first occurrence wins; company news processed before market → wins ties):
    #   1. Exact article ID     — same Finnhub/AV id seen twice
    #   2. Normalized URL       — same story different domain (strips query params)
    #   3. Headline Jaccard ≥ 0.70 — near-identical headline (wire reformats)
    # stats dict: {id, url, jaccard, old} — logged after each run

    def filter(articles) -> list[dict]
```

**Score tiers → email badge colors:**
- ≥ 8 = critical → 🔴 red
- 5–7 = high → 🟠 orange
- 3–4 = medium → 🟡 yellow

---

## Module: `news_alert.py`

Main orchestrator for the news digest. Entry point: `python news_alert.py`

### Key classes and functions

```python
class NewsAlertGenerator:
    # __init__(portfolio_file='portfolio.json')
    # Loads: portfolio config, holdings data (with position values), news config, providers, scorer, deduper

    def _load_holdings_data() -> dict
        # Returns {symbol: {company_name, position_value}}
        # position_value = sum(shares × purchase_price) — used to rank companies

    def fetch_all_news(lookback_days=1) -> dict
        # {company_news: {SYM: [raw articles]}, market_news: [raw articles]}
        # Flow: Finnhub company → AV batch (primed once) → Finnhub general/merger → MarketWatch RSS

    def score_and_rank_news(raw) -> dict
        # {company_news: {SYM: [scored]}, market_news: [scored], all_scored_flat: [scored+deduped]}
        # IMPORTANT: per-company dedup is independent (avoids cross-symbol article loss)
        # Cross-feed dedup only applied to market_news and all_scored_flat

    def build_digest_data(scored) -> DigestData  # pure JSON-serializable dict
        # Section 1: market_news          → top 10 market articles by score
        # Section 2: portfolio_impact_news → top 10 from all_scored_flat where portfolio_impact non-empty
        # Section 3: top_companies         → top 5 by position_value, 3 articles each

    def run()  # full pipeline: fetch → score → build → email

def render_email_html(digest) -> str   # standalone HTML email
def render_json(digest) -> str         # JSON output for future web API
def send_news_email(html, portfolio_config) -> bool  # Gmail SMTP
```

### DigestData shape

```python
{
  'generated_at': str,               # ISO timestamp
  'generated_at_display': str,       # "April 18, 2026 · 7:30 AM ET"
  'stats': {'articles_shown': int},
  'high_impact_symbols': [str, ...], # symbols with any score >= 8

  'market_news': [article, ...],           # Section 1 — top 10
  'portfolio_impact_news': [article, ...], # Section 2 — top 10
  'top_companies': [                       # Section 3 — top 5
    {
      'symbol': str, 'company_name': str,
      'position_value': float, 'rank': int,
      'articles': [article, ...]  # top 3
    }
  ]
}

# Article shape in DigestData:
# {id, symbol, score, score_tier, headline, summary, source, url,
#  age_display, impact_tags, portfolio_impact, is_portfolio_impact, av_sentiment_score}
```

---

## Module: `portfolio_analyzer.py`

Local runner for the portfolio summary email.

```python
class PortfolioAnalyzer:
    def load_portfolio()          # reads portfolio.json
    def load_holdings()           # reads holdings.csv, merges multi-lot positions (weighted avg cost)
    def analyze_portfolio()       # fetches all prices, computes gains/losses, sector allocation
    def get_benchmark_performance() # SPY + QQQ daily change %
    def calculate_diversity_score() # Herfindahl-Hirschman Index → 0-100
    def generate_summary()        # plain text summary
    def generate_html_summary()   # full HTML email
    def send_email_summary()      # Gmail SMTP
    def run_daily_analysis()      # entry point
```

---

## Module: `lambda_function.py`

AWS Lambda entry points. Two handlers in one file, deployed as one zip.

```python
class LambdaPortfolioAnalyzer:
    # Mirrors PortfolioAnalyzer but reads config from S3, secrets from Secrets Manager
    def initialize_credentials()  # loads API keys + SMTP from Secrets Manager
    def load_portfolio_config()    # loads portfolio.json from S3
    def load_holdings()            # loads holdings.csv from S3
    # ... same analysis/email methods as PortfolioAnalyzer

def lambda_handler(event, context)       # portfolio summary — triggered 2 PM ET daily
    # EventBridge: cron(0 19 * * ? *)

class LambdaNewsAlertGenerator:
    # Wraps LambdaPortfolioAnalyzer for credential/S3/SMTP reuse
    # Delegates news logic to news_alert.py, news_providers.py, news_scorer.py
    def initialize()
    def run() -> dict
    def _send_email(html, generated_at)

def news_alert_handler(event, context)   # news digest — triggered 7:30 AM ET weekdays
    # EventBridge: cron(30 12 ? * MON-FRI *)

class LambdaMarketDigestGenerator:
    # Reuses LambdaPortfolioAnalyzer for credential/S3/SMTP.
    # Delegates data + rendering to market_digest.py → market_trends.py + congress_providers.py.
    def initialize()    # loads credentials + portfolio_config + market_digest cfg
    def run() -> dict
    def _send_email(html, generated_at)

def market_digest_handler(event, context)   # Market Pulse — triggered daily 6 PM ET
    # EventBridge: cron(0 23 * * ? *)
    # Lambda: portfolio-market-digest, 512 MB, 300s timeout, DATA_PROVIDER=yfinance
```

**AWS Secrets Manager paths:**
- `portfolio-analyzer/api-keys` → `{finnhub_api_key, alpha_vantage_api_key}`
- `portfolio-analyzer/email-config` → `{smtp_user, smtp_password}`

**S3 Bucket:** `{stack-name}-portfolio-data`
- `portfolio.json`
- `holdings.csv`

---

## Module: `market_trends.py`

Free market-wide snapshot via yfinance. Pulls ~2 months of daily bars per symbol
and computes 1D / 5-session (1W) / 21-session (1M) % changes from the same
close series so periods are consistent.

```python
INDEXES  = [SPY, QQQ, IWM, DIA]
SECTORS  = [XLK, XLF, XLV, XLY, XLP, XLE, XLI, XLB, XLRE, XLU, XLC]  # SPDRs
MACRO    = [^VIX, ^TNX, DX-Y.NYB, CL=F, GC=F, BTC-USD]

def fetch_market_trends() -> dict
    # {
    #   'generated_at': ISO, 'generated_at_display': str,
    #   'indexes' | 'sectors' | 'macro': [{symbol, label, price, change_1d/_1w/_1m}, ...],
    #   'sector_movers': {'winners': [...3], 'losers': [...3]},  # by 1W
    #   'highlights': ['bullet', ...],  # deterministic, max 6
    # }
```

Highlight rules (plain English bullets): S&P direction, SPY-vs-IWM spread,
QQQ-vs-SPY spread, top/bottom sector, VIX >=10% move, 10Y yield >=3% move,
BTC >=5% move, oil >=4% move. Graceful per-symbol failure — other rows still
render. Pure yfinance (no key).

---

## Module: `congress_providers.py`

Normalized adapter for CapitolTrades House + Senate disclosures.

```python
CAPITOL_TRADES_URL     = "https://bff.capitoltrades.com/trades"
CAPITOL_TRADES_WEB_URL = "https://www.capitoltrades.com/trades"
PRIORITY_MEMBERS = {"pelosi"}  # lowercased substring match

def fetch_recent_trades(lookback_days=7, basis="disclosure") -> dict
    # basis='disclosure' → filter by when the PTR became public (default)
    # basis='transaction' → filter by actual trade date
    # Returns:
    # {
    #   lookback_days, basis, cutoff_date,
    #   counts: {house, senate, priority, total},
    #   priority_trades: [...newest first],       # Pelosi etc
    #   all_trades:      [...sorted by amount desc],
    # }
```

Normalized trade shape: `{chamber, member, party, state, district, ticker,
asset, transaction (purchase|sale|exchange|other), raw_type, amount_range,
amount_min, amount_max, transaction_date, disclosure_date, days_to_disclose,
ptr_url, is_priority}`.

Caveats: disclosures can lag actual trades by days or weeks. The provider tries
CapitolTrades' BFF JSON endpoint first, then falls back to parsing the
server-rendered `/trades?page=N` payload if the BFF is blocked or unavailable.
The fallback uses CapitolTrades' displayed transaction value as the normalized
amount.

---

## Module: `market_digest.py`

Main orchestrator for the Market Pulse email. Entry point: `python market_digest.py`.

```python
def load_digest_config(portfolio) -> dict
    # settings.market_digest keys (all optional):
    #   enabled, send_email, congress_lookback_days, congress_basis,
    #   max_priority_trades, max_other_trades

def build_digest(portfolio, cfg) -> dict
    # { generated_at, generated_at_display,
    #   trends:   <fetch_market_trends output>,
    #   congress: <fetch_recent_trades output + 'other_trades' = all minus priority> }

def render_email_html(digest) -> str
def send_email(html, portfolio, subject_date) -> bool
```

CLI:
- `python market_digest.py`        — build + send (reads `portfolio.json`)
- `python market_digest.py --dry`  — build + write `out/market_pulse_YYYYMMDD.{html,json}`, skip email
- `python market_digest.py --config path.json`

Email layout:
- Header banner (black) with generated timestamp
- Section 1: bullet highlights + Major indexes / Sector rotation / Macro tape tables
- Section 2: congressional trade counts summary + Priority table (yellow bg) + All others (by size)
- 1D / 1W / 1M columns with green (+) / red (-) coloring; tabular-nums for numeric alignment

---

## Running Locally

```bash
cd /Users/jagdeepkalsi/stock-portfolio-analyzer/.claude/worktrees/sleepy-gates

# News alert (test email + console output)
python news_alert.py

# Portfolio summary
python portfolio_analyzer.py

# Market Pulse (trends + congress)
python market_digest.py          # send email
python market_digest.py --dry    # write to ./out/ instead

# Cron schedule (add to crontab -e)
30 7 * * 1-5  cd /path/to/app && python news_alert.py >> logs/news_alert.log 2>&1
0 14 * * 1-5  cd /path/to/app && python portfolio_analyzer.py >> logs/portfolio.log 2>&1
```

---

## AWS Deployment

```bash
# Full deploy (build zip → S3 → CloudFormation → update both Lambda functions)
bash deploy.sh

# Update Finnhub key in Secrets Manager
aws secretsmanager put-secret-value \
  --secret-id portfolio-analyzer/api-keys \
  --secret-string '{"finnhub_api_key":"d7hcf0hr01qhiu0b52lgd7hcf0hr01qhiu0b52m0","alpha_vantage_api_key":"YOUR_KEY"}' \
  --region us-west-2

# Upload config to S3
aws s3 cp portfolio.json s3://portfolio-analyzer-deployment-jagdeep/
aws s3 cp holdings.csv   s3://portfolio-analyzer-deployment-jagdeep/

# Test invoke news alert Lambda
aws lambda invoke --function-name portfolio-news-alert --region us-west-2 response.json && cat response.json

# Watch logs
aws logs tail /aws/lambda/portfolio-news-alert --follow --region us-west-2

# Invoke Market Pulse Lambda on demand
aws lambda invoke --function-name portfolio-market-digest --region us-west-2 market-response.json && cat market-response.json

# Watch Market Pulse logs
aws logs tail /aws/lambda/portfolio-market-digest --follow --region us-west-2
```

**Market Pulse config (portfolio.json → settings.market_digest):**

```json
{
  "enabled":                true,
  "send_email":             true,
  "congress_lookback_days": 7,
  "congress_basis":         "disclosure",
  "max_priority_trades":    25,
  "max_other_trades":       50
}
```

---

## Known TODOs / Next Steps

1. **Add `ALPHA_VANTAGE_API_KEY` to `.env`** — enables sentiment scores (`[BULLISH]`/`[BEARISH]` tags) and a second company news source. Get a free key at https://www.alphavantage.co/support/#api-key

2. **Web UI** — `build_digest_data()` returns a JSON-serializable `DigestData` dict. To serve a browser UI, add a Flask/FastAPI endpoint that calls `NewsAlertGenerator` and returns `render_json(digest)`. No changes to the data layer needed.

3. **Push branch + open PR** — branch is `claude/sleepy-gates`, remote push requires GitHub credentials. Run:
   ```bash
   git push origin claude/sleepy-gates
   gh pr create --title "Add proactive pre-market news alert system" --base main
   ```

4. **AWS deployment** — CloudFormation template and deploy.sh are ready. Needs AWS CLI configured with credentials.

5. **Polygon.io** — Richer metadata (keywords, insights, multi-ticker per article). Would add as a 4th provider. Needs free API key at https://polygon.io

---

## Architecture Principles

- **Pluggable providers** — both `data_providers.py` and `news_providers.py` use an ABC pattern. New sources implement the interface and register in the factory function.
- **DigestData as the handoff** — `build_digest_data()` produces a pure JSON dict. Email and future web renderers are decoupled from the data pipeline.
- **Per-company dedup is independent** — cross-feed dedup (URL + Jaccard) only applies to market news and the portfolio-impact section, not per-company sections. This prevents valid articles from being dropped when the same story appears in multiple tickers' company feeds.
- **Graceful degradation** — missing API keys skip that provider with a warning; the run continues with whatever providers are available.
