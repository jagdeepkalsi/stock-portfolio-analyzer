# Future Upside Report Design

## Purpose

The Future Upside Watchlist is a screening report for future-facing public companies. It is built to identify companies in AI, semiconductors, robotics, biotech, pharma, cybersecurity, quantum, space, and related themes that may be trading at attractive levels with catalysts or momentum that could support upside.

This report is not investment advice. It is a ranked research aid. The output is generated as local static HTML plus JSON, with optional per-company detail pages.

## Entry Point

Primary script:

```bash
python3 future_upside_report.py --display-limit 50 --market-provider polygon
```

Important flags:

- `--display-limit`: number of ranked companies shown in the HTML report. `--limit` is a backward-compatible alias.
- `--scan-limit`: caps how many watchlist symbols are fetched and scored before ranking. Useful for fast testing.
- `--market-provider`: `auto`, `polygon`, `finnhub`, or `yahoo`.
- `--skip-options`: skips modeled option snapshot generation.
- `--skip-detail-pages`: skips per-company detail pages.
- `--congress-members`: comma-separated list of politicians to track.
- `--congress-lookback-days`: recent disclosure window for the congressional trade table.

Output files:

- `out/future_upside_YYYYMMDD.html`
- `out/future_upside_YYYYMMDD.json`
- `out/details/SYMBOL.html`

## Data Sources

### Watchlist Universe

Source:

- `future_watchlist.json`

What it contains:

- Future-facing themes.
- Theme descriptions.
- Sector/theme ETFs used for theme scoring.
- Curated company tickers assigned to each theme.

Current behavior:

- The report does not discover stocks from the full market.
- It ranks companies from the curated universe in `future_watchlist.json`.
- If a symbol appears in multiple themes, the symbol is deduplicated and keeps all theme memberships.

### Historical Market Data

Preferred source:

- Polygon daily adjusted OHLCV bars.

Required key:

- `POLYGON_API_KEY`

Endpoint shape:

- `/v2/aggs/ticker/{symbol}/range/1/day/{from}/{to}`

Used for:

- Theme ETF one-week and one-month momentum.
- Company one-day, five-day, and one-month price movement.
- 52-week low/high fallback when provider metrics are unavailable.
- Relative volume calculation.
- Six-month company detail-page chart.
- Earnings reaction windows before and after earnings dates.
- Realized volatility when possible.

Fallback sources:

- Finnhub candles and quote data when `FINNHUB_API_KEY` is present.
- Stooq daily history when `STOOQ_API_KEY` is present.
- Yahoo Finance chart endpoint when explicitly selected or no stronger provider is configured.

Known caveat:

- Finnhub and Yahoo may return quote-level data or fail due to rate limits. When daily candles are missing, theme rankings remain neutral and detail charts may show a limited-data message.

### Finnhub Enrichment

Required key:

- `FINNHUB_API_KEY`

Used for:

- Current quote/profile enrichment.
- 52-week high/low metrics.
- Company name, industry, and market cap.
- Company news.
- Earnings calendar rows.

Finnhub is useful even when Polygon is the primary history provider.

### Congressional Data

Source:

- CapitolTrades public disclosure pages and BFF endpoint, via `congress_providers.py`.

Used for:

- Recent tracked trades.
- Inferred disclosed portfolio cards.

Important caveat:

- Congressional public data is not a live portfolio feed. STOCK Act data reports trades and public disclosures. Annual financial disclosures may contain holdings, but are delayed and range-based. The report therefore labels the portfolio section as inferred from disclosed trades.

## Main HTML Sections

### Header

Shows:

- Report title.
- Generation timestamp.
- Number of symbols scanned.
- Total symbols in the watchlist universe.
- Number of companies displayed.

Data source:

- Runtime metadata from `build_report()`.
- `future_watchlist.json` for total universe size.

Logic:

- `total_symbols` is the number of unique symbols after flattening the watchlist.
- `scanned_symbols` is the number actually scored after applying `--scan-limit`.
- Displayed company count is the number remaining after ranking and applying `--display-limit`.

### Best Setups

Shows:

- Top five ranked companies.
- Ticker.
- Setup label.
- Score.
- Target upside field.

Data source:

- The ranked company list created by `score_company()`.

Logic:

- Companies are sorted by descending score.
- The first five are shown as the quick summary.
- Target upside is currently a placeholder because analyst target aggregation is not wired yet.

### Report Notes

Shows:

- Scoring disclaimer.
- Options disclaimer.
- Investment-advice disclaimer.

Data source:

- Static copy in `render_html()`.

Logic:

- These notes make clear that scores are screening aids and options are modeled estimates, not live quotes.

### Theme Ranking

Shows one card per future-facing theme:

- Theme score.
- Theme name.
- Theme description.
- Theme status.
- One-week average ETF change.
- One-month average ETF change.
- ETF symbols used for the theme.

Data sources:

- Theme definitions from `future_watchlist.json`.
- Historical ETF daily prices from the configured market provider, ideally Polygon.

Logic:

Each theme starts at a neutral score of `50.0`.

The score is adjusted by:

- Average one-week ETF performance.
- Average one-month ETF performance.
- Status bonus/penalty based on whether the theme looks like reversal, momentum, weak, mixed, or limited data.

Status rules:

- Reversal: one-week average positive, one-month average negative.
- Momentum: one-week and one-month averages both positive.
- Weak: one-week and one-month averages both negative.
- Mixed: available data does not match the above.
- Limited data: one-week or one-month ETF data is missing.

Why all themes can show `50.0`:

- If the configured provider does not return usable daily ETF history, one-week and one-month averages are `None`.
- With no ETF performance data, the score remains at the neutral baseline of `50.0`.

### Top Companies Table

Shows one ranked row per displayed company:

- Rank.
- Company/ticker.
- Theme and industry.
- Score.
- Setup label.
- Risk label.
- Current price.
- One-day change.
- Five-day change.
- One-month change.
- Percent above 52-week low.
- Percent below 52-week high.
- Target upside.
- Relative volume.
- Market cap.
- Options snapshot.

Data sources:

- Company universe from `future_watchlist.json`.
- Historical prices from Polygon or fallback provider.
- Finnhub quote/profile/metrics enrichment when available.
- Theme scores from the Theme Ranking section.

Company scoring logic:

Each company starts from a base score of `35.0`.

The score is adjusted by:

- Theme score above or below neutral.
- Proximity to 52-week low.
- Distance below 52-week high.
- Five-day price strength.
- One-month pullback or momentum.
- Relative volume spike.
- Market-cap stability.
- Penalty for very low price stocks.

Signals shown in the setup cell are generated from the same scoring inputs, such as:

- Limited data theme.
- Near 52w low.
- Discounted range.
- Well below high.
- Five-day strength.
- Pullback.
- One-month momentum.
- Volume spike.

Setup label logic:

- `Target upside`: target upside is high, once target data is wired.
- `Oversold turn`: close to 52-week low with positive five-day movement.
- `Breakout`: close to 52-week high with strong five-day movement.
- `Strong setup`: score is high.
- `Watchlist`: default.

Risk label logic:

- Risk increases for small market cap, high beta when available, very low stock price, or extreme proximity to 52-week low.
- Current labels are `Moderate`, `High`, and `Very high`.

Known caveat:

- Analyst target upside is currently not populated. The column is present for future target-price integration.

### Options Snapshot

Shows:

- Approximate 45-day expiration date.
- At-the-money strike.
- Modeled ATM bid/ask.
- Roughly 10% out-of-the-money strike.
- Breakeven upside percentage.

Data sources:

- Current stock price.
- Historical realized volatility when available.

Logic:

- Uses a Black-Scholes call model.
- Uses approximately 45 days to expiration.
- Rounds strikes according to price level.
- Uses 60-day realized volatility when available.
- Falls back to a conservative default volatility when history is unavailable.

Important caveat:

- These are not live option-chain bid/ask prices.
- Open interest, live volume, and real spreads are not currently included.
- This is a planning estimate only.

### Congressional Watchlist

Shows one card per tracked politician:

- Name.
- Trade count.
- Buy count.
- Sell count.
- Maximum disclosed buys.
- Maximum disclosed sells.
- Inferred disclosed portfolio.
- Most active disclosed tickers.

Also shows a Recent Tracked Trades table:

- Member.
- Ticker.
- Action.
- Size range.
- Transaction date.
- Disclosure date and disclosure delay.

Data sources:

- CapitolTrades public disclosures.
- `congress_providers.fetch_recent_trades()` for the recent table.
- `congress_providers.fetch_member_trades()` for member-specific inferred portfolio cards.

Default tracked members:

- Nancy Pelosi.
- Josh Gottheimer.
- Dan Crenshaw.
- Ro Khanna.

Alias handling:

- Ro Khanna is also matched as Rohit Khanna.
- Dan Crenshaw is also matched as Daniel Crenshaw.

Recent table logic:

- Uses `--congress-lookback-days`, default `365`.
- Filters trades by disclosure date.
- Keeps trades matching the tracked names or aliases.
- Sorts by disclosure date and disclosed amount.

Inferred portfolio logic:

- Pulls up to 1095 days of member-specific disclosed trades.
- Aggregates by ticker.
- Computes maximum disclosed buys minus maximum disclosed sells.
- Shows tickers with positive net disclosed buy amount.

Important caveats:

- This is not guaranteed current holdings.
- Public disclosures report ranges, not exact share counts.
- Sells may not map perfectly to prior buys.
- Options, funds, spouses, trusts, and non-equity assets may need additional normalization.
- Some politician IDs may return no data from CapitolTrades even if the generic feed has data elsewhere.

## Company Detail Pages

Each company name in the Top Companies table links to `out/details/SYMBOL.html` when detail pages are enabled.

### Detail Header

Shows:

- Back link.
- Ticker and company name.
- Theme.
- Current price.
- Score.
- Setup.
- Risk label.

Data source:

- Ranked company row from the main report.

### Six-Month Price History

Shows:

- SVG line/area chart.
- First and last date/price.
- Six-month percentage change.
- Six-month low/high range.
- Number of trading points.
- Report generation time.

Data source:

- Daily historical prices from the configured market provider, ideally Polygon.

Logic:

- Uses one year of fetched history.
- Filters to approximately the last six months.
- Requires at least 20 points to render a meaningful daily chart.
- If fewer than 20 points are available, the page shows a limited-data message instead of a misleading straight-line chart.

### Latest News

Shows:

- Five to ten recent articles.
- Date.
- Source.
- Headline.
- Short summary.
- Link to source article.

Data source:

- Finnhub company news endpoint.

Logic:

- Fetches roughly the last 30 days.
- Sorts newest to oldest.
- Limits to 10 articles.

### Earnings Reaction Table

Shows last four earnings rows when available:

- Earnings date.
- Quarter/year.
- EPS actual.
- EPS estimate.
- EPS surprise.
- Five-day pre-earnings move.
- Next-day post-earnings move.
- Five-day post-earnings move.

Data sources:

- Finnhub earnings calendar.
- Historical daily close data from the configured market provider.

Logic:

- Fetches recent earnings calendar rows.
- Finds nearby closing prices in historical data.
- Computes:
  - `pre_5d_pct`: move from five trading days before earnings to the previous close.
  - `next_day_pct`: move from previous close to next trading day close.
  - `post_5d_pct`: move from previous close to five trading days after earnings.

Known caveat:

- Accuracy depends on daily candle availability around each earnings event.
- Earnings timing before open or after close is displayed when Finnhub provides it, but the current reaction logic uses a simple previous/next trading-day window.

## JSON Output

The JSON file mirrors the HTML report data and is useful for debugging or future Lambda/email rendering.

Top-level fields include:

- `generated_at`
- `generated_at_display`
- `watchlist_path`
- `total_symbols`
- `scanned_symbols`
- `scan_limit`
- `display_limit`
- `themes`
- `companies`
- `company_details`
- `congress`
- `notes`

## Lambda/Email Plan

The report generator should remain the single source of business logic.

Recommended Lambda shape:

- Lambda handler loads API keys from environment variables or Secrets Manager.
- Handler calls `build_report()`.
- Handler writes HTML/JSON to `/tmp`.
- Handler either:
  - sends an inline HTML email summary,
  - attaches the generated HTML,
  - or uploads full HTML/detail pages to S3 and emails a link.

Recommended final shape:

- Email contains a concise summary and top-ranked table.
- Full report and detail pages are uploaded to S3.
- Email links to the hosted HTML report.

## Open Enhancements

- Add a true analyst target-price data source.
- Add live options chain pricing instead of modeled Black-Scholes estimates.
- Add annual financial disclosure parsing for a better congressional holdings view.
- Add S3 publishing and Lambda email mode.
- Add provider health checks in the HTML so users can see when a section is degraded.
- Add a configurable scoring weights file so scoring can be tuned without editing Python.
