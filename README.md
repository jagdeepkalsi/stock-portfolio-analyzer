# Stock Portfolio Analyzer

A Python tool that tracks your stock portfolio and sends daily email summaries with performance metrics.

## Features

- Fetch real-time stock prices using Alpha Vantage API
- Track multiple stocks with purchase prices and quantities
- Calculate gains/losses and daily changes
- Generate formatted daily summaries
- Send email notifications with portfolio updates

## Setup

1. **Get an Alpha Vantage API key:**
   - Visit https://www.alphavantage.co/support/#api-key
   - Sign up for a free API key (500 requests/day)

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API and email credentials:**
   ```bash
   cp .env.example .env
   # Edit .env with your Alpha Vantage API key and email credentials
   ```

4. **Configure your portfolio:**
   - The script creates sample config files on first run
   - Edit `holdings.csv` to add your actual portfolio holdings
   - Update email settings in `portfolio.json`

   For Gmail users:
   - Enable 2-factor authentication
   - Generate an App Password at https://myaccount.google.com/apppasswords
   - Use the App Password (not your regular password)

## Usage

Run the analyzer:
```bash
python portfolio_analyzer.py
```

The script will:
- Fetch current stock prices
- Calculate portfolio performance
- Display summary in terminal
- Send email summary if enabled in settings

### Future Upside Watchlist

Generate a local HTML report for future-facing sectors and potential upside trades:

```bash
python future_upside_report.py --display-limit 50
```

Outputs are written to `out/future_upside_YYYYMMDD.html`,
`out/future_upside_YYYYMMDD.json`, and per-company detail pages in
`out/details/`.

The report scans `future_watchlist.json`, ranks themes such as AI, biotech,
robotics, pharma, cybersecurity, semiconductors, and frontier compute, then
scores companies using price range, momentum, relative volume, sector strength,
and a modeled 45-day call option snapshot. Each company name links to a detail
page with six-month price history, latest company news, and last-four-quarter
earnings reaction trends. Add `POLYGON_API_KEY` to `.env` for the strongest
daily OHLCV history, theme scoring, momentum scoring, detail-page charts, and
earnings reaction windows. Add `FINNHUB_API_KEY` for company news, earnings
calendar details, profiles, and quote/profile enrichment.

Recommended setup:

```bash
echo 'POLYGON_API_KEY=your_key_here' >> .env
echo 'FINNHUB_API_KEY=your_key_here' >> .env
python future_upside_report.py --display-limit 50 --market-provider polygon
```

If Polygon is not configured, the script falls back to Finnhub when
`FINNHUB_API_KEY` is present, then Yahoo. Finnhub/Yahoo can provide useful
quotes and metadata, but they may not reliably return the daily candle history
needed for high-confidence theme rankings.

If Yahoo returns HTTP 429 rate-limit errors, use Finnhub or Polygon:

```bash
echo 'FINNHUB_API_KEY=your_key_here' >> .env
python future_upside_report.py --display-limit 50 --market-provider finnhub
```

For a faster layout-only run without per-company detail pages:

```bash
python future_upside_report.py --display-limit 50 --market-provider polygon --skip-detail-pages
```

For a faster data pull while testing, cap the number of watchlist symbols scanned:

```bash
python future_upside_report.py --scan-limit 25 --display-limit 10 --market-provider polygon
```

The same report includes a separate congressional watchlist section. Defaults
track Nancy Pelosi, Josh Gottheimer, Dan Crenshaw, and Ro Khanna:

```bash
python future_upside_report.py --congress-members "Nancy Pelosi,Josh Gottheimer,Dan Crenshaw,Ro Khanna"
```

### Hosted Future Upside Report

The AWS stack can also generate and host the Future Upside report as static
HTML behind CloudFront. The Lambda writes the latest report and a dated archive
copy into the private portfolio S3 bucket:

```text
future-upside/latest/index.html
future-upside/latest/details/SYMBOL.html
future-upside/archive/YYYY-MM-DD/index.html
```

Before deploying, update `portfolio-analyzer/api-keys` in AWS Secrets Manager:

```json
{
  "alpha_vantage_api_key": "your_alpha_vantage_key",
  "finnhub_api_key": "your_finnhub_key",
  "polygon_api_key": "your_polygon_key"
}
```

Deploy and invoke:

```bash
./deploy.sh
aws lambda invoke --function-name portfolio-future-upside future-response.json
cat future-response.json
```

The Lambda response includes `latest_url`, which points at the CloudFront-hosted
HTML report. The same function is scheduled by EventBridge, and can also be
invoked manually whenever you want to refresh the page.

## Configuration Files

### portfolio.json (Settings)
```json
{
    "settings": {
        "holdings_file": "holdings.csv",
        "send_email": false,
        "email_settings": {
            "recipient": "your-email@example.com",
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587
        }
    }
}
```

**Email Settings:**
- `send_email`: Set to `true` to enable email summaries, `false` to disable
- Default is `false` for testing
- When deploying, change to `true` to enable daily email reports

### holdings.csv (Your Portfolio)
```csv
account_name,account_type,symbol,shares,purchase_price,purchase_date
Fidelity_401k,401k,AAPL,10,150.00,2023-01-15
Fidelity_401k,401k,MSFT,8,300.00,2023-02-01
Schwab_Brokerage,brokerage,GOOGL,5,2500.00,2023-03-10
Schwab_Brokerage,brokerage,TSLA,3,800.00,2023-04-05
```

**Simple CSV Fields:**
- `account_name`: Your brokerage account name
- `account_type`: Type of account (401k, brokerage, IRA, etc.)
- `symbol`: Stock ticker symbol
- `shares`: Number of shares owned
- `purchase_price`: Average purchase price per share
- `purchase_date`: When you bought the stock

**Portfolio Metrics:**
- Benchmark comparison vs S&P 500 and NASDAQ
- Unrealized gains/losses tracking
- Portfolio diversity score and sector allocation (fetched automatically via API)
- Sector data automatically retrieved from Alpha Vantage

## Automation

To run daily, add to crontab:
```bash
# Run at 9 AM weekdays
0 9 * * 1-5 cd /path/to/stock-portfolio-analyzer && python portfolio_analyzer.py
```
