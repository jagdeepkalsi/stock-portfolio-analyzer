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