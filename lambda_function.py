#!/usr/bin/env python3
"""
AWS Lambda Portfolio Analyzer
Fetches stock prices and generates daily portfolio summaries
"""

import json
import os
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
import boto3
import logging
from botocore.exceptions import ClientError
import io

from data_providers import get_provider

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class LambdaPortfolioAnalyzer:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.secrets_client = boto3.client('secretsmanager')
        self.bucket_name = None
        self.alpha_vantage_key = None
        self.smtp_user = None
        self.smtp_password = None
        self.portfolio_config = None
        self.data_provider = None
        
    def get_secret(self, secret_name):
        """Retrieve secret from AWS Secrets Manager"""
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])
        except ClientError as e:
            logger.error(f"Error retrieving secret {secret_name}: {e}")
            raise
    
    def get_s3_object(self, bucket, key):
        """Download object from S3"""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Error downloading {key} from S3: {e}")
            raise
    
    def initialize_credentials(self):
        """Initialize API keys and configuration from AWS services"""
        try:
            provider_name    = os.environ.get('DATA_PROVIDER', 'finnhub').lower()
            finnhub_key      = None
            alpha_vantage_key = None

            # Load the API key for whichever provider is selected
            if provider_name == 'finnhub':
                api_secrets = self.get_secret('portfolio-analyzer/api-keys')
                finnhub_key = api_secrets.get('finnhub_api_key')
                logger.info("Finnhub API key loaded from Secrets Manager")

            elif provider_name == 'alpha_vantage':
                api_secrets       = self.get_secret('portfolio-analyzer/api-keys')
                alpha_vantage_key = api_secrets.get('alpha_vantage_api_key')
                logger.info("Alpha Vantage API key loaded from Secrets Manager")

            # yfinance needs no key — nothing to load

            # Email credentials are always required
            email_secrets = self.get_secret('portfolio-analyzer/email-config')
            self.smtp_user     = email_secrets['smtp_user']
            self.smtp_password = email_secrets['smtp_password']

            # S3 bucket from environment
            self.bucket_name = os.environ.get('S3_BUCKET_NAME')
            if not self.bucket_name:
                raise ValueError("S3_BUCKET_NAME environment variable not set")

            # Initialise the data provider
            self.data_provider = get_provider(
                provider_name,
                alpha_vantage_key=alpha_vantage_key,
                finnhub_key=finnhub_key,
            )
            logger.info(f"Data provider set to: {provider_name}")
            logger.info("Successfully initialized credentials")

        except Exception as e:
            logger.error(f"Failed to initialize credentials: {e}")
            raise
    
    def load_portfolio_config(self):
        """Load portfolio configuration from S3"""
        try:
            config_data = self.get_s3_object(self.bucket_name, 'portfolio.json')
            self.portfolio_config = json.loads(config_data.decode('utf-8'))
            logger.info("Successfully loaded portfolio configuration")
            return self.portfolio_config
        except Exception as e:
            logger.error(f"Failed to load portfolio configuration: {e}")
            raise
    
    def load_holdings(self):
        """Load holdings from S3 CSV file.

        Multiple rows for the same symbol in the same account (e.g. RSU lots
        with different vest prices) are merged into a single position using
        total shares and a weighted-average purchase price.
        """
        try:
            csv_data = self.get_s3_object(self.bucket_name, 'holdings.csv')
            df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))

            holdings = {"accounts": {}}

            for _, row in df.iterrows():
                account_name  = row['account_name']
                account_type  = row['account_type']
                symbol        = row['symbol']
                shares        = float(row['shares'])
                purchase_price = float(row['purchase_price'])
                purchase_date = row.get('purchase_date', '')

                if account_name not in holdings["accounts"]:
                    holdings["accounts"][account_name] = {
                        "account_type": account_type,
                        "holdings": {}
                    }

                acct_holdings = holdings["accounts"][account_name]["holdings"]

                if symbol not in acct_holdings:
                    acct_holdings[symbol] = {
                        "shares": shares,
                        "purchase_price": purchase_price,
                        "purchase_date": purchase_date
                    }
                else:
                    # Merge lot: accumulate shares, recalculate weighted avg cost
                    existing = acct_holdings[symbol]
                    total_shares = existing["shares"] + shares
                    weighted_avg_price = (
                        (existing["shares"] * existing["purchase_price"]) +
                        (shares * purchase_price)
                    ) / total_shares
                    existing["shares"] = total_shares
                    existing["purchase_price"] = round(weighted_avg_price, 4)
                    logger.info(
                        f"Merged lot for {symbol} in {account_name}: "
                        f"{total_shares} total shares @ ${weighted_avg_price:.4f} avg cost"
                    )

            logger.info(f"Successfully loaded holdings for {len(holdings['accounts'])} accounts")
            return holdings

        except Exception as e:
            logger.error(f"Failed to load holdings: {e}")
            raise
    
    def get_stock_data(self, symbol):
        """Fetch stock data via the configured data provider."""
        return self.data_provider.get_stock_data(symbol)
    
    def analyze_portfolio(self, holdings):
        """Analyze the entire portfolio across all accounts with enhanced metrics"""
        portfolio_data = []
        total_value = 0
        total_gain_loss = 0
        sector_allocation = {}
        
        for account_name, account_info in holdings.get('accounts', {}).items():
            logger.info(f"Processing account: {account_name}")
            
            for symbol, holdings_data in account_info.get('holdings', {}).items():
                logger.info(f"Fetching data for {symbol}")
                stock_data = self.get_stock_data(symbol)
                
                if stock_data:
                    shares = holdings_data['shares']
                    purchase_price = holdings_data['purchase_price']
                    current_value = stock_data['current_price'] * shares
                    purchase_value = purchase_price * shares
                    unrealized_gain_loss = current_value - purchase_value
                    gain_loss_percent = (unrealized_gain_loss / purchase_value) * 100 if purchase_value else 0
                    
                    # Track sector allocation using API data
                    sector = stock_data.get('sector', 'Unknown')
                    if sector not in sector_allocation:
                        sector_allocation[sector] = 0
                    sector_allocation[sector] += current_value
                    
                    portfolio_data.append({
                        'account': account_name,
                        'account_type': account_info.get('account_type', 'unknown'),
                        'symbol': symbol,
                        'sector': sector,
                        'company_name': stock_data['company_name'],
                        'shares': shares,
                        'purchase_price': purchase_price,
                        'current_price': stock_data['current_price'],
                        'current_value': current_value,
                        'unrealized_gain_loss': unrealized_gain_loss,
                        'gain_loss_percent': gain_loss_percent,
                        'daily_change': stock_data['change'] * shares,
                        'daily_change_percent': stock_data['change_percent']
                    })
                    
                    total_value += current_value
                    total_gain_loss += unrealized_gain_loss
                else:
                    logger.warning(f"Failed to get data for {symbol} in {account_name}")
        
        # Get benchmark data
        benchmark_data = self.get_benchmark_performance()
        
        return portfolio_data, total_value, total_gain_loss, sector_allocation, benchmark_data
    
    def get_benchmark_performance(self):
        """Get benchmark performance for S&P 500 and NASDAQ"""
        benchmarks = {}
        
        try:
            # S&P 500 (SPY)
            spy_data = self.get_stock_data('SPY')
            if spy_data:
                benchmarks['S&P 500'] = {
                    'symbol': 'SPY',
                    'current_price': spy_data['current_price'],
                    'daily_change_percent': spy_data['change_percent']
                }
            
            # NASDAQ (QQQ)
            qqq_data = self.get_stock_data('QQQ')
            if qqq_data:
                benchmarks['NASDAQ'] = {
                    'symbol': 'QQQ', 
                    'current_price': qqq_data['current_price'],
                    'daily_change_percent': qqq_data['change_percent']
                }
        except Exception as e:
            logger.error(f"Error fetching benchmark data: {e}")
        
        return benchmarks
    
    def calculate_diversity_score(self, sector_allocation, total_portfolio_value):
        """Calculate portfolio diversity score using Herfindahl-Hirschman Index"""
        if not sector_allocation or total_portfolio_value == 0:
            return 0
        
        # Calculate concentration (HHI)
        hhi = 0
        for sector_value in sector_allocation.values():
            market_share = sector_value / total_portfolio_value
            hhi += market_share ** 2
        
        # Convert to diversity score (0-100, higher = more diverse)
        diversity_score = max(0, (1 - hhi) * 100)
        return round(diversity_score, 1)
    
    def generate_html_summary(self, portfolio_data, total_value, total_unrealized_gains, sector_allocation, benchmark_data):
        """Generate an HTML version of the portfolio summary for email"""
        if not portfolio_data:
            return "<html><body><h2>No portfolio data available.</h2></body></html>"

        # ── Pre-compute metrics ──────────────────────────────────────────────
        total_purchase_value  = sum(d['purchase_price'] * d['shares'] for d in portfolio_data)
        total_gain_loss_pct   = (total_unrealized_gains / total_purchase_value * 100) if total_purchase_value else 0
        diversity_score       = self.calculate_diversity_score(sector_allocation, total_value)
        portfolio_color       = "#28a745" if total_unrealized_gains >= 0 else "#dc3545"

        # Group by account
        accounts = {}
        for d in portfolio_data:
            accounts.setdefault(d['account'], []).append(d)

        # Per-account aggregates
        account_stats = {}
        for acct_name, holdings in accounts.items():
            acct_value        = sum(h['current_value']       for h in holdings)
            acct_unrealized   = sum(h['unrealized_gain_loss'] for h in holdings)
            acct_daily_change = sum(h['daily_change']         for h in holdings)
            prev_value        = acct_value - acct_daily_change
            acct_daily_pct    = (acct_daily_change / prev_value * 100) if prev_value else 0
            account_stats[acct_name] = {
                'type':         holdings[0]['account_type'].title(),
                'value':        acct_value,
                'unrealized':   acct_unrealized,
                'daily_change': acct_daily_change,
                'daily_pct':    acct_daily_pct,
            }

        # Top 5 gainers / losers (by daily %)
        sorted_by_day = sorted(portfolio_data, key=lambda d: d['daily_change_percent'], reverse=True)
        top_gainers   = sorted_by_day[:5]
        top_losers    = sorted_by_day[-5:][::-1]   # worst first

        # ── HTML ─────────────────────────────────────────────────────────────
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 1300px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; margin-bottom: 20px; }}
        /* top metric cards */
        .overview {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .metric-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 1.5em; font-weight: bold; margin-top: 5px; }}
        /* account summary box */
        .account-summary-box {{ border: 1px solid #dee2e6; border-radius: 8px; margin-bottom: 24px; overflow: hidden; }}
        .account-summary-box h3 {{ margin: 0; padding: 10px 15px; background-color: #343a40; color: white; font-size: 1em; }}
        .account-summary-table {{ width: 100%; border-collapse: collapse; }}
        .account-summary-table th {{ background-color: #495057; color: #ccc; padding: 8px 12px; text-align: left; font-size: 0.85em; font-weight: 600; }}
        .account-summary-table td {{ padding: 9px 12px; border-bottom: 1px solid #f0f0f0; font-size: 0.9em; }}
        .account-summary-table tr:last-child td {{ border-bottom: none; }}
        .account-summary-table tr:nth-child(even) td {{ background-color: #f8f9fa; }}
        /* movers */
        .movers-section {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
        .movers-box {{ border-radius: 8px; overflow: hidden; border: 1px solid #dee2e6; }}
        .movers-box h3 {{ margin: 0; padding: 10px 14px; font-size: 0.95em; }}
        .gainers-title {{ background-color: #d4edda; color: #155724; }}
        .losers-title  {{ background-color: #f8d7da; color: #721c24; }}
        .movers-table {{ width: 100%; border-collapse: collapse; }}
        .movers-table td {{ padding: 7px 12px; border-bottom: 1px solid #f0f0f0; font-size: 0.88em; }}
        .movers-table tr:last-child td {{ border-bottom: none; }}
        /* benchmark */
        .benchmark-section {{ background-color: #e9ecef; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .benchmark-item {{ display: inline-block; margin: 5px 15px; padding: 5px 10px; background-color: #6c757d; color: white; border-radius: 5px; }}
        /* sector */
        .sector-section {{ background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .sector-item {{ margin: 5px 0; padding: 5px; background-color: #e9ecef; border-radius: 5px; }}
        /* account detail tables */
        .account-section {{ margin-bottom: 30px; }}
        .account-header {{ background-color: #007bff; color: white; padding: 10px; border-radius: 8px 8px 0 0; margin-bottom: 0; }}
        .holdings-table {{ width: 100%; border-collapse: collapse; margin-bottom: 0; }}
        .holdings-table th {{ background-color: #343a40; color: white; padding: 10px; text-align: left; }}
        .holdings-table td {{ padding: 8px; border-bottom: 1px solid #dee2e6; }}
        .holdings-table tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .account-footer {{ background-color: #e9ecef; padding: 8px 12px; border-top: 2px solid #dee2e6; border-radius: 0 0 8px 8px; font-size: 0.9em; margin-bottom: 20px; }}
        .positive {{ color: #28a745; font-weight: bold; }}
        .negative {{ color: #dc3545; font-weight: bold; }}
        .neutral  {{ color: #6c757d; }}
    </style>
</head>
<body>
<div class="container">

    <div class="header">
        <h1>📊 Daily Portfolio Summary</h1>
        <p>{datetime.now().strftime('%B %d, %Y')}</p>
    </div>

    <!-- ① Top-level metrics -->
    <div class="overview">
        <div class="metric-card">
            <div>Total Portfolio Value</div>
            <div class="metric-value">${total_value:,.2f}</div>
        </div>
        <div class="metric-card">
            <div>Unrealized Gains/Loss</div>
            <div class="metric-value" style="color:{portfolio_color};">${total_unrealized_gains:,.2f} ({total_gain_loss_pct:+.2f}%)</div>
        </div>
        <div class="metric-card">
            <div>Diversity Score</div>
            <div class="metric-value">{diversity_score}/100</div>
        </div>
    </div>

    <!-- ① Account summary box -->
    <div class="account-summary-box">
        <h3>🏦 Account Totals</h3>
        <table class="account-summary-table">
            <thead>
                <tr>
                    <th>Account</th>
                    <th>Type</th>
                    <th>Value</th>
                    <th>Unrealized P&L</th>
                    <th>Today's Change $</th>
                    <th>Today's Change %</th>
                </tr>
            </thead>
            <tbody>"""

        for acct_name, s in account_stats.items():
            unr_color   = "#28a745" if s['unrealized']   >= 0 else "#dc3545"
            daily_color = "#28a745" if s['daily_change'] >= 0 else "#dc3545"
            html += f"""
                <tr>
                    <td><strong>{acct_name}</strong></td>
                    <td>{s['type']}</td>
                    <td>${s['value']:,.2f}</td>
                    <td style="color:{unr_color};font-weight:bold;">${s['unrealized']:+,.2f}</td>
                    <td style="color:{daily_color};font-weight:bold;">${s['daily_change']:+,.2f}</td>
                    <td style="color:{daily_color};font-weight:bold;">{s['daily_pct']:+.2f}%</td>
                </tr>"""

        html += """
            </tbody>
        </table>
    </div>

    <!-- ② Top 5 Gainers / Losers -->
    <div class="movers-section">
        <div class="movers-box">
            <h3 class="gainers-title">🚀 Top 5 Gainers Today</h3>
            <table class="movers-table">"""

        for d in top_gainers:
            html += f"""
                <tr>
                    <td><strong>{d['symbol']}</strong></td>
                    <td>{d['company_name']}</td>
                    <td class="positive">{d['daily_change_percent']:+.2f}%</td>
                    <td class="positive">${d['daily_change']:+,.0f}</td>
                </tr>"""

        html += """
            </table>
        </div>
        <div class="movers-box">
            <h3 class="losers-title">📉 Top 5 Losers Today</h3>
            <table class="movers-table">"""

        for d in top_losers:
            html += f"""
                <tr>
                    <td><strong>{d['symbol']}</strong></td>
                    <td>{d['company_name']}</td>
                    <td class="negative">{d['daily_change_percent']:+.2f}%</td>
                    <td class="negative">${d['daily_change']:+,.0f}</td>
                </tr>"""

        html += """
            </table>
        </div>
    </div>

    <!-- Benchmark -->
    <div class="benchmark-section">
        <h3>🎯 Benchmark Comparison</h3>"""

        for benchmark_name, data in benchmark_data.items():
            color = "#28a745" if data['daily_change_percent'] >= 0 else "#dc3545"
            html += f'<span class="benchmark-item" style="background-color:{color};">{benchmark_name}: {data["daily_change_percent"]:+.2f}%</span>'

        html += f"""
    </div>

    <!-- Sector allocation -->
    <div class="sector-section">
        <h3>📊 Sector Allocation</h3>"""

        for sector, value in sorted(sector_allocation.items(), key=lambda x: x[1], reverse=True):
            pct = (value / total_value) * 100
            html += f'<div class="sector-item"><strong>{sector}:</strong> ${value:,.0f} ({pct:.1f}%)</div>'

        html += """
    </div>

    <h3>🏦 Holdings by Account</h3>"""

        # ④ Per-account detail tables
        for account_name, holdings_list in accounts.items():
            s             = account_stats[account_name]
            unr_color     = "#90EE90" if s['unrealized']   >= 0 else "#FFB6C1"
            daily_color   = "#90EE90" if s['daily_change'] >= 0 else "#FFB6C1"
            sorted_hold   = sorted(holdings_list, key=lambda x: x['daily_change_percent'], reverse=True)

            html += f"""
    <div class="account-section">
        <div class="account-header">
            <strong>{account_name} ({s['type']})</strong> &nbsp;|&nbsp;
            Value: ${s['value']:,.2f} &nbsp;|&nbsp;
            Unrealized: <span style="color:{unr_color};">${s['unrealized']:+,.2f}</span>
        </div>
        <table class="holdings-table">
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Company</th>
                    <th>Sector</th>
                    <th>Price</th>
                    <th>Shares</th>
                    <th>Value</th>
                    <th>Daily Change %</th>
                    <th>Daily Change $</th>
                </tr>
            </thead>
            <tbody>"""

            for d in sorted_hold:
                cc = "positive" if d['daily_change_percent'] >= 0 else "negative"
                html += f"""
                <tr>
                    <td><strong>{d['symbol']}</strong></td>
                    <td>{d['company_name']}</td>
                    <td>{d['sector']}</td>
                    <td>${d['current_price']:.2f}</td>
                    <td>{d['shares']:,.4g}</td>
                    <td>${d['current_value']:,.0f}</td>
                    <td class="{cc}">{d['daily_change_percent']:+.2f}%</td>
                    <td class="{cc}">${d['daily_change']:+,.0f}</td>
                </tr>"""

            # ③ Account daily aggregate footer
            footer_daily_color = "#28a745" if s['daily_change'] >= 0 else "#dc3545"
            html += f"""
            </tbody>
        </table>
        <div class="account-footer">
            <strong>Today's account total:</strong>
            <span style="color:{footer_daily_color};font-weight:bold;">
                ${s['daily_change']:+,.2f} ({s['daily_pct']:+.2f}%)
            </span>
            &nbsp;—&nbsp; {len(holdings_list)} position{'s' if len(holdings_list) != 1 else ''}
        </div>
    </div>"""

        html += """
</div>
</body>
</html>"""

        return html
    
    def send_email_summary(self, html_summary):
        """Send portfolio summary via email with HTML formatting"""
        try:
            email_settings = self.portfolio_config.get('settings', {}).get('email_settings', {})
            recipient = email_settings.get('recipient')
            
            if not recipient:
                raise ValueError("Email recipient not configured")
            
            # Create multipart message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.smtp_user
            msg['To'] = recipient
            msg['Subject'] = f"📊 Daily Portfolio Summary - {datetime.now().strftime('%B %d, %Y')}"
            
            # Create HTML version
            html_part = MIMEText(html_summary, 'html')
            msg.attach(html_part)
            
            # Send email
            server = smtplib.SMTP(email_settings.get('smtp_server', 'smtp.gmail.com'), 
                                email_settings.get('smtp_port', 587))
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.send_message(msg)
            server.quit()
            
            logger.info("HTML email sent successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            raise

def lambda_handler(event, context):
    """
    AWS Lambda handler function
    """
    start_time = datetime.now()
    logger.info(f"Portfolio analysis started at {start_time}")
    
    try:
        # Initialize the analyzer
        analyzer = LambdaPortfolioAnalyzer()
        
        # Initialize credentials and configuration
        analyzer.initialize_credentials()
        analyzer.load_portfolio_config()
        
        # Check if email is enabled
        send_email = analyzer.portfolio_config.get('settings', {}).get('send_email', False)
        if not send_email:
            logger.info("Email sending is disabled in configuration")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Portfolio analysis completed but email sending is disabled',
                    'timestamp': start_time.isoformat()
                })
            }
        
        # Load holdings and analyze portfolio
        holdings = analyzer.load_holdings()
        portfolio_data, total_value, total_gain_loss, sector_allocation, benchmark_data = analyzer.analyze_portfolio(holdings)
        
        if not portfolio_data:
            logger.warning("No portfolio data found")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No portfolio data available',
                    'timestamp': start_time.isoformat()
                })
            }
        
        # Generate and send HTML email
        html_summary = analyzer.generate_html_summary(portfolio_data, total_value, total_gain_loss, sector_allocation, benchmark_data)
        analyzer.send_email_summary(html_summary)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Portfolio analysis completed successfully in {execution_time:.2f} seconds")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Portfolio analysis completed successfully',
                'timestamp': start_time.isoformat(),
                'execution_time_seconds': execution_time,
                'total_portfolio_value': total_value,
                'total_unrealized_gains': total_gain_loss,
                'stocks_analyzed': len(portfolio_data)
            })
        }
        
    except Exception as e:
        logger.error(f"Portfolio analysis failed: {str(e)}", exc_info=True)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Portfolio analysis failed',
                'message': str(e),
                'timestamp': start_time.isoformat()
            })
        }


# ---------------------------------------------------------------------------
# News alert Lambda handler
# ---------------------------------------------------------------------------

class LambdaNewsAlertGenerator:
    """
    Lambda version of NewsAlertGenerator.
    Reuses LambdaPortfolioAnalyzer's credential/S3/SMTP infrastructure,
    delegating all news-specific logic to the modules in news_alert.py,
    news_providers.py, and news_scorer.py.
    """

    def __init__(self):
        # Reuse the same portfolio analyzer for credential + config loading
        self._analyzer = LambdaPortfolioAnalyzer()
        self.news_cfg  = None
        self.symbol_map = {}   # {symbol: symbol} — names resolved from news feed

    def initialize(self):
        """Load credentials, config, and holdings from AWS services."""
        self._analyzer.initialize_credentials()
        self._analyzer.load_portfolio_config()
        holdings = self._analyzer.load_holdings()

        # Extract unique symbols from loaded holdings
        for account_info in holdings.get('accounts', {}).values():
            for symbol in account_info.get('holdings', {}).keys():
                self.symbol_map[symbol.upper()] = symbol.upper()

        # Merge news config
        from news_alert import DEFAULT_NEWS_CONFIG
        cfg = {**DEFAULT_NEWS_CONFIG}
        user_cfg = self._analyzer.portfolio_config.get('settings', {}).get('news_alerts', {})
        cfg.update(user_cfg)
        self.news_cfg = cfg

    def run(self) -> dict:
        """
        Fetch, score, build, and email the news digest.
        Returns a summary dict for the Lambda response body.
        """
        from news_providers import get_news_provider
        from news_scorer import NewsDeduplicator, NewsScorer
        from news_alert import NewsAlertGenerator, render_email_html

        # Build a local-style generator but override provider + credentials
        finnhub_key = self._analyzer.finnhub_key if hasattr(self._analyzer, 'finnhub_key') else None

        # Retrieve finnhub_key from secrets if not already on the analyzer
        if not finnhub_key:
            try:
                api_secrets = self._analyzer.get_secret('portfolio-analyzer/api-keys')
                finnhub_key = api_secrets.get('finnhub_api_key')
            except Exception as e:
                logger.error(f"Could not retrieve Finnhub API key: {e}")
                raise

        provider = get_news_provider('finnhub', finnhub_key=finnhub_key)
        scorer   = NewsScorer(self.news_cfg)
        deduper  = NewsDeduplicator(window_hours=self.news_cfg.get('dedup_window_hours', 24))

        # Build a minimal generator-like object to reuse fetch/score/build logic
        from news_alert import NewsAlertGenerator as _Gen
        gen = object.__new__(_Gen)
        gen.portfolio     = self._analyzer.portfolio_config
        gen.symbol_map    = self.symbol_map
        gen.news_cfg      = self.news_cfg
        gen.provider      = provider
        gen.scorer        = scorer
        gen.deduper       = deduper

        lookback = self.news_cfg.get('lookback_days', 1)
        raw      = gen.fetch_all_news(lookback_days=lookback)
        scored   = gen.score_and_rank_news(raw)
        digest   = gen.build_digest_data(scored)
        html     = render_email_html(digest)

        if self.news_cfg.get('send_email', True):
            self._send_email(html, digest.get('generated_at_display', ''))

        return {
            'articles_shown':     digest['stats']['articles_shown'],
            'high_impact_symbols': digest['high_impact_symbols'],
        }

    def _send_email(self, html: str, generated_at: str):
        """Send news digest via SMTP using credentials loaded from Secrets Manager."""
        try:
            email_settings = self._analyzer.portfolio_config.get('settings', {}).get('email_settings', {})
            recipient      = email_settings.get('recipient')

            if not recipient or recipient == 'your-email@example.com':
                logger.warning("Email recipient not configured — skipping news digest email")
                return

            msg = MIMEMultipart('alternative')
            msg['From']    = self._analyzer.smtp_user
            msg['To']      = recipient
            msg['Subject'] = f"[Pre-Market Alert] Portfolio News Digest - {datetime.now().strftime('%B %d, %Y')}"

            msg.attach(MIMEText("Portfolio news digest — view in an HTML-capable client.", 'plain'))
            msg.attach(MIMEText(html, 'html'))

            server = smtplib.SMTP(
                email_settings.get('smtp_server', 'smtp.gmail.com'),
                email_settings.get('smtp_port', 587),
            )
            server.starttls()
            server.login(self._analyzer.smtp_user, self._analyzer.smtp_password)
            server.send_message(msg)
            server.quit()
            logger.info(f"News digest email sent to {recipient}")

        except Exception as e:
            logger.error(f"Failed to send news digest email: {e}")
            raise


def news_alert_handler(event, context):
    """
    AWS Lambda handler for the pre-market news alert.

    Triggered by EventBridge on a separate schedule from lambda_handler
    (typically 7:30 AM ET weekdays: cron(30 12 ? * MON-FRI *)).
    """
    start_time = datetime.now()
    logger.info(f"News alert handler started at {start_time.isoformat()}")

    try:
        generator = LambdaNewsAlertGenerator()
        generator.initialize()

        if not generator.news_cfg.get('enabled', False):
            logger.info("News alerts disabled in portfolio.json — exiting")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'News alerts disabled in portfolio config',
                    'timestamp': start_time.isoformat(),
                })
            }

        result = generator.run()

        end_time       = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"News alert completed in {execution_time:.2f}s — {result}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message':             'News alert completed successfully',
                'timestamp':           start_time.isoformat(),
                'execution_time_seconds': execution_time,
                'articles_shown':      result.get('articles_shown', 0),
                'high_impact_symbols': result.get('high_impact_symbols', []),
            })
        }

    except Exception as e:
        logger.error(f"News alert failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error':     'News alert failed',
                'message':   str(e),
                'timestamp': start_time.isoformat(),
            })
        }