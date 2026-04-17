#!/usr/bin/env python3
"""
AWS Lambda Portfolio Analyzer
Fetches stock prices and generates daily portfolio summaries
"""

import json
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import time
import boto3
import logging
from botocore.exceptions import ClientError
import io

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
            # Get secrets from Secrets Manager
            api_secrets = self.get_secret('portfolio-analyzer/api-keys')
            email_secrets = self.get_secret('portfolio-analyzer/email-config')
            
            self.alpha_vantage_key = api_secrets['alpha_vantage_api_key']
            self.smtp_user = email_secrets['smtp_user']
            self.smtp_password = email_secrets['smtp_password']
            
            # Get bucket name from environment variable
            self.bucket_name = os.environ.get('S3_BUCKET_NAME')
            if not self.bucket_name:
                raise ValueError("S3_BUCKET_NAME environment variable not set")
            
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
        """Load holdings from S3 CSV file"""
        try:
            csv_data = self.get_s3_object(self.bucket_name, 'holdings.csv')
            df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
            
            # Convert CSV to nested structure for compatibility
            holdings = {"accounts": {}}
            
            for _, row in df.iterrows():
                account_name = row['account_name']
                account_type = row['account_type']
                symbol = row['symbol']
                shares = row['shares']
                purchase_price = row['purchase_price']
                purchase_date = row.get('purchase_date', '')
                
                if account_name not in holdings["accounts"]:
                    holdings["accounts"][account_name] = {
                        "account_type": account_type,
                        "holdings": {}
                    }
                
                holdings["accounts"][account_name]["holdings"][symbol] = {
                    "shares": shares,
                    "purchase_price": purchase_price,
                    "purchase_date": purchase_date
                }
            
            logger.info(f"Successfully loaded holdings for {len(holdings['accounts'])} accounts")
            return holdings
            
        except Exception as e:
            logger.error(f"Failed to load holdings: {e}")
            raise
    
    def get_benchmark_data(self, symbol):
        """Fetch benchmark data (SPY for S&P 500, QQQ for NASDAQ)"""
        return self.get_stock_data(symbol)
    
    def get_stock_data(self, symbol):
        """Fetch stock data and company info using Alpha Vantage API"""
        if not self.alpha_vantage_key:
            raise ValueError("Alpha Vantage API key not available")
            
        try:
            # First get quote data
            quote_url = f"https://www.alphavantage.co/query"
            quote_params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(quote_url, params=quote_params, timeout=10)
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"Alpha Vantage rate limit for {symbol}: {data['Note']}")
                return None
            
            # Extract quote data
            quote = data.get('Global Quote', {})
            if not quote:
                logger.warning(f"No quote data found for {symbol}")
                return None
            
            current_price = float(quote.get('05. price', 0))
            prev_close = float(quote.get('08. previous close', 0))
            volume = int(quote.get('06. volume', 0))
            
            # Get company overview for sector info
            sector = "Unknown"
            company_name = symbol
            
            try:
                time.sleep(0.2)  # Rate limiting between API calls
                overview_params = {
                    'function': 'OVERVIEW',
                    'symbol': symbol,
                    'apikey': self.alpha_vantage_key
                }
                
                overview_response = requests.get(quote_url, params=overview_params, timeout=10)
                overview_data = overview_response.json()
                
                if overview_data and 'Sector' in overview_data:
                    sector = overview_data.get('Sector', 'Unknown')
                    company_name = overview_data.get('Name', symbol)
            except Exception as e:
                logger.warning(f"Failed to get overview data for {symbol}: {e}")
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'previous_close': prev_close,
                'change': current_price - prev_close,
                'change_percent': ((current_price - prev_close) / prev_close) * 100 if prev_close else 0,
                'volume': volume,
                'company_name': company_name,
                'sector': sector
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching data for {symbol}: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Data parsing error for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")
            return None
    
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
                time.sleep(0.4)  # Rate limiting for Alpha Vantage
                
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
            spy_data = self.get_benchmark_data('SPY')
            if spy_data:
                benchmarks['S&P 500'] = {
                    'symbol': 'SPY',
                    'current_price': spy_data['current_price'],
                    'daily_change_percent': spy_data['change_percent']
                }
            
            time.sleep(0.2)  # Rate limiting
            
            # NASDAQ (QQQ)
            qqq_data = self.get_benchmark_data('QQQ')
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
        
        # Calculate metrics
        total_purchase_value = sum([data['purchase_price'] * data['shares'] for data in portfolio_data])
        total_gain_loss_percent = (total_unrealized_gains / total_purchase_value) * 100 if total_purchase_value else 0
        diversity_score = self.calculate_diversity_score(sector_allocation, total_value)
        
        # Color coding for gains/losses
        portfolio_color = "#28a745" if total_unrealized_gains >= 0 else "#dc3545"
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; margin-bottom: 20px; }}
        .overview {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin-bottom: 20px; }}
        .metric-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 1.5em; font-weight: bold; margin-top: 5px; }}
        .benchmark-section {{ background-color: #e9ecef; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .benchmark-item {{ display: inline-block; margin: 5px 15px; padding: 5px 10px; background-color: #6c757d; color: white; border-radius: 5px; }}
        .sector-section {{ background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .sector-item {{ margin: 5px 0; padding: 5px; background-color: #e9ecef; border-radius: 5px; }}
        .account-section {{ margin-bottom: 30px; }}
        .account-header {{ background-color: #007bff; color: white; padding: 10px; border-radius: 8px; margin-bottom: 10px; }}
        .holdings-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
        .holdings-table th {{ background-color: #343a40; color: white; padding: 10px; text-align: left; }}
        .holdings-table td {{ padding: 8px; border-bottom: 1px solid #dee2e6; }}
        .holdings-table tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .positive {{ color: #28a745; font-weight: bold; }}
        .negative {{ color: #dc3545; font-weight: bold; }}
        .neutral {{ color: #6c757d; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Daily Portfolio Summary</h1>
            <p>{datetime.now().strftime('%B %d, %Y')}</p>
        </div>
        
        <div class="overview">
            <div class="metric-card">
                <div>Total Portfolio Value</div>
                <div class="metric-value">${total_value:,.2f}</div>
            </div>
            <div class="metric-card">
                <div>Unrealized Gains/Loss</div>
                <div class="metric-value" style="color: {portfolio_color};">${total_unrealized_gains:,.2f} ({total_gain_loss_percent:+.2f}%)</div>
            </div>
            <div class="metric-card">
                <div>Diversity Score</div>
                <div class="metric-value">{diversity_score}/100</div>
            </div>
        </div>
        
        <div class="benchmark-section">
            <h3>🎯 Benchmark Comparison</h3>"""
        
        for benchmark_name, data in benchmark_data.items():
            color = "#28a745" if data['daily_change_percent'] >= 0 else "#dc3545"
            html += f'<span class="benchmark-item" style="background-color: {color};">{benchmark_name}: {data["daily_change_percent"]:+.2f}%</span>'
        
        html += f"""
        </div>
        
        <div class="sector-section">
            <h3>📊 Sector Allocation</h3>"""
        
        for sector, value in sorted(sector_allocation.items(), key=lambda x: x[1], reverse=True):
            percentage = (value / total_value) * 100
            html += f'<div class="sector-item"><strong>{sector}:</strong> ${value:,.0f} ({percentage:.1f}%)</div>'
        
        html += """
        </div>
        
        <h3>🏦 Holdings by Account</h3>"""
        
        # Group by account
        accounts = {}
        for data in portfolio_data:
            account = data['account']
            if account not in accounts:
                accounts[account] = []
            accounts[account].append(data)
        
        for account_name, holdings_list in accounts.items():
            account_value = sum([h['current_value'] for h in holdings_list])
            account_gain_loss = sum([h['unrealized_gain_loss'] for h in holdings_list])
            account_type = holdings_list[0]['account_type'].title()
            
            html += f"""
        <div class="account-section">
            <div class="account-header">
                <strong>{account_name} ({account_type})</strong> - 
                Value: ${account_value:,.2f} | 
                Unrealized: <span style="color: {'#90EE90' if account_gain_loss >= 0 else '#FFB6C1'};">${account_gain_loss:,.2f}</span>
            </div>
            <table class="holdings-table">
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>Company</th>
                        <th>Sector</th>
                        <th>Price</th>
                        <th>Holdings</th>
                        <th>Daily Change %</th>
                        <th>Daily Change $</th>
                    </tr>
                </thead>
                <tbody>"""
            
            # Sort holdings by daily change percentage
            sorted_holdings = sorted(holdings_list, key=lambda x: x['daily_change_percent'], reverse=True)
            
            for data in sorted_holdings:
                change_class = "positive" if data['daily_change_percent'] >= 0 else "negative"
                holdings_text = f"{data['shares']} shares (${data['current_value']:,.0f})"
                
                html += f"""
                    <tr>
                        <td><strong>{data['symbol']}</strong></td>
                        <td>{data['company_name']}</td>
                        <td>{data['sector']}</td>
                        <td>${data['current_price']:.2f}</td>
                        <td>{holdings_text}</td>
                        <td class="{change_class}">{data['daily_change_percent']:+.2f}%</td>
                        <td class="{change_class}">${data['daily_change']:+,.0f}</td>
                    </tr>"""
            
            html += """
                </tbody>
            </table>
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