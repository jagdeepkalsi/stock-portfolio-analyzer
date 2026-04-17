#!/usr/bin/env python3
"""
Stock Portfolio Analyzer
Fetches stock prices and generates daily portfolio summaries
"""

import pandas as pd
from datetime import datetime, timedelta
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import requests
import time
from dotenv import load_dotenv

load_dotenv()

class PortfolioAnalyzer:
    def __init__(self, portfolio_file='portfolio.json'):
        self.portfolio_file = portfolio_file
        self.portfolio = self.load_portfolio()
        self.holdings = self.load_holdings()
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.alpha_vantage_key:
            print("Warning: ALPHA_VANTAGE_API_KEY not found in environment variables")
    
    def load_portfolio(self):
        """Load portfolio configuration from JSON file"""
        try:
            with open(self.portfolio_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Portfolio file {self.portfolio_file} not found. Creating sample portfolio.")
            return self.create_sample_portfolio()
    
    def create_sample_portfolio(self):
        """Create a sample portfolio configuration"""
        sample_portfolio = {
            "settings": {
                "holdings_file": "holdings.csv",
                "send_email": False,
                "email_settings": {
                    "recipient": "your-email@example.com",
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587
                }
            }
        }
        
        with open(self.portfolio_file, 'w') as f:
            json.dump(sample_portfolio, f, indent=4)
        
        return sample_portfolio
    
    def load_holdings(self):
        """Load holdings from CSV file"""
        holdings_file = self.portfolio.get('settings', {}).get('holdings_file', 'holdings.csv')
        try:
            df = pd.read_csv(holdings_file)
            
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
            
            return holdings
            
        except FileNotFoundError:
            print(f"Holdings file {holdings_file} not found. Creating sample holdings.")
            return self.create_sample_holdings(holdings_file)
    
    def create_sample_holdings(self, holdings_file):
        """Create a sample holdings CSV file"""
        sample_data = [
            ['account_name', 'account_type', 'symbol', 'shares', 'purchase_price', 'purchase_date'],
            ['Sample_Account', 'brokerage', 'AAPL', 10, 150.00, '2023-01-15'],
            ['Sample_Account', 'brokerage', 'GOOGL', 5, 2500.00, '2023-02-01'],
            ['Sample_Account', 'brokerage', 'MSFT', 8, 300.00, '2023-03-10'],
            ['Sample_Account', 'brokerage', 'TSLA', 3, 800.00, '2023-04-05']
        ]
        
        # Write CSV file
        with open(holdings_file, 'w', newline='') as f:
            import csv
            writer = csv.writer(f)
            writer.writerows(sample_data)
        
        # Return in expected format
        return {
            "accounts": {
                "Sample_Account": {
                    "account_type": "brokerage",
                    "holdings": {
                        "AAPL": {"shares": 10, "purchase_price": 150.00, "purchase_date": "2023-01-15"},
                        "GOOGL": {"shares": 5, "purchase_price": 2500.00, "purchase_date": "2023-02-01"},
                        "MSFT": {"shares": 8, "purchase_price": 300.00, "purchase_date": "2023-03-10"},
                        "TSLA": {"shares": 3, "purchase_price": 800.00, "purchase_date": "2023-04-05"}
                    }
                }
            }
        }
    
    def get_benchmark_data(self, symbol):
        """Fetch benchmark data (SPY for S&P 500, QQQ for NASDAQ)"""
        return self.get_stock_data(symbol)
    
    def get_stock_data(self, symbol):
        """Fetch stock data and company info using Alpha Vantage API"""
        if not self.alpha_vantage_key:
            print(f"No API key found, cannot fetch data for {symbol}")
            return None
            
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
                print(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                print(f"Alpha Vantage rate limit for {symbol}: {data['Note']}")
                return None
            
            # Extract quote data
            quote = data.get('Global Quote', {})
            if not quote:
                print(f"No quote data found for {symbol} - API response: {data}")
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
            except:
                # If overview fails, continue with basic data
                pass
            
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
            print(f"Network error fetching data for {symbol}: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            print(f"Data parsing error for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching data for {symbol}: {e}")
            return None
    
    def analyze_portfolio(self):
        """Analyze the entire portfolio across all accounts with enhanced metrics"""
        portfolio_data = []
        total_value = 0
        total_gain_loss = 0
        sector_allocation = {}
        
        for account_name, account_info in self.holdings.get('accounts', {}).items():
            for symbol, holdings in account_info.get('holdings', {}).items():
                stock_data = self.get_stock_data(symbol)
                time.sleep(0.4)  # Increased rate limiting for multiple API calls
                if stock_data:
                    shares = holdings['shares']
                    purchase_price = holdings['purchase_price']
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
        
        # Get benchmark data
        benchmark_data = self.get_benchmark_performance()
        
        return portfolio_data, total_value, total_gain_loss, sector_allocation, benchmark_data
    
    def get_benchmark_performance(self):
        """Get benchmark performance for S&P 500 and NASDAQ"""
        benchmarks = {}
        
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
        # Perfect diversity (equal distribution across 10 sectors) would have HHI = 0.1
        diversity_score = max(0, (1 - hhi) * 100)
        return round(diversity_score, 1)
    
    def generate_summary(self):
        """Generate a daily portfolio summary with enhanced metrics"""
        analysis_result = self.analyze_portfolio()
        portfolio_data, total_value, total_unrealized_gains, sector_allocation, benchmark_data = analysis_result
        
        if not portfolio_data:
            return "No portfolio data available."
        
        # Calculate total portfolio change percentage
        total_purchase_value = sum([data['purchase_price'] * data['shares'] for data in portfolio_data])
        total_gain_loss_percent = (total_unrealized_gains / total_purchase_value) * 100 if total_purchase_value else 0
        
        # Calculate diversity score
        diversity_score = self.calculate_diversity_score(sector_allocation, total_value)
        
        # Generate summary text
        summary = f"""
📊 DAILY PORTFOLIO SUMMARY - {datetime.now().strftime('%Y-%m-%d')}
{'='*70}

💰 PORTFOLIO OVERVIEW:
   Total Portfolio Value: ${total_value:,.2f}
   Total Unrealized Gains/Loss: ${total_unrealized_gains:,.2f} ({total_gain_loss_percent:+.2f}%)

🎯 BENCHMARK COMPARISON:"""
        
        # Add benchmark data
        for benchmark_name, data in benchmark_data.items():
            summary += f"""
   {benchmark_name}: {data['daily_change_percent']:+.2f}% today"""
        
        summary += f"""

🏆 PORTFOLIO DIVERSITY:
   Diversity Score: {diversity_score}/100 (Higher = More Diverse)
   
📊 SECTOR ALLOCATION:"""
        
        # Add sector breakdown
        if total_value > 0:
            for sector, value in sorted(sector_allocation.items(), key=lambda x: x[1], reverse=True):
                percentage = (value / total_value) * 100
                summary += f"""
   {sector}: ${value:,.0f} ({percentage:.1f}%)"""
        
        summary += f"""

🏦 HOLDINGS BY ACCOUNT:
"""
        
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
            
            # Get account type from first holding
            account_type = holdings_list[0]['account_type'].title()
            
            summary += f"""
🏦 {account_name} ({account_type}) - Value: ${account_value:,.2f} | Unrealized: ${account_gain_loss:,.2f}

{"Account":<20} {"Ticker":<8} {"Company":<25} {"Sector":<20} {"Price":<10} {"Holdings":<15} {"Daily %":<10} {"Daily $":<12}
{"-"*20} {"-"*8} {"-"*25} {"-"*20} {"-"*10} {"-"*15} {"-"*10} {"-"*12}"""
            
            # Sort holdings by daily change percentage (highest to lowest)
            sorted_holdings = sorted(holdings_list, key=lambda x: x['daily_change_percent'], reverse=True)
            
            # Show stock holdings in table format
            for data in sorted_holdings:
                account_short = account_name[:18] + ".." if len(account_name) > 20 else account_name
                company_short = data['company_name'][:23] + ".." if len(data['company_name']) > 25 else data['company_name']
                sector_short = data['sector'][:18] + ".." if len(data['sector']) > 20 else data['sector']
                holdings_text = f"{data['shares']} @ ${data['current_value']:,.0f}"
                daily_change_pct = f"{data['daily_change_percent']:+.2f}%"
                daily_change_amt = f"${data['daily_change']:+,.0f}"
                
                summary += f"""
{account_short:<20} {data['symbol']:<8} {company_short:<25} {sector_short:<20} ${data['current_price']:<9.2f} {holdings_text:<15} {daily_change_pct:<10} {daily_change_amt:<12}"""
        
        return summary
    
    def generate_html_summary(self):
        """Generate an HTML version of the portfolio summary for email"""
        analysis_result = self.analyze_portfolio()
        portfolio_data, total_value, total_unrealized_gains, sector_allocation, benchmark_data = analysis_result
        
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
    
    def send_email_summary(self, summary):
        """Send portfolio summary via email with HTML formatting"""
        try:
            email_settings = self.portfolio.get('settings', {}).get('email_settings', {})
            recipient = email_settings.get('recipient')
            
            if not recipient or recipient == 'your-email@example.com':
                print("Please update email settings in portfolio.json")
                return False
            
            # Email configuration from environment variables
            smtp_user = os.getenv('SMTP_USER')
            smtp_password = os.getenv('SMTP_PASSWORD')
            
            if not smtp_user or not smtp_password:
                print("Please set SMTP_USER and SMTP_PASSWORD environment variables")
                return False
            
            # Create multipart message
            msg = MIMEMultipart('alternative')
            msg['From'] = smtp_user
            msg['To'] = recipient
            msg['Subject'] = f"📊 Daily Portfolio Summary - {datetime.now().strftime('%B %d, %Y')}"
            
            # Create both plain text and HTML versions
            text_part = MIMEText(summary, 'plain')
            html_summary = self.generate_html_summary()
            html_part = MIMEText(html_summary, 'html')
            
            # Attach both versions
            msg.attach(text_part)
            msg.attach(html_part)
            
            # Send email
            server = smtplib.SMTP(email_settings.get('smtp_server', 'smtp.gmail.com'), 
                                email_settings.get('smtp_port', 587))
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
            server.quit()
            
            print("HTML email sent successfully!")
            return True
            
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
    
    def run_daily_analysis(self):
        """Run the daily portfolio analysis"""
        print("Analyzing portfolio...")
        summary = self.generate_summary()
        print(summary)
        
        # Check if email should be sent based on settings
        send_email = self.portfolio.get('settings', {}).get('send_email', False)
        if send_email:
            print("Sending email summary...")
            self.send_email_summary(summary)
        else:
            print("Email sending disabled in settings.")

def main():
    analyzer = PortfolioAnalyzer()
    
    # Run analysis with email setting from config
    analyzer.run_daily_analysis()

if __name__ == "__main__":
    main()