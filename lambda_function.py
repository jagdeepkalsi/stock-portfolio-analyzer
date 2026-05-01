#!/usr/bin/env python3
"""
AWS Lambda Portfolio Analyzer
Fetches stock prices and generates daily portfolio summaries
"""

import json
import os
import pandas as pd
from datetime import datetime
import html as html_lib
import re
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


def _email_recipients(email_settings: dict) -> list[str]:
    recipients = email_settings.get('recipients') or email_settings.get('recipient')
    if isinstance(recipients, str):
        recipients = [addr.strip() for addr in recipients.split(',')]
    elif isinstance(recipients, list):
        recipients = [str(addr).strip() for addr in recipients]
    else:
        recipients = []
    return [addr for addr in recipients if addr and addr != 'your-email@example.com']


def _fmt_money(value, digits=0, signed=False):
    if value is None:
        return "N/A"
    sign = "+" if signed else ""
    return f"${value:{sign},.{digits}f}"


def _fmt_pct(value, signed=True):
    if value is None:
        return "N/A"
    sign = "+" if signed else ""
    return f"{value:{sign}.2f}%"


def _css_class(value):
    if value is None:
        return "neutral"
    return "positive" if value >= 0 else "negative"


def _esc(value):
    return html_lib.escape(str(value or ""))


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
        symbol_cache = {}
        
        for account_name, account_info in holdings.get('accounts', {}).items():
            logger.info(f"Processing account: {account_name}")
            
            for symbol, holdings_data in account_info.get('holdings', {}).items():
                symbol = str(symbol).upper().strip()
                if symbol not in symbol_cache:
                    logger.info(f"Fetching data for {symbol}")
                    stock_data = self.get_stock_data(symbol)
                    if stock_data:
                        snapshots = self.data_provider.get_performance_snapshots(symbol)
                        stock_data.update(snapshots)
                    symbol_cache[symbol] = stock_data
                else:
                    stock_data = symbol_cache[symbol]
                
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
                        'daily_change_percent': stock_data['change_percent'],
                        'weekly_change_percent': stock_data.get('change_1w'),
                        'monthly_change_percent': stock_data.get('change_1m'),
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
                spy_data.update(self.data_provider.get_performance_snapshots('SPY'))
                benchmarks['S&P 500'] = {
                    'symbol': 'SPY',
                    'current_price': spy_data['current_price'],
                    'daily_change_percent': spy_data['change_percent'],
                    'weekly_change_percent': spy_data.get('change_1w'),
                    'monthly_change_percent': spy_data.get('change_1m'),
                }
            
            # NASDAQ (QQQ)
            qqq_data = self.get_stock_data('QQQ')
            if qqq_data:
                qqq_data.update(self.data_provider.get_performance_snapshots('QQQ'))
                benchmarks['NASDAQ'] = {
                    'symbol': 'QQQ', 
                    'current_price': qqq_data['current_price'],
                    'daily_change_percent': qqq_data['change_percent'],
                    'weekly_change_percent': qqq_data.get('change_1w'),
                    'monthly_change_percent': qqq_data.get('change_1m'),
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

        # Pre-compute metrics used across the briefing.
        total_purchase_value = sum(d['purchase_price'] * d['shares'] for d in portfolio_data)
        total_gain_loss_pct = (total_unrealized_gains / total_purchase_value * 100) if total_purchase_value else 0
        total_daily_change = sum(d['daily_change'] for d in portfolio_data)
        total_prev_value = total_value - total_daily_change
        total_daily_pct = (total_daily_change / total_prev_value * 100) if total_prev_value else 0

        def period_change(rows, pct_key):
            prior_total = 0
            current_total = 0
            covered_value = 0
            for row in rows:
                pct = row.get(pct_key)
                current_value = row['current_value']
                if pct is None or pct <= -100:
                    continue
                prior_total += current_value / (1 + pct / 100)
                current_total += current_value
                covered_value += current_value
            if not covered_value or not prior_total:
                return None, None
            dollar_change = current_total - prior_total
            return dollar_change, (dollar_change / prior_total * 100)

        total_weekly_change, total_weekly_pct = period_change(portfolio_data, 'weekly_change_percent')
        total_monthly_change, total_monthly_pct = period_change(portfolio_data, 'monthly_change_percent')

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
            acct_weekly_change, acct_weekly_pct = period_change(holdings, 'weekly_change_percent')
            acct_monthly_change, acct_monthly_pct = period_change(holdings, 'monthly_change_percent')
            account_stats[acct_name] = {
                'type':         holdings[0]['account_type'].title(),
                'value':        acct_value,
                'unrealized':   acct_unrealized,
                'daily_change': acct_daily_change,
                'daily_pct':    acct_daily_pct,
                'weekly_change': acct_weekly_change,
                'weekly_pct':    acct_weekly_pct,
                'monthly_change': acct_monthly_change,
                'monthly_pct':    acct_monthly_pct,
            }

        def normalized_account_name(name):
            return re.sub(r'[^a-z0-9]+', ' ', name.lower()).strip()

        def account_rank(name):
            normalized = normalized_account_name(name)
            account_type = account_stats.get(name, {}).get('type', '').lower()
            if ('rsu' in normalized and 'apple' in normalized) or account_type == 'rsu':
                return 0
            if 'rollover' in normalized and 'ira' in normalized:
                return 1
            if 'merrill' in normalized:
                return 2
            if 'etrade' in normalized or 'e trade' in normalized:
                return 3
            return 100

        account_summary_order = sorted(account_stats, key=lambda name: account_stats[name]['value'], reverse=True)
        account_detail_order = sorted(account_stats, key=lambda name: (account_rank(name), -account_stats[name]['value'], name.lower()))

        # Aggregate positions by ticker so cross-account holdings rank once.
        positions = {}
        for row in portfolio_data:
            symbol = row['symbol']
            position = positions.setdefault(symbol, {
                'symbol': symbol,
                'company_name': row['company_name'],
                'shares': 0,
                'current_value': 0,
                'daily_change': 0,
                'daily_change_percent': row['daily_change_percent'],
                'weekly_change_percent': row.get('weekly_change_percent'),
                'monthly_change_percent': row.get('monthly_change_percent'),
                'accounts': set(),
            })
            position['shares'] += row['shares']
            position['current_value'] += row['current_value']
            position['daily_change'] += row['daily_change']
            position['accounts'].add(row['account'])

        aggregated_positions = []
        for position in positions.values():
            position['portfolio_pct'] = (position['current_value'] / total_value * 100) if total_value else 0
            position['accounts_text'] = ', '.join(sorted(position['accounts']))
            aggregated_positions.append(position)

        pct_gainers = [p for p in aggregated_positions if p['daily_change_percent'] is not None and p['daily_change_percent'] >= 0]
        pct_losers = [p for p in aggregated_positions if p['daily_change_percent'] is not None and p['daily_change_percent'] < 0]
        dollar_gainers = [p for p in aggregated_positions if p['daily_change'] >= 0]
        dollar_losers = [p for p in aggregated_positions if p['daily_change'] < 0]

        top_pct_gainers = sorted(pct_gainers, key=lambda p: p['daily_change_percent'], reverse=True)[:5]
        top_pct_losers = sorted(pct_losers, key=lambda p: p['daily_change_percent'])[:5]
        top_dollar_gainers = sorted(dollar_gainers, key=lambda p: p['daily_change'], reverse=True)[:5]
        top_dollar_losers = sorted(dollar_losers, key=lambda p: p['daily_change'])[:5]
        largest_positions = sorted(aggregated_positions, key=lambda p: p['current_value'], reverse=True)[:10]
        watch_rows = sorted(aggregated_positions, key=lambda p: abs(p['daily_change']), reverse=True)[:5]
        top_5_concentration = sum(p['portfolio_pct'] for p in largest_positions[:5])
        top_10_concentration = sum(p['portfolio_pct'] for p in largest_positions[:10])

        def render_mover_rows(rows, emphasize_pct=True):
            if not rows:
                return '<tr><td colspan="5" class="muted">No matching movers today.</td></tr>'
            rendered = ""
            for row in rows:
                pct_class = _css_class(row['daily_change_percent'])
                dollar_class = _css_class(row['daily_change'])
                rendered += f"""
                    <tr>
                        <td><strong>{_esc(row['symbol'])}</strong><div class="subtle">{_esc(row['company_name'])}</div></td>
                        <td class="{pct_class}">{_fmt_pct(row['daily_change_percent'])}</td>
                        <td class="{dollar_class}">{_fmt_money(row['daily_change'], signed=True)}</td>
                        <td>{_fmt_money(row['current_value'])}</td>
                        <td class="subtle">{_esc(row['accounts_text'])}</td>
                    </tr>"""
            return rendered

        def benchmark_rows():
            rendered = ""
            for benchmark_name, data in benchmark_data.items():
                rendered += f"""
                    <tr>
                        <td><strong>{_esc(benchmark_name)}</strong><div class="subtle">{_esc(data.get('symbol'))}</div></td>
                        <td class="{_css_class(data.get('daily_change_percent'))}">{_fmt_pct(data.get('daily_change_percent'))}</td>
                        <td class="{_css_class(data.get('weekly_change_percent'))}">{_fmt_pct(data.get('weekly_change_percent'))}</td>
                        <td class="{_css_class(data.get('monthly_change_percent'))}">{_fmt_pct(data.get('monthly_change_percent'))}</td>
                    </tr>"""
            return rendered or '<tr><td colspan="4" class="muted">Benchmark data unavailable.</td></tr>'

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ margin: 0; padding: 0; background-color: #eef2f6; color: #172033; font-family: Arial, Helvetica, sans-serif; }}
        .shell {{ width: 100%; background-color: #eef2f6; padding: 24px 0; }}
        .container {{ max-width: 1280px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; border: 1px solid #d9e1ea; }}
        .header {{ background-color: #172033; color: #ffffff; padding: 26px 30px; }}
        .eyebrow {{ color: #aab7c8; font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; margin-bottom: 6px; }}
        h1 {{ margin: 0; font-size: 28px; line-height: 1.2; font-weight: 700; }}
        h2 {{ color: #172033; font-size: 18px; margin: 28px 0 12px; }}
        h3 {{ margin: 0; font-size: 15px; color: #172033; }}
        .content {{ padding: 24px 30px 30px; }}
        .snapshot {{ width: 100%; border-collapse: separate; border-spacing: 12px; margin: 0 -12px 8px; }}
        .snapshot td {{ background-color: #f7f9fc; border: 1px solid #e0e7ef; border-radius: 8px; padding: 16px; vertical-align: top; }}
        .label {{ color: #64748b; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
        .value {{ font-size: 22px; line-height: 1.25; font-weight: 700; margin-top: 5px; }}
        .subtle {{ color: #64748b; font-size: 12px; line-height: 1.35; margin-top: 2px; }}
        .muted {{ color: #64748b; font-size: 13px; }}
        .positive {{ color: #15803d; font-weight: 700; }}
        .negative {{ color: #b91c1c; font-weight: 700; }}
        .neutral {{ color: #64748b; font-weight: 700; }}
        .panel {{ border: 1px solid #dfe7f1; border-radius: 8px; overflow: hidden; margin-bottom: 18px; }}
        .panel-title {{ background-color: #f7f9fc; border-bottom: 1px solid #dfe7f1; padding: 12px 14px; }}
        table.data {{ width: 100%; border-collapse: collapse; }}
        table.data th {{ background-color: #fbfcfe; color: #64748b; font-size: 12px; font-weight: 700; padding: 10px 12px; text-align: left; border-bottom: 1px solid #e6edf5; }}
        table.data td {{ font-size: 13px; padding: 10px 12px; border-bottom: 1px solid #eef2f7; vertical-align: top; }}
        table.data tr:last-child td {{ border-bottom: none; }}
        .split {{ width: 100%; border-spacing: 14px; margin: 0 -14px; }}
        .split td.box {{ width: 50%; vertical-align: top; }}
        .account-header {{ background-color: #243044; color: #ffffff; padding: 13px 14px; }}
        .account-header .subtle {{ color: #cbd5e1; }}
        .small-note {{ color: #64748b; font-size: 12px; padding: 10px 12px; background-color: #f7f9fc; border-top: 1px solid #e2e8f0; }}
        @media only screen and (max-width: 760px) {{
            .content {{ padding: 18px; }}
            .snapshot, .split {{ border-spacing: 0; margin: 0; }}
            .snapshot td, .split td.box {{ display: block; width: auto; margin-bottom: 12px; }}
            table.data th, table.data td {{ font-size: 12px; padding: 8px; }}
        }}
    </style>
</head>
<body>
<div class="shell">
<div class="container">
    <div class="header">
        <div class="eyebrow">{datetime.now().strftime('%B %d, %Y')}</div>
        <h1>Daily Portfolio Summary</h1>
    </div>

    <div class="content">
        <table class="snapshot">
            <tr>
                <td>
                    <div class="label">Total Value</div>
                    <div class="value">{_fmt_money(total_value, digits=2)}</div>
                    <div class="subtle">Across {len(account_stats)} accounts and {len(aggregated_positions)} positions</div>
                </td>
                <td>
                    <div class="label">Today</div>
                    <div class="value {_css_class(total_daily_change)}">{_fmt_money(total_daily_change, digits=2, signed=True)}</div>
                    <div class="subtle {_css_class(total_daily_pct)}">{_fmt_pct(total_daily_pct)}</div>
                </td>
                <td>
                    <div class="label">1 Week</div>
                    <div class="value {_css_class(total_weekly_change)}">{_fmt_money(total_weekly_change, digits=2, signed=True)}</div>
                    <div class="subtle {_css_class(total_weekly_pct)}">{_fmt_pct(total_weekly_pct)}</div>
                </td>
                <td>
                    <div class="label">1 Month</div>
                    <div class="value {_css_class(total_monthly_change)}">{_fmt_money(total_monthly_change, digits=2, signed=True)}</div>
                    <div class="subtle {_css_class(total_monthly_pct)}">{_fmt_pct(total_monthly_pct)}</div>
                </td>
                <td>
                    <div class="label">Unrealized P&amp;L</div>
                    <div class="value {_css_class(total_unrealized_gains)}">{_fmt_money(total_unrealized_gains, digits=2, signed=True)}</div>
                    <div class="subtle {_css_class(total_gain_loss_pct)}">{_fmt_pct(total_gain_loss_pct)}</div>
                </td>
            </tr>
        </table>

        <h2>Accounts</h2>
        <div class="panel">
        <table class="data">
            <thead>
                <tr>
                    <th>Account</th>
                    <th>Type</th>
                    <th>Value</th>
                    <th>Unrealized P&L</th>
                    <th>Today</th>
                    <th>1W</th>
                    <th>1M</th>
                </tr>
            </thead>
            <tbody>"""

        for acct_name in account_summary_order:
            s = account_stats[acct_name]
            html += f"""
                <tr>
                    <td><strong>{_esc(acct_name)}</strong></td>
                    <td>{_esc(s['type'])}</td>
                    <td>{_fmt_money(s['value'], digits=2)}</td>
                    <td class="{_css_class(s['unrealized'])}">{_fmt_money(s['unrealized'], digits=2, signed=True)}</td>
                    <td class="{_css_class(s['daily_change'])}">{_fmt_money(s['daily_change'], digits=2, signed=True)}<div class="subtle {_css_class(s['daily_pct'])}">{_fmt_pct(s['daily_pct'])}</div></td>
                    <td class="{_css_class(s['weekly_change'])}">{_fmt_money(s['weekly_change'], signed=True)}<div class="subtle {_css_class(s['weekly_pct'])}">{_fmt_pct(s['weekly_pct'])}</div></td>
                    <td class="{_css_class(s['monthly_change'])}">{_fmt_money(s['monthly_change'], signed=True)}<div class="subtle {_css_class(s['monthly_pct'])}">{_fmt_pct(s['monthly_pct'])}</div></td>
                </tr>"""

        html += """
            </tbody>
        </table>
        </div>

        <h2>What Moved Today</h2>
        <table class="split">
            <tr>
                <td class="box">
                    <div class="panel">
                        <div class="panel-title"><h3>Top Gainers by %</h3></div>
                        <table class="data">
                            <thead><tr><th>Position</th><th>Daily %</th><th>Daily $</th><th>Value</th><th>Accounts</th></tr></thead>
                            <tbody>""" + render_mover_rows(top_pct_gainers) + """</tbody>
                        </table>
                    </div>
                </td>
                <td class="box">
                    <div class="panel">
                        <div class="panel-title"><h3>Top Losers by %</h3></div>
                        <table class="data">
                            <thead><tr><th>Position</th><th>Daily %</th><th>Daily $</th><th>Value</th><th>Accounts</th></tr></thead>
                            <tbody>""" + render_mover_rows(top_pct_losers) + """</tbody>
                        </table>
                    </div>
                </td>
            </tr>
            <tr>
                <td class="box">
                    <div class="panel">
                        <div class="panel-title"><h3>Top Gainers by $</h3></div>
                        <table class="data">
                            <thead><tr><th>Position</th><th>Daily %</th><th>Daily $</th><th>Value</th><th>Accounts</th></tr></thead>
                            <tbody>""" + render_mover_rows(top_dollar_gainers, emphasize_pct=False) + """</tbody>
                        </table>
                    </div>
                </td>
                <td class="box">
                    <div class="panel">
                        <div class="panel-title"><h3>Top Losers by $</h3></div>
                        <table class="data">
                            <thead><tr><th>Position</th><th>Daily %</th><th>Daily $</th><th>Value</th><th>Accounts</th></tr></thead>
                            <tbody>""" + render_mover_rows(top_dollar_losers, emphasize_pct=False) + """</tbody>
                        </table>
                    </div>
                </td>
            </tr>
        </table>

        <h2>Positions That Matter</h2>
        <div class="panel">
            <div class="panel-title"><h3>Largest Positions</h3></div>
            <table class="data">
                <thead>
                    <tr>
                        <th>Position</th>
                        <th>Value</th>
                        <th>% Portfolio</th>
                        <th>Today</th>
                        <th>1W</th>
                        <th>1M</th>
                        <th>Accounts</th>
                    </tr>
                </thead>
                <tbody>"""

        for row in largest_positions:
            html += f"""
                    <tr>
                        <td><strong>{_esc(row['symbol'])}</strong><div class="subtle">{_esc(row['company_name'])}</div></td>
                        <td>{_fmt_money(row['current_value'])}</td>
                        <td>{row['portfolio_pct']:.1f}%</td>
                        <td class="{_css_class(row['daily_change'])}">{_fmt_money(row['daily_change'], signed=True)}<div class="subtle {_css_class(row['daily_change_percent'])}">{_fmt_pct(row['daily_change_percent'])}</div></td>
                        <td class="{_css_class(row['weekly_change_percent'])}">{_fmt_pct(row['weekly_change_percent'])}</td>
                        <td class="{_css_class(row['monthly_change_percent'])}">{_fmt_pct(row['monthly_change_percent'])}</td>
                        <td class="subtle">{_esc(row['accounts_text'])}</td>
                    </tr>"""

        html += f"""
                </tbody>
            </table>
            <div class="small-note">
                Top 5 positions are {top_5_concentration:.1f}% of the portfolio. Top 10 positions are {top_10_concentration:.1f}%.
            </div>
        </div>

        <div class="panel">
            <div class="panel-title"><h3>Watch These Today</h3></div>
            <table class="data">
                <thead><tr><th>Position</th><th>Value</th><th>Today</th><th>1W</th><th>1M</th><th>Accounts</th></tr></thead>
                <tbody>"""

        for row in watch_rows:
            html += f"""
                    <tr>
                        <td><strong>{_esc(row['symbol'])}</strong><div class="subtle">{_esc(row['company_name'])}</div></td>
                        <td>{_fmt_money(row['current_value'])}</td>
                        <td class="{_css_class(row['daily_change'])}">{_fmt_money(row['daily_change'], signed=True)}<div class="subtle {_css_class(row['daily_change_percent'])}">{_fmt_pct(row['daily_change_percent'])}</div></td>
                        <td class="{_css_class(row['weekly_change_percent'])}">{_fmt_pct(row['weekly_change_percent'])}</td>
                        <td class="{_css_class(row['monthly_change_percent'])}">{_fmt_pct(row['monthly_change_percent'])}</td>
                        <td class="subtle">{_esc(row['accounts_text'])}</td>
                    </tr>"""

        html += """
                </tbody>
            </table>
        </div>

        <h2>Benchmarks</h2>
        <div class="panel">
            <table class="data">
                <thead><tr><th>Benchmark</th><th>1D</th><th>1W</th><th>1M</th></tr></thead>
                <tbody>""" + benchmark_rows() + """</tbody>
            </table>
        </div>

        <h2>Holdings by Account</h2>"""

        for account_name in account_detail_order:
            holdings_list = accounts[account_name]
            s             = account_stats[account_name]
            sorted_hold   = sorted(holdings_list, key=lambda x: x['current_value'], reverse=True)

            html += f"""
        <div class="panel">
        <div class="account-header">
            <strong>{_esc(account_name)} ({_esc(s['type'])})</strong>
            <div class="subtle">Value: {_fmt_money(s['value'], digits=2)} | Today: {_fmt_money(s['daily_change'], digits=2, signed=True)} ({_fmt_pct(s['daily_pct'])}) | Unrealized: {_fmt_money(s['unrealized'], digits=2, signed=True)}</div>
        </div>
        <table class="data">
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Company</th>
                    <th>Price</th>
                    <th>Shares</th>
                    <th>Value</th>
                    <th>Today</th>
                    <th>1W</th>
                    <th>1M</th>
                </tr>
            </thead>
            <tbody>"""

            for d in sorted_hold:
                html += f"""
                <tr>
                    <td><strong>{_esc(d['symbol'])}</strong></td>
                    <td>{_esc(d['company_name'])}</td>
                    <td>{_fmt_money(d['current_price'], digits=2)}</td>
                    <td>{d['shares']:,.4g}</td>
                    <td>{_fmt_money(d['current_value'])}</td>
                    <td class="{_css_class(d['daily_change'])}">{_fmt_money(d['daily_change'], signed=True)}<div class="subtle {_css_class(d['daily_change_percent'])}">{_fmt_pct(d['daily_change_percent'])}</div></td>
                    <td class="{_css_class(d.get('weekly_change_percent'))}">{_fmt_pct(d.get('weekly_change_percent'))}</td>
                    <td class="{_css_class(d.get('monthly_change_percent'))}">{_fmt_pct(d.get('monthly_change_percent'))}</td>
                </tr>"""

            html += f"""
            </tbody>
        </table>
        <div class="small-note">{len(holdings_list)} position{'s' if len(holdings_list) != 1 else ''}, sorted by current value.</div>
    </div>"""

        html += """
    </div>
</div>
</div>
</body>
</html>"""

        return html
    
    def send_email_summary(self, html_summary):
        """Send portfolio summary via email with HTML formatting"""
        try:
            email_settings = self.portfolio_config.get('settings', {}).get('email_settings', {})
            recipients = _email_recipients(email_settings)
            
            if not recipients:
                raise ValueError("Email recipient not configured")
            
            # Create multipart message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.smtp_user
            msg['To'] = ", ".join(recipients)
            msg['Subject'] = f"📊 Daily Portfolio Summary - {datetime.now().strftime('%B %d, %Y')}"
            
            # Create HTML version
            html_part = MIMEText(html_summary, 'html')
            msg.attach(html_part)
            
            # Send email
            server = smtplib.SMTP(email_settings.get('smtp_server', 'smtp.gmail.com'), 
                                email_settings.get('smtp_port', 587))
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.send_message(msg, to_addrs=recipients)
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
            recipients     = _email_recipients(email_settings)

            if not recipients:
                logger.warning("Email recipient not configured — skipping news digest email")
                return

            msg = MIMEMultipart('alternative')
            msg['From']    = self._analyzer.smtp_user
            msg['To']      = ", ".join(recipients)
            msg['Subject'] = f"[Pre-Market Alert] Portfolio News Digest - {datetime.now().strftime('%B %d, %Y')}"

            msg.attach(MIMEText("Portfolio news digest — view in an HTML-capable client.", 'plain'))
            msg.attach(MIMEText(html, 'html'))

            server = smtplib.SMTP(
                email_settings.get('smtp_server', 'smtp.gmail.com'),
                email_settings.get('smtp_port', 587),
            )
            server.starttls()
            server.login(self._analyzer.smtp_user, self._analyzer.smtp_password)
            server.send_message(msg, to_addrs=recipients)
            server.quit()
            logger.info(f"News digest email sent to {', '.join(recipients)}")

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


# ---------------------------------------------------------------------------
# Market Pulse digest (market trends + congressional trades)
# ---------------------------------------------------------------------------

class LambdaMarketDigestGenerator:
    """
    Lambda wrapper around market_digest.build_digest + render_email_html.
    Reuses LambdaPortfolioAnalyzer for credentials, config, and SMTP.
    """

    def __init__(self):
        self._analyzer = LambdaPortfolioAnalyzer()
        self.digest_cfg = None

    def initialize(self):
        # Populates self._analyzer.portfolio_config, smtp_user, smtp_password
        self._analyzer.initialize_credentials()
        self._analyzer.load_portfolio_config()

        # Market trends pulls from Finnhub regardless of DATA_PROVIDER, so we
        # always need the Finnhub key here. initialize_credentials only loads
        # it when DATA_PROVIDER == 'finnhub'; pull from secrets otherwise.
        try:
            api_secrets = self._analyzer.get_secret('portfolio-analyzer/api-keys')
            self.finnhub_api_key = api_secrets.get('finnhub_api_key') or ''
        except Exception as e:
            logger.error(f"Could not retrieve Finnhub API key for market digest: {e}")
            raise

        from market_digest import load_digest_config
        self.digest_cfg = load_digest_config(self._analyzer.portfolio_config)

    def run(self) -> dict:
        from market_digest import build_digest, render_email_html

        digest = build_digest(self._analyzer.portfolio_config, self.digest_cfg, self.finnhub_api_key)
        html   = render_email_html(digest)

        if self.digest_cfg.get('send_email', True):
            self._send_email(html, digest.get('generated_at_display', ''))

        counts = digest.get('congress', {}).get('counts', {})
        return {
            'generated_at':     digest.get('generated_at'),
            'congress_counts':  counts,
            'highlights':       digest.get('trends', {}).get('highlights', []),
        }

    def _send_email(self, html: str, generated_at: str):
        try:
            email_settings = self._analyzer.portfolio_config.get('settings', {}).get('email_settings', {})
            recipients     = _email_recipients(email_settings)

            if not recipients:
                logger.warning("Email recipient not configured — skipping Market Pulse email")
                return

            subject_date = generated_at.split(' · ')[0] if generated_at else datetime.now().strftime('%B %d, %Y')

            msg = MIMEMultipart('alternative')
            msg['From']    = self._analyzer.smtp_user
            msg['To']      = ", ".join(recipients)
            msg['Subject'] = f"[Market Pulse] {subject_date}"

            msg.attach(MIMEText("Market Pulse — view in an HTML-capable email client.", 'plain'))
            msg.attach(MIMEText(html, 'html'))

            server = smtplib.SMTP(
                email_settings.get('smtp_server', 'smtp.gmail.com'),
                email_settings.get('smtp_port', 587),
            )
            server.starttls()
            server.login(self._analyzer.smtp_user, self._analyzer.smtp_password)
            server.send_message(msg, to_addrs=recipients)
            server.quit()
            logger.info(f"Market Pulse email sent to {', '.join(recipients)}")

        except Exception as e:
            logger.error(f"Failed to send Market Pulse email: {e}")
            raise


def market_digest_handler(event, context):
    """
    AWS Lambda handler for the daily Market Pulse digest.

    Triggered by EventBridge on its own schedule (default: daily 6 PM ET,
    cron(0 23 * * ? *) UTC). Config lives in portfolio.json under
    settings.market_digest.
    """
    start_time = datetime.now()
    logger.info(f"Market digest handler started at {start_time.isoformat()}")

    try:
        generator = LambdaMarketDigestGenerator()
        generator.initialize()

        if not generator.digest_cfg.get('enabled', True):
            logger.info("Market digest disabled in portfolio.json — exiting")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message':   'Market digest disabled in portfolio config',
                    'timestamp': start_time.isoformat(),
                })
            }

        result = generator.run()

        end_time       = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"Market digest completed in {execution_time:.2f}s — {result.get('congress_counts')}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message':                'Market digest completed successfully',
                'timestamp':              start_time.isoformat(),
                'execution_time_seconds': execution_time,
                'congress_counts':        result.get('congress_counts', {}),
                'highlights':             result.get('highlights', []),
            })
        }

    except Exception as e:
        logger.error(f"Market digest failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error':     'Market digest failed',
                'message':   str(e),
                'timestamp': start_time.isoformat(),
            })
        }
