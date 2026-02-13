"""
Sarasota Market Pulse — Email Delivery System (V4)

Renders HTML email reports using Jinja2 templates and sends via Gmail SMTP.
V4 Changes: Weekly market intelligence format with market-level analytics.
"""

import os
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from jinja2 import Template
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# HTML Email Template (V4 - Weekly Market Intelligence)
EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
 <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 8px 8px 0 0;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 28px;
        }
        .header p {
            margin: 10px 0 0 0;
            opacity: 0.9;
        }
        .content {
            background: white;
            padding: 30px;
            border-radius: 0 0 8px 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-section {
            margin: 30px 0;
            border-left: 4px solid #667eea;
            padding-left: 20px;
        }
        .metric-section h2 {
            margin-top: 0;
            color: #667eea;
            font-size: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 14px;
        }
        th {
            background-color: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #dee2e6;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #dee2e6;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .footer {
            margin-top: 30px;
            padding: 20px;
            background-color: #f8f9fa;
            border-radius: 8px;
            font-size: 13px;
            color: #666;
        }
        .footer strong {
            color: #333;
        }
        .no-data {
            color: #999;
            font-style: italic;
            padding: 20px;
            text-align: center;
            background-color: #f8f9fa;
            border-radius: 4px;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
        }
        .badge-success {
            background-color: #d4edda;
            color: #155724;
        }
        .badge-warning {
            background-color: #fff3cd;
            color: #856404;
        }
        .badge-danger {
            background-color: #f8d7da;
            color: #721c24;
        }
        .degraded-alert {
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 20px;
            margin: 20px 0;
            border-radius: 4px;
        }
        .degraded-alert h2 {
            margin-top: 0;
            color: #856404;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏡 SRQ Pulse</h1>
        <p>Week of {{ date }}</p>
    </div>
    
    <div class="content">
        {% if is_degraded %}
        <div class="degraded-alert">
            <h2>⚠️ Pipeline Degraded</h2>
            <p>The data ingestion pipeline encountered errors. Some or all data may be unavailable for this week's report.</p>
            <div style="background-color: #fff; padding: 15px; border-radius: 4px; margin-top: 15px; font-family: monospace; font-size: 12px;">
                {{ error_log }}
            </div>
        </div>
        {% else %}
        
        <div class="metric-section">
            <h2>📉 Price Pressure Index</h2>
            <p>Median sale price + sale-to-list ratio trends (last 4 weeks).</p>
            {% if price_pressure|length > 0 %}
            <table>
                <thead>
                     <tr>
                        <th>Week</th>
                        <th>Median Price</th>
                        <th>WoW Δ</th>
                        <th>YoY Δ</th>
                        <th>Sale-to-List</th>
                        <th>Signal</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in price_pressure %}
                    <tr>
                        <td>{{ row.week }}</td>
                        <td>${{ "{:,.0f}".format(row.median_price) if row.median_price else 'N/A' }}</td>
                        <td>{{ "{:+.1%}".format(row.price_delta) if row.price_delta else '—' }}</td>
                        <td>{{ "{:+.1%}".format(row.price_yoy) if row.price_yoy else '—' }} YoY</td>
                        <td>{{ "{:.2%}".format(row.sale_to_list) if row.sale_to_list else 'N/A' }}</td>
                        <td>{{ row.signal }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Data unavailable this week (Redfin source failed or not yet configured)</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>📦 Inventory & Absorption</h2>
            <p>Weeks of supply + new listings vs homes sold (last 4 weeks).</p>
            {% if inventory|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Weeks of Supply</th>
                        <th>YoY Δ</th>
                        <th>New Listings</th>
                        <th>Homes Sold</th>
                        <th>Market State</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in inventory %}
                    <tr>
                        <td>{{ row.week }}</td>
                        <td>{{ "{:.1f}".format(row.weeks_of_supply) if row.weeks_of_supply else 'N/A' }}</td>
                        <td>{{ "{:+.1%}".format(row.supply_yoy) if row.supply_yoy else '—' }} YoY</td>
                        <td>{{ "{:,.0f}".format(row.new_listings) if row.new_listings else 'N/A' }}</td>
                        <td>{{ "{:,.0f}".format(row.homes_sold) if row.homes_sold else 'N/A' }}</td>
                        <td>{{ row.market_state }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Data unavailable this week (Redfin source failed or not yet configured)</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>📈 Cash Flow Trend (County-Level)</h2>
            <p>ZORI / ZHVI ratio over the last 6 months.</p>
            {% if trend_lines|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Month</th>
                        <th>ZHVI</th>
                        <th>ZORI</th>
                        <th>Annual Ratio</th>
                        <th>Direction</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in trend_lines %}
                    <tr>
                        <td>{{ row.month }}</td>
                        <td>${{ "{:,.0f}".format(row.zhvi) }}</td>
                        <td>${{ "{:,.0f}".format(row.zori) }}/mo</td>
                        <td>{{ "{:.2%}".format(row.flow_ratio) }}</td>
                        <td>{{ row.direction }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Trend data unavailable</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>💰 Cash Flow Zones</h2>
            <p>Sarasota zip codes ranked by monthly rent / home value ratio.</p>
            {% if cash_flow_zones|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Zip</th>
                        <th>Avg Assessed (JUST)</th>
                        <th>Est. Annual Rent</th>
                        <th>Cash Flow Ratio</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in cash_flow_zones[:10] %}
                    <tr>
                        <td>{{ row.rank }}</td>
                        <td>{{ row.zip }}</td>
                        <td>${{ "{:,.0f}".format(row.avg_assessed) }}</td>
                        <td>${{ "{:,.0f}".format(row.est_annual_rent) }}</td>
                        <td><span class="badge badge-success">{{ "{:.1%}".format(row.cash_flow_ratio) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Data unavailable this week (Zillow source failed)</div>
            {% endif %}
        </div>

        <div class="metric-section">
            <h2>📍 Zip-Level Price Trends</h2>
            <p>Median sale price per zip (Last 12 Months vs Prior Year).</p>
            {% if zip_price_trends|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Median Price (Current)</th>
                        <th>Median Price (Prior)</th>
                        <th>YoY Change</th>
                        <th>Sales Volume</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in zip_price_trends %}
                    <tr>
                        <td>{{ row.zip }}</td>
                        <td>${{ "{:,.0f}".format(row.price_now) }}</td>
                        <td>${{ "{:,.0f}".format(row.price_prior) }}</td>
                        <td><span class="badge {% if row.yoy_change > 0 %}badge-warning{% else %}badge-success{% endif %}">{{ "{:+.1%}".format(row.yoy_change) }}</span></td>
                        <td>{{ row.sales_volume }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Zip-level price trends unavailable</div>
            {% endif %}
        </div>

        <div class="metric-section">
            <h2>📊 Assessment Ratio by Zip</h2>
            <p>Sale Price vs County Appraised Value. Ratios below 1.0 indicate cooling.</p>
            {% if assessment_ratio|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Median Ratio</th>
                        <th>Meaning</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in assessment_ratio %}
                    <tr>
                        <td>{{ row.zip }}</td>
                        <td>{{ "{:.2f}".format(row.median_ratio) }}</td>
                        <td>{{ row.meaning }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Assessment ratio data unavailable</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>🔄 Flip Activity (Last 6 Months)</h2>
            <p><strong>{{ flip_summary }}</strong></p>
            
            {% if profitable_flips|length > 0 %}
            <h3 style="color: #2e7d32; margin-top: 15px;">Recent Profitable Flips</h3>
            <table>
                <thead>
                    <tr>
                        <th>Account</th>
                        <th>Bought for</th>
                        <th>Sold for</th>
                        <th>Markup</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in profitable_flips[:10] %}
                    <tr>
                        <td>{{ row.account }}</td>
                        <td>${{ "{:,.0f}".format(row.first_sale_price) }} ({{ row.first_sale_date }})</td>
                        <td>${{ "{:,.0f}".format(row.second_sale_price) }} ({{ row.second_sale_date }})</td>
                        <td><span class="badge badge-success">{{ "{:+.1%}".format(row.markup_pct) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}

            {% if loss_flips|length > 0 %}
            <h3 style="color: #c62828; margin-top: 15px;">Recent Flips at a Loss</h3>
            <table>
                <thead>
                    <tr>
                        <th>Account</th>
                        <th>Bought for</th>
                        <th>Sold for</th>
                        <th>Loss</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in loss_flips[:10] %}
                    <tr>
                        <td>{{ row.account }}</td>
                        <td>${{ "{:,.0f}".format(row.first_sale_price) }} ({{ row.first_sale_date }})</td>
                        <td>${{ "{:,.0f}".format(row.second_sale_price) }} ({{ row.second_sale_date }})</td>
                        <td><span class="badge badge-danger">{{ "{:+.1%}".format(row.markup_pct) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}

            {% if profitable_flips|length == 0 and loss_flips|length == 0 %}
            <div class="no-data">No flips detected in the last 180 days</div>
            {% endif %}
        </div>

        <div class="metric-section">
            <h2>🏢 Investor Activity by Zip</h2>
            <p>Likely investor share of total sales (Last 12 Months).</p>
            {% if investor_activity|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Total Sales</th>
                        <th>Investor %</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in investor_activity %}
                    <tr>
                        <td>{{ row.zip }}</td>
                        <td>{{ row.total_sales }}</td>
                        <td><span class="badge badge-warning">{{ "{:.1%}".format(row.investor_share) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">Investor activity data unavailable</div>
            {% endif %}
        </div>
        
        {% endif %}
        
        <div class="footer">
            <strong>⚙️ Pipeline Health:</strong><br>
            {% if stats.zillow_status %}<span class="{{ 'badge-success' if stats.zillow_status == 'OK' else 'badge-danger' }}">Zillow: {{ stats.zillow_status }}</span> {% endif %}
            {% if stats.redfin_status %}<span class="{{ 'badge-success' if stats.redfin_status == 'OK' else 'badge-danger' }}">Redfin: {{ stats.redfin_status }}</span> {% endif %}
            {% if stats.scpa_status %}<span class="{{ 'badge-success' if stats.scpa_status == 'OK' else 'badge-danger' }}">SCPA: {{ stats.scpa_status }}</span> {% endif %}
            <br>
            Execution Time: {{ stats.execution_time }}<br>
            <br>
            <em>This report is generated automatically every Monday by a serverless ETL pipeline. Data sourced from Redfin Data Center, Zillow Research, and Sarasota County Property Appraiser.</em>
        </div>
    </div>
</body>
</html>
"""


def render_email(results: dict, stats: dict, is_degraded: bool = False, error_log: str = "") -> str:
    """
    Render HTML email from transformation results (V4).
    
    Args:
        results: Dict of DataFrames from transform.py
        stats: Pipeline execution statistics
        is_degraded: Whether pipeline is in degraded mode
        error_log: Error messages if degraded
        
    Returns:
        Rendered HTML string
    """
    template = Template(EMAIL_TEMPLATE)
    
    # Convert DataFrames to list of dicts for template rendering
    def df_to_list(df):
        if df is None or len(df) == 0:
            return []
        return df.to_dict('records')
    
    rendered = template.render(
        date=datetime.now().strftime("%B %d, %Y"),
        is_degraded=is_degraded,
        error_log=error_log,
        price_pressure=df_to_list(results.get('price_pressure', pd.DataFrame())),
        inventory=df_to_list(results.get('inventory_absorption', pd.DataFrame())),
        trend_lines=df_to_list(results.get('trend_lines', pd.DataFrame())),
        cash_flow_zones=df_to_list(results.get('cash_flow_zones', pd.DataFrame())),
        zip_price_trends=df_to_list(results.get('zip_price_trends', pd.DataFrame())),
        assessment_ratio=df_to_list(results.get('assessment_ratio', pd.DataFrame())),
        profitable_flips=df_to_list(results.get('flip_detector', pd.DataFrame())[results.get('flip_detector', pd.DataFrame())['outcome'] == 'PROFITABLE'] if not results.get('flip_detector', pd.DataFrame()).empty else pd.DataFrame()),
        loss_flips=df_to_list(results.get('flip_detector', pd.DataFrame())[results.get('flip_detector', pd.DataFrame())['outcome'] == 'LOSS'] if not results.get('flip_detector', pd.DataFrame()).empty else pd.DataFrame()),
        flip_summary=results.get('flip_summary', 'No flips detected'),
        investor_activity=df_to_list(results.get('investor_activity', pd.DataFrame())),
        stats=stats
    )
    
    return rendered


def send_email(html_content: str, subject: str) -> bool:
    """
    Send email via Gmail SMTP with app password.
    
    Requires environment variables:
    - GMAIL_USER: Your Gmail address (e.g., yourname@gmail.com)
    - GMAIL_APP_PASSWORD: Gmail app password (not your regular password!)
    - EMAIL_TO: Recipient email address
    
    Args:
        html_content: Rendered HTML email
        subject: Email subject line
        
    Returns:
        bool: True if sent successfully
    """
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    email_to = os.environ.get("EMAIL_TO")
    
    if not gmail_user:
        logger.error("GMAIL_USER environment variable not set")
        return False
    
    if not gmail_password:
        logger.error("GMAIL_APP_PASSWORD environment variable not set")
        return False
    
    if not email_to:
        logger.error("EMAIL_TO environment variable not set")
        return False
    
    try:
        # Create message
        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = gmail_user
        message['To'] = email_to
        
        # Attach HTML content
        html_part = MIMEText(html_content, 'html')
        message.attach(html_part)
        
        # Connect to Gmail SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.send_message(message)
        
        logger.info(f"✅ Email sent successfully to {email_to}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {type(e).__name__}: {str(e)}")
        return False


def deliver_report(results: dict, stats: dict, is_degraded: bool = False):
    """
    Render and send the weekly market pulse report (V4).
    
    Args:
        results: Transformation results
        stats: Pipeline statistics
        is_degraded: Whether pipeline is in degraded mode
    """
    logger.info("=" * 60)
    logger.info("STARTING EMAIL DELIVERY (V4 WEEKLY FORMAT)")
    logger.info("=" * 60)
    
    # Load error log if degraded
    error_log = ""
    if is_degraded:
        error_log_path = Path("data/errors.log")
        if error_log_path.exists():
            with open(error_log_path, "r") as f:
                error_log = f.read()
    
    # Render email
    html_content = render_email(results, stats, is_degraded, error_log)
    
    # Determine subject line (V4: weekly format)
    week_of = datetime.now().strftime("%b %d, %Y")
    if is_degraded:
        subject = f"⚠️ SRQ Pulse — Pipeline Degraded — Week of {week_of}"
    else:
        subject = f"🏡 SRQ Pulse — Week of {week_of}"
    
    # Send email
    success = send_email(html_content, subject)
    
    if success:
        logger.info("✅ Report delivered successfully")
    else:
        logger.error("❌ Failed to deliver report")
    
    return success


if __name__ == "__main__":
    # Test with mock data
    mock_results = {
        'price_pressure': pd.DataFrame([
            {'week': 'Feb 08', 'median_price': 418250, 'price_delta': -0.023, 'price_yoy': 0.045, 'sale_to_list': 0.9526, 'signal': 'BUYERS LEVERAGE'},
            {'week': 'Feb 01', 'median_price': 428000, 'price_delta': 0.012, 'price_yoy': 0.051, 'sale_to_list': 0.9610, 'signal': 'NEUTRAL'}
        ]),
        'inventory_absorption': pd.DataFrame([
            {'week': 'Feb 08', 'weeks_of_supply': 12.4, 'supply_yoy': 0.15, 'new_listings': 450, 'homes_sold': 380, 'market_state': 'BALANCED MARKET'}
        ]),
        'trend_lines': pd.DataFrame([
            {'month': 'Jan 2026', 'zhvi': 450000, 'zori': 2200, 'flow_ratio': 0.0586, 'direction': '↑ Expanding'},
            {'month': 'Dec 2025', 'zhvi': 455000, 'zori': 2180, 'flow_ratio': 0.0575, 'direction': '→ Flat'}
        ]),
        'cash_flow_zones': pd.DataFrame([
            {'rank': 1, 'zip': 34231, 'avg_assessed': 350000, 'est_annual_rent': 26400, 'cash_flow_ratio': 0.0754},
            {'rank': 2, 'zip': 34233, 'avg_assessed': 420000, 'est_annual_rent': 26400, 'cash_flow_ratio': 0.0628}
        ]),
        'zip_price_trends': pd.DataFrame([
            {'zip': 34231, 'price_now': 450000, 'price_prior': 420000, 'yoy_change': 0.071, 'sales_volume': 120}
        ]),
        'assessment_ratio': pd.DataFrame([
            {'zip': 34231, 'median_ratio': 1.15, 'meaning': 'Stable/Hot (above assessed)'},
            {'zip': 34233, 'median_ratio': 0.92, 'meaning': 'Market cooling (below assessed)'}
        ]),
        'profitable_flips': pd.DataFrame([
            {'account': '123456', 'first_sale_date': 'Aug 12', 'first_sale_price': 250000, 'second_sale_date': 'Feb 05', 'second_sale_price': 385000, 'markup_pct': 0.54, 'outcome': 'PROFITABLE'}
        ]),
        'loss_flips': pd.DataFrame([
            {'account': '789012', 'first_sale_date': 'Sep 10', 'first_sale_price': 400000, 'second_sale_date': 'Jan 20', 'second_sale_price': 380000, 'markup_pct': -0.05, 'outcome': 'LOSS'}
        ]),
        'flip_summary': '2 total — 1 profitable, 1 loss',
        'investor_activity': pd.DataFrame([
            {'zip': 34231, 'total_sales': 250, 'investor_share': 0.38}
        ]),
    }
    
    mock_stats = {
        'zillow_status': 'OK',
        'redfin_status': 'OK',
        'scpa_status': 'OK',
        'execution_time': '12.4s'
    }
    
    html = render_email(mock_results, mock_stats)
    with open("test_report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Created test_report.html")
