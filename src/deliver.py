"""
Sarasota Market Pulse — Email Delivery System

Renders HTML email reports using Jinja2 templates and sends via Resend API.
Includes both normal mode (full market report) and degraded mode (error notification).
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


# HTML Email Template
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
    </style>
</head>
<body>
    <div class="header">
        <h1>🏡 Sarasota Market Pulse</h1>
        <p>{{ date }}</p>
    </div>
    
    <div class="content">
        {% if is_degraded %}
        <div style="background-color: #fff3cd; border-left: 4px solid #ffc107; padding: 20px; margin: 20px 0; border-radius: 4px;">
            <h2 style="margin-top: 0; color: #856404;">⚠️ Pipeline Degraded</h2>
            <p>The data ingestion pipeline encountered errors. Some or all data may be unavailable for today's report.</p>
            <div style="background-color: #fff; padding: 15px; border-radius: 4px; margin-top: 15px; font-family: monospace; font-size: 12px;">
                {{ error_log }}
            </div>
        </div>
        {% else %}
        
        <div class="metric-section">
            <h2>🔥 Panic Sellers (Price Cut Velocity)</h2>
            <p>Properties with significant price drops (&gt;$10k) within first 14 days on market.</p>
            {% if panic_sellers|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>List Price</th>
                        <th>Price Drop</th>
                        <th>Days on Market</th>
                    </tr>
                </thead>
                <tbody>
                    {% for property in panic_sellers %}
                    <tr>
                        <td>{{ property.address }}</td>
                        <td>${{ "{:,.0f}".format(property.list_price_today) }}</td>
                        <td><span class="badge badge-danger">${{ "{:,.0f}".format(property.price_delta|abs) }}</span></td>
                        <td>{{ property.days_on_market }} days</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">No panic sellers detected today.</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>🏚️ Stale Listings</h2>
            <p>Properties sitting for 90+ days - potentially overpriced and ripe for negotiation.</p>
            {% if stale_listings|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>List Price</th>
                        <th>Days on Market</th>
                    </tr>
                </thead>
                <tbody>
                    {% for property in stale_listings[:10] %}
                    <tr>
                        <td>{{ property.address }}</td>
                        <td>${{ "{:,.0f}".format(property.list_price) }}</td>
                        <td><span class="badge badge-warning">{{ property.days_on_market }} days</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% if stale_listings|length > 10 %}
            <p style="color: #666; font-size: 13px; margin-top: 10px;">
                Showing top 10 of {{ stale_listings|length }} total stale listings.
            </p>
            {% endif %}
            {% else %}
            <div class="no-data">No stale listings found.</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>💰 Cash Flow Picks (0.8% Rule)</h2>
            <p>Properties meeting the 0.8% monthly rent-to-price ratio for positive cash flow.</p>
            {% if cash_flow|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>List Price</th>
                        <th>Est. Rent</th>
                        <th>CF Ratio</th>
                    </tr>
                </thead>
                <tbody>
                    {% for property in cash_flow[:10] %}
                    <tr>
                        <td>{{ property.address }}</td>
                        <td>${{ "{:,.0f}".format(property.list_price) }}</td>
                        <td>${{ "{:,.0f}".format(property.estimated_rent) }}/mo</td>
                        <td><span class="badge badge-success">{{ "{:.2%}".format(property.cash_flow_ratio) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% if cash_flow|length > 10 %}
            <p style="color: #666; font-size: 13px; margin-top: 10px;">
                Showing top 10 of {{ cash_flow|length }} total cash flow opportunities.
            </p>
            {% endif %}
            {% else %}
            <div class="no-data">No properties passing the 0.8% cash flow screen today.</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>🔄 Probable Flips</h2>
            <p>Properties purchased 4-12 months ago and now re-listed - inspect renovation quality.</p>
            {% if flips|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>List Price</th>
                        <th>Sale Date</th>
                        <th>Sale Price</th>
                    </tr>
                </thead>
                <tbody>
                    {% for property in flips %}
                    <tr>
                        <td>{{ property.address }}</td>
                        <td>${{ "{:,.0f}".format(property.list_price) }}</td>
                        <td>{{ property.SaleDate.strftime('%Y-%m-%d') if property.SaleDate else 'N/A' }}</td>
                        <td>${{ "{:,.0f}".format(property.SalePrice) if property.SalePrice else 'N/A' }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">No probable flips detected today.</div>
            {% endif %}
        </div>
        
        <div class="metric-section">
            <h2>📊 Appraisal Gaps</h2>
            <p>Properties with significant deviation from county appraised values.</p>
            {% if appraisal_gaps|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Address</th>
                        <th>List Price</th>
                        <th>County Value</th>
                        <th>Gap</th>
                        <th>Flag</th>
                    </tr>
                </thead>
                <tbody>
                    {% for property in appraisal_gaps %}
                    <tr>
                        <td>{{ property.address }}</td>
                        <td>${{ "{:,.0f}".format(property.list_price) }}</td>
                        <td>${{ "{:,.0f}".format(property.JUST) }}</td>
                        <td>{{ "{:+.1%}".format(property.appraisal_gap) }}</td>
                        <td>
                            <span class="badge {% if property.gap_flag == 'OVERPRICED' %}badge-warning{% else %}badge-success{% endif %}">
                                {{ property.gap_flag }}
                            </span>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% else %}
            <div class="no-data">No significant appraisal gaps detected.</div>
            {% endif %}
        </div>
        
        {% endif %}
        
        <div class="footer">
            <strong>Pipeline Health:</strong><br>
            Records Ingested: {{ stats.records_ingested }}<br>
            County Data: {{ "✅ Available" if stats.county_available else "❌ Unavailable" }}<br>
            Execution Time: {{ stats.execution_time }}<br>
            <br>
            <em>This report is generated automatically by a serverless ELT pipeline. Data sourced from public MLS feeds and Sarasota County Property Appraiser records.</em>
        </div>
    </div>
</body>
</html>
"""


def render_email(results: dict, stats: dict, is_degraded: bool = False, error_log: str = "") -> str:
    """
    Render HTML email from transformation results.
    
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
        panic_sellers=df_to_list(results.get('price_cut_velocity', pd.DataFrame())),
        stale_listings=df_to_list(results.get('stale_hunter', pd.DataFrame())),
        cash_flow=df_to_list(results.get('cash_flow_screen', pd.DataFrame())),
        flips=df_to_list(results.get('flip_detector', pd.DataFrame())),
        appraisal_gaps=df_to_list(results.get('appraisal_gap', pd.DataFrame())),
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
    Render and send the market pulse report.
    
    Args:
        results: Transformation results
        stats: Pipeline statistics
        is_degraded: Whether pipeline is in degraded mode
    """
    logger.info("=" * 60)
    logger.info("STARTING EMAIL DELIVERY")
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
    
    # Determine subject line
    today = datetime.now().strftime("%Y-%m-%d")
    if is_degraded:
        subject = f"⚠️ Sarasota Market Pulse — Pipeline Degraded — {today}"
    else:
        subject = f"🏡 Sarasota Market Pulse — {today}"
    
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
        'price_cut_velocity': pd.DataFrame(),
        'stale_hunter': pd.DataFrame(),
        'cash_flow_screen': pd.DataFrame(),
        'flip_detector': pd.DataFrame(),
        'appraisal_gap': pd.DataFrame(),
    }
    
    mock_stats = {
        'records_ingested': 0,
        'county_available': False,
        'execution_time': '0s'
    }
    
    deliver_report(mock_results, mock_stats, is_degraded=True)
