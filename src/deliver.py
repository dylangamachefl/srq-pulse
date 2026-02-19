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


# HTML Email Template (V5 - Consumer-Friendly Redesign)
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
            background-color: #f0f4f8;
        }
        .header {
            background: linear-gradient(135deg, #1a56db 0%, #1e7e5e 100%);
            color: white;
            padding: 30px;
            border-radius: 8px 8px 0 0;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 28px;
            letter-spacing: -0.5px;
        }
        .header p {
            margin: 8px 0 0 0;
            opacity: 0.85;
            font-size: 15px;
        }
        .content {
            background: white;
            padding: 30px;
            border-radius: 0 0 8px 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }
        .metric-section {
            margin: 30px 0;
            border-left: 4px solid #1a56db;
            padding-left: 20px;
        }
        .metric-section h2 {
            margin-top: 0;
            color: #1a56db;
            font-size: 18px;
        }
        .section-teal { border-left-color: #0e9f6e; }
        .section-teal h2 { color: #0e9f6e; }
        .section-orange { border-left-color: #d03801; }
        .section-orange h2 { color: #d03801; }
        .section-green { border-left-color: #057a55; }
        .section-green h2 { color: #057a55; }
        .section-red { border-left-color: #c81e1e; }
        .section-red h2 { color: #c81e1e; }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 14px;
        }
        th {
            background-color: #f3f4f6;
            padding: 11px 12px;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #e5e7eb;
            font-size: 13px;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #e5e7eb;
            vertical-align: top;
        }
        tr:hover {
            background-color: #f9fafb;
        }
        .footer {
            margin-top: 30px;
            padding: 20px;
            background-color: #f3f4f6;
            border-radius: 8px;
            font-size: 13px;
            color: #6b7280;
        }
        .footer strong {
            color: #374151;
        }
        .no-data {
            color: #9ca3af;
            font-style: italic;
            padding: 20px;
            text-align: center;
            background-color: #f9fafb;
            border-radius: 4px;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
        }
        .badge-success {
            background-color: #def7ec;
            color: #03543f;
        }
        .badge-warning {
            background-color: #fef3c7;
            color: #92400e;
        }
        .badge-danger {
            background-color: #fde8e8;
            color: #9b1c1c;
        }
        .badge-down {
            background-color: #dbeafe;
            color: #1e40af;
        }
        .badge-neutral {
            background-color: #f3f4f6;
            color: #4b5563;
        }
        .degraded-alert {
            background-color: #fef3c7;
            border-left: 4px solid #f59e0b;
            padding: 20px;
            margin: 20px 0;
            border-radius: 4px;
        }
        .degraded-alert h2 {
            margin-top: 0;
            color: #92400e;
        }
        .market-phase-banner {
            background: linear-gradient(90deg, #eff6ff 0%, #f0fdf4 100%);
            border: 1px solid #bfdbfe;
            border-radius: 8px;
            padding: 16px 20px;
            margin: 0 0 24px 0;
            text-align: center;
        }
        .market-phase-label {
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            color: #6b7280;
            margin-bottom: 4px;
        }
        .market-phase-value {
            font-size: 20px;
            font-weight: 700;
            color: #1e3a5f;
        }
        .snapshot-table {
            width: 100%;
            border-collapse: collapse;
            margin: 12px 0 0 0;
        }
        .snapshot-cell {
            width: 50%;
            padding: 14px 16px;
            background-color: #f9fafb;
            border: 1px solid #e5e7eb;
            vertical-align: top;
        }
        .snapshot-cell:first-child {
            border-right: none;
            border-radius: 6px 0 0 6px;
        }
        .snapshot-cell:last-child {
            border-radius: 0 6px 6px 0;
        }
        .snapshot-label {
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #6b7280;
            margin-bottom: 4px;
        }
        .snapshot-value {
            font-size: 26px;
            font-weight: 700;
            color: #111827;
            line-height: 1.1;
        }
        .snapshot-sub {
            font-size: 12px;
            color: #6b7280;
            margin-top: 4px;
        }
        .insight-box {
            background-color: #f9fafb;
            border-left: 3px solid #9ca3af;
            padding: 10px 14px;
            margin: 12px 0 4px 0;
            font-size: 13px;
            color: #4b5563;
            border-radius: 0 4px 4px 0;
        }
        .insight-box strong {
            color: #1f2937;
        }
        .insight-orange {
            border-left-color: #f59e0b;
        }
        .data-note {
            font-size: 11px;
            color: #9ca3af;
            font-style: italic;
        }
        .prop-detail {
            font-size: 12px;
            color: #6b7280;
            margin-top: 2px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏡 SRQ Pulse</h1>
        <p>Sarasota Real Estate &mdash; Week of {{ date }}</p>
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

        <!-- MARKET SNAPSHOT -->
        {% if market_snapshot %}
        <div class="market-phase-banner">
            <div class="market-phase-label">This Week's Market Condition</div>
            <div class="market-phase-value">{{ market_snapshot.market_phase }}</div>
        </div>

        <div class="metric-section" style="border-left-color: #1a56db;">
            <h2 style="color: #1a56db;">📋 Market Snapshot</h2>
            <p style="color: #6b7280; font-size: 13px; margin-bottom: 0;">Key numbers for the week of {{ date }}</p>

            <table class="snapshot-table">
                <tr>
                    <td class="snapshot-cell">
                        <div class="snapshot-label">Median Sale Price</div>
                        {% if market_snapshot.median_price %}
                        <div class="snapshot-value">${{ "{:,.0f}".format(market_snapshot.median_price) }}</div>
                        <div class="snapshot-sub">{{ market_snapshot.price_yoy_label }}</div>
                        {% else %}
                        <div class="snapshot-value">—</div>
                        {% endif %}
                    </td>
                    <td class="snapshot-cell">
                        <div class="snapshot-label">Months of Supply</div>
                        {% if market_snapshot.weeks_supply %}
                        <div class="snapshot-value">{{ "{:.0f}".format(market_snapshot.weeks_supply) }} mo</div>
                        <div class="snapshot-sub">{{ market_snapshot.supply_label }}</div>
                        {% else %}
                        <div class="snapshot-value">—</div>
                        {% endif %}
                    </td>
                </tr>
            </table>

            <div class="insight-box">
                <strong>What this means:</strong> {{ market_snapshot.headline }}
            </div>
            {% if market_snapshot.hottest_zip %}
            <div class="insight-box insight-orange">
                <strong>Trending neighborhood:</strong> {{ market_snapshot.hottest_zip_label }}
            </div>
            {% endif %}
            {% if market_snapshot.best_value_zip %}
            <div class="insight-box">
                <strong>Best value area:</strong> {{ market_snapshot.best_value_label }}
            </div>
            {% endif %}
        </div>
        {% endif %}

        <!-- SECTION 1: ARE PRICES RISING OR FALLING? -->
        <div class="metric-section">
            <h2>📉 Are Prices Rising or Falling?</h2>
            <p>Median sale price and how close homes are selling to asking price (last 4 weeks).</p>
            {% if price_pressure|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Median Price</th>
                        <th>Week-over-Week</th>
                        <th>Year-over-Year</th>
                        <th>Sale-to-List</th>
                        <th>Signal</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in price_pressure %}
                    <tr>
                        <td>{{ row.week }}</td>
                        <td><strong>${{ "{:,.0f}".format(row.median_price) if row.median_price else 'N/A' }}</strong></td>
                        <td>{{ "{:+.1%}".format(row.price_delta) if (row.price_delta is not none and row.price_delta == row.price_delta) else '—' }}</td>
                        <td>{{ "{:+.1%}".format(row.price_yoy) if row.price_yoy is not none else '—' }}</td>
                        <td>{{ "{:.1%}".format(row.sale_to_list) if row.sale_to_list else 'N/A' }}</td>
                        <td>{{ row.signal }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% set latest_pp = price_pressure[-1] %}
            <div class="insight-box">
                <strong>What to know:</strong>
                {% if latest_pp.sale_to_list and latest_pp.sale_to_list < 0.97 %}
                Homes are selling below asking price ({{ "{:.1%}".format(latest_pp.sale_to_list) }} of list on average). Buyers may have room to negotiate.
                {% elif latest_pp.sale_to_list and latest_pp.sale_to_list > 1.0 %}
                Homes are selling above asking price ({{ "{:.1%}".format(latest_pp.sale_to_list) }} of list). Expect competition and consider offering at or above list price.
                {% else %}
                Homes are selling near asking price. The market is fairly balanced right now.
                {% endif %}
            </div>
            {% else %}
            <div class="no-data">Price data unavailable this week (Redfin source failed)</div>
            {% endif %}
        </div>

        <!-- SECTION 2: HOW MUCH HOUSING IS AVAILABLE? -->
        <div class="metric-section section-teal">
            <h2>📦 How Much Housing Is Available?</h2>
            <p>Months of supply, new listings coming to market, and homes going under contract (last 4 weeks).</p>
            {% if inventory|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Months of Supply</th>
                        <th>YoY Change</th>
                        <th>New Listings</th>
                        <th>Homes Sold</th>
                        <th>Market Condition</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in inventory %}
                    <tr>
                        <td>{{ row.week }}</td>
                        <td><strong>{{ "{:.1f}".format(row.weeks_of_supply) if row.weeks_of_supply is not none else 'N/A' }}</strong></td>
                        <td>{{ "{:+.1%}".format(row.supply_yoy) if row.supply_yoy is not none else 'N/A' }}</td>
                        <td>{{ "{:,.0f}".format(row.new_listings) if row.new_listings else 'N/A' }}</td>
                        <td>{{ "{:,.0f}".format(row.homes_sold) if row.homes_sold else 'N/A' }}</td>
                        <td>{{ row.market_state }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% set latest_inv = inventory[0] %}
            <div class="insight-box">
                <strong>What to know:</strong>
                {% if latest_inv.weeks_of_supply and latest_inv.weeks_of_supply > 18 %}
                At {{ "{:.0f}".format(latest_inv.weeks_of_supply) }} months of supply, buyers have significant negotiating power — there are more homes available than buyers right now.
                {% elif latest_inv.weeks_of_supply and latest_inv.weeks_of_supply < 8 %}
                At only {{ "{:.0f}".format(latest_inv.weeks_of_supply) }} months of supply, homes are moving fast. Buyers should be prepared to act quickly.
                {% else %}
                The market has a relatively balanced amount of inventory.
                {% endif %}
                {% if latest_inv.new_listings and latest_inv.homes_sold %}
                New listings ({{ "{:,.0f}".format(latest_inv.new_listings) }}) are {{ 'outpacing' if latest_inv.new_listings > latest_inv.homes_sold else 'below' }} homes sold ({{ "{:,.0f}".format(latest_inv.homes_sold) }}) this week.
                {% endif %}
            </div>
            {% else %}
            <div class="no-data">Inventory data unavailable this week (Redfin source failed)</div>
            {% endif %}
        </div>

        <!-- SECTION 3: HOME VALUE & RENT TREND -->
        <div class="metric-section section-teal">
            <h2>📈 Sarasota Home Value &amp; Rent Trend</h2>
            <p>How average home values and typical rents have moved over the past 6 months (county-wide).</p>
            {% if trend_lines|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Month</th>
                        <th>Avg. Home Value (Zillow)</th>
                        <th>Typical Monthly Rent</th>
                        <th>Rent-to-Value Ratio</th>
                        <th>Trend</th>
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
            {% set latest_tl = trend_lines[-1] %}
            <div class="insight-box">
                <strong>What this means:</strong>
                A {{ "{:.1%}".format(latest_tl.flow_ratio) }} annual rent-to-value ratio means that for a typical
                ${{ "{:,.0f}".format(latest_tl.zhvi) }} home, gross annual rent is roughly ${{ "{:,.0f}".format(latest_tl.zori * 12) }}.
                This is a county-wide estimate — actual figures vary significantly by neighborhood.
            </div>
            {% else %}
            <div class="no-data">Home value trend data unavailable (Zillow source failed)</div>
            {% endif %}
        </div>

        <!-- SECTION 4: WHERE ARE HOMES PRICED FAIRLY? (Buyer Value Index) -->
        <div class="metric-section section-orange">
            <h2>🏘️ Where Are Homes Priced Fairly?</h2>
            <p>Comparing recent median sale prices to county-assessed values by zip code (last 12 months, residential only).</p>
            {% if buyer_value_index|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Median Sale Price</th>
                        <th>Avg. Assessed Value</th>
                        <th>Sale vs. Assessed</th>
                        <th>Buyer Signal</th>
                        <th>Sales</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in buyer_value_index %}
                    <tr>
                        <td><strong>{{ row.zip }}</strong></td>
                        <td>${{ "{:,.0f}".format(row.median_sale_price) }}</td>
                        <td>${{ "{:,.0f}".format(row.avg_assessed) }}</td>
                        <td>
                            <span class="badge {% if row.value_ratio > 1.3 %}badge-warning{% elif row.value_ratio < 0.95 %}badge-success{% else %}badge-neutral{% endif %}">
                                {{ "{:.2f}x".format(row.value_ratio) }}
                            </span>
                        </td>
                        <td>{{ row.buyer_signal }}</td>
                        <td class="data-note">{{ row.sales_volume }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div class="insight-box">
                <strong>How to read this:</strong> A ratio above 1.0x means buyers are paying more than the county's assessed
                value — common in active markets. Below 1.0x may indicate a softer neighborhood. County assessments
                typically lag the market by 1–2 years, so this is a relative comparison, not an appraisal.
            </div>
            {% else %}
            <div class="no-data">Buyer value data unavailable this week (county data required)</div>
            {% endif %}
        </div>

        <!-- SECTION 5: PRICE CHANGES BY NEIGHBORHOOD -->
        <div class="metric-section">
            <h2>📍 Price Changes by Neighborhood (Year Over Year)</h2>
            <p>Median sale price in each zip code — last 12 months vs. the year before. Residential sales only. Zips marked * have fewer than 20 sales (treat as directional only).</p>
            {% if zip_price_trends|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Current Median</th>
                        <th>Prior Year Median</th>
                        <th>YoY Change</th>
                        <th>Sales</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in zip_price_trends %}
                    <tr>
                        <td><strong>{{ row.zip_display if row.zip_display else row.zip }}</strong></td>
                        <td>${{ "{:,.0f}".format(row.price_now) }}</td>
                        <td>${{ "{:,.0f}".format(row.price_prior) }}</td>
                        <td>
                            <span class="badge {% if row.yoy_change > 0.05 %}badge-warning{% elif row.yoy_change < -0.05 %}badge-down{% else %}badge-neutral{% endif %}">
                                {{ "{:+.1%}".format(row.yoy_change) }}
                            </span>
                            {% if row.yoy_flag == 'low_data' %}<span class="data-note"> limited data</span>{% endif %}
                        </td>
                        <td class="data-note">{{ row.sales_volume }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div class="insight-box">
                <strong>What to know:</strong> These are recorded county sales prices, not list prices or Zestimate estimates.
                Orange badge = prices rose vs. last year. Blue badge = prices fell.
                Large swings (like +40%) in low-volume zips may reflect a single unusual sale — look at the sales count.
            </div>
            {% else %}
            <div class="no-data">Zip-level price trends unavailable (county data required)</div>
            {% endif %}
        </div>

        <!-- SECTION 6: SALE PRICE VS. COUNTY ASSESSMENT -->
        <div class="metric-section section-green">
            <h2>📊 Are Sale Prices Above or Below County Assessments?</h2>
            <p>The Sarasota County Property Appraiser assigns a value to each property. This shows how actual sale prices compare.</p>
            {% if assessment_ratio|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Median Ratio</th>
                        <th>What It Means</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in assessment_ratio %}
                    <tr>
                        <td><strong>{{ row.zip }}</strong></td>
                        <td>
                            <span class="badge {% if row.median_ratio > 1.2 %}badge-warning{% elif row.median_ratio < 0.95 %}badge-success{% else %}badge-neutral{% endif %}">
                                {{ "{:.2f}x".format(row.median_ratio) }}
                            </span>
                        </td>
                        <td>{{ row.meaning }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div class="insight-box">
                <strong>Context:</strong> County assessments typically lag market values by 1–2 years.
                A ratio of 1.22x means homes are selling 22% above what the county assessed them at —
                this is normal in an appreciating market. A ratio near or below 1.0x can signal cooling prices.
            </div>
            {% else %}
            <div class="no-data">Assessment ratio data unavailable (county data required)</div>
            {% endif %}
        </div>

        <!-- SECTION 7: FLIP ACTIVITY -->
        <div class="metric-section section-red">
            <h2>🔄 Flip Activity (Last 6 Months)</h2>
            <p><strong>{{ flip_summary }}</strong> &mdash; properties bought and resold within 4–12 months.</p>

            {% if profitable_flips|length > 0 %}
            <h3 style="color: #057a55; margin-top: 20px; font-size: 15px;">Recent Profitable Flips</h3>
            <table>
                <thead>
                    <tr>
                        <th>Property</th>
                        <th>Purchased</th>
                        <th>Sold</th>
                        <th>Gain</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in profitable_flips[:10] %}
                    <tr>
                        <td>
                            <strong>{{ row.address if row.address else 'Address unavailable' }}</strong>
                            {% if row.sqft and row.sqft == row.sqft %}
                            <div class="prop-detail">{{ "{:,.0f}".format(row.sqft) }} sqft{% if row.beds and row.beds == row.beds %}, {{ "{:.0f}".format(row.beds) }} bed{% endif %}</div>
                            {% endif %}
                        </td>
                        <td>${{ "{:,.0f}".format(row.first_sale_price) }}<br><span class="data-note">{{ row.first_sale_date }}</span></td>
                        <td>${{ "{:,.0f}".format(row.second_sale_price) }}<br><span class="data-note">{{ row.second_sale_date }}</span></td>
                        <td><span class="badge badge-success">{{ "{:+.1%}".format(row.markup_pct) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}

            {% if loss_flips|length > 0 %}
            <h3 style="color: #9b1c1c; margin-top: 20px; font-size: 15px;">Recent Flips at a Loss</h3>
            <table>
                <thead>
                    <tr>
                        <th>Property</th>
                        <th>Purchased</th>
                        <th>Sold</th>
                        <th>Loss</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in loss_flips[:10] %}
                    <tr>
                        <td>
                            <strong>{{ row.address if row.address else 'Address unavailable' }}</strong>
                            {% if row.sqft and row.sqft == row.sqft %}
                            <div class="prop-detail">{{ "{:,.0f}".format(row.sqft) }} sqft{% if row.beds and row.beds == row.beds %}, {{ "{:.0f}".format(row.beds) }} bed{% endif %}</div>
                            {% endif %}
                        </td>
                        <td>${{ "{:,.0f}".format(row.first_sale_price) }}<br><span class="data-note">{{ row.first_sale_date }}</span></td>
                        <td>${{ "{:,.0f}".format(row.second_sale_price) }}<br><span class="data-note">{{ row.second_sale_date }}</span></td>
                        <td><span class="badge badge-danger">{{ "{:+.1%}".format(row.markup_pct) }}</span></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}

            {% if profitable_flips|length == 0 and loss_flips|length == 0 %}
            <div class="no-data">No flips detected in the last 180 days</div>
            {% endif %}

            <div class="insight-box">
                <strong>What flips tell us:</strong> Profitable flips indicate investors successfully adding value or
                timing the market. Loss flips may signal overpriced purchases, renovation overruns, or softening
                prices in that area. Data is from Sarasota County recorded Warranty Deed transactions.
            </div>
        </div>

        <!-- SECTION 8: WHO IS BUYING? -->
        <div class="metric-section">
            <h2>🏢 Who Is Buying? Owners vs. Investors by Zip</h2>
            <p>Share of sales in the past 12 months estimated to be investors (non-homesteaded purchasers). High investor share can mean more competition for owner-occupant buyers.</p>
            {% if investor_activity|length > 0 %}
            <table>
                <thead>
                    <tr>
                        <th>Zip</th>
                        <th>Total Sales</th>
                        <th>Estimated Investor Share</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in investor_activity %}
                    <tr>
                        <td><strong>{{ row.zip }}</strong></td>
                        <td>{{ row.total_sales }}</td>
                        <td>
                            <span class="badge {% if row.investor_share > 0.6 %}badge-warning{% elif row.investor_share < 0.3 %}badge-success{% else %}badge-neutral{% endif %}">
                                {{ "{:.0%}".format(row.investor_share) }}
                            </span>
                            <span style="display: inline-block; background: #fef3c7; height: 8px; width: {{ [100, (row.investor_share * 100)|int]|min }}px; vertical-align: middle; margin-left: 6px; border-radius: 2px;"></span>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div class="insight-box">
                <strong>Note:</strong> "Investor" is estimated from the absence of a homestead exemption at the time
                of the latest county data pull. This includes vacation homes, second homes, and short-term rentals
                alongside traditional investment properties — it's a proxy, not a definitive count.
            </div>
            {% else %}
            <div class="no-data">Investor activity data unavailable (county data required)</div>
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
            <em>Generated weekly by a serverless ETL pipeline. Data sourced from Redfin Data Center, Zillow Research, and Sarasota County Property Appraiser.</em>
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
    
    flip_df = results.get('flip_detector', pd.DataFrame())
    profitable_flips = df_to_list(flip_df[flip_df['outcome'] == 'PROFITABLE']) if not flip_df.empty else []
    loss_flips = df_to_list(flip_df[flip_df['outcome'] == 'LOSS']) if not flip_df.empty else []

    rendered = template.render(
        date=datetime.now().strftime("%B %d, %Y"),
        is_degraded=is_degraded,
        error_log=error_log,
        price_pressure=df_to_list(results.get('price_pressure', pd.DataFrame())),
        inventory=df_to_list(results.get('inventory_absorption', pd.DataFrame())),
        trend_lines=df_to_list(results.get('trend_lines', pd.DataFrame())),
        buyer_value_index=df_to_list(results.get('buyer_value_index', pd.DataFrame())),
        zip_price_trends=df_to_list(results.get('zip_price_trends', pd.DataFrame())),
        assessment_ratio=df_to_list(results.get('assessment_ratio', pd.DataFrame())),
        profitable_flips=profitable_flips,
        loss_flips=loss_flips,
        flip_summary=results.get('flip_summary', 'No flips detected'),
        investor_activity=df_to_list(results.get('investor_activity', pd.DataFrame())),
        market_snapshot=results.get('market_snapshot', None),
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
    
    return success, html_content


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
