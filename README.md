# Sarasota Market Pulse ("Market Strategist")

**A serverless ETL pipeline designed to ingest structured real estate data from Zillow and Redfin, normalize it against county property appraiser records, and compute market-level intelligence (Price Pressure, Inventory Absorption, Cash Flow Zones) for institutional-grade decision support. Deployed via GitHub Actions for automated weekly execution.**

---

## 🎯 Overview

The **Sarasota Market Pulse** automatically monitors the Sarasota, FL market and delivers weekly intelligence reports. Using **market-level strategist analytics**, providing a macro view of inventory, rent yields, and pricing power.

- **Zillow Research**: Ingests ZHVI (Home Values) and ZORI (Observed Rents) by zip code.
- **Redfin Data Center**: Tracks weeks of supply, sale-to-list ratios, and absorption rates.
- **County Property Appraiser**: Cross-references public records for flip detection and appraisal gaps.
- **Zero-Cost Infrastructure**: Runs entirely on GitHub Actions using JSON-based persistence.

---

## 📊 Market Strategist Metrics

**5 sophisticated indicator sets**:

### 1. 📉 Price Pressure Index
**Source:** Redfin  
**Signals:** Median sale price + average sale-to-list ratio. Identifies if sellers are losing control (ratio < 1.0) or if bidding wars are prevalent.

### 2. 📦 Inventory & Absorption
**Source:** Redfin  
**Signals:** Weeks of supply + New Listings vs. Homes Sold ratio. Flags "Buyer's Markets" when supply exceeds 18 weeks.

### 3. 💰 Cash Flow Zone Finder
**Source:** Zillow  
**Signals:** Rent-to-Value ratio (ZORI ÷ ZHVI) ranked by zip code. Identifies which Sarasota neighborhoods offer the highest gross yields for investors.

### 4. 🔄 Short Hold Flip Detector
**Source:** County Records  
**Signals:** Identifies properties purchased 4-12 months ago and re-sold. Tracks renovation markups across the county.

### 5. 📊 Appraisal vs. Market Gap
**Source:** Zillow + County  
**Signals:** Divergence between Zillow market values and county JUST (appraised) values. High gaps (15%+) signal a "hot" market running ahead of tax assessments.

---

## 🏗️ Architecture

### Pipeline Phases

```
┌─────────────────┐
│   INGESTION     │ • Zillow Research API (CSV)
│                 │ • Redfin Tableau Crosstabs
│                 │ • SCPA County ZIP Data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TRANSFORMATION  │ • Market-level aggregations
│                 │ • Zip-code yield analysis
│                 │ • 5 Strategist Metrics
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    DELIVERY     │ • Weekly Jinja2 HTML Report
│                 │ • SMTP via Gmail App PW
└─────────────────┘
```

### State Management (Project Memory)

The pipeline uses **JSON-based history** stored in `data/history/`. 
- **Persistence:** Snapshots are committed back to the repo weekly.
- **Intelligence:** Enables calculation of Week-over-Week (WoW) and Month-over-Month (MoM) trends.
- **Resilience:** Automatic cleanup of history older than 4 weeks.

---

## 📁 Project Structure

```
srq-pulse/
├── .github/workflows/
│   └── weekly_pulse.yml     # Weekly automation (Monday mornings)
├── data/
│   ├── zillow/              # ZHVI & ZORI raw data
│   ├── redfin/              # Market trend exports
│   ├── county/              # Parsed SCPA records
│   ├── history/             # Project memory (JSON snapshots)
│   └── errors.log           # Runtime error logs
├── src/
│   ├── ingest.py            # Multi-source ingestion (Zillow/Redfin/County)
│   ├── transform.py         # Market Strategist transformation logic
│   └── deliver.py           # HTML rendering & SMTP delivery
├── main.py                  # Master orchestrator
├── requirements.txt
└── README.md
```

---

## 🚀 Setup & Execution

### Local Setup

1. **Clone & Install:**
   ```bash
   git clone <repo-url>
   pip install -r requirements.txt
   ```

2. **Manual Redfin Ingestion (Optional):**
   If automation is bypassed, drop Redfin exports directly into `data/redfin/`. The pipeline will detect and standardize them automatically.

3. **Environment Variables:**
   ```bash
   export GMAIL_USER="your-email@gmail.com"
   export GMAIL_APP_PASSWORD="your-app-password"
   export EMAIL_TO="recipient@example.com"
   ```

4. **Run:**
   ```bash
   python main.py
   ```

---

## 🎓 Highlights

- **Advanced Data Integration:** Standardized disparate datasets (Tab-separated UTF-16, CSV, Zip-compressed) into a unified market intelligence schema.
- **Serverless Architectural Design:** Engineered a stateful ETL pipeline using GitHub Actions as an orchestrator and Git as a persistence layer.
- **Yield Analysis Engine:** Scaled rent-to-value calculations across 10+ zip codes to identify high-yield investment zones.
- **Automated Quality Assurance:** Implemented sanity checks to prevent corrupted state commits and graceful degradation for failing data sources.

---

## 📚 Data Attributions
- **Redfin**: Market data sourced from the Redfin Data Center.
- **Zillow**: Home value and rental indices via Zillow Research.
- **SCPA**: Public property records via [Sarasota County Property Appraiser](https://sc-pa.com).

---

## 📄 License
MIT License. **Not financial advice. Use at your own risk.**
