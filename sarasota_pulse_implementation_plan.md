# Sarasota Market Pulse — Implementation Plan (v3)

---

### Prompt for Your AI Developer

**Role:** You are a Senior Data Engineer specializing in Python, ETL pipelines, and Real Estate scraping.
**Objective:** Build a headless, automated "Market Pulse" reporting system for Sarasota, FL.
**Constraint:** The system must run entirely on GitHub Actions (free tier) and use only publicly available data.
**Output:** Python scripts, GitHub Action YAML configuration, and a requirements.txt file.

---

## Phase 1: The "Ingestion" Engine (Data Extraction)

**Context:** We need to fetch two types of data: Active Market data (listings) and County Record data (distress signals).
**Tools:** `homeharvest` (for MLS data), `pandas`, `requests`.

### Task 1: Build `ingest_listings.py`

* **Library:** Use `homeharvest`.
* **Parameters:**
  * `location`: "Sarasota, FL"
  * `listing_type`: "for_sale"
  * `past_days`: 1 (We only want the daily delta to keep execution time low).
* **Output:** Save to `data/latest_listings.csv`.

> **⚠️ CRITICAL — `homeharvest` Resilience:**
> `homeharvest` is an unofficial MLS scraper that breaks frequently when Realtor.com changes their site structure. You **must** implement the following:
>
> 1. **Pin the version** in `requirements.txt` (e.g., `homeharvest==0.3.30`). Never use unpinned.
> 2. **Wrap all calls in try/except.** On failure, the script should NOT crash the pipeline. Instead, it should:
>    * Log the error to `data/errors.log`.
>    * Set a flag (`INGEST_FAILED = True`) that `main.py` reads.
>    * Trigger a "Pipeline Degraded" notification email (see Phase 3).
> 3. **Validate output:** After a successful fetch, check that the DataFrame has > 0 rows. An empty DF is a silent failure.

### Task 2: Build `ingest_county_data.py`

* **Source:** Sarasota County Property Appraiser — Download Data page (`https://www.sc-pa.com/`).
* **Target File:** `SCPA_Parcels_Sales_CSV.zip` — a publicly available ZIP containing two CSV files we need:
  * `ParcelSales.csv` — Sales transaction history (Account, SaleDate, SalePrice, DeedType, QualCode, Grantor).
  * `Sarasota.csv` — Full parcel records (address, sqft, year built, bedrooms, baths, appraised values).

* **Method:**
  1. Download `SCPA_Parcels_Sales_CSV.zip` via `requests`. The direct download link should be captured from the sc-pa.com Download Data page (inspect the href on the "SCPA_Parcels_Sales_CSV.zip" link). Document the verified URL in a comment at the top of the script.
  2. Unzip in memory or to `data/` using Python's `zipfile` module.
  3. Load both CSVs into Pandas DataFrames.

* **Filtering & Cleanup:**
  * `Sarasota.csv`: Filter to `LOCCITY == "SARASOTA"`. Keep only useful columns: `ACCOUNT`, `LOCN`, `LOCS`, `LOCD`, `UNIT`, `LOCCITY`, `LOCZIP`, `LIVING`, `BEDR`, `BATH`, `YRBL`, `JUST` (appraised market value), `ASSD`, `SALE_AMT`, `SALE_DATE`.
  * `ParcelSales.csv`: Filter to only `DeedType == "WD"` (Warranty Deed = real arm's-length transactions). Exclude Quit Claim deeds (`QC`) and other non-market transfers which skew the data.
  * Join on `ACCOUNT` (Sarasota.csv) = `Account` (ParcelSales.csv).

* **Output:** Save to `data/county_parcels.csv` and `data/county_sales.csv`.

> **⚠️ NOTE — File Size:**
> The full `SCPA_Parcels_Sales_CSV.zip` covers all of Sarasota County. It is a large-ish file but well within GitHub Actions' free tier limits. The filtering step reduces it to a manageable size. If execution time becomes an issue, cache the downloaded ZIP and only re-download weekly (check `Last-Modified` header).

---

## Phase 2: The "Alpha" Processor (Transformation Logic)

**Context:** We don't just want a list of houses. We want *signals*. You need to write logic to calculate the following metrics using Pandas.

### Task: Build `transform.py`

* **Input:** Load `data/latest_listings.csv`, `data/history.csv` (previous day's state), `data/county_parcels.csv`, and `data/county_sales.csv`.

#### Metric 1: "Price Cut Velocity"

* Compare `list_price` today vs. `list_price` in `history.csv`.
* **Flag:** If `delta < -10,000` AND `days_on_market < 14`.
* *Meaning:* Seller is panic-selling immediately.

#### Metric 2: "The Stale Hunter"

* Filter: `days_on_market > 90` AND `price_change_count == 0`.
* *Meaning:* Stubborn seller, ripe for a lowball offer.

#### Metric 3: "The 0.8% Rule" (Cash Flow)

* Logic: `(estimated_rent / list_price) >= 0.008`.
* **Proxy:** If `estimated_rent` is missing, use a **tiered rent/sqft estimate** based on Sarasota averages:

| Sqft Range  | $/Sqft | Rationale                                 |
|-------------|--------|-------------------------------------------|
| < 1,000     | $2.00  | Small units command higher per-sqft rents  |
| 1,000–1,800 | $1.65  | Core Sarasota SFR rental range             |
| > 1,800     | $1.35  | Larger homes have diminishing rent returns |

> **Note:** This threshold is intentionally set at 0.8%, not the classic "1% Rule." True 1% deals are extremely rare in Sarasota's current market. Name it accurately in the report as the **"0.8% Cash Flow Screen"** to avoid confusion.

#### Metric 4: "Short Hold Flip Detector"

* **Data source:** `data/county_sales.csv` (from `ParcelSales.csv`).
* **Logic:** Match new MLS listings (by normalized address) against county sales records. Flag any property where the most recent `SaleDate` was **4–12 months ago**.
* **Filter:** Only count `DeedType == "WD"` (Warranty Deed) sales — ignore Quit Claims and other non-market transfers.
* *Meaning:* Someone bought this recently and is already re-listing. Likely a flip — inspect renovation quality and check if the markup is justified.

#### Metric 5: "Appraisal Gap" (County Value vs. List Price)

* **Data source:** `data/county_parcels.csv` (from `Sarasota.csv`, specifically the `JUST` column = county appraised market value).
* **Logic:** Match MLS listings to county records by normalized address. Calculate: `gap = (list_price - JUST) / JUST`.
* **Flag — Overpriced:** If `gap > 0.20` (listed 20%+ above appraised value). *Meaning:* Seller is reaching. Likely to sit or need price cuts.
* **Flag — Underpriced:** If `gap < -0.05` (listed 5%+ below appraised value). *Meaning:* Potential panic seller or estate sale — reinforces Metric 1 signal.

> **⚠️ CRITICAL — Address Normalization:**
> MLS addresses and county parcel addresses will **not** match as raw strings. The county data uses structured fields (`LOCN` for street number, `LOCS` for street name, `LOCD` for suffix). Build the county address as: `f"{LOCN} {LOCS} {LOCD}"` then apply the following normalization to **both** datasets before joining:
>
> ```python
> import re
>
> def normalize_address(addr: str) -> str:
>     """Normalize address for fuzzy matching across data sources."""
>     addr = addr.upper().strip()
>     addr = re.sub(r'[^A-Z0-9\s]', '', addr)  # Strip punctuation
>     replacements = {
>         ' STREET': ' ST', ' AVENUE': ' AVE', ' BOULEVARD': ' BLVD',
>         ' DRIVE': ' DR', ' LANE': ' LN', ' COURT': ' CT',
>         ' PLACE': ' PL', ' ROAD': ' RD', ' CIRCLE': ' CIR',
>         ' NORTH': ' N', ' SOUTH': ' S', ' EAST': ' E', ' WEST': ' W',
>     }
>     for full, abbr in replacements.items():
>         addr = addr.replace(full, abbr)
>     addr = re.sub(r'\s+', ' ', addr)  # Collapse whitespace
>     return addr
> ```
>
> Apply this to both the MLS listing address and the constructed county address before joining. Without this step, you will miss approximately 30–50% of valid matches.

---

## Phase 3: The "Delivery" & Orchestration (CI/CD)

**Context:** This needs to run automatically at 7:00 AM EST.

### Task 1: Build `report.py`

* **Tools:** `jinja2` (for HTML templating), `resend` (for email delivery).
* **Logic:**
  * Take the Pandas DataFrames from Phase 2.
  * Render them into a clean HTML email.
  * Use highly readable headers: "🔥 Panic Sellers", "🏚️ Stale Listings", "💰 Cash Flow Picks", "🔄 Probable Flips", "📊 Appraisal Gaps".
  * **Include a "Pipeline Health" footer** showing: records ingested, any warnings, and whether the county data source was available.

> **⚠️ IMPORTANT — Email Delivery:**
> Do **not** use `smtplib` with Gmail. Gmail aggressively blocks automated SMTP connections even with App Passwords, and failures are silent.
>
> **Use Resend** (3,000 emails/month free tier, simple Python SDK):
>
> Store the API key as a GitHub Actions secret (`EMAIL_API_KEY`).
>
> ```python
> import resend
> import os
>
> resend.api_key = os.environ["EMAIL_API_KEY"]
> resend.Emails.send({
>     "from": "pulse@yourdomain.com",
>     "to": os.environ["EMAIL_TO"],
>     "subject": "Sarasota Market Pulse — " + today,
>     "html": rendered_html
> })
> ```

* **Degraded Mode Email:** If `main.py` detects that ingestion failed (see Phase 1 flag), send a short "⚠️ Pipeline Degraded" email instead, with the error from `data/errors.log`. This way you **always** know if something broke rather than just not receiving a report.

### Task 2: Build `.github/workflows/daily_pulse.yml`

* **Schedule:** Cron `'0 11 * * *'` (11:00 UTC = 7:00 AM EST).
* **Permissions:** Write permissions needed to save `data/history.csv` back to the repo.
* **Steps:**
  1. Checkout code.
  2. Install Python 3.10.
  3. Pip install requirements.
  4. Run `main.py` (which triggers Ingest → Transform → Report).
  5. **Commit State** (with safeguards — see below).

> **⚠️ CRITICAL — State Management Safeguards:**
> Committing `history.csv` back to the repo is the "database" for this system. A corrupted commit breaks all future runs. Implement the following:
>
> 1. **Rolling history:** Instead of overwriting a single `history.csv`, save dated copies: `history_YYYYMMDD.csv`. Keep the last 3 days. This allows manual recovery if a bad file is committed.
>
> 2. **Sanity check before commit:** In `main.py`, before the pipeline exits, validate the new history file:
>    ```python
>    import os
>    new_rows = len(pd.read_csv("data/history_today.csv"))
>    if os.path.exists("data/history_yesterday.csv"):
>        old_rows = len(pd.read_csv("data/history_yesterday.csv"))
>        if new_rows < old_rows * 0.5:
>            raise ValueError(
>                f"History sanity check failed: {new_rows} rows today vs {old_rows} yesterday. "
>                "Aborting commit to prevent data corruption."
>            )
>    ```
>
> 3. **Git commit step in YAML:**
>    ```yaml
>    - name: Commit updated history
>      run: |
>        git config user.name "market-pulse-bot"
>        git config user.email "bot@noreply.com"
>        git add data/history_*.csv data/errors.log
>        git diff --cached --quiet || git commit -m "Daily state update $(date -u +%Y-%m-%d)"
>        git push
>    ```
>    The `git diff --cached --quiet ||` ensures we only commit if there are actual changes — prevents empty commits on failed runs.

---

## Recommended Folder Structure

```text
sarasota-pulse/
├── .github/
│   └── workflows/
│       └── daily_pulse.yml       # The automation config
├── data/
│   ├── history_YYYYMMDD.csv      # Rolling 3-day state files
│   ├── county_parcels.csv        # Filtered Sarasota.csv from SCPA
│   ├── county_sales.csv          # Filtered ParcelSales.csv from SCPA
│   └── errors.log                # Pipeline error log
├── src/
│   ├── ingest.py                 # HomeHarvest & SCPA ZIP download logic
│   ├── transform.py              # The "Alpha" math + address normalization
│   └── deliver.py                # Email via Resend API
├── main.py                       # Master conductor + error handling + sanity checks
├── .gitignore                    # Exclude: *.pyc, __pycache__, .env, temp files
├── requirements.txt              # All deps pinned to specific versions
└── README.md                     # ETL architecture documentation
```

### `.gitignore` (include this)

```text
__pycache__/
*.pyc
.env
*.tmp
data/latest_listings.csv
data/*.zip
```

---

## `requirements.txt` (pin all versions)

```text
homeharvest==0.3.30
pandas>=2.0,<3.0
requests>=2.31,<3.0
jinja2>=3.1,<4.0
resend>=0.7,<1.0
```

---

## README.md Guidance

When you commit this code, include a `README.md` that explains the **ETL Architecture**. Do not just say "Real Estate Bot."

**Write this in the README:**

> "A serverless ELT pipeline designed to ingest unstructured real estate data, normalize it against county property appraiser records, and compute derived financial metrics (Cash Flow Screen, Price Velocity, Appraisal Gap) for decision support. Deployed via GitHub Actions for zero-cost automated daily execution."

*Note: Avoid claiming "99.9% uptime" — the system depends on an unofficial scraper and a county website outside your control. The language above is accurate and still reads well on a resume.*

---

## Summary of Changes

| Area | v1 (Original) | v2 | v3 (Current) |
|------|---------------|-----|---------------|
| Location | Bradenton, FL | Sarasota, FL | — |
| `homeharvest` resilience | No error handling | try/except + degraded mode email + version pinning | — |
| County data source | Incorrect URL assumed to be API | ArcGIS primary, manual fallback | **Verified: `SCPA_Parcels_Sales_CSV.zip`** from sc-pa.com Download Data page |
| County data files | Single unspecified CSV | Single unspecified CSV | **Two files: `ParcelSales.csv` (sales history) + `Sarasota.csv` (parcel records)** |
| Metric 4 | Flipper Signal via permits | Flipper Signal via permits | **Short Hold Flip Detector** via sales history (4–12 month hold period) |
| Metric 5 | — | — | **NEW: Appraisal Gap** (county `JUST` value vs. MLS list price) |
| Permits dependency | Required separate data source | Required separate data source | **Removed entirely** — no permits needed |
| Address normalization | Raw string comparison | Generic normalization | **Updated for SCPA format** (`LOCN` + `LOCS` + `LOCD` concatenation) |
| Rent proxy | Flat $1.60/sqft | Tiered by sqft range | Tiered at Sarasota rates ($1.35–$2.00) |
| Cash flow metric name | "1% Rule" at 0.8% | "0.8% Cash Flow Screen" | — |
| Email delivery | `smtplib` + Gmail | Resend API | — |
| State management | Single `history.csv` overwrite | Rolling 3-day history + sanity check | — |
| Error visibility | Silent failures | Error log + degraded mode email | — |
| Repo hygiene | No `.gitignore` | `.gitignore` included | Added `data/*.zip` to gitignore |
