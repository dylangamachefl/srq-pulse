"""
Sarasota Market Pulse — Data Ingestion Engine (V4)

This module handles:
1. Zillow Research Data (ZHVI home values + ZORI rent index) - Direct CSV downloads
2. Redfin Data Center (6 Tableau crosstabs) - Playwright automation with inbox fallback
3. Sarasota County Property Appraiser data (parcels + sales) - Direct ZIP download

V4 Changes:
- Removed homeharvest (unreliable, breaks frequently)
- Added Zillow national datasets filtered to Sarasota zips
- Added Redfin Tableau automation (dual-path: inbox fallback → Playwright)
"""

import os
import io
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
import logging

import pandas as pd
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Sarasota zip codes for filtering Zillow data
SARASOTA_ZIPS = [34230, 34231, 34232, 34233, 34234, 34235, 34236, 34237, 34238, 34239, 34240, 34242, 34243]

# Global flags for pipeline health monitoring
ZILLOW_FAILED = False
REDFIN_FAILED = False
SCPA_FAILED = False


def log_error(message: str):
    """Log errors to data/errors.log for degraded mode notifications."""
    Path("data").mkdir(exist_ok=True)
    error_log_path = Path("data/errors.log")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_path, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    
    logger.error(message)


def ingest_zillow_data() -> bool:
    """
    Download Zillow ZHVI (home values) and ZORI (rent index) from Zillow Research Data.
    
    These are direct CSV downloads - no scraping needed.
    Files are national datasets (~50MB) - filter to Sarasota zips immediately.
    
    Returns:
        bool: True if successful, False if failed
    """
    global ZILLOW_FAILED
    
    try:
        logger.info("Downloading Zillow ZHVI (Home Value Index)...")
        
        # ZHVI: County-level, Mid-Tier Homes (SFR, Condo/Co-op), Smoothed, Seasonally Adjusted
        ZHVI_URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
        
        response = requests.get(ZHVI_URL, timeout=120)
        response.raise_for_status()
        
        logger.info(f"Downloaded ZHVI ({len(response.content) / 1024 / 1024:.2f} MB)")
        
        # Load and filter to Sarasota County
        zhvi_df = pd.read_csv(io.StringIO(response.text), low_memory=False)
        zhvi_df = zhvi_df[
            (zhvi_df['StateName'] == 'FL') & 
            (zhvi_df['RegionName'] == 'Sarasota County')
        ].copy()
        
        # Save to data directory
        zillow_dir = Path("data/zillow")
        zillow_dir.mkdir(parents=True, exist_ok=True)
        zhvi_path = zillow_dir / "zillow_zhvi.csv"
        zhvi_df.to_csv(zhvi_path, index=False)
        logger.info(f"✅ Saved {len(zhvi_df)} ZHVI records to {zhvi_path}")
        
        # ZORI: All Homes Plus Multifamily Time Series, Smoothed
        logger.info("Downloading Zillow ZORI (Observed Rent Index)...")
        
        # ZORI: County-level, All Homes Plus Multifamily, Smoothed
        ZORI_URL = "https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv"
        
        response = requests.get(ZORI_URL, timeout=120)
        response.raise_for_status()
        
        logger.info(f"Downloaded ZORI ({len(response.content) / 1024 / 1024:.2f} MB)")
        
        # Load and filter to Sarasota County
        zori_df = pd.read_csv(io.StringIO(response.text), low_memory=False)
        zori_df = zori_df[
            (zori_df['StateName'] == 'FL') & 
            (zori_df['RegionName'] == 'Sarasota County')
        ].copy()
        
        # Save to data directory
        zori_path = zillow_dir / "zillow_zori.csv"
        zori_df.to_csv(zori_path, index=False)
        logger.info(f"✅ Saved {len(zori_df)} ZORI records to {zori_path}")
        
        return True
        
    except Exception as e:
        error_msg = f"Zillow data ingestion failed: {type(e).__name__}: {str(e)}"
        log_error(error_msg)
        ZILLOW_FAILED = True
        return False


def check_redfin_existing() -> dict:
    """
    Check if fresh manual CSVs exist in data/redfin/.
    
    This is the fallback for when Tableau automation breaks.
    User manually downloads 6 CSVs from Redfin and drops them here.
    
    Redfin download names (lowercase, with spaces):
    - median sale price.csv
    - homes sold.csv
    - new listings.csv
    - days to close.csv
    - months of supply.csv (or weeks of supply.csv)
    - average sale to list ratio.csv
    
    Returns:
        dict: Map of metric name -> file path, or None if incomplete/stale
    """
    REDFIN_DIR = Path("data/redfin")
    MAX_AGE = timedelta(days=7)
    
    if not REDFIN_DIR.exists():
        return None
    
    required = {
        "median_sale_price": None,
        "homes_sold": None,
        "new_listings": None,
        "days_to_close": None,
        "weeks_of_supply": None,
        "avg_sale_to_list": None
    }
    
    # Map metric keys to possible file name patterns (Redfin uses these)
    name_patterns = {
        "median_sale_price": ["median sale price", "median_sale_price"],
        "homes_sold": ["homes sold", "homes_sold"],
        "new_listings": ["new listings", "new_listings"],
        "days_to_close": ["days to close", "days_to_close"],
        "weeks_of_supply": ["weeks of supply", "months of supply", "weeks_of_supply"],  # Redfin inconsistency!
        "avg_sale_to_list": ["average sale to list", "avg sale to list", "sale to list ratio"]
    }
    
    for f in REDFIN_DIR.iterdir():
        if not f.suffix == ".csv":
            continue
        
        # Check file age
        mod_time = datetime.fromtimestamp(f.stat().st_mtime)
        if datetime.now() - mod_time > MAX_AGE:
            logger.info(f"Skipping stale file: {f.name} (> 7 days old)")
            continue
        
        # Match file name to required metrics
        fname_lower = f.name.lower().replace("_", " ")  # Normalize for matching
        
        for key, patterns in name_patterns.items():
            if required[key] is not None:
                continue  # Already found
            
            # Check if any pattern matches
            if any(pattern in fname_lower for pattern in patterns):
                required[key] = str(f)
                logger.info(f"  Matched {f.name} -> {key}")
                break
    
    # Check if we have all 6 files
    if all(v is not None for v in required.values()):
        logger.info(f"✅ Found all 6 Redfin CSVs in data/redfin/ (< 7 days old)")
        return required
    else:
        missing = [k for k, v in required.items() if v is None]
        logger.info(f"Redfin data directory incomplete - missing: {missing}")
        return None


def ingest_redfin_via_playwright() -> bool:
    """
    Download Redfin Tableau crosstabs via Playwright automation.
    
    Uses Tableau Public URLs for 6 metrics (Median Sale Price, Homes Sold,
    New Listings, Days to Close, Weeks of Supply, Avg Sale to List).
    
    Returns:
        bool: True if successful, False if failed
    """
    try:
        from redfin_scraper import download_all_tabs
        return download_all_tabs()
    except Exception as e:
        log_error(f"Redfin Playwright failed: {e}")
        return False


def ingest_redfin_data() -> bool:
    """
    Dual-path Redfin ingestion: existing data check → Playwright automation.
    
    Returns:
        bool: True if successful, False if failed
    """
    global REDFIN_FAILED
    
    logger.info("Starting Redfin data ingestion (dual-path)...")
    
    # Path 1: Check for manual CSVs already in data/redfin/
    existing_files = check_redfin_existing()
    if existing_files is not None:
        logger.info("Using existing CSVs in data/redfin/ - skipping Playwright")
        
        # Ensure files are standardized (UTF-8)
        # Tableau exports often come as UTF-16/Tab-separated
        for metric_name, file_path in existing_files.items():
            try:
                # Test read with common Tableau format
                path_obj = Path(file_path)
                # If it's already a standardized file, we don't want to break it
                # But we need to ensure it's in a format transform.py can read
                try:
                    df = pd.read_csv(path_obj, encoding='utf-16', sep='\t')
                    # If this succeeds, it WAS a Tableau export, so standardize it
                    df.to_csv(path_obj, index=False, encoding='utf-8')
                    logger.info(f"  ✅ Standardized {metric_name}.csv from Tableau format")
                except:
                    # If it fails, maybe it's already a standard CSV
                    # Just verify it can be read
                    pd.read_csv(path_obj)
                    logger.info(f"  ✅ Verified {metric_name}.csv is standard format")
            except Exception as e:
                logger.warning(f"  ⚠️ Could not verify/standardize {metric_name}.csv: {e}")
        
        return True
    
    # Path 2: Run Playwright automation
    logger.info("data/redfin/ empty or stale - attempting Playwright automation...")
    playwright_success = ingest_redfin_via_playwright()
    
    if not playwright_success:
        error_msg = "Redfin ingestion failed - both existing data and Playwright unavailable"
        log_error(error_msg)
        REDFIN_FAILED = True
        return False
    
    return True


def ingest_county_data() -> bool:
    """
    Download and process Sarasota County Property Appraiser data.
    
    Source: https://www.sc-pa.com/ > Download Data > SCPA_Parcels_Sales_CSV.zip
    
    UNCHANGED from V3 - same download, unzip, filter logic.
    
    Returns:
        bool: True if successful, False if failed
    """
    global SCPA_FAILED
    
    # Direct download URL for SCPA_Parcels_Sales_CSV.zip
    # Verified on 2026-02-12 from sc-pa.com Download Data page
    SCPA_ZIP_URL = "https://www.sc-pa.com/downloads/SCPA_Parcels_Sales_CSV.zip"
    
    try:
        logger.info("Downloading Sarasota County data from sc-pa.com...")
        
        # Download the ZIP file
        response = requests.get(SCPA_ZIP_URL, timeout=120)
        response.raise_for_status()
        
        logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.2f} MB ZIP file")
        
        # Extract ZIP in memory
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Load parcel records (full property details)
            logger.info("Extracting Sarasota.csv...")
            with zf.open("Parcel_Sales_CSV/Sarasota.csv") as csv_file:
                parcels_df = pd.read_csv(csv_file, low_memory=False, encoding='latin-1', encoding_errors='replace')
            
            # Load sales transaction history
            logger.info("Extracting ParcelSales.csv...")
            with zf.open("Parcel_Sales_CSV/ParcelSales.csv") as csv_file:
                sales_df = pd.read_csv(csv_file, low_memory=False, encoding='latin-1', encoding_errors='replace')
        
        # Filter and clean parcel data
        logger.info("Filtering parcel data to LOCCITY == 'SARASOTA'...")
        parcels_df['LOCCITY'] = parcels_df['LOCCITY'].astype(str).str.strip().str.upper()
        parcels_df = parcels_df[parcels_df['LOCCITY'] == 'SARASOTA'].copy()
        
        # Keep only useful columns
        parcel_columns = [
            'ACCOUNT', 'LOCN', 'LOCS', 'LOCD', 'UNIT', 'LOCCITY', 'LOCZIP',
            'LIVING', 'BEDR', 'BATH', 'YRBL', 'JUST', 'ASSD', 'SALE_AMT', 'SALE_DATE',
            'HOMESTEAD'
        ]
        # Only keep columns that exist
        parcel_columns = [col for col in parcel_columns if col in parcels_df.columns]
        parcels_df = parcels_df[parcel_columns]
        
        # Filter sales to only Warranty Deeds (real arm's-length transactions)
        logger.info("Filtering sales to DeedType == 'WD' (Warranty Deeds)...")
        sales_df = sales_df[sales_df['DeedType'] == 'WD'].copy()
        
        # Save processed data
        county_dir = Path("data/county")
        county_dir.mkdir(parents=True, exist_ok=True)
        parcels_path = county_dir / "county_parcels.csv"
        sales_path = county_dir / "county_sales.csv"
        
        parcels_df.to_csv(parcels_path, index=False)
        sales_df.to_csv(sales_path, index=False)
        
        logger.info(f"✅ Saved {len(parcels_df)} parcel records to {parcels_path}")
        logger.info(f"✅ Saved {len(sales_df)} sales transactions to {sales_path}")
        
        return True
        
    except Exception as e:
        error_msg = f"County data ingestion failed: {type(e).__name__}: {str(e)}"
        log_error(error_msg)
        SCPA_FAILED = True
        return False


def run_ingestion() -> bool:
    """
    Run all data ingestion tasks (Zillow, Redfin, County).
    
    Returns:
        bool: True if at least one source succeeded (partial success OK)
    """
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION (V4)")
    logger.info("=" * 60)
    
    zillow_success = ingest_zillow_data()
    redfin_success = ingest_redfin_data()
    county_success = ingest_county_data()
    
    success_count = sum([zillow_success, redfin_success, county_success])
    
    if success_count == 3:
        logger.info("✅ All ingestion tasks completed successfully")
        return True
    elif success_count > 0:
        logger.warning(f"⚠️  Partial success: {success_count}/3 sources ingested - pipeline in degraded mode")
        return True
    else:
        logger.error("❌ All ingestion tasks failed - cannot proceed")
        return False


if __name__ == "__main__":
    success = run_ingestion()
    exit(0 if success else 1)
