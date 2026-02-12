"""
Sarasota Market Pulse — Data Ingestion Engine

This module handles:
1. MLS listing data via homeharvest (unofficial Realtor.com scraper)
2. Sarasota County Property Appraiser data (parcels + sales)

Critical: homeharvest is fragile and breaks frequently. All calls are wrapped
in error handling with fallback to degraded mode.
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
from homeharvest import scrape_property

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for pipeline health monitoring
INGEST_FAILED = False


def log_error(message: str):
    """Log errors to data/errors.log for degraded mode notifications."""
    Path("data").mkdir(exist_ok=True)
    error_log_path = Path("data/errors.log")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(error_log_path, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
    
    logger.error(message)


def ingest_mls_listings() -> bool:
    """
    Fetch MLS listings for Sarasota, FL using homeharvest.
    
    Returns:
        bool: True if successful, False if failed
    """
    global INGEST_FAILED
    
    try:
        logger.info("Starting MLS listing ingestion via homeharvest...")
        
        # Fetch listings from the past 1 day to keep execution time low
        properties = scrape_property(
            location="Sarasota, FL",
            listing_type="for_sale",
            past_days=1
        )
        
        # Validate output - empty DataFrame is a silent failure
        if properties is None or len(properties) == 0:
            error_msg = "homeharvest returned empty DataFrame - possible API change or rate limiting"
            log_error(error_msg)
            INGEST_FAILED = True
            return False
        
        # Save to data directory
        Path("data").mkdir(exist_ok=True)
        output_path = Path("data/latest_listings.csv")
        properties.to_csv(output_path, index=False)
        
        logger.info(f"✅ Successfully ingested {len(properties)} MLS listings")
        return True
        
    except Exception as e:
        error_msg = f"MLS ingestion failed: {type(e).__name__}: {str(e)}"
        log_error(error_msg)
        INGEST_FAILED = True
        return False


def ingest_county_data() -> bool:
    """
    Download and process Sarasota County Property Appraiser data.
    
    Source: https://www.sc-pa.com/ > Download Data > SCPA_Parcels_Sales_CSV.zip
    
    Returns:
        bool: True if successful, False if failed
    """
    global INGEST_FAILED
    
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
            with zf.open("Sarasota.csv") as csv_file:
                parcels_df = pd.read_csv(csv_file, low_memory=False)
            
            # Load sales transaction history
            logger.info("Extracting ParcelSales.csv...")
            with zf.open("ParcelSales.csv") as csv_file:
                sales_df = pd.read_csv(csv_file, low_memory=False)
        
        # Filter and clean parcel data
        logger.info("Filtering parcel data to LOCCITY == 'SARASOTA'...")
        parcels_df = parcels_df[parcels_df['LOCCITY'] == 'SARASOTA'].copy()
        
        # Keep only useful columns
        parcel_columns = [
            'ACCOUNT', 'LOCN', 'LOCS', 'LOCD', 'UNIT', 'LOCCITY', 'LOCZIP',
            'LIVING', 'BEDR', 'BATH', 'YRBL', 'JUST', 'ASSD', 'SALE_AMT', 'SALE_DATE'
        ]
        # Only keep columns that exist
        parcel_columns = [col for col in parcel_columns if col in parcels_df.columns]
        parcels_df = parcels_df[parcel_columns]
        
        # Filter sales to only Warranty Deeds (real arm's-length transactions)
        logger.info("Filtering sales to DeedType == 'WD' (Warranty Deeds)...")
        sales_df = sales_df[sales_df['DeedType'] == 'WD'].copy()
        
        # Save processed data
        Path("data").mkdir(exist_ok=True)
        parcels_path = Path("data/county_parcels.csv")
        sales_path = Path("data/county_sales.csv")
        
        parcels_df.to_csv(parcels_path, index=False)
        sales_df.to_csv(sales_path, index=False)
        
        logger.info(f"✅ Saved {len(parcels_df)} parcel records to {parcels_path}")
        logger.info(f"✅ Saved {len(sales_df)} sales transactions to {sales_path}")
        
        return True
        
    except Exception as e:
        error_msg = f"County data ingestion failed: {type(e).__name__}: {str(e)}"
        log_error(error_msg)
        INGEST_FAILED = True
        return False


def run_ingestion() -> bool:
    """
    Run both MLS and county data ingestion.
    
    Returns:
        bool: True if both succeeded, False if either failed
    """
    logger.info("=" * 60)
    logger.info("STARTING DATA INGESTION")
    logger.info("=" * 60)
    
    mls_success = ingest_mls_listings()
    county_success = ingest_county_data()
    
    success = mls_success and county_success
    
    if success:
        logger.info("✅ All ingestion tasks completed successfully")
    else:
        logger.warning("⚠️  Some ingestion tasks failed - pipeline in degraded mode")
    
    return success


if __name__ == "__main__":
    success = run_ingestion()
    exit(0 if success else 1)
