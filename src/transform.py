"""
Sarasota Market Pulse — Transformation Logic ("Alpha" Processor)

This module calculates 5 key investment signals:
1. Price Cut Velocity - Panic selling detection
2. Stale Hunter - Overpriced, stubborn listings
3. 0.8% Cash Flow Screen - Rental yield proxy
4. Short Hold Flip Detector - Recent purchase + re-list
5. Appraisal Gap - County value vs. MLS list price

Uses address normalization for fuzzy matching between MLS and county data.
"""

import re
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def normalize_address(addr: str) -> str:
    """
    Normalize address for fuzzy matching across data sources.
    
    Critical for matching MLS addresses against SCPA county records which use
    structured fields (LOCN + LOCS + LOCD). Without normalization, match rate
    drops from ~70% to ~20-30%.
    
    Args:
        addr: Raw address string
        
    Returns:
        Normalized address (uppercase, no punctuation, standardized abbreviations)
    """
    if pd.isna(addr) or not isinstance(addr, str):
        return ""
    
    addr = addr.upper().strip()
    addr = re.sub(r'[^A-Z0-9\s]', '', addr)  # Strip punctuation
    
    # Standardize street type abbreviations
    replacements = {
        ' STREET': ' ST', ' AVENUE': ' AVE', ' BOULEVARD': ' BLVD',
        ' DRIVE': ' DR', ' LANE': ' LN', ' COURT': ' CT',
        ' PLACE': ' PL', ' ROAD': ' RD', ' CIRCLE': ' CIR',
        ' NORTH': ' N', ' SOUTH': ' S', ' EAST': ' E', ' WEST': ' W',
        ' HIGHWAY': ' HWY', ' PARKWAY': ' PKWY', ' TERRACE': ' TER',
    }
    
    for full, abbr in replacements.items():
        addr = addr.replace(full, abbr)
    
    addr = re.sub(r'\s+', ' ', addr)  # Collapse whitespace
    return addr


def estimate_rent(sqft: float) -> float:
    """
    Estimate monthly rent based on Sarasota market rates.
    
    Uses tiered $/sqft rates based on property size. Smaller units
    command higher per-sqft rents due to demand and lower total cost.
    
    Args:
        sqft: Living square footage
        
    Returns:
        Estimated monthly rent in dollars
    """
    if pd.isna(sqft) or sqft <= 0:
        return 0
    
    if sqft < 1000:
        rate = 2.00  # Small units command premium
    elif sqft <= 1800:
        rate = 1.65  # Core Sarasota SFR range
    else:
        rate = 1.35  # Larger homes have diminishing returns
    
    return sqft * rate


def metric_price_cut_velocity(listings_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 1: Price Cut Velocity
    
    Flags properties with significant price drops in early listing period.
    Indicates panic selling or motivated seller.
    
    Criteria:
    - Price drop > $10,000
    - Days on market < 14
    
    Returns:
        DataFrame of flagged properties
    """
    logger.info("Calculating Metric 1: Price Cut Velocity...")
    
    if history_df is None or len(history_df) == 0:
        logger.info("No history data available - skipping price cut analysis")
        return pd.DataFrame()
    
    # Merge on property_url or address (whichever is available as unique ID)
    # homeharvest typically includes 'property_url' as a stable identifier
    id_col = 'property_url' if 'property_url' in listings_df.columns else 'address'
    
    if id_col not in listings_df.columns or id_col not in history_df.columns:
        logger.warning(f"ID column '{id_col}' not found - skipping price cut analysis")
        return pd.DataFrame()
    
    merged = listings_df.merge(
        history_df[[id_col, 'list_price']],
        on=id_col,
        how='inner',
        suffixes=('_today', '_yesterday')
    )
    
    merged['price_delta'] = merged['list_price_today'] - merged['list_price_yesterday']
    
    # Get days_on_market column (may be 'days_on_mls' or 'days_on_market')
    dom_col = 'days_on_mls' if 'days_on_mls' in merged.columns else 'days_on_market'
    
    if dom_col not in merged.columns:
        logger.warning("Days on market column not found - skipping velocity check")
        return pd.DataFrame()
    
    flagged = merged[
        (merged['price_delta'] < -10000) &
        (merged[dom_col] < 14)
    ].copy()
    
    logger.info(f"Found {len(flagged)} panic sellers (price cut velocity)")
    return flagged


def metric_stale_hunter(listings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 2: Stale Hunter
    
    Identifies overpriced listings that have sat for 90+ days with no price changes.
    These sellers are stubborn but may be getting desperate.
    
    Criteria:
    - Days on market > 90
    - No price changes (if data available)
    
    Returns:
        DataFrame of flagged properties
    """
    logger.info("Calculating Metric 2: Stale Hunter...")
    
    dom_col = 'days_on_mls' if 'days_on_mls' in listings_df.columns else 'days_on_market'
    
    if dom_col not in listings_df.columns:
        logger.warning("Days on market column not found - skipping stale analysis")
        return pd.DataFrame()
    
    flagged = listings_df[listings_df[dom_col] > 90].copy()
    
    logger.info(f"Found {len(flagged)} stale listings (90+ days)")
    return flagged


def metric_cash_flow_screen(listings_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 3: 0.8% Cash Flow Screen
    
    Identifies properties meeting the 0.8% rule: monthly rent / list price >= 0.008
    Note: This is NOT the classic "1% rule" - that's unrealistic in Sarasota.
    
    Returns:
        DataFrame of properties passing cash flow screen
    """
    logger.info("Calculating Metric 3: 0.8% Cash Flow Screen...")
    
    # Estimate rent based on square footage
    sqft_col = 'sqft' if 'sqft' in listings_df.columns else 'lot_sqft'
    
    if sqft_col not in listings_df.columns:
        logger.warning("Square footage column not found - skipping cash flow analysis")
        return pd.DataFrame()
    
    df = listings_df.copy()
    df['estimated_rent'] = df[sqft_col].apply(estimate_rent)
    df['cash_flow_ratio'] = df['estimated_rent'] / df['list_price']
    
    flagged = df[df['cash_flow_ratio'] >= 0.008].copy()
    
    logger.info(f"Found {len(flagged)} properties passing 0.8% cash flow screen")
    return flagged


def metric_flip_detector(listings_df: pd.DataFrame, sales_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 4: Short Hold Flip Detector
    
    Matches MLS listings against recent county sales (4-12 months ago).
    Indicates probable flips - check renovation quality vs. markup.
    
    Returns:
        DataFrame of flagged properties with recent sales
    """
    logger.info("Calculating Metric 4: Short Hold Flip Detector...")
    
    if sales_df is None or len(sales_df) == 0:
        logger.warning("No sales data available - skipping flip detection")
        return pd.DataFrame()
    
    # Build normalized addresses for county parcels
    parcels_df = parcels_df.copy()
    parcels_df['county_address'] = (
        parcels_df['LOCN'].astype(str) + ' ' +
        parcels_df['LOCS'].astype(str) + ' ' +
        parcels_df['LOCD'].astype(str)
    )
    parcels_df['county_address_norm'] = parcels_df['county_address'].apply(normalize_address)
    
    # Join sales with parcels to get addresses
    sales_with_addr = sales_df.merge(
        parcels_df[['ACCOUNT', 'county_address_norm']],
        left_on='Account',
        right_on='ACCOUNT',
        how='inner'
    )
    
    # Normalize MLS addresses
    addr_col = 'address' if 'address' in listings_df.columns else 'street'
    if addr_col not in listings_df.columns:
        logger.warning("Address column not found in listings - skipping flip detection")
        return pd.DataFrame()
    
    listings_df = listings_df.copy()
    listings_df['mls_address_norm'] = listings_df[addr_col].apply(normalize_address)
    
    # Parse sale dates
    sales_with_addr['SaleDate'] = pd.to_datetime(sales_with_addr['SaleDate'], errors='coerce')
    
    # Filter to sales 4-12 months ago
    today = datetime.now()
    cutoff_recent = today - timedelta(days=120)  # 4 months
    cutoff_old = today - timedelta(days=365)  # 12 months
    
    recent_sales = sales_with_addr[
        (sales_with_addr['SaleDate'] >= cutoff_old) &
        (sales_with_addr['SaleDate'] <= cutoff_recent)
    ]
    
    # Match MLS listings to recent sales
    flagged = listings_df.merge(
        recent_sales[['county_address_norm', 'SaleDate', 'SalePrice']],
        left_on='mls_address_norm',
        right_on='county_address_norm',
        how='inner'
    )
    
    logger.info(f"Found {len(flagged)} probable flips (4-12 month holds)")
    return flagged


def metric_appraisal_gap(listings_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 5: Appraisal Gap
    
    Compares MLS list price against county appraised value (JUST field).
    
    Flags:
    - Overpriced: Listed 20%+ above appraisal (likely to need price cuts)
    - Underpriced: Listed 5%+ below appraisal (panic seller or estate sale)
    
    Returns:
        DataFrame with gap calculations and flags
    """
    logger.info("Calculating Metric 5: Appraisal Gap...")
    
    # Build normalized county addresses
    parcels_df = parcels_df.copy()
    parcels_df['county_address'] = (
        parcels_df['LOCN'].astype(str) + ' ' +
        parcels_df['LOCS'].astype(str) + ' ' +
        parcels_df['LOCD'].astype(str)
    )
    parcels_df['county_address_norm'] = parcels_df['county_address'].apply(normalize_address)
    
    # Normalize MLS addresses
    addr_col = 'address' if 'address' in listings_df.columns else 'street'
    if addr_col not in listings_df.columns:
        logger.warning("Address column not found - skipping appraisal gap analysis")
        return pd.DataFrame()
    
    listings_df = listings_df.copy()
    listings_df['mls_address_norm'] = listings_df[addr_col].apply(normalize_address)
    
    # Join MLS with county appraisal values
    merged = listings_df.merge(
        parcels_df[['county_address_norm', 'JUST']],
        left_on='mls_address_norm',
        right_on='county_address_norm',
        how='inner'
    )
    
    # Calculate gap
    merged['appraisal_gap'] = (merged['list_price'] - merged['JUST']) / merged['JUST']
    
    # Flag overpriced and underpriced
    merged['gap_flag'] = ''
    merged.loc[merged['appraisal_gap'] > 0.20, 'gap_flag'] = 'OVERPRICED'
    merged.loc[merged['appraisal_gap'] < -0.05, 'gap_flag'] = 'UNDERPRICED'
    
    flagged = merged[merged['gap_flag'] != ''].copy()
    
    logger.info(f"Found {len(flagged)} properties with significant appraisal gaps")
    return flagged


def run_transformation() -> dict:
    """
    Execute all transformation metrics.
    
    Returns:
        dict: Results keyed by metric name
    """
    logger.info("=" * 60)
    logger.info("STARTING TRANSFORMATION (ALPHA PROCESSOR)")
    logger.info("=" * 60)
    
    results = {}
    
    # Load input data
    try:
        listings_df = pd.read_csv("data/latest_listings.csv")
        logger.info(f"Loaded {len(listings_df)} current listings")
    except FileNotFoundError:
        logger.error("latest_listings.csv not found - ingestion may have failed")
        return results
    
    # Load history (may not exist on first run)
    try:
        history_df = pd.read_csv("data/history.csv")
        logger.info(f"Loaded {len(history_df)} historical listings")
    except FileNotFoundError:
        logger.info("No history file found - this may be the first run")
        history_df = pd.DataFrame()
    
    # Load county data
    try:
        parcels_df = pd.read_csv("data/county_parcels.csv")
        sales_df = pd.read_csv("data/county_sales.csv")
        logger.info(f"Loaded {len(parcels_df)} parcels and {len(sales_df)} sales")
    except FileNotFoundError:
        logger.warning("County data not found - some metrics will be skipped")
        parcels_df = pd.DataFrame()
        sales_df = pd.DataFrame()
    
    # Run metrics
    results['price_cut_velocity'] = metric_price_cut_velocity(listings_df, history_df)
    results['stale_hunter'] = metric_stale_hunter(listings_df)
    results['cash_flow_screen'] = metric_cash_flow_screen(listings_df)
    results['flip_detector'] = metric_flip_detector(listings_df, sales_df, parcels_df)
    results['appraisal_gap'] = metric_appraisal_gap(listings_df, parcels_df)
    
    logger.info("✅ Transformation complete")
    return results


if __name__ == "__main__":
    results = run_transformation()
    
    # Print summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION RESULTS SUMMARY")
    print("=" * 60)
    for metric_name, df in results.items():
        print(f"{metric_name}: {len(df)} properties flagged")
