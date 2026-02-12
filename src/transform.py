"""
Sarasota Market Pulse — Transformation Logic (V4 "Market Strategist")

V4 Changes:
- Replaced listing-level metrics with market-level analytics
- Metric 1: Price Pressure Index (Redfin median price + sale-to-list ratio)
- Metric 2: Inventory & Absorption (Redfin weeks of supply + new/sold ratio)
- Metric 3: Cash Flow Zone Finder (Zillow ZORI ÷ ZHVI by zip)
- Metric 4: Short Hold Flip Detector (SCPA sales, unchanged)
- Metric 5: Appraisal Gap (Zillow ZHVI vs county JUST by zip)

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


def metric_price_pressure_index(redfin_dir: Path) -> pd.DataFrame:
    """
    Metric 1: Price Pressure Index
    
    Tracks median sale price + sale-to-list ratio trends over 4 weeks.
    Compares current year vs prior year lines.
    
    Signal:
    - Median price trending down + sale-to-list < 1.0 = buyers have leverage
    - Sale-to-list > 1.0 = bidding wars, sellers in control
    
    Returns:
        DataFrame with columns: week, median_price, price_delta, sale_to_list, signal
    """
    logger.info("Calculating Metric 1: Price Pressure Index...")
    
    try:
        median_price_df = pd.read_csv(redfin_dir / "median_sale_price.csv")
        sale_to_list_df = pd.read_csv(redfin_dir / "avg_sale_to_list.csv")
        
        # TODO: Parse Tableau crosstab format and calculate WoW trends
        # Expected columns from Tableau: Date, Median Sale Price (current year), Median Sale Price (prior year)
        
        # Placeholder until Redfin data structure is known
        result = pd.DataFrame(columns=['week', 'median_price', 'price_delta', 'sale_to_list', 'signal'])
        logger.info(f"Calculated {len(result)} weeks of price pressure data")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Redfin data not found - skipping price pressure analysis: {e}")
        return pd.DataFrame()


def metric_inventory_absorption(redfin_dir: Path) -> pd.DataFrame:
    """
    Metric 2: Inventory & Absorption
    
    Tracks weeks of supply trend + new listings vs homes sold ratio.
    
    Signal:
    - Weeks of supply rising = market shifting toward buyers
    - Above ~18 weeks = clear buyer's market
    - New listings > homes sold for 3+ weeks = supply piling up
    
    Returns:
        DataFrame with columns: week, weeks_of_supply, new_listings, homes_sold, market_state
    """
    logger.info("Calculating Metric 2: Inventory & Absorption...")
    
    try:
        supply_df = pd.read_csv(redfin_dir / "weeks_of_supply.csv")
        listings_df = pd.read_csv(redfin_dir / "new_listings.csv")
        sold_df = pd.read_csv(redfin_dir / "homes_sold.csv")
        
        # TODO: Parse Tableau crosstab format and calculate absorption ratio
        
        # Placeholder until Redfin data structure is known
        result = pd.DataFrame(columns=['week', 'weeks_of_supply', 'new_listings', 'homes_sold', 'market_state'])
        logger.info(f"Calculated {len(result)} weeks of inventory data")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Redfin data not found - skipping inventory analysis: {e}")
        return pd.DataFrame()


def metric_cash_flow_zones(zillow_zhvi_path: Path, zillow_zori_path: Path) -> pd.DataFrame:
    """
    Metric 3: Cash Flow Zone Finder (UPGRADED)
    
    Calculates rent/value ratio for each Sarasota zip code using real Zillow data.
    Ranks zips from best to worst cash flow potential.
    
    Returns:
        DataFrame with columns: zip_code, zhvi, zori, cash_flow_ratio, rank
    """
    logger.info("Calculating Metric 3: Cash Flow Zone Finder...")
    
    try:
        zhvi_df = pd.read_csv(zillow_zhvi_path)
        zori_df = pd.read_csv(zillow_zori_path)
        
        # Get the most recent month's data (last date column)
        date_cols_zhvi = [col for col in zhvi_df.columns if re.match(r'\d{4}-\d{2}-\d{2}', col)]
        date_cols_zori = [col for col in zori_df.columns if re.match(r'\d{4}-\d{2}-\d{2}', col)]
        
        if not date_cols_zhvi or not date_cols_zori:
            logger.warning("No date columns found in Zillow data")
            return pd.DataFrame()
        
        latest_zhvi_col = sorted(date_cols_zhvi)[-1]
        latest_zori_col = sorted(date_cols_zori)[-1]
        
        # Extract zip, value, rent for each zip
        zhvi_clean = zhvi_df[['RegionName', latest_zhvi_col]].copy()
        zhvi_clean.columns = ['zip_code', 'zhvi']
        
        zori_clean = zori_df[['RegionName', latest_zori_col]].copy()
        zori_clean.columns = ['zip_code', 'zori']
        
        # Merge
        result = zhvi_clean.merge(zori_clean, on='zip_code', how='inner')
        
        # Calculate cash flow ratio (monthly rent / home value)
        result['cash_flow_ratio'] = result['zori'] / result['zhvi']
        
        # Rank by ratio (higher = better cash flow)
        result['rank'] = result['cash_flow_ratio'].rank(ascending=False).astype(int)
        result = result.sort_values('rank')
        
        logger.info(f"Ranked {len(result)} Sarasota zip codes by cash flow potential")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Zillow data not found - skipping cash flow analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error calculating cash flow zones: {e}")
        return pd.DataFrame()


def metric_flip_detector(sales_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 4: Short Hold Flip Detector (UNCHANGED from V3)
    
    Identifies properties with 4-12 month hold periods in SCPA sales data.
    Indicates probable flips - check renovation quality vs. markup.
    
    Returns:
        DataFrame of flagged properties with recent repeat sales
    """
    logger.info("Calculating Metric 4: Short Hold Flip Detector...")
    
    if sales_df is None or len(sales_df) == 0:
        logger.warning("No sales data available - skipping flip detection")
        return pd.DataFrame()
    
    try:
        # Parse sale dates
        sales_df = sales_df.copy()
        sales_df['SaleDate'] = pd.to_datetime(sales_df['SaleDate'], errors='coerce')
        
        # Group by account to find repeat sales
        flips = []
        for account, group in sales_df.groupby('Account'):
            group = group.sort_values('SaleDate')
            
            for i in range(len(group) - 1):
                sale1 = group.iloc[i]
                sale2 = group.iloc[i + 1]
                
                days_held = (sale2['SaleDate'] - sale1['SaleDate']).days
                
                # Flag if held 4-12 months
                if 120 <= days_held <= 365:
                    flips.append({
                        'account': account,
                        'first_sale_date': sale1['SaleDate'],
                        'first_sale_price': sale1['SalePrice'],
                        'second_sale_date': sale2['SaleDate'],
                        'second_sale_price': sale2['SalePrice'],
                        'days_held': days_held,
                        'markup': sale2['SalePrice'] - sale1['SalePrice'],
                        'markup_pct': (sale2['SalePrice'] - sale1['SalePrice']) / sale1['SalePrice']
                    })
        
        result = pd.DataFrame(flips)
        logger.info(f"Found {len(result)} probable flips (4-12 month holds)")
        return result
        
    except Exception as e:
        logger.error(f"Error in flip detection: {e}")
        return pd.DataFrame()


def metric_appraisal_gap(zillow_zhvi_path: Path, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 5: Appraisal Gap (UPDATED for V4)
    
    Compares Zillow ZHVI (market home values) vs county JUST (assessed values) by zip code.
    
    Signal:
    - ZHVI > JUST by 15%+ = market running hot vs assessments
    - ZHVI at or below JUST = market cooling toward assessed values
    
    Returns:
        DataFrame with columns: zip_code, zhvi, avg_just, gap_pct, flag
    """
    logger.info("Calculating Metric 5: Appraisal Gap...")
    
    try:
        zhvi_df = pd.read_csv(zillow_zhvi_path)
        
        # Get most recent ZHVI
        date_cols = [col for col in zhvi_df.columns if re.match(r'\d{4}-\d{2}-\d{2}', col)]
        if not date_cols:
            logger.warning("No date columns in ZHVI data")
            return pd.DataFrame()
        
        latest_col = sorted(date_cols)[-1]
        zhvi_clean = zhvi_df[['RegionName', latest_col]].copy()
        zhvi_clean.columns = ['zip_code', 'zhvi']
        
        # Calculate average JUST by zip from county parcels
        parcels_df = parcels_df.copy()
        parcels_df['JUST'] = pd.to_numeric(parcels_df['JUST'], errors='coerce')
        
        county_avg = parcels_df.groupby('LOCZIP')['JUST'].mean().reset_index()
        county_avg.columns = ['zip_code', 'avg_just']
        county_avg['zip_code'] = pd.to_numeric(county_avg['zip_code'], errors='coerce')
        
        # Merge
        result = zhvi_clean.merge(county_avg, on='zip_code', how='inner')
        
        # Calculate gap
        result['gap_pct'] = (result['zhvi'] - result['avg_just']) / result['avg_just']
        
        # Flag significant gaps
        result['flag'] = ''
        result.loc[result['gap_pct'] > 0.15, 'flag'] = 'HOT_MARKET'
        result.loc[result['gap_pct'] <= 0, 'flag'] = 'COOLING'
        
        result = result[result['flag'] != '']
        logger.info(f"Found {len(result)} zip codes with significant appraisal gaps")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Data not found - skipping appraisal gap analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error calculating appraisal gap: {e}")
        return pd.DataFrame()


def run_transformation() -> dict:
    """
    Execute all transformation metrics for V4.
    
    Returns:
        dict: Results keyed by metric name
    """
    logger.info("=" * 60)
    logger.info("STARTING TRANSFORMATION (V4 MARKET STRATEGIST)")
    logger.info("=" * 60)
    
    results = {}
    
    # Load input data
    redfin_dir = Path("data/redfin")
    zillow_zhvi_path = Path("data/zillow_zhvi.csv")
    zillow_zori_path = Path("data/zillow_zori.csv")
    
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
    results['price_pressure'] = metric_price_pressure_index(redfin_dir)
    results['inventory_absorption'] = metric_inventory_absorption(redfin_dir)
    results['cash_flow_zones'] = metric_cash_flow_zones(zillow_zhvi_path, zillow_zori_path)
    results['flip_detector'] = metric_flip_detector(sales_df, parcels_df)
    results['appraisal_gap'] = metric_appraisal_gap(zillow_zhvi_path, parcels_df)
    
    logger.info("✅ Transformation complete")
    return results


if __name__ == "__main__":
    results = run_transformation()
    
    # Print summary
    print("\n" + "=" * 60)
    print("TRANSFORMATION RESULTS SUMMARY (V4)")
    print("=" * 60)
    for metric_name, df in results.items():
        print(f"{metric_name}: {len(df)} records")
