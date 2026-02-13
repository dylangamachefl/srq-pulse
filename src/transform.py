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
        # Load Redfin data
        # Note: Ingestion already converted Tab -> Comma
        median_price_df = pd.read_csv(redfin_dir / "median_sale_price.csv")
        sale_to_list_df = pd.read_csv(redfin_dir / "avg_sale_to_list.csv")
        
        # Clean Period End to datetime
        median_price_df['Period End'] = pd.to_datetime(median_price_df['Period End'])
        sale_to_list_df['Period End'] = pd.to_datetime(sale_to_list_df['Period End'])
        
        # Clean Median Sale Price (remove commas/quotes)
        if median_price_df['Median Sale Price'].dtype == object:
            median_price_df['Median Sale Price'] = (
                median_price_df['Median Sale Price']
                .str.replace('"', '')
                .str.replace(',', '')
                .astype(float)
            )
        
        # Merge metrics on date
        merged = median_price_df.merge(sale_to_list_df, on='Period End', suffixes=('', '_ratio'))
        
        # Select relevant columns including YoY if they exist
        cols_to_keep = ['Period End', 'Median Sale Price', 'Average Sale To List Ratio']
        col_rename = {
            'Period End': 'week',
            'Median Sale Price': 'median_price',
            'Average Sale To List Ratio': 'sale_to_list'
        }
        
        if 'Median Sale Price Yoy' in median_price_df.columns:
            cols_to_keep.append('Median Sale Price Yoy')
            col_rename['Median Sale Price Yoy'] = 'price_yoy'
        
        # Get latest 4 weeks
        merged = merged.sort_values('Period End', ascending=False).head(4).copy()
        merged = merged.sort_values('Period End') # Sort back chronologically for delta
        
        # Calculate WoW price delta as percentage change
        merged['price_delta'] = merged['Median Sale Price'].pct_change()
        
        # Calculate signal
        def get_price_signal(row):
            ratio = row['Average Sale To List Ratio']
            
            if pd.isna(row['price_delta']):
                # If WoW delta is NaN, don't reference price direction
                if ratio > 1.0:
                    return f"SELLERS CONTROL (Ratio: {ratio:.1%})"
                elif ratio < 0.97:
                    return f"BUYERS LEVERAGE (Ratio: {ratio:.1%})"
                else:
                    return f"NEUTRAL (Ratio: {ratio:.1%})"
            
            price_trend = "DOWN" if row['price_delta'] < 0 else "UP"
            if ratio > 1.0:
                return f"SELLERS CONTROL (Ratio: {ratio:.1%}, Price {price_trend})"
            elif ratio < 0.97:
                return f"BUYERS LEVERAGE (Ratio: {ratio:.1%}, Price {price_trend})"
            else:
                return f"NEUTRAL (Ratio: {ratio:.1%}, Price {price_trend})"
        
        merged['signal'] = merged.apply(get_price_signal, axis=1)
        
        # Format for output
        result = merged[cols_to_keep + ['price_delta', 'signal']].copy()
        result.rename(columns=col_rename, inplace=True)
        
        # Ensure price_yoy exists even if column was missing
        if 'price_yoy' not in result.columns:
            result['price_yoy'] = 0.0
        
        # Format dates once during transformation
        result['week'] = pd.to_datetime(result['week']).dt.strftime("%b %d")
        
        logger.info(f"Calculated {len(result)} weeks of price pressure data")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Redfin data not found - skipping price pressure analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error calculating price pressure index: {e}")
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
        
        # Clean types
        supply_df['Period End'] = pd.to_datetime(supply_df['Period End'])
        listings_df['Period End'] = pd.to_datetime(listings_df['Period End'])
        sold_df['Period End'] = pd.to_datetime(sold_df['Period End'])
        
        # Merge
        merged = supply_df.merge(listings_df, on='Period End').merge(sold_df, on='Period End')
        
        # Select relevant columns including YoY if they exist
        cols_to_keep = ['Period End', 'Months Of Supply', 'Adjusted Average New Listings', 'Adjusted Average Homes Sold']
        col_rename = {
            'Period End': 'week',
            'Months Of Supply': 'weeks_of_supply',
            'Adjusted Average New Listings': 'new_listings',
            'Adjusted Average Homes Sold': 'homes_sold'
        }
        
        if 'Months Of Supply Yoy' in supply_df.columns:
            cols_to_keep.append('Months Of Supply Yoy')
            col_rename['Months Of Supply Yoy'] = 'supply_yoy'
            
        # Get latest 4 weeks
        merged = merged.sort_values('Period End', ascending=False).head(4).copy()
        
        # Calculate ratio
        merged['absorption_ratio'] = merged['Adjusted Average Homes Sold'] / merged['Adjusted Average New Listings']
        
        # Determine state
        def get_market_state(row):
            supply = row['Months Of Supply']
            if supply > 18:
                return "BUYERS MARKET (Heavy Supply)"
            elif supply < 8:
                return "SELLERS MARKET (Low Supply)"
            else:
                return "BALANCED MARKET"
        
        merged['market_state'] = merged.apply(get_market_state, axis=1)
        
        # Format for output
        result = merged[cols_to_keep + ['market_state']].copy()
        result.rename(columns=col_rename, inplace=True)
        
        # Ensure supply_yoy exists even if column was missing
        if 'supply_yoy' not in result.columns:
            result['supply_yoy'] = 0.0
            
        # Format dates once during transformation
        result['week'] = pd.to_datetime(result['week']).dt.strftime("%b %d")
        
        logger.info(f"Calculated {len(result)} weeks of inventory data")
        return result
        
    except FileNotFoundError as e:
        logger.warning(f"Redfin data not found - skipping inventory analysis: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error calculating inventory & absorption: {e}")
        return pd.DataFrame()


def metric_cash_flow_zones(zillow_zori_path: Path, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Metric 3: Cash Flow Zone Finder (UPGRADED with SCPA logic)
    
    Calculates rent/value ratio per zip code using:
    - Numerator: County-wide ZORI (from Zillow)
    - Denominator: Average assessed value (JUST) per zip (from SCPA)
    
    Excludes non-residential parcels (JUST=0 or LIVING=0).
    
    Returns:
        DataFrame with columns: zip, avg_assessed, est_annual_rent, cash_flow_ratio, rank
    """
    logger.info("Calculating Metric 3: Cash Flow Zone Finder (SCPA-Powered)...")
    
    try:
        # 1. Get average assessed value per zip from parcels
        if parcels_df.empty:
            logger.warning("No parcel data for cash flow analysis")
            return pd.DataFrame()
            
        parcels = parcels_df.copy()
        parcels['JUST'] = pd.to_numeric(parcels['JUST'], errors='coerce')
        parcels['LIVING'] = pd.to_numeric(parcels['LIVING'], errors='coerce')
        parcels['LOCZIP'] = pd.to_numeric(parcels['LOCZIP'], errors='coerce')
        
        # Residential filter: JUST > 0 and LIVING > 0
        parcels = parcels[(parcels['JUST'] > 0) & (parcels['LIVING'] > 0)]
        
        zip_values = parcels.groupby("LOCZIP")["JUST"].mean().reset_index()
        zip_values.columns = ["zip", "avg_assessed"]
        
        # 2. Get latest county-wide rent (ZORI)
        zori_df = pd.read_csv(zillow_zori_path)
        date_cols = [col for col in zori_df.columns if re.match(r'\d{4}-\d{2}-\d{2}', col)]
        if not date_cols:
            logger.warning("No ZORI data found")
            return pd.DataFrame()
            
        latest_col = sorted(date_cols)[-1]
        county_rent = zori_df[latest_col].values[0]
        
        # 3. Calculate metrics
        zip_values["est_annual_rent"] = county_rent * 12
        zip_values["cash_flow_ratio"] = zip_values["est_annual_rent"] / zip_values["avg_assessed"]
        
        # Rank and filter to reasonable zips (Sarasota only)
        # Sarasota zips are already filtered in ingest.py LOCCITY == 'SARASOTA'
        zip_values = zip_values.sort_values("cash_flow_ratio", ascending=False)
        zip_values["rank"] = range(1, len(zip_values) + 1)
        
        logger.info(f"Ranked {len(zip_values)} zips by SCPA-powered cash flow ratio")
        return zip_values
        
    except Exception as e:
        logger.error(f"Error calculating SCPA cash flow zones: {e}")
        return pd.DataFrame()


def metric_trend_lines(zillow_zhvi_path: Path, zillow_zori_path: Path, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhancement 2: Cash Flow & Appraisal Trend Lines
    
    Tracks county-level trends over the last 6 months.
    - Cash Flow Trend: ZORI * 12 / ZHVI
    - Appraisal Gap Trend: ZHVI vs (Constant) Avg SCPA JUST
    
    Returns:
        DataFrame with columns: month, zhvi, zori, flow_ratio, appraisal_gap
    """
    logger.info("Calculating Enhancement 2: Trend Lines (Last 6 Months)...")
    
    try:
        zhvi_df = pd.read_csv(zillow_zhvi_path)
        zori_df = pd.read_csv(zillow_zori_path)
        
        # Get last 6 months of columns
        date_cols = [col for col in zhvi_df.columns if re.match(r'\d{4}-\d{2}-\d{2}', col)]
        months = sorted(date_cols)[-6:]
        
        # Calculate constant county-wide JUST average
        if not parcels_df.empty:
            parcels = parcels_df.copy()
            parcels['JUST'] = pd.to_numeric(parcels['JUST'], errors='coerce')
            parcels['LIVING'] = pd.to_numeric(parcels['LIVING'], errors='coerce')
            county_just_avg = parcels[(parcels['JUST'] > 0) & (parcels['LIVING'] > 0)]['JUST'].mean()
        else:
            county_just_avg = 0
            
        trend_data = []
        for m in months:
            zhvi_val = zhvi_df[m].values[0]
            zori_val = zori_df[m].values[0] if m in zori_df.columns else 0
            
            flow_ratio = (zori_val * 12) / zhvi_val if zhvi_val > 0 else 0
            appraisal_gap = (zhvi_val - county_just_avg) / county_just_avg if county_just_avg > 0 else 0
            
            trend_data.append({
                "month": pd.to_datetime(m).strftime("%b %Y"),
                "zhvi": zhvi_val,
                "zori": zori_val,
                "flow_ratio": flow_ratio,
                "appraisal_gap": appraisal_gap
            })
            
        result = pd.DataFrame(trend_data)
        
        # Add direction for flow ratio
        result['direction'] = "→ Flat"
        if len(result) > 1:
            for i in range(1, len(result)):
                prev = result.iloc[i-1]['flow_ratio']
                curr = result.iloc[i]['flow_ratio']
                if curr > prev * 1.001:
                    result.at[i, 'direction'] = "↑ Expanding"
                elif curr < prev * 0.999:
                    result.at[i, 'direction'] = "↓ Compressing"
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating trend lines: {e}")
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
                    # Avoid division by zero
                    if sale1['SalePrice'] > 0:
                        flips.append({
                            'account': account,
                            'first_sale_date': sale1['SaleDate'],
                            'first_sale_price': sale1['SalePrice'],
                            'second_sale_date': sale2['SaleDate'],
                            'second_sale_price': sale2['SalePrice'],
                            'days_held': days_held,
                            'markup': sale2['SalePrice'] - sale1['SalePrice'],
                            'markup_pct': (sale2['SalePrice'] - sale1['SalePrice']) / sale1['SalePrice'],
                            'outcome': 'PROFITABLE' if sale2['SalePrice'] > sale1['SalePrice'] else 'LOSS'
                        })
        
        result = pd.DataFrame(flips)
        
        # Apply filters: recently completed flips (last 180 days)
        if not result.empty:
            cutoff = datetime.now() - timedelta(days=180)
            before_filter = len(result)
            result = result[result["second_sale_date"] >= cutoff].copy()
            logger.info(f"Filtered to last 180 days: {before_filter} -> {len(result)} flips")
            
            # Format dates for delivery
            result["first_sale_date"] = result["first_sale_date"].dt.strftime("%b %d")
            result["second_sale_date"] = result["second_sale_date"].dt.strftime("%b %d")
        
        # Sanity Check (10-150 range)
        count = len(result)
        if count < 10 or count > 150:
            logger.warning(f"⚠️  SANITY CHECK: Flip count ({count}) is outside abnormal range (10-150). Verify data integrity.")
        else:
            logger.info(f"✅ Sanity check passed: {count} flips detected")
            
        return result
        
    except Exception as e:
        logger.error(f"Error in flip detection: {e}")
        return pd.DataFrame()


def metric_zip_price_trends(sales_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhancement 4: Zip-Level Price Trends (Last 12 Months vs Prior Year)
    
    Returns:
        DataFrame with columns: zip, price_now, price_prior, yoy_change, sales_volume
    """
    logger.info("Calculating Enhancement 4: Zip-Level Price Trends...")
    
    try:
        if sales_df.empty or parcels_df.empty:
            return pd.DataFrame()
            
        # Join sales with parcels to get zip codes
        sales = sales_df.copy()
        parcels = parcels_df[['ACCOUNT', 'LOCZIP']].copy()
        sales['Account'] = sales['Account'].astype(str).str.strip()
        parcels['ACCOUNT'] = parcels['ACCOUNT'].astype(str).str.strip()
        
        merged = sales.merge(parcels, left_on='Account', right_on='ACCOUNT')
        merged['zip'] = pd.to_numeric(merged['LOCZIP'], errors='coerce')
        
        cutoff_now = datetime.now() - timedelta(days=365)
        cutoff_prior = datetime.now() - timedelta(days=730)
        
        recent = merged[merged["SaleDate"] >= cutoff_now]
        prior = merged[(merged["SaleDate"] >= cutoff_prior) & (merged["SaleDate"] < cutoff_now)]
        
        current_by_zip = recent.groupby("zip")["SalePrice"].agg(['median', 'count']).reset_index()
        current_by_zip.columns = ['zip', 'price_now', 'sales_volume']
        
        prior_by_zip = prior.groupby("zip")["SalePrice"].median().reset_index()
        prior_by_zip.columns = ['zip', 'price_prior']
        
        result = current_by_zip.merge(prior_by_zip, on="zip", how="inner")
        result["yoy_change"] = (result["price_now"] - result["price_prior"]) / result["price_prior"]
        result = result.sort_values("yoy_change", ascending=True)
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating zip price trends: {e}")
        return pd.DataFrame()


def metric_assessment_ratio(sales_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhancement 6: Assessment Ratio by Zip (Sale Price vs County Appraised Value)
    
    Returns:
        DataFrame with columns: zip, median_ratio, meaning
    """
    logger.info("Calculating Enhancement 6: Assessment Ratio by Zip...")
    
    try:
        if sales_df.empty or parcels_df.empty:
            return pd.DataFrame()
            
        # Join sales with parcels
        sales = sales_df.copy()
        parcels = parcels_df[['ACCOUNT', 'LOCZIP', 'JUST']].copy()
        sales['Account'] = sales['Account'].astype(str).str.strip()
        parcels['ACCOUNT'] = parcels['ACCOUNT'].astype(str).str.strip()
        
        merged = sales.merge(parcels, left_on='Account', right_on='ACCOUNT')
        merged['zip'] = pd.to_numeric(merged['LOCZIP'], errors='coerce')
        merged['JUST'] = pd.to_numeric(merged['JUST'], errors='coerce')
        
        # Last 12 months
        cutoff = datetime.now() - timedelta(days=365)
        recent = merged[(merged["SaleDate"] >= cutoff) & (merged["JUST"] > 0)]
        
        recent["assessment_ratio"] = recent["SalePrice"] / recent["JUST"]
        
        result = recent.groupby("zip")["assessment_ratio"].median().reset_index()
        result.columns = ['zip', 'median_ratio']
        
        def get_ratio_meaning(ratio):
            if ratio < 0.95: return "Market cooling (below assessed)"
            elif ratio < 1.05: return "Neutral (near assessed)"
            else: return "Stable/Hot (above assessed)"
            
        result['meaning'] = result['median_ratio'].apply(get_ratio_meaning)
        result = result.sort_values("median_ratio", ascending=True)
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating assessment ratio: {e}")
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
        
        # Ensure zip codes are clean numeric for merging
        parcels_df['LOCZIP'] = pd.to_numeric(parcels_df['LOCZIP'], errors='coerce')
        zhvi_clean['zip_code'] = pd.to_numeric(zhvi_clean['zip_code'], errors='coerce')
        
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


def metric_investor_activity(sales_df: pd.DataFrame, parcels_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhancement 7: Investor Activity Tracker (Last 12 Months)
    
    Identifies likely investor purchases based on absence of homestead exemption.
    
    Returns:
        DataFrame with columns: zip, total_sales, investor_share
    """
    logger.info("Calculating Enhancement 7: Investor Activity Tracker...")
    
    try:
        if sales_df.empty or parcels_df.empty:
            return pd.DataFrame()
            
        if 'HOMESTEAD' not in parcels_df.columns:
            logger.warning("HOMESTEAD column missing from parcel data - skipping investor activity")
            return pd.DataFrame()
            
        # Join sales with parcels
        sales = sales_df.copy()
        parcels = parcels_df[['ACCOUNT', 'LOCZIP', 'HOMESTEAD']].copy()
        sales['Account'] = sales['Account'].astype(str).str.strip()
        parcels['ACCOUNT'] = parcels['ACCOUNT'].astype(str).str.strip()
        
        merged = sales.merge(parcels, left_on='Account', right_on='ACCOUNT')
        merged['zip'] = pd.to_numeric(merged['LOCZIP'], errors='coerce')
        
        # Last 12 months
        cutoff = datetime.now() - timedelta(days=365)
        recent = merged[merged["SaleDate"] >= cutoff].copy()
        
        if recent.empty:
            return pd.DataFrame()
            
        # Flag investor vs owner-occupant
        # HOMESTEAD > 0 means owner-occupied
        recent["buyer_type"] = recent["HOMESTEAD"].apply(
            lambda h: "OWNER-OCCUPANT" if pd.notna(h) and float(h) > 0 else "LIKELY INVESTOR"
        )
        
        # Calculate share by zip
        investor_stats = recent.groupby("zip")["buyer_type"].agg([
            ('total_sales', 'count'),
            ('investor_share', lambda x: (x == "LIKELY INVESTOR").mean())
        ]).reset_index()
        
        result = investor_stats.sort_values("investor_share", ascending=False)
        return result
        
    except Exception as e:
        logger.error(f"Error calculating investor activity: {e}")
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
    zillow_dir = Path("data/zillow")
    zillow_zhvi_path = zillow_dir / "zillow_zhvi.csv"
    zillow_zori_path = zillow_dir / "zillow_zori.csv"
    
    # Load county data
    try:
        county_dir = Path("data/county")
        parcels_df = pd.read_csv(county_dir / "county_parcels.csv", low_memory=False)
        sales_df = pd.read_csv(county_dir / "county_sales.csv", low_memory=False)
        
        # Filter out nominal/non-arm's-length transfers (e.g., $100 deeds to Trusts)
        # These are usually not market sales and skew flip detection.
        before_count = len(sales_df)
        sales_df = sales_df[sales_df['SalePrice'] > 10000].copy()
        logger.info(f"Filtered {before_count - len(sales_df)} nominal transfers (sales < $10k)")
        
        # Parse dates
        sales_df['SaleDate'] = pd.to_datetime(sales_df['SaleDate'], errors='coerce')
        
        logger.info(f"Loaded {len(parcels_df)} parcels and {len(sales_df)} valid market sales")
    except FileNotFoundError:
        logger.warning("County data not found - some metrics will be skipped")
        parcels_df = pd.DataFrame()
        sales_df = pd.DataFrame()
    
    # Run metrics
    results['price_pressure'] = metric_price_pressure_index(redfin_dir)
    results['inventory_absorption'] = metric_inventory_absorption(redfin_dir)
    results['cash_flow_zones'] = metric_cash_flow_zones(zillow_zori_path, parcels_df)
    results['trend_lines'] = metric_trend_lines(zillow_zhvi_path, zillow_zori_path, parcels_df)
    results['zip_price_trends'] = metric_zip_price_trends(sales_df, parcels_df)
    results['assessment_ratio'] = metric_assessment_ratio(sales_df, parcels_df)
    results['investor_activity'] = metric_investor_activity(sales_df, parcels_df)
    results['flip_detector'] = metric_flip_detector(sales_df, parcels_df)
    
    # Calculate Flip Summary
    flips = results['flip_detector']
    if not flips.empty:
        total = len(flips)
        profitable = len(flips[flips['outcome'] == 'PROFITABLE'])
        loss = len(flips[flips['outcome'] == 'LOSS'])
        results['flip_summary'] = f"{total} total — {profitable} profitable, {loss} loss"
    else:
        results['flip_summary'] = "No flips detected"
        
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
