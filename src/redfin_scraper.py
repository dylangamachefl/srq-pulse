"""
Redfin Tableau Scraper (V4) - Direct Dashboard Access

Downloads 6 CSV crosstabs from Redfin's Tableau dashboards.
Uses direct Tableau URLs and tab navigation instead of separate pages.

Based on browser inspection findings:
- Direct URL: https://public.tableau.com/views/RedfinCOVID-19HousingMarket/[TabName]
- Tab IDs: #tableauTabbedNavigation_tab_X
- Filters: span.tabComboBox custom controls
- Download: button#download → Crosstab → CSV
"""

from pathlib import Path
from playwright.sync_api import sync_playwright
import logging
import time

logger = logging.getLogger(__name__)

# Base Tableau URL with tab names mapped to their IDs
TABLEAU_BASE_URL = "https://public.tableau.com/views/RedfinCOVID-19HousingMarket/NewListings"

# Tab mappings (name -> tab ID for navigation)
TABS = {
    "median_sale_price": "#tableauTabbedNavigation_tab_5",
    "homes_sold": "#tableauTabbedNavigation_tab_3",
    "new_listings": "#tableauTabbedNavigation_tab_0",
    "days_to_close": "#tableauTabbedNavigation_tab_6",
    "weeks_of_supply": "#tableauTabbedNavigation_tab_11",
    "avg_sale_to_list": "#tableauTabbedNavigation_tab_13",
}


def set_filters(page) -> bool:
    """
    Set Region Type to County and Region Name to Sarasota County, FL.
    
    Returns:
        bool: True if successful
    """
    try:
        logger.info("Setting filters...")
        
        #1. Region Type: County
        # Find the combo box with "metro" text and click it
        region_type_boxes = page.locator("span.tabComboBox").all()
        for box in region_type_boxes:
            if "metro" in box.inner_text().lower():
                box.click()
                time.sleep(1)
                
                # Click "county" option in dropdown
                county_option = page.locator("a[title='county']").first
                if county_option.is_visible(timeout=5000):
                    county_option.click()
                    time.sleep(2)
                    logger.info("  ✓ Set Region Type to County")
                break
        
        # 2. Region Name: Sarasota County, FL
        # Find combo box with "All Redfin Metros" and click it
        region_name_boxes = page.locator("span.tabComboBox").all()
        for box in region_name_boxes:
            if "all redfin metros" in box.inner_text().lower():
                box.click()
                time.sleep(1)
                
                # Type in search box
                search_input = page.locator("input.tab-filterSearchInp").first
                if search_input.is_visible(timeout=5000):
                    search_input.fill("Sarasota County, FL")
                    time.sleep(1)
                    search_input.press("Enter")
                    time.sleep(3)  # Wait for dashboard to update
                    logger.info("  ✓ Set Region Name to Sarasota County, FL")
                break
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to set filters: {e}")
        return False


def download_crosstab(page, metric_name: str, output_dir: Path) -> bool:
    """
    Download crosstab CSV for current view.
    
    Returns:
        bool: True if successful
    """
    try:
        logger.info(f"Downloading {metric_name}...")
        
        # Click download button
        download_btn = page.locator("button#download").first
        if not download_btn.is_visible(timeout=10000):
            logger.error("Download button not visible")
            return False
        
        download_btn.click()
        time.sleep(2)
        
        # Click Crosstab in menu
        crosstab_option = page.locator("div[role='menuitem']:has-text('Crosstab')").first
        if not crosstab_option.is_visible(timeout=5000):
            logger.error("Crosstab option not visible")
            return False
        
        crosstab_option.click()
        time.sleep(2)
        
        # Select CSV format (should be default)
        csv_label = page.locator("label:has-text('CSV')").first
        if csv_label.is_visible(timeout=3000):
            csv_label.click()
            time.sleep(1)
        
        # Click Download button in modal
        with page.expect_download(timeout=30000) as download_info:
            final_download_btn = page.locator("button.fc9tep5").first
            if not final_download_btn.is_visible(timeout=5000):
                # Fallback: try by text
                final_download_btn = page.locator("button:has-text('Download')").last
            final_download_btn.click()
        
        download = download_info.value
        
        # Save with standardized name
        save_path = output_dir / f"{metric_name}.csv"
        download.save_as(save_path)
        
        logger.info(f"✅ Downloaded {metric_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download {metric_name}: {e}")
        return False


def scroll_to_tab(page, tab_id: str, max_clicks: int = 3) -> bool:
    """
    Scroll the tab carousel to reveal a hidden tab.
    Some tabs require clicking the right arrow button.
    
    Args:
        page: Playwright page
        tab_id: Tab ID to reveal
        max_clicks: Maximum arrow clicks to try
        
    Returns:
        bool: True if tab is now visible
    """
    # Check if tab is already visible
    tab = page.locator(tab_id).first
    if tab.is_visible(timeout=1000):
        return True
    
    # Look for right arrow button in tab navigation
    right_arrow = page.locator("button.tabNavigationArrow.tabNavigationArrowRight").first
    
    for i in range(max_clicks):
        if not right_arrow.is_visible(timeout=1000):
            logger.warning("Right arrow button not found")
            return False
        
        logger.info(f"  Clicking right arrow to reveal tab (attempt {i+1})...")
        right_arrow.click()
        time.sleep(1)
        
        # Check if tab is now visible
        if tab.is_visible(timeout=1000):
            logger.info(f"  ✓ Tab {tab_id} is now visible")
            return True
    
    logger.warning(f"Tab {tab_id} still not visible after {max_clicks} arrow clicks")
    return False


def download_all_tabs() -> bool:
    """
    Download all 6 Redfin metrics by navigating tabs on a single dashboard.
    
    Returns:
        bool: True if at least one download succeeded
    """
    output_dir = Path("data/redfin")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    success_count = 0
    failed_metrics = []
    
    with sync_playwright() as p:
        logger.info("Launching browser...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        
        try:
            # Navigate to base dashboard
            logger.info(f"Loading Tableau dashboard...")
            page.goto(TABLEAU_BASE_URL, timeout=120000)
            time.sleep(10)  # Give Tableau time to fully render
            
            logger.info("Dashboard loaded")
            
            # Set filters once (applies to all tabs)
            if not set_filters(page):
                logger.warning("Filter setting failed, continuing anyway...")
            
            # Download each metric by clicking tabs
            for metric_name, tab_id in TABS.items():
                if tab_id is None:
                    logger.warning(f"Skipping {metric_name} - tab ID not mapped yet")
                    failed_metrics.append(metric_name)
                    continue
                
                logger.info(f"\nSwitching to {metric_name} tab...")
                
                # Scroll to tab if needed (for hidden tabs)
                if not scroll_to_tab(page, tab_id):
                    logger.error(f"Could not reveal tab {tab_id}")
                    failed_metrics.append(metric_name)
                    continue
                
                # Click the tab
                tab = page.locator(tab_id).first
                if tab.is_visible(timeout=5000):
                    tab.click()
                    time.sleep(5)  # Wait for tab content to load
                    
                    # Download this tab's data
                    if download_crosstab(page, metric_name, output_dir):
                        success_count += 1
                    else:
                        failed_metrics.append(metric_name)
                else:
                    logger.error(f"Tab {tab_id} not found")
                    failed_metrics.append(metric_name)
            
        except Exception as e:
            logger.error(f"Browser automation failed: {e}")
        
        finally:
            logger.info("\nClosing browser...")
            browser.close()
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Downloaded {success_count}/6 Redfin metrics")
    if failed_metrics:
        logger.warning(f"Failed: {', '.join(failed_metrics)}")
    logger.info(f"{'='*60}")
    
    return success_count > 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    success = download_all_tabs()
    exit(0 if success else 1)
