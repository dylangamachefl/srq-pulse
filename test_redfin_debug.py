"""
Redfin Tableau Public Scraper - DEBUG MODE v2

Test version with visible browser - focuses on Median Sale Price only.
"""

from pathlib import Path
from playwright.sync_api import sync_playwright
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TEST_URL = "https://public.tableau.com/app/profile/redfin/viz/RedfinCOVID-19HousingMarket/MedianSalePrice"

output_dir = Path("data/redfin")
output_dir.mkdir(parents=True, exist_ok=True)

print("\n" + "="*60)
print("REDFIN SCRAPER DEBUG TEST")
print("="*60)
print(f"Testing URL: {TEST_URL}")
print(f"Output dir: {output_dir}")
print("="*60 + "\n")

with sync_playwright() as p:
    print("1. Launching Chromium (visible)...")
    browser = p.chromium.launch(headless=False, slow_mo=1000)  # Slow down for visibility
    
    context = browser.new_context(
        accept_downloads=True,
        viewport={'width': 1920, 'height': 1080}
    )
    page = context.new_page()
    
    try:
        print("2. Navigating to Tableau page...")
        page.goto(TEST_URL, wait_until="domcontentloaded", timeout=120000)
        print("   ✅ Page loaded (DOM ready)")
        
        print("3. Waiting for Tableau viz to render...")
        time.sleep(10)  # Give Tableau time to fully load
        
        print("4. Looking for canvas element (Tableau chart)...")
        canvas_count = page.locator("canvas").count()
        print(f"   Found {canvas_count} canvas elements")
        
        if canvas_count > 0:
            print("   ✅ Tableau visualization appears to be loaded")
        else:
            print("   ⚠️  No canvas found - page may not have loaded correctly")
        
        print("\n5. Looking for filter controls...")
        select_count = page.locator("select").count()
        print(f"   Found {select_count} dropdown selectors")
        
        print("\n6. Current page title:", page.title())
        
        print("\n" + "="*60)
        print("MANUAL INSPECTION")
        print("="*60)
        print("The browser window should be visible now.")
        print("Please check:")
        print("  - Does the Tableau dashboard load?")
        print("  - Can you see the filters (Region Type, Region Name, etc.)?")
        print("  - Does the chart render properly?")
        print("\nPress ENTER when ready to continue...")
        print("="*60 + "\n")
        
        input()
        
        print("\n7. Attempting to set filters...")
        try:
            # Try to find and set Region Type
            print("   Looking for Region Type dropdown...")
            selects = page.locator("select").all()
            print(f"   Found {len(selects)} select elements total")
            
            for i, select in enumerate(selects):
                options = select.locator("option").all()
                option_texts = [opt.inner_text() for opt in options]
                print(f"   Select #{i}: {option_texts[:3]}...")  # Show first 3 options
                
        except Exception as e:
            print(f"   ❌ Error inspecting filters: {e}")
        
        print("\n8. Press ENTER to close browser...")
        input()
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        print("\nPress ENTER to close browser...")
        input()
    
    finally:
        print("\n9. Closing browser...")
        try:
            browser.close()
            print("   ✅ Browser closed successfully")
        except:
            print("   ⚠️  Browser already closed")

print("\n" + "="*60)
print("DEBUG TEST COMPLETE")
print("="*60 + "\n")
