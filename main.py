"""
Sarasota Market Pulse — Main Orchestrator

Master conductor for the entire ETL pipeline:
1. Ingestion (MLS + County data)
2. Transformation (5 investment metrics)
3. Delivery (Email via Resend)

Includes state management with sanity checks and rolling 3-day history.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
import time

import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from ingest import run_ingestion, ZILLOW_FAILED, REDFIN_FAILED, SCPA_FAILED
from transform import run_transformation
from deliver import deliver_report

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def manage_history_state(metrics_data: dict, html_report: str = None) -> bool:
    """
    Manage rolling 4-week history with sanity checks (V4 weekly cadence).
    
    This is critical - the history JSON is our "database". A corrupted
    commit breaks all future runs.
    
    Args:
        metrics_data: Dict of metric results from transformation
        html_report: Optional HTML content of the report to archive
        
    Returns:
        bool: True if state management succeeded
    """
    logger.info("Managing history state...")
    
    history_dir = Path("data/history")
    history_dir.mkdir(parents=True, exist_ok=True)
    
    today = datetime.now().strftime("%Y%m%d")
    
    # Paths (V4: Only use JSON)
    history_today = history_dir / f"history_{today}.json"
    history_latest = history_dir / "history.json"
    
    # Report paths
    report_today = history_dir / f"report_{today}.html"
    report_latest = history_dir / "report_latest.html"
    
    # Save HTML report if provided
    if html_report:
        with open(report_today, 'w', encoding='utf-8') as f:
            f.write(html_report)
        with open(report_latest, 'w', encoding='utf-8') as f:
            f.write(html_report)
        logger.info(f"Saved HTML report to {report_today}")
    
    # Save today's state
    import json
    
    metrics_summary = {
        'run_date': today,
        'metric_counts': {k: len(v) if isinstance(v, pd.DataFrame) else 0 for k, v in metrics_data.items()}
    }
    
    with open(history_today, 'w') as f:
        json.dump(metrics_summary, f, indent=2)
    
    with open(history_latest, 'w') as f:
        json.dump(metrics_summary, f, indent=2)
    
    logger.info(f"Saved metrics summary to {history_today}")
    
    # Sanity check: For weekly data, just verify we have some metrics
    if len(metrics_data) == 0:
        logger.error("⚠️  SANITY CHECK FAILED: No metrics generated. Aborting commit.")
        return False
    
    logger.info(f"✅ Sanity check passed: Generated {len(metrics_data)} metrics")
    
    # Clean up old history files (keep only last 4 weeks)
    for old_file in history_dir.glob("history_*.json"):
        file_date_str = old_file.stem.replace("history_", "")
        if len(file_date_str) == 8 and file_date_str.isdigit():
            try:
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                days_old = (datetime.now() - file_date).days
                
                if days_old > 28:  # V4: 4 weeks instead of 3 days
                    old_file.unlink()
                    logger.info(f"Cleaned up old history: {old_file.name}")
            except ValueError:
                pass
    
    return True


def calculate_stats(ingestion_success: bool, start_time: float) -> dict:
    """
    Calculate pipeline execution statistics (V4: source-specific status).
    
    Returns:
        dict: Stats for email footer
    """
    from ingest import ZILLOW_FAILED, REDFIN_FAILED, SCPA_FAILED
    
    execution_time = time.time() - start_time
    
    stats = {
        'zillow_status': 'FAILED' if ZILLOW_FAILED else 'OK',
        'redfin_status': 'FAILED' if REDFIN_FAILED else 'OK',
        'scpa_status': 'FAILED' if SCPA_FAILED else 'OK',
        'execution_time': f"{execution_time:.1f}s"
    }
    
    return stats


def main():
    """
    Main pipeline orchestrator.
    """
    logger.info("=" * 80)
    logger.info("SARASOTA MARKET PULSE — ETL PIPELINE")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    # Phase 1: Ingestion
    logger.info("\n📥 PHASE 1: INGESTION")
    ingestion_success = run_ingestion()
    
    # If all sources failed, send degraded mode email and exit
    if ZILLOW_FAILED and REDFIN_FAILED and SCPA_FAILED:
        logger.warning("All data sources failed - entering degraded mode")
        stats = calculate_stats(ingestion_success, start_time)
        deliver_report({}, stats, is_degraded=True)
        return 1
    elif ZILLOW_FAILED or REDFIN_FAILED or SCPA_FAILED:
        logger.warning("Partial ingestion failure - some metrics may be unavailable")
    
    # Phase 2: Transformation
    logger.info("\n🧮 PHASE 2: TRANSFORMATION")
    try:
        results = run_transformation()
    except Exception as e:
        logger.error(f"Transformation failed: {type(e).__name__}: {str(e)}")
        stats = calculate_stats(ingestion_success, start_time)
        deliver_report({}, stats, is_degraded=True)
        return 1
    
    # Phase 3: Delivery
    logger.info("\n📧 PHASE 3: DELIVERY")
    stats = calculate_stats(ingestion_success, start_time)
    
    html_report = None
    try:
        success, html_report = deliver_report(results, stats, is_degraded=False)
    except Exception as e:
        logger.error(f"Delivery failed: {type(e).__name__}: {str(e)}")
        return 1
    
    # Phase 4: State Management
    logger.info("\n💾 PHASE 4: STATE MANAGEMENT")
    try:
        state_success = manage_history_state(results, html_report)
        
        if not state_success:
            logger.error("State management sanity check failed - aborting")
            return 1
    except Exception as e:
        logger.error(f"State management failed: {type(e).__name__}: {str(e)}")
        return 1
    
    # Success
    logger.info("\n" + "=" * 80)
    logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Total execution time: {stats['execution_time']}")
    logger.info("=" * 80)
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
