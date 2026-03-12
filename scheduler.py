"""
scheduler.py — Master Pipeline Orchestrator
=============================================
This is the MAIN file you run to start the entire project.

What it does:
  1. Creates the database (first time only)
  2. Immediately runs the full pipeline (scrape → clean → report)
  3. Keeps running, repeating the pipeline every 7 days

Demonstrates Chapter 4 (Data Engineering & Automation):
  - Automated ETL orchestration
  - schedule library for CRON-style automation
  - Logging and error handling in pipelines (via logger.py)
  - @timer decorator to track stage durations (via utils.py)

How to run:
  python scheduler.py

Keep this running in one terminal window.
In another terminal, run: streamlit run dashboard.py
"""

import schedule
import time
import sys
from datetime import datetime

# ── Logging + decorators (Chapter 1 & 4) ─────────────────────
from logger import get_logger
from obselete.utils  import timer

logger = get_logger("job_analyzer.scheduler")

# ── Import the other modules ───────────────────────────────────
from cleaner.database import setup_database
try:
    from scraper.merojob_scraper import scrape_jobs as scrape_mero
except ImportError as e:
    logger.critical(f"Could not import merojob_scraper: {e}")
    sys.exit(1)

try:
    from scraper.scrape_kumari import scrape_kumari_jobs as scrape_kumari
except ImportError as e:
    logger.critical(f"Could not import scrape_kumari: {e}")
    sys.exit(1)

try:
    from cleaner.clean_data import clean_and_merge
except ImportError as e:
    logger.critical(f"Could not import clean_data: {e}")
    sys.exit(1)

try:
    from reportGenerator.report_generator import generate_all_reports
except ImportError as e:
    logger.warning(f"report_generator not available: {e}")
    generate_all_reports = None


# ══════════════════════════════════════════════════════════════
#  FULL PIPELINE FUNCTION
#  @timer decorator (from utils.py) logs how long it takes
# ══════════════════════════════════════════════════════════════

@timer
def full_pipeline():
    """
    Runs all four stages in order:
      1. Scrape MeroJob
      2. Scrape KumariJob
      3. Clean and merge data into jobs_clean table
      4. Generate HTML + Excel reports
    """
    start = datetime.now()
    logger.info("=" * 55)
    logger.info(f"PIPELINE STARTED — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    # ── Stage 1: Scrape MeroJob ────────────────────────────────
    logger.info("[1/4] Scraping MeroJob...")
    try:
        mero_count = scrape_mero()
        logger.info(f"      MeroJob scraping done — {mero_count} jobs")
    except Exception as e:
        logger.error(f"      MeroJob scraping failed: {e}")
        mero_count = 0

    # ── Stage 2: Scrape KumariJob ──────────────────────────────
    logger.info("[2/4] Scraping KumariJob...")
    try:
        kumari_count = scrape_kumari()
        logger.info(f"      KumariJob scraping done — {kumari_count} jobs")
    except Exception as e:
        logger.error(f"      KumariJob scraping failed: {e}")
        kumari_count = 0

    # ── Stage 3: Clean and merge ───────────────────────────────
    logger.info("[3/4] Cleaning and processing data...")
    try:
        df          = clean_and_merge()
        clean_count = len(df) if df is not None else 0
        logger.info(f"      Cleaning done — {clean_count} clean jobs in DB")
    except Exception as e:
        logger.error(f"      Cleaning failed: {e}")
        clean_count = 0

    # ── Stage 4: Generate reports ──────────────────────────────
    logger.info("[4/4] Generating HTML + Excel reports...")
    if generate_all_reports:
        try:
            html_path, xlsx_path = generate_all_reports()
            logger.info(f"      HTML report : {html_path}")
            if xlsx_path:
                logger.info(f"      Excel report: {xlsx_path}")
        except Exception as e:
            logger.error(f"      Report generation failed: {e}")
    else:
        logger.warning("      Skipped — report_generator not available")

    # ── Summary ────────────────────────────────────────────────
    end      = datetime.now()
    duration = (end - start).seconds
    logger.info("=" * 55)
    logger.info(f"PIPELINE COMPLETE — {end.strftime('%H:%M:%S')}")
    logger.info(f"  Duration      : {duration}s")
    logger.info(f"  MeroJob jobs  : {mero_count}")
    logger.info(f"  KumariJob jobs: {kumari_count}")
    logger.info(f"  Clean total   : {clean_count}")
    logger.info(f"  Dashboard     : Refresh your browser to see updated data")
    logger.info("=" * 55)


# ══════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════

logger.info("Nepal Job Market Dashboard — Pipeline Scheduler starting")

# Step 1: Make sure database exists
logger.info("Setting up database...")
setup_database()

# Step 2: Run once immediately on startup
logger.info("Running initial data collection...")
full_pipeline()

# Step 3: Schedule to run every 7 days
schedule.every(7).days.do(full_pipeline)

logger.info("Scheduler active — next run in 7 days.")
logger.info("Open a new terminal and run: streamlit run dashboard.py")
logger.info("Keeping scheduler alive... (Ctrl+C to stop)")

# Keep the scheduler alive (checks every minute)
while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        sys.exit(0)
