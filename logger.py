"""
logger.py — Centralized Logging Configuration
==============================================
Covers Chapter 4 of the syllabus:
  - Logging, monitoring, and error handling in pipelines (4.4)

All other modules import the shared logger from here so that
every log message (from scrapers, ETL, dashboard, scheduler)
ends up in the same log file with a consistent format.

Usage in any other file:
    from logger import get_logger
    logger = get_logger(__name__)
    logger.info("Something happened")
    logger.warning("Careful here")
    logger.error("Something broke")
"""

import logging
import os
from datetime import datetime

# ── Configuration constants ────────────────────────────────────
LOG_DIR  = "logs"
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")

os.makedirs(LOG_DIR, exist_ok=True)


# ── Log format ─────────────────────────────────────────────────
#  Columns: timestamp | level (padded) | module name | message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str = "job_analyzer") -> logging.Logger:
    """
    Returns a configured logger instance.

    Every logger shares the same two handlers:
      1. StreamHandler  — prints INFO+ messages to the console
      2. FileHandler    — writes DEBUG+ messages to logs/pipeline.log

    The root logger for this project is named 'job_analyzer'.
    Child loggers (e.g. 'job_analyzer.scraper') automatically
    inherit its handlers.

    Args:
        name (str): Logger name, typically __name__ of the calling module.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers when the same logger is requested twice
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)  # Capture everything at root level

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    # Handler 1 — Console (INFO and above only, to keep terminal readable)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Handler 2 — File (DEBUG and above, full detail for debugging)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# ── Pre-built project logger ────────────────────────────────────
#  Import this directly for the quickest usage:
#      from logger import logger
logger = get_logger("job_analyzer")


# ══════════════════════════════════════════════════════════════
#  DEMO — run this file directly to test logging
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log = get_logger("job_analyzer.demo")
    log.debug("This is a DEBUG message  — only written to file")
    log.info("This is an INFO message  — shown in terminal + file")
    log.warning("This is a WARNING        — shown in terminal + file")
    log.error("This is an ERROR         — shown in terminal + file")
    print(f"\nLog file written to: {os.path.abspath(LOG_FILE)}")
