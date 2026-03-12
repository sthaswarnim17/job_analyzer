"""
utils.py — Utility Decorators, Generators, and Helpers
=======================================================
This module demonstrates key Python concepts from Chapter 1 of the syllabus:

  - Decorators  : @timer, @retry — modifying function behaviour without
                  changing the function's source code.
  - Generators  : paginated_jobs_generator — lazy evaluation for large
                  paginated API responses (memory-efficient streaming).
  - Lambda use  : salary_formatter helper for quick inline transformations.

Import from other modules:
    from utils import timer, retry, paginated_jobs_generator
"""

import functools
import time
import logging

logger = logging.getLogger("job_analyzer")


# ══════════════════════════════════════════════════════════════
#  DECORATOR 1: @timer
#  Logs how long any function takes to execute.
#  Usage: decorate any function with @timer
# ══════════════════════════════════════════════════════════════

def timer(func):
    """
    Decorator that measures and logs the execution time of the
    decorated function.

    Example:
        @timer
        def scrape_jobs():
            ...
    """
    @functools.wraps(func)  # Preserves the original function's name/docstring
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start_time
        logger.info(f"[TIMER] {func.__name__}() finished in {elapsed:.2f}s")
        return result
    return wrapper


# ══════════════════════════════════════════════════════════════
#  DECORATOR 2: @retry
#  Automatically retries a function if it raises an exception.
#  Useful for network calls that occasionally fail.
#  Usage: @retry(max_attempts=3, delay=2.0)
# ══════════════════════════════════════════════════════════════

def retry(max_attempts=3, delay=1.0, exceptions=(Exception,)):
    """
    Parameterized decorator factory that retries the decorated
    function up to `max_attempts` times on failure.

    Args:
        max_attempts (int): Maximum number of tries before giving up.
        delay        (float): Seconds to wait between attempts (doubles each try).
        exceptions   (tuple): Exception types that trigger a retry.

    Example:
        @retry(max_attempts=3, delay=2.0, exceptions=(requests.RequestException,))
        def fetch_page(url):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    wait = delay * attempt   # Exponential-ish back-off
                    logger.warning(
                        f"[RETRY] {func.__name__}() attempt {attempt}/{max_attempts} "
                        f"failed: {exc}. Retrying in {wait:.1f}s..."
                    )
                    time.sleep(wait)
            logger.error(
                f"[RETRY] {func.__name__}() failed after {max_attempts} attempts."
            )
            raise last_exc
        return wrapper
    return decorator


# ══════════════════════════════════════════════════════════════
#  GENERATOR: paginated_jobs_generator
#  Lazily yields one page of jobs at a time from a paginated
#  API. Memory-efficient: never loads all pages at once.
#  Demonstrates generators for data streaming (Chapter 1.3).
# ══════════════════════════════════════════════════════════════

def paginated_jobs_generator(fetch_func, start_url):
    """
    Generator function that fetches paginated API results one
    page at a time, yielding a list of job records per page.

    Args:
        fetch_func (callable): A function that accepts a URL and
                               returns the parsed JSON dict.
        start_url  (str): The first page URL to start from.

    Yields:
        list: The 'results' list from each API response page.

    Example:
        for page_jobs in paginated_jobs_generator(fetch, base_url):
            process(page_jobs)
    """
    url = start_url
    page_num = 1
    while url:
        logger.debug(f"[GENERATOR] Fetching page {page_num}: {url}")
        data = fetch_func(url)
        results = data.get("results", [])
        if not results:
            logger.debug("[GENERATOR] Empty page — stopping pagination.")
            return
        yield results
        url = data.get("next")   # None signals end of pages
        page_num += 1


# ══════════════════════════════════════════════════════════════
#  LAMBDA EXAMPLE: salary_formatter
#  Lambda expressions for quick one-line data transformations.
#  Chapter 1.2 — Advanced Functions and Lambda Expressions.
# ══════════════════════════════════════════════════════════════

# Format a numeric salary value as a readable NPR string
salary_formatter = lambda x: f"NPR {x:,.0f}" if x and x > 0 else "Negotiable"

# Capitalize each word in a job title, stripping extra whitespace
title_cleaner = lambda t: " ".join(t.strip().split()).title() if isinstance(t, str) else "Unknown"
