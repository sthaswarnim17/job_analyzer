"""
merojob_scraper.py  — MODIFIED VERSION
Changes from original:
  - Removed the schedule/while loop (scheduler.py handles that now)
  - Saves to SQLite database instead of CSV files
  - scrape_jobs() is now a plain function you import and call
"""

import requests
import os
from datetime import datetime

# Import our central database module
from cleaner.database import save_merojob_data


def scrape_jobs():
    """
    Scrapes all job listings from the MeroJob API and saves them
    to the SQLite database. Returns the number of jobs saved.
    """
    print(f"\n[MeroJob] Scraping started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://merojob.com/search/"
    }

    # Start from page 1
    url = "https://api.merojob.com/api/v1/jobs/?page=1&page_size=50"
    all_jobs = []

    while url:
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"  [MeroJob] Error fetching page: {e}")
            break

        for job in data.get('results', []):
            # Safely extract location (API has two possible places for it)
            location = job.get("client", {}).get("location", "N/A")
            if not location or location == "N/A":
                job_locs = job.get("job_locations", [])
                if job_locs:
                    location = job_locs[0].get("address", "N/A")

            all_jobs.append({
                "id":         job.get("id"),
                "title":      job.get("title", "N/A"),
                "company":    job.get("client", {}).get("client_name", "N/A"),
                "location":   location,
                "categories": ", ".join(job.get("categories", [])),
                "deadline":   job.get("deadline", "N/A"),
                "job_level":  job.get("job_level", "N/A"),
                "vacancies":  job.get("vacancies", "N/A"),
                "salary_min": job.get("offered_salary", {}).get("minimum", "N/A"),
                "salary_max": job.get("offered_salary", {}).get("maximum", "N/A"),
                "currency":   job.get("offered_salary", {}).get("currency", "N/A"),
                "skills":     ", ".join(job.get("skills", [])),
                "job_url":    "https://www.merojob.com" + job.get("absolute_url", "")
            })

        print(f"  [MeroJob] Fetched {len(all_jobs)} jobs so far...")

        # Follow pagination — API gives a 'next' URL or None
        url = data.get("next")

    if all_jobs:
        save_merojob_data(all_jobs)
        print(f"[MeroJob] ✅ Done — {len(all_jobs)} total jobs scraped")
    else:
        print("[MeroJob] ⚠️  No jobs found. Check your internet or the API.")

    return len(all_jobs)


if __name__ == "__main__":
    # When you run this file directly, it sets up DB and runs once
    from database import setup_database
    setup_database()
    count = scrape_jobs()
    print(f"\nFinished. {count} jobs saved to jobs.db")
