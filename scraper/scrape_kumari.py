"""
scrape_kumari.py  — MODIFIED VERSION
Changes from original:
  - Saves to SQLite database instead of CSV files
  - scrape_kumari_jobs() returns the jobs_map dict instead of writing CSV
  - All scraping logic is identical — only the output section changed
"""

import requests
import sys
import time
from bs4 import BeautifulSoup
from datetime import datetime

# Import our central database module
from cleaner.database import save_kumari_data


def scrape_kumari_jobs():
    """
    Scrapes all job listings from KumariJob website and saves them
    to the SQLite database. Returns the number of jobs saved.
    """
    url = "https://www.kumarijob.com/"
    print(f"\n[KumariJob] Scraping started at {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        print(f"  [KumariJob] Fetching main page...")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        job_cards = soup.find_all(attrs={"data-jobid": True})
        print(f"  [KumariJob] Found {len(job_cards)} job cards on homepage")

        jobs_map = {}

        for card in job_cards:
            try:
                job_id    = card.get('data-jobid')
                job_title = "N/A"
                company   = "N/A"
                job_link  = "N/A"

                # Strategy 1: Standard card (h5/h6 layout)
                if card.find('h5'):
                    title_tag = card.find('h5')
                    job_title = title_tag.get_text(strip=True)
                    link_tag  = title_tag.find('a')
                    job_link  = link_tag['href'] if link_tag else "N/A"
                    company_tag = card.find('h6')
                    company   = company_tag.get_text(strip=True) if company_tag else "N/A"

                # Strategy 2: Featured / other card layout
                elif card.find(class_='job-info'):
                    title_elem = card.find(class_='job-info')
                    job_title  = title_elem.get_text(strip=True)
                    job_link   = title_elem['href'] if 'href' in title_elem.attrs else "N/A"
                    company_elem = card.find(class_='featured-job-company-name')
                    if company_elem:
                        company = company_elem.get_text(strip=True)
                    else:
                        logo_fig = card.find(class_='featured-job-company-logo')
                        if logo_fig:
                            img = logo_fig.find('img')
                            if img and 'alt' in img.attrs:
                                company = img['alt']

                # Skip if we have nothing useful
                if job_title == "N/A" and job_link == "N/A":
                    continue

                # Extract salary / experience from description list
                salary     = "N/A"
                experience = "N/A"
                desc_ul    = card.find('ul', class_='description')
                if desc_ul:
                    for li in desc_ul.find_all('li'):
                        text = li.get_text(strip=True)
                        if 'Year' in text or 'Fresher' in text:
                            experience = text
                        elif 'Nrs.' in text or 'Negotiable' in text:
                            salary = text

                # Merge duplicate job IDs (keep the entry with the most info)
                if job_id in jobs_map:
                    current = jobs_map[job_id]
                    if current['Company'] == "N/A" and company != "N/A":
                        current['Company'] = company
                    if current.get('Salary', 'N/A') == 'N/A' and salary != 'N/A':
                        current['Salary'] = salary
                    if current.get('Experience', 'N/A') == 'N/A' and experience != 'N/A':
                        current['Experience'] = experience
                else:
                    jobs_map[job_id] = {
                        'Job Title':  job_title,
                        'Company':    company,
                        'Link':       job_link,
                        'Salary':     salary,
                        'Experience': experience
                    }

            except AttributeError as e:
                continue

        print(f"  [KumariJob] Initial pass: {len(jobs_map)} unique jobs")

        # ── Deep Scrape: Visit each job detail page ────────────────────
        def fetch_with_retry(link, retries=3):
            for i in range(retries):
                try:
                    r = requests.get(link, headers=headers, timeout=10)
                    if r.status_code == 200:
                        return r
                    elif r.status_code == 429:
                        time.sleep(2 * (i + 1))
                except requests.RequestException:
                    time.sleep(1)
            return None

        print(f"  [KumariJob] Deep scraping {len(jobs_map)} job detail pages...")
        total  = len(jobs_map)
        count  = 0

        for job_id, job_data in jobs_map.items():
            count += 1
            link = job_data['Link']
            if link == "N/A":
                continue

            print(f"  [{count}/{total}] Fetching detail page...", end='\r')
            time.sleep(0.5)  # Be polite to the server

            resp = fetch_with_retry(link)
            if resp and resp.status_code == 200:
                detail_soup = BeautifulSoup(resp.content, 'html.parser')

                # Layout A: Premium info cards
                info_cards = detail_soup.find_all(class_='premium-info-card')
                if info_cards:
                    for card in info_cards:
                        t = card.find(class_='premium-info-card-title')
                        v = card.find(class_='premium-info-card-text')
                        if t and v:
                            title_text = t.get_text(strip=True)
                            val_text   = v.get_text(strip=True)
                            if title_text == 'Industry':         job_data['Industry']          = val_text
                            elif title_text == 'Job Level':      job_data['Job Level']         = val_text
                            elif title_text == 'Education':      job_data['Education']         = val_text
                            elif title_text == 'Desired Candidate': job_data['Desired Candidate'] = val_text
                            elif title_text == 'Experience':     job_data['Experience']        = val_text

                # Layout B: Basic/standard list
                else:
                    detail_box = detail_soup.find('ul', class_='job-detail-box')
                    if detail_box:
                        for li in detail_box.find_all('li', class_='row'):
                            left  = li.find('span', class_='basic-item__left')
                            right = li.find('span', class_='basic-item__right')
                            if left and right:
                                label = left.get_text(strip=True)
                                value = " ".join(right.get_text(strip=True).split())
                                if 'Industry'  in label: job_data['Industry']  = value
                                elif 'Job Level' in label: job_data['Job Level'] = value
                                elif 'Education' in label: job_data['Education'] = value
                                elif 'Desired'   in label: job_data['Desired Candidate'] = value
                                elif 'Experience' in label: job_data['Experience'] = value

        print(f"\n  [KumariJob] Deep scraping complete")

        # ── Save to database ───────────────────────────────────────────
        save_kumari_data(jobs_map)
        print(f"[KumariJob] ✅ Done — {len(jobs_map)} jobs saved")
        return len(jobs_map)

    except requests.exceptions.RequestException as e:
        print(f"[KumariJob] ❌ Network error: {e}")
        return 0
    except Exception as e:
        print(f"[KumariJob] ❌ Unexpected error: {e}")
        return 0


if __name__ == "__main__":
    from database import setup_database
    setup_database()
    count = scrape_kumari_jobs()
    print(f"\nFinished. {count} jobs saved to jobs.db")
