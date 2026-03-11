"""
clean_data.py — Data Cleaning & Transformation Pipeline
=========================================================
This script:
  1. Loads raw data from both merojob_raw and kumari_raw tables
  2. Standardizes column names and formats
  3. Cleans messy values (N/A, wrong formats, abbreviations)
  4. Merges both sources into one unified table: jobs_clean
  5. Saves the clean table back to the database

Run manually:  python clean_data.py
Called by:     scheduler.py (automatically after each scrape)
"""

import pandas as pd
import sqlite3
import re
from datetime import datetime

from database import load_raw_chunks

DB_PATH = "jobs.db"


# ══════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════

def clean_location(loc):
    """Standardizes location strings — maps abbreviations to full names."""
    if not isinstance(loc, str):
        return 'Unknown'
    loc = loc.strip()
    if not loc or loc.lower() in ('n/a', 'na', ''):
        return 'Unknown'
    # Map common abbreviations
    mapping = {
        'KTM': 'Kathmandu', 'Ktm': 'Kathmandu', 'ktm': 'Kathmandu',
        'Kathmandu Valley': 'Kathmandu',
        'PKR': 'Pokhara', 'PKR.': 'Pokhara', 'Pkr': 'Pokhara',
        'BKT': 'Bhaktapur', 'Bkt': 'Bhaktapur',
        'LLT': 'Lalitpur', 'Llt': 'Lalitpur', 'Patan': 'Lalitpur',
        'BRT': 'Birgunj', 'BTW': 'Butwal',
    }
    return mapping.get(loc, loc)


def extract_salary_min(salary_str):
    """
    Extracts the minimum salary from strings like:
      'Nrs. 20,000 - 30,000'  → 20000.0
      'Negotiable'             → None
      'N/A'                    → None
    """
    if not isinstance(salary_str, str):
        return None
    nums = re.findall(r'[\d,]+', salary_str)
    if nums:
        try:
            return float(nums[0].replace(',', ''))
        except ValueError:
            return None
    return None


def extract_salary_max(salary_str):
    """Extracts the maximum salary from salary strings."""
    if not isinstance(salary_str, str):
        return None
    nums = re.findall(r'[\d,]+', salary_str)
    if len(nums) > 1:
        try:
            return float(nums[1].replace(',', ''))
        except ValueError:
            return None
    return None


def standardize_job_level(level_str):
    """Maps various job level strings to 5 standard categories."""
    if not isinstance(level_str, str):
        return 'Not Specified'
    level_lower = level_str.lower()
    if any(w in level_lower for w in ['entry', 'junior', 'fresher', 'fresh', 'graduate']):
        return 'Entry Level'
    elif any(w in level_lower for w in ['mid', 'intermediate', 'associate']):
        return 'Mid Level'
    elif any(w in level_lower for w in ['senior', 'sr.', 'lead', 'principal']):
        return 'Senior Level'
    elif any(w in level_lower for w in ['manager', 'head', 'director', 'vp', 'chief', 'ceo', 'cto']):
        return 'Management'
    elif level_lower in ('n/a', 'na', '', 'not specified'):
        return 'Not Specified'
    else:
        return level_str.strip().title()


# ══════════════════════════════════════════════════════════════
#  CHUNK CLEANING HELPERS
#  These functions clean one chunk of raw data at a time.
#  Used by clean_and_merge() during chunked/lazy loading.
# ══════════════════════════════════════════════════════════════

def _clean_merojob_chunk(mero):
    """Cleans a single chunk (DataFrame) of raw MeroJob data."""
    mero_clean = pd.DataFrame(index=mero.index)
    mero_clean['source']      = 'merojob'
    mero_clean['job_id']      = 'mj_' + mero['id'].astype(str)
    mero_clean['title']       = mero['title'].str.strip().str.title()
    mero_clean['company']     = mero['company'].str.strip().fillna('Unknown Company')
    mero_clean['location']    = mero['location'].apply(clean_location)
    mero_clean['category']    = mero['categories'].fillna('Unknown').replace('', 'Unknown')
    mero_clean['job_level']   = mero['job_level'].apply(standardize_job_level)
    mero_clean['skills']      = mero['skills'].fillna('')
    mero_clean['salary_min']  = pd.to_numeric(mero['salary_min'], errors='coerce')
    mero_clean['salary_max']  = pd.to_numeric(mero['salary_max'], errors='coerce')
    mero_clean['currency']    = mero['currency'].fillna('NPR')
    mero_clean['deadline']    = pd.to_datetime(mero['deadline'], errors='coerce')
    mero_clean['scraped_at']  = pd.to_datetime(mero['scraped_at'], errors='coerce')
    mero_clean['job_url']     = mero['job_url']
    mero_clean['experience']  = 'N/A'   # MeroJob API doesn't return this field
    mero_clean['education']   = 'N/A'
    return mero_clean


def _clean_kumari_chunk(kumari):
    """Cleans a single chunk (DataFrame) of raw KumariJob data."""
    kumari_clean = pd.DataFrame(index=kumari.index)
    kumari_clean['source']      = 'kumarijob'
    kumari_clean['job_id']      = 'kj_' + kumari['job_id'].astype(str)
    kumari_clean['title']       = kumari['job_title'].str.strip().str.title()
    kumari_clean['company']     = kumari['company'].str.strip().fillna('Unknown Company')
    kumari_clean['location']    = 'Kathmandu'  # KumariJob is primarily KTM-based
    kumari_clean['category']    = kumari['industry'].fillna('Unknown').replace('', 'Unknown')
    kumari_clean['job_level']   = kumari['job_level'].apply(standardize_job_level)
    kumari_clean['skills']      = ''
    kumari_clean['salary_min']  = kumari['salary'].apply(extract_salary_min)
    kumari_clean['salary_max']  = kumari['salary'].apply(extract_salary_max)
    kumari_clean['currency']    = 'NPR'
    kumari_clean['deadline']    = pd.NaT
    kumari_clean['scraped_at']  = pd.to_datetime(kumari['scraped_at'], errors='coerce')
    kumari_clean['job_url']     = kumari['link']
    kumari_clean['experience']  = kumari['experience'].fillna('N/A')
    kumari_clean['education']   = kumari['education'].fillna('N/A')
    return kumari_clean


# ══════════════════════════════════════════════════════════════
#  MAIN CLEANING FUNCTION
# ══════════════════════════════════════════════════════════════

def clean_and_merge():
    """
    Main function: loads raw data in chunks → cleans each chunk →
    merges all chunks → saves jobs_clean table.

    Uses generator-based chunked loading (database.load_raw_chunks)
    to demonstrate lazy evaluation: only one chunk of raw data is
    held in memory at a time during the loading/cleaning phase.
    Each chunk is cleaned immediately before the next is fetched.

    Returns the cleaned DataFrame.
    """
    cleaned_frames = []

    # ── Load & clean MeroJob data in chunks (lazy evaluation) ──
    #    The generator yields one chunk at a time — only that chunk
    #    is in memory, not the entire merojob_raw table.
    print("  Loading MeroJob data in chunks (lazy evaluation)...")
    mero_chunk_count = 0
    mero_row_count   = 0
    for chunk in load_raw_chunks("merojob_raw", chunk_size=50):
        cleaned_chunk = _clean_merojob_chunk(chunk)
        cleaned_frames.append(cleaned_chunk)
        mero_chunk_count += 1
        mero_row_count   += len(chunk)
    print(f"  Processed {mero_row_count} MeroJob rows across {mero_chunk_count} chunks")

    # ── Load & clean KumariJob data in chunks (lazy evaluation) ─
    print("  Loading KumariJob data in chunks (lazy evaluation)...")
    kumari_chunk_count = 0
    kumari_row_count   = 0
    for chunk in load_raw_chunks("kumari_raw", chunk_size=50):
        cleaned_chunk = _clean_kumari_chunk(chunk)
        cleaned_frames.append(cleaned_chunk)
        kumari_chunk_count += 1
        kumari_row_count   += len(chunk)
    print(f"  Processed {kumari_row_count} KumariJob rows across {kumari_chunk_count} chunks")

    # ── Handle the case where both tables are empty ────────────
    if not cleaned_frames:
        print("⚠️  No raw data found. Run the scrapers first!")
        return pd.DataFrame()

    # ── Merge both sources ─────────────────────────────────────
    df = pd.concat(cleaned_frames, ignore_index=True)

    # ── Data Quality Steps ─────────────────────────────────────

    # 1. Remove rows with no title
    before = len(df)
    df = df[df['title'].notna() & (df['title'].str.strip() != '') & (df['title'] != 'N/A')]
    print(f"  Removed {before - len(df)} rows with missing title")

    # 2. Remove obvious duplicates (same title + company from same source)
    before = len(df)
    df = df.drop_duplicates(subset=['title', 'company', 'source'])
    print(f"  Removed {before - len(df)} duplicate job entries")

    # 3. Cap unrealistic salary values (above 10 million NPR = likely data error)
    df.loc[df['salary_min'] > 10_000_000, 'salary_min'] = None
    df.loc[df['salary_max'] > 10_000_000, 'salary_max'] = None

    # 4. Add a scrape_date column (date only, for easier grouping)
    df['scrape_date'] = df['scraped_at'].dt.date.astype(str)

    # 5. Convert datetime columns to strings for SQLite compatibility
    # Handle deadline column (may have NaT values)
    df['deadline'] = df['deadline'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '')
    
    # Handle scraped_at column
    df['scraped_at'] = df['scraped_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else '')

    # ── Save cleaned data to database ─────────────────────────
    conn = sqlite3.connect(DB_PATH)
    df.to_sql('jobs_clean', conn, if_exists='replace', index=False)
    conn.close()

    print(f"\n[SUCCESS] Cleaning complete!")
    print(f"   Total clean jobs: {len(df)}")
    print(f"   MeroJob: {len(df[df['source']=='merojob'])}")
    print(f"   KumariJob: {len(df[df['source']=='kumarijob'])}")
    print(f"   Saved to 'jobs_clean' table in jobs.db")

    return df


# ══════════════════════════════════════════════════════════════
#  WHEN RUN DIRECTLY: show a quick summary
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Running data cleaning pipeline...\n")
    df = clean_and_merge()

    if len(df) > 0:
        print("\n── Quick Summary ──────────────────────────────────")
        print(f"Columns: {list(df.columns)}")
        print(f"\nTop categories:")
        print(df['category'].value_counts().head(8).to_string())
        print(f"\nTop locations:")
        print(df['location'].value_counts().head(6).to_string())
        print(f"\nSalary stats (NPR):")
        print(df['salary_min'].describe().to_string())
