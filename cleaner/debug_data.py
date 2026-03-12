import pandas as pd
import sqlite3

# Debug the data loading issue
try:
    conn = sqlite3.connect("jobs.db")
    print("Connected to database successfully")
    
    df = pd.read_sql("SELECT * FROM jobs_clean", conn)
    print(f"Loaded {len(df)} rows from database")
    
    conn.close()
    print("Database connection closed")
    
    # Process the DataFrame
    df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
    df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
    
    print(f"DataFrame shape: {df.shape}")
    print(f"DataFrame columns: {df.columns.tolist()}")
    print(f"DataFrame empty?: {df.empty}")
    
    if not df.empty:
        print("First few rows:")
        print(df.head())
    
except Exception as e:
    print(f"Error occurred: {e}")
    print(f"Error type: {type(e)}")