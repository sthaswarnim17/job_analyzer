import sqlite3
import os

db_path = "jobs.db" 
print(f"Database file exists: {os.path.exists(db_path)}")

if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"Tables in database: {tables}")
    
    # Check if jobs_clean table exists and has data
    if ('jobs_clean',) in tables:
        cursor.execute("SELECT COUNT(*) FROM jobs_clean")
        count = cursor.fetchone()[0]
        print(f"Records in jobs_clean table: {count}")
        
        if count > 0:
            cursor.execute("SELECT * FROM jobs_clean LIMIT 3")
            sample_data = cursor.fetchall()
            print("Sample data:", sample_data)
    else:
        print("jobs_clean table not found")
    
    conn.close()
else:
    print("Database file not found!")