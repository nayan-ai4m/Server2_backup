import psycopg2
from psycopg2 import OperationalError
import csv
import os
import time
from datetime import datetime

# Database connection details
DB_HOST = "localhost"
DB_NAME = "hul"
DB_USER = "postgres"
DB_PASSWORD = "ai4m2024"
OUTPUT_DIR = "/tmp/"

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 1000

def get_db_connection():
    """Create database connection with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
        except OperationalError as e:
            if attempt == MAX_RETRIES - 1:
                print(f"Failed to connect to database after {MAX_RETRIES} attempts: {e}")
                raise
            print(f"Connection attempt {attempt + 1} failed, retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

def fetch_distinct_dates(cursor, table_name):
    """Fetch distinct dates"""
    query = f"SELECT DISTINCT DATE(timestamp) AS date FROM {table_name} ORDER BY date LIMIT 100"
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def save_data_for_date(cursor, table_name, date):
    """Save data for a specific date using batching"""
    try:
        start_timestamp = f"{date} 00:00:00+05:30"
        end_timestamp = f"{date} 23:59:59+05:30"
        
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        output_file = f"{OUTPUT_DIR}{table_name}_{date.strftime('%d_%m_%Y')}.csv"
        
        # Check if file already exists
        if os.path.exists(output_file):
            print(f"File {output_file} already exists, skipping...")
            return

        # Get column names
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        columns = [desc[0] for desc in cursor.description]
        
        # Use server-side cursor for efficient memory usage
        cursor.execute(f"DECLARE data_cursor CURSOR FOR SELECT * FROM {table_name} WHERE timestamp >= %s AND timestamp <= %s",
                      (start_timestamp, end_timestamp))
        
        # Open CSV file and write header
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            
            # Fetch and write data in batches
            while True:
                cursor.execute(f"FETCH {BATCH_SIZE} FROM data_cursor")
                rows = cursor.fetchall()
                if not rows:
                    break
                    
                writer.writerows(rows)
                time.sleep(0.1)  # Small delay to prevent overload
            
        cursor.execute("CLOSE data_cursor")
        print(f"Data for {date} saved to {output_file}")
        
    except Exception as e:
        print(f"Error saving data for date {date}: {e}")
        raise

def main(table_name):
    """Main function with error handling"""
    start_time = datetime.now()
    print(f"Starting export for table {table_name}")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        dates = fetch_distinct_dates(cursor, table_name)
        print(f"Found {len(dates)} distinct dates to process")
        
        for i, date in enumerate(dates, 1):
            try:
                print(f"Processing date {date} ({i}/{len(dates)})")
                save_data_for_date(cursor, table_name, date)
            except Exception as e:
                print(f"Failed to process date {date}: {e}")
                continue
                
        cursor.close()
        
    except Exception as e:
        print(f"Export failed: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Export completed in {duration}")

if __name__ == "__main__":
    table = "mc22"
    try:
        main(table)
    except Exception as e:
        print(f"Script failed: {e}")
        exit(1)
