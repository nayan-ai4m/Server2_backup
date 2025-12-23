import psycopg2
from datetime import datetime, timedelta
import csv
import sys

def get_last_two_weeks_data_in_chunks(
    output_file,
    chunk_size=10000,
    dbname="hul",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432"
):
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("Connected to database successfully")
        
        # First get the column names using a regular cursor
        with conn.cursor() as schema_cursor:
            schema_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'event_table' 
                ORDER BY ordinal_position;
            """)
            columns = [col[0] for col in schema_cursor.fetchall()]
            print(f"Found {len(columns)} columns")
        
        # Open CSV file and write headers
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            # Calculate date range for the last 2 weeks
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=14)
            
            # Now use a named cursor for the main data fetch
            with conn.cursor('fetch_cursor') as data_cursor:
                data_cursor.execute("""
                    SELECT *
                    FROM event_table 
                    WHERE DATE(timestamp) >= %s AND DATE(timestamp) <= %s
                    ORDER BY timestamp DESC;
                """, (start_date, end_date))
                
                total_rows = 0
                while True:
                    rows = data_cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    writer.writerows(rows)
                    total_rows += len(rows)
                    print(f"Processed {total_rows:,} rows", end='\r')
                
                print(f"\nCompleted! Total rows written: {total_rows:,}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

def main():
    # Generate filename with timestamp
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"event_table_data_{current_time}.csv"
    
    print(f"Starting data extraction to {output_file}")
    get_last_two_weeks_data_in_chunks(output_file)

if __name__ == "__main__":
    main()
