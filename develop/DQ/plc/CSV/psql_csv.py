import psycopg2
from datetime import datetime
import csv
import sys

def get_todays_data_in_chunks(
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
                WHERE table_name = 'mc17' 
                ORDER BY ordinal_position;
            """)
            columns = [col[0] for col in schema_cursor.fetchall()]
            print(f"Found {len(columns)} columns")

        # Open CSV file and write headers
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            # Now use a named cursor for the main data fetch
            with conn.cursor('fetch_cursor') as data_cursor:
                data_cursor.execute("""
                    SELECT *
                    FROM mc17 
                    WHERE DATE(timestamp) = CURRENT_DATE
                    ORDER BY timestamp DESC;
                """)
                
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
    output_file = f"mc17_data_{current_time}.csv"
    
    print(f"Starting data extraction to {output_file}")
    get_todays_data_in_chunks(output_file)

if __name__ == "__main__":
    main()

