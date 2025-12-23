import psycopg2
import csv
import sys
from datetime import datetime

def create_table_if_not_exists(cursor, target_table, csv_file):
    """Create table based on CSV structure if it doesn't exist"""
    # Read CSV header
    with open(csv_file, 'r') as f:
        header = next(csv.reader(f))
    
    # Assuming timestamp column exists and should be timestamptz
    columns = []
    for col in header:
        if 'timestamp' in col.lower():
            columns.append(f"{col} TIMESTAMPTZ")
        else:
            columns.append(f"{col} TEXT")  # Default to TEXT, can be modified later
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        {', '.join(columns)}
    );
    """
    
    cursor.execute(create_table_sql)
    print(f"Table {target_table} structure ensured")

def import_csv_to_postgres(
    csv_file,
    target_table,
    dbname="postgres",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432",
    chunk_size=10000
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
        conn.autocommit = True  # For better performance
        print("Connected to database successfully")

        with conn.cursor() as cursor:
            # Create table if it doesn't exist
            create_table_if_not_exists(cursor, target_table, csv_file)
            
            # Count total lines in CSV for progress tracking
            with open(csv_file, 'r') as f:
                total_lines = sum(1 for _ in f) - 1  # Subtract 1 for header
            print(f"Found {total_lines:,} rows to import")

            # Use COPY command for efficient data loading
            with open(csv_file, 'r') as f:
                print("Starting data import...")
                cursor.copy_expert(f"""
                    COPY {target_table} FROM STDIN WITH (
                        FORMAT CSV,
                        HEADER true,
                        DELIMITER ','
                    )
                """, f)
                
            # Verify the import
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            final_count = cursor.fetchone()[0]
            print(f"\nImport completed! Records in table: {final_count:,}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

def main():
    # You can modify these parameters as needed
    csv_file = input("Enter the CSV file path (e.g., mc17_data_20250116_112840.csv): ").strip()
    target_table = input("Enter the target table name (default: mc17_import): ").strip() or "mc17_import"
    
    print(f"\nStarting import from {csv_file} to table {target_table}")
    import_csv_to_postgres(csv_file, target_table)

if __name__ == "__main__":
    main()

