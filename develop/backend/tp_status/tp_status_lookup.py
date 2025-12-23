import psycopg2
import psycopg2.extras
import json
import time
from datetime import datetime

# Database connection parameters
DB_PARAMS = {
    "dbname": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "host": "192.168.1.168",
    "port": "5432"
}

# Polling interval in seconds
POLL_INTERVAL = 10

# List of machine tables to monitor
MACHINE_TABLES = [f"mc{i}_tp_status" for i in range(17, 23)] + [f"mc{i}_tp_status" for i in range(25, 31)]

# Store previous state of JSONB columns for each table
previous_states = {table: {} for table in MACHINE_TABLES}

def connect_db():
    """Establish a connection to the PostgreSQL database with RealDictCursor."""
    try:
        return psycopg2.connect(**DB_PARAMS, cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def fetch_tp_status(conn, table_name):
    """Fetch all JSONB columns from the specified table."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()
            print(f"Fetched {len(rows)} rows from {table_name}")
            return rows
    except Exception as e:
        print(f"Error fetching from {table_name}: {e}")
        return []

def check_existing_record(conn, uuid, tp_column, machine):
    """Check if a record with the given uuid, tp_column, and machine exists."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM tp_status_lookup
                WHERE uuid = %s AND tp_column = %s AND machine = %s
                """,
                (uuid, tp_column, machine)
            )
            exists = cur.fetchone() is not None
            print(f"Checked existence of uuid={uuid}, tp_column={tp_column}, machine={machine}: {'exists' if exists else 'does not exist'}")
            return exists
    except Exception as e:
        print(f"Error checking existing record in tp_status_lookup: {e}")
        return False

def insert_into_lookup(conn, uuid, tp_column, machine, timestamp, active, filepath, colorcode, updated_time=None):
    """Insert a record into tp_status_lookup table if it doesn't already exist."""
    try:
        # Check if the record already exists
        if check_existing_record(conn, uuid, tp_column, machine):
            print(f"Skipping insert: Record with uuid={uuid}, tp_column={tp_column}, machine={machine} already exists")
            return

        print(f"Inserting into tp_status_lookup: uuid={uuid}, tp_column={tp_column}, machine={machine}, timestamp={timestamp}, active={active}, filepath={filepath}, colorcode={colorcode}, updated_time={updated_time}")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tp_status_lookup (uuid, tp_column, machine, timestamp, active, filepath, colorcode, updated_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (uuid, tp_column, machine, timestamp, active, filepath, colorcode, updated_time)
            )
        conn.commit()
        print(f"Successfully inserted record for uuid={uuid}")
    except Exception as e:
        print(f"Error inserting into tp_status_lookup: {e}")
        conn.rollback()

def process_changes(current_rows, previous_state, table_name, conn):
    """Process changes in JSONB columns and log to tp_status_lookup if needed."""
    machine = table_name.split('_')[0]
    print(f"Processing changes for table {table_name}")

    for row in current_rows:
        columns = [col for col in row.keys() if col.startswith("tp")]
        for col in columns:
            current_data = row[col]
            previous_data = previous_state.get(col)

            if not current_data:
                print(f"Skipping {col} in {table_name}: current data is None or empty")
                continue

            try:
                current_json = json.loads(current_data) if isinstance(current_data, str) else current_data
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON for {col} in {table_name}: {e}")
                continue

            if col not in previous_state:
                print(f"Initializing state for {col} in {table_name}")
                previous_state[col] = current_json
                continue

            previous_json = previous_state[col]
            print(f"Comparing {col} in {table_name}: previous_uuid={previous_json.get('uuid')}, current_uuid={current_json.get('uuid')}")

            if (previous_json.get("active") == 1 and current_json.get("active") == 1 and (
                previous_json.get("uuid") != current_json.get("uuid") or
                previous_json.get("timestamp") != current_json.get("timestamp")
            )):
                insert_into_lookup(
                    conn=conn,
                    uuid=previous_json.get("uuid"),
                    tp_column=col,
                    machine=machine,
                    timestamp=previous_json.get("timestamp"),
                    active=previous_json.get("active"),
                    filepath=previous_json.get("filepath"),
                    colorcode=previous_json.get("color_code"),
                    updated_time=None
                )

            previous_state[col] = current_json

def main():
    """Main function to run the monitoring loop."""
    conn = connect_db()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    try:
        while True:
            try:
                for table_name in MACHINE_TABLES:
                    current_rows = fetch_tp_status(conn, table_name)
                    if current_rows:
                        process_changes(current_rows, previous_states[table_name], table_name, conn)
                    else:
                        print(f"No rows found in {table_name}")
                time.sleep(POLL_INTERVAL)
            except Exception as e:
                print(f"Error during processing: {e}")
                conn.close()
                conn = connect_db()
                if not conn:
                    print("Failed to reconnect to database. Exiting.")
                    break
    except KeyboardInterrupt:
        print("Stopping the script...")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()

