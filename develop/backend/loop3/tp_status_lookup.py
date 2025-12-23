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
POLL_INTERVAL = 60

# List of machine tables to monitor
MACHINE_TABLES = [f"mc{i}_tp_status" for i in range(17, 23)] + [f"mc{i}_tp_status" for i in range(25, 31)]

# Store previous state of JSONB columns for each table
previous_states = {table: {} for table in MACHINE_TABLES}

def connect_db():
    """Establish a connection to the PostgreSQL database with RealDictCursor."""
    return psycopg2.connect(**DB_PARAMS, cursor_factory=psycopg2.extras.RealDictCursor)

def fetch_tp_status(conn, table_name):
    """Fetch all JSONB columns from the specified table."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            return cur.fetchall()  # Return all rows
    except Exception as e:
        print(f"Error fetching from {table_name}: {e}")
        return []

def insert_into_lookup(conn, uuid, tp_column, machine, timestamp, active, updated_time):
    """Insert a record into tp_status_lookup table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tp_status_lookup (uuid, tp_column, machine, timestamp, active, updated_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (uuid, tp_column, machine, timestamp, active, updated_time)
            )
        conn.commit()
    except Exception as e:
        print(f"Error inserting into tp_status_lookup: {e}")
        conn.rollback()

def process_changes(current_rows, previous_state, table_name, conn):
    """Process changes in JSONB columns and log to tp_status_lookup if needed."""
    # Extract machine name from table_name (e.g., mc17 from mc17_tp_status)
    machine = table_name.split('_')[0]

    for row in current_rows:
        # Get all columns starting with 'tp'
        columns = [col for col in row.keys() if col.startswith("tp")]

        for col in columns:
            current_data = row[col]
            previous_data = previous_state.get(col)

            # Skip if current data is None or empty
            if not current_data:
                continue

            # Parse JSONB data
            current_json = json.loads(current_data) if isinstance(current_data, str) else current_data

            # Initialize previous state if not exists
            if col not in previous_state:
                previous_state[col] = current_json
                continue

            previous_json = previous_state[col]

            # Check if active is 1 in both previous and current, and uuid or timestamp has changed
            if (previous_json.get("active") == 1 and current_json.get("active") == 1 and (
                previous_json.get("uuid") != current_json.get("uuid") or
                previous_json.get("timestamp") != current_json.get("timestamp")
            )):
                # Log previous state to tp_status_lookup
                insert_into_lookup(
                    conn=conn,
                    uuid=previous_json.get("uuid"),
                    tp_column=col,
                    machine=machine,
                    timestamp=previous_json.get("timestamp"),
                    active=previous_json.get("active"),
                    updated_time=previous_json.get("updated_timestamp")  # Use updated_timestamp from JSONB
                )

            # Update previous state
            previous_state[col] = current_json

def main():
    """Main function to run the monitoring loop."""
    conn = connect_db()
    
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
                conn = connect_db()  # Reconnect on error
    except KeyboardInterrupt:
        print("Stopping the script...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
