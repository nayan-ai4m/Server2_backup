import psycopg2
from psycopg2 import Error
from datetime import datetime
import time
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transfer_events.log'),
        logging.StreamHandler()
    ]
)

def connect_to_db(host, dbname, user, password, port=5432):
    """Establish connection to a PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=host,
            database=dbname,
            user=user,
            password=password,
            port=port
        )
        return conn
    except Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def fetch_new_records(source_conn, last_timestamp):
    """Fetch new records from source database since last timestamp."""
    try:
        cursor = source_conn.cursor()
        query = """
        SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark, 
               resolution_time, filename, event_type, alert_type, assigned_time, acknowledge
        FROM event_table
        WHERE timestamp > %s
        ORDER BY timestamp
        """
        cursor.execute(query, (last_timestamp,))
        records = cursor.fetchall()
        cursor.close()
        return records
    except Error as e:
        logging.error(f"Error fetching records: {e}")
        return []

def insert_records(target_conn, records):
    """Insert records into target database."""
    try:
        cursor = target_conn.cursor()
        insert_query = """
        INSERT INTO event_table (
            timestamp, event_id, zone, camera_id, assigned_to, action, remark, 
            resolution_time, filename, event_type, alert_type, assigned_time, acknowledge
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, records)
        target_conn.commit()
        cursor.close()
        return len(records)
    except Error as e:
        logging.error(f"Error inserting records: {e}")
        target_conn.rollback()
        return 0

def get_latest_timestamp(target_conn):
    """Get the latest timestamp from the target database."""
    try:
        cursor = target_conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM event_table")
        result = cursor.fetchone()[0]
        cursor.close()
        return result if result else datetime.min
    except Error as e:
        logging.error(f"Error fetching latest timestamp: {e}")
        return datetime.min

def main():
    # Source database (Server-2) connection details
    source_db_config = {
        'host': '192.168.1.168',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'port': 5432
    }

    # Target database (Server-1) connection details
    target_db_config = {
        'host': '192.168.1.149',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'port': 5432
    }

    # Polling interval in seconds
    polling_interval = 2

    # Keep track of connections
    source_conn = None
    target_conn = None

    while True:
        try:
            # Connect to databases if not already connected
            if source_conn is None or source_conn.closed:
                source_conn = connect_to_db(**source_db_config)
                if source_conn is None:
                    logging.error("Failed to connect to source database, retrying...")
                    time.sleep(polling_interval)
                    continue

            if target_conn is None or target_conn.closed:
                target_conn = connect_to_db(**target_db_config)
                if target_conn is None:
                    logging.error("Failed to connect to target database, retrying...")
                    time.sleep(polling_interval)
                    continue

            # Get the latest timestamp from target database
            last_timestamp = get_latest_timestamp(target_conn)
            logging.info(f"Last timestamp in target database: {last_timestamp}")

            # Fetch new records from source database
            new_records = fetch_new_records(source_conn, last_timestamp)

            if new_records:
                # Insert new records into target database
                inserted_count = insert_records(target_conn, new_records)
                logging.info(f"Successfully transferred {inserted_count} new records")
            else:
                logging.info("No new records to transfer")

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            # Close connections to force reconnection on next iteration
            if source_conn and not source_conn.closed:
                source_conn.close()
            if target_conn and not target_conn.closed:
                target_conn.close()
            source_conn = None
            target_conn = None

        # Wait before the next iteration
        time.sleep(polling_interval)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Script terminated by user")
        # Close connections on exit
        if 'source_conn' in locals() and source_conn and not source_conn.closed:
            source_conn.close()
        if 'target_conn' in locals() and target_conn and not target_conn.closed:
            target_conn.close()
