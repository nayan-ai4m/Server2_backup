import psycopg2
import json
import time
from datetime import datetime
from psycopg2.extras import RealDictCursor
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tp_history.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(file_path='config.json'):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config['database']
    except FileNotFoundError:
        logger.error(f"Config file {file_path} not found")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format in {file_path}")
        raise
    except KeyError:
        logger.error(f"'database' key not found in {file_path}")
        raise

DB_CONFIG = load_config()

TABLES = [
    f'mc{i}_tp_status' for i in range(17, 23)
] + [
    f'mc{i}_tp_status' for i in range(25, 31)
]

previous_state = {table: {f'tp{i:02d}': None for i in range(1, 71)} for table in TABLES}

def connect_db(db_params, dict_cursor=False):
    try:
        return psycopg2.connect(
            host=db_params['host'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password'],
            port=db_params['port'],
            connect_timeout=db_params['connect_timeout'],
            cursor_factory=RealDictCursor if dict_cursor else None
        )
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database {db_params['host']}: {e}")
        raise

def fetch_tp_status(conn, table):
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT * FROM {table}")
            return cur.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Error fetching data from {table}: {e}")
            conn.rollback()
            return []

def check_existing_history(conn, uuid, tp_column, machine):
    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT 1 FROM tp_history 
                WHERE uuid = %s AND tp_column = %s AND machine = %s
            """, (uuid, tp_column, machine))
            return cur.fetchone() is not None
        except psycopg2.Error as e:
            logger.error(f"Error checking existing history for {machine}.{tp_column} with UUID {uuid}: {e}")
            conn.rollback()
            return False

def insert_history(conn, uuid, tp_column, machine, timestamp, updated_time, filepath, colorcode):
    with conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO tp_history (uuid, tp_column, machine, timestamp, updated_time, filepath, colorcode)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (uuid, tp_column, machine, timestamp, updated_time, filepath, colorcode))
            conn.commit()
            return True
        except psycopg2.Error as e:
            logger.error(f"Error inserting into tp_history for {machine}.{tp_column} with UUID {uuid}: {e}")
            conn.rollback()
            return False

def main():
    try:
        conn = connect_db(DB_CONFIG, dict_cursor=True)
    except Exception as e:
        logger.error("Failed to establish database connection, exiting")
        sys.exit(1)
    
    try:
        while True:
            for table in TABLES:
                machine = table.split('_')[0].upper()
                
                rows = fetch_tp_status(conn, table)
                for row in rows:
                    for i in range(1, 71):
                        tp_column = f'tp{i:02d}'
                        try:
                            column_data = row.get(tp_column)
                            
                            if column_data is None:
                                continue
                            elif isinstance(column_data, dict):
                                data = column_data
                            elif isinstance(column_data, str):
                                try:
                                    data = json.loads(column_data)
                                except json.JSONDecodeError:
                                    logger.error(f"Error decoding JSON for {table}.{tp_column}: Invalid JSON string")
                                    continue
                            else:
                                logger.error(f"Error processing {table}.{tp_column}: Unexpected data type {type(column_data)}")
                                continue
                            
                            current_active = data.get('active', None)
                            uuid = data.get('uuid', None)
                            timestamp = data.get('timestamp', None)
                            updated_timestamp = data.get('updated_timestamp', None)
                            filepath = data.get('filepath', None)
                            colorcode = data.get('color_code', None)
                            
                            timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00')) if timestamp else None
                            updated_time_dt = datetime.fromisoformat(updated_timestamp.replace('Z', '+00:00')) if updated_timestamp else None
                            
                            # Use timestamp as updated_time if updated_timestamp is not present or None
                            if current_active == 0 and updated_time_dt is None and timestamp_dt is not None:
                                updated_time_dt = timestamp_dt
                            
                            # Insert if active is 0 and was previously 1 or None, and required fields are present
                            if (current_active == 0 and 
                                (previous_state[table][tp_column] == 1 or previous_state[table][tp_column] is None) and 
                                uuid and 
                                timestamp_dt and 
                                updated_time_dt):
                                if not check_existing_history(conn, uuid, tp_column, machine):
                                    success = insert_history(
                                        conn,
                                        uuid,
                                        tp_column,
                                        machine,
                                        timestamp_dt,
                                        updated_time_dt,
                                        filepath,
                                        colorcode
                                    )
                                    if success:
                                        logger.info(f"Inserted {table}.{tp_column} with UUID {uuid} into tp_history")
                            
                            previous_state[table][tp_column] = current_active
                            
                        except Exception as e:
                            logger.error(f"Error processing {table}.{tp_column}: {str(e)}")
                            continue
                            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        logger.info("Stopping monitoring...")
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()
