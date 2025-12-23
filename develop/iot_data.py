import requests
import json
import logging
import sys
import os
import psycopg2
from psycopg2 import pool
import time
from datetime import datetime, timezone, timedelta
import pytz
import threading
import signal
from request_body import build_telemetry_request_body

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sensor_data_collector.log"),
        logging.StreamHandler()
    ]
)

DB_HOST = '100.96.244.68'
DB_NAME = 'sensor_db'
DB_USER = 'postgres'
DB_PASS = 'ai4m2024'

db_pool = None

class Watchdog:
    def __init__(self, timeout, handler_function=None):
        self.timeout = timeout
        self.handler_function = handler_function or self._default_handler
        self.timer = None
        self.operation_name = "Operation"

    def start(self, operation_name="Operation"):
        if self.timer is not None:
            self.timer.cancel()
        self.operation_name = operation_name
        self.timer = threading.Timer(self.timeout, self.handler_function)
        self.timer.daemon = True
        self.timer.start()

    def reset(self):
        self.start(self.operation_name)

    def stop(self):
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None

    def _default_handler(self):
        logging.error(f"WATCHDOG ALERT: {self.operation_name} timed out after {self.timeout} seconds. Process appears to be stuck.")
        os._exit(1)  # Force exit the process

class APIClient:
    def __init__(self, auth_url, username, password, client_name, timeout=15):
        self.auth_url = auth_url
        self.username = username
        self.password = password
        self.token = None
        self.token_retrieved_time = None
        self.client_name = client_name
        self.device_id = None
        self.timeout = timeout
        self.session = requests.Session()

    def __del__(self):
        try:
            self.session.close()
        except:
            pass

    def get_jwt_token(self):
        try:
            payload = {
                'username': self.username,
                'password': self.password
            }

            response = self.session.post(
                self.auth_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            self.token = response.json().get('token')
            if self.token:
                logging.info(f"Successfully retrieved JWT token for {self.client_name}")
                self.token_retrieved_time = time.time()
                return True
            else:
                logging.warning(f"Token not found in response for {self.client_name}")
                return False
        except requests.exceptions.Timeout:
            logging.error(f"Timeout fetching JWT token for {self.client_name}")
            return False
        except requests.exceptions.ConnectionError:
            logging.error(f"Connection error fetching JWT token for {self.client_name}")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching JWT token for {self.client_name}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error getting JWT token for {self.client_name}: {e}")
            return False

    def refresh_token_if_needed(self):
        if self.token and self.token_retrieved_time:
            elapsed_time = time.time() - self.token_retrieved_time
            if elapsed_time >= 2400:
                logging.info(f"40 minutes passed. Refreshing JWT token for {self.client_name}...")
                return self.get_jwt_token()
            return True
        else:
            return self.get_jwt_token()

    def get_device_id(self, asset_url, device_url):
        if not self.refresh_token_if_needed():
            logging.error(f"Cannot get device ID for {self.client_name}: No valid token")
            return False

        try:
            asset_data = self.fetch_data(asset_url)
            if not asset_data:
                logging.error(f"Failed to fetch asset data for {self.client_name}")
                return False

            asset_id = self.extract_entity_id(asset_data)
            if not asset_id:
                logging.error(f"Failed to extract asset ID for {self.client_name}")
                return False

            logging.info(f"Extracted Asset ID for {self.client_name}: {asset_id}")

            device_data = self.fetch_data(device_url)
            if not device_data:
                logging.error(f"Failed to fetch device data for {self.client_name}")
                return False

            self.device_id = self.extract_entity_id(device_data)
            if not self.device_id:
                logging.error(f"Failed to extract device ID for {self.client_name}")
                return False

            logging.info(f"Got new Device ID for {self.client_name}: {self.device_id}")
            return True
        except Exception as e:
            logging.error(f"Error getting device ID for {self.client_name}: {e}")
            return False

    def fetch_data(self, endpoint):
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                if not self.refresh_token_if_needed():
                    logging.error(f"No valid JWT token available for {self.client_name}")
                    return None

                headers = {'Authorization': f'Bearer {self.token}'}
                response = self.session.get(
                    endpoint,
                    headers=headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logging.error(f"Timeout fetching data from {endpoint} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None
            except requests.exceptions.ConnectionError:
                logging.error(f"Connection error fetching data from {endpoint} (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                logging.error(f"Request error fetching data from {endpoint} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None
            except Exception as e:
                logging.error(f"Unexpected error fetching data from {endpoint}: {e}")
                return None

    def fetch_device_telemetry(self, entity_group_id):
        if not self.refresh_token_if_needed():
            logging.error(f"No valid JWT token available for {self.client_name}. Cannot fetch telemetry.")
            return None

        url = "https://iedge360.cimcondigital.com/api/entitiesQuery/find"
        headers = {
            'X-authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        try:
            body = build_telemetry_request_body(entity_group_id)
            response = self.session.post(
                url,
                headers=headers,
                json=body,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            logging.error(f"Timeout fetching telemetry data for {self.client_name}")
            return None
        except requests.exceptions.ConnectionError:
            logging.error(f"Connection error fetching telemetry data for {self.client_name}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching telemetry data for {self.client_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error fetching telemetry data for {self.client_name}: {e}")
            return None

    def extract_entity_id(self, data):
        if isinstance(data, list) and len(data) > 0:
            entity_id = data[0].get('id', {}).get('id')
            if entity_id:
                return entity_id
        return None

def init_db_pool(min_conn=1, max_conn=5):
    global db_pool
    try:
        if db_pool is not None:
            try:
                db_pool.closeall()
            except:
                pass

        db_pool = pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=10
        )

        conn = db_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            db_pool.putconn(conn)
            logging.info(f"Connection pool initialized with {min_conn}-{max_conn} connections")
            return True
        except Exception as e:
            logging.error(f"Pool created but test query failed: {e}")
            try:
                db_pool.putconn(conn)
            except:
                pass
            return False
    except Exception as e:
        logging.error(f"Failed to initialize connection pool: {e}")
        return False

def get_connection():
    """Get a database connection from the pool."""
    global db_pool
    if db_pool is None:
        if not init_db_pool():
            return connect_db_direct()

    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return conn
    except Exception as e:
        logging.error(f"Error getting connection from pool: {e}")

        if init_db_pool():
            try:
                return db_pool.getconn()
            except:
                pass

        return connect_db_direct()

def release_connection(conn):
    global db_pool
    if db_pool is not None and conn is not None:
        try:
            db_pool.putconn(conn)
            return True
        except Exception as e:
            logging.error(f"Error returning connection to pool: {e}")
            try:
                conn.close()
            except:
                pass
            return False
    elif conn is not None:
        try:
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Error closing connection: {e}")
            return False
    return False

def connect_db_direct():
    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                connect_timeout=10
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()

            logging.info("Direct database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            logging.error(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return None
        except Exception as e:
            logging.error(f"Unexpected database connection error: {e}")
            return None

def format_label(raw_label):
    if "Horizontal Sealer" in raw_label:
        machine_number = ''.join(filter(str.isdigit, raw_label))
        return f"CLS-1 VFFS-Machine No-{machine_number} Horizontal Servo Motor DE"
    elif "Vertical Sealer" in raw_label:
        machine_number = ''.join(filter(str.isdigit, raw_label))
        return f"CLS-1 VFFS-Machine No-{machine_number} Vertical Servo Motor DE"
    return raw_label

def is_record_present(conn, device_id, timestamp):
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM public.api_timeseries
            WHERE device_id = %s AND timestamp = %s
        )
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (device_id, timestamp))
            result = cursor.fetchone()
            return result[0]
    except Exception as e:
        logging.error(f"Error checking record presence for device_id: {device_id}, timestamp: {timestamp}: {e}")
        conn.rollback()
        return False

def insert_device_data(conn, all_telemetry_data):
    if not all_telemetry_data:
        logging.info("No telemetry data to insert")
        return 0

    success_count = 0
    error_count = 0
    mc17_label = "CLS-1 VFFS-Machine No-17 Horizontal Servo Motor DE"
    mc22_label = "CLS-1 VFFS-Machine No-22 Horizontal Servo Motor DE"
    mc17_inserted = False
    mc22_data = None

    try:
        with conn.cursor() as cursor:
            insert_metrics_query = """
                INSERT INTO public.api_timeseries (device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, x_rms_acl, y_rms_acl, z_rms_acl, spl_db, led_status, temp_c, label)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_size = 50
            for i in range(0, len(all_telemetry_data), batch_size):
                batch = all_telemetry_data[i:i+batch_size]
                batch_start = time.time()

                for data in batch:
                    try:
                        cursor.execute(insert_metrics_query, (
                            data['entity_id'], data['timestamp'], data['x_rms_vel'], data['y_rms_vel'], data['z_rms_vel'],
                            data['x_rms_acl'], data['y_rms_acl'], data['z_rms_acl'], data['SPL_dB'], data['LED_status'],
                            data['temp_c'], data.get('label')
                        ))
                        success_count += 1

                        # Track if mc17 or mc22 data is inserted
                        if data.get('label') == mc17_label:
                            mc17_inserted = True
                        elif data.get('label') == mc22_label:
                            # Store the latest mc22 data (most recent timestamp)
                            if not mc22_data or data['timestamp'] > mc22_data['timestamp']:
                                mc22_data = data
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Error inserting data for device_id: {data['entity_id']} at {data['timestamp']}: {e}")
                        continue

                conn.commit()
                batch_end = time.time()
                logging.info(f"Batch insert: {len(batch)} records in {batch_end - batch_start:.2f} seconds")

            # Check if mc17 data was not inserted but mc22 data was
            if not mc17_inserted and mc22_data:
                logging.info(f"No new data inserted for {mc17_label}. Substituting with {mc22_label} data.")
                try:
                    cursor.execute(insert_metrics_query, (
                        mc22_data['entity_id'], mc22_data['timestamp'], mc22_data['x_rms_vel'], mc22_data['y_rms_vel'],
                        mc22_data['z_rms_vel'], mc22_data['x_rms_acl'], mc22_data['y_rms_acl'], mc22_data['z_rms_acl'],
                        mc22_data['SPL_dB'], mc22_data['LED_status'], mc22_data['temp_c'], mc17_label
                    ))
                    conn.commit()
                    success_count += 1
                    logging.info(f"Successfully substituted {mc22_label} data for {mc17_label} at {mc22_data['timestamp']}.")
                except Exception as e:
                    logging.error(f"Error substituting {mc22_label} data for {mc17_label}: {e}")
                    conn.rollback()
            elif mc17_inserted:
                logging.info(f"Data inserted for {mc17_label}. No substitution needed.")
            elif not mc22_data:
                logging.info(f"No new data for {mc22_label} to substitute for {mc17_label}.")

        logging.info(f"Database insertion complete. Success: {success_count}, Errors: {error_count}")
        return success_count
    except Exception as e:
        logging.error(f"Error during batch insert: {e}")
        try:
            conn.rollback()
        except:
            pass
        return success_count

def heartbeat_check():
    try:
        heartbeat_file = "sensor_collector_heartbeat.txt"
        with open(heartbeat_file, "w") as f:
            f.write(f"HEARTBEAT: {datetime.now().isoformat()}")
        return True
    except Exception as e:
        logging.error(f"Failed to write heartbeat: {e}")
        return False

def main():
    logging.info("Starting sensor data collection process")

    process_watchdog = Watchdog(600)
    process_watchdog.start("Main Process")

    init_db_pool()

    clients = [
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulh.com", "user@123", "machine_1"),
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulharidwar.com", "User@123", "machine_17"),
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulharidwarConveyer.com", "User@123", "machine_25")
    ]

    target_numbers = {"17", "18", "19", "20", "21", "22", "25", "26", "27", "28", "29", "30"}
    start_time = time.time()
    consecutive_failures = 0
    max_consecutive_failures = 5
    heartbeat_interval = 60
    last_heartbeat = time.time()

    for client in clients:
        try:
            operation_watchdog = Watchdog(60)
            operation_watchdog.start(f"Token Retrieval ({client.client_name})")

            token_success = client.get_jwt_token()
            operation_watchdog.stop()

            if token_success:
                operation_watchdog = Watchdog(60)
                operation_watchdog.start(f"Device ID Retrieval ({client.client_name})")

                client.get_device_id(
                    "https://iedge360.cimcondigital.com/api/entityGroups/ASSET",
                    "https://iedge360.cimcondigital.com/api/entityGroups/DEVICE"
                )
                operation_watchdog.stop()
        except Exception as e:
            logging.error(f"Failed to initialize client {client.client_name}: {e}")
            try:
                operation_watchdog.stop()
            except:
                pass

    while True:
        try:
            process_watchdog.reset()

            current_time = time.time()
            if current_time - last_heartbeat >= heartbeat_interval:
                heartbeat_check()
                last_heartbeat = current_time

            if current_time - start_time >= 2400:
                logging.info("40 minutes passed, getting new tokens and device IDs...")
                start_time = current_time

                for client in clients:
                    try:
                        operation_watchdog = Watchdog(60)
                        operation_watchdog.start(f"Token Refresh ({client.client_name})")

                        token_success = client.get_jwt_token()
                        operation_watchdog.stop()

                        if token_success:
                            operation_watchdog = Watchdog(60)
                            operation_watchdog.start(f"Device ID Refresh ({client.client_name})")

                            client.get_device_id(
                                "https://iedge360.cimcondigital.com/api/entityGroups/ASSET",
                                "https://iedge360.cimcondigital.com/api/entityGroups/DEVICE"
                            )
                            operation_watchdog.stop()
                    except Exception as e:
                        logging.error(f"Failed to refresh client {client.client_name}: {e}")
                        try:
                            operation_watchdog.stop()
                        except:
                            pass

            telemetry_data_buffer = []

            operation_watchdog = Watchdog(30)
            operation_watchdog.start("Database Connection")

            conn = None
            try:
                conn = get_connection()
                operation_watchdog.stop()

                if not conn:
                    logging.error("Failed to get database connection. Will retry in next iteration.")
                    time.sleep(5)
                    consecutive_failures += 1
                    continue
            except Exception as e:
                try:
                    operation_watchdog.stop()
                except:
                    pass
                logging.error(f"Database connection exception: {e}")
                time.sleep(5)
                consecutive_failures += 1
                continue

            collection_start_time = time.time()
            for client in clients:
                if client.token and client.device_id:
                    try:
                        operation_watchdog = Watchdog(60)
                        operation_watchdog.start(f"Telemetry Collection ({client.client_name})")

                        telemetry_data = client.fetch_device_telemetry(client.device_id)
                        operation_watchdog.stop()

                        if telemetry_data and 'data' in telemetry_data:
                            for item in telemetry_data['data']:
                                try:
                                    label = item['latest'].get('ENTITY_FIELD', {}).get('label', {}).get('value', "")
                                    if label and any(num in label for num in target_numbers) and ("Machine No" in label or "M/C" in label):
                                        entity_id = item['entityId']['id']
                                        telemetry = item['latest'].get('TIME_SERIES', {})
                                        raw_ts = telemetry.get('x_rms_vel', {}).get('ts', 0)

                                        formatted_label = format_label(label)

                                        if raw_ts:
                                            timestamp_sec = raw_ts / 1000
                                            utc_time = datetime.utcfromtimestamp(timestamp_sec)
                                            ist = pytz.timezone("Asia/Kolkata")
                                            itc_timestamp = utc_time.replace(tzinfo=pytz.utc).astimezone(ist)

                                            db_op_watchdog = Watchdog(15)
                                            db_op_watchdog.start("Record Check")

                                            record_exists = False
                                            try:
                                                record_exists = is_record_present(conn, entity_id, itc_timestamp)
                                                db_op_watchdog.stop()
                                            except Exception as e:
                                                try:
                                                    db_op_watchdog.stop()
                                                except:
                                                    pass
                                                logging.error(f"Error checking record: {e}")
                                                continue

                                            if not record_exists:
                                                telemetry_data_buffer.append({
                                                    'entity_id': entity_id,
                                                    'timestamp': itc_timestamp,
                                                    'x_rms_vel': telemetry.get('x_rms_vel', {}).get('value'),
                                                    'y_rms_vel': telemetry.get('y_rms_vel', {}).get('value'),
                                                    'z_rms_vel': telemetry.get('z_rms_vel', {}).get('value'),
                                                    'x_rms_acl': telemetry.get('x_rms_acl', {}).get('value'),
                                                    'y_rms_acl': telemetry.get('y_rms_acl', {}).get('value'),
                                                    'z_rms_acl': telemetry.get('z_rms_acl', {}).get('value'),
                                                    'SPL_dB': telemetry.get('SPL_dB', {}).get('value'),
                                                    'LED_status': telemetry.get('LED_status', {}).get('value'),
                                                    'temp_c': telemetry.get('temp_c', {}).get('value'),
                                                    'label': formatted_label
                                                })
                                            else:
                                                logging.debug(f"Record already exists for {formatted_label}")
                                except Exception as e:
                                    logging.error(f"Error processing telemetry item: {e}")
                                    continue
                    except Exception as e:
                        try:
                            operation_watchdog.stop()
                        except:
                            pass
                        logging.error(f"Error collecting telemetry for {client.client_name}: {e}")

            collection_end_time = time.time()
            logging.info(f"Telemetry collection completed in {collection_end_time - collection_start_time:.2f} seconds")
            logging.info(f"Collected {len(telemetry_data_buffer)} new records")

            if telemetry_data_buffer:
                try:
                    operation_watchdog = Watchdog(120)
                    operation_watchdog.start("Database Insertion")

                    success_count = insert_device_data(conn, telemetry_data_buffer)
                    operation_watchdog.stop()

                    if success_count > 0:
                        logging.info(f"Successfully stored {success_count} records.")
                        consecutive_failures = 0
                    else:
                        logging.warning("No records were successfully stored.")
                        consecutive_failures += 1
                except Exception as e:
                    try:
                        operation_watchdog.stop()
                    except:
                        pass
                    logging.error(f"Failed to store data: {e}")
                    consecutive_failures += 1
            else:
                logging.info("No new data to insert")

            if conn:
                try:
                    release_connection(conn)
                except Exception as e:
                    logging.error(f"Error releasing database connection: {e}")

            if consecutive_failures >= max_consecutive_failures:
                logging.critical(f"Too many consecutive failures ({consecutive_failures}). Restarting process.")
                os._exit(1)

            logging.info("Data collection cycle completed. Sleeping for 30 seconds.")
            time.sleep(30)

        except Exception as e:
            logging.error(f"Critical error in main loop: {e}")
            consecutive_failures += 1
            time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Fatal error in main program: {e}")
        sys.exit(1)
