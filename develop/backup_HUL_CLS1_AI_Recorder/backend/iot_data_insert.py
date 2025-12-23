import requests
import json
import logging
import sys
import os
import psycopg2
from request_body import build_telemetry_request_body
import time
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DB_HOST = '192.168.4.11'
DB_NAME = 'sensor_db'
DB_USER = 'postgres'
DB_PASS = 'ai4m2024'

class APIClient:
    def __init__(self, auth_url, username, password,client_name):
        self.auth_url = auth_url
        self.username = username
        self.password = password
        self.token = None
        self.token_retrieved_time = None
        self.client_name = client_name
        self.device_id = None

    def get_jwt_token(self):
        try:
            payload = {
                'username': self.username,
                'password': self.password
            }
            response = requests.post(self.auth_url, json=payload)
            response.raise_for_status()

            self.token = response.json().get('token')
            if self.token:
                logging.info(f"Successfully retrieved JWT token for {self.client_name}")
                self.token_retrieved_time = time.time()
                return self.token
            else:
                logging.warning(f"Token not found in response for {self.client_name}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching JWT token for {self.client_name}: {e}")
            return None
        
    def refresh_token_if_needed(self):
        """Check if 40 minutes have passed since the last token retrieval and refresh if needed."""

        if self.token and self.token_retrieved_time:
            elapsed_time = time.time() - self.token_retrieved_time
            if elapsed_time >= 2400:  
                logging.info(f"40 minutes passed. Refreshing JWT token for {self.client_name}...")
                self.get_jwt_token()
        else:
            self.get_jwt_token()
        
    def get_device_id(self, asset_url, device_url):
        """Get new device ID using current token"""
        if self.token:
            asset_data = self.fetch_data(asset_url)
            if asset_data:
                asset_id = self.extract_entity_id(asset_data)
                if asset_id:
                    logging.info(f"Extracted Asset ID for {self.client_name}: {asset_id}")
                    device_data = self.fetch_data(device_url)
                    if device_data:
                        self.device_id = self.extract_entity_id(device_data)
                        logging.info(f"Got new Device ID for {self.client_name}: {self.device_id}")
                        return True
        return False

    def fetch_data(self, endpoint):
    
        self.refresh_token_if_needed()
        if not self.token:
            logging.error("No valid JWT token available. Please authenticate first.")
            return None

        headers = {
            'Authorization': f'Bearer {self.token}'
        }

        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {endpoint}: {e}")
            return None

    def fetch_device_telemetry(self, entity_group_id):
        """Fetch telemetry data for the specified device group ID."""
        self.refresh_token_if_needed()
        if not self.token:
            logging.error("No valid JWT token available. Please authenticate first.")
            return None

        url = "https://iedge360.cimcondigital.com/api/entitiesQuery/find"
        headers = {
            'X-authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        body = build_telemetry_request_body(entity_group_id)

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching device telemetry data: {e}")
            return None

    def extract_entity_id(self, data):
        """Extracts the 'id' from the entityGroup in the data."""
        if isinstance(data, list) and len(data) > 0:
            entity_id = data[0].get('id', {}).get('id')
            if entity_id:
                return entity_id
        return None

def connect_db():
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error("Database connection failed.")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f"{exc_type} {fname} {exc_tb.tb_lineno}: {e}")

def format_label(raw_label):
    if "Horizontal Sealer" in raw_label:
        machine_number = ''.join(filter(str.isdigit, raw_label))
        return f"CLS-1 VFFS-Machine No-{machine_number} Horizontal Servo Motor DE"
    elif "Vertical Sealer" in raw_label:
        machine_number = ''.join(filter(str.isdigit, raw_label))
        return f"CLS-1 VFFS-Machine No-{machine_number} Vertical Servo Motor DE"
    return raw_label

def is_record_present(conn, device_id, timestamp):
    """Check if the record with the given device_id and timestamp exists in the database."""
    query = """
        SELECT EXISTS (
            SELECT 1 
            FROM public.api_timeseries
            WHERE device_id = %s AND timestamp = %s AT TIME ZONE 'UTC'
        )
    """   
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (device_id, timestamp))
            result = cursor.fetchone()
            return result[0]  # True if record exists, False otherwise
    except Exception as e:
        logging.error(f"Error checking record presence for device_id: {device_id}, timestamp: {timestamp}: {e}")
        return False


def insert_device_data(conn, all_telemetry_data):
    """Insert telemetry data into the database."""
    try:
        with conn.cursor() as cursor:
            insert_metrics_query = """
                INSERT INTO public.api_timeseries (device_id, timestamp, x_rms_vel, y_rms_vel, z_rms_vel, x_rms_acl, y_rms_acl, z_rms_acl, spl_db, led_status, temp_c, label)
                VALUES (%s, %s AT TIME ZONE 'UTC', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            for data in all_telemetry_data:
                try:
                    cursor.execute(insert_metrics_query, (
                        data['entity_id'], data['timestamp'], data['x_rms_vel'], data['y_rms_vel'], data['z_rms_vel'],
                        data['x_rms_acl'], data['y_rms_acl'], data['z_rms_acl'], data['SPL_dB'], data['LED_status'],
                        data['temp_c'], data.get('label')
                    ))
                except Exception as e:
                    logging.error(f"Error inserting data for device_id: {data['entity_id']} and timestamp: {data['timestamp']}: {e}")
                    continue
        
        conn.commit()
        logging.info(f"Successfully inserted {len(all_telemetry_data)} telemetry records into the database.")
    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")
        raise


def main():
  
    clients = [
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulh.com", "user@123", "machine_1"),
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulharidwar.com", "User@123", "machine_17"),
        APIClient("https://iedge360.cimcondigital.com/api/auth/login", "user@hulharidwarConveyer.com", "User@123", "machine_25")
    ]

    target_numbers = {"17", "18", "19", "20", "21", "22", "25", "26", "27", "28", "29", "30"}
    start_time = time.time()

    for client in clients:
        if client.get_jwt_token():
            client.get_device_id(
                "https://iedge360.cimcondigital.com/api/entityGroups/ASSET",
                "https://iedge360.cimcondigital.com/api/entityGroups/DEVICE"
            )

    while True:
        try:
            current_time = time.time()
            if current_time - start_time >= 2400:
                logging.info("40 minutes passed, getting new tokens and device IDs...")
                start_time = current_time
                for client in clients:
                    if client.get_jwt_token():
                        client.get_device_id(
                            "https://iedge360.cimcondigital.com/api/entityGroups/ASSET",
                            "https://iedge360.cimcondigital.com/api/entityGroups/DEVICE"
                        )

            telemetry_data_buffer = []

            # Collect telemetry data from all clients
            conn = connect_db()
            if not conn:
                logging.error("Failed to connect to the database.")
                continue

            for client in clients:
                if client.token and client.device_id:
                    telemetry_data = client.fetch_device_telemetry(client.device_id)
                    if telemetry_data:
                        for item in telemetry_data['data']:
                            label = item['latest'].get('ENTITY_FIELD', {}).get('label', {}).get('value', "")
                            if label and any(num in label for num in target_numbers) and ("Machine No" in label or "M/C" in label):
                                entity_id = item['entityId']['id']
                                telemetry = item['latest'].get('TIME_SERIES', {})
                                raw_ts = telemetry.get('x_rms_vel', {}).get('ts', 0)

                                formatted_label = format_label(label)

                                if raw_ts:
                                    timestamp = datetime.fromtimestamp(raw_ts / 1000, tz=timezone.utc)
                                    itc_timestamp = timestamp + timedelta(hours=5, minutes=30)

                                    # Check if the record already exists
                                    if not is_record_present(conn, entity_id, itc_timestamp):
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
                                    else :
                                        logging.info(f"not appending data for {formatted_label}")

            print(f"Telemetry Data Buffer: {telemetry_data_buffer}")
            if telemetry_data_buffer:
                try:
                    insert_device_data(conn, telemetry_data_buffer)
                    logging.info(f"Successfully stored {len(telemetry_data_buffer)} records.")
                except Exception as e :
                    logging.error("Faild to store data")
                    
            conn.close()
            logging.info("sleeping for 30 sec")
            time.sleep(30)


        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            logging.info("sleeping for 30 sec")
            time.sleep(30)


if __name__ == "__main__":
    main()
    
    
    

