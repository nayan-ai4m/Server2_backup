import sys
import os
import random
import psycopg2
import json
import time
import datetime
from paho.mqtt import client as mqtt_client

class MQTTDatabaseHandler:
    def __init__(self, broker, port, db_config, topic="iotgatewayloop4"):
        self.broker = broker
        self.port = port
        self.db_config = db_config
        self.topic = topic
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.conn = psycopg2.connect(**db_config)
        self.mc27_data = {}
        self.mc30_data = {}
        self.last_message_time = time.time()
        self.timeout = 30  # Timeout in seconds for no data
        self.client = self.connect_mqtt()
    
    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
                self.subscribe()
            else:
                print(f"Failed to connect, return code {rc}")

        def on_disconnect(client, userdata, rc):
            print(f"Disconnected with return code {rc}")
            if rc != 0:
                print("Unexpected disconnection. Attempting to reconnect...")

        client = mqtt_client.Client(self.client_id)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = self.on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        
        try:
            client.connect(self.broker, self.port, keepalive=60)
        except Exception as e:
            print(f"Failed to connect to broker: {e}")
            self.reconnect()
        return client
    
    def reconnect(self):
        print("Attempting to reconnect to MQTT broker...")
        while True:
            try:
                self.client.reconnect()
                break
            except Exception as e:
                print(f"Reconnect failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def insert_data_into_table(self, table_name, data):
        try:
            # Handle blank values for text fields and NULL for numeric fields
            filtered_data = {}
            for key, value_dict in data.items():
                value = value_dict['value']
                if value == "":
                    # Check if the column is numeric (e.g., real, integer)
                    if self.is_numeric_column(table_name, key):
                        filtered_data[key] = None  # Insert NULL for numeric columns
                    else:
                        filtered_data[key] = ""  # Insert blank for text columns
                else:
                    filtered_data[key] = value
            
            if not filtered_data:
                print(f"No valid data to insert into {table_name}. Skipping.")
                return
            
            with self.conn.cursor() as cur:
                columns = ', '.join(filtered_data.keys())
                values_placeholder = ', '.join(['%s'] * len(filtered_data))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
                cur.execute(query, list(filtered_data.values()))
                self.conn.commit()
                print(f"Data inserted into {table_name} successfully:",filtered_data)
        except psycopg2.OperationalError as e:
            print(f"Database error: {e}")
            self.conn.rollback()
        except Exception as e:
            print(f"Error: {e} at line {sys.exc_info()[2].tb_lineno}")

    def is_numeric_column(self, table_name, column_name):
        """
        Check if a column in the table is numeric (e.g., real, integer).
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, column_name))
                result = cur.fetchone()
                if result:
                    data_type = result[0]
                    return data_type in ('integer', 'real', 'numeric', 'double precision', 'float')
                return False
        except Exception as e:
            print(f"Error checking column type: {e}")
            return False

    def process_item(self, item):
        split_id = item['id'].split('.')
        machine = split_id[1]
        new_id = split_id[2]
        return machine, new_id, {'value': item['value'], 'quality': item['quality'], 'ts': item['ts']}
    
    def on_message(self, client, userdata, msg):
        self.last_message_time = time.time()
        data = json.loads(msg.payload.decode())
        
        for item in data["values"]:
            machine, new_id, processed_value = self.process_item(item)
            if machine == 'mc27':
                self.mc27_data[new_id] = processed_value
            elif machine == 'mc30':
                self.mc30_data[new_id] = processed_value
        
        timestamp = {'value': str(datetime.datetime.now())}
        self.mc27_data["timestamp"] = timestamp
        self.mc30_data["timestamp"] = timestamp
        
        self.insert_data_into_table("mc27", self.mc27_data)
        self.insert_data_into_table("mc30", self.mc30_data)
    
    def subscribe(self):
        self.client.subscribe(self.topic)
        print(f"Subscribed to topic: {self.topic}")
    
    def check_data_flow(self):
        while True:
            if time.time() - self.last_message_time > self.timeout:
                print(f"No data received for {self.timeout} seconds. Reconnecting...")
                self.reconnect()
            time.sleep(5)
    
    def run(self):
        import threading
        threading.Thread(target=self.check_data_flow, daemon=True).start()
        self.client.loop_forever()

if __name__ == '__main__':
    db_config = {
        'host': 'localhost',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024'
    }
    mqtt_handler = MQTTDatabaseHandler('192.168.1.149', 1883, db_config)
    mqtt_handler.run()
