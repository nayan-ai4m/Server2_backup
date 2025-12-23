import sys
import os
import random
import psycopg2
import json
import time
import datetime
from paho.mqtt import client as mqtt_client

class MQTTDatabaseHandler:
    def __init__(self):
        self.broker = '192.168.1.149'
        self.port = 1883
        self.topic = "loop4"
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.conn = psycopg2.connect(host="localhost", dbname="hul", user="postgres", password="ai4m2024")
        self.mc25_data = {}
        self.mc26_data = {}
        self.last_message_time = time.time()
        self.timeout = 30
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
    
    def process_item_as_key(self, item):
        split_id = item['id'].split('.')
        return split_id[1], split_id[2], {'value': item['value'], 'quality': item['quality'], 'ts': item['ts']}
    
    def insert_data_into_table(self, table_name, data):
        try:
            with self.conn.cursor() as cur:
                columns = ', '.join(data.keys())
                values_placeholder = ', '.join(['%s'] * len(data))
                
                values = []
                for key, item in data.items():
                    if key == 'timestamp':
                        values.append(item['value'])
                    else:
                        val = item['value']
                        if val == '':
                            values.append(None)
                        else:
                            try:
                                values.append(float(val))
                            except (ValueError, TypeError):
                                values.append(val)
                
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) RETURNING *"
                cur.execute(query, values)
                inserted_row = cur.fetchone()
                self.conn.commit()
                print(f"Data inserted successfully into {table_name}: {inserted_row}")
        except psycopg2.OperationalError as e:
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno)
            self.conn.rollback()
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()
    
    def on_message(self, client, userdata, msg):
        self.last_message_time = time.time()
        data = json.loads(msg.payload.decode())
        for item in data["values"]:
            machine, new_id, processed_value = self.process_item_as_key(item)
            if machine == 'mc25':
                self.mc25_data[new_id] = processed_value
            elif machine == 'mc26':
                self.mc26_data[new_id] = processed_value
        
        timestamp = {'value': str(datetime.datetime.now())}
        self.mc25_data["timestamp"] = timestamp
        self.mc26_data["timestamp"] = timestamp
        
        self.insert_data_into_table("mc25", self.mc25_data)
        self.insert_data_into_table("mc26", self.mc26_data)
        time.sleep(1)
    
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
    MQTTDatabaseHandler().run()
