import sys
import os
import random
import psycopg2
import json
import time
import datetime
from psycopg2 import sql
from paho.mqtt import client as mqtt_client

class MQTTDatabaseHandler:
    def __init__(self):
        self.broker = '192.168.1.149'
        self.port = 1883
        self.topic = "loop4"
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        self.conn = psycopg2.connect(host="localhost", dbname="hul", user="postgres", password="ai4m2024")
        self.mc25_data, self.mc26_data = {}, {}
        self.client = self.connect_mqtt()
    
    def connect_mqtt(self):
        client = mqtt_client.Client(self.client_id)
        client.on_connect = lambda c, u, f, rc: print("Connected to MQTT Broker!" if rc == 0 else f"Failed to connect, return code {rc}")
        client.connect(self.broker, self.port)
        return client
    
    def process_item_as_key(self, item):
        split_id = item['id'].split('.')
        if item['value'] == '':
            item['value'] = '0'
        return split_id[1], split_id[2], {'value': float(item['value']), 'quality': item['quality'], 'ts': item['ts']}
    
    
    def insert_data_into_table(self, table_name, data):
        try:
            with self.conn.cursor() as cur:
                columns = ', '.join(data.keys())
                values_placeholder = ', '.join(['%s'] * len(data))
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder}) RETURNING *"
                print(query)
                cur.execute(query, [cols['value'] for cols in data.values()])
                inserted_row = cur.fetchone()
                self.conn.commit()
                print(f"Data inserted successfully into {table_name}: {inserted_row}")
        except psycopg2.OperationalError as e:
            exc_type, _, exc_tb = sys.exc_info()
            print(exc_type, os.path.split(exc_tb.tb_frame.f_code.co_filename)[1], exc_tb.tb_lineno)
    
    def on_message(self, client, userdata, msg):
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
        self.client.on_message = self.on_message
    
    def run(self):
        self.subscribe()
        self.client.loop_forever()

if __name__ == '__main__':
    MQTTDatabaseHandler().run()

