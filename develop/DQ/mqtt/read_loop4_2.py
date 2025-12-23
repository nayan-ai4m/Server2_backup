import sys
import os
import random
import psycopg2
from paho.mqtt import client as mqtt_client
import json
import datetime

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

    def connect_mqtt(self):
        client = mqtt_client.Client(self.client_id)
        client.on_connect = self.on_connect
        client.connect(self.broker, self.port)
        return client

    def on_connect(self, client, userdata, flags, rc):
        print("Connected to MQTT Broker!" if rc == 0 else f"Failed to connect, return code {rc}")

    def insert_data_into_table(self, table_name, data):
        try:
            # Remove keys where all values are None or empty
            filtered_data = {k: v for k, v in data.items() if any(v.values())}

            if not filtered_data:
                print(f"No valid data to insert into {table_name}. Skipping.")
                return

            with self.conn.cursor() as cur:
                columns = ', '.join(filtered_data.keys())
                values = tuple(cols['value'] if cols['value'] != '' else None for cols in filtered_data.values())

                print(f"Inserting into {table_name}:")
                print(f"Columns: {columns}")
                print(f"Values: {values}")

                query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(filtered_data))})"
                cur.execute(query, values)
                self.conn.commit()
                print(f"Data inserted into {table_name} successfully.")
        except Exception as e:
            print(f"Error: {e} at {sys.exc_info()[2].tb_lineno}")

    def process_item(self, item):
        split_id = item['id'].split('.')
        machine = split_id[1]
        new_id = split_id[2]
        return machine, new_id, {'value': item['value'], 'quality': item['quality'], 'ts': item['ts']}

    def subscribe(self, client):
        def on_message(client, userdata, msg):
            data = json.loads(msg.payload.decode())

            for item in data["values"]:
                machine, new_id, processed_value = self.process_item(item)

                if machine == 'mc27':
                    self.mc27_data[new_id] = processed_value
                elif machine == 'mc30':
                    self.mc30_data[new_id] = processed_value

            timestamp = {'value': str(datetime.datetime.now())}  # Ensure timestamp is valid
            self.mc27_data["timestamp"] = timestamp
            self.mc30_data["timestamp"] = timestamp

            self.insert_data_into_table("mc27", self.mc27_data)
            self.insert_data_into_table("mc30", self.mc30_data)

        client.subscribe(self.topic)
        client.on_message = on_message

    def run(self):
        client = self.connect_mqtt()
        self.subscribe(client)
        client.loop_forever()

if __name__ == '__main__':
    db_config = {
        'host': 'localhost',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024'
    }
    mqtt_handler = MQTTDatabaseHandler('192.168.1.149', 1883, db_config)
    mqtt_handler.run()

