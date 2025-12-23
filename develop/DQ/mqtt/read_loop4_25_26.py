import sys
import os
import random
import psycopg2
import json
import time
import datetime
from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading

class MQTTDatabaseHandler:
    def __init__(self):
        # MQTT configuration
        self.broker = '192.168.1.149'
        self.port = 1883
        self.topic = "loop4"
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        
        # Database configuration
        self.conn = psycopg2.connect(host="localhost", dbname="hul", user="postgres", password="ai4m2024")
        
        # Kafka configuration
        self.kafka_topic = 'l4_stoppage_code'
        self.kafka_bootstrap_servers = '192.168.1.149:9092'
        self.producer = None
        
        # Data storage
        self.mc25_data = {}
        self.mc26_data = {}
        
        # Status tracking
        self.last_message_time = time.time()
        self.timeout = 30
        self.mc25_status_queue = []
        self.mc26_status_queue = []
        self.queue_lock = threading.Lock()
        
        # Initialize connections
        self.client = self.connect_mqtt()
        self.connect_kafka()
    
    def connect_kafka(self):
        """Connect to Kafka with continuous retry"""
        while True:
            try:
                if self.producer:
                    self.producer.close()
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5
                )
                print(f"Successfully connected to Kafka at {self.kafka_bootstrap_servers}")
                return
            except Exception as e:
                print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
                time.sleep(5)
    
    def ensure_kafka_connection(self):
        """Ensure we have a valid Kafka connection"""
        try:
            if self.producer is None:
                print("Kafka connection lost. Reconnecting...")
                self.connect_kafka()
            return True
        except Exception as e:
            print(f"Error checking Kafka connection: {e}")
            return False
    
    def _handle_kafka_error(self, error, message, machine):
        print(message)
        with self.queue_lock:
            if machine == 'mc25':
                self.mc25_status_queue.insert(0, message['status'])
            else:
                self.mc26_status_queue.insert(0, message['status'])
            print(f"Message failed for {machine}: {error}")
            self.connect_kafka()
    
    def produce_kafka_messages(self):
        """Continuously process status codes from the queues and send to Kafka"""
        while True:
            try:
                if not self.ensure_kafka_connection():
                    continue
                
                mc25_message = None
                with self.queue_lock:
                    if self.mc25_status_queue:
                        mc25_message = {"mc25": self.mc25_status_queue.pop(0)} 
                if mc25_message:
                    try:
                        self.producer.send(
                            self.kafka_topic, 
                            value=mc25_message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, mc25_message, 'mc25'))
                    except Exception as e:
                        self._handle_kafka_error(e, mc25_message, 'mc25')
                
                mc26_message = None
                with self.queue_lock:
                    if self.mc26_status_queue:
                        mc26_message = {"mc26": self.mc26_status_queue.pop(0)}
                
                if mc26_message:
                    try:
                        self.producer.send(
                            self.kafka_topic, 
                            value=mc26_message,
                        ).add_errback(lambda e: self._handle_kafka_error(e, mc26_message, 'mc26'))
                    except Exception as e:
                        self._handle_kafka_error(e, mc26_message, 'mc26')
                
                time.sleep(0.001)  # Minimal sleep between checks
            except Exception as e:
                print(f"Unexpected error in Kafka producer: {e}")
                time.sleep(1)
    
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
                #print(f"Data inserted successfully into {table_name}: {inserted_row}")
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
        
        # Reset data dictionaries for new message
        self.mc25_data = {}
        self.mc26_data = {}
        
        for item in data["values"]:
            machine, new_id, processed_value = self.process_item_as_key(item)
            
            # Check for status codes and add to appropriate queue
            if 'status' in new_id.lower():
                with self.queue_lock:
                    if machine == 'mc25':
                        self.mc25_status_queue.append(processed_value['value'])
                    elif machine == 'mc26':
                        self.mc26_status_queue.append(processed_value['value'])
            
            # Store all data for database insertion
            if machine == 'mc25':
                self.mc25_data[new_id] = processed_value
            elif machine == 'mc26':
                self.mc26_data[new_id] = processed_value
        
        # Add timestamp and insert into database
        timestamp = {'value': str(datetime.datetime.now())}
        if self.mc25_data:
            self.mc25_data["timestamp"] = timestamp
            self.insert_data_into_table("mc25", self.mc25_data)
        if self.mc26_data:
            self.mc26_data["timestamp"] = timestamp
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
        # Start all background threads
        threads = [
            threading.Thread(target=self.check_data_flow, daemon=True),
            threading.Thread(target=self.produce_kafka_messages, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        # Start MQTT loop in main thread
        self.client.loop_forever()

if __name__ == '__main__':
    MQTTDatabaseHandler().run()
