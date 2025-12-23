from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
import uuid
import time

class StoppageConsumer:
    def __init__(self):
        self.db_config = {
            'host': '192.168.1.168',
            'database': 'hul',
            'user': 'postgres',
            'password': 'ai4m2024'
        }

        self.kafka_broker = '192.168.1.168:9092'
        self.kafka_topic = 'l3_stoppage_code'

        # Add machines
        self.machine_status = {
            **{f'mc{mc}': None for mc in range(17, 23)},
            "l3_case_erector": None,
            "l3_taping": None,
            "l3_check_weigher": None
        }

        # Map machines to their respective table names
        self.machine_table_map = {
            **{f'mc{mc}': f"mc{mc}_tp_status" for mc in range(17, 23)},
            "l3_case_erector": "case_erector_tp_status",
            "l3_taping": "tpmc_tp_status",
            "l3_check_weigher": "check_weigher_tp_status"
        }

        self.db_conn = None
        self.consumer = None
        self.connect_db()
        self.connect_kafka()

        self.select_query = "SELECT tp01 FROM {} LIMIT 1"
        self.insert_query = "INSERT INTO {} (tp01) VALUES (%s)"
        self.update_query = "UPDATE {} SET tp01 = %s"

    def connect_db(self):
        while True:
            try:
                if self.db_conn and not self.db_conn.closed:
                    self.db_conn.close()
                self.db_conn = psycopg2.connect(**self.db_config)
                print("Successfully connected to PostgreSQL database")
                return
            except Exception as e:
                print(f"Database connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def connect_kafka(self):
        while True:
            try:
                self.consumer = KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=self.kafka_broker,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                )
                print(f"Connected to Kafka topic {self.kafka_topic}")
                return
            except Exception as e:
                print(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def ensure_db_connection(self):
        try:
            if self.db_conn is None or self.db_conn.closed:
                print("Database connection lost. Reconnecting...")
                self.connect_db()
            return True
        except Exception as e:
            print(f"Error checking database connection: {e}")
            return False

    def handle_status_change(self, machine, status_code):
        if not self.ensure_db_connection():
            print("Cannot proceed without database connection")
            return

        cursor = None
        try:
            table_name = self.machine_table_map.get(machine)
            if not table_name:
                print(f"No table mapping found for {machine}")
                return

            cursor = self.db_conn.cursor()
            cursor.execute(self.select_query.format(table_name))
            record = cursor.fetchone()

            current_json = {}

            if record and record[0]:
                if isinstance(record[0], str):
                    try:
                        current_json = json.loads(record[0])
                    except json.JSONDecodeError:
                        current_json = {}
                elif isinstance(record[0], dict):
                    current_json = record[0]

            is_special = machine in ["l3_case_erector", "l3_taping", "l3_check_weigher"]

            new_data = {
                "uuid": current_json.get("uuid", str(uuid.uuid4())),
                "active": 0 if is_special and status_code != 0 else 1 if is_special else 1 if status_code != 0 else 0,
                "timestamp": datetime.now().isoformat(),
                "color_code": 1,
                "stoppages_code": 0 if is_special else str(status_code) if status_code != 0 else current_json.get("stoppages_code", "0")
            }

            if record:
                cursor.execute(self.update_query.format(table_name), (json.dumps(new_data),))
                print(f"Updated tp01 for {machine} with status {status_code}")
            else:
                cursor.execute(self.insert_query.format(table_name), (json.dumps(new_data),))
                print(f"Inserted row with tp01 for {machine} with status {status_code}")

            self.db_conn.commit()

        except Exception as e:
            print(f"Error processing {machine}: {e}")
            if cursor and not self.db_conn.closed:
                self.db_conn.rollback()
            self.connect_db()
        finally:
            if cursor:
                cursor.close()

    def process_messages(self):
        print(f"Listening for messages on topic '{self.kafka_topic}'...")
        while True:
            try:
                for message in self.consumer:
                    try:
                        data = message.value
                        #print(data)
                        if not isinstance(data, dict):
                            print(f"Unexpected message format: {data}")
                            continue

                        for machine, status_code in data.items():
                            status_code = int(status_code)
                            if machine in self.machine_status and self.machine_status[machine] != status_code:
                                self.handle_status_change(machine, status_code)
                                self.machine_status[machine] = status_code
                    except Exception as e:
                        print(f"Error processing message: {e}")
            except Exception as e:
                print(f"Kafka consumer error: {e}. Reconnecting...")
                self.connect_kafka()
                time.sleep(5)

    def run(self):
        try:
            self.process_messages()
        except KeyboardInterrupt:
            print("Shutting down gracefully...")
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
            if self.consumer:
                self.consumer.close()

if __name__ == '__main__':
    consumer = StoppageConsumer()
    consumer.run()
