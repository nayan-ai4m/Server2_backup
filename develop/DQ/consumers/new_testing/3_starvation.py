import json
import sys
import uuid
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Any
from dataclasses import dataclass
from shapely.geometry import Polygon

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extensions import connection
from psycopg2.pool import SimpleConnectionPool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Constants
CONFIG = {
    'kafka': {
        'topic': 'loop3',
        'bootstrap_servers': ['localhost:9092'],
        'auto_offset_reset': 'latest'
    },
    'postgres': {
        'host': 'localhost',
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'min_connections': 1,
        'max_connections': 50
    },
    'zones': {
        'file_path': 'jamming_zones_v1.json',
        'status_file': 'zone_status.json'
    }
}

@dataclass
class ZoneThresholds:
    infeed_min: int = 2
    infeed_max: int = 3

class DatabaseManager:
    def __init__(self, config: dict):
        try:
            self.pool = SimpleConnectionPool(
                config['min_connections'],
                config['max_connections'],
                host=config['host'],
                dbname=config['dbname'],
                user=config['user'],
                password=config['password']
            )
            logging.info("Database connection pool initialized successfully.")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            sys.exit(1)

    def get_connection(self) -> connection:
        return self.pool.getconn()

    def return_connection(self, conn: connection):
        self.pool.putconn(conn)

    def insert_event(self, conn: connection, event_data: Tuple):
        query = """
            INSERT INTO public.sample_event_table(
                timestamp, event_id, zone, camera_id, event_type, alert_type, filename
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query, event_data)
                conn.commit()
                logging.info(f"Inserted event: {event_data}")
        except Exception as e:
            logging.error(f"Error inserting event: {e}")
            conn.rollback()

class ZoneManager:
    def __init__(self, zones_file: str, status_file: str):
        self.zones_file = zones_file
        self.status_file = status_file
        self.thresholds = ZoneThresholds()

    def load_status(self) -> Dict:
        try:
            with open(self.status_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Error loading zone status: {e}")
            return {}

    def update_status(self, status: Dict):
        try:
            with open(self.status_file, 'w') as f:
                json.dump(status, f, indent=4)
            logging.info("Updated zone status successfully.")
        except Exception as e:
            logging.error(f"Error saving zone status: {e}")

class ProductionMonitor:
    def __init__(self, config: dict):
        self.db = DatabaseManager(config['postgres'])
        self.zone_manager = ZoneManager(
            config['zones']['file_path'],
            config['zones']['status_file']
        )
        self.consumer = self._setup_kafka_consumer(config['kafka'])

    def _setup_kafka_consumer(self, config: dict) -> KafkaConsumer:
        try:
            consumer = KafkaConsumer(
                config['topic'],
                bootstrap_servers=config['bootstrap_servers'],
                auto_offset_reset=config['auto_offset_reset'],
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Kafka consumer initialized successfully.")
            return consumer
        except Exception as e:
            logging.error(f"Error setting up Kafka consumer: {e}")
            sys.exit(1)

    def _check_infeed_zone(self, zone_id: str, count: int, zone_status: Dict, sensor_id: str) -> None:
        conn = self.db.get_connection()
        try:
            current_time = datetime.now() + timedelta(hours=5, minutes=30, seconds=30)
            logging.info(f"Checking infeed zone {zone_id} with count {count}")

            if count < self.zone_manager.thresholds.infeed_min and zone_status.get(zone_id, {}).get("status", 0) in [0, 2]:
                logging.info(f"Zone {zone_id} triggered possible starvation event")
                zone_status[zone_id] = {"status": 1}
                self.db.insert_event(conn, (
                    str(current_time),
                    str(uuid.uuid4()),
                    sensor_id,
                    zone_id,
                    "possible starvation",
                    "productivity",
                    f"{current_time}.jpeg"
                ))

            elif count > self.zone_manager.thresholds.infeed_max and zone_status.get(zone_id, {}).get("status", 0) in [0, 1]:
                logging.info(f"Zone {zone_id} triggered possible jamming event")
                zone_status[zone_id] = {"status": 2}
                self.db.insert_event(conn, (
                    str(current_time),
                    str(uuid.uuid4()),
                    sensor_id,
                    zone_id,
                    "possible jamming",
                    "productivity",
                    f"{current_time}.jpeg"
                ))

        except Exception as e:
            logging.error(f"Error processing infeed zone {zone_id}: {e}")
        finally:
            conn.commit()
            self.db.return_connection(conn)


    def process_message(self, message: Dict[str, Any]):
        if "objects" not in message or "roi" not in message or "sensorId" not in message:
            logging.warning(f"Invalid message format: {message}")
            return

        logging.info(f"Processing message: {message}")
        objects = message["objects"]
        zones = message["roi"]
        sensor_id = message["sensorId"]
        zone_status = self.zone_manager.load_status()

        for zone_id, count in zones.items():
            if zone_id in self.zone_manager.polygons and sensor_id in ['25_26', '27_28', '29_30']:
                self._check_zone_conditions(zone_id, count, zone_status, sensor_id)

        self.zone_manager.update_status(zone_status)

    def run(self):
        try:
            logging.info("Starting Kafka consumer...")
            for message in self.consumer:
                self.process_message(message.value)
        except KeyboardInterrupt:
            logging.info("Consumer interrupted by user")
        except Exception as e:
            logging.error(f"Error in Kafka consumer: {e}")
        finally:
            self.consumer.close()
            logging.info("Kafka consumer closed")


if __name__ == "__main__":
    monitor = ProductionMonitor(CONFIG)
    monitor.run()

