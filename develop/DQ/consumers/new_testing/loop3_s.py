import json
import sys
import uuid
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Any
from dataclasses import dataclass
from shapely.geometry import Polygon, Point
import time
from collections import defaultdict

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
    infeed_min: int = 1
    infeed_max: int = 3
    outfeed_min: int = 1
    outfeed_max: int = 2
    dropping_min: int = 1
    dropping_max: int = 2
    ecld_min: int = 4
    ecld_max: int = 4
    fcld_max: int = 3

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
        self.polygons = self._load_zone_polygons()
        self.thresholds = ZoneThresholds()
    
    def _load_zone_polygons(self) -> Dict[str, Polygon]:
        try:
            with open(self.zones_file, 'r') as f:
                data = json.load(f)

            polygons = {
                name_id: Polygon([
                    (int(coord) for coord in coords.split(';')[i:i+2])
                    for i in range(0, len(coords.split(';')), 2)
                ])
                for name_id, coords in data.items()
            }
            logging.info("Loaded zone polygons successfully.")
            return polygons
        except Exception as e:
            logging.error(f"Error loading zones: {e}")
            return {}

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

class ZoneTracker:
    def __init__(self, zone_id: str, thresholds: ZoneThresholds):
        self.zone_id = zone_id
        self.thresholds = thresholds
        self.history = []
        self.last_event_time = None
        
    def add_measurement(self, count: int, timestamp: float):
        self.history.append({
            'count': count,
            'timestamp': timestamp
        })
        # Keep only measurements from last 30 seconds
        current_time = timestamp
        self.history = [
            h for h in self.history 
            if current_time - h['timestamp'] <= 30
        ]
    
    def check_conditions(self, current_count: int, zone_status: Dict) -> Tuple[bool, str, int]:
        if not self.history:
            return False, "", 0
            
        # Check if count has been consistently below/above threshold for 30 seconds
        current_time = time.time()
        if len(self.history) < 2:  # Need at least 2 measurements
            return False, "", 0
            
        first_measurement = self.history[0]
        if current_time - first_measurement['timestamp'] < 30:
            return False, "", 0
            
        # Check conditions based on zone type
        zone_type = self._get_zone_type()
        
        if zone_type == "infeed":
            min_threshold = self.thresholds.infeed_min
            max_threshold = self.thresholds.infeed_max
        elif zone_type == "outfeed":
            min_threshold = self.thresholds.outfeed_min
            max_threshold = self.thresholds.outfeed_max
        elif zone_type == "dropping":
            min_threshold = self.thresholds.dropping_min
            max_threshold = self.thresholds.dropping_max
        elif zone_type == "ecld":
            min_threshold = self.thresholds.ecld_min
            max_threshold = self.thresholds.ecld_max
        elif zone_type == "fcld":
            min_threshold = 0
            max_threshold = self.thresholds.fcld_max
        else:
            return False, "", 0

        # Check if all measurements are consistently below min threshold
        all_below_min = all(h['count'] < min_threshold for h in self.history)
        all_above_max = all(h['count'] > max_threshold for h in self.history)
        
        current_status = zone_status.get(self.zone_id, {}).get("status", 0)
        
        if all_below_min and current_status in [0, 2]:
            return True, "possible starvation", 1
        elif all_above_max and current_status in [0, 1]:
            return True, "possible jamming", 2
            
        return False, "", 0
        
    def _get_zone_type(self) -> str:
        for zone_type in ["infeed", "outfeed", "dropping", "ecld", "fcld"]:
            if zone_type in self.zone_id:
                return zone_type
        return "unknown"

class EnhancedProductionMonitor:
    def __init__(self, config: dict):
        self.db = DatabaseManager(config['postgres'])
        self.zone_manager = ZoneManager(
            config['zones']['file_path'],
            config['zones']['status_file']
        )
        self.consumer = self._setup_kafka_consumer(config['kafka'])
        self.zone_trackers = {}
        self._initialize_zone_trackers()

    def _setup_kafka_consumer(self, config: dict) -> KafkaConsumer:
        try:
            consumer = KafkaConsumer(
                config['topic'],
                bootstrap_servers=config['bootstrap_servers'],
                auto_offset_reset=config['auto_offset_reset'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='production_monitor'
            )
            logging.info("Kafka consumer initialized successfully.")
            return consumer
        except Exception as e:
            logging.error(f"Kafka consumer initialization error: {e}")
            sys.exit(1)
        
    def _initialize_zone_trackers(self):
        # Initialize trackers for MC 17-22 zones
        mc_numbers = ['17', '18', '19', '20', '21', '22']  # Extended to include all machines
        zone_types = ['ecld', 'fcld', 'infeed', 'outfeed', 'dropping']
        
        for mc_num in mc_numbers:
            for zone_type in zone_types:
                zone_id = f"mc_{mc_num}_{zone_type}"
                self.zone_trackers[zone_id] = ZoneTracker(
                    zone_id=zone_id,
                    thresholds=self.zone_manager.thresholds
                )
                logging.info(f"Initialized tracker for zone: {zone_id}")

    def process_message(self, message: Dict[str, Any]):
        if "objects" not in message or "roi" not in message or "sensorId" not in message:
            logging.warning(f"Invalid message format: {message}")
            return

        logging.info(f"Processing message: {message}")
        zones = message["roi"]
        sensor_id = message["sensorId"]
        zone_status = self.zone_manager.load_status()

        # Modified to handle all relevant sensors
        if sensor_id in ['17_18', '19_20', '21_22']:  # Updated sensor list
            for zone_id, count in zones.items():
                if zone_id in self.zone_trackers:
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
    monitor = EnhancedProductionMonitor(CONFIG)
    monitor.run()
