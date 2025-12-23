import json
import sys
import uuid
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Any
from collections import deque
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



# Constants
SAVE_DIR = "/home/ai4m/develop/backup/develop/DQ/consumers/images"
os.makedirs(SAVE_DIR, exist_ok=True)

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
        'file_path': 'jamming_zones_v1_demo.json',
        'status_file': 'zone_status_loop3_demo.json'
    }
}

@dataclass
class ZoneThresholds:
    infeed_min: int = 0
    infeed_max: int = 3
    outfeed_min: int = 0
    outfeed_max: int = 2
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
            print("Database connection pool initialized.")
        except Exception as e:
            print(f"Database connection error: {e}")
            sys.exit(1)

    def get_connection(self) -> connection:
        return self.pool.getconn()

    def return_connection(self, conn: connection):
        self.pool.putconn(conn)

    def insert_event(self, conn: connection, event_data: Tuple):
        query = """
            INSERT INTO public.event_table(
                timestamp, event_id, zone, camera_id, event_type, alert_type, filename
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query, event_data)
                conn.commit()
                print(f"Inserted event: {event_data}")
        except Exception as e:
            print(f"Error inserting event: {e}")
            conn.rollback()

class ZoneManager:
    def __init__(self, zones_file: str, status_file: str):
        self.zones_file = zones_file
        self.status_file = status_file
        self.polygons = self._load_zone_polygons()
        self.thresholds = ZoneThresholds()
        # Cache for status to reduce file operations
        self._status_cache = self.load_status()
        self._last_update_time = 0
    
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
            print("Loaded zone polygons.")
            return polygons
        except Exception as e:
            print(f"Error loading zones: {e}")
            return {}

    def load_status(self) -> Dict:
        try:
            with open(self.status_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading zone status: {e}")
            return {}

    def update_status(self, status: Dict):
        current_time = time.time()
        # Only write to disk every 2 seconds or when forced
        if current_time - self._last_update_time >= 2:
            try:
                with open(self.status_file, 'w') as f:
                    json.dump(status, f, indent=4)
                print(f"Updated zone status at {datetime.now()}")
                self._last_update_time = current_time
                self._status_cache = status
            except Exception as e:
                print(f"Error saving zone status: {e}")
        else:
            # Just update the in-memory cache
            self._status_cache = status

    def get_current_status(self):
        return self._status_cache

class ZoneTracker:
    def __init__(self, zone_id: str, thresholds: ZoneThresholds):
        self.zone_id = zone_id
        self.thresholds = thresholds
        self.history = deque(maxlen=25)  # Limit to 10 recent readings
        self.buffer_time = 30  # Buffer time in seconds
        self.zone_type = self._get_zone_type()
        self.min_threshold, self.max_threshold = self._get_zone_thresholds()

    def add_measurement(self, count: int, timestamp: float):
        """Add a measurement and keep only the latest within buffer time"""
        self.history.append({'count': count, 'timestamp': timestamp})
        
        # Remove old entries beyond buffer time (more efficient with maxlen)
        current_time = timestamp
        while self.history and current_time - self.history[0]['timestamp'] > self.buffer_time:
            self.history.popleft()

    def check_conditions(self, current_count: int, zone_status: Dict) -> Tuple[bool, str, int]:
        if not self.history or len(self.history) < 2:
            return False, "", 0
            
        # Check if enough time has elapsed for reliable detection
        current_time = time.time()
        first_measurement = self.history[0]
        if current_time - first_measurement['timestamp'] < self.buffer_time:
            return False, "", 0
            
        # Check conditions based on zone thresholds
        all_below_min = all(h['count'] <= self.min_threshold for h in self.history)
        all_above_max = all(h['count'] > self.max_threshold for h in self.history)
        all_in_range = all(self.min_threshold < h['count'] <= self.max_threshold for h in self.history)
        
        current_status = zone_status.get(self.zone_id, {}).get("status", 0)
        
        if all_below_min and current_status in [0, 2]:
            return True, f"Possible Starvation in {self.zone_type}", 1
        elif all_above_max and current_status in [0, 1]:
            return True, f"Possible Jamming in {self.zone_type}", 2
        elif all_in_range and current_status in [1, 2]:
            # Just update status in JSON - normal operation
            return True, "", 0

        return False, "", 0

    def _get_zone_type(self) -> str:
        for zone_type in ["infeed", "outfeed", "ecld", "fcld"]:  
            if zone_type in self.zone_id:
                return zone_type
        return "unknown"

    def _get_zone_thresholds(self) -> Tuple[int, int]:
        """Get min/max thresholds for the zone type"""
        zone_type = self._get_zone_type()
        
        if zone_type == "infeed":
            return (self.thresholds.infeed_min, self.thresholds.infeed_max)
        elif zone_type == "outfeed":
            return (self.thresholds.outfeed_min, self.thresholds.outfeed_max)
        elif zone_type == "ecld":
            return (self.thresholds.ecld_min, self.thresholds.ecld_max)
        elif zone_type == "fcld":
            return (0, self.thresholds.fcld_max)
        return (0, 0)

class OptimizedProductionMonitor:
    def __init__(self, config: dict):
        print(f"=== Starting Optimized Production Monitor at {datetime.now()} ===")
        self.db = DatabaseManager(config['postgres'])
        self.zone_manager = ZoneManager(
            config['zones']['file_path'],
            config['zones']['status_file']
        )
        self.consumer = self._setup_kafka_consumer(config['kafka'])
        self.zone_trackers = {}
        self._initialize_zone_trackers()
        self.cycle_count = 0
        self.last_print_time = time.time()

    def _setup_kafka_consumer(self, config: dict) -> KafkaConsumer:
        """Initialize and configure Kafka consumer"""
        try:
            consumer = KafkaConsumer(
                config['topic'],
                bootstrap_servers=config['bootstrap_servers'],
                auto_offset_reset=config['auto_offset_reset'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='production_monitor'
            )
            print("Kafka consumer initialized.")
            return consumer
        except Exception as e:
            print(f"Kafka consumer initialization error: {e}")
            sys.exit(1)
        
    def _initialize_zone_trackers(self):
        """Initialize trackers for all monitored zones"""
        # Initialize trackers for MC 17-22 zones
        mc_numbers = ['17', '18', '19', '20', '21', '22']
        zone_types = ['ecld', 'fcld', 'infeed', 'outfeed']
        
        for mc_num in mc_numbers:
            for zone_type in zone_types:
                zone_id = f"mc_{mc_num}_{zone_type}"
                self.zone_trackers[zone_id] = ZoneTracker(
                    zone_id=zone_id,
                    thresholds=self.zone_manager.thresholds
                )
        
        print(f"Initialized {len(self.zone_trackers)} zone trackers")

    def _check_zone_conditions(self, zone_id: str, count: int, zone_status: Dict, sensor_id: str) -> None:
        if zone_id not in self.zone_trackers:
            return
            
        current_time = time.time()
        tracker = self.zone_trackers[zone_id]
        tracker.add_measurement(count, current_time)
        
        should_alert, event_type, new_status = tracker.check_conditions(
            count, zone_status
        )

        if should_alert:
            if new_status in [1, 2]:
                self._handle_alert(zone_id, event_type, new_status, zone_status, sensor_id)
            elif new_status == 0:
                # Just update the status
                zone_status[zone_id] = {"status": new_status}

    def _handle_alert(self, zone_id: str, event_type: str, new_status: int, 
                     zone_status: Dict, sensor_id: str):
        """Handle zone alerts with minimal logging"""
        conn = self.db.get_connection()
        try:
            current_datetime = datetime.now() + timedelta(hours=5, minutes=30, seconds=25)
            timestamp_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            image_filename = f"{timestamp_str}.jpeg"
            zone_status[zone_id] = {"status": new_status}
            
            event_data = (
                str(current_datetime),
                str(uuid.uuid4()),
                f"Camera_{sensor_id}",
                zone_id,
                event_type,
                "Productivity",
                image_filename
            )
            
            self.db.insert_event(conn, event_data)
            print(f"Alert generated for {zone_id}: {event_type} (Status: {new_status})")
            
        except Exception as e:
            print(f"Error processing zone {zone_id}: {e}")
        finally:
            conn.commit()
            self.db.return_connection(conn)

    def process_message(self, message: Dict[str, Any]):
        """Process incoming Kafka messages"""
        self.cycle_count += 1
        current_time = time.time()
        
        if "objects" not in message or "roi" not in message or "sensorId" not in message:
            return

        zones = message["roi"]
        sensor_id = message["sensorId"]
        zone_status = self.zone_manager.get_current_status()

        # Only process messages from relevant sensors
        if sensor_id in ['17_18', '19_20', '21_22']:
            for zone_id, count in zones.items():
                if zone_id in self.zone_trackers:
                    self._check_zone_conditions(zone_id, count, zone_status, sensor_id)

        # Print cycle info every 5 seconds
        if current_time - self.last_print_time >= 5:
            print(f"Cycle {self.cycle_count}: Processed message from sensor {sensor_id}")
            print(f"Active zones: {list(zones.keys())}")
            print(f"Current status: {json.dumps(zone_status, indent=2)}")
            print("-" * 50)
            self.last_print_time = current_time

        # Update the zone status
        self.zone_manager.update_status(zone_status)

    def run(self):
        """Main execution loop"""
        try:
            print("Starting message processing loop...")
            for message in self.consumer:
                self.process_message(message.value)
        except KeyboardInterrupt:
            print("Monitor interrupted by user")
        except Exception as e:
            print(f"Error in monitor execution: {e}")
        finally:
            self.consumer.close()
            print("Monitor shutdown complete")


if __name__ == "__main__":
    monitor = OptimizedProductionMonitor(CONFIG)
    monitor.run()
