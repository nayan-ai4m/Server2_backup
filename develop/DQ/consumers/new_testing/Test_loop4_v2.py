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


SAVE_DIR = "/home/ai4m/develop/backup/develop/DQ/consumers/images"
os.makedirs(SAVE_DIR, exist_ok=True)  # Ensure the directory exists

# Constants
CONFIG = {
    'kafka': {
        'topic': 'loop4',
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
        'status_file': 'zone_status_loop4_demo.json'
    }
}

@dataclass
class ZoneThresholds:
    infeed_min: int = 1
    infeed_max: int = 3
    outfeed_min: int = 0
    outfeed_max: int = 2
    # dropping_min: int = 1
    # dropping_max: int = 2
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
        
        # Debug logging of measurements
        self._log_measurements()
        
    def _log_measurements(self):
        """Log the current state of measurements for debugging"""
        measurements_str = "\n".join([
            f"    Time: {datetime.fromtimestamp(h['timestamp']).strftime('%H:%M:%S.%f')}, "
            f"Count: {h['count']}"
            for h in self.history
        ])
        
        zone_type = self._get_zone_type()
        thresholds = self._get_zone_thresholds()
        
        debug_msg = f"""
Zone: {self.zone_id}
Type: {zone_type}
Thresholds: Min={thresholds[0]}, Max={thresholds[1]}
Number of measurements in last 30s: {len(self.history)}
Measurements:
{measurements_str}
{'='*50}
"""
        logging.debug(debug_msg)
    
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
        min_threshold, max_threshold = self._get_zone_thresholds()

        # Check if all measurements are consistently below min threshold
        all_below_min = all(h['count'] < min_threshold for h in self.history)
        all_above_max = all(h['count'] > max_threshold for h in self.history)
        
        current_status = zone_status.get(self.zone_id, {}).get("status", 0)
        
        if all_below_min and current_status in [0, 2]:
            return True, f"Possible Starvation in {zone_type}", 1
        elif all_above_max and current_status in [0, 1]:
            return True, f"Possible Jamming in {zone_type}", 2
        elif not all_below_min and not all_above_max and current_status in [2, 1]:
            # Count is fluctuating between min and max, just update status in JSON
            zone_status[self.zone_id] = {"status": current_status}
            return True, "", 0  # No alert, just update status in JSON

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
        # elif zone_type == "dropping":
        #     return (self.thresholds.dropping_min, self.thresholds.dropping_max)
        elif zone_type == "ecld":
            return (self.thresholds.ecld_min, self.thresholds.ecld_max)
        elif zone_type == "fcld":
            return (0, self.thresholds.fcld_max)
        return (0, 0)

class EnhancedProductionMonitor:
    def __init__(self, config: dict):
        # Set up debug logging
        self._setup_logging()
        
        self.db = DatabaseManager(config['postgres'])
        self.zone_manager = ZoneManager(
            config['zones']['file_path'],
            config['zones']['status_file']
        )
        self.consumer = self._setup_kafka_consumer(config['kafka'])
        self.zone_trackers = {}
        self._initialize_zone_trackers()
        
        # Create debug log directory if it doesn't exist
        self.log_dir = "zone_logs"
        os.makedirs(self.log_dir, exist_ok=True)

    def _setup_logging(self):
        """Set up detailed logging configuration"""
        # Create formatters and handlers
        debug_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler for debug logs
        debug_handler = logging.FileHandler('debug_zone_tracking_loop4.log')
        debug_handler.setLevel(logging.ERROR)
        debug_handler.setFormatter(debug_formatter)
        
        # Console handler for info logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(debug_formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(debug_handler)
        root_logger.addHandler(console_handler)

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
            logging.info("Kafka consumer initialized successfully.")
            return consumer
        except Exception as e:
            logging.error(f"Kafka consumer initialization error: {e}")
            sys.exit(1)
        
    def _initialize_zone_trackers(self):
        """Initialize trackers for all monitored zones"""
        # Initialize trackers for MC 25-30 zones
        mc_numbers = ['25', '26', '27', '28', '29', '30']
        zone_types = ['ecld', 'fcld', 'infeed', 'outfeed']
        
        for mc_num in mc_numbers:
            for zone_type in zone_types:
                zone_id = f"mc_{mc_num}_{zone_type}"
                self.zone_trackers[zone_id] = ZoneTracker(
                    zone_id=zone_id,
                    thresholds=self.zone_manager.thresholds
                )
                logging.info(f"Initialized tracker for zone: {zone_id}")
                
        # Log initial zone configuration
        self._log_zone_configuration()

    def _log_zone_configuration(self):
        """Log the initial configuration of all zones"""
        config_summary = ["=== Zone Configuration Summary ==="]
        
        for zone_id, tracker in sorted(self.zone_trackers.items()):
            zone_type = tracker._get_zone_type()
            min_threshold, max_threshold = tracker._get_zone_thresholds()
            
            config_summary.append(f"\nZone: {zone_id}")
            config_summary.append(f"Type: {zone_type}")
            config_summary.append(f"Thresholds: Min={min_threshold}, Max={max_threshold}")
        
        logging.debug("\n".join(config_summary))

    def _check_zone_conditions(self, zone_id: str, count: int, 
                             zone_status: Dict, sensor_id: str) -> None:
        """Check conditions for a specific zone and generate alerts if necessary"""
        if zone_id not in self.zone_trackers:
            return
            
        current_time = time.time()
        tracker = self.zone_trackers[zone_id]
        tracker.add_measurement(count, current_time)
        
        # Generate zone summary every 5 seconds
        if not hasattr(self, '_last_summary_time') or \
           current_time - getattr(self, '_last_summary_time', 0) >= 5:

            self._last_summary_time = current_time
        
        should_alert, event_type, new_status = tracker.check_conditions(
            count, zone_status
        )
        
        if should_alert and new_status in [1,2]:
            self._handle_alert(zone_id, event_type, new_status, zone_status, sensor_id)



    def _handle_alert(self, zone_id: str, event_type: str, new_status: int, 
                     zone_status: Dict, sensor_id: str):
        """Handle zone alerts with detailed logging"""
        conn = self.db.get_connection()
        try:
            current_datetime = datetime.now() + timedelta(hours=5, minutes=30, seconds=25)
            timestamp_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            image_filename = f"{timestamp_str}.jpeg"
            image_path = os.path.join(SAVE_DIR, image_filename)
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
            
            # Log detailed alert information
            alert_msg = f"""
Alert Generated:
Zone: {zone_id}
Event Type: {event_type}
New Status: {new_status}
Sensor: {sensor_id}
Time: {current_datetime}
History (last 30s):
{self._format_zone_history(self.zone_trackers[zone_id])}
{'='*50}
"""
            logging.debug(alert_msg)
            
            # Save alert to separate file
            alert_file = os.path.join(self.log_dir, 
                                    f"alert_{zone_id}_{current_datetime.strftime('%Y%m%d_%H%M%S')}.txt")
            with open(alert_file, 'w') as f:
                f.write(alert_msg)
            
        except Exception as e:
            logging.error(f"Error processing zone {zone_id}: {e}")
        finally:
            conn.commit()
            self.db.return_connection(conn)

    def _format_zone_history(self, tracker: ZoneTracker) -> str:
        """Format zone history for logging"""
        history_lines = []
        for entry in tracker.history:
            timestamp = datetime.fromtimestamp(entry['timestamp'])
            history_lines.append(f"  {timestamp.strftime('%H:%M:%S.%f')} - Count: {entry['count']}")
        return "\n".join(history_lines)

    def process_message(self, message: Dict[str, Any]):
        """Process incoming Kafka messages"""
        if "objects" not in message or "roi" not in message or "sensorId" not in message:
            logging.warning(f"Invalid message format: {message}")
            return

       #logging.info(f"Processing message: {message}")
        zones = message["roi"]
        sensor_id = message["sensorId"]
        zone_status = self.zone_manager.load_status()

        # Only process messages from relevant sensors
        if sensor_id in ['25_26', '27_28', '29_30']:
            for zone_id, count in zones.items():
                if zone_id in self.zone_trackers:
                    self._check_zone_conditions(zone_id, count, zone_status, sensor_id)

        self.zone_manager.update_status(zone_status)

    def run(self):
        """Main execution loop"""
        try:
            logging.info("Starting Enhanced Production Monitor...")
            for message in self.consumer:
                self.process_message(message.value)
        except KeyboardInterrupt:
            logging.info("Monitor interrupted by user")
        except Exception as e:
            logging.error(f"Error in monitor execution: {e}")
        finally:
            self.consumer.close()
            logging.info("Monitor shutdown complete")


if __name__ == "__main__":
    monitor = EnhancedProductionMonitor(CONFIG)
    monitor.run()
