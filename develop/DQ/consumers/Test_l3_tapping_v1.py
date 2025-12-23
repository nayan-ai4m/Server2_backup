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
os.makedirs(SAVE_DIR, exist_ok=True)

CONFIG = {
    'kafka': {
        'topic': 'loop3',
        'bootstrap_servers': ['192.168.1.149:9092'],
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
        'status_file': 'zone_status_l3_tapping_demo.json'
    }
}

@dataclass
class ZoneThresholds:
    infeed_min: int = 7
    infeed_max: int = 19
    outfeed_min: int = 2
    outfeed_max: int = 15
    tapping_infeed_min: int = 3
    tapping_infeed_max: int = 10
    tapping_outfeed_min: int = 0
    tapping_outfeed_max: int = 9

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
            INSERT INTO public.event_table(
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
        current_time = timestamp
        self.history = [
            h for h in self.history 
            if current_time - h['timestamp'] <= 30
        ]
        self._log_measurements()
        
    def _log_measurements(self):
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
            
        current_time = time.time()
        if len(self.history) < 2:
            return False, "", 0
            
        first_measurement = self.history[0]
        if current_time - first_measurement['timestamp'] < 30:
            return False, "", 0
            
        zone_type = self._get_zone_type()
        min_threshold, max_threshold = self._get_zone_thresholds()

        all_below_min = all(h['count'] < min_threshold for h in self.history)
        all_above_max = all(h['count'] > max_threshold for h in self.history)
        
        current_status = zone_status.get(self.zone_id, {}).get("status", 0)
        
        if all_below_min and current_status in [0, 2]:
            return True, f"Possible Starvation in {zone_type}", 1
        elif all_above_max and current_status in [0, 1]:
            return True, f"Possible Jamming in {zone_type}", 2
            
        return False, "", 0

    def _get_zone_type(self) -> str:
        if "tapping_infeed" in self.zone_id:
            return "tapping_infeed"
        elif "tapping_outfeed" in self.zone_id:
            return "tapping_outfeed"
        elif "infeed" in self.zone_id:
            return "infeed"
        elif "outfeed" in self.zone_id:
            return "outfeed"
        return "unknown"

    def _get_zone_thresholds(self) -> Tuple[int, int]:
        zone_type = self._get_zone_type()
        
        if zone_type == "tapping_infeed":
            return (self.thresholds.tapping_infeed_min, self.thresholds.tapping_infeed_max)
        elif zone_type == "tapping_outfeed":
            return (self.thresholds.tapping_outfeed_min, self.thresholds.tapping_outfeed_max)
        elif zone_type == "infeed":
            return (self.thresholds.infeed_min, self.thresholds.infeed_max)
        elif zone_type == "outfeed":
            return (self.thresholds.outfeed_min, self.thresholds.outfeed_max)
        return (0, 0)


class l3TapingMonitor:
    def __init__(self, config: dict):
        self._setup_logging()
        self.db = DatabaseManager(config['postgres'])
        self.zone_manager = ZoneManager(
            config['zones']['file_path'],
            config['zones']['status_file']
        )
        self.consumer = self._setup_kafka_consumer(config['kafka'])
        self.zone_trackers = {}
        self._initialize_zone_trackers()
        self.log_dir = "zone_logs"
        os.makedirs(self.log_dir, exist_ok=True)

    def _setup_logging(self):
        """Set up detailed logging configuration"""
        debug_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        debug_handler = logging.FileHandler('debug_l3_taping.log')
        debug_handler.setLevel(logging.ERROR)
        debug_handler.setFormatter(debug_formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(debug_formatter)
        
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
                group_id='l3_taping_monitor'
            )
            logging.info("Kafka consumer initialized successfully.")
            return consumer
        except Exception as e:
            logging.error(f"Kafka consumer initialization error: {e}")
            sys.exit(1)
        
    def _initialize_zone_trackers(self):
        """Initialize trackers for all monitored zones"""
        zones = [
            'l3_tapping_outfeed',
            'l3_tapping_infeed',
            'l3_outfeed',
            'l3_infeed'
        ]
        
        for zone_id in zones:
            self.zone_trackers[zone_id] = ZoneTracker(
                zone_id=zone_id,
                thresholds=self.zone_manager.thresholds
            )
            logging.info(f"Initialized tracker for zone: {zone_id}")
                
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

    def _check_zone_conditions(self, zone_id: str, count: int, zone_status: Dict) -> None:
        """Check conditions for a specific zone and generate alerts if necessary"""
        if zone_id not in self.zone_trackers:
            return
            
        current_time = time.time()
        tracker = self.zone_trackers[zone_id]
        tracker.add_measurement(count, current_time)
        
        if not hasattr(self, '_last_summary_time') or \
           current_time - getattr(self, '_last_summary_time', 0) >= 5:
            self._last_summary_time = current_time
        
        should_alert, event_type, new_status = tracker.check_conditions(
            count, zone_status
        )
        
        if should_alert:
            self._handle_alert(zone_id, event_type, new_status, zone_status)

    def _handle_alert(self, zone_id: str, event_type: str, new_status: int, zone_status: Dict):
        """Handle zone alerts with detailed logging"""
        conn = self.db.get_connection()
        try:
            current_datetime = datetime.now() + timedelta(hours=5, minutes=30, seconds=25)
            timestamp_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            image_filename = f"{timestamp_str}.jpeg"
            
            zone_status[zone_id] = {
                "status": new_status,
                "timestamp": str(current_datetime)
            }
            
            event_data = (
                str(current_datetime),
                str(uuid.uuid4()),
                "Camera_L3_Taping",
                zone_id,
                event_type,
                "Productivity",
                image_filename
            )
            
            self.db.insert_event(conn, event_data)
            
            alert_msg = f"""
Alert Generated:
Zone: {zone_id}
Event Type: {event_type}
New Status: {new_status}
Time: {current_datetime}
History (last 30s):
{self._format_zone_history(self.zone_trackers[zone_id])}
{'='*50}
"""
            logging.debug(alert_msg)
            
            alert_file = os.path.join(
                self.log_dir, 
                f"alert_{zone_id}_{current_datetime.strftime('%Y%m%d_%H%M%S')}.txt"
            )
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
        try:
            # Validate message format
            if "roi" not in message or "sensorId" not in message:
                logging.warning(f"Invalid message format: {message}")
                return

            # Check if message is from l3_taping sensor
            if message["sensorId"] != "l3_taping":
                logging.debug(f"Ignoring message from sensor: {message['sensorId']}")
                return

            # Get zones data and current status
            zones = message["roi"]
            zone_status = self.zone_manager.load_status()

            # Process each zone in the message
            for zone_id, count in zones.items():
                if zone_id in self.zone_trackers:
                    logging.debug(f"Processing zone {zone_id} with count {count}")
                    self._check_zone_conditions(zone_id, count, zone_status)
                else:
                    logging.warning(f"Unknown zone ID received: {zone_id}")

            # Update zone status file
            self.zone_manager.update_status(zone_status)

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            logging.error(f"Message content: {message}")

    def run(self):
        """Main execution loop"""
        try:
            logging.info("Starting l3 Taping Monitor...")
            logging.info("Waiting for messages...")
            
            for message in self.consumer:
                try:
                    logging.debug(f"Received message: {message.value}")
                    self.process_message(message.value)
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logging.info("Monitor interrupted by user")
        except Exception as e:
            logging.error(f"Error in monitor execution: {e}")
        finally:
            self.consumer.close()
            logging.info("Monitor shutdown complete")

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.consumer.close()
            logging.info("Kafka consumer closed")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    try:
        monitor = l3TapingMonitor(CONFIG)
        monitor.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        monitor.cleanup()
