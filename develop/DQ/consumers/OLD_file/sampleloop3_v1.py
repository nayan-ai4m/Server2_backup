import json
import sys
import uuid
import zmq
from shapely.geometry import Polygon, Point, box
import os
import datetime
import psycopg2

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer

# Database connection
conn = psycopg2.connect(
    host="localhost",
    dbname="hul",
    user="postgres",
    password="ai4m2024"
)

# SQL query for inserting events
insert_query = """
    INSERT INTO public.event_table(
        "timestamp", event_id, zone, camera_id, event_type, alert_type, filename)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

def insert_data_into_table(data):
    try:
        print(data)
        with conn.cursor() as cur:
            cur.execute(insert_query, data)
            conn.commit()
        print("Data inserted successfully.")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

# Initialize ZMQ socket
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.setsockopt(zmq.CONFLATE, 1)
socket.bind("tcp://192.168.4.11:5556")

# Initialize zone status dictionary
zone_status = {
    "mc_21_fcld": {"status": "", "timestamp": ""},
    "mc_21_dropping": {"status": "", "timestamp": ""},
    "mc_21_outfeed": {"status": "", "timestamp": ""},
    "mc_21_ecld": {"status": "", "timestamp": ""},
    "mc_21_infeed": {"status": "", "timestamp": ""},
    "mc_22_outfeed": {"status": "", "timestamp": ""},
    "mc_22_fcld": {"status": "", "timestamp": ""},
    "mc_22_ecld": {"status": "", "timestamp": ""},
    "mc_22_dropping": {"status": "", "timestamp": ""},
    "mc_22_infeed": {"status": "", "timestamp": ""},
    "mc_17_dropping": {"status": 0, "timestamp": ""},
    "mc_17_ecld": {"status": 0, "timestamp": ""},
    "mc_18_outfeed": {"status": 0, "timestamp": ""},
    "mc_17_fcld": {"status": 0, "timestamp": ""},
    "mc_17_infeed": {"status": 0, "timestamp": ""},
    "mc_17_outfeed": {"status": 0, "timestamp": ""},
    "mc_18_fcld": {"status": 0, "timestamp": ""},
    "mc_18_dropping": {"status": 0, "timestamp": ""},
    "mc_18_ecld": {"status": 0, "timestamp": ""},
    "mc_18_infeed": {"status": 0, "timestamp": ""},
    "l3_tapping_outfeed": {"status": "", "timestamp": ""},
    "l3_tapping_infeed": {"status": "", "timestamp": ""},
    "l3_outfeed": {"status": "", "timestamp": ""},
    "l3_infeed": {"status": "", "timestamp": ""},
    "mc_27_fcld": {"status": "", "timestamp": ""},
    "mc_27_dropping": {"status": "", "timestamp": ""},
    "mc_27_ecld": {"status": "", "timestamp": ""},
    "mc_27_infeed": {"status": "", "timestamp": ""},
    "mc_28_fcld": {"status": "", "timestamp": ""},
    "mc_27_outfeed": {"status": "", "timestamp": ""},
    "mc_28_dropping": {"status": "", "timestamp": ""},
    "mc_28_ecld": {"status": "", "timestamp": ""},
    "mc_28_outfeed": {"status": "", "timestamp": ""},
    "mc_28_infeed": {"status": "", "timestamp": ""},
    "mc_25_fcld": {"status": "", "timestamp": ""},
    "mc_25_dropping": {"status": "", "timestamp": ""},
    "mc_25_ecld": {"status": "", "timestamp": ""},
    "mc_25_infeed": {"status": 0, "timestamp": ""},
    "mc_26_outfeed": {"status": "", "timestamp": ""},
    "mc_25_outfeed": {"status": 0, "timestamp": ""},
    "mc_26_fcld": {"status": "", "timestamp": ""},
    "mc_26_dropping": {"status": "", "timestamp": ""},
    "mc_26_ecld": {"status": "", "timestamp": ""},
    "mc_26_infeed": {"status": "", "timestamp": ""},
    "l4_tapping_outfeed": {"status": "", "timestamp": ""},
    "l4_tapping_infeed": {"status": "", "timestamp": ""},
    "l4_outfeed": {"status": "", "timestamp": ""},
    "l4_infeed": {"status": "", "timestamp": ""},
    "mc_29_outfeed": {"status": "", "timestamp": ""},
    "mc_29_fcld": {"status": "", "timestamp": ""},
    "mc_29_ecld": {"status": "", "timestamp": ""},
    "mc_29_infeed": {"status": "", "timestamp": ""},
    "mc_3_outfeed": {"status": "", "timestamp": ""},
    "mc_3_dropping": {"status": "", "timestamp": ""},
    "mc_29_dropping": {"status": "", "timestamp": ""},
    "mc_3_fcld": {"status": "", "timestamp": ""},
    "mc_3_ecld": {"status": "", "timestamp": ""},
    "mc_19_outfeed": {"status": "", "timestamp": ""},
    "mc_19_fcld": {"status": 0, "timestamp": ""},
    "mc_19_ecld": {"status": 0, "timestamp": ""},
    "mc_19_infeed": {"status": 0, "timestamp": ""},
    "mc_2_outfeed": {"status": "", "timestamp": ""},
    "mc_19_dropping": {"status": "", "timestamp": ""},
    "mc_2_dropping": {"status": "", "timestamp": ""},
    "mc_2_ecld": {"status": "", "timestamp": ""},
    "mc_2_fcld": {"status": "", "timestamp": ""},
    "mc_20_infeed": {"status": 0, "timestamp": ""}
}

# Load zone configurations
with open('jamming_zones.json', 'r') as zones:
    zone_data = json.load(zones)

# Convert zone coordinates to Shapely polygons
polygon_data = {}
for name_id, zone in zone_data.items():
    coordinates = zone.split(';')
    points = [(int(coordinates[i]), int(coordinates[i + 1]))
             for i in range(0, len(coordinates), 2)]
    polygon_data[name_id] = Polygon(points)

# Set up Kafka consumer
consumer = KafkaConsumer(
    "loop3",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

tracking_id_counts = {}

def process_zone_condition(zones, zone_name, current_timestamp, sensor_id, condition_type):
    """Helper function to process zone conditions and insert data"""
    if zone_name not in zone_status:
        return

    current_status = zone_status[zone_name]["status"]
    zone_count = zones.get(zone_name, 0)

    conditions = {
        "infeed_low": {
            "threshold": 2,
            "status_check": [0, 2],
            "new_status": 1,
            "event_type": "possible starvation"
        },
        "outfeed_low": {
            "threshold": 1,
            "status_check": [0, 2],
            "new_status": 1,
            "event_type": "possible cld stuck btw converoy"
        },
        "dropping_low": {
            "threshold": 1,
            "status_check": [0, 2],
            "new_status": 1,
            "event_type": "possible starvation"
        },
        "ecld_low": {
            "threshold": 4,
            "status_check": [0, 2],
            "new_status": 1,
            "event_type": "possible starvation"
        },
        "fcld_high": {
            "threshold": 3,
            "status_check": [0, 1],
            "new_status": 2,
            "event_type": "possible converoy block the outfeed"
        },
        "infeed_high": {
            "threshold": 3,
            "status_check": [1, 0],
            "new_status": 2,
            "event_type": "possible jamming"
        },
        "outfeed_high": {
            "threshold": 2,
            "status_check": [0, 1],
            "new_status": 2,
            "event_type": "possible jamming"
        },
        "dropping_high": {
            "threshold": 2,
            "status_check": [0, 1],
            "new_status": 2,
            "event_type": "possible jamming"
        },
        "ecld_high": {
            "threshold": 4,
            "status_check": [0, 1],
            "new_status": 2,
            "event_type": "possible jamming"
        }
    }

    condition = conditions.get(condition_type)
    if not condition:
        return

    if (condition_type.endswith('_low') and zone_count < condition["threshold"]) or (condition_type.endswith('_high') and zone_count > condition["threshold"]):
        if current_status in condition["status_check"]:
            zone_status[zone_name]["status"] = condition["new_status"]
            insert_data_into_table((
                current_timestamp,
                str(uuid.uuid4()),
                sensor_id,
                zone_name,
                condition["event_type"],
                "productivity",
                f"{current_timestamp}.jpg"
            ))

def process_machine_zones(zones, machine_id, sensor_id):
    """Process all zones for a specific machine"""
    current_timestamp = str(datetime.datetime.now())
    base_zone = f"mc_{machine_id}"

    # Process low count conditions
    for zone_type in ['infeed', 'outfeed', 'dropping', 'ecld']:
        zone_name = f"{base_zone}_{zone_type}"
        process_zone_condition(zones, zone_name, current_timestamp, sensor_id, f"{zone_type}_low")

    # Process fcld high condition
    process_zone_condition(zones, f"{base_zone}_fcld", current_timestamp, sensor_id, "fcld_high")

    # Process high count conditions
    for zone_type in ['infeed', 'outfeed', 'dropping', 'ecld']:
        zone_name = f"{base_zone}_{zone_type}"
        process_zone_condition(zones, zone_name, current_timestamp, sensor_id, f"{zone_type}_high")

print("Starting consumer...")
try:
    for message in consumer:
        msg = message.value
        objects = msg["objects"]
        zones = msg["zones"]
        sensor_id = msg["sensorId"]

        # Process objects in zones
        for location in zones.keys():
            if location in polygon_data and (sensor_id in ["17_18", "19_20", "21_22"]):
                for cld in objects:
                    parts = cld.split("|")
                    center_x = (float(parts[1]) + float(parts[3])) / 2
                    center_y = (float(parts[4]) + float(parts[2])) / 2
                    centroid = Point(center_x, center_y)

                    tracking_id = parts[0]
                    if centroid.within(polygon_data[location]):
                        if tracking_id in tracking_id_counts:
                            tracking_id_counts[tracking_id]["count"] += 1
                        else:
                            tracking_id_counts[tracking_id] = {
                                "count": 1,
                                "flag": False,
                                "camera": msg['sensorId'],
                                "zone": location
                            }

        # Process machine zones based on sensor ID
        if sensor_id == "17_18":
            process_machine_zones(zones, "17", sensor_id)
            process_machine_zones(zones, "18", sensor_id)
        elif sensor_id == "19_20":
            process_machine_zones(zones, "19", sensor_id)
            process_machine_zones(zones, "20", sensor_id)
        elif sensor_id == "21_22":
            process_machine_zones(zones, "21", sensor_id)
            process_machine_zones(zones, "22", sensor_id)

        # Check tracking IDs for jamming
        for tracking_id, data in tracking_id_counts.items():
            if data['count'] > 60.0 and not data['flag']:
                data['flag'] = True
                socket.send_json({
                    "camera": data["camera"],
                    "filename": f"{datetime.datetime.now()}.jpg"
                })

except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    consumer.close()
    print("Consumer closed")
    if conn:
        conn.close()

