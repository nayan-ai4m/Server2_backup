import json
import sys
import uuid
import zmq
from shapely.geometry import Polygon,Point,box
import os
if sys.version_info >= (3, 12, 0):
    import six

    sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer
import psycopg2
import datetime

conn = psycopg2.connect(
    host="localhost", dbname="hul", user="postgres", password="ai4m2024"
)
insert_query = """INSERT INTO public.event_table(
	"timestamp", event_id, zone, camera_id,event_type,alert_type,filename)
	VALUES (%s, %s, %s, %s, %s,%s,%s);"""
# Set up the Kafka consumer


def insert_data_into_table(data):
    # Define the table name

    # Extract the keys (column names) and values from the JSON data
    try:
        print(data)
        cur = conn.cursor()
        cur.execute(insert_query, data)
        # Commit the transaction
        conn.commit()

        print("Data inserted successfully.")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    finally:
        if cur:
            cur.close()


consumer = KafkaConsumer(
    "loop3",  # The Kafka topic to subscribe to
    bootstrap_servers=["localhost:9092"],  # Kafka broker addresses
    auto_offset_reset="latest",  # Start from the earliest message
    enable_auto_commit=True,  # Automatically commit offsets
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    ),  # Deserialize message to JSON
)
tracking_id_counts = {}
print("Starting consumer...")
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.setsockopt(zmq.CONFLATE, 1)
socket.bind("tcp://192.168.4.11:5556")
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
    "mc_20_infeed": {"status": 0, "timestamp": ""},
}

with open('jamming_zones.json','r') as zones:
    data = json.load(zones)

polygon_data = {}

for name_id, zone in data.items():

    # Split the string into coordinate pairs

    coordinates = zone.split(';')

    # Convert to tuples of (x, y) for the Polygon, assuming alternating x and y coordinates

    points = [(int(coordinates[i]), int(coordinates[i + 1])) for i in range(0, len(coordinates), 2)]

    # Create a Shapely Polygon from the points

    polygon_data[name_id] = Polygon(points)


# Consume messages
try:
    for message in consumer:
        msg = message.value
        objects = msg["objects"]
        zones = msg["zones"]
        sensorId = msg["sensorId"]
        print(sensorId)
        for locations in zones.keys() :
            if locations in polygon_data.keys() and sensorId == '17_18' :
                for clds in objects:
                    parts = clds.split("|")
                    center_x,center_y = (float(parts[1])+float(parts[3]))/2,(float(parts[4])+float(parts[2]))/2
                    centroid = Point(center_x,center_y)
                    #centroid = box(float(parts[1]),float(parts[2]),float(parts[3]),float(parts[4]))
                    tracking_id = parts[0]
                    #print(tracking_id,centroid)
                    if centroid.within(polygon_data[locations]):
                        #print(parts,sensorId,locations)
                        if tracking_id in tracking_id_counts:
                            tracking_id_counts[tracking_id]["count"] += 1
                        else:
                            tracking_id_counts[tracking_id] = {}
                            tracking_id_counts[tracking_id]["count"] = 1
                            tracking_id_counts[tracking_id]["flag"] = False
                            tracking_id_counts[tracking_id]["camera"] = msg['sensorId']
                            tracking_id_counts[tracking_id]["zone"] = locations
                zone_parts = locations.split('_')
                if zone_parts[0] == 'mc':
                    #print(sensorId,locations)
                    machine = zone_parts[1]
                    infeed = 'mc_'+machine+'_infeed'
                    outfeed = 'mc_'+machine+'_outfeed'
                    ecld = 'mc_'+machine+'_ecld'
                    #print(zones[infeed],zones[outfeed],zones[ecld])
        if sensorId == "17_18":
            if zones["mc_17_infeed"] < 2:
                if "mc_17_infeed" in zone_status.keys():
                    if zone_status["mc_17_infeed"]["status"] == 0 or zone_status["mc_17_infeed"]["status"] == 2:
                        zone_status["mc_17_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_17_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_17_infeed"] > 3:
                    if "mc_17_infeed" in zone_status.keys():
                        if zone_status["mc_17_infeed"]["status"] == 1 or zone_status["mc_17_infeed"]["status"] == 0:
                            zone_status["mc_17_infeed"]["status"] = 2


            if zones["mc_18_infeed"] < 2:
                if "mc_18_infeed" in zone_status.keys():
                    if zone_status["mc_18_infeed"]["status"] == 0 or zone_status["mc_18_infeed"]["status"] == 2:
                        zone_status["mc_18_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_18_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_18_infeed"] > 3:
                    if "mc_18_infeed" in zone_status.keys():
                        if zone_status["mc_18_infeed"]["status"] == 1 or zone_status["mc_18_infeed"]["status"] == 0:
                            zone_status["mc_18_infeed"]["status"] = 2
        if sensorId == "19_20":
            #print(zones)
            if zones["mc_19_infeed"] < 2:
                if "mc_19_infeed" in zone_status.keys():
                    if zone_status["mc_19_infeed"]["status"] == 0 or zone_status["mc_19_infeed"]["status"] == 2:
                        zone_status["mc_19_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_19_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_19_infeed"] > 3:
                    if "mc_19_infeed" in zone_status.keys():
                        if zone_status["mc_19_infeed"]["status"] == 1 or zone_status["mc_19_infeed"]["status"] == 0:
                            zone_status["mc_19_infeed"]["status"] = 2


            if zones["mc_20_infeed"] < 2:
                if "mc_20_infeed" in zone_status.keys():
                    if zone_status["mc_20_infeed"]["status"] == 0 or zone_status["mc_20_infeed"]["status"] == 2:
                        zone_status["mc_20_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_20_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_20_infeed"] > 3:
                    if "mc_20_infeed" in zone_status.keys():
                        if zone_status["mc_20_infeed"]["status"] == 1 or zone_status["mc_20_infeed"]["status"] == 0:
                            zone_status["mc_20_infeed"]["status"] = 2
        if sensorId == "21_22":
            print(zones)
            if zones["mc_21_infeed"] < 2:
                if "mc_21_infeed" in zone_status.keys():
                    if zone_status["mc_21_infeed"]["status"] == 0 or zone_status["mc_21_infeed"]["status"] == 2:
                        zone_status["mc_21_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_21_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_21_infeed"] > 3:
                    if "mc_21_infeed" in zone_status.keys():
                        if zone_status["mc_21_infeed"]["status"] == 1 or zone_status["mc_21_infeed"]["status"] == 0:
                            zone_status["mc_21_infeed"]["status"] = 2


            if zones["mc_22_infeed"] < 2:
                if "mc_22_infeed" in zone_status.keys():
                    if zone_status["mc_22_infeed"]["status"] == 0 or zone_status["mc_22_infeed"]["status"] == 2:
                        zone_status["mc_22_infeed"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_22_infeed",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpg"
                            )
                        )
            else:
                if zones["mc_22_infeed"] > 3:
                    if "mc_22_infeed" in zone_status.keys():
                        if zone_status["mc_22_infeed"]["status"] == 1 or zone_status["mc_22_infeed"]["status"] == 0:
                            zone_status["mc_22_infeed"]["status"] = 2


        #print(tracking_id_counts)
        for tracking_id, data in tracking_id_counts.items():
            if data['count']  > 60.0 and data['flag'] == False :
                #insert_data_into_table((str(datetime.datetime.now()),str(uuid.uuid4()),msg['sensorId'],tracking_id_counts[tracking_id]["zone"],"possible_jamming","productvity",str(datetime.datetime.now())+".jpg"))
                tracking_id_counts[tracking_id]['flag'] = True
                socket.send_json({"camera":tracking_id_counts[tracking_id]["camera"],"filename":str(datetime.datetime.now())+".jpg"})
except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    consumer.close()
    print("Consumer closed")
