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
    host="localhost", dbname="short_data_hul", user="postgres", password="ai4m2024"
)
insert_query = """INSERT INTO public.event_table_short(
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
# context = zmq.Context()
# socket = context.socket(zmq.PUB)
# socket.setsockopt(zmq.CONFLATE, 1)
# socket.bind("tcp://192.168.4.11:5556")


def load_zone_status_from_file():
    try:
        with open('zone_status.json', 'r') as json_file:
            return json.load(json_file)
    except Exception as e:
        print(f"Error while loading zone status: {e}")
        return {}

# Function to update zone_status and save it to a JSON file
def update_zone_status_to_file(zone_status):
    try:
        with open('zone_status.json', 'w') as json_file:
            json.dump(zone_status, json_file, indent=4)
            print("Zone status updated and saved to 'zone_status.json'.")
    except Exception as e:
        print(f"Error while saving zone status: {e}")

with open('jamming_zones_v1.json','r') as zones:
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
        objects = msg['objects']
        zones = msg['roi']
        sensorId = msg['sensorId']
        print(sensorId)

        zone_status = load_zone_status_from_file()


        for locations in zones.keys() :
            #if locations in polygon_data.keys() and sensorId == '17_18' :
            if locations in polygon_data.keys() and sensorId in ['17_18', '19_20', '21_22']:

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
                    fcld = 'mc_'+machine+'_fcld'
                    dropping = 'mc_'+machine+'_dropping'
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
                                str(datetime.datetime.now())+".jpeg"
                            )

                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_17_outfeed"] < 1:
                    if "mc_17_outfeed" in zone_status.keys():
                        if zone_status["mc_17_outfeed"]["status"] == 1 or zone_status["mc_17_outfeed"]["status"] == 0:
                            zone_status["mc_17_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_17_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_17_dropping"] < 1:
                    if "mc_17_dropping" in zone_status.keys():
                        if zone_status["mc_17_dropping"]["status"] == 2 or zone_status["mc_17_dropping"]["status"] == 0:
                            zone_status["mc_17_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_17_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_17_ecld"] < 4:
                if "mc_17_ecld" in zone_status.keys():
                    if zone_status["mc_17_ecld"]["status"] == 0 or zone_status["mc_17_ecld"]["status"] == 2:
                        zone_status["mc_17_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_17_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_17_fcld"] > 3:
                    if "mc_17_fcld" in zone_status.keys():
                        if zone_status["mc_17_fcld"]["status"] == 1 or zone_status["mc_17_fcld"]["status"] == 0:
                            zone_status["mc_17_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_17_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_17_infeed"] > 3:
                    if "mc_17_infeed" in zone_status.keys():
                        if zone_status["mc_17_infeed"]["status"] == 1 or zone_status["mc_17_infeed"]["status"] == 0:
                            zone_status["mc_17_infeed"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_17_infeed",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_17_outfeed"] > 2:
                    if "mc_17_outfeed" in zone_status.keys():
                        if zone_status["mc_17_outfeed"]["status"] == 0 or zone_status["mc_17_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                            zone_status["mc_17_outfeed"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_17_outfeed",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_17_dropping"] > 2:
                    if "mc_17_dropping" in zone_status.keys():
                        if zone_status["mc_17_dropping"]["status"] == 0 or zone_status["mc_17_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_17_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_17_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)

                if zones["mc_17_ecld"] > 4:
                    if "mc_17_ecld" in zone_status.keys():
                        if zone_status["mc_17_ecld"]["status"] == 0 or zone_status["mc_17_ecld"]["status"] == 1:
                            zone_status["mc_17_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_17_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)


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
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_18_outfeed"] < 1:
                    if "mc_18_outfeed" in zone_status.keys():
                        if zone_status["mc_18_outfeed"]["status"] == 1 or zone_status["mc_18_outfeed"]["status"] == 0:
                            zone_status["mc_18_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_18_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_18_dropping"] < 1:
                    if "mc_18_dropping" in zone_status.keys():
                        if zone_status["mc_18_dropping"]["status"] == 2 or zone_status["mc_18_dropping"]["status"] == 0:
                            zone_status["mc_18_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_18_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_18_ecld"] < 4:
                if "mc_18_ecld" in zone_status.keys():
                    if zone_status["mc_18_ecld"]["status"] == 0 or zone_status["mc_18_ecld"]["status"] == 2:
                        zone_status["mc_18_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_18_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
                        
            if zones["mc_18_fcld"] > 3:
                    if "mc_18_fcld" in zone_status.keys():
                        if zone_status["mc_18_fcld"]["status"] == 1 or zone_status["mc_18_fcld"]["status"] == 0:
                            zone_status["mc_18_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_18_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_18_infeed"] > 3:
                    if "mc_18_infeed" in zone_status.keys():
                        if zone_status["mc_18_infeed"]["status"] == 1 or zone_status["mc_18_infeed"]["status"] == 0:
                            zone_status["mc_18_infeed"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_18_infeed",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_18_outfeed"] > 2:
                    if "mc_18_outfeed" in zone_status.keys():
                        if zone_status["mc_18_outfeed"]["status"] == 0 or zone_status["mc_18_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_18_outfeed"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_18_outfeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                                
                if  zones["mc_18_dropping"] > 2:
                    if "mc_18_dropping" in zone_status.keys():
                        if zone_status["mc_18_dropping"]["status"] == 0 or zone_status["mc_18_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_18_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_18_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                                
                if zones["mc_18_ecld"] > 4:
                    if "mc_18_ecld" in zone_status.keys():
                        if zone_status["mc_18_ecld"]["status"] == 0 or zone_status["mc_18_ecld"]["status"] == 1:
                            zone_status["mc_18_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_18_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)
        
        if sensorId == "19_20":
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
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_19_outfeed"] < 1:
                    if "mc_19_outfeed" in zone_status.keys():
                        if zone_status["mc_19_outfeed"]["status"] == 1 or zone_status["mc_19_outfeed"]["status"] == 0:
                            zone_status["mc_19_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_19_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_19_dropping"] < 1:
                    if "mc_19_dropping" in zone_status.keys():
                        if zone_status["mc_19_dropping"]["status"] == 2 or zone_status["mc_19_dropping"]["status"] == 0:
                            zone_status["mc_19_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_19_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_19_ecld"] < 4:
                if "mc_19_ecld" in zone_status.keys():
                    if zone_status["mc_19_ecld"]["status"] == 0 or zone_status["mc_19_ecld"]["status"] == 2:
                        zone_status["mc_19_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_19_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_19_fcld"] > 3:
                    if "mc_19_fcld" in zone_status.keys():
                        if zone_status["mc_19_fcld"]["status"] == 1 or zone_status["mc_19_fcld"]["status"] == 0:
                            zone_status["mc_19_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_19_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_19_infeed"] > 3:
                    if "mc_19_infeed" in zone_status.keys():
                        if zone_status["mc_19_infeed"]["status"] == 1 or zone_status["mc_19_infeed"]["status"] == 0:
                            zone_status["mc_19_infeed"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_19_infeed",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_19_outfeed"] > 2:
                    if "mc_19_outfeed" in zone_status.keys():
                        if zone_status["mc_19_outfeed"]["status"] == 0 or zone_status["mc_19_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_19_outfeed"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_19_outfeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if  zones["mc_19_dropping"] > 2:
                    if "mc_19_dropping" in zone_status.keys():
                        if zone_status["mc_19_dropping"]["status"] == 0 or zone_status["mc_19_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_19_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_19_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if zones["mc_19_ecld"] > 4:
                    if "mc_19_ecld" in zone_status.keys():
                        if zone_status["mc_19_ecld"]["status"] == 0 or zone_status["mc_19_ecld"]["status"] == 1:
                            zone_status["mc_19_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_19_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)




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
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_20_outfeed"] < 1:
                    if "mc_20_outfeed" in zone_status.keys():
                        if zone_status["mc_20_outfeed"]["status"] == 1 or zone_status["mc_20_outfeed"]["status"] == 0:
                            zone_status["mc_20_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_20_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_20_dropping"] < 1:
                    if "mc_20_dropping" in zone_status.keys():
                        if zone_status["mc_20_dropping"]["status"] == 2 or zone_status["mc_20_dropping"]["status"] == 0:
                            zone_status["mc_20_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_20_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_20_ecld"] < 4:
                if "mc_20_ecld" in zone_status.keys():
                    if zone_status["mc_20_ecld"]["status"] == 0 or zone_status["mc_20_ecld"]["status"] == 2:
                        zone_status["mc_20_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_20_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_20_fcld"] > 3:
                    if "mc_20_fcld" in zone_status.keys():
                        if zone_status["mc_20_fcld"]["status"] == 1 or zone_status["mc_20_fcld"]["status"] == 0:
                            zone_status["mc_20_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_20_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_20_infeed"] > 3:
                    if "mc_20_infeed" in zone_status.keys():
                        if zone_status["mc_20_infeed"]["status"] == 1 or zone_status["mc_20_infeed"]["status"] == 0:
                            zone_status["mc_20_infeed"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_20_infeed",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_20_outfeed"] > 2:
                    if "mc_20_outfeed" in zone_status.keys():
                        if zone_status["mc_20_outfeed"]["status"] == 0 or zone_status["mc_20_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_20_outfeed"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_20_outfeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if  zones["mc_20_dropping"] > 2:
                    if "mc_20_dropping" in zone_status.keys():
                        if zone_status["mc_20_dropping"]["status"] == 0 or zone_status["mc_20_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_20_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_20_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if zones["mc_20_ecld"] > 4:
                    if "mc_20_ecld" in zone_status.keys():
                        if zone_status["mc_20_ecld"]["status"] == 0 or zone_status["mc_20_ecld"]["status"] == 1:
                            zone_status["mc_20_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_20_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)
        
        if sensorId == "21_22":
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
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)
            if zones["mc_21_outfeed"] < 1:
                    if "mc_21_outfeed" in zone_status.keys():
                        if zone_status["mc_21_outfeed"]["status"] == 1 or zone_status["mc_21_outfeed"]["status"] == 0:
                            zone_status["mc_21_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_21_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_21_dropping"] < 1:
                    if "mc_21_dropping" in zone_status.keys():
                        if zone_status["mc_21_dropping"]["status"] == 2 or zone_status["mc_21_dropping"]["status"] == 0:
                            zone_status["mc_21_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_21_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_21_ecld"] < 4:
                if "mc_21_ecld" in zone_status.keys():
                    if zone_status["mc_21_ecld"]["status"] == 0 or zone_status["mc_21_ecld"]["status"] == 2:
                        zone_status["mc_21_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_21_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_21_fcld"] > 3:
                    if "mc_21_fcld" in zone_status.keys():
                        if zone_status["mc_21_fcld"]["status"] == 1 or zone_status["mc_21_fcld"]["status"] == 0:
                            zone_status["mc_21_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_21_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_21_infeed"] > 3:
                    if "mc_21_infeed" in zone_status.keys():
                        if zone_status["mc_21_infeed"]["status"] == 1 or zone_status["mc_21_infeed"]["status"] == 0:
                            zone_status["mc_21_infeed"]["status"] = 2
                            insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_21_infeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_21_outfeed"] > 2:
                    if "mc_21_outfeed" in zone_status.keys():
                        if zone_status["mc_21_outfeed"]["status"] == 0 or zone_status["mc_21_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_21_outfeed"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_21_outfeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if  zones["mc_21_dropping"] > 2:
                    if "mc_21_dropping" in zone_status.keys():
                        if zone_status["mc_21_dropping"]["status"] == 0 or zone_status["mc_21_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_21_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_21_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if zones["mc_21_ecld"] > 4:
                    if "mc_21_ecld" in zone_status.keys():
                        if zone_status["mc_21_ecld"]["status"] == 0 or zone_status["mc_21_ecld"]["status"] == 1:
                            zone_status["mc_21_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_21_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)




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
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_22_outfeed"] < 1:
                    if "mc_22_outfeed" in zone_status.keys():
                        if zone_status["mc_22_outfeed"]["status"] == 1 or zone_status["mc_22_outfeed"]["status"] == 0:
                            zone_status["mc_22_outfeed"]["status"] = 2 # there is issue cld is missing or not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_22_outfeed",
                                    "possible cld stuck btw converoy",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

            if zones["mc_22_dropping"] < 1:
                    if "mc_22_dropping" in zone_status.keys():
                        if zone_status["mc_22_dropping"]["status"] == 2 or zone_status["mc_22_dropping"]["status"] == 0:
                            zone_status["mc_22_dropping"]["status"] = 1
                            insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_22_dropping",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)

            if zones["mc_22_ecld"] < 4:
                if "mc_22_ecld" in zone_status.keys():
                    if zone_status["mc_22_ecld"]["status"] == 0 or zone_status["mc_22_ecld"]["status"] == 2:
                        zone_status["mc_22_ecld"]["status"] = 1
                        insert_data_into_table(
                            (
                                str(datetime.datetime.now()),
                                str(uuid.uuid4()),
                                msg["sensorId"],
                                "mc_22_ecld",
                                "possible starvation",
                                "productivity",
                                str(datetime.datetime.now())+".jpeg"
                            )
                        )
                        update_zone_status_to_file(zone_status)
            if zones["mc_22_fcld"] > 3:
                    if "mc_22_fcld" in zone_status.keys():
                        if zone_status["mc_22_fcld"]["status"] == 1 or zone_status["mc_22_fcld"]["status"] == 0:
                            zone_status["mc_22_fcld"]["status"] = 2 # there is issue will be cld will not able to move forward
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_22_fcld",
                                    "possible converoy block the outfeed for both mchine",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)



            else:
                if zones["mc_22_infeed"] > 3:
                    if "mc_22_infeed" in zone_status.keys():
                        if zone_status["mc_22_infeed"]["status"] == 1 or zone_status["mc_22_infeed"]["status"] == 0:
                            zone_status["mc_22_infeed"]["status"] = 2
                            insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_22_infeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                            )
                            update_zone_status_to_file(zone_status)

                if  zones["mc_22_outfeed"] > 2:
                    if "mc_22_outfeed" in zone_status.keys():
                        if zone_status["mc_22_outfeed"]["status"] == 0 or zone_status["mc_22_outfeed"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_22_outfeed"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_22_outfeed",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if  zones["mc_22_dropping"] > 2:
                    if "mc_22_dropping" in zone_status.keys():
                        if zone_status["mc_22_dropping"]["status"] == 0 or zone_status["mc_22_dropping"]["status"] == 1:   # 0 = ok  , 1 = starvation , 2 = jamming
                                zone_status["mc_22_dropping"]["status"] = 2
                                insert_data_into_table(
                                    (
                                        str(datetime.datetime.now()),
                                        str(uuid.uuid4()),
                                        msg["sensorId"],
                                        "mc_22_dropping",
                                        "possible jamming",
                                        "productivity",
                                        str(datetime.datetime.now())+".jpeg"
                                    )
                                )
                                update_zone_status_to_file(zone_status)
                if zones["mc_22_ecld"] > 4:
                    if "mc_22_ecld" in zone_status.keys():
                        if zone_status["mc_22_ecld"]["status"] == 0 or zone_status["mc_22_ecld"]["status"] == 1:
                            zone_status["mc_22_ecld"]["status"] = 2
                            insert_data_into_table(
                                (
                                    str(datetime.datetime.now()),
                                    str(uuid.uuid4()),
                                    msg["sensorId"],
                                    "mc_22_ecld",
                                    "possible jamming",
                                    "productivity",
                                    str(datetime.datetime.now())+".jpeg"
                                )
                            )
                            update_zone_status_to_file(zone_status)

        #print(tracking_id_counts)


        for tracking_id, data in tracking_id_counts.items():
            if data['count']  > 60.0 and data['flag'] == False :
                #insert_data_into_table((str(datetime.datetime.now()),str(uuid.uuid4()),msg['sensorId'],tracking_id_counts[tracking_id]["zone"],"possible_jamming","productvity",str(datetime.datetime.now())+".jpeg"))
                tracking_id_counts[tracking_id]['flag'] = True
                # socket.send_json({"camera":tracking_id_counts[tracking_id]["camera"],"filename":str(datetime.datetime.now())+".jpeg"})
except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    consumer.close()
    print("Consumer closed")
