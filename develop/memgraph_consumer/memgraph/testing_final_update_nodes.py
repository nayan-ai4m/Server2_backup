from gqlalchemy import Memgraph
from kafka import KafkaProducer
import time
import json
from datetime import datetime, timedelta
import re
from collections import deque

KAFKA_TOPIC_1 = "cctv_l3_1"  
KAFKA_TOPIC_2 = "cctv_l3_2"
KAFKA_TOPIC_3 = "cctv_l3_3"


# Reading machines names from graph_relations.json
with open("graph_relations.json", "r") as fp:
    graph_relations = json.load(fp)

zone_nodes = graph_relations["nodes"]

# Initialize Kafka Producer with retry logic
def initialize_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='192.168.1.168:9092',  # Change if needed
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[Kafka] Connected to Kafka broker.")
            return producer
        except Exception as e:
            print(f"[Kafka] Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

producer = initialize_kafka_producer()

def fetch_machine_names(db):
    return [m['name'] for m in db.execute_and_fetch("""
        MATCH (m:kafkaData)
        RETURN m.name as name;
    """)]

def fetch_latest_kafka_data(db, machine_name):
    result = list(db.execute_and_fetch(
        """
        MATCH (m:kafkaData {name: $machine_name})
        RETURN m.cld_status as cld_status, m.orientation as orientation, m.source_id as source_id, m.num_boxes as cld_count
        ORDER BY m.last_updated DESC LIMIT 1;
        """,
        {"machine_name": machine_name}
    ))
   # print(f" latest_result  ++++++ : {result}")
   
    if not result:
        return {"cld_status": "Empty", "orientation": "None", "source_id": None, "cld_count": 0}
   
    return result[0]

def update_machine_status(db, machine_name, new_status, orientation, cld_status, timestamp, source_id, cld_count):
    db.execute("""
        MATCH (m:zone {name: $machine_name})  
        SET m.status = $new_status,
            m.orientation = $orientation,
            m.cld_count = $cld_count,
            m.cld_status = $cld_status,
            m.last_updated = $timestamp,
            m.source_id = $source_id
    """, {
        "machine_name": machine_name,
        "new_status": new_status,
        "orientation": orientation,
        "cld_count": cld_count,
        "cld_status": cld_status,
        "timestamp": timestamp,
        "source_id": source_id
    })

def get_current_status(db, machine_name):
    result = list(db.execute_and_fetch("""
        MATCH (m:zone {name: $machine_name})
        RETURN m.status as status, m.last_updated as timestamp;
    """, {"machine_name": machine_name}))
   
    if not result:
        return None
    return result[0]

def check_jamming_starvation(status, current_timestamp):
    try:
        parsed_timestamp = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        parsed_timestamp = datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S')

    elapsed = datetime.now() - parsed_timestamp

    if "temp" in status:
        if elapsed >= timedelta(seconds=25):
            if "bad_orientation" in status: 
                return "jamming & bad_orientation"
            else:
                return "jamming"
            
    elif "highbay" in status:
        if elapsed >= timedelta(seconds=10):
            return "jamming"
        
    elif "infeedj" in status:
        if elapsed >= timedelta(seconds=20):
            return "jamming"
        
    elif "no_cld" in status:
        if elapsed >= timedelta(seconds=25):
            return "possible_starvation"

    return status

bad_orientation_history = {}
BAD_ORIENTATION_DURATION = 5  # seconds

def should_promote_bad_orientation(machine_name):
    now = datetime.now()
    bo_deque = bad_orientation_history.setdefault(machine_name, deque())

    # Remove old timestamps
    while bo_deque and (now - bo_deque[0]).total_seconds() > BAD_ORIENTATION_DURATION:
        bo_deque.popleft()

    # Return whether it has persisted long enough
    duration = (bo_deque[-1] - bo_deque[0]).total_seconds() if len(bo_deque) > 1 else 0
    return duration >= BAD_ORIENTATION_DURATION

def track_bad_orientation(machine_name, status):
    if machine_name not in bad_orientation_history:
        bad_orientation_history[machine_name] = deque()

    if status == "bad_orientation":
        bad_orientation_history[machine_name].append(datetime.now())
    else:
        bad_orientation_history[machine_name].clear()

def node_status_updater():
    db = Memgraph()  
    while True:
        machine_names = fetch_machine_names(db)
        for machine_name in machine_names:
            if machine_name in zone_nodes:
                kafka_data = fetch_latest_kafka_data(db, machine_name)
                #print(f" kafka  ++++++ : {kafka_data}")
                cld_status = kafka_data.get("cld_status", "Empty")
                cld_count = kafka_data.get("cld_count", 0)
                orientation = str(kafka_data.get("orientation", "None"))
                source_id = kafka_data.get("source_id", None)
                timestamp = str(datetime.now())

                new_status = None



                # ------------Linecross-----------
                if machine_name.startswith("mc") and machine_name.endswith("_cld"):
                    if cld_count == 1:
                        if "bad" in orientation.lower():
                            new_status = "bad_orientation"
                        elif "good" in orientation.lower():
                            new_status = "Healthy"
                        else:
                            new_status = "Healthy"
                    else:
                        new_status = "None"
                
                #elif machine_name.startswith("mc") and machine_name.endswith("_infeed_rh"):
                #    if "bad" in orientation.lower():
                #        new_status = "bad_orientation"
                #    elif "good" in orientation.lower():
                #        new_status = "Healthy"
                #    else:
                #        new_status = "Healthy"

                # ------------Machine Orientation Infeed  zone -----------
                elif machine_name.startswith("mc") and machine_name.endswith("_infeed_jamming"):
                    if cld_count == 0:
                        new_status = "no_cld"
                    elif cld_count > 3:
                        new_status = "infeedj"
                    else:
                        new_status = "Healthy"

                # ------------Machine Outfeed-----------
                # elif machine_name=="mc17_outfeed":
                elif re.match(r"mc\d+_outfeed", machine_name):
                    if cld_count > 1 and "bad" in orientation.lower():
                        new_status = "temp & bad_orientation"
                    elif cld_count > 1:
                        new_status = "temp"
                    elif "bad" in orientation.lower():
                        new_status = "bad_orientation"
                    elif "good" in orientation.lower():
                        new_status = "Healthy"
                    else:
                        new_status = "Healthy"
                
                # ------------Taping Machine-----------
                elif machine_name=="l3_tapping_outfeed":
                    if "bad" in orientation.lower():
                        new_status = "bad_orientation"
                    elif "good" in orientation.lower():
                        new_status = "tttt"
                    else:
                        new_status = "tttt"
                
                #elif machine_name=="l3_tapping_postOutfeed":
                #    if "bad" in orientation.lower():
                #        new_status = "bad_orientation"
                #    elif "good" in orientation.lower():
                #        new_status = "Healthy"
                #    else:
                #        new_status = "Healthy"

                elif machine_name=="l3_outfeed":
                    if cld_count >= 5:
                        new_status = "temp"
                    elif "good" in orientation.lower():
                        new_status = "Healthy"
                    else:
                        new_status = "Healthy"
                
                # ------------Mat Filling-----------
                #elif machine_name=="pre_taping_l3":
                #    if "badloop3" in orientation.lower():
                #        new_status = "bad_orientation"
                #    elif "goodloop3" in orientation.lower():
                #        new_status = "tttt"
                #    else:
                #        new_status = "nothing"


                elif machine_name == "pre_taping_l3":

                    if "bad" in orientation.lower():
                        new_status = "bad_orientation"
                    elif "good" in orientation.lower():
                        new_status = "tttt"
                    else:
                        new_status = "nothing"


                #elif machine_name=="outfeed_L3C4_l3":
                #    if "bad" in orientation.lower():
                #        new_status = "bad_orientation"
                #    elif "good" in orientation.lower():
                #        new_status = "Healthy"
                
                # ------------EOL-----------
                elif machine_name== "case_erector_conveyor_l3":
                    if cld_count > 5:
                        new_status = "temp"
                    else:
                        new_status = "Healthy"
                
                elif machine_name== "check_weigher_rejection_l3":
                    if cld_count > 4:
                        new_status = "temp"
                    else:
                        new_status = "Healthy"

                # ------------Highbay-----------
                elif machine_name== "highbay_l3":
                    if cld_count > 4:
                        new_status = "highbay"
                    else:
                        new_status = "Healthy"

                
                # elif machine_name== "cw_outfeed_l3":
                #     if cld_count >= 2:
                #         new_status = "temp"
                #     else:
                #         new_status = "Healthy"
                
                # -----------------------
                else:
                    if "bad" in orientation.lower():
                        new_status = "bad_orientation"
                    elif "good" in orientation.lower():
                        new_status = "Healthy"
                    else:
                        new_status = "None"
               
               # Getting the Current Status of the Node/Zone from Memgraph to check with the New Status
                current_machine_details = get_current_status(db, machine_name)
                if not current_machine_details:
                    current_status = "None"
                    current_timestamp = str(datetime.now())
                else:
                    current_status = current_machine_details.get("status", "None") or "None"
                    current_timestamp = current_machine_details.get("timestamp", str(datetime.now()))

                # Checking if Jamming/Starvation is occuring for a specified time period
                if "temp" in current_status or 'no_cld' in current_status or 'possible_' in current_status or 'highbay' in current_status or 'infeedj' in current_status:
                    promoted_status = check_jamming_starvation(current_status, current_timestamp)
                    if promoted_status != current_status:
                        new_status = promoted_status
               
                # Prevent downgrading from jamming/sravation to possible/temp/np_cld
                downgrade_jamming = current_status == "jamming" and "temp" in new_status or current_status == "jamming" and "highbay" in new_status or current_status == "jamming" and "infeedj" in new_status
                #downgrade_jamming = (
                 #   (current_status == "jamming" and "temp" in new_status)
                  #  or (current_status == "jamming & bad_orientation" and "temp" in new_status)
                   # or (current_status == "jamming & bad_orientation" and "temp & bad_orientation" in new_status)
                #)
                downgrade_starvation = current_status == "possible_starvation" and "no_cld" in new_status
                
                track_bad_orientation(machine_name, new_status)
                promote_bad = new_status == "bad_orientation" and should_promote_bad_orientation(machine_name)

                # if new_status == "bad_orientation" and not should_promote_bad_orientation(machine_name):
                    # continue  # Wait until 3s passes for bad_orientation

                # Updating with the New Status if the the New Status is different from the Current Status
                # if new_status != current_status and not (downgrade_jamming or downgrade_starvation):
                if (new_status != current_status or promote_bad) and not (downgrade_jamming or downgrade_starvation):
                    update_machine_status(db, machine_name, new_status, orientation, cld_status, timestamp, source_id, cld_count)
                   
                    message = {
                        "machine_name": machine_name,
                        "status": new_status,
                        "orientation": orientation
                    }
                    try:
                        producer.send(KAFKA_TOPIC_1, message)
                        producer.send(KAFKA_TOPIC_2, message)
                        producer.send(KAFKA_TOPIC_3, message)
                        print(f"[Kafka] Sent to Kafka: {message}")
                    except Exception as e:
                        print(f"[Kafka] Send failed: {e}")
           
if __name__ == "__main__":
    # Run the main Memgraph updater
    node_status_updater()

