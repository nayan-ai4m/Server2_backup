from gqlalchemy import Memgraph
from kafka import KafkaProducer
import time
import json
from datetime import datetime, timedelta
import re

KAFKA_TOPIC = "cctv_status"  

# Reading machines names from graph_relations.json
with open("graph_relations.json", "r") as file:
    graph_relations = json.load(file)

zone_nodes = graph_relations["nodes"]

# Tracking Possiblity of Starvation or Jamming
with open("check_possibility.json", "r") as file:
    check_possibility = json.load(file)


# Initialize Kafka Producer with retry logic
def initialize_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='192.168.1.149:9092',  # Change if needed
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
    
    if not result:
        return {"cld_status": "Empty", "orientation": "None", "source_id": None}
    
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
    if "possible_jamming" in status or "possible_starvation" in status:
        elapsed = datetime.now() - datetime.strptime(current_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        # print("----------------------")
        # print("Now:",datetime.now())
        # print("Memgrpah:",current_timestamp)
        # print("Difference", elapsed)
        if elapsed >= timedelta(seconds=25):
            print(timedelta(seconds=25))
            if "possible_jamming" in status:
                return "jamming"
            elif "possible_starvation" in status:
                return "starvation"
    return status

def node_status_updater():
    db = Memgraph()  
    while True:
        machine_names = fetch_machine_names(db)
        for machine_name in machine_names:
            if machine_name in zone_nodes:
                kafka_data = fetch_latest_kafka_data(db, machine_name)
                cld_status = kafka_data.get("cld_status", "Empty")
                cld_count = kafka_data.get("cld_count", 0)
                orientation = kafka_data.get("orientation", "None")
                source_id = kafka_data.get("source_id", None)
                timestamp = str(datetime.now())
                
                current_machine_details = get_current_status(db, machine_name)
                if not current_machine_details:
                    current_status = "None"
                    current_timestamp = str(datetime.now())
                else:
                    current_status = current_machine_details.get("status", "None")
                    current_timestamp = current_machine_details.get("timestamp", str(datetime.now()))

                '''
                check_possibility[machine_name]["flag"] = 0 -> Healthy
                check_possibility[machine_name]["flag"] = 1 -> Starvation
                check_possibility[machine_name]["flag"] = 2 -> Jamming
                '''
                updated = False
                # For mc<n>_infeed zones
                if machine_name.startswith("mc") and machine_name.endswith("_infeed"):

                    if cld_count == 0:
                        if check_possibility[machine_name].get("flag") == 1:
                            last_check = check_possibility[machine_name].get("timestamp")
                            if datetime.now()-datetime.strptime(last_check, '%Y-%m-%d %H:%M:%S.%f') >= timedelta(seconds=35):
                                new_status = "possible_starvation"
                        else:
                            check_possibility[machine_name]["flag"]=1
                            check_possibility[machine_name]["timestamp"] = timestamp
                            updated = True
                            new_status=current_status

                    elif cld_count >= 4:
                        if check_possibility[machine_name].get("flag") == 2:
                            last_check = check_possibility[machine_name].get("timestamp")
                            if datetime.now()-datetime.strptime(last_check, '%Y-%m-%d %H:%M:%S.%f') >= timedelta(seconds=10):
                                if "bad" in orientation.lower():
                                    new_status = "possible_jamming & bad_orientation"
                                else:
                                    new_status = "possible_jamming"
                        else:
                            check_possibility[machine_name]["flag"]=2
                            check_possibility[machine_name]["timestamp"] = timestamp
                            updated = True
                            new_status=current_status

                    elif "bad" in orientation.lower():
                        new_status = "bad_orientation"
                        if check_possibility[machine_name].get("flag")!=0:
                            check_possibility[machine_name]["flag"]=0
                            updated = True
                    elif "good" in orientation.lower():
                        new_status = "Healthy"
                        if check_possibility[machine_name].get("flag")!=0:
                            check_possibility[machine_name]["flag"]=0
                            updated = True
                    else:
                        new_status = "None"
                        if check_possibility[machine_name].get("flag")!=0:
                            check_possibility[machine_name]["flag"]=0
                            updated = True
                    
                # # For mc<n>_post_outfeed nodes
                # elif re.fullmatch(r"mc\d+_post_outfeed", machine_name):
                #     if cld_count >= 4:
                #         if check_possibility[machine_name].get("flag") == 2:
                #             last_check = check_possibility[machine_name].get("timestamp")
                #             if datetime.now()-datetime.strptime(last_check, '%Y-%m-%d %H:%M:%S.%f') >= timedelta(seconds=20):
                #                 new_status = "possible_jamming"
                #         else:
                #             check_possibility[machine_name]["flag"]=2
                #             check_possibility[machine_name]["timestamp"] = timestamp
                #             updated = True
                #             new_status=current_status
                    
                #     else:
                #         new_status = "Healthy"
                #         if check_possibility[machine_name].get("flag")!=0:
                #             check_possibility[machine_name]["flag"]=0
                #             updated = True

                # # For mc<n>_post_outfeed_L3C<n> nodes
                # elif re.fullmatch(r"mc\d+_post_outfeed", machine_name):
                #     if cld_count >= 4:
                #         if check_possibility[machine_name].get("flag") == 2:
                #             last_check = check_possibility[machine_name].get("timestamp")
                #             if datetime.now()-datetime.strptime(last_check, '%Y-%m-%d %H:%M:%S.%f') >= timedelta(seconds=20):
                #                 new_status = "possible_jamming"
                #         else:
                #             check_possibility[machine_name]["flag"]=2
                #             check_possibility[machine_name]["timestamp"] = timestamp
                #             updated = True
                #             new_status=current_status
                    
                #     else:
                #         new_status = "Healthy"
                #         if check_possibility[machine_name].get("flag")!=0:
                #             check_possibility[machine_name]["flag"]=0
                #             updated = True
                
                else:
                    if "bad" in orientation.lower():
                        new_status = "bad_orientation" 
                    elif "good" in orientation.lower():
                        new_status = "Healthy"
                    else:
                        new_status = "None"
                
                if updated:
                    with open("check_possibility.json", "w") as file:
                        json.dump(check_possibility, file, indent=4)

                if "possible_" in current_status:
                    promoted_status = check_jamming_starvation(current_status, current_timestamp)
                    if promoted_status != current_status:
                        new_status = promoted_status

                # Prevent downgrading from final to possible
                downgrade_jamming = current_status == "jamming" and "possible_jamming" in new_status
                downgrade_starvation = current_status == "starvation" and "possible_starvation" in new_status

                if new_status != current_status and not (downgrade_jamming or downgrade_starvation):
                    update_machine_status(db, machine_name, new_status, orientation, cld_status, timestamp, source_id, cld_count)

                    message = {
                        "machine_name": machine_name,
                        "status": new_status,
                        "orientation": orientation
                    }

                    try:
                        producer.send(KAFKA_TOPIC, message)
                        print(f"[Kafka] Sent to Kafka: {message}")
                    except Exception as e:
                        print(f"[Kafka] Send failed: {e}")
               

if __name__ == "__main__":
    # Run the main Memgraph updater
    node_status_updater()
