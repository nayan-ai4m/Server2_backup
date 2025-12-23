# from gqlalchemy import Memgraph
from kafka import KafkaConsumer
from json import loads
import uuid
import threading
import time

# memgraph = Memgraph()
consumer = KafkaConsumer(
    "loop3",
    bootstrap_servers='192.168.1.149:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=str(uuid.uuid4()),
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# consumer.poll()
# consumer.seek_to_end()

print("Kafka to Memgraph service started...")

for message in consumer:
    data = message.value
    print(data)
    timestamp = data.get("@timestamp")  # Global timestamp
    sensor_id = data.get("sensorId")  # Source ID
    objects = data.get("objects", [])  # Detected objects
    roi = data.get("roi", {})  # Number of boxes per machine

    # for obj in objects:
    #     fields = obj.split("|")
    #     orientation = "bad"
    #     if len(fields) < 9:
    #         continue  # Skip if the format is incorrect
                    
    #     print(fields[8])
        # orientation_value = "good_orientation" if 'good' in orientation.lower() else "bad_orientation" if 'bad' in orientation.lower() else "None"

    # machine_data = {}
    
    # # Extract data from `objects`
    # for obj in objects:
    #     fields = obj.split("|")
    #     if len(fields) < 9:
    #         continue  # Skip if the format is incorrect

    #     machine_name = fields[8]  # Extract machine name
    #     orientation = fields[-1]  # Extract orientation (good/bad)

    #     if not machine_name:  # Skip empty machine names
    #         continue

    #     num_boxes = roi.get(machine_name, 0)  # Get number of boxes from ROI
    #     cld_status = 1 if num_boxes > 0 else 0  # Set cld_status

    #     # Store machine data
    #     machine_data[machine_name] = {
    #         "orientation": orientation,
    #         "source_id": sensor_id,
    #         "num_boxes": num_boxes,
    #         "cld_status": cld_status
    #     }
    
    # # Ensure all machines in `roi` are updated, even if missing in `objects`
    # for machine_name, num_boxes in roi.items():
    #     if machine_name not in machine_data:  # Machines missing in `objects`
    #         machine_data[machine_name] = {
    #             "orientation": "unknown",  # Default value when no object is detected
    #             "source_id": sensor_id,
    #             "num_boxes": 0,  # No boxes detected
    #             "cld_status": 0  # Mark as inactive
    #         }

    # # Update Machine Nodes
    # for machine_name, details in machine_data.items():
    #     query = """
    #     MERGE (m:Machine {name: $machine_name})
    #     SET m.source_id = $source_id,
    #         m.orientation = $orientation,
    #         m.num_boxes = $num_boxes,
    #         m.cld_status = $cld_status,
    #         m.last_updated = $timestamp
    #     """
        
    #     memgraph.execute(query, {
    #         "machine_name": machine_name,
    #         "source_id": details["source_id"],
    #         "orientation": details["orientation"],
    #         "num_boxes": details["num_boxes"],
    #         "cld_status": details["cld_status"],
    #         "timestamp": timestamp
    #     })

    #     print(f"Updated {machine_name}: Source ID={details['source_id']}, Orientation={details['orientation']}, Boxes={details['num_boxes']} CLD Status={details['cld_status']}")

    #     # Insert KafkaData Node for Debugging
    #     kafka_query = """
    #     CREATE (d:KafkaData {
    #         machine: $machine_name,
    #         cld_status: $cld_status,
    #         timestamp: $timestamp
    #     })
    #     """
    #     memgraph.execute(kafka_query, {
    #         "machine_name": machine_name,
    #         "cld_status": details["cld_status"],
    #         "timestamp": timestamp
    #     })

    #     print(f"Inserted KafkaData for {machine_name}: CLD Status={details['cld_status']} Timestamp={timestamp}")
 