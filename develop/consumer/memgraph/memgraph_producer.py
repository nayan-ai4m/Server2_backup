import time
import json
from gqlalchemy import Memgraph
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_BROKER = "192.168.1.149:9092"  # Change if your Kafka broker is different
KAFKA_TOPIC = "cctv_status"

# Connect to Memgraph
db = Memgraph("localhost", 7687)  # Change IP/port if needed

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# def get_node_properties(node_str):
#     properties_str = node_str.split("properties=")[1].strip("{}>")
#     properties = dict(item.split(": ", 1) for item in properties_str.split(", "))
#     return json.dumps(properties, indent=4)

def get_node_properties(node_str):
    properties_str = node_str.split("properties=")[1].strip("{}>")
    properties = {}
    
    for item in properties_str.split(", "):
        key, value = item.split(": ", 1)
        key = key.strip("'")  # Remove surrounding quotes from keys
        
        # Convert value to appropriate type
        if value == "True":
            value = True
        elif value == "False":
            value = False
        elif value.isdigit():
            value = int(value)
        elif value.replace(".", "", 1).isdigit():
            value = float(value)
        elif value.startswith("'") and value.endswith("'"):
            value = value.strip("'")
        
        properties[key] = value
    
    return properties

def fetch_and_send_machine_data():
    """Fetch all Machine nodes from Memgraph and send to Kafka."""
    query = "MATCH (m:kafkaData) RETURN m"
    results = db.execute_and_fetch(query)
    
    for result in results:
        machine_data = result["m"]  # Extract machine node
        node_data = get_node_properties(str(machine_data))
        print(type(node_data))
        # Convert to JSON-serializable format
        data = {
            "name": node_data.get("name", "None"),
            "source_id": node_data.get("source_id", None),
            "orientation": node_data.get("orientation", None),
            "num_boxes": node_data.get("num_boxes", 0),
            "cld_status": node_data.get("cld_status", 0),
            "plc_status": node_data.get("plc_status", None),
            "last_updated": node_data.get("last_updated", None),
        }

        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent to Kafka: {data}")

if __name__ == "__main__":
    while True:
        fetch_and_send_machine_data()
        time.sleep(1)  # Wait 1 second before fetching again