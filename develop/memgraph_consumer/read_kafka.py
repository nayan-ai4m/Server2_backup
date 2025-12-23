from kafka import KafkaConsumer
import json
from pprint import pprint
# Kafka consumer setup
consumer = KafkaConsumer(
    "loop4",  # The Kafka topic to subscribe to
    bootstrap_servers=["192.168.1.168:9092"],  # Kafka broker addresses
    auto_offset_reset="latest",  # Start from the earliest available message
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # Deserialize the JSON message
)

# Consume messages
for message in consumer:
#    print(f"Received message: {message.value}")
     data = message.value
     #roi=data.get("roi", {})
     #pprint(data)
     print(data)
