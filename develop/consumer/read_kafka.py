from kafka import KafkaConsumer
import json

# Kafka consumer setup
consumer = KafkaConsumer(
    "loop4",  # The Kafka topic to subscribe to
    bootstrap_servers=["192.168.1.149:9092"],  # Kafka broker addresses
    auto_offset_reset="latest",  # Start from the earliest available message
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # Deserialize the JSON message
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
    
    # Here you can process the message further or store it
    # For example, you can append it to a list or store it in a file.
    # For now, let's just print it out.

