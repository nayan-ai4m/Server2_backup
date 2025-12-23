from kafka import KafkaConsumer
import json

# Kafka consumer setup
consumer = KafkaConsumer(
    "l3l4_no_rtsp",  # The Kafka topic to subscribe to
    bootstrap_servers=["localhost:9092"],  # Kafka broker addresses
    auto_offset_reset="latest",  # Start from the earliest available message
    enable_auto_commit=True,  # Automatically commit offsets
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # Deserialize the JSON message
    group_id="loop4_consumer_group"  # Consumer group ID (optional but useful for load balancing)
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
    
    # Here you can process the message further or store it
    # For example, you can append it to a list or store it in a file.
    # For now, let's just print it out.

