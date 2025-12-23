from kafka import KafkaConsumer
import json

KAFKA_BROKER = '192.168.1.168:9092'  # Use your Kafka broker
KAFKA_TOPIC = 'loop3'

#'l3l4_no_rtsp' 

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',  # Start from the latest message
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON
)

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print("Received:", message.value)


