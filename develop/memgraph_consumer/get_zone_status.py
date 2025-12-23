from kafka import KafkaConsumer
import json

KAFKA_BROKER = '192.168.1.149:9092' 
KAFKA_TOPIC = 'zone_status'  

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest', 
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print("Received:", message.value)

