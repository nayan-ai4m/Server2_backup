from kafka import KafkaConsumer
import json

KAFKA_BROKER = '192.168.1.168:9092'  # Use your Kafka broker
KAFKA_TOPIC = 'loop3'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

for message in consumer:
    data = message.value
    linecross = data.get('linecross', {})
    
    # Check if any linecross value is 1
    if any(value == 1 for value in linecross.values()):
        print("Received:", data)


