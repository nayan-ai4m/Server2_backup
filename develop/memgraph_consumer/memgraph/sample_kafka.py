from kafka import KafkaConsumer
import json

KAFKA_TOPIC = "cctv_l3_1"  # or "cctv_l3_2"
KAFKA_BROKER = "192.168.1.168:9092"

def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',  # or 'earliest' if you want old messages
        group_id="debug_consumer_group",  # Change or keep as is
        enable_auto_commit=True
    )

    print(f"[Kafka Consumer] Listening to topic: {KAFKA_TOPIC}")
    for message in consumer:
        print(f"[Kafka Consumer] Received message: {message.value}")

if __name__ == "__main__":
    consume_kafka_messages()

