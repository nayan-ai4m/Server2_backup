import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'cctv_status_l4',# 'cctv_l3_2',
    bootstrap_servers='192.168.1.168:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='status-monitor-group',
    enable_auto_commit=True
)

print("[Kafka] Consumer started...")

for message in consumer:
    data = message.value
    print(f"[Kafka] Received message: {data}")

