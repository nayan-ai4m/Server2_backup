from kafka import KafkaConsumer
from json import loads
import uuid
import threading
import time

# memgraph = Memgraph()
consumer = KafkaConsumer(
    "loop4",
    bootstrap_servers='192.168.1.168:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=str(uuid.uuid4()),
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)


print("Kafka to Memgraph service started...")

for message in consumer:
    data = message.value
    print(data)
