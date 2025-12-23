from kafka import KafkaConsumer
import json
import os
import cv2
import numpy as np
from datetime import datetime

# Kafka consumer setup
KAFKA_BROKER = 'localhost:9092'  # Change to your Kafka broker address
KAFKA_TOPIC = 'loop3'  # Change to your Kafka topic
SAVE_DIR = '/home/ai4m/develop/backup/develop/DQ/consumers/Bad_Orintation_images'

# Mapping sensor IDs to video feeds
VIDEO_SOURCES = {
    'A': 'rtsp://admin:unilever2024@192.168.1.21:554/Streaming/Channels/101',  # Change to RTSP or local path
    'B': 'rtsp://admin:unilever2024@192.168.1.20:554/Streaming/Channels/101',
    'C': 'rtsp://admin:unilever2024@192.168.1.22:554/Streaming/Channels/101'

}

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# Track saved object IDs
saved_object_ids = set()

# Open video capture objects
video_caps = {sensor_id: cv2.VideoCapture(path) for sensor_id, path in VIDEO_SOURCES.items()}

def get_frame_at_timestamp(sensor_id, timestamp):
    cap = video_caps.get(sensor_id)
    if cap is None or not cap.isOpened():
        print(f"Error: Video source for {sensor_id} not available.")
        return None
    
    cap.set(cv2.CAP_PROP_POS_MSEC, timestamp)
    ret, frame = cap.read()
    return frame if ret else None

def process_message(message):
    try:
        data = json.loads(message.value.decode('utf-8'))
        objects = data.get('objects', [])
        kafka_timestamp = datetime.strptime(data['@timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        video_timestamp = kafka_timestamp.timestamp() * 1000  # Convert to milliseconds
        
        for obj in objects:
            parts = obj.split('|')
            object_id, x_min, y_min, x_max, y_max, sensor_id, classification = parts[0], float(parts[2]), float(parts[3]), float(parts[4]), float(parts[5]), parts[8], parts[-1]
            
            if classification == 'bad' and object_id not in saved_object_ids:
                saved_object_ids.add(object_id)
                frame = get_frame_at_timestamp(sensor_id, video_timestamp)
                if frame is not None:
                    save_image(frame, object_id, x_min, y_min, x_max, y_max)
    except Exception as e:
        print(f"Error processing message: {e}")

def save_image(frame, object_id, x_min, y_min, x_max, y_max):
    cropped_img = frame[int(y_min):int(y_max), int(x_min):int(x_max)]
    
    if cropped_img.size > 0:
        filename = os.path.join(SAVE_DIR, f"{object_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        cv2.imwrite(filename, cropped_img)
        print(f"Saved image: {filename}")
    else:
        print(f"Skipped empty image for object ID {object_id}")

if __name__ == "__main__":
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER)
    print("Listening for messages...")
    
    for message in consumer:
        process_message(message)
    
    for cap in video_caps.values():
        cap.release()

