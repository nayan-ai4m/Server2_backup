import json
import time
from datetime import datetime
from uuid import uuid4
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.149',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': '192.168.1.149:9092',
    'topic': 'cctv_status',
    'group_id': 'cctv_processor'
}

VALID_INFEED_MACHINES = [f"mc{i}_infeed" for i in range(17, 23)]

# Default values for required fields
DEFAULT_VALUES = {
    'assigned_to': 'System',
    'action': 'Pending',
    'remark': 'Automatically generated',
    'resolution_time': None,
    'assigned_time': datetime.now(),
    'acknowledge': False
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def format_camera_id(machine_name):
    name = machine_name.lower()
    if name == 'pre_taping':
        return 'L3-Pre-TM'
    elif name == 'l3_tapping_outfeed':
        return 'L3-TM'
    elif name.startswith("mc") and "_infeed" in name:
        return machine_name.split("_")[0].upper()
    else:
        return machine_name

def format_event_type(status_or_orientation, machine_name):
    mc_name = machine_name.split("_")[0].upper()

    if status_or_orientation == 'jamming':
        return f"Jamming in {mc_name} infeed"
    elif status_or_orientation == 'starvation':
        return f"Starvation in {mc_name} infeed"
    elif status_or_orientation == 'bad_orientation':
        return f"Bad CLD orientation detected in {mc_name} infeed"
    elif status_or_orientation == 'starvation & bad orientation':
        return f"Starvation and Bad orientation detected in {mc_name} infeed"
    else:
        return status_or_orientation

def record_exists(cursor, camera_id):
    cursor.execute("SELECT 1 FROM event_table WHERE camera_id = %s LIMIT 1", (camera_id,))
    return cursor.fetchone() is not None

def upsert_event(machine_name, event_type_text, camera_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        timestamp = datetime.now()
        filename = f"/home/ai4m/{machine_name}_data/image_{timestamp.strftime('%Y%m%d_%H%M%S')}.jpeg"
        zone = "Smart Camera"
        alert_type = "Breakdown"
        event_id = str(uuid4())

        # Check if record exists first
        exists = record_exists(cursor, camera_id)
        
        if exists:
            # Update existing record
            update_query = """
            UPDATE event_table
            SET 
                timestamp = %s,
                event_type = %s,
                filename = %s,
                remark = %s
            WHERE camera_id = %s
            """
            cursor.execute(update_query, (
                timestamp,
                event_type_text,
                filename,
                event_type_text,  # Using event_type_text as remark
                camera_id
            ))
            print(f"Updated existing record for {camera_id}")
        else:
            # Insert new record with all required fields
            insert_query = """
            INSERT INTO event_table (
                timestamp,
                event_id,
                zone,
                camera_id,
                event_type,
                alert_type,
                filename,
                assigned_to,
                action,
                remark,
                assigned_time,
                acknowledge
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                timestamp,
                event_id,
                zone,
                camera_id,
                event_type_text,
                alert_type,
                filename,
                DEFAULT_VALUES['assigned_to'],
                DEFAULT_VALUES['action'],
                event_type_text,  # Using event_type_text as remark
                DEFAULT_VALUES['assigned_time'],
                DEFAULT_VALUES['acknowledge']
            ))
            print(f"Inserted new record for {camera_id} with ID {event_id}")
        
        conn.commit()
        
    except psycopg2.Error as e:
        print(f"Database error for {machine_name}: {str(e)}")
        if conn:
            conn.rollback()
        
        # Fallback - try minimal insert
        try:
            if conn is None:
                conn = get_db_connection()
            cursor = conn.cursor()
            
            event_id = str(uuid4())
            cursor.execute("""
            INSERT INTO event_table (
                timestamp,
                event_id,
                zone,
                camera_id,
                event_type,
                alert_type,
                remark
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                event_id,
                "Smart Camera",
                camera_id,
                event_type_text,
                "Breakdown",
                event_type_text  # Using event_type_text as remark
            ))
            conn.commit()
            print(f"Inserted minimal record for {camera_id}")
        except Exception as e2:
            print(f"Fallback insert failed for {machine_name}: {str(e2)}")
            if conn:
                conn.rollback()
    finally:
        if conn:
            conn.close()

def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_CONFIG['topic'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id=KAFKA_CONFIG['group_id'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            print(f"Connected to Kafka. Listening on topic '{KAFKA_CONFIG['topic']}'...")
            return consumer
        except NoBrokersAvailable:
            print("Kafka broker not available. Retrying in 5 seconds...")
            time.sleep(5)

def process_kafka_messages():
    consumer = create_kafka_consumer()
    
    for message in consumer:
        try:
            msg = message.value
            print(f"Received: {msg}")
            
            machine_name = msg.get('machine_name', '')
            orientation = msg.get('orientation', '')
            status = msg.get('status', '')
            combined_status = msg.get('status', '').lower()

            machine_name_lower = machine_name.lower()
            camera_id = format_camera_id(machine_name)

            # ---------- Special Case: pre_taping ----------
            if machine_name_lower == 'pre_taping':
                if 'bad_orientation' in orientation:
                    event_type_text = 'Bad Mat Filling'
                    upsert_event(machine_name, event_type_text, camera_id)
                continue

            # ---------- Special Case: l3_tapping_outfeed ----------
            if machine_name_lower == 'l3_tapping_outfeed':
                if 'bad_orientation' in orientation:
                    event_type_text = 'Bad taping detected'
                    upsert_event(machine_name, event_type_text, camera_id)
                continue

            # ---------- Combined status ----------
            if combined_status == 'starvation & bad orientation':
                event_type_text = format_event_type(combined_status, machine_name)
                upsert_event(machine_name, event_type_text, camera_id)

            # ---------- Infeed Bad Orientation ----------
            elif 'infeed' in machine_name_lower and orientation == 'bad_orientation':
                event_type_text = format_event_type(orientation, machine_name)
                upsert_event(machine_name, event_type_text, camera_id)

            # ---------- Jamming or Starvation ----------
            if machine_name_lower in VALID_INFEED_MACHINES:
                if status in ('jamming', 'starvation'):
                    event_type_text = format_event_type(status, machine_name)
                    upsert_event(machine_name, event_type_text, camera_id)

        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == '__main__':
    process_kafka_messages()
