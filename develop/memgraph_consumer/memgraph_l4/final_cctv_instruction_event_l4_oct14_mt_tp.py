import json
import time
from datetime import datetime
from uuid import uuid4
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Database configuration
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap_servers': '192.168.1.168:9092',
    'topic': 'cctv_l4_2',
    'group_id': 'cctv_processor',
    'auto_offset_reset': 'latest'
}

#VALID_INFEED_MACHINES_RH = [f"mc{i}_infeed_rh" for i in [26, 28, 30]]
#VALID_INFEED_MACHINES_LH = ["mc{i}_infeed_lh" for i in [25, 27, 29]]
VALID_OUTFEED_MACHINES = [f"mc{i}_outfeed" for i in range(25, 31)]
VALID_INFEED_MACHINES_JAMMING = [f"mc{i}_infeed_jamming" for i in range(25, 31)]


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def format_camera_id(machine_name):
    name = machine_name.lower()
    #if name == 'pre_taping_l4':
    #    return 'L4-Pre-TM'
    if name == 'outfeed_L4C4_l4':
        return 'L4-Pre-TM'
    elif name == 'l4_outfeed':
        return 'L4-Pre-TM'
    #elif name == 'l4_tapping_outfeed':
    #    return 'L4-TM'
    #elif name == 'l4_tapping_postOutfeed':
    #    return 'L4-TM'
    elif name == 'check_weigher_rejection_l4':
        return 'L4_EOL'
    elif name == 'case_erector_conveyor_l4':
        return 'L4_EOL'
    elif name == 'highbay_l4':
        return 'L4_High-bay'
    elif name.startswith("mc") and "_infeed" in name:
        return machine_name.split("_")[0].upper()
    else:
        return machine_name

def format_event_type(status_or_orientation, machine_name):
    mc_name = machine_name.split("_")[0].upper()
    direction = "infeed" if "infeed" in machine_name else "outfeed"

    if status_or_orientation == 'jamming':
        return f"CLD Jamming in {mc_name} {direction}"
    elif status_or_orientation == 'starvation':
        return f"Starvation in {mc_name} {direction}"
    elif status_or_orientation == 'bad_orientation':
        return f"Bad orientation detected in {mc_name} {direction}"
    elif status_or_orientation == 'bad_orientation & starvation':
        return f"Bad orientation and Starvation detected in {mc_name} {direction}"
    elif status_or_orientation == 'jamming & bad_orientation':
        return f"Bad orientation and CLD Jamming detected in {mc_name} {direction}"
    elif status_or_orientation == 'possible_starvation':
        return f"Possible starvation in {mc_name} {direction}"
    elif status_or_orientation == 'possible_jamming':
        return f"Possible CLD jamming in {mc_name} {direction}"
    else:
        return f"Unknown event in {mc_name} {direction}"

def insert_event(machine_name, event_type_text, camera_id):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        timestamp = datetime.now()
        filename = f"/home/ai4m/{machine_name}data/image{timestamp.strftime('%Y%m%d_%H%M%S')}.jpeg"
        zone = "Smart Camera"
        alert_type = "Productivity"
        event_id = str(uuid4())

        # Insert new record with only essential fields
        insert_query = """
        INSERT INTO event_table (
            timestamp,
            event_id,
            zone,
            camera_id,
            event_type,
            alert_type,
            filename
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            timestamp,
            event_id,
            zone,
            camera_id,
            event_type_text,
            alert_type,
            filename
        ))
        print(f"Inserted new record for {camera_id} with ID {event_id}")
        
        conn.commit()
        
    except psycopg2.Error as e:
        print(f"Database error for {machine_name}: {str(e)}")
        if conn:
            conn.rollback()
        
        # Minimal fallback insert
        try:
            if conn is None:
                conn = get_db_connection()
            cursor = conn.cursor()
            
            event_id = str(uuid4())
            cursor.execute("""
            INSERT INTO event_table (
                timestamp,
                event_id,
                camera_id,
                event_type
            ) VALUES (%s, %s, %s, %s)
            """, (
                datetime.now(),
                event_id,
                camera_id,
                event_type_text
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

            # Special Case: pre_taping_l4
            # if machine_name == 'pre_taping_l4':
            #     if 'bad_orientation' in orientation:
            #         event_type_text = 'Bad Mat Filling'
            #         insert_event(machine_name, event_type_text, camera_id)
            #     continue

            # Special Case: outfeed_L4C4_l4
            #if machine_name == 'outfeed_L4C4_l4':
            #   if 'bad_orientation' in orientation:
            #        event_type_text = 'Bad Mat Filling'
            #        insert_event(machine_name, event_type_text, camera_id)
            #    continue

            # Special Case: l4_outfeed
            if machine_name == 'l4_outfeed':
                if 'possible_jamming' in status:
                    event_type_text = 'Possible CLD jamming in Pre Taping Conveyor'
                    insert_event(machine_name, event_type_text, camera_id)
                elif 'jamming' in status:
                    event_type_text = 'CLD Jamming in Pre Taping Conveyor'
                    insert_event(machine_name, event_type_text, camera_id)
                continue

            # Special Case: l4_tapping_outfeed
            # if machine_name == 'l4_tapping_outfeed':
            #     if 'bad_orientation' in orientation:
            #         event_type_text = 'Bad taping detected'
            #         insert_event(machine_name, event_type_text, camera_id)
            #     continue
            
            # # Special Case: l4_tapping_postOutfeed
            # if machine_name == 'l4_tapping_postOutfeed':
            #     if 'bad_orientation' in orientation:
            #         event_type_text = 'Bad taping detected'
            #         insert_event(machine_name, event_type_text, camera_id)
            #     continue
            
            # Special Case: check_weigher_rejection_l4
            if machine_name == 'check_weigher_rejection_l4':
                if 'possible_jamming' in status:
                    event_type_text = 'Possible CLD jamming in Loop4 Check Weigher Rejection Zone'
                    insert_event(machine_name, event_type_text, camera_id)
                elif 'jamming' in status:
                    event_type_text = 'CLD Jamming in Loop4 Check Weigher Rejection Zone'
                    insert_event(machine_name, event_type_text, camera_id)
                continue
            
            # Special Case: case_erector_conveyor_l4
            if machine_name == 'case_erector_conveyor_l4':
                if 'possible_jamming' in status:
                    event_type_text = 'Possible CLD jamming in Case Erector Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                elif 'jamming' in status:
                    event_type_text = 'CLD Jamming in Case Erector Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                continue

            # Special Case: Loop 4 HighBay
            if machine_name == 'highbay_l4':
                if 'possible_jamming' in status:
                    event_type_text = 'Possible CLD jamming in Highbay Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                elif 'jamming' in status:
                    event_type_text = 'CLD Jamming in Highbay Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                continue

            # Special Case: Loop 4 HighBay
            if machine_name == 'cw_outfeed_l4':
                if 'possible_jamming' in status:
                    event_type_text = 'Possible CLD jamming in CW To highbay Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                elif 'jamming' in status:
                    event_type_text = 'CLD Jamming in CW To highbay Coveyor Loop 4'
                    insert_event(machine_name, event_type_text, camera_id)
                continue

            # # Combined status
            # if '_infeed' in machine_name_lower and combined_status in ['bad_orientation & starvation','bad_orientation & jamming']:
            #     event_type_text = format_event_type(combined_status, machine_name)
            #     insert_event(machine_name, event_type_text, camera_id)

            # # Infeed Bad Orientation
            # elif '_infeed' in machine_name_lower and orientation == 'bad_orientation':
            #     event_type_text = format_event_type(orientation, machine_name)
            #     insert_event(machine_name, event_type_text, camera_id)

            # Jamming or Starvation
            #if machine_name_lower in VALID_INFEED_MACHINES:
                #if status in ('jamming', 'starvation', 'possible_jamming','possible_starvation'):
                    #event_type_text = format_event_type(status, machine_name)
                    #insert_event(machine_name, event_type_text, camera_id)
            
            if machine_name_lower in VALID_OUTFEED_MACHINES:
                if status in ('jamming', 'starvation', 'possible_jamming', 'possible_starvation', 'bad_orientation'):
                    event_type_text = format_event_type(status, machine_name)
                    insert_event(machine_name, event_type_text, camera_id)

            if machine_name_lower in VALID_INFEED_MACHINES_JAMMING:
                if status in ('jamming', 'starvation', 'possible_jamming', 'possible_starvation'):
                    event_type_text = format_event_type(status, machine_name)
                    insert_event(machine_name, event_type_text, camera_id)
        
        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == '__main__':
    process_kafka_messages()



