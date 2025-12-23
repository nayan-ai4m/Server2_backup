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
    'topic': 'cctv_status_l4',
    'group_id': 'cctv_processor'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_table_name(machine_name):
    machine_lower = machine_name.lower()
    if machine_lower == 'pre_taping':
        return 'press_tp_status_l4'
    elif machine_lower == 'l4_tapping_outfeed':
        return 'tpmc_tp_status_l4'
    elif machine_lower.startswith('mc'):
        mc_num = ''.join(filter(str.isdigit, machine_lower.split('_')[0]))
        return f"mc{mc_num}_tp_status"
    else:
        return None

def get_tp_columns(machine_part):
    machine_part_lower = machine_part.lower()
    return {
        'infeed': ['tp08', 'tp13', 'tp15', 'tp42', 'tp43', 'tp48', 'tp49'],
        'preinfeed': ['tp09', 'tp14'],
        'postoutfeed': ['tp10'],
        'outfeed': ['tp11'],
        'dropping_zone': ['tp12'],
        'l4_tapping_outfeed': ['tp24'],
        'pre_taping': ['tp31']
    }.get(machine_part_lower, [])

def get_tp_column(status, orientation, machine_part):
    status_lower = status.lower() if status else 'none'
    orientation_lower = orientation.lower() if orientation else 'none'
    machine_part_lower = machine_part.lower()
    
    # Special cases for complete machine names
    if machine_part_lower == 'l4_tapping_outfeed':
        if status_lower == 'bad_orientation' or orientation_lower == 'bad_orientation':
            return 'tp24'
    if machine_part_lower == 'pre_taping':
        if status_lower == 'bad_orientation' or orientation_lower == 'bad_orientation':
            return 'tp31'
    
    # Status mappings for machine parts
    if status_lower == 'starvation':
        if machine_part_lower == 'infeed':
            return 'tp13'
        elif machine_part_lower == 'preinfeed':
            return 'tp14'
    if status_lower == 'jamming':
        if machine_part_lower == 'infeed':
            return 'tp08'
        elif machine_part_lower == 'preinfeed':
            return 'tp09'
        elif machine_part_lower == 'postoutfeed':
            return 'tp10'
        elif machine_part_lower == 'outfeed':
            return 'tp11'
        elif machine_part_lower == 'dropping_zone':
            return 'tp12'
    
    if status_lower == 'possible_jamming':
        if machine_part_lower == 'infeed':
            return 'tp48'
    if status_lower == 'possible_starvation':
        if machine_part_lower == 'infeed':
            return 'tp49'
    if status_lower == 'bad_orientation & starvation':
        if machine_part_lower == 'infeed':
            return 'tp42'
    if status_lower == 'bad_orientation & jamming':
        if machine_part_lower == 'infeed':
            return 'tp43'

    # Orientation mapping
    if orientation_lower == 'bad_orientation' and machine_part_lower == 'infeed':
        return 'tp15'
    
    return None

def format_event_data(status, orientation, machine_name, machine_part, is_problem):
    timestamp = datetime.now().isoformat()
    return {
        "timestamp": timestamp,
        "uuid": str(uuid4()),
        "active": 1 if is_problem else 0,
        "filepath": f"/home/ai4m/{machine_name.lower()}_data/image_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpeg",
        "color_code": 3 if is_problem else 3,
        "machine_part": machine_part
    }

def update_tp_status(machine_name, status, orientation):
    conn = None
    cursor = None
    try:
        # Extract machine part - keep full name for special cases
        machine_part = machine_name.lower()
        if machine_part not in ['l4_tapping_outfeed', 'pre_taping']:
            parts = machine_name.lower().split('_')
            if len(parts) > 1:
                machine_part = parts[1]
        
        table_name = get_table_name(machine_name)
        if not table_name:
            print(f"No table defined for machine: {machine_name}")
            return
        
        # Determine if this is a problem case
        is_problem = False
        problem_statuses = [
            'starvation', 'jamming', 'possible_jamming', 
            'possible_starvation', 'bad_orientation & starvation', 
            'bad_orientation & jamming', 'bad_orientation'
        ]
        if status and status.lower() in problem_statuses:
            is_problem = True
        if orientation and orientation.lower() == 'bad_orientation':
            is_problem = True
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table has any rows
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1;")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            # Create the single row if it doesn't exist
            cursor.execute(f"INSERT INTO {table_name} DEFAULT VALUES;")
            conn.commit()

        if not is_problem and status and status.lower() == 'healthy':
            # For healthy status, update all relevant columns
            possible_columns = get_tp_columns(machine_part)
            if not possible_columns:
                print(f"No TP columns defined for machine part: {machine_part}")
                return
            
            for tp_column in possible_columns:
                cursor.execute(f"""
                    SELECT {tp_column} FROM {table_name} 
                    WHERE {tp_column} IS NOT NULL 
                    AND ({tp_column}->>'machine_part' = %s OR {tp_column}->>'machine_part' IS NULL)
                    LIMIT 1;
                """, (machine_part,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
                    if data.get('active') == 1:
                        data.update({
                            "active": 0,
                            "color_code": 3,
                            "updated_timestamp": datetime.now().isoformat()
                        })
                    else:
                        data.update({
                            "updated_timestamp": datetime.now().isoformat()
                        })
                    cursor.execute(f"""
                        UPDATE {table_name}
                        SET {tp_column} = %s
                        WHERE {tp_column}->>'machine_part' = %s OR {tp_column} IS NULL;
                    """, (json.dumps(data), machine_part))
                    print(f"Healthy update in {tp_column} of {table_name} for {machine_name}")
                else:
                    # If no existing record, create a new healthy event
                    event_data = format_event_data(status, orientation, machine_name, machine_part, False)
                    cursor.execute(f"""
                        UPDATE {table_name}
                        SET {tp_column} = %s
                        WHERE {tp_column}->>'machine_part' = %s OR {tp_column} IS NULL;
                    """, (json.dumps(event_data), machine_part))
                    print(f"Healthy update (new record) in {tp_column} of {table_name} for {machine_name}")
            
            conn.commit()
        else:
            # For problem cases, get the specific TP column
            tp_column = get_tp_column(status, orientation, machine_part)
            if not tp_column:
                print(f"No TP column defined for problem status: {status}, {orientation}")
                return
            
            event_data = format_event_data(status, orientation, machine_name, machine_part, is_problem)
            
            # Update the specific TP column
            update_query = f"""
                UPDATE {table_name}
                SET {tp_column} = %s
                WHERE {tp_column}->>'machine_part' = %s OR {tp_column} IS NULL;
                """

            cursor.execute(f"""
                UPDATE {table_name}
                SET {tp_column} = %s
                WHERE {tp_column}->>'machine_part' = %s OR {tp_column} IS NULL;
            """, (json.dumps(event_data), machine_part))
            conn.commit()
            
            print(f"Alert in {tp_column} of {table_name} for {machine_name}")
        
    except psycopg2.Error as e:
        print(f"Database error for {machine_name}: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
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
            
            update_tp_status(machine_name, status, orientation)

        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == '__main__':
    process_kafka_messages()
