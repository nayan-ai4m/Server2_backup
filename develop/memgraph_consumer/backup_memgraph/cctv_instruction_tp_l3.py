import json
import time
from datetime import datetime
from uuid import uuid4
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

DB_CONFIG = {'host': '192.168.1.149', 'database': 'hul', 'user': 'postgres', 'password': 'ai4m2024'}
KAFKA_CONFIG = {'bootstrap_servers': '192.168.1.149:9092', 'topic': 'cctv_l3_1', 'group_id': 'cctv_processor'}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_table(machine):
    machine = machine.lower()
    if machine == 'pre_taping_l3': return 'press_tp_status'
    if machine == 'outfeed_l3c4_l3': return 'press_tp_status'
    if machine == 'l3_outfeed': return 'press_tp_status'
    if machine == 'l3_tapping_outfeed': return 'tpmc_tp_status'
    if machine == 'l3_tapping_postoutfeed': return 'tpmc_tp_status'
    if machine == 'check_weigher_rejection_l3': return 'check_weigher_tp_status'
    if machine == 'case_erector_conveyor_l3': return 'case_erector_tp_status'
    if machine.startswith('mc'): return f"mc{''.join(filter(str.isdigit, machine.split('_')[0]))}_tp_status"

def get_tp_columns(part):
    part = part.lower()
    return {
        'infeed': ['tp08', 'tp13', 'tp15', 'tp42', 'tp43', 'tp48', 'tp49'],
        'preinfeed': ['tp09', 'tp14'],
        'postoutfeed': ['tp10'],
        'outfeed': ['tp11'],
        'dropping_zone': ['tp12'],
        'l3_tapping_outfeed': ['tp24'],
        'l3_tapping_postoutfeed': ['tp24'],
        'l3_outfeed': ['tp32'],
        'pre_taping_l3': ['tp31'],
        'outfeed_l3c4_l3': ['tp31'],
        'check_weigher_rejection_l3': ['tp28'],
        'case_erector_conveyor_l3': ['tp40']
    }.get(part, [])

def get_tp_column(status, orient, part):
    status, orient, part = (x.lower() if x else 'none' for x in [status, orient, part])
    if part == 'l3_tapping_outfeed' and ('bad_orientation' in [status, orient]): return 'tp24'
    if part == 'l3_tapping_postoutfeed' and ('bad_orientation' in [status, orient]): return 'tp24'
    if part == 'pre_taping_l3' and ('bad_orientation' in [status, orient]): return 'tp31'
    if part == 'outfeed_l3c4_l3' and ('bad_orientation' in [status, orient]): return 'tp31'
    if part == 'l3_outfeed' and ('jamming' in status) and ('possible_jamming' not in status): return 'tp32'
    if part == 'check_weigher_rejection_l3' and ('jamming' in status) and ('possible_jamming' not in status): return 'tp28'
    if part == 'case_erector_conveyor_l3' and ('jamming' in status) and ('possible_jamming' not in status): return 'tp40'
    if status == 'starvation': return 'tp13' if part == 'infeed' else 'tp14' if part == 'preinfeed' else None
    if status == 'jamming': return {'infeed':'tp08', 'preinfeed':'tp09', 'post_outfeed':'tp10', 'outfeed':'tp11', 'dropping_zone':'tp12'}.get(part)
    if status == 'possible_jamming' and part == 'infeed': return 'tp48'
    if status == 'possible_starvation' and part == 'infeed': return 'tp49'
    if 'bad_orientation' in status and part == 'infeed': return 'tp42' if 'starvation' in status else 'tp43' if 'jamming' in status else 'tp15'
    if 'bad_orientation' in status and part == 'outfeed': return 'tp43' if 'jamming' in status else 'tp15'
    #if 'bad_orientation' in status and part == 'post_outfeed': return 'tp10' if 'jamming' in status else 'tp15'
    return None

def format_event(status, orient, machine, part, is_state):
    return {
        "timestamp": datetime.now().isoformat(),
        "uuid": str(uuid4()),
        "active": 1 if is_state else 0,
        "filepath": "http://192.168.0.158:8015/mc18_detection_1742302886.png",
        "color_code": 3 if is_state else 1,
        "machine_part": part
    }

def update_status(machine, status, orient):
    try:
        part = machine.lower()
        if part not in ['l3_tapping_outfeed', 'l3_tapping_postoutfeed', 'pre_taping_l3', "outfeed_l3c4_l3", "l3_outfeed", "check_weigher_rejection_l3", "case_erector_conveyor_l3"]:
            parts = machine.lower().split('_')
            if len(parts) > 1: part = parts[1]
        
        if not (table := get_table(machine)): return print(f"No table for {machine}")
        
        if status and status.lower() == 'healthy':
            with get_db_conn() as conn, conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO {table} DEFAULT VALUES")
                
                for col in get_tp_columns(part):
                    cur.execute(f"SELECT {col} FROM {table} WHERE {col}->>'machine_part' = %s LIMIT 1", (part,))
                    if (res := cur.fetchone()) and res[0]:
                        data = json.loads(res[0]) if isinstance(res[0], str) else res[0]
                        data.update({"active":0, "color_code":1, "timestamp":datetime.now().isoformat()})
                        cur.execute(f"UPDATE {table} SET {col} = %s WHERE {col}->>'machine_part' = %s", (json.dumps(data), part))
                        print(f"Set {col} healthy in {table} for {machine}")
                conn.commit()
            return

        is_state = (status and status.lower() in ['starvation', 'jamming', 'possible_jamming', 'possible_starvation', 
                  'bad_orientation & starvation', 'bad_orientation & jamming', 'bad_orientation']) or \
                 (orient and orient.lower() == 'bad_orientation')
        if not is_state: return

        if not (col := get_tp_column(status, orient, part)): return print(f"No TP column for {status}, {orient}")

        with get_db_conn() as conn, conn.cursor() as cur:
            # Insert default row if table is empty
            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
            if not cur.fetchone():
                cur.execute(f"INSERT INTO {table} DEFAULT VALUES")
            
            # Update the specific column
            cur.execute(f"""
                UPDATE {table} 
                SET {col} = %s 
                WHERE {col}->>'machine_part' = %s OR {col} IS NULL
            """, (json.dumps(format_event(status, orient, machine, part, True)), part))
            conn.commit()
            print(f"Alert in {col} of {table} for {machine}")

    except Exception as e: print(f"Error for {machine}: {str(e)}")

def kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_CONFIG['topic'],
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                group_id=KAFKA_CONFIG['group_id'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            print(f"Connected to Kafka. Listening on {KAFKA_CONFIG['topic']}...")
            for msg in consumer:
                try: update_status(msg.value.get('machine_name'), msg.value.get('status'), msg.value.get('orientation'))
                except Exception as e: print(f"Message error: {str(e)}")
        except NoBrokersAvailable:
            print("Kafka broker not available. Retrying...")
            time.sleep(5)

if __name__ == '__main__': kafka_consumer()
