#loop4_tp_table

import json
import time
from datetime import datetime
from uuid import uuid4
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

DB_CONFIG = {'host': '192.168.1.168', 'database': 'hul', 'user': 'postgres', 'password': 'ai4m2024'}
KAFKA_CONFIG = {'bootstrap_servers': '192.168.1.168:9092', 'topic': 'cctv_l4_1', 'group_id': 'cctv_processor', 'auto_offset_reset':'latest'}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_table(machine):
    machine = machine.lower()
    if machine in ['pre_taping_l4', 'l4_outfeed']:
        return 'press_tp_status_loop4'
    if machine in ['l4_tapping_outfeed', 'l4_tapping_postoutfeed']:
        return 'tpmc_tp_status_loop4'
    if machine == 'check_weigher_rejection_l4':
        return 'check_weigher_tp_status_loop4'
    if machine in ['highbay_l4', 'cw_outfeed_l4']:
        return 'highbay_tp_status_loop4'
    if machine == 'case_erector_conveyor_l4':
        return 'case_erector_tp_status_loop4'
    if machine.startswith('mc'):
        return f"mc{''.join(filter(str.isdigit, machine.split('_')[0]))}_tp_status"
    return None

    
def get_tp_columns(part):
    part = part.lower()
    allowed_ids = {17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30}

    if any(part == f"mc{i}_cld" for i in allowed_ids):
        return ['tp15']  # Only tp15 for mc{i}_cld
    

    return {
        'infeed': ['tp08', 'tp13', 'tp42', 'tp43', 'tp48', 'tp49'],
        'preinfeed': ['tp09', 'tp14'],
        'postoutfeed': ['tp10'],
        'outfeed': ['tp11'],
        'dropping_zone': ['tp12'],
        'l4_tapping_outfeed': ['tp24'],
        'l4_tapping_postoutfeed': ['tp24'],
        'l4_outfeed': ['tp32'],
        'pre_taping_l4': ['tp31'],
        'outfeed_l4c4_l4': ['tp31'],
        'check_weigher_rejection_l4': ['tp28'],
        'case_erector_conveyor_l4': ['tp40'],
        'highbay_l4': ['tp30'], 
        'cw_outfeed_l4': ['tp30']
    }.get(part, [])


def get_tp_column(status, orient, part):
    status, orient, part = (x.lower() if x else 'none' for x in [status, orient, part])
    allowed_ids = {17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30}

    if part in ['l4_tapping_outfeed', 'l4_tapping_postoutfeed'] and 'bad_orientation' in [status, orient]:
        return 'tp24'
    if part in ['pre_taping_l4', 'outfeed_l4c4_l4'] and 'bad_orientation' in [status, orient]:
        return 'tp31'
    if part == 'l4_outfeed' and 'jamming' in status and 'possible_jamming' not in status:
        return 'tp32'
    if part == 'check_weigher_rejection_l4' and 'jamming' in status and 'possible_jamming' not in status:
        return 'tp28'
    if part in ['case_erector_conveyor_l4'] and 'jamming' in status and 'possible_jamming' not in status:
        return 'tp40'
    if part in ['highbay_l4', 'cw_outfeed_l4'] and 'jamming' in status and 'possible_jamming' not in status:
        return 'tp30'
    if status == 'starvation':
        return 'tp13' if part == 'infeed' else 'tp14' if part == 'preinfeed' else None
    if status == 'jamming':
        return {
            'infeed': 'tp08',
            'preinfeed': 'tp09',
            'post_outfeed': 'tp10',
            'outfeed': 'tp11',
            'dropping_zone': 'tp12'
        }.get(part)
    if status == 'possible_jamming' and part == 'infeed':
        return 'tp48'
    if status == 'possible_starvation' and part == 'infeed':
        return 'tp49'
    if 'bad_orientation' in status or 'bad_orientation' in orient:
        for i in allowed_ids:
            if part == f"mc{i}_cld":
                return 'tp15'
    return None


def get_mapped_machine_part(original_part, tp_column):
    """Map machine_part based on tp_column for database storage"""
    if tp_column == 'tp15':
        # For tp15, map mc{i}_cld to infeed
        allowed_ids = {17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30}
        for i in allowed_ids:
            if original_part.lower() == f"mc{i}_cld":
                return 'infeed'
    return original_part


def format_event(status, orient, machine, part, is_state, tp_column=None):
    # Use mapped machine_part for tp15
    mapped_part = get_mapped_machine_part(part, tp_column) if tp_column else part
    
    return {
        "timestamp": datetime.now().isoformat(),
        "uuid": str(uuid4()),
        "active": 1 if is_state else 0,
        "filepath": "http://192.168.0.158:8015/mc18_detection_1742302886.png",
        "color_code": 3 if is_state else 1,
        "machine_part": mapped_part,
        "status": status,
        "orientation": orient
    }



def normalize_part(machine: str) -> str:
    m = machine.lower()
    if m in ['l4_tapping_outfeed', 'pre_taping_l4',
             'l4_tapping_postoutfeed', "l4_outfeed",
             "check_weigher_rejection_l4", "case_erector_conveyor_l4",
             "highbay_l4", "cw_outfeed_l4"]:
        return m

    parts = m.split('_')
    if len(parts) > 1 and parts[1] == "cld" and m.startswith("mc"):
        mc_num = int(''.join(filter(str.isdigit, machine.split('_')[0])))
        if (17 <= mc_num <= 22) or (25 <= mc_num <= 30):
            return f"mc{mc_num}_cld"

    return parts[1] if len(parts) > 1 else m


def safe_load_json(val):
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None



def update_status(machine, status, orient):
    try:
        part = normalize_part(machine)
        table = get_table(machine)
        if not table:
            print(f"No table for {machine}")
            return

        with get_db_conn() as conn, conn.cursor() as cur:
            # ---------- HEALTHY RESET ----------
            if status and status.lower() == 'healthy':
                cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
                if not cur.fetchone():
                    cur.execute(f"INSERT INTO {table} DEFAULT VALUES")

                for col in get_tp_columns(part):
                    if col == "tp15" and not part.endswith("_cld"):
                        continue

                    # For tp15, use mapped machine_part for query
                    query_part = get_mapped_machine_part(part, col)
                    cur.execute(f"SELECT {col} FROM {table} WHERE {col}->>'machine_part' = %s LIMIT 1", (query_part,))
                    res = cur.fetchone()
                    if not res or not res[0]:
                        continue

                    data = safe_load_json(res[0])
                    if col in ["tp24", "tp31", "tp15"]:
                        if data.get("active") == 1 and data.get("machine_part") == query_part and (
                            "bad_orientation" in (data.get("status") or "").lower() or
                            "bad_orientation" in (orient or "").lower()
                        ):
                            print(f"⚠️ Skipping healthy reset for {col} of {machine} (persistent bad_orientation)")
                            continue

                    data.update({
                        "active": 0,
                        "color_code": 1,
                        "timestamp": datetime.now().isoformat()
                    })
                    cur.execute(f"UPDATE {table} SET {col} = %s WHERE {col}->>'machine_part' = %s",
                                (json.dumps(data), query_part))
                    print(f"Set {col} healthy in {table} for {machine}")
                conn.commit()
                return

            # ---------- ALERT / ACTIVE STATE ----------
            is_state = (status and status.lower() in [
                'starvation', 'jamming', 'possible_jamming',
                'possible_starvation', 'bad_orientation & starvation',
                'bad_orientation & jamming', 'bad_orientation'
            ]) or (orient and orient.lower() == 'bad_orientation')

            if not is_state:
                return

            col = get_tp_column(status, orient, part)
            if not col:
                print(f"No TP column for {status}, {orient}")
                return

            cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
            if not cur.fetchone():
                cur.execute(f"INSERT INTO {table} DEFAULT VALUES")

            # For tp15, use mapped machine_part for query and event creation
            query_part = get_mapped_machine_part(part, col)
            cur.execute(f"SELECT {col} FROM {table} WHERE {col}->>'machine_part' = %s LIMIT 1", (query_part,))
            existing = cur.fetchone()
            new_event = format_event(status, orient, machine, part, True, col)

            if existing and existing[0]:
                existing_data = safe_load_json(existing[0])
                # persistent alert refresh
                if col in ["tp24", "tp31", "tp15"] and existing_data.get("active") == 1:
                    if "bad_orientation" in (existing_data.get("status") or "").lower():
                        existing_data["timestamp"] = datetime.now().isoformat()
                        cur.execute(f"UPDATE {table} SET {col} = %s WHERE {col}->>'machine_part' = %s",
                                    (json.dumps(existing_data), query_part))
                        print(f"Refreshed timestamp for {col} of {machine}")
                    else:
                        cur.execute(f"""
                            UPDATE {table} SET {col} = %s
                            WHERE {col}->>'machine_part' = %s OR {col} IS NULL
                        """, (json.dumps(new_event), query_part))
                        print(f"Alert updated in {col} of {table} for {machine}")
                else:
                    cur.execute(f"""
                        UPDATE {table} SET {col} = %s
                        WHERE {col}->>'machine_part' = %s OR {col} IS NULL
                    """, (json.dumps(new_event), query_part))
                    print(f"Alert updated in {col} of {table} for {machine}")
            else:
                cur.execute(f"""
                    UPDATE {table} SET {col} = %s
                    WHERE {col}->>'machine_part' = %s OR {col} IS NULL
                """, (json.dumps(new_event), query_part))
                print(f"Alert inserted in {col} of {table} for {machine}")
            conn.commit()

    except Exception as e:
        print(f"Error for {machine}: {str(e)}")


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

if __name__ == '__main__': 
    kafka_consumer()


