import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
from kafka import KafkaProducer
import json

DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

KAFKA_CONFIG = {
    'bootstrap_servers': '192.168.1.149:9092',
    'topic': 'l4_stoppage_code'
}

def read_plc_tag(tag_queue, plc_ip):
    mc_28 = ["MC28_STATUS", "MC28_FAULT_STS", "MC28_EYE_COUNT", "MC28_SPEED", "MC28_CLD"]
    mc_29 = ["MC29_STATUS", "MC29_FAULT_STS", "MC29_EYE_COUNT", "MC29_SPEED", "MC29_CLD"]
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=30000
    )

    try:
        with LogixDriver(plc_ip) as plc:
            while True:
                try:
                    tag_values = plc.read(*mc_28)
                    mc28_data = {
                        'id': 'mc28',
                        'timestamp': str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        'status': str(tag_values[0].value),
                        'fault_sts': str(tag_values[1].value),
                        'eye_count': str(float(tag_values[2].value)),
                        'speed': str(tag_values[3].value),
                        'cld': str(tag_values[4].value)
                    }
                    tag_queue.put(mc28_data)
                    producer.send(KAFKA_CONFIG['topic'], value={'mc28': mc28_data['status']})
                    print(mc28_data)

                    tag_values = plc.read(*mc_29)
                    mc29_data = {
                        'id': 'mc29',
                        'timestamp': str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        'status': str(tag_values[0].value),
                        'fault_sts': str(int(tag_values[1].value)),
                        'eye_count': str(float(tag_values[2].value)),
                        'speed': str(tag_values[3].value),
                        'cld': str(tag_values[4].value)
                    }
                    tag_queue.put(mc29_data)
                    producer.send(KAFKA_CONFIG['topic'], value={'mc29': mc29_data['status']})
                    print(mc29_data)

                    producer.flush()
                    time.sleep(2)
                except Exception as e:
                    print(f"Error reading from PLC: {e}")
                    time.sleep(5)
    except Exception as e:
        print(f"Error connecting to PLC: {e}")
    finally:
        producer.close()
shift_a_start = datetime.time(7, 0)    # 7:00 AM
shift_a_end = datetime.time(15, 0)     # 3:00 PM
shift_b_start = datetime.time(15, 0)    # 3:00 PM
shift_b_end = datetime.time(23, 0)      # 11:00 PM
shift_c_start = datetime.time(23, 0)    # 11:00 PM
shift_c_end = datetime.time(6, 50) 
def insert_into_postgres(tag_queue):
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()


    while True:
        if not tag_queue.empty():
            data = tag_queue.get()
            try:
                print(data['id'])
                insert_query = """INSERT INTO {} ("timestamp", state,cld_a,cld_b,cld_c) VALUES (%s,%s,%s,%s,%s);""".format(data['id'])
                timing = datetime.datetime.strptime(data['timestamp'],"%Y-%m-%d %H:%M:%S")
                if shift_a_start < timing.time() < shift_a_end:
                    print("shift a")
                    cld_a = data['cld']
                    cld_b = 0
                    cld_c = 0
                if shift_b_start < timing.time() < shift_b_end:
                    print("shift b")
                    cld_b = data['cld']
                    cld_a = 0
                    cld_c = 0
                if shift_c_start < timing.time() < shift_c_end:
                    print("shift c")
                    cld_c = data['cld']
                    cld_b = 0
                    cld_a = 0
                cursor.execute(insert_query, (
                    timing,
                    data['status'],
                    cld_a,
                    cld_b,
                    cld_c,
                ))
                conn.commit()
                print(f"Inserted data: {data}")
            except Exception as e:
                print(f"Error inserting into PostgreSQL: {e}")
                conn.rollback()

if __name__ == '__main__':
    tag_queue = mp.Queue()
    plc_ip = '141.141.143.20'

    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    plc_process.start()
    db_process.start()

    plc_process.join()
    db_process.join()
