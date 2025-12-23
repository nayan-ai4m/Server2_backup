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
    mc_27 = ["T3_4[10].DN","HOURLY_CLD.LOOP3_4[80]","HOURLY_CLD.LOOP3_4[81]","HOURLY_CLD.LOOP3_4[82]","HOURLY_CLD.LOOP3_4[83]","HOURLY_CLD.LOOP3_4[84]","HOURLY_CLD.LOOP3_4[85]","HOURLY_CLD.LOOP3_4[86]","HOURLY_CLD.LOOP3_4[87]","HOURLY_CLD.LOOP3_4[208]","HOURLY_CLD.LOOP3_4[209]","HOURLY_CLD.LOOP3_4[210]","HOURLY_CLD.LOOP3_4[211]","HOURLY_CLD.LOOP3_4[212]","HOURLY_CLD.LOOP3_4[213]","HOURLY_CLD.LOOP3_4[214]","HOURLY_CLD.LOOP3_4[215]","HOURLY_CLD.LOOP3_4[336]","HOURLY_CLD.LOOP3_4[337]","HOURLY_CLD.LOOP3_4[338]","HOURLY_CLD.LOOP3_4[339]","HOURLY_CLD.LOOP3_4[340]","HOURLY_CLD.LOOP3_4[341]","HOURLY_CLD.LOOP3_4[342]","HOURLY_CLD.LOOP3_4[343]"]
    mc_30 = ["T3_4[13].DN","HOURLY_CLD.LOOP3_4[104]","HOURLY_CLD.LOOP3_4[105]","HOURLY_CLD.LOOP3_4[106]","HOURLY_CLD.LOOP3_4[107]","HOURLY_CLD.LOOP3_4[108]","HOURLY_CLD.LOOP3_4[109]","HOURLY_CLD.LOOP3_4[110]","HOURLY_CLD.LOOP3_4[111]","HOURLY_CLD.LOOP3_4[232]","HOURLY_CLD.LOOP3_4[233]","HOURLY_CLD.LOOP3_4[234]","HOURLY_CLD.LOOP3_4[235]","HOURLY_CLD.LOOP3_4[236]","HOURLY_CLD.LOOP3_4[237]","HOURLY_CLD.LOOP3_4[238]","HOURLY_CLD.LOOP3_4[239]","HOURLY_CLD.LOOP3_4[360]","HOURLY_CLD.LOOP3_4[361]","HOURLY_CLD.LOOP3_4[362]","HOURLY_CLD.LOOP3_4[363]","HOURLY_CLD.LOOP3_4[364]","HOURLY_CLD.LOOP3_4[365]","HOURLY_CLD.LOOP3_4[366]","HOURLY_CLD.LOOP3_4[367]"]
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=30000
    )

    try:
        with LogixDriver(plc_ip) as plc:
            while True:
                try:
                    tag_values = plc.read(*mc_27)
                    mc27_data = {
                        'id': 'mc27',
                        "timestamp": str(datetime.datetime.now()),
                        "status": int(tag_values[0].value == False),
                        'cld_a': str(tag_values[1].value+tag_values[2].value+tag_values[3].value+tag_values[4].value+tag_values[5].value+tag_values[6].value+tag_values[7].value+tag_values[8].value),
                        'cld_b': str(tag_values[2].value),
                        'cld_c': str(tag_values[3].value)
                    }
                    tag_queue.put(mc27_data)
                    producer.send(KAFKA_CONFIG['topic'], value={'mc27': mc27_data['status']})
                    print(mc27_data)

                    tag_values = plc.read(*mc_30)
                    mc30_data = {
                        'id': 'mc30',
                        "timestamp": str(datetime.datetime.now()),
                        "status": int(tag_values[0].value == False),
                        'cld_a': str(tag_values[1].value+tag_values[2].value+tag_values[3].value+tag_values[4].value+tag_values[5].value+tag_values[6].value+tag_values[7].value+tag_values[8].value),
                        'cld_b': str(tag_values[2].value),
                        'cld_c': str(tag_values[3].value)
                    }
                    tag_queue.put(mc30_data)
                    producer.send(KAFKA_CONFIG['topic'], value={'mc30': mc30_data['status']})
                    print(mc30_data)

                    producer.flush()
                    time.sleep(1)
                except Exception as e:
                    print(f"Error reading from PLC: {e}")
                    time.sleep(5)
    except Exception as e:
        print(f"Error connecting to PLC: {e}")
    finally:
        producer.close()

def insert_into_postgres(tag_queue):
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    while True:
        if not tag_queue.empty():
            data = tag_queue.get()
            try:
                insert_query = """INSERT INTO {} (
                    "timestamp", status, cld_count_a, cld_count_b, cld_count_c)
                    VALUES (%s, %s, %s, %s, %s);""".format(data['id'])
                
                cursor.execute(insert_query, (
                    data['timestamp'],
                    data['status'],
                    data['cld_a'],
                    data['cld_b'],
                    data['cld_c']
                ))
                conn.commit()
                print(f"Inserted data: {data}")
            except Exception as e:
                print(f"Error inserting into PostgreSQL: {e}")
                conn.rollback()

if __name__ == '__main__':
    tag_queue = mp.Queue()
    plc_ip = '141.141.143.62'

    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    plc_process.start()
    db_process.start()

    plc_process.join()
    db_process.join()
