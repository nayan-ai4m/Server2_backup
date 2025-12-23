import multiprocessing as mp
import datetime
from pycomm3 import SLCDriver
import psycopg2
import time

# PostgreSQL connection settings
DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}
insert_query = """
INSERT INTO taping(timestamp,loop3_status,loop4_status) VALUES(%s,%s,%s);
"""
# Function to read a tag from PLC
def read_plc_tag(tag_queue, plc_ip):
    """Read a tag from PLC and put it into the queue."""
    while True:
        try :
            plc = SLCDriver("141.141.143.69")
            plc1 = SLCDriver("141.141.143.70")
            plc.open()
            plc1.open()
            while True:
                try:
                    # Reading the tag from the PLC
                    loop3_status = plc.read("B3:0/0").value
                    loop4_status = plc1.read("I:0/0").value
                    tag_queue.put((loop3_status,loop4_status))
                    time.sleep(30)
                except Exception as e:
                    print(f"Error reading from PLC: {e}")
        except Exception as e:
            plc.close()
            plc1.close()
            print("Reconnecting")
            time.sleep(5)
# Function to insert the tag value into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert the tag value into PostgreSQL."""
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    while True:
            # Get tag value from the queue
        if not tag_queue.empty():
            data = tag_queue.get()
            timestamp = datetime.datetime.now()
                # Insert the value into PostgreSQL
            cursor.execute(insert_query, (
                    str(timestamp),data[0],data[1]
                ))
            conn.commit()
                #print(f"Inserted tag value: {tag_value} into PostgreSQL")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    # Queue for communicating between processes
    tag_queue = mp.Queue()

    # PLC IP and tag name
    plc_ip = '141.141.141.52'
    tag_name = 'MC19'

    # Creating separate processes
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    # Starting processes
    plc_process.start()
    db_process.start()

    # Joining processes
    plc_process.join()
    db_process.join()

