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
INSERT INTO taping(timestamp, loop3_status, loop4_status) VALUES(%s, %s, %s);
"""

# Function to read a tag from PLC
def read_plc_tag(tag_queue):
    """Read a tag from PLC and put it into the queue."""
    while True:
        try:
            plc = SLCDriver("141.141.143.69")
            plc1 = SLCDriver("141.141.143.70")
            plc.open()
            plc1.open()
            while True:
                try:
                    # Reading the tag values
                    loop3_status = plc.read("B3:0/0").value
                    loop4_status = plc1.read("I:0/0").value

                    # Convert boolean to integer
                    loop3_status = 1 if loop3_status else 0
                    loop4_status = 1 if loop4_status else 0

                    tag_queue.put((loop3_status, loop4_status))
                    time.sleep(30)

                except Exception as e:
                    print(f"Error reading from PLC: {e}")
                    # If PLC read fails, insert 2
                    tag_queue.put((2, 2))
                    time.sleep(5)

        except Exception as e:
            print("PLC connection failed, retrying in 5 seconds...")
            tag_queue.put((2, 2))  # Insert 2 when PLC is unreachable
            time.sleep(5)

# Function to insert the tag value into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert the tag value into PostgreSQL."""
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    while True:
        if not tag_queue.empty():
            data = tag_queue.get()
            timestamp = datetime.datetime.now()
            cursor.execute(insert_query, (str(timestamp), data[0], data[1]))
            conn.commit()
            print(f"Inserted: Timestamp={timestamp}, Loop3={data[0]}, Loop4={data[1]}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    tag_queue = mp.Queue()

    # Create separate processes
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue,))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    # Start processes
    plc_process.start()
    db_process.start()

    # Join processes
    plc_process.join()
    db_process.join()

