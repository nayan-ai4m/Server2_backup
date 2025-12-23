import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver
import psycopg2
import time

# PostgreSQL connection settings
DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Insert query for PostgreSQL
insert_query = """INSERT INTO loop4_sku("timestamp", "ht_transfer_ts", "level", "primary_tank", "secondary_tank", "batch_no", "mfg_date", "batch_sku", "shift") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

# Function to read tags from PLC
def read_plc_tag(tag_queue, plc_ip):
    """Read tags from PLC and put them into the queue."""
    tag_name = ["MX03_HT_Transfer_TS", "HTloop4_Level", "HTloop4_Primary_Tank", "HTloop4_Secondary_Tank", "MX03_Batch_No_D", "MX03_Batch_Mfg_date", "MX03_Batch_SKU", "MX03_Shift_D"]
    while True:
        try:
            with LogixDriver(plc_ip) as plc:
                while True:
                    try:
                        # Reading the tags from the PLC
                        tag_values = plc.read(*tag_name)
                        # Store tag values in a dictionary
                        tags = {
                            "timestamp": datetime.datetime.now(),
                            "mx03_ht_transfer_ts": tag_values[0].value,
                            "htloop4_level": tag_values[1].value,
                            "htloop4_primary_tank": tag_values[2].value,
                            "htloop4_secondary_tank": tag_values[3].value,
                            "mx03_batch_no_d": tag_values[4].value,
                            "mx03_batch_mfg_date": tag_values[5].value,
                            "mx03_batch_sku": tag_values[6].value,
                            "mx03_shift_d": tag_values[7].value,
                        }
                        tag_queue.put(tags)
                        time.sleep(60)  # Read every 60 seconds
                    except Exception as e:
                        print(f"Error reading from PLC: {e}")
                        time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Error connecting to PLC: {e}")

# Function to insert tag values into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert tag values into PostgreSQL."""
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    while True:
        # Get tag values from the queue
        if not tag_queue.empty():
            data = tag_queue.get()
            try:
                # Insert the values into PostgreSQL
                cursor.execute(insert_query, (
                    data['timestamp'],
                    data['mx03_ht_transfer_ts'],
                    data['htloop4_level'],
                    data['htloop4_primary_tank'],
                    data['htloop4_secondary_tank'],
                    data['mx03_batch_no_d'],
                    data['mx03_batch_mfg_date'],
                    data['mx03_batch_sku'],
                    data['mx03_shift_d']
                ))
                conn.commit()
                print(f"Inserted data: {data}")
            except Exception as e:
                print(f"Error inserting into PostgreSQL: {e}")
                conn.rollback()  # Roll back in case of error

    # Close cursor and connection outside of the loop, if needed (but not reachable in this infinite loop)
    cursor.close()
    conn.close()

if __name__ == '__main__':
    # Queue for communicating between processes
    tag_queue = mp.Queue()

    # PLC IP address
    plc_ip = '141.141.142.31'

    # Creating separate processes for reading from PLC and inserting into PostgreSQL
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    # Starting processes
    plc_process.start()
    db_process.start()

    # Joining processes
    plc_process.join()
    db_process.join()
