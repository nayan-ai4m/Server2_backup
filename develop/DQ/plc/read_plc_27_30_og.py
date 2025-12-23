import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver,SLCDriver
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

# Function to read tags from PLC
def read_plc_tag(tag_queue, plc_ip):
    """Read tags from PLC and put them into the queue."""
    mc_27 = ["T3_4[10].DN","HOURLY_CLD.LOOP3_4[80]","HOURLY_CLD.LOOP3_4[81]","HOURLY_CLD.LOOP3_4[82]"]
    mc_30 = ["T3_4[13].DN","HOURLY_CLD.LOOP3_4[104]","HOURLY_CLD.LOOP3_4[105]","HOURLY_CLD.LOOP3_4[106]"]
    try:
        with LogixDriver(plc_ip) as plc:
            while True:
                try:
                    # Reading the tags from the PLC
                    tag_values = plc.read(*mc_27)
                    # Store tag values in a dictionary
                    tags = {'id':'mc27',
                            "timestamp": str(datetime.datetime.now()),
                        "status":int (tag_values[0].value == False),
                        'cld_a':str(tag_values[1].value),
                        'cld_b':str(tag_values[2].value),
                        'cld_c':str(tag_values[3].value),
                    }
                    tag_queue.put(tags)
                    print(tags)
                    tag_values = plc.read(*mc_30)
                    # Store tag values in a dictionary
                    tags = {'id':'mc30',
                        "timestamp": str(datetime.datetime.now()),
                        "status": int (tag_values[0].value == False ),
                        'cld_a':str(tag_values[1].value),
                        'cld_b':str(tag_values[2].value),
                        'cld_c':str(tag_values[3].value),
                    }
                    print(tags)
                    tag_queue.put(tags)
                    time.sleep(1)  # Read every 60 seconds
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
                print(data)
                # Insert the values into PostgreSQL
                insert_query = """INSERT INTO {} (
	"timestamp",status,cld_count_a,cld_count_b,cld_count_c)
	VALUES (%s, %s, %s, %s, %s);""".format(data['id'])
                print(insert_query)
                
                cursor.execute(insert_query, (str(datetime.datetime.now()),
                    int(data['status']),
                    data['cld_a'],
                    data['cld_b'],
                    data['cld_c'],
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
    plc_ip = '141.141.143.62'

    # Creating separate processes for reading from PLC and inserting into PostgreSQL
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    # Starting processes
    plc_process.start()
    db_process.start()

    # Joining processes
    plc_process.join()
    db_process.join()
