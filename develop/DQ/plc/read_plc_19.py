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
insert_query = """
INSERT INTO mc19(
	timestamp, ver_sealer_front_1_temp, ver_sealer_front_2_temp, ver_sealer_front_3_temp, ver_sealer_front_4_temp, ver_sealer_front_5_temp, ver_sealer_front_6_temp, ver_sealer_front_7_temp, ver_sealer_front_8_temp, ver_sealer_front_9_temp, ver_sealer_front_10_temp, ver_sealer_front_11_temp, ver_sealer_front_12_temp, ver_sealer_front_13_temp, ver_sealer_rear_1_temp, ver_sealer_rear_2_temp, ver_sealer_rear_3_temp, ver_sealer_rear_4_temp, ver_sealer_rear_5_temp, ver_sealer_rear_6_temp, ver_sealer_rear_7_temp, ver_sealer_rear_8_temp, ver_sealer_rear_9_temp, ver_sealer_rear_10_temp, ver_sealer_rear_11_temp, ver_sealer_rear_12_temp, ver_sealer_rear_13_temp, hor_sealer_rear_1_temp, hor_sealer_front_1_temp, hor_sealer_rear_2_temp, hor_sealer_rear_3_temp, hor_sealer_rear_4_temp, hor_sealer_rear_5_temp, hor_sealer_rear_6_temp, hor_sealer_rear_7_temp, hor_sealer_rear_8_temp, hor_sealer_rear_9_temp, hopper_1_level, hopper_2_level, piston_stroke_length, hor_sealer_current, ver_sealer_current, rot_valve_1_current, fill_piston_1_current, fill_piston_2_current, rot_valve_2_current, web_puller_current, hor_sealer_position, ver_sealer_position, rot_valve_1_position, fill_piston_1_position, fill_piston_2_position, rot_valve_2_position, web_puller_position, sachet_count, cld_count, status, status_code, cam_position, pulling_servo_current, pulling_servo_position, hor_pressure, ver_pressure, eye_mark_count)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
# Function to read a tag from PLC
def read_plc_tag(tag_queue, plc_ip):
    """
    Function to read a tag from the PLC and put it into the queue.
    If connection is lost, it will attempt to reconnect.
    """
    while True:
        try:
            # Establish connection with PLC
            with LogixDriver(plc_ip) as plc:
                while True:
                    try:
                        # Reading the tag "MC17" from the PLC
                        tag_value = plc.read("MC19")
                        if tag_value:
                            tag_queue.put(tag_value.value)
                        time.sleep(0.02)
                    except Exception as e:
                        print(f"Error reading from PLC: {e}. Attempting to reconnect.")
                        break  # Break inner loop to reconnect after a brief pause
        except Exception as e:
            print(f"Failed to connect to PLC: {e}. Retrying in 5 seconds.")
            time.sleep(5)  # Wait before retrying connection
# Function to insert the tag value into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert the tag value into PostgreSQL."""
    conn = psycopg2.connect(**DB_SETTINGS)
    cursor = conn.cursor()

    while True:
            # Get tag value from the queue
        if not tag_queue.empty():
            data = tag_queue.get()
            timestamp = datetime.datetime(data['Year'], data['Month'], data['Day'], data['Hour'], data['Min'], data['Sec'], data['Microsecond'])
                # Insert the value into PostgreSQL
            cursor.execute(insert_query, (
                    str(timestamp),
                    data['MC_Ver_Sealer_Front_1_Temp'], data['MC_Ver_Sealer_Front_2_Temp'], 
                    data['MC_Ver_Sealer_Front_3_Temp'], data['MC_Ver_Sealer_Front_4_Temp'],
                    data['MC_Ver_Sealer_Front_5_Temp'], data['MC_Ver_Sealer_Front_6_Temp'], 
                    data['MC_Ver_Sealer_Front_7_Temp'], data['MC_Ver_Sealer_Front_8_Temp'],
                    data['MC_Ver_Sealer_Front_9_Temp'], data['MC_Ver_Sealer_Front_10_Temp'], 
                    data['MC_Ver_Sealer_Front_11_Temp'], data['MC_Ver_Sealer_Front_12_Temp'],
                    data['MC_Ver_Sealer_Front_13_Temp'], data['MC_Ver_Sealer_Rear_1_Temp'], 
                    data['MC_Ver_Sealer_Rear_2_Temp'], data['MC_Ver_Sealer_Rear_3_Temp'],
                    data['MC_Ver_Sealer_Rear_4_Temp'], data['MC_Ver_Sealer_Rear_5_Temp'], 
                    data['MC_Ver_Sealer_Rear_6_Temp'], data['MC_Ver_Sealer_Rear_7_Temp'],
                    data['MC_Ver_Sealer_Rear_8_Temp'], data['MC_Ver_Sealer_Rear_9_Temp'], 
                    data['MC_Ver_Sealer_Rear_10_Temp'], data['MC_Ver_Sealer_Rear_11_Temp'],
                    data['MC_Ver_Sealer_Rear_12_Temp'], data['MC_Ver_Sealer_Rear_13_Temp'], 
                    data['MC_Hor_Sealer_Rear_1_Temp'], data['MC_Hor_Sealer_Front_1_Temp'], 
                    data['MC_Hor_Sealer_Rear_2_Temp'], data['MC_Hor_Sealer_Rear_3_Temp'],
                    data['MC_Hor_Sealer_Rear_4_Temp'], data['MC_Hor_Sealer_Rear_5_Temp'], 
                    data['MC_Hor_Sealer_Rear_6_Temp'], data['MC_Hor_Sealer_Rear_7_Temp'],
                    data['MC_Hor_Sealer_Rear_8_Temp'], data['MC_Hor_Sealer_Rear_9_Temp'], 
                    data['MC_Hopper_1_Level'], data['MC_Hopper_2_Level'], 
                    data['MC_Piston_Stroke_Length'], data['MC_Hor_Sealer_Current'], 
                    data['MC_Ver_Sealer_Current'], data['MC_Rot_Valve_1_Current'], 
                    data['MC_Fill_Piston_1_Current'], data['MC_Fill_Piston_2_Current'],
                    data['MC_Rot_Valve_2_Current'], data['MC_Web_Puller_Current'], 
                    data['MC_Hor_Sealer_Position'], data['MC_Ver_Sealer_Position'], 
                    data['MC_Rot_Valve_1_Position'], data['MC_Fill_Piston_1_Position'], 
                    data['MC_Fill_Piston_2_Position'], data['MC_Rot_Valve_2_Position'], 
                    data['MC_Web_Puller_Position'], data['MC_Sachet_Count'], 
                    data['MC_CLD_Count'], data['MC_Status'], data['MC_Status_Code'], 
                    data['MC_Cam_Position'], data['MC_Pulling_Servo_Current'], 
                    data['MC_Pulling_Servo_Position'], data['MC_Hor_Pressure'], 
                    data['MC_Ver_Pressure'], data['MC_Eye_Mark_Count'] 
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

