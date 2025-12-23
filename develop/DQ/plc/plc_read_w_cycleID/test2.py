import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# PostgreSQL connection settings
DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}
insert_query = """
INSERT INTO mc17(
	"timestamp", ver_sealer_front_1_temp, ver_sealer_front_2_temp, ver_sealer_front_3_temp, ver_sealer_front_4_temp, ver_sealer_front_5_temp, ver_sealer_front_6_temp, ver_sealer_front_7_temp, ver_sealer_front_8_temp, ver_sealer_front_9_temp, ver_sealer_front_10_temp, ver_sealer_front_11_temp, ver_sealer_front_12_temp, ver_sealer_front_13_temp, ver_sealer_rear_1_temp, ver_sealer_rear_2_temp, ver_sealer_rear_3_temp, ver_sealer_rear_4_temp, ver_sealer_rear_5_temp, ver_sealer_rear_6_temp, ver_sealer_rear_7_temp, ver_sealer_rear_8_temp, ver_sealer_rear_9_temp, ver_sealer_rear_10_temp, ver_sealer_rear_11_temp, ver_sealer_rear_12_temp, ver_sealer_rear_13_temp, hor_sealer_rear_1_temp, hor_sealer_front_1_temp, hor_sealer_rear_2_temp, hor_sealer_rear_3_temp, hor_sealer_rear_4_temp, hor_sealer_rear_5_temp, hor_sealer_rear_6_temp, hor_sealer_rear_7_temp, hor_sealer_rear_8_temp, hor_sealer_rear_9_temp, hopper_1_level, hopper_2_level, piston_stroke_length, hor_sealer_current, ver_sealer_current, rot_valve_1_current, fill_piston_1_current, fill_piston_2_current, rot_valve_2_current, web_puller_current, hor_sealer_position, ver_sealer_position, rot_valve_1_position, fill_piston_1_position, fill_piston_2_position, rot_valve_2_position, web_puller_position, sachet_count, cld_count, status, status_code, cam_position, pulling_servo_current, pulling_servo_position, hor_pressure, ver_pressure, eye_mark_count, spare1)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# Global variables for cycle tracking
class CycleTracker:
    def __init__(self):
        self.cycle_id = 1
        self.last_cam_position = None
        self.current_day = None

def connect_to_plc(plc_ip):
    """Establish connection to PLC with retry mechanism"""
    while True:
        try:
            plc = LogixDriver(plc_ip)
            plc.open()
            logging.info("Successfully connected to PLC")
            return plc
        except Exception as e:
            logging.error(f"Failed to connect to PLC: {e}")
            logging.info("Retrying PLC connection in 5 seconds...")
            time.sleep(5)

def connect_to_database():
    """Establish connection to database with retry mechanism"""
    while True:
        try:
            conn = psycopg2.connect(**DB_SETTINGS)
            logging.info("Successfully connected to database")
            return conn
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            logging.info("Retrying database connection in 5 seconds...")
            time.sleep(5)

# Function to read a tag from PLC
def read_plc_tag(tag_queue, plc_ip):
    """Read a tag from PLC and put it into the queue."""
    while True:
        plc = None
        try:
            plc = connect_to_plc(plc_ip)
            while True:
                try:
                    # Reading the tag from the PLC
                    tag_value = plc.read("MC17")
                    tag_queue.put(tag_value.value)
                    time.sleep(0.04)
                except Exception as e:
                    logging.error(f"Error reading from PLC: {e}")
                    raise  # Raise the exception to trigger reconnection
        except Exception as e:
            logging.error(f"PLC connection lost: {e}")
            if plc:
                try:
                    plc.close()
                except:
                    pass
            logging.info("Attempting to reconnect to PLC in 5 seconds...")
            time.sleep(5)

# Function to insert the tag value into PostgreSQL
def insert_into_postgres(tag_queue):
    """Insert the tag value into PostgreSQL."""
    while True:
        conn = None
        cursor = None
        try:
            conn = connect_to_database()
            cursor = conn.cursor()
            
            # Initialize cycle tracker
            cycle_tracker = CycleTracker()
            
            while True:
                try:
                    # Get tag value from the queue
                    if not tag_queue.empty():
                        data = tag_queue.get()
                        timestamp = datetime.datetime(data['Year'], data['Month'], data['Day'], data['Hour'], data['Min'], data['Sec'], data['Microsecond'])
                        
                        # Check for day change
                        current_day = timestamp.date()
                        if cycle_tracker.current_day is None:
                            cycle_tracker.current_day = current_day
                        elif current_day != cycle_tracker.current_day:
                            cycle_tracker.cycle_id = 1
                            cycle_tracker.current_day = current_day
                        
                        # Check for cycle completion
                        current_cam_position = data['MC_Cam_Position']
                        if cycle_tracker.last_cam_position is not None:
                            position_diff = abs(current_cam_position - cycle_tracker.last_cam_position)
                            if position_diff > 280:
                                cycle_tracker.cycle_id += 1
                        
                        cycle_tracker.last_cam_position = current_cam_position

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
                            data['MC_Ver_Pressure'], data['MC_Eye_Mark_Count'],
                            cycle_tracker.cycle_id
                        ))
                        conn.commit()
                except psycopg2.Error as e:
                    logging.error(f"Database error: {e}")
                    raise  # Raise the exception to trigger reconnection
                
        except Exception as e:
            logging.error(f"Database connection lost: {e}")
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass
            logging.info("Attempting to reconnect to database in 5 seconds...")
            time.sleep(5)

if __name__ == '__main__':
    # Queue for communicating between processes
    tag_queue = mp.Queue()

    # PLC IP and tag name
    plc_ip = '141.141.141.128'
    tag_name = 'MC117'

    # Creating separate processes
    plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
    db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))

    # Starting processes
    plc_process.start()
    db_process.start()

    # Joining processes
    plc_process.join()
    db_process.join()
