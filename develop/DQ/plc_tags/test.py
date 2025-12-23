import multiprocessing as mp
import datetime
from pycomm3 import LogixDriver
import psycopg2
import time
from typing import Dict, Any

# PostgreSQL connection settings
DB_SETTINGS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Expected PLC tags 
EXPECTED_TAGS = [
    'Year', 'Month', 'Day', 'Hour', 'Min', 'Sec', 'Microsecond',
    'MC_Ver_Sealer_Front_1_Temp', 'MC_Ver_Sealer_Front_2_Temp',
    'MC_Ver_Sealer_Front_3_Temp', 'MC_Ver_Sealer_Front_4_Temp',
    'MC_Ver_Sealer_Front_5_Temp', 'MC_Ver_Sealer_Front_6_Temp',
    'MC_Ver_Sealer_Front_7_Temp', 'MC_Ver_Sealer_Front_8_Temp',
    'MC_Ver_Sealer_Front_9_Temp', 'MC_Ver_Sealer_Front_10_Temp',
    'MC_Ver_Sealer_Front_11_Temp', 'MC_Ver_Sealer_Front_12_Temp',
    'MC_Ver_Sealer_Front_13_Temp', 'MC_Ver_Sealer_Rear_1_Temp',
    'MC_Ver_Sealer_Rear_2_Temp', 'MC_Ver_Sealer_Rear_3_Temp',
    'MC_Ver_Sealer_Rear_4_Temp', 'MC_Ver_Sealer_Rear_5_Temp',
    'MC_Ver_Sealer_Rear_6_Temp', 'MC_Ver_Sealer_Rear_7_Temp',
    'MC_Ver_Sealer_Rear_8_Temp', 'MC_Ver_Sealer_Rear_9_Temp',
    'MC_Ver_Sealer_Rear_10_Temp', 'MC_Ver_Sealer_Rear_11_Temp',
    'MC_Ver_Sealer_Rear_12_Temp', 'MC_Ver_Sealer_Rear_13_Temp',
    'MC_Hor_Sealer_Rear_1_Temp', 'MC_Hor_Sealer_Front_1_Temp',
    'MC_Hor_Sealer_Rear_2_Temp', 'MC_Hor_Sealer_Rear_3_Temp',
    'MC_Hor_Sealer_Rear_4_Temp', 'MC_Hor_Sealer_Rear_5_Temp',
    'MC_Hor_Sealer_Rear_6_Temp', 'MC_Hor_Sealer_Rear_7_Temp',
    'MC_Hor_Sealer_Rear_8_Temp', 'MC_Hor_Sealer_Rear_9_Temp',
    'MC_Hopper_1_Level', 'MC_Hopper_2_Level',
    'MC_Piston_Stroke_Length', 'MC_Hor_Sealer_Current',
    'MC_Ver_Sealer_Current', 'MC_Rot_Valve_1_Current',
    'MC_Fill_Piston_1_Current', 'MC_Fill_Piston_2_Current',
    'MC_Rot_Valve_2_Current', 'MC_Web_Puller_Current',
    'MC_Hor_Sealer_Position', 'MC_Ver_Sealer_Position',
    'MC_Rot_Valve_1_Position', 'MC_Fill_Piston_1_Position',
    'MC_Fill_Piston_2_Position', 'MC_Rot_Valve_2_Position',
    'MC_Web_Puller_Position', 'MC_Sachet_Count',
    'MC_CLD_Count', 'MC_Status', 'MC_Status_Code',
    'MC_Cam_Position', 'MC_Pulling_Servo_Current',
    'MC_Pulling_Servo_Position', 'MC_Hor_Pressure',
    'MC_Ver_Pressure', 'MC_Eye_Mark_Count'
]

def check_record_count(cursor):
    """Check and print the current record count in the database."""
    try:
        cursor.execute("SELECT COUNT(*) FROM mc18")
        count = cursor.fetchone()[0]
        print(f"Current record count in database: {count}")
    except Exception as e:
        print(f"Error checking record count: {e}")

def validate_data(data: Dict[str, Any]) -> bool:
    """Validate that all required tags are present in the data."""
    for tag in EXPECTED_TAGS:
        if tag not in data:
            print(f"Missing tag: {tag}")
            return False
    return True

def read_plc_tag(tag_queue: mp.Queue, plc_ip: str) -> None:
    """Read tags from PLC and put them into the queue."""
    while True:
        try:
            with LogixDriver(plc_ip) as plc:
                print("Connected to PLC successfully")
                while True:
                    try:
                        tag_value = plc.read("MC18")
                        if tag_value and tag_value.value:
                            if not hasattr(read_plc_tag, 'first_read'):
                                print("First PLC read data structure:", tag_value.value)
                                read_plc_tag.first_read = True
                            tag_queue.put(tag_value.value)
                        time.sleep(0.04)
                    except Exception as e:
                        print(f"Error reading from PLC: {e}")
                        time.sleep(1)
        except Exception as e:
            print(f"PLC connection error: {e}")
            print("Attempting to reconnect...")
            time.sleep(5)

def insert_into_postgres(tag_queue: mp.Queue) -> None:
    """Insert the tag values into PostgreSQL database."""
    previous_cam = None
    cycle_id = 1
    ref_time = None
    insert_count = 0
    last_count_check = time.time()
    
    while True:
        try:
            with psycopg2.connect(**DB_SETTINGS) as conn:
                print("Connected to PostgreSQL successfully")
                with conn.cursor() as cursor:
                    # Initial record count
                    check_record_count(cursor)
                    
                    while True:
                        if not tag_queue.empty():
                            try:
                                data = tag_queue.get()
                                
                                # Validate data
                                if not validate_data(data):
                                    print("Invalid data structure:", data.keys())
                                    continue
                                
                                # Create timestamp
                                timestamp = datetime.datetime(
                                    data['Year'], data['Month'], data['Day'],
                                    data['Hour'], data['Min'], data['Sec'],
                                    data.get('Microsecond', 0)
                                )
                                
                                # Cycle ID logic
                                current_time = f"{data['Day']}{data['Month']}{data['Year']}"
                                if ref_time is None:
                                    ref_time = current_time
                                
                                if current_time != ref_time:
                                    cycle_id = 1
                                    ref_time = current_time
                                
                                current_cam = data['MC_Cam_Position']
                                if previous_cam is not None:
                                    if abs(current_cam - previous_cam) > 280:
                                        cycle_id += 1
                                previous_cam = current_cam

                                # Create insert data
                                insert_data = [str(timestamp)]
                                for tag in EXPECTED_TAGS[7:]:  # Skip timestamp fields
                                    if tag not in data:
                                        print(f"Missing tag in data: {tag}")
                                        raise ValueError(f"Missing tag: {tag}")
                                    insert_data.append(data[tag])
                                insert_data.append(cycle_id)
                                
                                # Execute insert
                                cursor.execute("""
                                    INSERT INTO mc18(
                                        "timestamp", ver_sealer_front_1_temp, ver_sealer_front_2_temp, 
                                        ver_sealer_front_3_temp, ver_sealer_front_4_temp, 
                                        ver_sealer_front_5_temp, ver_sealer_front_6_temp, 
                                        ver_sealer_front_7_temp, ver_sealer_front_8_temp, 
                                        ver_sealer_front_9_temp, ver_sealer_front_10_temp, 
                                        ver_sealer_front_11_temp, ver_sealer_front_12_temp, 
                                        ver_sealer_front_13_temp, ver_sealer_rear_1_temp, 
                                        ver_sealer_rear_2_temp, ver_sealer_rear_3_temp, 
                                        ver_sealer_rear_4_temp, ver_sealer_rear_5_temp, 
                                        ver_sealer_rear_6_temp, ver_sealer_rear_7_temp, 
                                        ver_sealer_rear_8_temp, ver_sealer_rear_9_temp, 
                                        ver_sealer_rear_10_temp, ver_sealer_rear_11_temp, 
                                        ver_sealer_rear_12_temp, ver_sealer_rear_13_temp, 
                                        hor_sealer_rear_1_temp, hor_sealer_front_1_temp, 
                                        hor_sealer_rear_2_temp, hor_sealer_rear_3_temp, 
                                        hor_sealer_rear_4_temp, hor_sealer_rear_5_temp, 
                                        hor_sealer_rear_6_temp, hor_sealer_rear_7_temp, 
                                        hor_sealer_rear_8_temp, hor_sealer_rear_9_temp, 
                                        hopper_1_level, hopper_2_level, piston_stroke_length, 
                                        hor_sealer_current, ver_sealer_current, rot_valve_1_current, 
                                        fill_piston_1_current, fill_piston_2_current, 
                                        rot_valve_2_current, web_puller_current, hor_sealer_position, 
                                        ver_sealer_position, rot_valve_1_position, 
                                        fill_piston_1_position, fill_piston_2_position, 
                                        rot_valve_2_position, web_puller_position, sachet_count, 
                                        cld_count, status, status_code, cam_position, 
                                        pulling_servo_current, pulling_servo_position, 
                                        hor_pressure, ver_pressure, eye_mark_count, spare1)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, insert_data)
                                
                                conn.commit()
                                insert_count += 1
                                print(f"Successfully inserted record #{insert_count}")
                                
                                # Check record count every minute
                                current_time = time.time()
                                if current_time - last_count_check >= 60:
                                    check_record_count(cursor)
                                    last_count_check = current_time
                                    
                            except Exception as e:
                                print(f"Error processing data: {e}")
                                print("Data received:", data)
                                conn.rollback()
                        else:
                            time.sleep(0.01)
                            
        except Exception as e:
            print(f"Database connection error: {e}")
            print("Attempting to reconnect to database...")
            time.sleep(5)

def main():
    """Main function to start the PLC data logger."""
    mp.set_start_method('fork')
    tag_queue = mp.Queue()
    plc_ip = '141.141.141.138'
    
    try:
        # Create and start processes
        plc_process = mp.Process(target=read_plc_tag, args=(tag_queue, plc_ip))
        db_process = mp.Process(target=insert_into_postgres, args=(tag_queue,))
        
        plc_process.start()
        db_process.start()
        
        # Wait for processes to complete
        plc_process.join()
        db_process.join()
        
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        plc_process.terminate()
        db_process.terminate()
        plc_process.join()
        db_process.join()
        
    except Exception as e:
        print(f"Error in main process: {e}")
        
    finally:
        print("Program terminated")

if __name__ == '__main__':
    main()
