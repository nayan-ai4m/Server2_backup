from pycomm3 import LogixDriver
from pycomm3.exceptions import CommError
import psycopg2
import json
from datetime import datetime
import time
import sys
import os

# Define the PLC IP address and database connection details
DB_PARAMS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Define all tag lists for mc17 to mc22
mc_17_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

mc_18_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_1_Level_Percentage", "Hopper_2_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC18_Hor_Torque", "MC18_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

mc_19_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC19_Hor_Torque", "MC19_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

mc_20_tags = [
   "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC20_Hor_Torque", "MC20_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

mc_21_tags = [
   "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC21_Hor_Torque", "MC21_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

mc_22_tags = [
    "Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC22_Hor_Torque", "MC22_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"
]

def read_plc_data(plc_ip, mc_tags):
    """
    Read tags from PLC without retrying on connection failure
    """
    try:
        with LogixDriver(plc_ip) as plc:
            # Read each tag one by one
            response = plc.read(*mc_tags)
            return {tags.tag: tags.value for tags in response}
                
    except CommError as e:
        print(f"Cannot connect to PLC {plc_ip}: {str(e)}")
        return {}
                
    except Exception as e:
        print(f"Unexpected error reading from PLC {plc_ip}: {str(e)}")
        return {}

def main():
    """
    Main function with improved error handling and no connection retries
    """
    db_connection = None
    running = True

    # PLC configurations
    plc_configs = {
        'mc17': {'ip': '141.141.141.128', 'tags': mc_17_tags},
        'mc18': {'ip': '141.141.141.138', 'tags': mc_18_tags},
        'mc19': {'ip': '141.141.141.52', 'tags': mc_19_tags},
        'mc20': {'ip': '141.141.141.62', 'tags': mc_20_tags},
        'mc21': {'ip': '141.141.141.72', 'tags': mc_21_tags},
        'mc22': {'ip': '141.141.141.82', 'tags': mc_22_tags}
    }

    try:
        # Initial database connection
        db_connection = psycopg2.connect(**DB_PARAMS)
        print("Database connected successfully")

        while running:
            try:
                # Read data from all PLCs
                plc_data = {}
                for mc_name, config in plc_configs.items():
                    data = read_plc_data(config['ip'], config['tags'])
                    if data:  # Only include data if PLC responded
                        plc_data[mc_name] = data
                    else:
                        plc_data[mc_name] = {}  # Empty dict for non-responding PLCs

                # Insert data only if at least one PLC responded
                if any(plc_data.values()):
                    insert_query = """INSERT INTO public.loop3_checkpoints(
                        timestamp, mc17, mc18, mc19, mc20, mc21, mc22)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                    
                    with db_connection.cursor() as cur:
                        cur.execute(insert_query, (
                            str(datetime.now()),
                            json.dumps(plc_data.get('mc17', {})),
                            json.dumps(plc_data.get('mc18', {})),
                            json.dumps(plc_data.get('mc19', {})),
                            json.dumps(plc_data.get('mc20', {})),
                            json.dumps(plc_data.get('mc21', {})),
                            json.dumps(plc_data.get('mc22', {}))
                        ))
                        db_connection.commit()
                        print(f"Data inserted at {datetime.now()}")
                else:
                    print("No data collected from any PLC")

                time.sleep(60)

            except psycopg2.Error as e:
                print(f"Database error: {str(e)}")
                # Try to reconnect to database
                try:
                    if db_connection:
                        db_connection.close()
                    db_connection = psycopg2.connect(**DB_PARAMS)
                    print("Database reconnected successfully")
                except Exception as e:
                    print(f"Failed to reconnect to database: {str(e)}")
                    time.sleep(5)

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(f"Error in main loop: {exc_type}, {fname}, {exc_tb.tb_lineno}")
                print(f"Error details: {str(e)}")
                time.sleep(5)

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        running = False

    finally:
        if db_connection:
            db_connection.close()
            print("Database connection closed")
        print("Program terminated")

if __name__ == "__main__":
    main()
