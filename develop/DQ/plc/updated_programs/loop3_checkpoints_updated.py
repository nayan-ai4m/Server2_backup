from pycomm3 import LogixDriver
import psycopg2
import json
from datetime import datetime
import time
import sys
import os

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Function to create zero-filled dictionary for disconnected PLCs
def create_zero_dict(tag_list):
    return {tag: 0 for tag in tag_list}

# Function to read and insert tags into PostgreSQL
def read_and_insert_tags(plc_ip, mc_tags, table_name):
    try:
        with LogixDriver(plc_ip) as plc:
            if not plc.connected:
                print(f"Failed to connect to PLC at {plc_ip}")
                return create_zero_dict(mc_tags)
                
            # Add timeout and retry parameters
            plc.socket_timeout = 5.0  # 5 seconds timeout
            
            tag_data = {}
            response = plc.read(*mc_tags)
            for tags in response:
                tag_data[tags.tag] = tags.value if tags.value is not None else 0
            return tag_data
            
    except Exception as e:
        print(f"Error reading from PLC {plc_ip}: {str(e)}")
        return create_zero_dict(mc_tags)

# Main loop
db_connection = psycopg2.connect(**DB_PARAMS)
while True:
    try:
        # Tag lists for each machine
        mc_17_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
        mc_18_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_1_Level_Percentage", "Hopper_2_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC18_Hor_Torque", "MC18_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
        mc_19_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
        mc_20_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
        mc_21_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
        mc_22_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]

        results = {
            'mc17': read_and_insert_tags('141.141.141.128', mc_17_tags, "mc17_data"),
            'mc18': read_and_insert_tags('141.141.141.138', mc_18_tags, "mc18_data"),
            'mc19': read_and_insert_tags('141.141.141.52', mc_19_tags, "mc19_data"),
            'mc20': read_and_insert_tags('141.141.141.62', mc_20_tags, "mc20_data"),
            'mc21': read_and_insert_tags('141.141.141.72', mc_21_tags, "mc21_data"),
            'mc22': read_and_insert_tags('141.141.141.82', mc_22_tags, "mc22_data")
        }

        insert_query = """INSERT INTO public.loop3_checkpoints(
            "timestamp", mc17, mc18, mc19, mc20, mc21, mc22)
            VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        
        with db_connection.cursor() as cur:
            cur.execute(insert_query, (
                str(datetime.now()),
                json.dumps(results['mc17']),
                json.dumps(results['mc18']),
                json.dumps(results['mc19']),
                json.dumps(results['mc20']),
                json.dumps(results['mc21']),
                json.dumps(results['mc22'])
            ))
            db_connection.commit()
            print("Data inserted successfully")
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"Error at {fname} line {exc_tb.tb_lineno}: {str(e)}")
        
        # Attempt to reconnect to database if connection is lost
        try:
            if db_connection.closed:
                db_connection = psycopg2.connect(**DB_PARAMS)
        except:
            print("Failed to reconnect to database")
            
    time.sleep(30)  # Wait for 60 seconds before next iteration
