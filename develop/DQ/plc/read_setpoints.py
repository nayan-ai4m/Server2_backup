from pycomm3 import LogixDriver
import psycopg2
import json
from datetime import datetime

DB_PARAMS = {
    'host': 'localhost',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# Define tag lists for each PLC
mc_17_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Front_27", "HMI_Hor_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC17_Hor_Torque", "MC17_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
mc_18_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_1_Level_Percentage", "Hopper_2_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Hor_Seal_Rear_35", "HMI_Hor_Seal_Rear_36", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC18_Hor_Torque", "MC18_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
mc_19_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC19_Hor_Torque", "MC19_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
mc_20_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC20_Hor_Torque", "MC20_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
mc_21_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC21_Hor_Torque", "MC21_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]
mc_22_tags = ["Shift_1_Data", "Shift_2_Data", "Shift_3_Data", "Hopper_Level_Percentage", "Machine_Speed_PPM", "HMI_Ver_Seal_Front_1", "HMI_Ver_Seal_Front_2", "HMI_Ver_Seal_Front_3", "HMI_Ver_Seal_Front_4", "HMI_Ver_Seal_Front_5", "HMI_Ver_Seal_Front_6", "HMI_Ver_Seal_Front_7", "HMI_Ver_Seal_Front_8", "HMI_Ver_Seal_Front_9", "HMI_Ver_Seal_Front_10", "HMI_Ver_Seal_Front_11", "HMI_Ver_Seal_Front_12", "HMI_Ver_Seal_Front_13", "HMI_Ver_Seal_Rear_14", "HMI_Ver_Seal_Rear_15", "HMI_Ver_Seal_Rear_16", "HMI_Ver_Seal_Rear_17", "HMI_Ver_Seal_Rear_18", "HMI_Ver_Seal_Rear_19", "HMI_Ver_Seal_Rear_20", "HMI_Ver_Seal_Rear_21", "HMI_Ver_Seal_Rear_22", "HMI_Ver_Seal_Rear_23", "HMI_Ver_Seal_Rear_24", "HMI_Ver_Seal_Rear_25", "HMI_Ver_Seal_Rear_26", "HMI_Ver_Seal_Rear_27", "HMI_Ver_Seal_Rear_28", "HMI_Hor_Sealer_Strk_1", "HMI_Hor_Sealer_Strk_2", "HMI_Ver_Sealer_Strk_1", "HMI_Ver_Sealer_Strk_2", "MC22_Hor_Torque", "MC22_Ver_Torque", "HMI_Rot_Valve_Open_Start_Deg", "HMI_Rot_Valve_Open_End_Deg", "HMI_Rot_Valve_Close_Start_Deg", "HMI_Rot_Valve_Close_End_Deg", "HMI_Suction_Start_Deg", "HMI_Suction_End_Degree", "HMI_Filling_Stroke_Deg", "HMI_VER_CLOSE_END", "HMI_VER_CLOSE_START", "HMI_VER_OPEN_END", "HMI_VER_OPEN_START", "HMI_HOZ_CLOSE_END", "HMI_HOZ_CLOSE_START", "HMI_HOZ_OPEN_END", "HMI_HOZ_OPEN_START"]


# Map PLC IPs to tag lists and PostgreSQL table names
plc_map = {
    PLC_IPS[0]: ("mc17", mc17_tags),
    PLC_IPS[1]: ("mc18", mc18_tags),
    PLC_IPS[2]: ("mc19", mc19_tags),
    PLC_IPS[3]: ("mc20", mc20_tags),
    PLC_IPS[4]: ("mc21", mc21_tags),
    PLC_IPS[5]: ("mc22", mc22_tags),
}

# Function to read and insert tags for a given PLC IP and tag list
def read_and_insert_tags(ip, tag_list, table_name):
    # Connect to the PLC
    with LogixDriver(ip) as plc:
        # Establish database connection
        db_connection = psycopg2.connect(**DB_PARAMS)
        db_cursor = db_connection.cursor()

        # Initialize dictionary to hold tag values
        tag_data = {}

        # Read each tag one by one
        for tag in tag_list:
            response = plc.read(tag)
            if response.error:
                print(f"Error reading tag {tag} on PLC {ip}: {response.error}")
                tag_data[tag] = None  # Set value to None if error
            else:
                tag_data[tag] = response.value  # Store the tag value

        # Add timestamp
        tag_data["timestamp"] = datetime.now().isoformat()

        # Convert the dictionary to JSON
        tag_data_json = json.dumps(tag_data)

        # Insert JSON data into PostgreSQL
        try:
            insert_query = f"INSERT INTO {table_name} (data) VALUES (%s);"
            db_cursor.execute(insert_query, (tag_data_json,))
            db_connection.commit()
            print(f"Data inserted successfully into {table_name} for PLC {ip}.")
        except Exception as e:
            db_connection.rollback()
            print(f"Error inserting data into {table_name} for PLC {ip}: {e}")
        finally:
            # Close database connection
            db_cursor.close()
            db_connection.close()

# Loop through each PLC IP, tag list, and corresponding table name
for ip, (tag_key, table_name) in plc_map.items():
    read_and_insert_tags(ip, mc_tags[tag_key], table_name)

