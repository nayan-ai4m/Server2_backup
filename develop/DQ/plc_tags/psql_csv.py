import psycopg2
import pandas as pd
from datetime import datetime

# Database connection parameters
db_params = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

# Get date and month from user
date_value = 27
month_value = 10
year_value = 2024

# Create timestamp range for the specified date
start_timestamp = f"{year_value}-{month_value:02d}-{date_value:02d} 00:00:00+05:30"
end_timestamp = f"{year_value}-{month_value:02d}-{date_value:02d} 23:59:59.999999+05:30"

# Query to execute
query = f"""
    SELECT *
    FROM mc21
    WHERE timestamp >= '{start_timestamp}'::timestamptz 
    AND timestamp < '{end_timestamp}'::timestamptz 
    ORDER BY timestamp DESC;
"""

# Output file path
output_file_path = f'data_21_{date_value}_{month_value}_{year_value}.csv'

# New header
new_header = [
    "timestamp", "ver_sealer_front_1_temp", "ver_sealer_front_2_temp", "ver_sealer_front_3_temp", 
    "ver_sealer_front_4_temp", "ver_sealer_front_5_temp", "ver_sealer_front_6_temp", 
    "ver_sealer_front_7_temp", "ver_sealer_front_8_temp", "ver_sealer_front_9_temp", 
    "ver_sealer_front_10_temp", "ver_sealer_front_11_temp", "ver_sealer_front_12_temp", 
    "ver_sealer_front_13_temp", "ver_sealer_rear_1_temp", "ver_sealer_rear_2_temp", 
    "ver_sealer_rear_3_temp", "ver_sealer_rear_4_temp", "ver_sealer_rear_5_temp", 
    "ver_sealer_rear_6_temp", "ver_sealer_rear_7_temp", "ver_sealer_rear_8_temp", 
    "ver_sealer_rear_9_temp", "ver_sealer_rear_10_temp", "ver_sealer_rear_11_temp", 
    "ver_sealer_rear_12_temp", "ver_sealer_rear_13_temp", "hor_sealer_rear_1_temp", 
    "hor_sealer_front_1_temp", "hor_sealer_rear_2_temp", "hor_sealer_rear_3_temp", 
    "hor_sealer_rear_4_temp", "hor_sealer_rear_5_temp", "hor_sealer_rear_6_temp", 
    "hor_sealer_rear_7_temp", "hor_sealer_rear_8_temp", "hor_sealer_rear_9_temp", 
    "hopper_1_level", "hopper_2_level", "piston_stroke_length", "hor_sealer_current", 
    "ver_sealer_current", "rot_valve_1_current", "fill_piston_1_current", 
    "fill_piston_2_current", "rot_valve_2_current", "web_puller_current", 
    "hor_sealer_position", "ver_sealer_position", "rot_valve_1_position", 
    "fill_piston_1_position", "fill_piston_2_position", "rot_valve_2_position", 
    "web_puller_position", "sachet_count", "cld_count", "status", "status_code", 
    "cam_position", "pulling_servo_current", "pulling_servo_position", "hor_pressure", 
    "ver_pressure", "eye_mark_count", "spare1", "spare2", "spare3", "spare4", "spare5"
]

def replace_csv_header(file_path, new_header):
    try:
        df = pd.read_csv(file_path, header=0)
        df.columns = new_header
        df.to_csv(file_path, index=False)
        print("CSV header replaced successfully.")
    except Exception as e:
        print(f"Error replacing CSV header: {e}")

def export_to_csv():
    try:
        conn = psycopg2.connect(**db_params)
        print(f"Exporting data for date: {date_value}/{month_value}/{year_value}")
        print(f"Time range: {start_timestamp} to {end_timestamp}")
        
        with conn.cursor(name='large_fetch') as cursor:
            cursor.execute(query)
            
            with open(output_file_path, 'w') as f:
                first_chunk = True
                while True:
                    df = pd.DataFrame(cursor.fetchmany(10000))
                    if df.empty:
                        break
                    if first_chunk:
                        df.to_csv(f, index=False)
                        first_chunk = False
                    else:
                        df.to_csv(f, header=False, index=False)
                    
        print(f"Data exported successfully to {output_file_path}")
        replace_csv_header(output_file_path, new_header)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    export_to_csv()
