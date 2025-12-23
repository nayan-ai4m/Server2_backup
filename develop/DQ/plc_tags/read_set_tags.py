from pycomm3 import LogixDriver
import time
import csv
from datetime import datetime

# Define all tags
tags = [
    'Shift_1_Data', 'Shift_2_Data', 'Shift_3_Data', 'Hopper_Level_Percentage',
    'Machine_Speed_PPM', 
    'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3',
    'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6',
    'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9',
    'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12',
    'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15',
    'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18',
    'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21',
    'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24',
    'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Hor_Seal_Front_27',
    'HMI_Hor_Seal_Rare_27', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2',
    'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'MC17_Hor_Torque',
    'MC17_Ver_Torque', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg',
    'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg',
    'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg',
    'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END',
    'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START',
    'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START'
]

try:
    # Open CSV file and write header
    with open('data22.csv', 'w', newline='') as csvfile:
        fieldnames = ['Timestamp'] + tags
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Initialize PLC connection
        with LogixDriver('141.141.141.82') as plc:
            while True:
                # Create a dictionary for the current row
                row_data = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}
                
                # Read all tags
                for tag in tags:
                    tag_data = plc.read(tag)
                    row_data[tag] = tag_data.value if tag_data else None
                
                # Write the row to CSV
                writer.writerow(row_data)
                csvfile.flush()  # Ensure data is written to disk
                
                # Wait for 0.1 seconds
                time.sleep(0.1)

except KeyboardInterrupt:
    print("\nLogging stopped by user")
except Exception as e:
    print(f"An error occurred: {str(e)}")
