from pycomm3 import LogixDriver
import csv
from datetime import datetime, timedelta
import time
import os
# Connect to the PLC
plc = LogixDriver('141.141.141.82')

def create_csv_with_headers(filename, headers):
    """Create a new CSV file with headers if it doesn't exist"""
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

def log_plc_data(end_time):
    # Generate filename with current timestamp
    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"plc_data_141_141_141_82.csv"
    
    try:
        # Open a connection to the PLC
        with plc:
            # Get the list of all tags first
            all_tags = plc.get_tag_list()
            tag_names = ['Timestamp']  # Start with timestamp as first column
            
            # Collect all tag names
            for tag in all_tags:
                tag_names.append(tag['tag_name'])
            
            # Create CSV with headers
            create_csv_with_headers(csv_filename, tag_names)
            
            print(f"Starting data collection for 1.5 minutes...")
            reading_count = 0
            
            # Read data until we reach the end time
            while datetime.now() < end_time:
                # Current timestamp for this reading
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                
                # Read all tag values
                values = [current_time]  # Start with timestamp
                for tag in all_tags:
                    try:
                        tag_value = plc.read(tag['tag_name']).value
                        values.append(str(tag_value))
                    except Exception as tag_error:
                        print(f"Error reading tag {tag['tag_name']}: {str(tag_error)}")
                        values.append("ERROR")
                
                # Append the values to the CSV
                with open(csv_filename, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(values)
                
                reading_count += 1
                print(f"\rReadings taken: {reading_count}", end='')
                
                # Small delay to prevent overwhelming the PLC
                #time.sleep(0.1)  # 100ms delay between readings
            
            print(f"\nData collection completed. Total readings: {reading_count}")
            print(f"Data saved to: {csv_filename}")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    print("Starting PLC data logging program...")
    
    try:
        # Calculate end time (1.5 minutes from now)
        end_time = datetime.now() + timedelta(minutes=9, seconds=30)
        
        log_plc_data(end_time)
            
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    finally:
        # Ensure the connection is closed
        if plc.connected:
            plc.close()
        print("Program completed.")

if __name__ == "__main__":
    main()
