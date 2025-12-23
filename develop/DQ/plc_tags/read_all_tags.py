from pycomm3 import LogixDriver
import csv
from datetime import datetime

# Connect to the PLC
plc = LogixDriver('141.141.141.139')

# Generate a timestamp for the filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"plc_tags_{timestamp}.csv"

try:
    # Open a connection to the PLC
    with plc:
        # Get the list of all tags
        all_tags = plc.get_tag_list()
        
        # Open CSV file for writing
        with open(csv_filename, 'w', newline='') as csvfile:
            # Create a CSV writer object
            csvwriter = csv.writer(csvfile)
            
            # Write the header row
            csvwriter.writerow(['Tag Name', 'Data Type', 'Dimensions', 'External Access'])
            
            # Write tag information to CSV
            for tag in all_tags:
                csvwriter.writerow([
                    tag['tag_name'],
                    tag['data_type'],
                    tag['dim'] if 'dim' in tag else '',
                    tag['external_access'] if 'external_access' in tag else ''
                ])
        
        print(f"Tag list has been saved to {csv_filename}")

except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    # Ensure the connection is closed
    if plc.connected:
        plc.close()
