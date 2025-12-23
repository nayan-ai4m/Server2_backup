from pycomm3 import LogixDriver
import time

# Initialize PLC connection
with LogixDriver('141.141.141.138') as plc:
    total_time = 0
    
    for _ in range(3):
        start_time = time.time()
        
        # Read the structured tag once
        tag_name = 'HMI_I_Start'
        tag_data = plc.read(tag_name)

        if tag_data:
            data = tag_data.value

            # Extract individual components with error handling
            #control = data.get('Control', 'N/A')
            #pre = data.get('PRE', 'N/A')
            #acc = data.get('ACC', 'N/A')
            #cu = data.get('CU', 'N/A')
            #cd = data.get('CD', 'N/A')
            #dn = data.get('DN', 'N/A')
            #ov = data.get('OV', 'N/A')
            #un = data.get('UN', 'N/A')

            # Print the data
            #print(f"Iteration {_+1}: Control: {control}, PRE: {pre}, ACC: {acc}, CU: {cu}, CD: {cd}, DN: {dn}, OV: {ov}, UN: {un}")
            print(data)

        else:
            print(f"Failed to read tag {tag_name}")

        end_time = time.time()
        iteration_time = end_time - start_time
        total_time += iteration_time
    
    print(f"Total time for 100 iterations: {total_time:.2f} seconds")

