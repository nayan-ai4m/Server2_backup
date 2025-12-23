from pycomm3 import LogixDriver
import time

# Initialize PLC connection
with LogixDriver('141.141.141.138') as plc:
    total_time = 0
    
    # Tag name
    tag_name = 'HMI_I_Start'
    
    # Read the original value to reset it later
    original_value = plc.read(tag_name).value

    # Example single value to write to the tag
    new_value = 0 # Assuming the tag accepts a single integer valu

    # Write the new value once at the beginning
    plc.write((tag_name, new_value))

    for _ in range(1):
        start_time = time.time()
        
        # Print the current value being used in each iteration
        print(f"Iteration {_+1}: Value: {new_value}")

        end_time = time.time()
        iteration_time = end_time - start_time
        total_time += iteration_time
    
    # Reset the tag to its original value
    #plc.write((tag_name, original_value))
    
    print(f"Total time for 100 iterations: {total_time:.2f} seconds")

