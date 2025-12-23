from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import time
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Replace with your Modbus device's IP address and port
MODBUS_SERVER_IP = '141.141.143.85'  # Replace with your device's IP address
MODBUS_SERVER_PORT = 502             # Default Modbus TCP port

# Define the Modbus unit identifier (usually 1 for Modbus TCP)
UNIT_ID = 1

# Starting address and number of registers to read
START_ADDRESS = 500
REGISTER_COUNT = 26  # Reading registers from address 500 to 525 inclusive

# PostgreSQL connection parameters
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'hul'
DB_USER = 'postgres'
DB_PASSWORD = 'ai4m2024'

def read_floats_from_registers():
    # Initialize the Modbus TCP client
    client = ModbusTcpClient(host=MODBUS_SERVER_IP, port=MODBUS_SERVER_PORT)
    
    # Establish database connection
    try:
        db_connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        db_cursor = db_connection.cursor()
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return

    previous_value = 0.0  # Initialize the previous value to zero

    try:
        while True:
            # Check if the client is connected; if not, try to connect
            if not client.connected:
                connection = client.connect()
                if not connection:
                    print(f"Unable to connect to Modbus server at {MODBUS_SERVER_IP}:{MODBUS_SERVER_PORT}. Retrying in 5 seconds...")
                    time.sleep(5)  # Wait before retrying
                    continue  # Retry the connection

            try:
                # Read holding registers
                result = client.read_holding_registers(START_ADDRESS, REGISTER_COUNT, slave=UNIT_ID)
                
                # Check for errors
                if result.isError():
                    print(f"Error reading registers: {result}. Reconnecting...")
                    client.close()
                    time.sleep(1)
                    continue  # Retry the connection in the next loop iteration
                
                # Retrieve the registers
                registers = result.registers

                # Extract the first two registers (registers 500 and 501)
                first_two_registers = registers[:2]

                # Decode the first float value (from registers 500 and 501)
                decoder = BinaryPayloadDecoder.fromRegisters(
                    first_two_registers,
                    byteorder=Endian.BIG,
                    wordorder=Endian.LITTLE
                )

                current_value = decoder.decode_32bit_float()

                # Check if previous value was zero and current value is not zero
                if previous_value == 0 and current_value != 0:
                    # Decode all floats from the registers
                    decoder_all = BinaryPayloadDecoder.fromRegisters(
                        registers,
                        byteorder=Endian.BIG,
                        wordorder=Endian.LITTLE
                    )
                    
                    float_values = []
                    for _ in range(int(REGISTER_COUNT / 2)):
                        float_value = decoder_all.decode_32bit_float()
                        float_values.append(str(float_value))
                    
                    # Prepare data for insertion
                    timestamp = datetime.now()
                    data_to_insert = []
                    data_to_insert = [str(datetime.now())]+float_values[:5]+float_values[10:] 
                    print(data_to_insert)
                    # Insert data into the database
                    insert_query = """
                        INSERT INTO loop3_checkweigher("timestamp", cld_weight, cld_total, cld_over, cld_under, cld_proper, target_weight, upper_limit, lower_limit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                    try:
                        db_cursor.execute(insert_query,tuple(data_to_insert))
                        db_connection.commit()
                        print(f"Inserted {len(data_to_insert)} records into the database.")
                    except Exception as e:
                        print(f"Failed to insert data into the database: {e}")
                        db_connection.rollback()
                
                # Update the previous value for the next iteration
                previous_value = current_value

            except Exception as e:
                print(f"Exception occurred: {e}. Reconnecting...")
                client.close()
                time.sleep(1)
                continue  # Retry the connection

            # Delay before the next read to prevent excessive polling
            time.sleep(0.5)  # Adjust the delay as needed (in seconds)

    except KeyboardInterrupt:
        print("Program interrupted by user.")

    finally:
        # Close the client connection
        client.close()
        # Close the database connection
        db_cursor.close()
        db_connection.close()

if __name__ == "__main__":
    read_floats_from_registers()

