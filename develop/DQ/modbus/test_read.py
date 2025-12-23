from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

# Replace with your Modbus device's IP address and port
MODBUS_SERVER_IP = '141.141.143.1'  # Example IP address
MODBUS_SERVER_PORT = 502            # Default Modbus TCP port

# Define the Modbus unit identifier (usually 1 for Modbus TCP)
UNIT_ID = 1

# Starting address and number of registers to read
START_ADDRESS = 138
REGISTER_COUNT = 4  # Reading registers from address 500 to 525 inclusive

def read_floats_from_registers():
    # Initialize the Modbus TCP client
    client = ModbusTcpClient(host=MODBUS_SERVER_IP, port=MODBUS_SERVER_PORT)
    
    try:
        # Connect to the Modbus server
        connection = client.connect()
        if not connection:
            print(f"Unable to connect to Modbus server at {MODBUS_SERVER_IP}:{MODBUS_SERVER_PORT}")
            return
        
        # Read holding registers
        result = client.read_input_registers(START_ADDRESS, REGISTER_COUNT, slave=UNIT_ID)
        
        # Check for errors
        if result.isError():
            print(f"Error reading registers: {result}")
            return
        
        # Retrieve the registers
        registers = result.registers
        print(registers)    
        # Create a decoder with the correct byte and word order
        """
        decoder = BinaryPayloadDecoder.fromRegisters(
            registers,
            byteorder=Endian.BIG,     # Byte order within each register (most significant byte first)
            wordorder=Endian.LITTLE   # Register order (least significant register first)
        )
        
        # Decode the registers into floats
        float_values = []
        for _ in range(int(REGISTER_COUNT / 2)):
            float_value = decoder.decode_32bit_float()
            float_values.append(float_value)
        
        # Print the float values
        print(float_values[:5]+float_values[10:])
        """
    finally:
        # Close the client connection
        client.close()

if __name__ == "__main__":
    read_floats_from_registers()

