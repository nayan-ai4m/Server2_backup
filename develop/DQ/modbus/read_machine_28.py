from pymodbus.client import ModbusTcpClient
from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
# Configure Modbus client connection parameters


insert_query = """INSERT INTO public.mc28(
	timestamp, vertical_sealer_front_1_temp, vertical_sealer_front_2_temp, vertical_sealer_front_3_temp, vertical_sealer_front_4_temp, vertical_sealer_front_5_temp, vertical_sealer_front_6_temp, vertical_sealer_front_7_temp, vertical_sealer_front_8_temp, vertical_sealer_front_9_temp, vertical_sealer_front_10_temp, vertical_sealer_front_11_temp, vertical_sealer_front_12_temp, vertical_sealer_front_13_temp, vertical_sealer_rear_1_temp, vertical_sealer_rear_2_temp, vertical_sealer_rear_3_temp, vertical_sealer_rear_4_temp, vertical_sealer_rear_5_temp, vertical_sealer_rear_6_temp, vertical_sealer_rear_7_temp, vertical_sealer_rear_8_temp, vertical_sealer_rear_9_temp, vertical_sealer_rear_10_temp, vertical_sealer_rear_11_temp, vertical_sealer_rear_12_temp, vertical_sealer_rear_13_temp, horizontal_servo_position, vertical_servo_position, rotational_valve_position, fill_piston_position, web_puller_position, cam_position, horizontal_sealer_front_1_temp, horizontal_sealer_rear_1_temp, state, position, horizontal_sealing_time, horizontal_sealer_stroke_1, horizontal_sealer_stroke_2, vertical_sealer_stroke_1, vertical_sealer_stroke_2, batch_cut_on_degree, batch_cut_off_degree, vertical_sealer_front_1_setpoint, horizontal_sealer_front_1_setpoint, speed)
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

holding_registers = {'values':[
    {
        "name": "second",
        "register_address": 357,
        "data_type": 16
    },{
        "name": "horizontal_sealing_time",
        "register_address": 170,
        "data_type": 16
    },
    {
        "name": "horizontal_sealer_stroke_1",
        "register_address": 207,
        "data_type": 16
    },
    {
        "name": "horizontal_sealer_stroke_2",
        "register_address": 208,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_stroke_1",
        "register_address": 209,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_stroke_2",
        "register_address": 210,
        "data_type": 16
    },
    {
        "name": "batch_cut_on_degree",
        "register_address": 130,
        "data_type": 16
    },
    {
        "name": "batch_cut_off_degree",
        "register_address": 132,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_1_setpoint",
        "register_address": 1,
        "data_type": 32
    },
    {
        "name": "horizontal_sealer_front_1_setpoint",
        "register_address": 55,
        "data_type": 32
    },
    {
        "name":"speed",
        "register_address":100,
        "data_type":16
    }
]
}

input_registers = {'values':[
    {
        "name": "vertical_sealer_front_1_temp",
        "register_address": 1,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_2_temp",
        "register_address": 2,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_3_temp",
        "register_address": 3,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_4_temp",
        "register_address": 4,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_5_temp",
        "register_address": 5,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_6_temp",
        "register_address": 6,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_7_temp",
        "register_address": 7,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_8_temp",
        "register_address": 8,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_9_temp",
        "register_address": 9,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_10_temp",
        "register_address": 10,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_11_temp",
        "register_address": 11,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_12_temp",
        "register_address": 12,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_front_13_temp",
        "register_address": 13,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_1_temp",
        "register_address": 14,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_2_temp",
        "register_address": 15,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_3_temp",
        "register_address": 16,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_4_temp",
        "register_address": 17,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_5_temp",
        "register_address": 18,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_6_temp",
        "register_address": 19,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_7_temp",
        "register_address": 20,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_8_temp",
        "register_address": 21,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_9_temp",
        "register_address": 22,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_10_temp",
        "register_address": 23,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_11_temp",
        "register_address": 24,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_12_temp",
        "register_address": 25,
        "data_type": 16
    },
    {
        "name": "vertical_sealer_rear_13_temp",
        "register_address": 26,
        "data_type": 16
    },
    {
        "name": "horizontal_servo_position",
        "register_address": 81,
        "data_type": 32
    },
    {
        "name": "vertical_servo_position",
        "register_address": 79,
        "data_type": 32
    },
    {
        "name": "rotational_valve_position",
        "register_address": 77,
        "data_type": 32
    },
    {
        "name": "fill_piston_position",
        "register_address": 75,
        "data_type": 32
    },
    {
        "name": "web_puller_position",
        "register_address": 83,
        "data_type": 32
    },
    {
        "name": "cam_position",
        "register_address": 85,
        "data_type": 32
    },
    {
        "name": "horizontal_sealer_front_1_temp",
        "register_address": 27,
        "data_type": 16
    },
    {
        "name": "horizontal_sealer_rear_1_temp",
        "register_address": 28,
        "data_type": 16
    }
    ,
    {
        "name": "state",
        "register_address": 97,
        "data_type": 16
    }
    ,
    {
        "name": "position",
        "register_address": 85,
        "data_type": 16
    }

    ]



        }
client = ModbusTcpClient('141.141.143.9', port=502)  # Replace with actual IP and port

# Connect to the Modbus server
client.connect()

# Starting address and count of registers (adjusted to zero-based indexing)
    
# Read the registers
try:
    response = client.read_input_registers(start_address, num_registers, slave=1)
    if response.isError():
        print(f"Error reading registers: {response}")
    else:
        # Extract register values
        registers = response.registers
        print("Register values:", registers)

except Exception as e:
    print(f"An error occurred: {e}")

# Close the client connection
client.close()

