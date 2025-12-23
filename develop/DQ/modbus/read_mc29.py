from pymodbus.client import ModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import psycopg2
from datetime import datetime

# Database connection setup
db_connection = psycopg2.connect(
    host="localhost",
    database="hul",
    user="postgres",
    password="ai4m2024"
)

# Modbus client connection setup
client = ModbusTcpClient('141.141.143.1', port=502)  # Replace with actual IP and port
client.connect()

def read_modbus_register(register_address, data_type, is_holding=True):
    """Read and decode a Modbus register based on its type."""
    if data_type == 16:  # Single 16-bit register
        response = client.read_holding_registers(register_address, 1) if is_holding else client.read_input_registers(register_address, 1)
        if response.isError():
            return None
        return response.registers[0]

    elif data_type == 32:  # Two 16-bit registers, forming a 32-bit value
        response = client.read_holding_registers(register_address, 2) if is_holding else client.read_input_registers(register_address, 2)
        if response.isError():
            return None
        decoder = BinaryPayloadDecoder.fromRegisters(
                        response.registers,
                        byteorder=Endian.BIG,
                        wordorder=Endian.LITTLE
                    )

        return decoder.decode_32bit_float()

# Dictionary to hold values for insertion
data_values = {}


holding_registers = {'values':[
    {
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
        "name":"cld_a",
        "register_address":138,
        "data_type":16
    },
    {
        "name":"cld_b",
        "register_address":139,
        "data_type":16
    },
    {
        "name":"cld_c",
        "register_address":140,
        "data_type":16
    },

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


while True:
    try:
        # Reading holding registers
        for item in holding_registers['values']:
            data_values[item['name']] = str(read_modbus_register(item['register_address'], item['data_type'], is_holding=True))

        # Reading input registers
        for item in input_registers['values']:
            data_values[item['name']] = str(read_modbus_register(item['register_address'], item['data_type'], is_holding=False))

        # Adding a timestamp
        data_values['timestamp'] = str(datetime.now())
        print(data_values)
        # Insert data into PostgreSQL
        insert_query = """INSERT INTO public.mc29("timestamp", vertical_sealer_front_1_temp, vertical_sealer_front_2_temp, vertical_sealer_front_3_temp, vertical_sealer_front_4_temp, vertical_sealer_front_5_temp, vertical_sealer_front_6_temp, vertical_sealer_front_7_temp, vertical_sealer_front_8_temp, vertical_sealer_front_9_temp, vertical_sealer_front_10_temp, vertical_sealer_front_11_temp, vertical_sealer_front_12_temp, vertical_sealer_front_13_temp, vertical_sealer_rear_1_temp, vertical_sealer_rear_2_temp, vertical_sealer_rear_3_temp, vertical_sealer_rear_4_temp, vertical_sealer_rear_5_temp, vertical_sealer_rear_6_temp, vertical_sealer_rear_7_temp, vertical_sealer_rear_8_temp, vertical_sealer_rear_9_temp, vertical_sealer_rear_10_temp, vertical_sealer_rear_11_temp, vertical_sealer_rear_12_temp, vertical_sealer_rear_13_temp, horizontal_servo_position, vertical_servo_position, rotational_valve_position, fill_piston_position, web_puller_position, cam_position, horizontal_sealer_front_1_temp, horizontal_sealer_rear_1_temp, state, "position", horizontal_sealing_time, horizontal_sealer_stroke_1, horizontal_sealer_stroke_2, vertical_sealer_stroke_1, vertical_sealer_stroke_2, batch_cut_on_degree, batch_cut_off_degree, vertical_sealer_front_1_setpoint, horizontal_sealer_front_1_setpoint, speed,cld_a,cld_b,cld_c) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s);"""

        values = tuple(data_values.get(column) for column in ['timestamp', 'vertical_sealer_front_1_temp', 'vertical_sealer_front_2_temp', 'vertical_sealer_front_3_temp','vertical_sealer_front_4_temp', 'vertical_sealer_front_5_temp', 'vertical_sealer_front_6_temp', 'vertical_sealer_front_7_temp',
    'vertical_sealer_front_8_temp', 'vertical_sealer_front_9_temp', 'vertical_sealer_front_10_temp', 'vertical_sealer_front_11_temp',
    'vertical_sealer_front_12_temp', 'vertical_sealer_front_13_temp', 'vertical_sealer_rear_1_temp', 'vertical_sealer_rear_2_temp',
    'vertical_sealer_rear_3_temp', 'vertical_sealer_rear_4_temp', 'vertical_sealer_rear_5_temp', 'vertical_sealer_rear_6_temp',
    'vertical_sealer_rear_7_temp', 'vertical_sealer_rear_8_temp', 'vertical_sealer_rear_9_temp', 'vertical_sealer_rear_10_temp',
    'vertical_sealer_rear_11_temp', 'vertical_sealer_rear_12_temp', 'vertical_sealer_rear_13_temp', 'horizontal_servo_position',
    'vertical_servo_position', 'rotational_valve_position', 'fill_piston_position', 'web_puller_position', 'cam_position',
    'horizontal_sealer_front_1_temp', 'horizontal_sealer_rear_1_temp', 'state', 'position', 'horizontal_sealing_time',
    'horizontal_sealer_stroke_1', 'horizontal_sealer_stroke_2', 'vertical_sealer_stroke_1', 'vertical_sealer_stroke_2',
    'batch_cut_on_degree', 'batch_cut_off_degree', 'vertical_sealer_front_1_setpoint', 'horizontal_sealer_front_1_setpoint', 'speed','cld_a','cld_b','cld_c'])
        print(values)
    # Execute and commit
        db_cursor = db_connection.cursor()
        db_cursor.execute(insert_query, values)
        db_connection.commit()
        db_cursor.close()
    except Exception as e:
        print(e)
# Close connections
client.close()
db_connection.close()

