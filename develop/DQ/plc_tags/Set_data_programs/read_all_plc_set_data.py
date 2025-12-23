
from pycomm3 import LogixDriver
from datetime import datetime
import json
import time
import psycopg2
from psycopg2.extras import Json
from typing import Dict, Optional

# Database configuration
DB_CONFIG = {
    'dbname': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024',
    'host': 'localhost',
    'port': '5432'
}

# Default data for when PLC is not connected
NO_PLC_DATA = {
    'data': "No PLC Data",
    'timestamp': None,
    'status': 'disconnected'
}

# PLC configurations
PLC_CONFIGS = {
    'PLC1': {
        'ip': '141.141.141.128',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Hor_Seal_Front_27', 'HMI_Hor_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    },
    'PLC2': {
        'ip': '141.141.141.138',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Hor_Seal_Rear_35', 'HMI_Hor_Seal_Rear_36', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    },
    'PLC3': {
        'ip': '141.141.141.52',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    },
    'PLC4': {
        'ip': '141.141.141.62',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    },
    'PLC5': {
        'ip': '141.141.141.72',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    },
    'PLC6': {
        'ip': '141.141.141.82',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
            ]
    }
}

def setup_database():
    """Initialize database table"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS plc_unified_data (
            counter SERIAL PRIMARY KEY,
            plc1 JSONB,
            plc2 JSONB,
            plc3 JSONB,
            plc4 JSONB,
            plc5 JSONB,
            plc6 JSONB
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Database setup completed successfully")
        
    except Exception as e:
        print(f"Database setup error: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

class PLCMonitor:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        
    def read_plc(self, plc_id: str, config: dict) -> dict:
        """Read data from a single PLC"""
        try:
            with LogixDriver(config['ip']) as plc:
                data = {}
                for tag in config['tags']:
                    tag_data = plc.read(tag)
                    if tag_data is None:
                        print(f"{plc_id}: Failed to read tag {tag}")
                        return NO_PLC_DATA
                    data[tag] = tag_data.value
                
                return {
                    'data': data,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'status': 'connected'
                }
                
        except Exception as e:
            print(f"{plc_id} connection error: {str(e)}")
            return NO_PLC_DATA

    def get_latest_data(self) -> Optional[dict]:
        """Retrieve most recent data from database"""
        try:
            cur = self.conn.cursor()
            
            query = """
            SELECT plc1, plc2, plc3, plc4, plc5, plc6, counter
            FROM plc_unified_data
            ORDER BY counter DESC
            LIMIT 1;
            """
            
            cur.execute(query)
            result = cur.fetchone()
            cur.close()
            
            if result:
                return {
                    'PLC1': result[0] or NO_PLC_DATA,
                    'PLC2': result[1] or NO_PLC_DATA,
                    'PLC3': result[2] or NO_PLC_DATA,
                    'PLC4': result[3] or NO_PLC_DATA,
                    'PLC5': result[4] or NO_PLC_DATA,
                    'PLC6': result[5] or NO_PLC_DATA,
                    'counter': result[6]
                }
            return None
            
        except Exception as e:
            print(f"Database read error: {str(e)}")
            return None

    def has_changes(self, old_data: Optional[dict], current_readings: dict) -> bool:
        """Check if there are changes in any connected PLC's data"""
        if not old_data:
            return True
            
        for plc_id in PLC_CONFIGS.keys():
            old_plc = old_data.get(plc_id, NO_PLC_DATA)
            new_plc = current_readings.get(plc_id, NO_PLC_DATA)
            
            if new_plc['status'] == 'connected':  # Only check connected PLCs
                if old_plc['data'] != new_plc['data']:
                    return True
        return False

    def monitor_cycle(self):
        """Execute one monitoring cycle"""
        current_readings = {}
        connected_plcs = []
        disconnected_plcs = []
        
        # Read from all PLCs
        for plc_id, config in PLC_CONFIGS.items():
            print(f"\nReading {plc_id}...")
            plc_data = self.read_plc(plc_id, config)
            current_readings[plc_id] = plc_data
            
            if plc_data['status'] == 'connected':
                connected_plcs.append(plc_id)
                print(f"{plc_id}: Read successful")
            else:
                disconnected_plcs.append(plc_id)
                print(f"{plc_id}: Not connected")
        
        # Process data if at least one PLC is connected
        if connected_plcs:
            previous_data = self.get_latest_data()
            
            if self.has_changes(previous_data, current_readings):
                print("\nChanges detected in connected PLCs")
                self.save_data(current_readings)
                
                # Print status
                print("\nStatus Summary:")
                for plc_id, data in current_readings.items():
                    status = data['status']
                    timestamp = data['timestamp'] or 'Never'
                    print(f"{plc_id}: {status.upper()} (Last Update: {timestamp})")
            else:
                print("\nNo changes detected in connected PLCs")
        
        # Print connection summary
        print("\nConnection Summary:")
        print(f"Connected PLCs: {', '.join(connected_plcs) if connected_plcs else 'None'}")
        print(f"Disconnected PLCs: {', '.join(disconnected_plcs) if disconnected_plcs else 'None'}")

    def save_data(self, data: Dict):
        """Save PLC data to database"""
        try:
            cur = self.conn.cursor()
            
            query = """
            INSERT INTO plc_unified_data (plc1, plc2, plc3, plc4, plc5, plc6)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            values = [Json(data.get(f'PLC{i}', NO_PLC_DATA)) for i in range(1, 7)]
            cur.execute(query, values)
            self.conn.commit()
            cur.close()
            print("Data saved successfully")
            
        except Exception as e:
            print(f"Database save error: {str(e)}")
            self.conn.rollback()

    def run(self):
        """Run the monitoring loop"""
        print("Starting PLC monitoring...")
        
        while True:
            try:
                print("\n" + "="*50)
                print(f"Starting monitoring cycle at {datetime.now()}")
                self.monitor_cycle()
                
                print("\nWaiting 60 seconds...")
                time.sleep(60)
                
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                break
            except Exception as e:
                print(f"\nUnexpected error: {str(e)}")
                print("Continuing monitoring...")
                time.sleep(60)

    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    setup_database()
    monitor = PLCMonitor()
    monitor.run()

if __name__ == "__main__":
    main()

