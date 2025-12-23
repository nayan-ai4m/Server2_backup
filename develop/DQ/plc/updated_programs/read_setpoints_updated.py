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

# Default data for unconnected PLC
NO_PLC_DATA = {
    'data': "No PLC Data",
    'timestamp': None,
    'status': 'never_connected',
    'changed_data': [],
    'remark': '',  # Added remark field
    'operator': ''  # Added operator field
}

# PLC configurations
PLC_CONFIGS = {
    'PLC17': {
        'ip': '141.141.141.128',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Hor_Seal_Front_27', 'HMI_Hor_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
        ]
    },
    'PLC18': {
        'ip': '141.141.141.138',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Hor_Seal_Rear_35', 'HMI_Hor_Seal_Rear_36', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
        ]
    },
    'PLC19': {
        'ip': '141.141.141.52',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
        ]
    },
    'PLC20': {
        'ip': '141.141.141.62',
        'tags': [
           'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
        ]
    },
    'PLC21': {
        'ip': '141.141.141.72',
        'tags': [
            'HMI_Ver_Seal_Front_1', 'HMI_Ver_Seal_Front_2', 'HMI_Ver_Seal_Front_3', 'HMI_Ver_Seal_Front_4', 'HMI_Ver_Seal_Front_5', 'HMI_Ver_Seal_Front_6', 'HMI_Ver_Seal_Front_7', 'HMI_Ver_Seal_Front_8', 'HMI_Ver_Seal_Front_9', 'HMI_Ver_Seal_Front_10', 'HMI_Ver_Seal_Front_11', 'HMI_Ver_Seal_Front_12', 'HMI_Ver_Seal_Front_13', 'HMI_Ver_Seal_Rear_14', 'HMI_Ver_Seal_Rear_15', 'HMI_Ver_Seal_Rear_16', 'HMI_Ver_Seal_Rear_17', 'HMI_Ver_Seal_Rear_18', 'HMI_Ver_Seal_Rear_19', 'HMI_Ver_Seal_Rear_20', 'HMI_Ver_Seal_Rear_21', 'HMI_Ver_Seal_Rear_22', 'HMI_Ver_Seal_Rear_23', 'HMI_Ver_Seal_Rear_24', 'HMI_Ver_Seal_Rear_25', 'HMI_Ver_Seal_Rear_26', 'HMI_Ver_Seal_Rear_27', 'HMI_Ver_Seal_Rear_28', 'HMI_Hor_Sealer_Strk_1', 'HMI_Hor_Sealer_Strk_2', 'HMI_Ver_Sealer_Strk_1', 'HMI_Ver_Sealer_Strk_2', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Suction_Start_Deg', 'HMI_Suction_End_Degree', 'HMI_Filling_Stroke_Deg', 'HMI_VER_CLOSE_END', 'HMI_VER_CLOSE_START', 'HMI_VER_OPEN_END', 'HMI_VER_OPEN_START', 'HMI_HOZ_CLOSE_END', 'HMI_HOZ_CLOSE_START', 'HMI_HOZ_OPEN_END', 'HMI_HOZ_OPEN_START', 'HMI_Rot_Valve_Close_End_Deg', 'HMI_Rot_Valve_Close_Start_Deg', 'HMI_Rot_Valve_Open_End_Deg', 'HMI_Rot_Valve_Open_Start_Deg', 'HMI_Rot_Valve_Stroke'
        ]
    },
    'PLC22': {
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
            plc17 JSONB,
            plc18 JSONB,
            plc19 JSONB,
            plc20 JSONB,
            plc21 JSONB,
            plc22 JSONB,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
        """Initialize PLCMonitor"""
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.previous_cycle_data = {}
        self.first_cycle = True
        print("PLC Monitor initialized")

    def process_tag_value(self, tag: str, value: any) -> any:
        """Process tag value based on tag name"""
        if isinstance(value, dict) and ('Front' in tag or 'Rear' in tag):
            return value.get('SetValue')
        return value

    def read_plc(self, plc_id: str, config: dict) -> dict:
        """Read and process data from PLC"""
        try:
            with LogixDriver(config['ip']) as plc:
                processed_data = {}
                read_success = True
                changed_data = []  # List to store changed values
                
                for tag in config['tags']:
                    try:
                        tag_data = plc.read(tag)
                        if tag_data is None:
                            raise Exception(f"Failed to read tag {tag}")
                            
                        processed_value = self.process_tag_value(tag, tag_data.value)
                        processed_data[tag] = processed_value
                        
                        # Check for changes if not first cycle
                        if not self.first_cycle and plc_id in self.previous_cycle_data:
                            prev_value = self.previous_cycle_data[plc_id].get('data', {}).get(tag)
                            if prev_value is not None and processed_value != prev_value:
                                changed_data.append({
                                    'tag': tag,
                                    'previous_value': prev_value,
                                    'current_value': processed_value
                                })
                        
                    except Exception as e:
                        print(f"{plc_id} - Error reading tag {tag}: {str(e)}")
                        read_success = False
                        break
                
                if read_success:
                    return {
                        'data': processed_data,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f%z'),
                        'status': 'connected',
                        'changed_data': changed_data,
                        'remark': '',  # Added empty remark
                        'operator': ''  # Added empty operator
                    }
                    
                return self.handle_disconnection(plc_id)
                
        except Exception as e:
            print(f"{plc_id} - Connection error: {str(e)}")
            return self.handle_disconnection(plc_id)

    def handle_disconnection(self, plc_id: str) -> dict:
        """Handle PLC disconnection"""
        if plc_id in self.previous_cycle_data:
            last_data = self.previous_cycle_data[plc_id].copy()
            last_data['status'] = 'disconnected'
            last_data['changed_data'] = []  # Clear changed data on disconnection
            # Keep existing remark and operator values
            return last_data
        return NO_PLC_DATA.copy()

    def detect_changes(self, current_readings: dict) -> tuple[bool, list]:
        """Detect changes between cycles"""
        if self.first_cycle:
            self.first_cycle = False
            self.previous_cycle_data = current_readings.copy()
            return True, ["Initial reading"]
        
        changes_detected = False
        change_log = []
        
        for plc_id in PLC_CONFIGS.keys():
            current = current_readings.get(plc_id, {})
            
            # Skip if PLC is not connected
            if current.get('status') != 'connected':
                continue
            
            # Log any changes detected during reading
            if current.get('changed_data'):
                changes_detected = True
                for change in current['changed_data']:
                    change_log.append(f"{plc_id} - {change['tag']}: {change['previous_value']} -> {change['current_value']}")
        
        if changes_detected:
            self.previous_cycle_data = current_readings.copy()
            
        return changes_detected, change_log

    def monitor_cycle(self):
        """Execute one monitoring cycle"""
        current_readings = {}
        connected_plcs = []
        disconnected_plcs = []
        
        # Read all PLCs
        for plc_id, config in PLC_CONFIGS.items():
            print(f"\nReading {plc_id}...")
            plc_data = self.read_plc(plc_id, config)
            current_readings[plc_id] = plc_data
            
            if plc_data['status'] == 'connected':
                connected_plcs.append(plc_id)
                print(f"{plc_id} - Connected")
            else:
                disconnected_plcs.append(plc_id)
                print(f"{plc_id} - {plc_data['status'].upper()}")
        
        # Process data if any PLC is connected
        if connected_plcs:
            changes_detected, changes = self.detect_changes(current_readings)
            
            if changes_detected:
                print("\nChanges Detected:")
                for change in changes:
                    print(f"  {change}")
                print("\nSaving to database...")
                self.save_data(current_readings)
            else:
                print("\nNo changes detected")
        
        self.print_status(connected_plcs, disconnected_plcs, current_readings)

    def save_data(self, data: Dict):
        """Save PLC data to database"""
        try:
            cur = self.conn.cursor()
            
            query = """
            INSERT INTO plc_unified_data (plc17, plc18, plc19, plc20, plc21, plc22)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            values = [Json(data.get(f'PLC{i}', NO_PLC_DATA)) for i in range(17, 23)]
            cur.execute(query, values)
            self.conn.commit()
            cur.close()
            print("Data saved successfully")
            
        except Exception as e:
            print(f"Database save error: {str(e)}")
            self.conn.rollback()

    def print_status(self, connected: list, disconnected: list, readings: dict):
        """Print status summary"""
        print("\n" + "="*50)
        print("STATUS SUMMARY")
        print("="*50)
        
        if connected:
            print("\nConnected PLCs:")
            for plc_id in connected:
                data = readings[plc_id]
                print(f"  ✓ {plc_id}")
                print(f"    Last Update: {data['timestamp']}")
                if data['changed_data']:
                    print("    Changed Values:")
                    for change in data['changed_data']:
                        print(f"      - {change['tag']}: {change['previous_value']} -> {change['current_value']}")
                # Added printing of remark and operator if they exist
                if data['remark']:
                    print(f"    Remark: {data['remark']}")
                if data['operator']:
                    print(f"    Operator: {data['operator']}")
        
        if disconnected:
            print("\nDisconnected PLCs:")
            for plc_id in disconnected:
                data = readings[plc_id]
                status = data['status']
                last_update = data['timestamp'] or 'Never'
                print(f"  ✗ {plc_id} ({status})")
                if status == 'disconnected':
                    print(f"    Last Known Data: {last_update}")
        
        print("="*50)

    def run(self):
        """Run monitoring loop"""
        print("\nStarting PLC monitoring system")
        print("Press Ctrl+C to stop")
        
        while True:
            try:
                print(f"\nStarting cycle at {datetime.now()}")
                self.monitor_cycle()
                
                print("\nWaiting 60 seconds...")
                time.sleep(60)
                
            except KeyboardInterrupt:
                print("\nStopping monitoring system")
                break
            except Exception as e:
                print(f"\nUnexpected error: {str(e)}")
                print("Continuing to next cycle...")
                time.sleep(60)

    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    setup_database()
    monitor = PLCMonitor()
    monitor.run()

if __name__ == "__main__":
    main()

