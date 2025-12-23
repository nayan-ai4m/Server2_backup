import psycopg2
from psycopg2 import extras
from datetime import datetime
import pandas as pd
import time
import os

class StatusMonitor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.tables = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
        
        # Excel file path
        excel_path = '/home/suraj-rathod/Desktop/HUL_Hridwar/_4Machine Monitor/loop3_status_msg.xlsx'
        
        if not os.path.exists(excel_path):
            print(f"Error: Excel file not found at: {excel_path}")
            self.fault_messages = {}
            return
        
        try:
            self.fault_codes = pd.read_excel(excel_path)
            self.fault_codes.columns = self.fault_codes.columns.str.strip()
            
            # Convert fault codes to dictionary using the actual column names
            self.fault_messages = {}
            for index, row in self.fault_codes.iterrows():
                try:
                    code = int(row['fault code'])
                    message = str(row['MESSAGE'])
                    self.fault_messages[code] = message.strip()
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error loading Excel file: {str(e)}")
            self.fault_messages = {}
    
    def get_fault_message(self, code):
        """Get the fault message for a given status code"""
        if not self.fault_messages:
            return "Error: No fault codes loaded"
            
        try:
            if code is None:
                return "No status code provided"
                
            code = int(code)
            message = self.fault_messages.get(code)
            if message:
                return message
            return f"Machine stopped for unknown reason (Code: {code})"
        except (ValueError, TypeError):
            return f"Invalid status code: {code}"
    
    def connect_db(self):
        return psycopg2.connect(**self.db_config)
    
    def monitor_status_batch(self):
        query = """
        WITH latest_status AS (
            {}
        )
        SELECT * FROM latest_status
        ORDER BY timestamp DESC;
        """.format(
            " UNION ALL ".join(
                f"""
                SELECT 
                    '{table}' as table_name,
                    status,
                    status_code,
                    timestamp
                FROM {table}
                WHERE timestamp = (
                    SELECT MAX(timestamp) 
                    FROM {table}
                )
                """
                for table in self.tables
            )
        )
        
        try:
            with self.connect_db() as conn:
                with conn.cursor(cursor_factory=extras.DictCursor) as cursor:
                    cursor.execute(query)
                    return cursor.fetchall()
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None
    
    def monitor_changes(self):
        """
        Continuously monitor status changes with 6-second interval
        """
        previous_statuses = {}
        previous_codes = {}
        
        while True:
            try:
                current_statuses = self.monitor_status_batch()
                
                if current_statuses:
                    print("\n" + "="*80)
                    print(f"Status Update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("="*80)
                    
                    for row in current_statuses:
                        table = row['table_name']
                        status = row['status']
                        status_code = row['status_code']
                        timestamp = row['timestamp']
                        fault_message = self.get_fault_message(status_code)
                        
                        print(f"Table: {table}")
                        print(f"Status: {status}")
                        print(f"Status Code: {status_code}")
                        print(f"Message: {fault_message}")
                        print(f"Timestamp: {timestamp}")
                        
                        # Check for changes
                        status_changed = table not in previous_statuses or previous_statuses[table] != status
                        code_changed = table not in previous_codes or previous_codes[table] != status_code
                        
                        if status_changed or code_changed:
                            print("\n*** CHANGES DETECTED ***")
                            if status_changed:
                                print(f"Status changed: {previous_statuses.get(table, 'N/A')} -> {status}")
                            if code_changed:
                                prev_code = previous_codes.get(table, 'N/A')
                                prev_message = self.get_fault_message(prev_code) if prev_code != 'N/A' else 'N/A'
                                print(f"Status Code changed: {prev_code} -> {status_code}")
                                print(f"Message changed: {prev_message} -> {fault_message}")
                        
                        previous_statuses[table] = status
                        previous_codes[table] = status_code
                        print("-"*60)
                
                time.sleep(6)
                
            except Exception as e:
                print(f"Error in monitoring loop: {str(e)}")
                time.sleep(6)

if __name__ == "__main__":
    db_config = {
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'host': '100.114.56.95',
        'port': '5432'
    }
    
    try:
        monitor = StatusMonitor(db_config)
        print("Starting monitoring...")
        monitor.monitor_changes()
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")