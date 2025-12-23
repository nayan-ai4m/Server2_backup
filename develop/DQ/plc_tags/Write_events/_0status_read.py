import psycopg2
from psycopg2 import extras
from datetime import datetime
import pandas as pd
import time

class StatusMonitor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.tables = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
    
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
        Continuously monitor status changes with 1-minute interval
        """
        previous_statuses = {}
        previous_codes = {}
        
        while True:
            try:
                current_statuses = self.monitor_status_batch()
                
                if current_statuses:
                    print("\n" + "="*50)
                    print(f"Status Update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("="*50)
                    
                    for row in current_statuses:
                        table = row['table_name']
                        status = row['status']
                        status_code = row['status_code']
                        timestamp = row['timestamp']
                        
                        # Always print current status
                        print(f"Table: {table}")
                        print(f"Status: {status}")
                        print(f"Status Code: {status_code}")
                        print(f"Timestamp: {timestamp}")
                        
                        # Indicate if status or status_code has changed
                        status_changed = table not in previous_statuses or previous_statuses[table] != status
                        code_changed = table not in previous_codes or previous_codes[table] != status_code
                        
                        if status_changed or code_changed:
                            print("*** CHANGES DETECTED ***")
                            if status_changed:
                                print(f"Status changed: {previous_statuses.get(table, 'N/A')} -> {status}")
                            if code_changed:
                                print(f"Status Code changed: {previous_codes.get(table, 'N/A')} -> {status_code}")
                        
                        previous_statuses[table] = status
                        previous_codes[table] = status_code
                        print("-"*30)
                
                time.sleep(6)  # 6-second interval
                
            except Exception as e:
                print(f"Error: {str(e)}")
                time.sleep(6)

if __name__ == "__main__":
    db_config = {
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'host': '100.114.56.95',
        'port': '5432'
    }
    
    monitor = StatusMonitor(db_config)
    print("Starting status monitoring (6-second interval)...")
    monitor.monitor_changes()