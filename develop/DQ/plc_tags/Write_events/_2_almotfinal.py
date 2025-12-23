
import psycopg2
from psycopg2 import extras
from datetime import datetime
import pandas as pd
import time
import os
import json
import uuid

class StatusMonitor:
    def __init__(self, db_config):
        self.db_config = db_config
        self.tables = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
        self.event_tracker_file = 'event_tracker.json'
        self.unresolved_events = self.load_event_tracker()
        
        # Load fault code messages from Excel
        try:
            excel_path = '/home/suraj-rathod/Desktop/HUL_Hridwar/_4Machine Monitor/loop3_status_msg.xlsx'
            self.fault_codes = pd.read_excel(excel_path)
            self.fault_codes.columns = self.fault_codes.columns.str.strip()
            self.fault_messages = dict(zip(self.fault_codes['fault code'], self.fault_codes['MESSAGE']))
        except Exception as e:
            print(f"Error loading Excel file: {str(e)}")
            self.fault_messages = {}

    def load_event_tracker(self):
        """Load unresolved events from config file"""
        if os.path.exists(self.event_tracker_file):
            try:
                with open(self.event_tracker_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_event_tracker(self):
        """Save unresolved events to config file"""
        with open(self.event_tracker_file, 'w') as f:
            json.dump(self.unresolved_events, f, indent=4)

    def generate_event_id(self, machine, status, timestamp):
        """Generate event ID in format MC17_20241103_141610_2"""
        time_str = timestamp.strftime("%Y%m%d_%H%M%S")
        return f"{machine.upper()}_{time_str}_{status}"

    def get_fault_message(self, code):
        """Get the fault message for a given status code"""
        try:
            code = int(code)
            return self.fault_messages.get(code, "Unknown reason")
        except (ValueError, TypeError):
            return "Unknown reason"

    def connect_db(self):
        return psycopg2.connect(**self.db_config)

    def insert_event(self, machine, status, status_code, timestamp):
        """Insert new event into database and track it"""
        event_id = self.generate_event_id(machine, status, timestamp)
        
        # Check if event already exists
        if machine in self.unresolved_events:
            return False

        try:
            with self.connect_db() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO event_table 
                        (timestamp, event_id, zone, event_type, alert_type)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        timestamp,
                        event_id,
                        f"Loop3_{machine}",
                        f"{status_code}: {self.get_fault_message(status_code)}",
                        "productivity"
                    ))
                    
            # Track this event
            self.unresolved_events[machine] = {
                "event_id": event_id,
                "timestamp": timestamp.isoformat()
            }
            self.save_event_tracker()
            
            print(f"\nALERT: Machine {machine} stopped")
            print(f"Time: {timestamp}")
            print(f"Issue: {status_code}: {self.get_fault_message(status_code)}")
            
            return True
            
        except Exception as e:
            print(f"Error inserting event: {str(e)}")
            return False

    def resolve_event(self, machine, timestamp):
        """Update resolution time for an event"""
        if machine not in self.unresolved_events:
            return False

        event_id = self.unresolved_events[machine]["event_id"]
        
        try:
            with self.connect_db() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE event_table 
                        SET resolution_time = %s
                        WHERE event_id = %s
                    """, (timestamp, event_id))
            
            # Remove from tracker
            prev_time = datetime.fromisoformat(self.unresolved_events[machine]["timestamp"])
            downtime = timestamp - prev_time
            
            print(f"\nALERT: Machine {machine} running again")
            print(f"Time: {timestamp}")
            print(f"Downtime: {downtime}")
            
            del self.unresolved_events[machine]
            self.save_event_tracker()
            
            return True
            
        except Exception as e:
            print(f"Error resolving event: {str(e)}")
            return False

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

    def handle_initial_state(self, machine, status, status_code, timestamp):
        """Handle machines that are already stopped when monitoring starts"""
        if status != 1 and machine not in self.unresolved_events:
            self.insert_event(machine, status, status_code, timestamp)

    def monitor_changes(self):
        """Monitor status changes"""
        previous_statuses = {}
        first_run = True
        
        while True:
            try:
                current_statuses = self.monitor_status_batch()
                
                if current_statuses:
                    for row in current_statuses:
                        machine = row['table_name']
                        status = row['status']
                        status_code = row['status_code']
                        timestamp = row['timestamp']
                        
                        # Handle initial state
                        if first_run:
                            self.handle_initial_state(machine, status, status_code, timestamp)
                        
                        # Check for status changes
                        if machine in previous_statuses:
                            prev_status = previous_statuses[machine]
                            
                            # If changed from running to not running
                            if prev_status == 1 and status != 1:
                                self.insert_event(machine, status, status_code, timestamp)
                            
                            # If changed to running
                            elif prev_status != 1 and status == 1:
                                self.resolve_event(machine, timestamp)
                        
                        previous_statuses[machine] = status
                    
                    first_run = False
                
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
