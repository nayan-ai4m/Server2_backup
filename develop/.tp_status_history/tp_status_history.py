import json
import time
import psycopg2
from datetime import datetime

# DB config
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_current_active_tps():
    """Fetch all currently active TPs from overview_tp_status_l3 and overview_tp_status_loop4"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Build a dict: {(machine, tp_column, uuid): timestamp}
        current_state = {}

        # Query both L3 and L4 tables
        for table_name in ['overview_tp_status_l3', 'overview_tp_status_loop4']:
            cursor.execute(f"SELECT machine_status FROM {table_name}")
            rows = cursor.fetchall()

            for row in rows:
                machine_data = row[0]
                machine = machine_data.get('id')
                active_tp = machine_data.get('active_tp', {})

                for tp_column, tp_data in active_tp.items():
                    uuid = tp_data.get('uuid')
                    timestamp = tp_data.get('timestamp')

                    if machine and tp_column and uuid and timestamp:
                        key = (machine, tp_column, uuid)
                        current_state[key] = timestamp

        return current_state

    except psycopg2.Error as e:
        print(f"Error fetching current TPs: {str(e)}")
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def log_new_tps(new_tps):
    """Insert new TP events into history table"""
    if not new_tps:
        return

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for (machine, tp_column, uuid), timestamp in new_tps.items():
            cursor.execute("""
                INSERT INTO tp_status_history (machine, tp_column, event_timestamp)
                VALUES (%s, %s, %s)
            """, (machine, tp_column, timestamp))

        conn.commit()
        print(f"Logged {len(new_tps)} new TP events at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    except psycopg2.Error as e:
        print(f"Error logging TPs to history: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    print("Starting TP history logger...")
    print("Monitoring overview_tp_status_l3 and overview_tp_status_loop4 for TP changes...\n")

    # Track previous state
    previous_state = {}

    while True:
        try:
            # Get current active TPs
            current_state = get_current_active_tps()

            # Find NEW TPs (present now but not before)
            new_tps = {
                key: timestamp
                for key, timestamp in current_state.items()
                if key not in previous_state
            }

            # Log new TPs to history
            if new_tps:
                log_new_tps(new_tps)
                for (machine, tp_column, uuid), timestamp in new_tps.items():
                    print(f"  → {machine} | {tp_column} | {timestamp}")

            # Update previous state
            previous_state = current_state

            # Wait before next check
            time.sleep(1)

        except KeyboardInterrupt:
            print("\nStopping TP history logger...")
            break
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            time.sleep(5)

if __name__ == '__main__':
    main()
