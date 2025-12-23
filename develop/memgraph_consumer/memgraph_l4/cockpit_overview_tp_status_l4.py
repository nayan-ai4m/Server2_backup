import json
import time
import psycopg2
from datetime import datetime, timedelta

# DB config
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

MACHINES = [
    "MC 25", "MC 26", "MC 27", "MC 28", 
    "MC 29", "MC 30", "Tp MC 4", "Press 4",
    "Ck Wr 4", "CE 4", "HighBay 4"
]

# Only these TP columns are considered
COCKPIT_TPS = [
   "tp16", "tp44", "tp45", "tp46", "tp27", "tp47","tp52", "tp53", "tp61", "tp23", "tp26", "tp62", "tp69", "tp71", "tp72", "tp50", "tp56", "tp57", "tp58", "tp59"
]

with open("../touchpoints_updated.json", "r") as f:
    raw_touchpoints = json.load(f)

# Convert to lookup dictionary: {"TP01": {...}, "TP02": {...}, ...}
TOUCHPOINT_DETAILS = {}
for entry in raw_touchpoints:
    for key, value in entry.items():
        TOUCHPOINT_DETAILS[key.upper()] = value

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_table_name(machine_id):
    machine_lower = machine_id.lower().replace(" ", "")
    if machine_lower == "tpmc4":
        return "tpmc_tp_status_loop4"
    elif machine_lower == "press4":
        return "press_tp_status_loop4"
    elif machine_lower == "ckwr4":
        return "check_weigher_tp_status_loop4"
    elif machine_lower == "ce4":
        return "case_erector_tp_status_loop4"
    elif machine_lower == "highbay4":
        return "highbay_tp_status_loop4"
    else:
        return f"{machine_lower}_tp_status"

def get_latest_suggestion(machine_id):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        machine_number = machine_id.lower().replace(" ", "")
        query = """
            SELECT alert_details, suggestion_details, timestamp
            FROM suggestions
            WHERE machine_number = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        cursor.execute(query, (machine_number,))
        result = cursor.fetchone()

        if not result:
            return None, None, None

        alert_details, suggestion_details, suggestion_timestamp = result

        if not alert_details or not suggestion_details or not suggestion_timestamp:
            return None, None, None

        try:
            tp_number = int(alert_details.get("tp"))
            suggestion_data = suggestion_details.get("suggestion", {})

            # Inject timestamp into suggestion_data
            suggestion_data["timestamp"] = suggestion_timestamp.isoformat()

            return f"TP{tp_number:02}", suggestion_data, suggestion_timestamp
        except (json.JSONDecodeError, TypeError, ValueError):
            return None, None, None

    except psycopg2.Error as e:
        print(f"Error fetching suggestion for {machine_id}: {str(e)}")
        return None, None, None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        
def count_active_alerts(table_name):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name LIKE 'tp%%'
        """, (table_name,))
        tp_columns = [row[0] for row in cursor.fetchall() if row[0] in COCKPIT_TPS]

        if not tp_columns:
            return 0, 0, {}

        total_active = 0
        tp01_active = 0
        active_column_data = {}

        for column in tp_columns:
            cursor.execute(f"""
                SELECT {column}::jsonb
                FROM {table_name}
                WHERE {column}::jsonb->>'active' = '1'
            """)
            rows = cursor.fetchall()
            total_active += len(rows)
            
            for row in rows:
                if not row or not row[0]:
                    continue
                
                try:
                    data = row[0]
                    uuid = data.get("uuid")
                    filepath = data.get("filepath")
                    color_code = data.get("color_code")
                    timestamp = data.get("timestamp")
                    tp_name = column.upper()
                    if uuid and tp_name in TOUCHPOINT_DETAILS:
                        enriched = TOUCHPOINT_DETAILS[tp_name].copy()
                        enriched["uuid"] = uuid
                        enriched["timestamp"] = timestamp
                        enriched["color_code"] = color_code
                        enriched["filepath"] = filepath or ""
                        enriched["gsm_key"] = tp_name == "TP72"
                        active_column_data[tp_name] = enriched
                except (json.JSONDecodeError, TypeError):
                    continue
        
        # Get active status of latest tp01
        if 'tp01' in tp_columns:
            cursor.execute(f"""
                SELECT {table_name}.tp01::jsonb->>'active'
                FROM {table_name}
            """)
            result = cursor.fetchone()
            tp01_active = int(result[0]) if result and result[0] is not None else 0

        return total_active, tp01_active, active_column_data

    except psycopg2.Error as e:
        print(f"Error counting alerts for {table_name}: {str(e)}")
        return 0, 0, {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_overview_status():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for machine in MACHINES:
            table_name = get_table_name(machine)
            active_count, tp01_active, active_column_data = count_active_alerts(table_name)
            tp_with_suggestion, suggestion_data, suggestion_timestamp = get_latest_suggestion(machine)

            if active_count == 0:
                color = "green"
                status = "ok"
            elif active_count > 0 and tp01_active == 0:
                color = "orange"
                status = "alert"
            elif tp01_active == 1:
                color = "red"
                status = "machine_off"
            else:
                color = "orange"

            if tp_with_suggestion and suggestion_data:
                if tp_with_suggestion in active_column_data:
                    active_column_data[tp_with_suggestion]["suggestion"] = suggestion_data
                    
            status_json = {
                "id": machine,
                "active": active_count,
                "status": status,
                "color": color,
                "active_tp": active_column_data
            }

            # Try update
            cursor.execute("""
                UPDATE cockpit_overview_tp_status_l4 
                SET machine_status = %s
                WHERE machine_status->>'id' = %s
            """, (json.dumps(status_json), machine))

            # If no update happened, insert new
            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO cockpit_overview_tp_status_l4 (machine_status)
                    VALUES (%s)
                """, (json.dumps(status_json),))

        conn.commit()
        print(f"Updated cockpit_overview_tp_status_l4 at {time.strftime('%Y-%m-%d %H:%M:%S')}")

    except psycopg2.Error as e:
        print(f"Error updating overview status: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cockpit_overview_tp_status_l4 (
                machine_status JSONB
            );
        """)
        conn.commit()
        print("Ensured cockpit_overview_tp_status_l4 table exists")
    except psycopg2.Error as e:
        print(f"Error creating overview table: {str(e)}")
    finally:
        if conn:
            conn.close()

    while True:
        update_overview_status()
        time.sleep(1)

if __name__ == '__main__':
    main()
