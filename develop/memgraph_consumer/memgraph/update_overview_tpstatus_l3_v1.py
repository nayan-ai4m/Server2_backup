import json
import time
import psycopg2
from datetime import datetime, timedelta

# DB config for Server2
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

# DB config for Server1 (machine stoppages)
SERVER1_DB_CONFIG = {
    'host': '192.168.1.149',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

MACHINES = [
    "MC 17", "MC 18", "MC 19", "MC 20", 
    "MC 21", "MC 22", "Tp MC 3", "Press 3", 
    "Ck Wr 3", "CE 3", "HighBay 3"
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

def get_server1_db_connection():
    return psycopg2.connect(**SERVER1_DB_CONFIG)

def get_latest_machine_stoppage_reason(machine_id):
    """
    Fetch the latest stoppage reason for a given machine from Server1.
    Returns the reason string or "No Instruction" if no record exists.
    """
    conn = None
    cursor = None
    try:
        # Convert machine name: "MC 17" -> "mc17"
        machine_name = machine_id.lower().replace(" ", "")

        conn = get_server1_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT reason
            FROM machine_stoppages
            WHERE machine = %s
            ORDER BY stop_at DESC
            LIMIT 1
        """, (machine_name,))

        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        else:
            return "No Instruction"

    except psycopg2.Error as e:
        print(f"Error fetching stoppage reason for {machine_id}: {str(e)}")
        return "No Instruction"
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_table_name(machine_id):
    machine_lower = machine_id.lower().replace(" ", "")
    if machine_lower == "tpmc3":
        return "tpmc_tp_status"
    elif machine_lower == "press3":
        return "press_tp_status"
    elif machine_lower == "ckwr3":
        return "check_weigher_tp_status"
    elif machine_lower == "ce3":
        return "case_erector_tp_status"
    elif machine_lower == "highbay3":
        return "highbay_tp_status"
    else:
        return f"{machine_lower}_tp_status"

def count_active_alerts(table_name, machine_id):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name LIKE 'tp%%'
        """, (table_name,))
        tp_columns = [row[0] for row in cursor.fetchall() if row[0]]

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

                        # If this is TP01, fetch the latest stoppage reason from Server1
                        if tp_name == "TP01":
                            stoppage_reason = get_latest_machine_stoppage_reason(machine_id)
                            enriched["instruction"] = stoppage_reason

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
            active_count, tp01_active, active_column_data = count_active_alerts(table_name, machine)

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
                status = "alert"

            status_json = {
                "id": machine,
                "active": active_count,
                "status": status,
                "color": color,
                "active_tp": active_column_data
            }

            # Try to update existing record
            cursor.execute("""
                UPDATE overview_tp_status_l3 
                SET machine_status = %s
                WHERE machine_status->>'id' = %s
            """, (json.dumps(status_json), machine))

            # If no rows updated, insert new record
            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO overview_tp_status_l3 (machine_status)
                    VALUES (%s)
                """, (json.dumps(status_json),))

        conn.commit()
        print(f"Updated overview_tp_status_l3 at {time.strftime('%Y-%m-%d %H:%M:%S')}")

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
    # Ensure overview table exists
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS overview_tp_status_l3 (
                machine_status JSONB
            );
        """)
        conn.commit()
        print("Ensured overview_tp_status_l3 table exists")
    except psycopg2.Error as e:
        print(f"Error creating overview table: {str(e)}")
    finally:
        if conn:
            conn.close()

    # Run continuous updates
    while True:
        update_overview_status()
        time.sleep(1)

if __name__ == '__main__':
    main()
