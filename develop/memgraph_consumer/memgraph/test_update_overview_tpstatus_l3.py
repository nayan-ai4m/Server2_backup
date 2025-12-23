import json
import time
import psycopg2

# DB config
DB_CONFIG = {
    'host': '192.168.1.168',
    'database': 'hul',
    'user': 'postgres',
    'password': 'ai4m2024'
}

MACHINES = [
    "MC 17", "MC 18", "MC 19", "MC 20", 
    "MC 21", "MC 22", "Tp MC 3", "Press 3", 
    "Ck Wr 3", "CE 3", "HighBay 3"
]

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

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

def count_active_alerts(table_name):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all TP columns for the table
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = %s AND column_name LIKE 'tp%%'
        """, (table_name,))
        tp_columns = [row[0] for row in cursor.fetchall()]

        if not tp_columns:
            return 0

        total_active = 0
        tp01_active = 0
        
        # Count active alerts in each TP column
        for column in tp_columns:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE {column}::jsonb->>'active' = '1'
            """)
            total_active += cursor.fetchone()[0]
        
        # Get active status of latest tp01
        if 'tp01' in tp_columns:
            cursor.execute(f"""
                SELECT {table_name}.tp01::jsonb->>'active'
                FROM {table_name}
            """)
            result = cursor.fetchone()
            # tp01_active = result[0] if result else 0
            tp01_active = int(result[0]) if result and result[0] is not None else 0
        
        return total_active, tp01_active

    except psycopg2.Error as e:
        print(f"Error counting alerts for {table_name}: {str(e)}")
        return 0, 0
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
            active_count, tp01_active = count_active_alerts(table_name)

            # status = "ok" if active_count == 0 else "alert"
            # color = "green" if active_count == 0 else "red"
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

            status_json = {
                "id": machine,
                "active": active_count,
                "status": status,
                "color": color
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
