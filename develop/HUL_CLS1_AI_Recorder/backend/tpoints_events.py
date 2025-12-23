import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from fastapi import HTTPException, Query, APIRouter
from psycopg2 import sql
import traceback
from database import get_db, close_db, get_server2_db, close_server2_db

MACHINE_ALIAS_MAP = {
    # Loop 3 machines
    "mcckwr3": "check_weigher_l3",
    "ckwr3": "check_weigher_l3",
    "mctp3": "tpmc_l3",
    "tpmc3": "tpmc_l3",
    "mcce3": "case_erector_l3",
    "ce3": "case_erector_l3",
    "mchighbay3": "highbay_l3",
    "highbay3": "highbay_l3",
    "mcpress3": "press_l3",
    "press3": "press_l3",
    # Loop 4 machines
    "mcckwr4": "check_weigher_l4",
    "ckwr4": "check_weigher_l4",
    "mctp4": "tpmc_l4",
    "tpmc4": "tpmc_l4",
    "mcce4": "case_erector_l4",
    "ce4": "case_erector_l4",
    "mchighbay4": "highbay_l4",
    "highbay4": "highbay_l4",
    "mcpress4": "press_l4",
    "press4": "press_l4",
}

# Mapping for formatted output machine IDs
FORMATTED_MACHINE_MAP = {
    "mc17": "MC 17",
    "mc18": "MC 18",
    "mc19": "MC 19",
    "mc20": "MC 20",
    "mc21": "MC 21",
    "mc22": "MC 22",
    "mc25": "MC 25",
    "mc26": "MC 26",
    "mc27": "MC 27",
    "mc28": "MC 28",
    "mc29": "MC 29",
    "mc30": "MC 30",
    "tpmc3": "Tp MC 3",
    "tpmc4": "Tp MC 4",
    "press3": "Press 3",
    "press4": "Press 4",
    "ce3": "CE 3",
    "ce4": "CE 4",
    "ckwr3": "Ck Wr 3",
    "ckwr4": "Ck Wr 4",
    "highbay3": "HighBay 3",
    "highbay4": "HighBay 4",
    "mcckwr3": "Ck Wr 3",
    "mcckwr4": "Ck Wr 4",
    "mctp3": "Tp MC 3",
    "mctp4": "Tp MC 4",
    "mcce3": "CE 3",
    "mcce4": "CE 4",
    "mchighbay3": "HighBay 3",
    "mchighbay4": "HighBay 4",
    "mcpress3": "Press 3",
    "mcpress4": "Press 4",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TP_DESCRIPTION_FILE = os.path.join(BASE_DIR, "touchpoints.json")

with open(TP_DESCRIPTION_FILE, "r") as f:
    raw_descriptions = json.load(f)
    TP_DETAILS = {
        key.lower(): {
            "title": value.get("title", ""),
            "instruction": value.get("instruction", ""),
            "hasVisible": value.get("hasVisible"),
            "rolesVisibleTo": value.get("rolesVisibleTo", []),
            "prediction_category": value.get("prediction_category", [])
        }
        for item in raw_descriptions
        for key, value in item.items()
    }

def normalize_machine_id(machine_id: str) -> str:
    """
    Normalize machine IDs to match tp_history machine column values.
    Handles inputs like 'MC 17', 'HighBay 3', etc.
    """
    machine_id = machine_id.lower().replace(" ", "")
    mapping = {
        "mc17": "mc17",
        "mc18": "mc18",
        "mc19": "mc19",
        "mc20": "mc20",
        "mc21": "mc21",
        "mc22": "mc22",
        "mc25": "mc25",
        "mc26": "mc26",
        "mc27": "mc27",
        "mc28": "mc28",
        "mc29": "mc29",
        "mc30": "mc30",
        "mcckwr3": "mcckwr3",
        "mcckwr4": "mcckwr4",
        "mcce3": "mcce3",
        "mcce4": "mcce4",
        "mchighbay3": "mchighbay3",
        "mchighbay4": "mchighbay4",
        "mctp3": "mctp3",
        "mctp4": "mctp4",
        "mcpress3": "mcpress3",
        "mcpress4": "mcpress4",
        "tpmc3": "tpmc3",
        "tpmc4": "tpmc4",
        "press3": "press3",
        "press4": "press4",
        "ce3": "ce3",
        "ce4": "ce4",
        "ckwr3": "ckwr3",
        "ckwr4": "ckwr4",
        "highbay3": "highbay3",
        "highbay4": "highbay4",
    }
    return mapping.get(machine_id, machine_id)

def get_table_name(machine_id: str) -> str:
    """
    Map machine_id to the correct TP status table.
    """
    machine_id = normalize_machine_id(machine_id).lower()
    loop4_mapping = {
        "tpmc4": "tpmc_tp_status_loop4",
        "press4": "press_tp_status_loop4",
        "ce4": "case_erector_tp_status_loop4",
        "mcce4": "case_erector_tp_status_loop4",
        "ckwr4": "check_weigher_tp_status_loop4",
        "mcckwr4": "check_weigher_tp_status_loop4",
        "highbay4": "highbay_tp_status_loop4",
        "mchighbay4": "highbay_tp_status_loop4"
    }
    loop3_mapping = {
        "tpmc3": "tpmc_tp_status",
        "press3": "press_tp_status",
        "ce3": "case_erector_tp_status",
        "mcce3": "case_erector_tp_status",
        "ckwr3": "check_weigher_tp_status",
        "mcckwr3": "check_weigher_tp_status",
        "highbay3": "highbay_tp_status",
        "mchighbay3": "highbay_tp_status"
    }
    if machine_id in loop4_mapping:
        return loop4_mapping[machine_id]
    if machine_id in loop3_mapping:
        return loop3_mapping[machine_id]
    return f"{machine_id}_tp_status"

def fetch_tp_status_data(machine_id: str, role: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch all TP columns from the appropriate TP status table where active=1.
    """
    table_name = get_table_name(machine_id)
    conn, cursor = get_server2_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")
    try:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        columns_data = cursor.fetchall()
        tp_keys = [col[0] for col in columns_data if col[0].startswith("tp")]
        if not tp_keys:
            raise HTTPException(status_code=404, detail="No TP columns found")
        where_clauses = [
            sql.SQL("({} ->> 'active')::int = 1").format(sql.Identifier(col))
            for col in tp_keys
        ]
        where_sql = sql.SQL(" OR ").join(where_clauses)
        query = sql.SQL("SELECT * FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            where_sql
        )
        
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            for tp_key in tp_keys:
                tp_data = row_dict.get(tp_key)
                if isinstance(tp_data, dict) and tp_data.get("active") == 1:
                    roles = TP_DETAILS.get(tp_key, {}).get("rolesVisibleTo", [])
                    if role is None or role.lower() in [r.lower() for r in roles]:
                        result.append({
                            "id": tp_key,
                            "uuid": tp_data.get("uuid"),
                            "filepath": tp_data.get("filepath"),
                            "timestamp": tp_data.get("timestamp"),
                            "color_code": tp_data.get("color_code"),
                            "title": TP_DETAILS.get(tp_key, {}).get("title", ""),
                            "instruction": TP_DETAILS.get(tp_key, {}).get("instruction", ""),
                            "hasVisible": TP_DETAILS.get(tp_key, {}).get("hasVisible"),
                            "rolesVisibleTo": roles,
                            "prediction_category": TP_DETAILS.get(tp_key, {}).get("prediction_category")
                        })
        result.sort(key=lambda x: x.get("timestamp"), reverse=True)
        return result
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching TP data")
    finally:
        close_server2_db(conn, cursor)

def mark_tp_inactive(machine: str, target_uuid: str, updated_time: Optional[str] = None):
    """
    Set 'active' = 0 for the TP where 'uuid' matches in the given machine's _tp_status table.
    If not found in _tp_status, check tp_status_lookup and update active to 0 with current timestamp.
    """
    machine = normalize_machine_id(machine)
    table_name = get_table_name(machine)
    conn, cursor = get_server2_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Check if the machine's _tp_status table exists
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
            (table_name,)
        )
        if not cursor.fetchone()[0]:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        # Get all TP columns from the _tp_status table
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        columns = [row[0] for row in cursor.fetchall() if row[0].startswith("tp")]
        if not columns:
            raise HTTPException(status_code=404, detail=f"No TP columns found in table {table_name}")

        # Search for uuid in _tp_status table
        for tp_col in columns:
            cursor.execute(
                sql.SQL("SELECT {col} FROM {table}").format(
                    col=sql.Identifier(tp_col),
                    table=sql.Identifier(table_name)
                ))
            for row in cursor.fetchall():
                data = row[0]
                if data and isinstance(data, dict) and data.get("uuid") == target_uuid:
                    data["active"] = 0
                    data["updated_timestamp"] = updated_time or datetime.now().isoformat()  # Update JSONB's updated_timestamp
                    cursor.execute(
                        sql.SQL("UPDATE {table} SET {col} = %s").format(
                            table=sql.Identifier(table_name),
                            col=sql.Identifier(tp_col)
                        ),
                        (json.dumps(data),)
                    )
                    conn.commit()
                    return {"status": "success", "column": tp_col, "machine": machine}

        # If uuid not found in _tp_status, check tp_status_lookup
        cursor.execute(
            """
            SELECT uuid, tp_column, machine
            FROM tp_status_lookup
            WHERE uuid = %s AND machine = %s AND active = 1
            """,
            (target_uuid, machine)
        )
        lookup_row = cursor.fetchone()
        if lookup_row:
            # Update active to 0 and set updated_time to current timestamp in tp_status_lookup
            cursor.execute(
                """
                UPDATE tp_status_lookup
                SET active = 0, updated_time = %s
                WHERE uuid = %s AND tp_column = %s AND machine = %s
                """,
                (datetime.now().isoformat(), target_uuid, lookup_row[1], machine)
            )
            conn.commit()
            return {"status": "success", "column": lookup_row[1], "machine": machine, "source": "tp_status_lookup"}

        # If uuid not found in either table
        raise HTTPException(status_code=404, detail=f"UUID {target_uuid} not found in {table_name} or tp_status_lookup")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating uuid {target_uuid} in {table_name} or tp_status_lookup: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error marking TP inactive")
    finally:
        close_server2_db(conn, cursor)

async def fetch_historical_work_instructions(
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    loop: Optional[str] = None,
    machine_number: Optional[str] = None,
    shift: Optional[str] = None
    ) -> List[Dict[str, Any]]:
    """Fetch tp_history records based on updated_time, loop, machine number, and shift, sorted by updated_time DESC."""
    conn, cursor = get_server2_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    try:
        loop_machines = {
            '3': ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22', 'tpmc3', 'press3', 'ce3', 'ckwr3', 'highbay3', 'mcckwr3', 'mctp3', 'mcce3', 'mchighbay3', 'mcpress3'],
            '4': ['mc25', 'mc26', 'mc27', 'mc28', 'mc29', 'mc30', 'tpmc4', 'press4', 'ce4', 'ckwr4', 'highbay4', 'mcckwr4', 'mctp4', 'mcce4', 'mchighbay4', 'mcpress4'],
            'both': [
                'mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22',
                'mc25', 'mc26', 'mc27', 'mc28', 'mc29', 'mc30',
                'tpmc3', 'press3', 'ce3', 'ckwr3', 'highbay3',
                'tpmc4', 'press4', 'ce4', 'ckwr4', 'highbay4',
                'mcckwr3', 'mcckwr4', 'mctp3', 'mctp4',
                'mcce3', 'mcce4', 'mchighbay3', 'mchighbay4',
                'mcpress3', 'mcpress4'
            ]
        }
        
        today = date.today()
        default_start = datetime.combine(today, datetime.min.time())
        default_end = datetime.combine(today, datetime.max.time()).replace(microsecond=0)
        
        if start_time:
            try:
                start_time = datetime.fromisoformat(start_time.replace('Z', '+05:30'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_time format. Use ISO 8601 (e.g., 2025-06-20T00:00:00)")
        else:
            start_time = default_start
        
        if end_time:
            try:
                end_time = datetime.fromisoformat(end_time.replace('Z', '+05:30'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_time format. Use ISO 8601 (e.g., 2025-06-20T00:00:00)")
        else:
            end_time = default_end
        
        query = """
            SELECT uuid, tp_column, machine, timestamp, updated_time, filepath, colorcode 
            FROM tp_history
            WHERE updated_time >= %s AND updated_time <= %s
        """
        params = [start_time, end_time]
        print(query)
        if machine_number and machine_number.lower() != 'all':
            normalized_machine = machine_number.lower()
            query += " AND machine = %s"
            print(query)
            params.append(normalized_machine)
            print(params)
        elif loop in loop_machines:
            machines = loop_machines[loop]
            machine_placeholders = ','.join(['%s'] * len(machines))
            query += f" AND machine IN ({machine_placeholders})"
            params.extend(machines)
        else:
            all_machines = loop_machines['both']
            machine_placeholders = ','.join(['%s'] * len(all_machines))
            query += f" AND machine IN ({machine_placeholders})"
            params.extend(all_machines)
        
        if shift:
            shift_condition = get_shift_condition(shift)
            query += f" AND ({shift_condition})"
        
        query += " ORDER BY updated_time DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        result_dict = [dict(zip(columns, row)) for row in results]
        
        machine_data = {}
        for row in result_dict:
            machine = row["machine"].lower()  # Ensure consistent case
            if machine not in machine_data:
                machine_data[machine] = []
            machine_data[machine].append(row)
        
        final_result = []
        for machine, rows in machine_data.items():
            active_count = len(rows)
            formatted_machine = FORMATTED_MACHINE_MAP.get(machine, machine)
            
            tp_data = {}
            for row in rows:
                tp_column = row["tp_column"].lower()
                tp_details = TP_DETAILS.get(tp_column, {})
                
                if tp_column.upper() not in tp_data:
                    tp_data[tp_column.upper()] = []
                
                tp_data[tp_column.upper()].append({
                    "uuid": str(row["uuid"]),
                    "title": tp_details.get("title", ""),
                    "filepath": row["filepath"] or "",
                    "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                    "updated_time": row["updated_time"].isoformat() if row["updated_time"] else None,
                    "color_code": row["colorcode"],
                    "hasVisible": tp_details.get("hasVisible", False),
                    "instruction": tp_details.get("instruction", ""),
                    "rolesVisibleTo": tp_details.get("rolesVisibleTo", []),
                    "prediction_category": tp_details.get("prediction_category", "")
                })
            
            final_result.append({
                "id": formatted_machine,
                "active": str(active_count),
                **tp_data
            })
        
        return final_result
        
    except Exception as e:
        print(f"Error fetching historical work instructions: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        close_server2_db(conn, cursor)
        

def get_shift_condition(shift: str) -> str:
    """
    Generate SQL condition for shift filtering based on updated_time
    Shift A: 07:00 to 15:00 (7 AM to 3 PM)
    Shift B: 15:00 to 23:00 (3 PM to 11 PM)
    Shift C: 23:00 to 07:00+1 day (11 PM to 7 AM next day)
    """
    if shift == 'A':
        return "updated_time::time >= '07:00:00' AND updated_time::time < '15:00:00'"
    elif shift == 'B':
        return "updated_time::time >= '15:00:00' AND updated_time::time < '23:00:00'"
    elif shift == 'C':
        return "updated_time::time >= '23:00:00' OR updated_time::time < '07:00:00'"
    else:
        return "1=1"