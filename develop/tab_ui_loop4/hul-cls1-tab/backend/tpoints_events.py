import json
import os
from typing import List, Dict, Any, Optional
from datetime import datetime,date
from fastapi import HTTPException, Query, APIRouter
from psycopg2 import sql
import traceback

from database import get_db, close_db




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

def get_table_name(machine_id: str) -> str:
    """
    Map machine_id to the correct TP status table.
    """
    machine_id = machine_id.lower()

    loop4_mapping = {
        "tpmc4": "tpmc_tp_status_loop4",
        "press4": "press_tp_status_loop4",
        "ce4": "case_erector_tp_status_loop4",
        "ckwr4": "check_weigher_tp_status_loop4",
        "highbay4": "highbay_tp_status_loop4"
    }

    loop3_mapping = {
        "tpmc3": "tpmc_tp_status",
        "press3": "press_tp_status",
        "ce3": "case_erector_tp_status",
        "ckwr3": "check_weigher_tp_status",
        "highbay3": "highbay_tp_status"
    }

    if machine_id in loop4_mapping:
        return loop4_mapping[machine_id]
    if machine_id in loop3_mapping:
        return loop3_mapping[machine_id]

    return f"{machine_id}_tp_status"




# def fetch_tp_status_data(machine_id: str) -> List[Dict[str, Any]]:
def fetch_tp_status_data(machine_id: str, role: Optional[str] = None) -> List[Dict[str, Any]]:

    """
    Fetch all TP columns from the appropriate TP status table where active=1.
    """
    table_name = get_table_name(machine_id)
    conn, cursor = get_db()
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
        # print(result)
        return result

    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching TP data")

    finally:
        close_db(conn, cursor)


def mark_tp_inactive(machine: str, target_uuid: str, updated_time: Optional[str] = None):
    """
    Set 'active' = 0 for the TP where 'uuid' matches in the given machine's _tp_status table.
    """
    machine = machine.lower().replace(" ", "")
    table_name = get_table_name(machine)
    # print("machiine",machine ,"\ntablename",table_name)

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
            (table_name,)
        )
        if not cursor.fetchone()[0]:
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,)
        )
        # print(query)
        columns = [row[0] for row in cursor.fetchall() if row[0].startswith("tp")]
        
        if not columns:
            raise HTTPException(status_code=404, detail=f"No TP columns found in table {table_name}")


        # print(f"Searching for UUID: {target_uuid} in table {table_name}")
        for tp_col in columns:
            cursor.execute(
                sql.SQL("SELECT {col} FROM {table}").format(
                    col=sql.Identifier(tp_col),
                    table=sql.Identifier(table_name)
            ))
            
            for row in cursor.fetchall():
                data = row[0]
                if data and isinstance(data, dict):
                    # print(f"Column {tp_col} has UUID: {data.get('uuid')}")
                    if data.get("uuid") == target_uuid:
                        data["active"] = 0
                        if updated_time:
                            data["updated_time"] = updated_time

                        cursor.execute(
                            sql.SQL("UPDATE {table} SET {col} = %s").format(
                                table=sql.Identifier(table_name),
                                col=sql.Identifier(tp_col)
                            ),
                            (json.dumps(data),)
                        )
                        conn.commit()
                        # print(f"TP column {tp_col} for machine {machine} marked inactive.")
                        return {"status": "success", "column": tp_col, "machine": machine}
        
        raise HTTPException(status_code=404, detail=f"UUID {target_uuid} not found in any TP column of {table_name}")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error updating uuid {target_uuid} in {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error marking TP inactive")
    finally:
        close_db(conn, cursor)
        





