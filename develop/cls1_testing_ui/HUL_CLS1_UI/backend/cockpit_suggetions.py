from fastapi import HTTPException
from datetime import datetime, timezone
from database import get_db, close_db
from typing import Union, Optional 
from tpoints_events import fetch_tp_status_data 

def fetch_cockpit_suggetions(machine_number: str, role: Optional[str] = None):
    today = datetime.now(timezone.utc).date()
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    query = """
    SELECT timestamp, alert_details, suggestion_details, acknowledge, acknowledge_time, ignored_time
    FROM suggestions 
    WHERE machine_number = %s 
      AND timestamp::date = %s 
    ORDER BY timestamp DESC 
    LIMIT 1
    """
    
    try:
        cursor.execute(query, (machine_number, today))
        result = cursor.fetchone()
    
        if not result:
            raise HTTPException(status_code=404, detail="No data available in suggestions")
        
        timestamp = result[0]
        alert_data = result[1] or {}
        suggestion_details = result[2] or {}
        acknowledge = result[3]
        acknowledge_time = result[4]
        ignored_time = result[5]

        suggestion_only = suggestion_details.get("suggestion", {})

        tp_status_data = fetch_tp_status_data(machine_number, role)
        alert_tp = str(alert_data.get("tp", ""))
        formatted_alert_tp = f"tp{alert_tp}" if alert_tp.isdigit() else alert_tp

        include_suggestion = any(tp.get("id") == formatted_alert_tp for tp in tp_status_data)

        response = {
            "timestamp": timestamp,
            "alert_details": alert_data,
            "acknowledge": acknowledge,
            "acknowledge_time": acknowledge_time,
            "ignored_time": ignored_time
        }

        if include_suggestion:
            response["suggestion"] = suggestion_only

        return response
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from suggestions")
    finally:
        close_db(conn, cursor)


def update_laminate_settings(
    machine_number: str,
    status: Optional[str],
    acknowledge_time: Optional[str],
    ignored_time: Optional[str],
    timestamp: str,
    role: Optional[str] = None
):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Check current acknowledge status and acknowledge_time for the given machine_number and timestamp
        check_query = """
        SELECT acknowledge, acknowledge_time
        FROM suggestions 
        WHERE machine_number = %s 
          AND timestamp = %s
        """
        cursor.execute(check_query, (machine_number, timestamp))
        existing_record = cursor.fetchone()

        # If record exists and acknowledge is 2 with a non-null acknowledge_time, prevent update
        if existing_record and existing_record[0] == "2" and existing_record[1] is not None:
            raise HTTPException(
                status_code=400,
                detail="Cannot update to ignored status (3) when acknowledge status is 2 with a set acknowledge_time"
            )

        # If status is 3 and ignored_time is None, set ignored_time to current time
        if status == "3" and ignored_time is None:
            ignored_time = datetime.now(timezone.utc).isoformat()

        # Update query
        update_query = """
        UPDATE suggestions 
        SET acknowledge = %s,
            acknowledge_time = %s,
            ignored_time = %s
        WHERE machine_number = %s 
          AND timestamp = %s
        RETURNING timestamp, alert_details, suggestion_details, acknowledge, acknowledge_time, ignored_time
        """
        
        cursor.execute(update_query, (
            status,
            acknowledge_time if acknowledge_time else None,
            ignored_time if ignored_time else None,
            machine_number,
            timestamp
        ))
        conn.commit()
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found to update for the specified machine and timestamp")
        
        timestamp, alert_data, suggestion_details, acknowledge, acknowledge_time, ignored_time = result
        suggestion_only = suggestion_details.get("suggestion", {})

        # Apply TP-matching logic
        tp_status_data = fetch_tp_status_data(machine_number, role)
        alert_tp = str(alert_data.get("tp", ""))
        formatted_alert_tp = f"tp{alert_tp}" if alert_tp.isdigit() else alert_tp
        include_suggestion = any(tp.get("id") == formatted_alert_tp for tp in tp_status_data)

        response = {
            "timestamp": timestamp,
            "alert_details": alert_data,
            "acknowledge": acknowledge,
            "acknowledge_time": acknowledge_time,
            "ignored_time": ignored_time
        }

        if include_suggestion:
            response["suggestion"] = suggestion_only

        return response
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Error updating laminate settings: {e}")
        raise HTTPException(status_code=500, detail="Error updating laminate settings")
    finally:
        close_db(conn, cursor)