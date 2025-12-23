from datetime import datetime
from fastapi import HTTPException
import traceback
from database import get_db, close_db

# Define auxiliary equipment groupings
# Maps dropdown option to list of camera_ids in database
AUXILIARY_EQUIPMENT_MAP = {
    # Taping Machines
    "Taping Machine 3": ['Lpre_taping_l3_mat', 'Ll3_taping_taping', 'L3-Pre-TM'],
    "Taping Machine 4": ['Lpre_taping_l4_mat', 'Ll4_taping_taping'],

    # Case Erectors (loop3/loop4 are Case Erector data + EOL)
    "Case Erector 3": ['loop3'],
    "Case Erector 4": ['loop4'],

    # Checkweigher (combined - shows all checkweigher data)
    "Checkweigher 3": ['Check Weigher', 'L3_EOL'],
    "Checkweigher 4": ['Check Weigher', 'L4_EOL'],

    # High Bay Storage
    "HighBay 3": ['L3_High-bay'],
    "HighBay 4": ['L4_High-bay']
}



def fetch_today_events(
    machine_name: str = None,
    assigned_to: str = None,
    page_no: int = 1,
    page_size: int = 10,
    alert_type: str = None,
    source: str = None
):
    """
    Fetch today's events with proper pagination support.

    Args:
        machine_name: Filter by machine/camera ID or special value "Support Machines"
        assigned_to: Filter by assigned user
        page_no: Page number (starts from 1)
        page_size: Number of records per page
        alert_type: Filter by alert type (Productivity, Breakdown, Quality)
        source: Filter by source (Smart Camera, PLC, IOT Sensor, etc.)

    Returns:
        dict: {
            "events": [...],
            "total_records": int,
            "page": int,
            "page_size": int
        }
    """
    today_date = datetime.today().date()

    # Base WHERE clause
    base_where = """
    WHERE DATE(timestamp) = %s
      AND event_id IS NOT NULL
      AND (action IS NULL OR action IN ('Assigned', 'Reassigned'))
    """
    params = [today_date]

    # Add filters dynamically (using ILIKE for case-insensitive matching)
    if machine_name:
        if machine_name in AUXILIARY_EQUIPMENT_MAP:
            # Filter for auxiliary equipment using explicit IN clause
            camera_ids = AUXILIARY_EQUIPMENT_MAP[machine_name]
            placeholders = ','.join(['%s'] * len(camera_ids))
            base_where += f" AND camera_id IN ({placeholders})"
            params.extend(camera_ids)
        else:
            # For main machines, include both the machine and its outfeed camera
            machine_lower = machine_name.lower()
            base_where += """ AND (
                camera_id ILIKE %s OR
                camera_id ILIKE %s OR
                camera_id = %s
            )"""
            params.append(machine_name)  # Exact match (e.g., "MC17")
            params.append(f"{machine_lower}")  # Lowercase (e.g., "mc17")
            params.append(f"{machine_lower}_outfeed")  # Outfeed camera (e.g., "mc17_outfeed")

    if assigned_to:
        base_where += " AND assigned_to ILIKE %s"
        params.append(f"%{assigned_to}%")

    if alert_type:
        base_where += " AND alert_type ILIKE %s"
        params.append(f"%{alert_type}%")

    if source:
        base_where += " AND zone ILIKE %s"
        params.append(f"%{source}%")

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Step 1: Get total count of records
        count_query = f"SELECT COUNT(*) FROM event_table {base_where}"
        cursor.execute(count_query, tuple(params))
        total_records = cursor.fetchone()[0]

        # Step 2: Calculate offset for pagination
        offset = (page_no - 1) * page_size

        # Step 3: Fetch paginated data
        data_query = f"""
        SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark,
               resolution_time, event_type, alert_type, assigned_time, acknowledge
        FROM event_table
        {base_where}
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
        """

        # Add pagination params
        paginated_params = params + [page_size, offset]
        cursor.execute(data_query, tuple(paginated_params))
        result = cursor.fetchall()

    except Exception as e:
        print(f"Error fetching data from event_table: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from event_table")
    finally:
        close_db(conn, cursor)

    # Map results to dict
    events = [
        {
            "timestamp": row[0],
            "event_id": row[1],
            "zone": row[2],
            "camera_id": row[3],
            "assigned_to": row[4],
            "action": row[5],
            "remark": row[6],
            "resolution_time": row[7],
            "event_type": row[8],
            "alert_type": row[9],
            "assigned_time": row[10],
            "acknowledge": row[11]
        }
        for row in result
    ]

    # Return with pagination metadata
    return {
        "events": events,
        "total_records": total_records,
        "page": page_no,
        "page_size": page_size
    }


def fetch_latest_event():
    query = """
    SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark, 
           resolution_time, event_type, alert_type, assigned_time, acknowledge 
    FROM event_table
    WHERE assigned_to IS NULL
    ORDER BY timestamp DESC
    LIMIT 1
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()

        if result is None:
            count_cursor = conn.cursor()
            try:
                count_cursor.execute("SELECT COUNT(*) FROM event_table")
                total_events = count_cursor.fetchone()[0]

                if total_events > 0:
                    return {"message": "All events are assigned"}
                else:
                    return {"message": "No events found"}
            finally:
                count_cursor.close()
        else:
            event = {
                "timestamp": result[0],
                "event_id": result[1],
                "zone": result[2],
                "camera_id": result[3],
                "assigned_to": result[4],
                "action": result[5],
                "remark": result[6],
                "resolution_time": result[7],
                "event_type": result[8],
                "alert_type": result[9],
                "assigned_time": result[10],
                "acknowledge": result[11]
            }
            return event
    except Exception as e:
        print(f"Error fetching data from event_table: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from event_table")
    finally:
        close_db(conn, cursor)