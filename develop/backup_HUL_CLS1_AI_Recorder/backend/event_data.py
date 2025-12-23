from datetime import datetime
from fastapi import HTTPException
import traceback
from database import get_db, close_db

# def fetch_today_events(machine_name: str = None):
#     query = """
#     SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark, 
#            resolution_time, event_type, alert_type, assigned_time, acknowledge
#     FROM event_table 
#     WHERE timestamp >= NOW() - INTERVAL '7 days'  -- Ensures last 7 days 
#           AND event_id IS NOT NULL
#     """
#     params = []

#     if machine_name:
#         query += " AND zone LIKE %s"
#         params.append(f"%{machine_name}%")  

#     query += " ORDER BY timestamp DESC;"
#     print("Final Query",query)
#     conn, cursor = get_db()
#     if not conn or not cursor:
#         raise HTTPException(status_code=500, detail="Database connection error")

#     try:
#         cursor.execute(query, tuple(params))
#         result = cursor.fetchall()
#     except Exception as e:
#         print(f"Error fetching data from sample_event_table: {e}")
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail="Error fetching data from sample_event_table")
#     finally:
#         close_db(conn, cursor)

#     events = [
#         {
#             "timestamp": row[0],
#             "event_id": row[1],  
#             "zone": row[2],
#             "camera_id": row[3],
#             "assigned_to": row[4],
#             "action": row[5],
#             "remark": row[6],
#             "resolution_time": row[7],
#             "event_type": row[8],
#             "alert_type": row[9],
#             "assigned_time": row[10],
#             "acknowledge": row[11]
#         }
#         for row in result
#     ]
#     #print("\nEvents:\n",events)
#     return events



def fetch_today_events(machine_name: str = None, assigned_to: str = None):
    today_date = datetime.today().date()  # Get today's date
    query = """
    SELECT timestamp, event_id, zone, camera_id, assigned_to, action, remark, 
           resolution_time, event_type, alert_type, assigned_time, acknowledge
    FROM event_table 
    WHERE DATE(timestamp) = %s AND event_id IS NOT NULL AND (action IS NULL OR action IN ('Assigned', 'Reassigned'))
    """
    params = [today_date]
    
    if machine_name:
        query += " AND camera_id LIKE %s"
        params.append(f"%{machine_name}%")

    if assigned_to:
        query += " AND assigned_to LIKE %s"
        params.append(f"%{assigned_to}%")

    query += " ORDER BY timestamp DESC"

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from event_table: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from event_table")
    finally:
        close_db(conn, cursor)

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
    return events


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
