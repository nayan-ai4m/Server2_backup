from datetime import datetime,timedelta
from fastapi import HTTPException
import traceback
from database import get_db, close_db


def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60

    if hours > 0:
        return f"{hours} hrs {minutes} mins"
    else:
        return f"{minutes} mins"

def fetch_latest_machine_data(machine_id: str = "mc17"):
    query = """
    SELECT timestamp, machine_no, shift, value, machine_uptime
    FROM cld_history
    WHERE machine_no = %s
    ORDER BY timestamp DESC
    LIMIT 1
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query, (machine_id,))
        result = cursor.fetchone()
    except Exception as e:
        print(f"Error fetching data from machine_data: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from machine_data")
    finally:
        close_db(conn, cursor)

    if not result:
        return {"error": "No data available for the given machine"}

    latest_data = {
        "timestamp": result[0].strftime("%H:%M") if isinstance(result[0], datetime) else result[0],
        "machine_no": result[1],
        "shift": result[2],
        "value": result[3],
        "machine_uptime": format_timedelta(result[4]) if isinstance(result[4], timedelta) else result[4],
    }
    return latest_data