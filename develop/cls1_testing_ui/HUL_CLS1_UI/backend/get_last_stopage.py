from datetime import datetime, timedelta
from fastapi import HTTPException
import traceback
from database import get_db, close_db

def format_duration(td):
    """Convert timedelta to 'HH:MM:SS' format."""
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"
def fetch_last_10_stoppages(machine_id: str = None):
    query = """
    SELECT uid, machine, stop_at, stop_till, duration, reason
    FROM machine_stoppages
    {filter_clause}
    ORDER BY stop_at DESC
    LIMIT 10
    """

    filter_clause = "WHERE machine = %s" if machine_id else ""

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        final_query = query.format(filter_clause=filter_clause)
        if machine_id:
            cursor.execute(final_query, (machine_id,))
        else:
            cursor.execute(final_query)

        result = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from machine_stoppages: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from machine_stoppages")
    finally:
        close_db(conn, cursor)

    stoppages = [
        {
            "uid": row[0],
            "machine": row[1],
            "stop_at": row[2].strftime("%H:%M") if isinstance(row[2], datetime) else row[2],
            "stop_till": row[3].strftime("%H:%M") if isinstance(row[3], datetime) else row[3],
            "duration": format_duration(row[4]) if isinstance(row[4], timedelta) else row[4],
            "reason": row[5],
        }
        for row in result
    ]
    return stoppages
