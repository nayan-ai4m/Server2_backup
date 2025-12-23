from datetime import datetime, timedelta
from fastapi import HTTPException
import traceback
from database import get_db, close_db

def format_duration(td):
    """Convert timedelta to 'HH:MM:SS' format."""
    if td is None:
        return "00:00:00"  
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

def fetch_stoppages_history(machine_id: str = None, shift: str = None, start_date: str = None, end_date: str = None, page_no: int = 1, page_size: int = 18):
    count_query = """
    SELECT COUNT(*) FROM machine_stoppages {filter_clause}
    """

    query = """
    SELECT uid, machine, stop_at, stop_till, duration, reason
    FROM machine_stoppages {filter_clause}
    ORDER BY stop_at DESC
    LIMIT %s OFFSET %s
    """

    filters = []
    values = []

    if machine_id:
        filters.append("machine = %s")
        values.append(machine_id)

    if start_date and end_date:
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            filters.append("stop_at::date BETWEEN %s AND %s")
            values.extend([start_date_obj.date(), end_date_obj.date()])
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    shift_times = {
        "shift_1": ("07:00:00", "15:00:00"),
        "shift_2": ("15:00:00", "23:00:00"),
        "shift_3": ("23:00:00", "07:00:00")  # Shift 3 spans two dates
    }

    if shift and shift != "all":
        if shift not in shift_times:
            raise HTTPException(status_code=400, detail="Invalid shift. Use 'shift_1', 'shift_2', 'shift_3', or 'all'.")
        start_time, end_time = shift_times[shift]
        if shift == "shift_3":
            filters.append("(stop_at::time >= %s OR stop_at::time < %s)")
        else:
            filters.append("stop_at::time BETWEEN %s AND %s")
        values.extend([start_time, end_time])

    filter_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    offset = (page_no - 1) * page_size
    values_for_query = values + [page_size, offset]

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        final_count_query = count_query.format(filter_clause=filter_clause)
        cursor.execute(final_count_query, tuple(values))
        total_records = cursor.fetchone()[0]

        final_query = query.format(filter_clause=filter_clause)
        cursor.execute(final_query, tuple(values_for_query))
        result = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from machine_stoppages: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from machine_stoppages")
    finally:
        close_db(conn, cursor)

    stoppages = []
    for row in result:
        stop_at_time = row[2] if isinstance(row[2], datetime) else datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")

        # Determine the shift correctly
        shift_label = "unknown"
        stop_at_time_only = stop_at_time.time()

        for shift_name, (start_time, end_time) in shift_times.items():
            start_time = datetime.strptime(start_time, "%H:%M:%S").time()
            end_time = datetime.strptime(end_time, "%H:%M:%S").time()

            if shift_name == "shift_3":
                if stop_at_time_only >= start_time or stop_at_time_only < end_time:
                    shift_label = shift_name
                    break
            else:
                if start_time <= stop_at_time_only <= end_time:
                    shift_label = shift_name
                    break

        stoppages.append({
            "uid": row[0],
            "machine": row[1],
            "stop_at": stop_at_time.strftime("%H:%M"),
            "stop_till": row[3].strftime("%H:%M") if isinstance(row[3], datetime) else row[3],
           # "duration": format_duration(row[4]),
            "duration": format_duration(row[4]) if row[4] is not None else "00:00:00",
            "reason": row[5],
            "date": stop_at_time.strftime("%d-%m-%Y"),
            "shift": shift_label
        })

    return {
        "total_records": total_records,
        "stoppages": stoppages,
        "selected_start_date": start_date,
        "selected_end_date": end_date,
        "selected_shift": shift
    }


def fetch_production_history(machine_id: str = None, shift: str = None, start_date: str = None, end_date: str = None, page_no: int = 1, page_size: int = 18):
    count_query = """
    SELECT COUNT(*) FROM cld_history {filter_clause}
    """

    query = """
    SELECT timestamp, machine_no, shift, value, machine_uptime
    FROM cld_history {filter_clause}
    ORDER BY timestamp DESC
    LIMIT %s OFFSET %s
    """

    filters = []
    values = []

    if machine_id:
        filters.append("machine_no = %s")
        values.append(machine_id)

    if start_date and end_date:
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            filters.append("timestamp::date BETWEEN %s AND %s")
            values.extend([start_date_obj.date(), end_date_obj.date()])
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    """
    if shift and shift.lower() != 'all':
        filters.append("shift = %s")
        values.append(shift)
    """
    if shift and shift.lower() != 'all':
        filters.append("LOWER(shift) = LOWER(%s)")
        values.append(shift)
    filter_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    offset = (page_no - 1) * page_size
    values_for_query = values + [page_size, offset]

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        final_count_query = count_query.format(filter_clause=filter_clause)
        cursor.execute(final_count_query, tuple(values))
        total_records = cursor.fetchone()[0]

        final_query = query.format(filter_clause=filter_clause)
        cursor.execute(final_query, tuple(values_for_query))
        results = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching data from cld_history: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from cld_history")
    finally:
        close_db(conn, cursor)

    machine_data = [
        {
            "timestamp": row[0].strftime("%H:%M"),
            "machine_no": row[1],
            "shift": row[2],
            "value": row[3],
            "machine_uptime": format_duration(row[4]),
            "date": row[0].strftime("%d-%m-%Y")
        }
        for row in results
    ]

    return {
        "total_records": total_records,
        "page_no": page_no,
        "machine_data": machine_data if machine_data else "No records found"
    }
