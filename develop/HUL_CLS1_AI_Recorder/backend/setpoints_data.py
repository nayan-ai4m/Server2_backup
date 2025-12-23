from fastapi import HTTPException
from database import get_db, close_db

def fetch_machine_setpoints(machine_id):
    """
    Unified function to fetch setpoints data for ALL machines (MC17-MC30) from cls1_setpoints table.

    This replaces the separate Loop 3 and Loop 4 functions with a single implementation.
    All machines now use the same cls1_setpoints table structure.

    Background: A background service (setpoints.py) handles all heavy processing:
    - Fetches data from individual machine tables (mc17_mid, mc18_mid, mc25_mid, etc.)
    - Detects changes and updates last_update timestamps
    - Stores processed data in cls1_setpoints table

    This endpoint simply reads the pre-calculated data → fast response time.

    Returns: JSONB data with structure:
    {
        "data": {...},           # Setpoint values (_sv suffix)
        "TempDisplay": {...},    # Actual values (_av suffix)
        "Avg": {...},           # Calculated averages (for Loop 4)
        "last_update": {...}    # Timestamps when each value last changed
    }
    """
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(f"SELECT {machine_id} FROM cls1_setpoints LIMIT 1")
        result = cursor.fetchone()

        if result:
            machine_data = result[0] if result[0] is not None else "No data available"
        else:
            machine_data = "No data available"

    except Exception as e:
        print(f"Error fetching setpoints data for {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching setpoints data for {machine_id}")
    finally:
        close_db(conn, cursor)

    return machine_data


def fetch_machine_setpoints_data(machine_id):
    """
    Legacy function for Loop 3 machines (MC17-MC22).
    Maintained for backward compatibility.
    Calls the unified fetch_machine_setpoints() function.
    """
    return fetch_machine_setpoints(machine_id)


def fetch_machine_setpoints_loop4(machine_id):
    """
    Legacy function for Loop 4 machines (MC25-MC30).
    Maintained for backward compatibility.
    Calls the unified fetch_machine_setpoints() function.
    """
    return fetch_machine_setpoints(machine_id)


def fetch_recent_machine_setpoints(machine_id):
    """
    Fetch the last 5 setpoint values for a given machine.
    Used by the sealar_front_temperature endpoint for both Loop 3 and Loop 4.
    """
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    last_5_values = []
    try:
        query = f"""
            SELECT {machine_id}
            FROM cls1_setpoints
            WHERE {machine_id} IS NOT NULL
            ORDER BY row_number() OVER () DESC  -- Emulate recency
            LIMIT 5
        """
        cursor.execute(query)
        results = cursor.fetchall()
        for result in results:
            last_5_values.append(result[0])
        last_5_values.reverse()

        if not last_5_values:
            raise HTTPException(status_code=404, detail="No recent data available in cls1_setpoints")

    except Exception as e:
        print(f"Error fetching recent setpoints data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
    finally:
        close_db(conn, cursor)

    return last_5_values
