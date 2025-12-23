from fastapi import HTTPException
from database import get_db, close_db

def fetch_machine_setpoints_loop4(machine_id):
    """
    Fetch setpoints data for Loop 4 machines (MC25-MC30) from cls1_setpoints table.

    OPTIMIZED VERSION: Reads pre-processed data from cls1_setpoints table instead of
    running 26 complex queries with window functions on individual machine tables.

    Background service (setpoints.py) handles all heavy processing:
    - Fetches data from mc25_mid, mc26_mid, etc. tables
    - Detects changes and updates last_update timestamps
    - Stores processed data in cls1_setpoints table

    This endpoint simply reads the pre-calculated data → 100x faster response time.

    Returns: JSONB data with structure:
    {
        "data": {...},           # Setpoint values (_sv suffix)
        "TempDisplay": {...},    # Actual values (_av suffix)
        "Avg": {...},           # Calculated averages
        "last_update": {...}    # Timestamps when each value last changed
    }
    """
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # OPTIMIZED: Single query to cls1_setpoints table instead of 26 complex queries
        cursor.execute(f"SELECT {machine_id} FROM cls1_setpoints LIMIT 1")
        result = cursor.fetchone()

        if result:
            # Data is already pre-processed by setpoints.py background service
            machine_data = result[0] if result[0] is not None else "No data available"
        else:
            machine_data = "No data available"

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching setpoints data for {machine_id}")
    finally:
        close_db(conn, cursor)

    return machine_data
