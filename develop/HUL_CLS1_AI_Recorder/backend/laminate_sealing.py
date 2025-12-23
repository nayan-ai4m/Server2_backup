from datetime import datetime
from fastapi import HTTPException
import traceback
from database import get_db, close_db


def fetch_latest_ambient_temperature():
    """
    Fetch the latest ambient temperature from plant_params
    """
    query = """
    SELECT plant_params->>'PLANT_TEMPERATURE' AS plant_temperature
    FROM dark_cascade_overview
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        print("Temp result is ", result)
        temperature_data = result[0] if result else None
        return {"plant_temperature": temperature_data}

    except Exception as e:
        print(f"Error fetching ambient temperature data: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching ambient temperature data"
        )
    finally:
        close_db(conn, cursor)


def fetch_avg_pressure():
    query = """
    SELECT AVG(hor_pressure) 
    FROM mc17
    WHERE cam_position BETWEEN 150 AND 210
      AND spare1 = (
        SELECT spare1
        FROM mc17
        ORDER BY timestamp DESC
        LIMIT 1
      );
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        print("Avg pressure result is ", result)
        avg_pressure = result[0] if result else None
        return {"average_pressure": avg_pressure}
    except Exception as e:
        print(f"Error fetching average pressure: {e}")
        raise HTTPException(status_code=500, detail="Error fetching average pressure")
    finally:
        close_db(conn, cursor)


def fetch_latest_strokes():
    query = """
    SELECT
        mc17->>'HMI_Hor_Sealer_Strk_1' AS hor_sealer_strk_1,
        mc17->>'HMI_Hor_Sealer_Strk_2' AS hor_sealer_strk_2
    FROM loop3_checkpoints
    WHERE
        mc17->>'HMI_Hor_Sealer_Strk_1' IS NOT NULL
        AND mc17->>'HMI_Hor_Sealer_Strk_2' IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        print("Latest strokes result is", result)

        if result:
            return {"current_stroke1": result[0], "current_stroke2": result[1]}
        else:
            return {"current_stroke1": None, "current_stroke2": None}
    except Exception as e:
        print(f"Error fetching latest strokes: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest strokes")
    finally:
        close_db(conn, cursor)
