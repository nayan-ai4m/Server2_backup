from fastapi import HTTPException
from database import get_db, close_db
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

def insert_gsm_values(data: dict):
    """
    Insert a new row into roll_gsm table, setting the timestamp in Asia/Kolkata timezone
    and the machine column specified by data['key']. Other machine columns are populated
    with their latest non-null values from previous rows, if available.
    """
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        ts = data.get("timestamp")
        if not ts:
            raise HTTPException(status_code=400, detail="Missing timestamp field")

        if isinstance(ts, str):
            try:
                # Parse UTC timestamp and convert to Asia/Kolkata
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts = ts.astimezone(ZoneInfo("Asia/Kolkata"))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid timestamp format")

        key = data.get("key")
        value = data.get("value")

        if not key or value is None:
            raise HTTPException(status_code=400, detail="Missing 'key' or 'value' field")

        machine_columns = {
            "mc17", "mc18", "mc19", "mc20", "mc21", "mc22",
            "mc25", "mc26", "mc27", "mc28", "mc29", "mc30"
        }

        if key not in machine_columns:
            raise HTTPException(status_code=400, detail=f"Invalid machine column key: {key}")

        # Fetch the latest non-null value for each machine column
        latest_values = {col: None for col in machine_columns}
        for col in machine_columns:
            query = f"""
                SELECT {col}
                FROM roll_gsm
                WHERE {col} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                latest_values[col] = result[0]

        # Prepare columns and values for the INSERT query
        columns = ["timestamp"] + list(machine_columns)
        placeholders = ["%s"] * len(columns)
        values = [ts]

        # Populate values for all machine columns
        for col in machine_columns:
            if col == key:
                # Use the new value for the specified column
                values.append(float(value))
            else:
                # Use the latest non-null value, or None if none exists
                values.append(latest_values[col])

        # Construct and execute the INSERT query
        query = f"""
            INSERT INTO roll_gsm ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        cursor.execute(query, values)
        conn.commit()

        return {"status": "success", "message": "GSM value inserted"}

    except Exception as e:
        conn.rollback()
        print(f"Error inserting gsm values: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error inserting gsm values: {str(e)}")

    finally:
        close_db(conn, cursor)


def get_latest_gsm(machine_number: str):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        machine_columns = {
            "mc17", "mc18", "mc19", "mc20", "mc21", "mc22",
            "mc25", "mc26", "mc27", "mc28", "mc29", "mc30"
        }
        if machine_number not in machine_columns:
            raise HTTPException(status_code=400, detail=f"Invalid machine column: {machine_number}")

        query = f"""
            SELECT timestamp, {machine_number}
            FROM roll_gsm
            WHERE {machine_number} IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()

        if not result:
            return {}

        data = {
            "timestamp": str(result[0]),
            machine_number: float(result[1]) if result[1] is not None else None
        }

        return data

    except Exception as e:
        print(f"Error fetching latest gsm value for {machine_number}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching latest gsm value: {str(e)}")

    finally:
        close_db(conn, cursor)