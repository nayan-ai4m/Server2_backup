from database import get_db, close_db  
from fastapi import HTTPException
import traceback
import json

def fetch_prediction_overview_data():
    """Fetches the latest prediction data from prediction_overview where machine names are columns."""
    query = "SELECT * FROM prediction_overview LIMIT 1;"  # Only one row with multiple machine columns

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error for prediction_overview")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]  # Get column names
    except Exception as e:
        print(f"Error fetching data from prediction_overview: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error fetching data from prediction_overview")
    finally:
        close_db(conn, cursor)

    prediction_data = {}
    if result:
        for i, machine_name in enumerate(columns):
            raw_data = result[i]
            if isinstance(raw_data, str):
                try:
                    prediction = json.loads(raw_data)
                except json.JSONDecodeError:
                    prediction = {"error": "Invalid JSON"}
            else:
                prediction = raw_data
            prediction_data[machine_name] = prediction

    return prediction_data
