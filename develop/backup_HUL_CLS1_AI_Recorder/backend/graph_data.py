import json
import traceback
from fastapi import HTTPException
from database import get_device_db, close_db
from datetime import datetime, timedelta
import psycopg2.extras 
# def fetch_device_graph_data(limit=24):
#     """Fetch telemetry data for a device from the database."""
#     conn2, cursor2 = get_device_db()  
#     try:
#         query = """
#         WITH ranked_data AS (
#             SELECT
#                 device_id,
#                 timestamp,
#                 x_rms_vel,
#                 y_rms_vel,
#                 z_rms_vel,
#                 spl_db,
#                 temp_c,
#                 label,
#                 ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
#             FROM public.api_timeseries
#         )
#         SELECT
#             device_id,
#             timestamp,
#             x_rms_vel,
#             y_rms_vel,
#             z_rms_vel,
#             spl_db,
#             temp_c,
#             label
#         FROM ranked_data
#         WHERE row_num = 1
#         ORDER BY timestamp DESC, label DESC
#         LIMIT %s;
#         """
#         cursor2.execute(query, (limit,))
#         result = cursor2.fetchall()
#         # print("Device Data:", result)
        
#         if result:
#             data = [
#                 {
#                     "device_id": row[0],
#                     "timestamp": row[1],
#                     "x_rms_vel": row[2],
#                     "y_rms_vel": row[3],
#                     "z_rms_vel": row[4],
#                     "spl_db": row[5],
#                     "temp_c": row[6],
#                     "label": row[7]
#                 } for row in result
#             ]
#         else:
#             data = []

#         return data

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
#     finally:
#         close_db(conn2, cursor2)      
def fetch_device_graph_data(limit=24):
    """Fetch telemetry data for a device from the database."""
    conn2, cursor2 = get_device_db()
    if not conn2 or not cursor2:
        raise HTTPException(status_code=500, detail="Database connection error")
    
    try:
        query = """
        SELECT DISTINCT ON (label) 
            device_id,
            timestamp,
            x_rms_vel,
            y_rms_vel,
            z_rms_vel,
            spl_db,
            temp_c,
            label
        FROM public.api_timeseries
        ORDER BY label, timestamp DESC
        FETCH FIRST %s ROWS ONLY;
        """
        
        cursor2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)  # Fetch results as dictionary
        cursor2.execute(query, (limit,))
        result = cursor2.fetchall()

        return [dict(row) for row in result] if result else []

    except Exception as e:
        print(f"Error fetching device graph data: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")
    finally:
        close_db(conn2, cursor2)
def fetch_last_5mins_data(machine_name: list = None, title_condition: str = ""):
    """Fetch the last 50 telemetry records, filtered by machine numbers and label contents."""
    conn2, cursor2 = get_device_db()
    try:
        if machine_name:
            machine_conditions = " OR ".join([f"label ILIKE '%{num}%'" for num in machine_name])
            query = f"""
            WITH ranked_data AS (
                SELECT
                    x_rms_vel,
                    y_rms_vel,
                    z_rms_vel,
                    label,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
                FROM
                    api_timeseries
                WHERE
                    ({machine_conditions}) -- Dynamically include the machine numbers
                    AND (label ILIKE '%Vertical%' OR label ILIKE '%Horizontal%') -- Include labels with Vertical or Horizontal
                    {title_condition}  -- Apply the title filter if provided
            )
            SELECT
                x_rms_vel,
                y_rms_vel,
                z_rms_vel,
                label,
                timestamp
            FROM
                ranked_data
            WHERE
                row_num <= 50
            ORDER BY
                label, timestamp DESC;
            """
        else:
            query = f"""
            WITH ranked_data AS (
                SELECT
                    x_rms_vel,
                    y_rms_vel,
                    z_rms_vel,
                    label,
                    timestamp,
                    ROW_NUMBER() OVER (PARTITION BY label ORDER BY timestamp DESC) AS row_num
                FROM
                    api_timeseries
                WHERE
                    label ILIKE '%Vertical%' OR label ILIKE '%Horizontal%' -- Only Vertical or Horizontal labels
                    {title_condition}  -- Apply the title filter if provided
            )
            SELECT
                x_rms_vel,
                y_rms_vel,
                z_rms_vel,
                label,
                timestamp
            FROM
                ranked_data
            WHERE
                row_num <= 50
            ORDER BY
                label, timestamp DESC;
            """
        cursor2.execute(query)

        result = cursor2.fetchall()

        if result:
            data = [
                {
                    "x_rms_vel": row[0],
                    "y_rms_vel": row[1],
                    "z_rms_vel": row[2],
                    "label": row[3],
                    "timestamp": row[4],
                }
                for row in result
            ]
        else:
            data = []

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching telemetry data: {str(e)}")
    finally:
        close_db(conn2, cursor2)