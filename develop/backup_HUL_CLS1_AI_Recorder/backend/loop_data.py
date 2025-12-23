# from database import get_db, close_db  # Importing necessary functions from database.py
# from fastapi import FastAPI, HTTPException
# import traceback


# def fetch_loop3_data():
#     query = """
#     SELECT last_update, primary_tank_level, secondary_tank_level, bulk_settling_time,
#            ega, cld_production, taping_machine, case_erector, plantair, planttemperature
#     FROM loop3_overview 
#     ORDER BY last_update DESC
#     LIMIT 1
#     """
    
#     conn, cursor = get_db()  # Now this will use the imported get_db() function
#     if not conn or not cursor:
#         raise HTTPException(status_code=500, detail="Main database connection error")

#     try:
#         cursor.execute(query)
#         result = cursor.fetchone()
#     except Exception as e:
#         print(f"Error fetching data from loop3_overview: {e}")
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail="Error fetching data from loop3_overview")
#     finally:
#         close_db(conn, cursor)

#     if result:
#         return {
#             "last_update": result[0],
#             "primary_tank_level": result[1],
#             "secondary_tank_level": result[2],
#             "bulk_settling_time": result[3],
#             "ega": result[4],
#             "cld_production": result[5],
#             "taping_machine": result[6],
#             "case_erector": result[7],
#             "plantair": result[8],
#             "planttemperature": result[9]
#         }
#     return {}

# def fetch_loop4_data():
#     query = """
#     SELECT last_update, primary_tank_level, secondary_tank_level, bulk_settling_time,
#            ega, cld_production, taping_machine, case_erector 
#     FROM loop4_overview
#     ORDER BY last_update DESC
#     LIMIT 1
#     """
    
#     conn, cursor = get_db()
#     if not conn or not cursor:
#         raise HTTPException(status_code=500, detail="Secondary database connection error")

#     try:
#         cursor.execute(query)
#         result = cursor.fetchone()
#     except Exception as e:
#         print(f"Error fetching data from loop4_overview: {e}")
#         print(traceback.format_exc())
#         raise HTTPException(status_code=500, detail="Error fetching data from loop4_overview")
#     finally:
#         close_db(conn, cursor)

#     if result:
#         return {
#             "last_update": result[0],
#             "primary_tank_level": result[1],
#             "secondary_tank_level": result[2],
#             "bulk_settling_time": result[3],
#             "ega": result[4],
#             "cld_production": result[5],
#             "taping_machine": result[6],
#             "case_erector": result[7]
#         }
#     return {}

from database import get_db, close_db  # Importing necessary functions from database.py
from fastapi import HTTPException
import traceback

def fetch_loop_data(table_name: str, columns: list):
    """Generic function to fetch the latest record from a given table."""
    query = f"""
    SELECT {', '.join(columns)}
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail=f"Database connection error for {table_name}")

    try:
        cursor.execute(query)
        result = cursor.fetchone()
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
    finally:
        close_db(conn, cursor)

    # Convert result to a dictionary
    data = {columns[i]: result[i] for i in range(len(columns))} if result else {}

    # Shift mapping
    shift_mapping = {
        "S:1": "Shift A",
        "S:2": "Shift B",
        "S:3": "Shift C"
    }

    # Replace shift value if present
    if "shift" in data and data["shift"] in shift_mapping:
        data["shift"] = shift_mapping[data["shift"]]

    return data

def fetch_loop3_data():
    columns = [
        "last_update", "primary_tank_level", "secondary_tank_level", "bulk_settling_time",
        "ega", "cld_production", "taping_machine", "case_erector", "plantair", "planttemperature",
        "batch_sku", "cobot_status", "shift"
    ]
    return fetch_loop_data("loop3_overview", columns)

def fetch_loop4_data():
    columns = [
        "last_update", "primary_tank_level", "secondary_tank_level", "bulk_settling_time",
        "ega", "cld_production", "taping_machine", "case_erector", "batch_sku", "cobot_status", "shift"
    ]
    return fetch_loop_data("loop4_overview", columns)
