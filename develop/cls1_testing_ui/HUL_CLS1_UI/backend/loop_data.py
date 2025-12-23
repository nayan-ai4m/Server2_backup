
from database import get_db, close_db
from fastapi import HTTPException
import traceback

def get_previous_shift(current_shift: str) -> str:
    """Determine previous shift in the cycle A -> B -> C -> A"""
    shift_cycle = {
        "A": "C",
        "B": "A",
        "C": "B"
    }
    return shift_cycle.get(current_shift, "C")

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

def fetch_loop_data_with_previous(table_name: str, columns: list):
    """Fetch both current and previous shift data from a given table."""
    # Query for current shift (latest record)
    current_query = f"""
    SELECT {', '.join(columns)}
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail=f"Database connection error for {table_name}")

    try:
        # Get current shift data
        cursor.execute(current_query)
        current_result = cursor.fetchone()

        if not current_result:
            return {"current_shift": {}, "previous_shift": {}}

        # Convert current result to dictionary
        current_data = {columns[i]: current_result[i] for i in range(len(columns))}

        # Shift mapping
        shift_mapping = {
            "S:1": "Shift A",
            "S:2": "Shift B",
            "S:3": "Shift C"
        }

        # Get the raw shift value before mapping for determining previous shift
        current_shift_raw = current_result[columns.index("shift")]

        # Apply shift mapping for current data
        if "shift" in current_data and current_data["shift"] in shift_mapping:
            current_data["shift"] = shift_mapping[current_data["shift"]]

        # Determine previous shift (using raw value)
        previous_shift_letter = get_previous_shift(current_shift_raw)

        # Query for previous shift data
        previous_query = f"""
        SELECT {', '.join(columns)}
        FROM {table_name}
        WHERE shift = %s
        ORDER BY last_update DESC
        LIMIT 1
        """

        cursor.execute(previous_query, (previous_shift_letter,))
        previous_result = cursor.fetchone()

        # Convert previous result to dictionary
        if previous_result:
            previous_data = {columns[i]: previous_result[i] for i in range(len(columns))}

            # Apply shift mapping for previous data
            if "shift" in previous_data and previous_data["shift"] in shift_mapping:
                previous_data["shift"] = shift_mapping[previous_data["shift"]]
        else:
            previous_data = {}

        return {
            "current_shift": current_data,
            "previous_shift": previous_data
        }

    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
    finally:
        close_db(conn, cursor)

def fetch_loop3_data():
    columns = [
        "last_update", "primary_tank_level", "secondary_tank_level", "bulk_settling_time",
        "ega", "cld_production", "taping_machine", "case_erector", "plantair", "planttemperature",
        "batch_sku", "cobot_status", "shift", "checkweigher"
    ]
    return fetch_loop_data_with_previous("loop3_overview_new", columns)

def fetch_loop4_data():
    columns = [
        "last_update", "primary_tank_level", "secondary_tank_level", "bulk_settling_time",
        "ega", "cld_production", "taping_machine", "case_erector", "batch_sku", "cobot_status", "shift","checkweigher"
    ]
    return fetch_loop_data_with_previous("loop4_overview_new", columns)
