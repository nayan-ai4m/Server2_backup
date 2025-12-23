from datetime import datetime
from fastapi import HTTPException
import traceback
from database import get_db, close_db, close_db2, get_db2


# def fetch_latest_strokes():
#     query = """
#     SELECT
#         mc17->>'HMI_Hor_Sealer_Strk_1' AS hor_sealer_strk_1,
#         mc17->>'HMI_Hor_Sealer_Strk_2' AS hor_sealer_strk_2
#     FROM loop3_checkpoints
#     WHERE
#         mc17->>'HMI_Hor_Sealer_Strk_1' IS NOT NULL
#         AND mc17->>'HMI_Hor_Sealer_Strk_2' IS NOT NULL
#     ORDER BY timestamp DESC
#     LIMIT 1;
#     """

#     conn, cursor = get_db()
#     if not conn or not cursor:
#         raise HTTPException(status_code=500, detail="Database connection error")

#     try:
#         cursor.execute(query)
#         result = cursor.fetchone()
#         print("Latest strokes result is", result)

#         if result:
#             return {"current_stroke1": result[0], "current_stroke2": result[1]}
#         else:
#             return {"current_stroke1": None, "current_stroke2": None}
#     except Exception as e:
#         print(f"Error fetching latest strokes: {e}")
#         raise HTTPException(status_code=500, detail="Error fetching latest strokes")
#     finally:
#         close_db(conn, cursor)


def fetch_latest_strokes_mc17():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc17'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_strokes_mc18():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc18'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_strokes_mc19():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc19'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_strokes_mc20():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc20'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_strokes_mc21():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc21'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_strokes_mc22():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc22'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract stroke values if available
            stroke1 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_1")
            stroke2 = suggestion_data.get("suggestion", {}).get("HMI_Hor_Sealer_Strk_2")

            return {"current_stroke1": stroke1, "current_stroke2": stroke2}
        else:
            return {"current_stroke1": None, "current_stroke2": None}

    except Exception as e:
        print(f"Error fetching latest suggestions: {e}")
        raise HTTPException(status_code=500, detail="Error fetching latest suggestions")
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc17():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc17'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc18():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc18'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc19():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc19'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc20():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc20'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc21():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc21'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


def fetch_latest_temperature_mc22():
    query = """
    SELECT
        suggestion_details
    FROM suggestions
    WHERE 
        suggestion_details IS NOT NULL
        AND machine_number = 'mc22'
    ORDER BY timestamp DESC
    LIMIT 1;
    """

    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query)
        row = cursor.fetchone()
        print("Latest suggestion row is", row)

        if row and row[0]:
            suggestion_data = row[0]  # JSON from DB
            # Extract temperature values if available
            front_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_27"
            )
            rear_jaw_temp = suggestion_data.get("suggestion", {}).get(
                "HMI_Hor_Seal_Front_28"
            )

            return {
                "current_front_jaw_temp": front_jaw_temp,
                "current_rear_jaw_temp": rear_jaw_temp,
            }
        else:
            return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}

    except Exception as e:
        print(f"Error fetching latest temperatures: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching latest temperatures"
        )
    finally:
        close_db2(conn, cursor)


from fastapi import HTTPException
from database import get_db2, close_db2


def fetch_latest_suggestion(machine_number: str, value_type: str):
    """
    Fetch latest stroke or temperature values for a given machine.

    :param machine_number: Machine number string like 'mc17'
    :param value_type: Either 'strokes' or 'temperature'
    :return: Dict with stroke or temperature values
    """
    query = """
    SELECT
        gsm_suggestions
    FROM suggestions
    WHERE machine_number = %s
    ORDER BY timestamp DESC
    LIMIT 1;
    """


    conn, cursor = get_db2()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        cursor.execute(query, (machine_number,))
        row = cursor.fetchone()
        print(f"Latest suggestion row for {machine_number} is", row)

        if not row or not row[0]:
            # No results
            if value_type == "strokes":
                return {"current_stroke1": None, "current_stroke2": None}
            elif value_type == "temperature":
                return {"current_front_jaw_temp": None, "current_rear_jaw_temp": None}
            else:
                raise ValueError("Invalid value_type")

        suggestion_data = row[0]  # JSON from DB
        #suggestion = suggestion_data.get("suggestion", {})

        if value_type == "strokes":
            return {
                "current_stroke1": suggestion_data.get("suggested_s1"),
                "current_stroke2": suggestion_data.get("suggested_s2"),
            }
        elif value_type == "temperature":
            return {
                "current_front_jaw_temp": suggestion_data.get("suggested_temp"),
                "current_rear_jaw_temp": suggestion_data.get("suggested_temp"),
            }
        else:
            raise ValueError("Invalid value_type")

    except Exception as e:
        print(f"Error fetching latest {value_type} for {machine_number}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching latest {value_type} for {machine_number}",
        )
    finally:
        close_db2(conn, cursor)
