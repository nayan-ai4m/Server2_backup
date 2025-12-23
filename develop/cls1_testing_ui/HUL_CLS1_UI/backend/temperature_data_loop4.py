from fastapi import HTTPException
from database import get_db, close_db
from datetime import datetime, timedelta
import pytz

def calculate_laminate_wastage(machine_id: str, conn, cursor):
    """
    Calculate laminate wastage for Loop 4 machines based on filling_on_off transitions.

    Logic:
    1. Track filling_on_off = 0 (filling OFF) → capture timestamp (wastage starts)
    2. Track filling_on_off = 1 (filling ON) → capture timestamp (wastage ends)
    3. Calculate duration between OFF→ON transitions
    4. Sum durations for current shift
    5. Convert time to meters using machine speed

    Returns:
    {
        "laminate_length": <meters>,
        "laminate_duration": <minutes>
    }
    """
    try:
        table_name = f"{machine_id}_mid"

        # Get current shift boundaries (IST timezone)
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)

        # Determine current shift boundaries
        # Shift A: 7-15h, Shift B: 15-23h, Shift C: 23-7h
        current_hour = now.hour

        if 7 <= current_hour < 15:  # Shift A
            shift_start = now.replace(hour=7, minute=0, second=0, microsecond=0)
            shift_end = now.replace(hour=15, minute=0, second=0, microsecond=0)
        elif 15 <= current_hour < 23:  # Shift B
            shift_start = now.replace(hour=15, minute=0, second=0, microsecond=0)
            shift_end = now.replace(hour=23, minute=0, second=0, microsecond=0)
        else:  # Shift C (23-7h, spans midnight)
            if current_hour >= 23:
                shift_start = now.replace(hour=23, minute=0, second=0, microsecond=0)
                shift_end = (now + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0)
            else:  # 0-7h
                shift_start = (now - timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
                shift_end = now.replace(hour=7, minute=0, second=0, microsecond=0)

        # Query filling_on_off data for current shift
        query = f"""
            SELECT timestamp, filling_on_off
            FROM {table_name}
            WHERE timestamp >= %s AND timestamp < %s
            AND filling_on_off IS NOT NULL
            ORDER BY timestamp ASC
        """
        cursor.execute(query, (shift_start, shift_end))
        results = cursor.fetchall()

        if not results or len(results) < 2:
            # No data or insufficient data for calculation
            return {"laminate_length": 0, "laminate_duration": 0}

        # Calculate total wastage duration
        total_wastage_seconds = 0
        off_timestamp = None

        for i in range(len(results)):
            timestamp, filling_status = results[i]

            if filling_status == 0:
                # Filling turned OFF - start tracking wastage
                if off_timestamp is None:
                    off_timestamp = timestamp
            elif filling_status == 1 and off_timestamp is not None:
                # Filling turned ON - end tracking wastage
                on_timestamp = timestamp
                duration = (on_timestamp - off_timestamp).total_seconds()
                total_wastage_seconds += duration
                off_timestamp = None  # Reset for next cycle

        # If still OFF at end of shift, count duration till now
        if off_timestamp is not None:
            duration = (now - off_timestamp).total_seconds()
            total_wastage_seconds += duration

        # Convert to minutes
        laminate_duration = round(total_wastage_seconds / 60, 2)

        # Calculate laminate wastage using the actual formula
        # Formula: total_seconds / 0.6 * 7 = waste in cm
        # Then convert cm to meters
        laminate_wastage_cm = (total_wastage_seconds / 0.6) * 7
        laminate_length = round(laminate_wastage_cm / 100, 2)  # Convert cm to meters

        return {
            "laminate_length": laminate_length,
            "laminate_duration": laminate_duration
        }

    except Exception as e:
        print(f"Error calculating laminate wastage for {machine_id}: {e}")
        return {"laminate_length": 0, "laminate_duration": 0}

def fetch_machine_temperatures_loop4(machine_id: str):
    """
    Fetch temperature and hopper level data for Loop 4 machines (MC25-MC30).

    Queries individual mc{id}_mid tables for temperatures and mc{id}_fast for hopper levels.

    Returns:
    {
        "machine_id": "mc25",
        "vertical_sealing": {
            "front_temp": <average of 13 zones>,
            "rear_temp": <average of 13 zones>
        },
        "horizontal_sealing": {
            "front_temp": <single value>,
            "rear_temp": <single value>
        },
        "hopper_levels": {
            "hopper_left_level": <value>,
            "hopper_right_level": <value>
        }
    }
    """
    machine_num = int(machine_id.replace('mc', '').replace('MC', ''))

    # Validate Loop 4 machines only
    if machine_num not in [25, 26, 27, 28, 29, 30]:
        raise HTTPException(status_code=400, detail=f"Invalid Loop 4 machine_id: {machine_id}. Use MC25-MC30 only.")

    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        # Query individual mc{id}_mid table for temperatures
        table_name_mid = f"{machine_id}_mid"
        cursor.execute(f"SELECT * FROM {table_name_mid} ORDER BY timestamp DESC LIMIT 1")
        result = cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail=f"No data available for {machine_id}")

        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        row_dict = dict(zip(column_names, result))

        # Calculate vertical sealer averages (zones 1-13, using PV values)
        front_temps = [
            row_dict.get(f"vert_front_{i}_pv")
            for i in range(1, 14)
            if row_dict.get(f"vert_front_{i}_pv") is not None
        ]
        rear_temps = [
            row_dict.get(f"vert_rear_{i}_pv")
            for i in range(1, 14)
            if row_dict.get(f"vert_rear_{i}_pv") is not None
        ]

        vertical_front_avg = round(sum(front_temps) / len(front_temps), 2) if front_temps else 0
        vertical_rear_avg = round(sum(rear_temps) / len(rear_temps), 2) if rear_temps else 0

        # Get horizontal sealer temps (single values)
        horizontal_front = row_dict.get("hor_temp_27_pv_front", 0)
        horizontal_rear = row_dict.get("hor_temp_28_pv_rear", 0)

        # Query mc{id}_fast table for hopper levels
        # MC25, MC26 = Dual hopper (hopper_left_level, hopper_right_level)
        # MC27, MC28, MC29, MC30 = Single hopper (hopper_level only)
        table_name_fast = f"{machine_id}_fast"

        hopper_left = 0
        hopper_right = 0

        if machine_num in [25, 26]:
            # Dual-hopper machines
            cursor.execute(f"SELECT hopper_left_level, hopper_right_level FROM {table_name_fast} ORDER BY timestamp DESC LIMIT 1")
            hopper_result = cursor.fetchone()
            if hopper_result:
                hopper_left = hopper_result[0] if hopper_result[0] is not None else 0
                hopper_right = hopper_result[1] if hopper_result[1] is not None else 0
        else:
            # Single-hopper machines (MC27, MC28, MC29, MC30)
            cursor.execute(f"SELECT hopper_level FROM {table_name_fast} ORDER BY timestamp DESC LIMIT 1")
            hopper_result = cursor.fetchone()
            if hopper_result:
                # Map single hopper_level to hopper_left_level for unified response
                hopper_left = hopper_result[0] if hopper_result[0] is not None else 0
                hopper_right = 0  # Single hopper machines don't have right hopper

        # Build response object
        response = {
            "machine_id": machine_id,
            "vertical_sealing": {
                "front_temp": vertical_front_avg,
                "rear_temp": vertical_rear_avg
            },
            "horizontal_sealing": {
                "front_temp": horizontal_front or 0,
                "rear_temp": horizontal_rear or 0
            },
            "hopper_levels": {
                "hopper_left_level": hopper_left,
                "hopper_right_level": hopper_right
            }
        }

        # Add laminate wastage ONLY for single hopper machines (MC27, MC28, MC29, MC30)
        # Exclude dual hopper machines (MC25, MC26)
        if machine_num in [27, 28, 29, 30]:
            laminate_wastage = calculate_laminate_wastage(machine_id, conn, cursor)
            response["laminate_wastage"] = {
                "laminate_length": laminate_wastage["laminate_length"],
                "laminate_duration": laminate_wastage["laminate_duration"]
            }

        return response

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching temperature data for {machine_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching temperature data for {machine_id}")
    finally:
        close_db(conn, cursor)
