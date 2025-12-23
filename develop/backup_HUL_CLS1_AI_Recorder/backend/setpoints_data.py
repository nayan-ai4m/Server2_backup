from fastapi import HTTPException
from database import get_db, close_db

def fetch_machine_setpoints_data(machine_id):
    # machine_columns = [f"mc{i}" for i in range(17, 31) if i not in (23, 24)]
    # column_names = ", ".join(machine_columns)  # Join column names for the query
    
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    machine_data = {}

    try:
        
        cursor.execute(f"SELECT {machine_id} FROM cls1_setpoints LIMIT 1")
        result = cursor.fetchone()
        
        if result: 
            machine_data = result[0] if result[0] is not None else "No data available"
        else:
            raise HTTPException(status_code=404, detail="No data available in cls1_setpoints")
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
    finally:
        close_db(conn, cursor)

    return machine_data
    
def fetch_recent_machine_setpoints(machine_id):
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
        print(f"Error fetching data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from cls1_setpoints")
    finally:
        close_db(conn, cursor)

    return last_5_values

    
    