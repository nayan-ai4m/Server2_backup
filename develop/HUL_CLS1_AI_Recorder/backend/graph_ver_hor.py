from fastapi import HTTPException
from database import get_db, close_db

def fetch_horizontal_sealer_graph(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        
        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {machine_id}->'hor_front1' AS hor_sealer_front1,
                {machine_id}->'hor_rear1' AS hor_sealer_rear1
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data available in historical_graph")

        historical_data = {
            "time_bucket": [],
            "hor_sealer_front1": [],
            "hor_sealer_rear1": []
        }

        for row in results:
            historical_data["time_bucket"].extend(row[0])  
            historical_data["hor_sealer_front1"].extend(row[1]) 
            historical_data["hor_sealer_rear1"].extend(row[2])  

        # print("Historical Data:", historical_data)
        return historical_data

    except Exception as e:
        print(f"Error fetching historical data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
    finally:
        close_db(conn, cursor)

def fetch_hopper_data(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:
        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {machine_id}->'hopper_1' AS hopper_1,
                {machine_id}->'hopper_2' AS hopper_2
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        if not results:
            raise HTTPException(status_code=404, detail="No hopper data available")

        hopper_data = {
            "time_bucket": [],
            "hopper_1": [],
            "hopper_2": []
        }

        for row in results:
            hopper_data["time_bucket"].extend(row[0])  
            hopper_data["hopper_1"].extend(row[1])  
            hopper_data["hopper_2"].extend(row[2])  

        # print("Hopper Data:", hopper_data)
        return hopper_data

    except Exception as e:
        print(f"Error fetching hopper data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graphs")
    finally:
        close_db(conn, cursor)
        
def fetch_vertical_sealer_full_graph(machine_id):
    conn, cursor = get_db()
    if not conn or not cursor:
        raise HTTPException(status_code=500, detail="Database connection error")

    try:

        front_columns = ", ".join([f"{machine_id}->>'front{i}' AS front{i}" for i in range(1, 14)])
        rear_columns = ", ".join([f"{machine_id}->>'rear{i}' AS rear{i}" for i in range(1, 14)])

        query = f"""
            SELECT 
                {machine_id}->'time_bucket' AS time_bucket,
                {front_columns},
                {rear_columns}
            FROM historical_graphs 
            WHERE {machine_id} IS NOT NULL;
        """

        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data available in historical_graph")
        vertical_data = {
            "time_bucket": [],
            **{f"front{i}": [] for i in range(1, 14)},
            **{f"rear{i}": [] for i in range(1, 14)}
        }

    
        for row in results:
            vertical_data["time_bucket"].append(row[0])  

            for i in range(1, 14):
            
                try:
                    front_values = json.loads(row[i]) if row[i] else []
                    rear_values = json.loads(row[13 + i]) if row[13 + i] else []
                except json.JSONDecodeError:
                    front_values = rear_values = []

        
                vertical_data[f"front{i}"].extend(front_values)
                vertical_data[f"rear{i}"].extend(rear_values)

        # print("Full Vertical Data:", vertical_data)
        return vertical_data

    except Exception as e:
        print(f"Error fetching full vertical data: {e}")
        raise HTTPException(status_code=500, detail="Error fetching data from historical_graph")
    finally:
        close_db(conn, cursor)
