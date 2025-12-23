from database import get_db, close_db
from fastapi import HTTPException
import json
import traceback


def fetch_machine_data():
    query = """
    SELECT 
        last_update,
        machines
    FROM {table_name}
    ORDER BY last_update DESC
    LIMIT 1
    """
    
    data = {}
    for table_name in ["loop3_overview", "loop4_overview"]:
        conn, cursor = get_db()
        if not conn or not cursor:
            raise HTTPException(status_code=500, detail="Database connection error")
        
        try:
            cursor.execute(query.format(table_name=table_name))
            result = cursor.fetchone()
            
            if result:
                last_update, machines_json = result
                
                if isinstance(machines_json, str):
                    machines_data = json.loads(machines_json)
                elif isinstance(machines_json, dict):
                    machines_data = machines_json
                else:
                    raise TypeError("machines_json is neither a string nor a dictionary")
                data[table_name] = {
                    "last_update": last_update,
                    "machines": machines_data
                }
                
        except Exception as e:
            print(f"Error fetching data from {table_name}: {e}")
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error fetching data from {table_name}")
        finally:
            close_db(conn, cursor)
    
    return data