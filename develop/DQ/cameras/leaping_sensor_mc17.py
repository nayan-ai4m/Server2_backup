import psycopg2
from datetime import datetime
from uuid import uuid4
import json
import time

class RollEndSensor_TP:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            dbname="hul",
            user="postgres",
            password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        self.fetch_mq_query = "SELECT status FROM mc17 ORDER BY timestamp DESC LIMIT 1;"
        self.fetch_ls_query = '''
	        SELECT 
                CASE 
                    WHEN AVG(
                        CASE 
                            WHEN leaping_value = 'true' THEN 1
                            WHEN leaping_value = 'false' THEN 0
                            ELSE NULL 
                        END
                    ) > 0.5 THEN 1
                    ELSE 0
                END AS leaping_result
            FROM (
                SELECT mc17 ->> 'LEAPING_SENSOR' AS leaping_value
                FROM loop3_checkpoints
                WHERE mc17 ->> 'LEAPING_SENSOR' IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 10
            ) subquery;
        '''
        self.check_tp_exists_query = "SELECT tp51 FROM mc17_tp_status;"
        self.update_tp_query = f"UPDATE mc17_tp_status SET tp51 = %s;"
        self.insert_tp_query = "INSERT INTO mc17_tp_status (tp51) VALUES (%s);"

        self.insert_event_query = """INSERT INTO public.event_table(timestamp, event_id, zone, camera_id, event_type, alert_type) VALUES (%s, %s, %s, %s, %s, %s);"""
        
        self.ensure_tp51_column_exists()

    def ensure_tp51_column_exists(self):
        """Checks if the column tp51 exists in mc17_tp_status and adds it if not."""
        try:
            self.cursor.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='mc17_tp_status' AND column_name='tp51';
            """)
            column_exists = self.cursor.fetchone()
            if not column_exists:
                print("Column 'tp51' does not exist. Creating it...")
                self.cursor.execute("ALTER TABLE mc17_tp_status ADD COLUMN tp51 JSONB;")
                self.conn.commit()
                print("Column 'tp51' created successfully.")
        except Exception as e:
            print(f"Error checking/creating column 'tp51': {e}")
            self.conn.rollback()

    @staticmethod   
    def format_event_data(is_problem):
        timestamp = datetime.now().isoformat()
        return {
            "uuid": str(uuid4()),
            "active": 1 if is_problem else 0,
            "timestamp": timestamp,
            "color_code": 3 if is_problem else 1
        }
    
    def run(self):
        previous_is_problem = None  

        while True:
            try:
                self.cursor.execute(self.fetch_mq_query)
                mq_row = self.cursor.fetchone()
                #print(mq_row[0])
                mq = int(mq_row[0]) if mq_row else None

                self.cursor.execute(self.fetch_ls_query)
                ls_row = self.cursor.fetchone()
                print(ls_row)
                if ls_row and ls_row[0] is not None:
                    ls = int(ls_row[0])  # Already 0 or 1 from the SQL query
                else:
                    ls = None

                print(f"mq={mq}, ls={ls}")

                # tp_name = "tp51"

                if mq == 1 and ls == 0:
                    is_problem = True
                elif mq == 1 and ls == 1:
                    is_problem = False
                else:
                    is_problem = False #None

                # Only update if is_problem changes
                if is_problem is not None and is_problem != previous_is_problem:
                    event_data = self.format_event_data(is_problem=is_problem)

                    if is_problem == True:
                        data = (
                            datetime.now(), str(uuid4()), "PLC", 
                            "MC17", "Leaping Detected", "Quality"
                        )
                        print(data)
                        self.cursor.execute(self.insert_event_query, data)
                        self.conn.commit()

                     # Check if tp<n> exists
                    self.cursor.execute(self.check_tp_exists_query)
                    exists = self.cursor.fetchone()

                    if exists:
                        print(f"Updating tp51 with event data: {event_data}")
                        self.cursor.execute(self.update_tp_query, (json.dumps(event_data),))
                    else:
                        print(f"Inserting new tp51 with event data: {event_data}")
                        self.cursor.execute(self.insert_tp_query, (json.dumps(event_data),))

                    self.conn.commit()

                    previous_is_problem = is_problem

                time.sleep(5)

            except psycopg2.Error as db_err:
                print(f"Database error: {db_err}")
                self.conn.rollback()
                time.sleep(5)
            except Exception as ex:
                print(f"Unexpected error: {ex}")
                time.sleep(5)


    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

if __name__ == '__main__':
    RollEndSensor_TP().run()
