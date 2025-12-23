import json
import psycopg2
from datetime import datetime
import time

# Database connection parameters - replace with your actual details
DB_HOST = '192.168.1.168'  # or your Postgres host
DB_NAME = 'hul'
DB_USER = 'postgres'
DB_PASSWORD = 'ai4m2024'

# Sleep interval between executions (in seconds, e.g., 300 = 5 minutes)
SLEEP_INTERVAL = 300

def main():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cur = conn.cursor()

            machines = [17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30]
            columns = ['tp17', 'tp18', 'tp19', 'tp20', 'tp21', 'tp22','tp15','tp24','tp31']

            for m in machines:
                table = f"mc{m}_tp_status"
                machine = f"MC{m}"

                select_query = f"SELECT {', '.join(columns)} FROM {table};"
                try:
                    cur.execute(select_query)
                    row = cur.fetchone()

                    if row is None:
                        print(f"No data in table {table}. Skipping.")
                        continue

                    for i, col in enumerate(columns):
                        json_data = row[i]
                        if json_data is None:
                            print(f"No data in {table}.{col}. Skipping.")
                            continue

                        try:
                            if isinstance(json_data, str):
                                data = json.loads(json_data)
                            elif isinstance(json_data, dict):
                                data = json_data
                            else:
                                print(f"Invalid data type in {table}.{col}: {type(json_data)}. Expected str or dict.")
                                continue

                            current_ts_str = data.get('timestamp')
                            if not current_ts_str:
                                print(f"No timestamp in {table}.{col}. Skipping.")
                                continue

                            try:
                                current_ts = datetime.fromisoformat(current_ts_str)
                            except ValueError as e:
                                print(f"Invalid timestamp format in {table}.{col}: {current_ts_str}. Error: {e}")
                                continue

                            filepath = data.get('filepath')
                            if not filepath:
                                print(f"No filepath in {table}.{col}. Skipping.")
                                continue

                            hist_query = """
                                SELECT timestamp
                                FROM baumer_tp_history
                                WHERE machine = %s AND tp_column = %s
                                ORDER BY timestamp DESC
                                LIMIT 1;
                            """
                            cur.execute(hist_query, (machine, col))
                            last_row = cur.fetchone()
                            last_ts = last_row[0] if last_row else None

                            if last_ts is None or current_ts != last_ts:
                                insert_query = """
                                    INSERT INTO baumer_tp_history (timestamp, machine, tp_column, filepath)
                                    VALUES (%s, %s, %s, %s);
                                """
                                cur.execute(insert_query, (current_ts, machine, col, filepath))
                                print(f"Inserted history for {machine}.{col} at {datetime.now()}")

                        except Exception as e:
                            print(f"Error processing {table}.{col}: {e}")
                            continue

                except Exception as e:
                    print(f"Error querying table {table}: {e}")
                    continue

            conn.commit()

        except Exception as e:
            print(f"Database connection or execution error: {e}")

        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

        print(f"Sleeping for {SLEEP_INTERVAL} seconds...")
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    main()
