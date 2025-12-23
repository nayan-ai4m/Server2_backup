import json
import psycopg2
from datetime import datetime, timedelta, timezone
import time

# Database connection parameters
DB_HOST = '192.168.1.168'
DB_NAME = 'hul'
DB_USER = 'postgres'
DB_PASSWORD = 'ai4m2024'

# Sleep interval between executions (in seconds)
SLEEP_INTERVAL = 0.5  # 0.5 seconds - match tp_history polling frequency

# Cleanup settings
CLEANUP_INTERVAL = 3600  # Run cleanup once every hour (3600 seconds)
last_cleanup_time = None


def get_table_columns(cursor, table_name):
    """Fetch the list of columns for a given table from the database."""
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s;
    """
    cursor.execute(query, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def cleanup_old_data(cursor):
    """Delete records older than 20 days from overall_tp_history."""
    global last_cleanup_time
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=20)
        delete_query = """
            DELETE FROM overall_tp_history
            WHERE timestamp < %s;
        """
        cursor.execute(delete_query, (cutoff_date,))
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            print(f"[CLEANUP] Deleted {deleted_count} records older than 20 days.")
        else:
            print(f"[CLEANUP] No old records to delete.")
        last_cleanup_time = time.time()
    except Exception as e:
        print(f"[CLEANUP ERROR] {e}")


def main():
    global last_cleanup_time
    while True:
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            conn.autocommit = True
            cur = conn.cursor()

            machines = [17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30]
            all_columns = [
                'tp01','tp02','tp03','tp04','tp05','tp06','tp07','tp08','tp09','tp10',
                'tp11','tp12','tp13','tp14','tp15','tp16','tp17','tp18','tp19','tp20',
                'tp21','tp22','tp23','tp24','tp25','tp26','tp27','tp28','tp29','tp30',
                'tp31','tp32','tp33','tp34','tp35','tp36','tp37','tp38','tp39','tp40',
                'tp41','tp42','tp43','tp44','tp45','tp46','tp47','tp48','tp49','tp50',
                'tp51','tp54','tp55','tp56','tp57','tp58','tp59','tp60','tp61','tp62',
                'tp63','tp64','tp65','tp66','tp67','tp68','tp69','tp70','tp71','tp72',
                'tp73','tp74','tp75','tp76','tp77'
            ]

            for m in machines:
                table = f"mc{m}_tp_status"
                machine = f"MC{m}"

                # Get actual columns for the table
                try:
                    table_columns = get_table_columns(cur, table)
                    valid_columns = [col for col in all_columns if col in table_columns]
                    if not valid_columns:
                        print(f"No valid columns found in {table}. Skipping.")
                        continue

                    select_query = f"SELECT {', '.join(valid_columns)} FROM {table};"
                    cur.execute(select_query)
                    row = cur.fetchone()
                    if row is None:
                        print(f"No data in table {table}. Skipping.")
                        continue

                    for i, col in enumerate(valid_columns):
                        json_data = row[i]
                        if json_data is None:   
                            continue

                        try:
                            if isinstance(json_data, str):
                                data = json.loads(json_data)
                            elif isinstance(json_data, dict):
                                data = json_data
                            else:
                                print(f"Invalid data type in {table}.{col}: {type(json_data)}")
                                continue

                            current_ts_str = data.get('timestamp')
                            if not current_ts_str:
                                continue

                            try:
                                current_ts = datetime.fromisoformat(current_ts_str.replace('Z', '+00:00'))
                            except ValueError as e:
                                # Handle possible formats like with 'Z' or without
                                try:
                                    current_ts = datetime.fromisoformat(current_ts_str)
                                except:
                                    print(f"Invalid timestamp in {table}.{col}: {current_ts_str}")
                                    continue

                            filepath = data.get('filepath', '')  # Default to empty string if not present
                            uuid = data.get('uuid', '')  # Get UUID from source data

                            # Check if this exact record already exists
                            hist_query = """
                                SELECT COUNT(*)
                                FROM overall_tp_history
                                WHERE machine = %s
                                  AND tp_column = %s
                                  AND timestamp = %s
                                  AND uuid = %s
                                  AND filepath = %s;
                            """
                            cur.execute(hist_query, (machine, col, current_ts, uuid, filepath))
                            exists = cur.fetchone()[0] > 0

                            should_insert = not exists

                            if should_insert:
                                insert_query = """
                                    INSERT INTO overall_tp_history (timestamp, machine, tp_column, filepath, uuid)
                                    VALUES (%s, %s, %s, %s, %s);
                                """
                                cur.execute(insert_query, (current_ts, machine, col, filepath, uuid))
                                print(f"Inserted: {machine}.{col} @ {current_ts} [filepath={filepath}, uuid={uuid}]")

                        except Exception as e:
                            print(f"Error processing {table}.{col}: {e}")
                            continue

                except Exception as e:
                    print(f"Error with table {table}: {e}")
                    continue

            # === RUN CLEANUP ONCE PER HOUR ===
            current_time = time.time()
            if (last_cleanup_time is None or 
                (current_time - last_cleanup_time) >= CLEANUP_INTERVAL):
                cleanup_old_data(cur)

        except Exception as e:
            print(f"Database connection or execution error: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        print(f"Sleeping for {SLEEP_INTERVAL} seconds... (Next run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    print(f"Starting TP History Sync Daemon @ {datetime.now()}")
    print("Keeping only last 20 days of data. Cleanup runs hourly.")
    main()
