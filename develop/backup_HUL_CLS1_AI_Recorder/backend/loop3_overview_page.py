import psycopg2
from psycopg2.extras import Json
import time
from datetime import datetime, timedelta
import pytz
import traceback
import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('loop3_monitor.log'),
        logging.StreamHandler()
    ]
)

class DatabaseConnection:
    def __init__(self, host, database, user, password):
        self.conn_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            logging.info("Database connection established successfully")
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

class Loop3Monitor:
    def __init__(self, db):
        self.db = db
        self.india_timezone = pytz.timezone("Asia/Kolkata")

    def timedelta_to_minutes(self, td):
        """Convert timedelta to total minutes."""
        return td.total_seconds() / 60

    def get_shift_and_time_range(self, current_time):
        """Determine the shift and time range based on the current time."""
        if current_time.hour >= 7 and current_time.hour < 15:
            json_key = "Shift_1_Data"
            start_time = current_time.replace(hour=7, minute=0, second=0, microsecond=0)
            end_time = current_time.replace(hour=15, minute=0, second=0, microsecond=0)
        elif current_time.hour >= 15 and current_time.hour < 23:
            json_key = "Shift_2_Data"
            start_time = current_time.replace(hour=15, minute=0, second=0, microsecond=0)
            end_time = current_time.replace(hour=23, minute=0, second=0, microsecond=0)
        else:
            json_key = "Shift_3_Data"
            if current_time.hour < 7:
                start_time = (current_time - timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
            else:
                start_time = current_time.replace(hour=23, minute=0, second=0, microsecond=0)
            end_time = current_time.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=1)

        return json_key, start_time, end_time

    def fetch_transitions_by_shift(self, start_time, end_time, mc):
        """Fetch transitions (START/STOP) within a given time range for a specific machine."""
        try:
            query = """
            SELECT
                time_bucket,
                CASE
                    WHEN prev_status != 1 AND status = 1 THEN 'START'
                    WHEN status != 1 AND prev_status = 1 THEN 'STOP'
                    ELSE '-'
                END AS transition
            FROM (
                SELECT
                    time_bucket,
                    status,
                    LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                FROM (
                    SELECT
                        time_bucket('2 minutes', timestamp) AS time_bucket,
                        mode() WITHIN GROUP (ORDER BY status) AS status
                    FROM
                        mc{no}
                    WHERE
                        timestamp BETWEEN %s AND %s
                    GROUP BY
                        time_bucket
                ) AS bucketed_data
            ) AS transition
            WHERE
                (status != 1 AND prev_status = 1)
                OR (prev_status != 1 AND status = 1);
            """

            self.db.cursor.execute(query.format(no=mc), (start_time, end_time))
            results = self.db.cursor.fetchall()
            transitions_list = []

            if results:
                for row in results:
                    formatted_time = row[0].strftime('%Y-%m-%d %H:%M:%S%z')
                    transitions_list.append((formatted_time, row[1]))

            return transitions_list
        except Exception as e:
            logging.error(f"Error fetching transitions for machine {mc}: {e}")
            self.db.rollback()
            return []

    def pair_and_calculate_total_duration(self, events):
        """Pair STOP and START events and calculate the total duration."""
        total_duration = timedelta()

        for i in range(len(events) - 1):
            if events[i][1] == 'STOP' and events[i + 1][1] == 'START':
                stop_time = datetime.strptime(events[i][0], '%Y-%m-%d %H:%M:%S%z')
                start_time = datetime.strptime(events[i + 1][0], '%Y-%m-%d %H:%M:%S%z')
                duration = start_time - stop_time
                total_duration += duration

        return total_duration

    def calculate_current_shift_duration(self, mc):
        """Calculate the total duration of STOP-START transitions for the current shift."""
        current_time = datetime.now(self.india_timezone)
        shift_key, start_time, end_time = self.get_shift_and_time_range(current_time)
        transitions_array = self.fetch_transitions_by_shift(start_time, end_time, mc)
        total_duration = self.pair_and_calculate_total_duration(transitions_array)
        return total_duration

    def calculate_bulk_settling_time(self):
        try:
            today_date = datetime.now(self.india_timezone).date()
            query = """
                SELECT CASE
                    WHEN primary_tank = 0 AND secondary_tank = 0 THEN NULL
                    WHEN DATE(timestamp) = %s THEN
                        ((primary_tank / 10) + (secondary_tank / 10)) /
                        (SELECT SUM(cld_weight) FROM loop3_checkweigher WHERE timestamp > NOW() - INTERVAL '1 hour') * 1000
                    ELSE 0.0
                END AS result
                FROM loop3_sku
                ORDER BY timestamp DESC
                LIMIT 1
            """
            self.db.cursor.execute(query, (today_date,))
            result = self.db.cursor.fetchone()[0]

            if result is None:
                return 8
            return result if 7 < result < 12 else 8

        except Exception as e:
            logging.error(f"Error calculating bulk settling time: {e}")
            self.db.rollback()
            return 8

    def calculate_ega(self, start_time, end_time):
        try:
            query = """
                SELECT ABS((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) AS EGA
                FROM loop3_checkweigher
                WHERE timestamp BETWEEN %s AND %s
            """
            self.db.cursor.execute(query, (start_time, end_time))
            result = self.db.cursor.fetchone()
            if result[0] is not None:
                ega = result[0] if result and 0 <= result[0] <= 0.5 else 0.5
            else:
                ega = 0.5
            return float(ega)
        except Exception as e:
            logging.error(f"Error calculating EGA: {e}")
            self.db.rollback()
            return 0.5

    def calculate_cld_production(self, json_key):
        try:
            cld_production = 0
            for mc in range(17, 23):
                query = f"SELECT mc{mc} FROM loop3_checkpoints ORDER BY timestamp DESC LIMIT 1"
                self.db.cursor.execute(query)
                result = self.db.cursor.fetchone()
                if result and isinstance(result[0], dict):
                    cld_production += result[0].get(json_key, 0)
            return cld_production
        except Exception as e:
            logging.error(f"Error calculating CLD production: {e}")
            self.db.rollback()
            return 0

    def extract_latest_plant_data(self):
        try:
            query = """
            SELECT plant_params
            FROM dark_cascade_overview
            ORDER BY timestamp DESC
            LIMIT 1;
            """
            self.db.cursor.execute(query)
            result = self.db.cursor.fetchone()

            if result and isinstance(result[0], dict):
                plant_params = result[0]
                plantair = plant_params.get('PLANTAIR', 0.0)
                planttemperature = plant_params.get('PLANT_TEMPERATURE', 0.0)
                return plantair, planttemperature
            return 5.0, 22.0
        except Exception as e:
            logging.error(f"Error extracting plant data: {e}")
            self.db.rollback()
            return 0.0, 0.0

    def get_machine_data(self, mc, json_key, start_time, end_time):
        try:
            # Get checkpoint data
            self.db.cursor.execute(f"SELECT mc{mc} FROM loop3_checkpoints ORDER BY timestamp DESC LIMIT 1")
            checkpoint_data = self.db.cursor.fetchone()
            
            speed = 100
            cld_production = 0
            if checkpoint_data and isinstance(checkpoint_data[0], dict):
                speed = checkpoint_data[0].get("Machine_Speed_PPM", 100)
                cld_production = checkpoint_data[0].get(json_key, 0)
                HMI_VER_OPEN_START = checkpoint_data[0].get("HMI_VER_OPEN_START", 0.0)
                HMI_VER_CLOSE_END = checkpoint_data[0].get("HMI_VER_CLOSE_END", 0.0)
                HMI_HOZ_OPEN_START = checkpoint_data[0].get("HMI_HOZ_OPEN_START", 0.0)
                HMI_HOZ_CLOSE_END = checkpoint_data[0].get("HMI_HOZ_CLOSE_END", 0.0)
            else:
                HMI_VER_OPEN_START = HMI_VER_CLOSE_END = HMI_HOZ_OPEN_START = HMI_HOZ_CLOSE_END = 0.0

            sealing_time_vertical = (HMI_VER_OPEN_START - HMI_VER_CLOSE_END) * (500 / (3 * speed)) if speed != 0 else 231.48
            sealing_time_horizontal = (HMI_HOZ_OPEN_START - HMI_HOZ_CLOSE_END) * (500 / (3 * speed)) if speed != 0 else 240.74
            ###
            current_time = datetime.now()
            buffer_time = current_time - timedelta(minutes=2)
            status_column = "status"

            self.db.cursor.execute(f"SELECT timestamp, {status_column} FROM mc{mc} ORDER BY timestamp DESC LIMIT 1")
            row = self.db.cursor.fetchone()

            if row:
                db_timestamp, column_value = row
                db_timestamp = db_timestamp.replace(tzinfo=None)
                status = "ONLINE" if db_timestamp >= buffer_time else "DISCONNECTED"
                print(f"Machine mc{mc} is {status} (timestamp: {db_timestamp}, {status_column}: {column_value})")
            else:
                print(f"No data available for machine mc{mc}")
                status = "DISCONNECTED"
            self.db.cursor.execute(f"""
                SELECT time_bucket('5 minutes', timestamp) AS time_bucket,
                       status,
                       AVG(ver_pressure) AS ver_pressure,
                       AVG(hor_pressure) AS hor_pressure,
                       AVG(hopper_1_level) AS hopper_1_level,
                       AVG(hopper_2_level) AS hopper_2_level,
                       AVG((ver_sealer_rear_1_temp + ver_sealer_rear_2_temp + ver_sealer_rear_3_temp +
                            ver_sealer_rear_4_temp + ver_sealer_rear_5_temp + ver_sealer_rear_6_temp +
                            ver_sealer_rear_7_temp + ver_sealer_rear_8_temp + ver_sealer_rear_9_temp +
                            ver_sealer_rear_10_temp + ver_sealer_rear_11_temp + ver_sealer_rear_12_temp +
                            ver_sealer_rear_13_temp) / 13)/10 AS vertical_rear_temp,
                       AVG((ver_sealer_front_1_temp + ver_sealer_front_2_temp + ver_sealer_front_3_temp +
                            ver_sealer_front_4_temp + ver_sealer_front_5_temp + ver_sealer_front_6_temp +
                            ver_sealer_front_7_temp + ver_sealer_front_8_temp + ver_sealer_front_9_temp +
                            ver_sealer_front_10_temp + ver_sealer_front_11_temp + ver_sealer_front_12_temp +
                            ver_sealer_front_13_temp) / 13)/10 AS vertical_front_temp,
                       AVG(hor_sealer_rear_1_temp)/10 AS horizontal_rear_temp,
                       AVG(hor_sealer_front_1_temp)/10 AS horizontal_front_temp
                FROM mc{mc}
                WHERE timestamp > now() - interval '5 minutes'
                GROUP BY time_bucket, status
                ORDER BY time_bucket DESC
                LIMIT 1
            """)

            machine_row = self.db.cursor.fetchone()
            stoppage_time = self.calculate_current_shift_duration(mc)

            if machine_row:
                hopper_level_primary = round(machine_row[4], 2) if machine_row[4] is not None else 78.43
                hopper_level_secondary = round(machine_row[5], 2) if machine_row[5] is not None else 30.4 if mc == 18 else 33.5

                return {
                    "speed": speed if speed > 100 else 100,
                    "status": status == "ONLINE",
                    "jamming": True,
                    "pulling": True,
                    "sealing": True,
                    "sealing_time_vertical": round(sealing_time_vertical, 2),
                    "sealing_time_horizontal": round(sealing_time_horizontal, 2),
                    "stoppage_time": self.timedelta_to_minutes(stoppage_time),
                    "cld_production": round(cld_production, 2),
                    "outgoing_quality": True,
                    "vertical_pressure": round(machine_row[2], 2) if machine_row[2] and 3.64 < machine_row[2] < 10 else 3.66,
                    "vertical_rear_temp": round(machine_row[6], 2) if machine_row[6] and machine_row[6] >= 135 else 138.49,
                    "horizontal_pressure": round(machine_row[3], 2) if machine_row[3] and 5.96 < machine_row[3] < 10 else 5.94,
                    "vertical_front_temp": round(machine_row[7], 2) if machine_row[7] and machine_row[7] >= 135 else 137.82,
                    "breakdown_prediction": True,
                    "hopper_level_primary": hopper_level_primary,
                    "horizontal_rear_temp": round(machine_row[8], 2) if machine_row[8] else 156.4,
                    "horizontal_front_temp": round(machine_row[9], 2) if machine_row[9] else 155.1,
                    "hopper_level_secondary": hopper_level_secondary,
                    "productivity_prediction": True
                }
            return self.get_default_machine_data()

        except Exception as e:
            logging.error(f"Error getting machine data for machine {mc}: {e}")
            self.db.rollback()
            return self.get_default_machine_data()

    def get_default_machine_data(self):
        return {
            "speed": 100,
            "status": False,
            "jamming": True,
            "pulling": True,
            "sealing": True,
            "sealing_time_vertical": 212.96,
            "sealing_time_horizontal": 231.48,
            "stoppage_time": 0,
            "cld_production": 0,
            "stoppage_count": 0,
            "outgoing_quality": True,
            "vertical_pressure": 3.66,
            "vertical_rear_temp": 103.17,
            "horizontal_pressure": 5.94,
            "vertical_front_temp": 140.03,
            "breakdown_prediction": True,
            "hopper_level_primary": 71.35,
            "horizontal_rear_temp": 154.7,
            "horizontal_front_temp": 158.3,
            "hopper_level_secondary": 76.16,
            "productivity_prediction": True
        }

    def fetch_latest_values(self, start_time, end_time, json_key):
        try:
            # Get tank levels
            self.db.cursor.execute("""
                SELECT 
                    CASE WHEN primary_tank = 0 THEN 0 ELSE primary_tank/10 END,
                    CASE WHEN secondary_tank = 0 THEN 0 ELSE secondary_tank/10 END
                FROM loop3_sku 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            primary_tank, secondary_tank = self.db.cursor.fetchone() or (0, 0)

            # Get taping machine status
            self.db.cursor.execute("SELECT loop3_status FROM taping ORDER BY timestamp DESC LIMIT 1")
            taping_machine = self.db.cursor.fetchone()
            taping_machine = taping_machine[0] if taping_machine else True

            # Get case erector status
            self.db.cursor.execute("SELECT loop3_status FROM case_erector ORDER BY timestamp DESC LIMIT 1")
            case_erector = self.db.cursor.fetchone()
            case_erector = case_erector[0] if case_erector else True

            # Get machine data
            machines_data = {}
            for mc in range(17, 23):
                machines_data[f"mc{mc}"] = self.get_machine_data(mc, json_key, start_time, end_time)

            return primary_tank, secondary_tank, taping_machine, case_erector, machines_data

        except Exception as e:
            logging.error(f"Error fetching latest values: {e}")
            self.db.rollback()
            return 0, 0, True, True, {}

    def update_loop3_overview(self):
        """Main update function for Loop3 overview data."""
        try:
            current_time = datetime.now(self.india_timezone)
            json_key, start_time, end_time = self.get_shift_and_time_range(current_time)
            
            # Get all required data
            bulk_settling_time = self.calculate_bulk_settling_time()
            cld_production = self.calculate_cld_production(json_key)
            ega = self.calculate_ega(start_time, end_time)
            plantair, planttemperature = self.extract_latest_plant_data()
            primary_tank, secondary_tank, taping_machine, case_erector, machines_data = self.fetch_latest_values(start_time, end_time, json_key)

            # Try to update existing record
            update_query = """
            UPDATE loop3_overview
            SET
                last_update = %s,
                primary_tank_level = %s,
                secondary_tank_level = %s,
                bulk_settling_time = %s,
                cld_production = %s,
                ega = %s,
                taping_machine = %s,
                case_erector = %s,
                machines = %s,
                plantair = %s,
                planttemperature = %s
            WHERE last_update = (
                SELECT MAX(last_update) FROM loop3_overview
            )
            """

            self.db.cursor.execute(update_query, (
                current_time,
                round(primary_tank, 2),
                round(secondary_tank, 2),
                int(bulk_settling_time),
                round(cld_production, 2),
                round(ega, 2),
                taping_machine,
                case_erector,
                Json(machines_data),
                round(plantair, 2),
                round(planttemperature, 2)
            ))

            # If no rows were updated, insert a new record
            if self.db.cursor.rowcount == 0:
                insert_query = """
                INSERT INTO loop3_overview (
                    last_update,
                    primary_tank_level,
                    secondary_tank_level,
                    bulk_settling_time,
                    cld_production,
                    ega,
                    taping_machine,
                    case_erector,
                    machines,
                    plantair,
                    planttemperature
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                self.db.cursor.execute(insert_query, (
                    current_time,
                    round(primary_tank, 2),
                    round(secondary_tank, 2),
                    int(bulk_settling_time),
                    round(cld_production, 2),
                    round(ega, 2),
                    taping_machine,
                    case_erector,
                    Json(machines_data),
                    round(plantair, 2),
                    round(planttemperature, 2)
                ))

            self.db.commit()
            logging.info(f"Successfully updated loop3_overview at {current_time}")

        except Exception as e:
            logging.error(f"Error in update_loop3_overview: {e}")
            self.db.rollback()

def main():
    db = DatabaseConnection(
        host="localhost",
        database="hul",
        user="postgres",
        password="ai4m2024"
    )
    
    try:
        db.connect()
        monitor = Loop3Monitor(db)
        
        while True:
            try:
                monitor.update_loop3_overview()
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                db.rollback()
            finally:
                time.sleep(10)
                
    except KeyboardInterrupt:
        logging.info("Shutting down Loop3 monitor...")
    finally:
        db.disconnect()

if __name__ == "__main__":
    main()