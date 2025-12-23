import psycopg2
from psycopg2.extras import Json
import time
from datetime import datetime, timedelta,date
import pytz
import traceback
import os
import sys
import logging
import threading
import json

today_date = date.today()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('loop3_monitor.log'),
        logging.StreamHandler()
    ]
)

def load_config():
    try:
        with open('loop_config.json') as config_file:
            config = json.load(config_file)
            return config
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        raise

class DatabaseConnection:
    def __init__(self, config, db_type = "read"):
        self.conn_params =  config['database'][db_type]
        self.conn = None
        self.cursor = None
        self.connect_db()

    def connect_db(self):
        while True:
            try:
                self.close_db_connection()
                self.conn = psycopg2.connect(**self.conn_params)
                self.cursor = self.conn.cursor()
                print("Successfully connected to PostgreSQL database")
                return True
            except Exception as e:
                logging.error(f"Database connection failed: {e}")
                os._exit(1)

    def close_db_connection(self):
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")
            os._exit(1)
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")
            os._exit(1)
        self.cursor = None
        self.conn = None

    def ensure_db_connection(self):
        """Lightweight check to ensure we have a valid database connection."""
        while True:

            if self.conn is None or self.conn.closed != 0 or self.cursor is None or self.cursor.closed:
                print("Database connection lost. Reconnecting...")
                return self.connect_db()
            return True

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()


class Loop3Monitor:
    def __init__(self, db,write_db,config):
        self.db = db
        self.write_db = write_db
        self.cursor = db.cursor
        self.india_timezone = pytz.timezone("Asia/Kolkata")
        self.bboee = [81.0,83.2,84.0,82.3,89.6,82.2]
        
        self.UPDATE_LOOP3_QUERY = """
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
                planttemperature = %s,
                batch_sku = %s,
                shift = %s ,
                cobot_status = 'Offline',
                checkweigher = %s
            WHERE last_update = (
                SELECT MAX(last_update) FROM loop3_overview
            )
        """

        self.INSERT_LOOP3_QUERY = """
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
                planttemperature,
                batch_sku,
                shift,
                cobot_status,
                checkweigher 
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'Offline',%s)
        """

        self.EGA_QUERY = """
            with query1 as (select timestamp,cld_weight,target_weight,lower_limit,upper_limit,
                CASE 
                when cld_weight > (target_weight - lower_limit) and cld_weight < (target_weight + upper_limit) then 'ok'
                else 'notok'
                end as status
                FROM loop3_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                order by timestamp desc )
                SELECT 
                    CASE 
                WHEN COUNT(*) > 0 THEN 
                    LEAST(50, GREATEST(-50,
                    ((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) * 100
                 ))
                ELSE 0
                END AS EGA
                FROM query1
                where status = 'ok'
        """

        self.BULK_SETTLING_TIME_QUERY = """
            WITH query1 AS (
                SELECT timestamp, cld_weight,
                    CASE 
                        WHEN cld_weight > (target_weight - lower_limit) 
                        AND cld_weight < (target_weight + upper_limit) THEN 'ok'
                        ELSE 'notok'
                    END AS status
                FROM loop3_checkweigher
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            ),
            sum_ok_weight AS (
                SELECT SUM(cld_weight) AS total_weight, COUNT(*) AS count_all
                FROM query1
                WHERE status = 'ok'
            ),
            cld_zero_check AS (
                SELECT COUNT(*) = 0 AS is_zero
                FROM query1
            )
            SELECT 
                CASE
                    WHEN primary_tank = 0 AND secondary_tank = 0 THEN NULL
                    WHEN is_zero THEN NULL 
                    ELSE (
                        ((primary_tank / 10.0) + (secondary_tank / 10.0)) / 
                        GREATEST(s.total_weight, 1)
                    ) * 1000 -- Removed the LEAST function that was capping at 12.0
                END AS result
            FROM loop3_sku
            CROSS JOIN sum_ok_weight s
            CROSS JOIN cld_zero_check c
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        self.CLD_PRODUCTION_QUERY_TEMPLATE = """
            SELECT mc{mc}, timestamp
            FROM loop3_checkpoints
            WHERE mc{mc} != 'null'
            AND timestamp >= now() - interval '15 minutes'
            ORDER BY timestamp DESC
            LIMIT 1;
        """

        self.QUERY_PRIMARY_SECONDARY_TANK = """
            SELECT 
                CASE WHEN primary_tank = 0 THEN 0 ELSE primary_tank/10 END,
                CASE WHEN secondary_tank = 0 THEN 0 ELSE secondary_tank/10 END
            FROM loop3_sku 
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC 
            LIMIT 1
        """

        self.QUERY_TAPING_MACHINE = """
            SELECT loop3_status 
            FROM taping 
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC 
            LIMIT 1
        """

        self.QUERY_CASE_ERECTOR = """
            SELECT loop3_status 
            FROM case_erector 
            WHERE DATE(timestamp) = %s
            ORDER BY timestamp DESC 
            LIMIT 1
        """

        self.QUERY_EXTRACT_LATEST_PLANT_DATA = """
            SELECT plant_params, timestamp
            FROM dark_cascade_overview
            WHERE DATE(timestamp) >= current_date
            ORDER BY timestamp DESC
            LIMIT 1;
        """


        self.GET_MACHINE_STATUS_QUERY = """
            SELECT status, timestamp 
            FROM mc{mc}
            WHERE timestamp >= now() - interval '5 minutes'
            ORDER BY timestamp DESC
            LIMIT 1;
        """

        self.GET_checkweigher_STATUS_QUERY = """
            SELECT loop3 
            FROM checkweigher_status 
            WHERE timestamp::date = CURRENT_DATE;
        """

        self.GET_CHECKPOINT_QUERY = """
            SELECT mc{mc}, timestamp
            FROM loop3_checkpoints
            WHERE mc{mc} != 'null'
            AND timestamp >= now() - interval '15 minutes'
            ORDER BY timestamp DESC
            LIMIT 1;
        """

        self.GET_MACHINE_DATA_QUERY = """
            SELECT time_bucket('5 minutes', timestamp) AS time_bucket,
                status,
                EXTRACT(EPOCH FROM timestamp) AS epoch_timestamp,
                AVG(ver_pressure) AS ver_pressure,
                AVG(hor_pressure) AS hor_pressure,
                LAST(hopper_1_level, timestamp) AS hopper_1_level,  -- Gets last value in window
                LAST(hopper_2_level, timestamp) AS hopper_2_level,
                AVG((ver_sealer_rear_1_temp + ver_sealer_rear_2_temp + ver_sealer_rear_3_temp +
                        ver_sealer_rear_4_temp + ver_sealer_rear_5_temp + ver_sealer_rear_6_temp +
                        ver_sealer_rear_7_temp + ver_sealer_rear_8_temp + ver_sealer_rear_9_temp +
                        ver_sealer_rear_10_temp + ver_sealer_rear_11_temp + ver_sealer_rear_12_temp +
                        ver_sealer_rear_13_temp) / 13)  AS vertical_rear_temp,
                AVG((ver_sealer_front_1_temp + ver_sealer_front_2_temp + ver_sealer_front_3_temp +
                        ver_sealer_front_4_temp + ver_sealer_front_5_temp + ver_sealer_front_6_temp +
                        ver_sealer_front_7_temp + ver_sealer_front_8_temp + ver_sealer_front_9_temp +
                        ver_sealer_front_10_temp + ver_sealer_front_11_temp + ver_sealer_front_12_temp +
                        ver_sealer_front_13_temp) / 13)  AS vertical_front_temp,
                AVG(hor_sealer_rear_1_temp) AS horizontal_rear_temp,
                AVG(hor_sealer_front_1_temp) AS horizontal_front_temp
            FROM mc{mc}
            WHERE timestamp > now() - interval '5 minutes'
            GROUP BY time_bucket, status, timestamp
            ORDER BY time_bucket DESC
            LIMIT 1;
        """

        self.hopper_level_query = """
            select hopper_1_level,hopper_2_level from mc{mc} where timestamp > now() - interval '5 minutes'
            limit 1;
        """

        self.QUERY_LATEST_ROW = """
            SELECT ver_pressure, hor_pressure, hopper_1_level, hopper_2_level,
                        (ver_sealer_rear_1_temp + ver_sealer_rear_2_temp + ver_sealer_rear_3_temp + 
                            ver_sealer_rear_4_temp + ver_sealer_rear_5_temp + ver_sealer_rear_6_temp +
                            ver_sealer_rear_7_temp + ver_sealer_rear_8_temp + ver_sealer_rear_9_temp +
                            ver_sealer_rear_10_temp + ver_sealer_rear_11_temp + ver_sealer_rear_12_temp +
                            ver_sealer_rear_13_temp) / 13 AS vertical_rear_temp,
                        hor_sealer_rear_1_temp AS horizontal_rear_temp,
                        hor_sealer_front_1_temp AS horizontal_front_temp
                    FROM mc{mc}
                    ORDER BY timestamp DESC
                    LIMIT 1
        """

        self.BATCH_SKU_SHIFT_QUERY = """
            SELECT batch_sku, shift 
            FROM loop3_sku
            WHERE timestamp >= now() - interval '5 minutes'
            ORDER BY timestamp DESC 
            LIMIT 1
        """

        self.GET_MACHINE_UPTIME = """
            SELECT COALESCE(ch.machine_uptime, INTERVAL '00:00:00') 
            FROM (SELECT INTERVAL '00:00:00' AS default_uptime) fallback
            LEFT JOIN cld_history ch 
                ON DATE(ch.timestamp) = CURRENT_DATE 
                AND ch.shift = %s 
                AND ch.machine_no = %s

        """

        self.STOPAGE_COUNT_QUERY = """
            SELECT COUNT(*) 
            FROM machine_stoppages
            WHERE machine = %s
            AND stop_at BETWEEN %s AND %s
        """

        self.GET_STOPPAGE_TIME = """
            SELECT 
                mc{mc} ->> 'stoppage_time' AS stoppage_time,
                mc{mc} ->> 'stoppage_count' AS stoppage_count,
                mc{mc} ->> 'longest_duration' AS longest_duration,
                mc{mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            WHERE shift = %s
            ORDER BY timestamp DESC
            LIMIT 2;
            """
        self.GET_PREV_STOPPAGE_TIME = """
            SELECT 
                mc{mc} ->> 'stoppage_time' AS stoppage_time,
                mc{mc} ->> 'stoppage_count' AS stoppage_count,
                mc{mc} ->> 'longest_duration' AS longest_duration,
                mc{mc} ->> 'highest_count' AS highest_count
            FROM machine_stoppage_status
            ORDER BY timestamp DESC
            LIMIT 2;
        """

    def get_shift_and_time_range(self, current_time):
        
        config = load_config()
        shift_config = config['shifts']
        
        current_hour = current_time.hour
    
        # Check for Shift A (7am-3pm)
        if current_hour >= shift_config["A"]["start_hour"] and current_hour < shift_config["A"]["end_hour"]:
            shift = "A"
            json_key = shift_config["A"]["loop3_name"]
            start_time = current_time.replace(
                hour=shift_config["A"]["start_hour"], 
                minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["A"]["end_hour"], 
                minute=0, second=0, microsecond=0
            )
        
        # Check for Shift B (3pm-11pm)
        elif current_hour >= shift_config["B"]["start_hour"] and current_hour < shift_config["B"]["end_hour"]:
            shift = "B"
            json_key = shift_config["B"]["loop3_name"]
            start_time = current_time.replace(
                hour=shift_config["B"]["start_hour"], 
                minute=0, second=0, microsecond=0
            )
            end_time = current_time.replace(
                hour=shift_config["B"]["end_hour"], 
                minute=0, second=0, microsecond=0
            )
        
        # Otherwise it's Shift C (11pm-7am)
        else:
            shift = "C"
            json_key = shift_config["C"]["loop3_name"]
            
            if current_hour < shift_config["C"]["end_hour"]:  # Before 7am
                start_time = (current_time - timedelta(days=1)).replace(
                    hour=shift_config["C"]["start_hour"], 
                    minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"], 
                    minute=0, second=0, microsecond=0
                )
            else:  # After 11pm
                start_time = current_time.replace(
                    hour=shift_config["C"]["start_hour"], 
                    minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(
                    hour=shift_config["C"]["end_hour"], 
                    minute=0, second=0, microsecond=0
                ) + timedelta(days=1)
        
        return shift, json_key, start_time, end_time

    def calculate_bulk_settling_time(self):
        try:
            today_date = datetime.now(self.india_timezone).date()
            self.db.cursor.execute(self.BULK_SETTLING_TIME_QUERY, (today_date,))
            result = self.db.cursor.fetchone()

            #bulk_settling_time = 0.0 if result is None or result[0] is None else result[0]
            bulk_settling_time = 0 if result is None or result[0] is None else int(result[0])
            bulk_settling_time = max(0, min(bulk_settling_time, 99))

            print("bulk settling time ",bulk_settling_time)
            return bulk_settling_time

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(f"Error in calculate_bulk_settling_time: {exc_type} in {fname} at line {exc_tb.tb_lineno}: {e}")
            os._exit(1)

    def calculate_ega(self):
        try:
            self.db.cursor.execute(self.EGA_QUERY)
            result = self.db.cursor.fetchone()
            
            if result and result[0] is not None:
                return result[0]
            else:
                return 0.0
        except Exception as e:
            logging.error(f"Error calculating EGA: {e}")
            self.db.rollback()
            os._exit(1)

    
    def calculate_cld_production(self, json_key):
        try:
            cld_production = 0
            for mc in range(17, 23):
                query = self.CLD_PRODUCTION_QUERY_TEMPLATE.format(mc=mc)  
                self.db.cursor.execute(query)
                result = self.db.cursor.fetchone()

                if result and isinstance(result[0], dict):
                    cld_production += result[0].get(json_key, 0)
                else :
                    cld_production += 0 
                
            print("\nTotal CLD Production:", cld_production)
            return cld_production

        except Exception as e:
            logging.error(f"Error calculating CLD production: {e}")
            self.db.rollback()
            os._exit(1)

    def extract_latest_plant_data(self):
        try:
            self.db.cursor.execute(self.QUERY_EXTRACT_LATEST_PLANT_DATA)
            result = self.db.cursor.fetchone()
            
            if result:
                plant_params = result[0]
                timestamp = result[1]
                if isinstance(plant_params, dict):
                    plantair = plant_params.get('PLANTAIR', 0.0)
                    planttemperature = plant_params.get('PLANT_TEMPERATURE', 0.0)
                    
                    plantair = plantair if plantair is not None else 0.0
                    planttemperature = planttemperature if planttemperature is not None else 0.0
                    return plantair, planttemperature

            return 0.0, 0.0

        except Exception as e:
            print("error",e)
            logging.error(f"Error extracting plant data: {e}")
            self.db.rollback()
            os._exit(1)


    def fetch_latest_values(self, start_time, end_time, json_key,shift):
        try:
            self.db.cursor.execute(self.QUERY_PRIMARY_SECONDARY_TANK, (today_date,))
            result = self.db.cursor.fetchone()

            if result is None:
                primary_tank, secondary_tank = 0, 0
            else:
                primary_tank = result[0] if result[0] is not None else 0
                secondary_tank = result[1] if result[1] is not None else 0
           
            print("primary tank ", primary_tank)
            print("secondary tank ", secondary_tank)

            self.db.cursor.execute(self.QUERY_TAPING_MACHINE, (today_date,))
            taping_machine = self.db.cursor.fetchone()
            taping_machine = int(taping_machine[0]) if taping_machine and taping_machine[0] is not None else 2
            print("taping machine = ", taping_machine)

           
            self.db.cursor.execute(self.QUERY_CASE_ERECTOR, (today_date,))
            case_erector = self.db.cursor.fetchone()
            case_erector = int(case_erector[0]) if case_erector and case_erector[0] is not None else 2
            print("case erector = ", case_erector)

            self.db.cursor.execute(self.GET_checkweigher_STATUS_QUERY)
            row = self.db.cursor.fetchone()

            if row and row[0] is not None:
                checkweigher = row[0] 
            else:
                checkweigher = 'Offline'

            print(f"Checkweigher Status: {checkweigher}")
           
            machines_data = {}
            for mc in range(17, 23):
                machines_data[f"mc{mc}"] = self.get_machine_data(mc, json_key, start_time, end_time,shift)
                print(machines_data[f"mc{mc}"])

            return primary_tank, secondary_tank, taping_machine, case_erector, machines_data,checkweigher
        
        except Exception as e:
            logging.error(f"Error fetching latest values: {e}")
            self.db.rollback()
            os._exit(1)
            #return 0, 0, 2, 2, {}

    def fetch_batch_sku_and_shift(self):
        try:
            self.db.cursor.execute(self.BATCH_SKU_SHIFT_QUERY)
            result = self.db.cursor.fetchone()

            if result:
                batch_sku, shift = result
                return batch_sku, shift
            else:
                return " ", " "

        except Exception as e:
            logging.error(f"Error fetching batch_sku and shift: {e}")
            self.db.rollback()
            os._exit(1)
            
    def get_previous_shift(self, current_shift):
        shift_sequence = {'A': 'C', 'B': 'A', 'C': 'B'}
        return shift_sequence.get(current_shift, 'A')
    
    def get_machine_data(self, mc, json_key, start_time, end_time,shift):
        print("\n---------------------------------------------------")
        print(f"\nMachine {mc}")
        print("---------------------------------------------------")

        try:
            shift_value = json_key.split('_Data')[0] 
            machine_no = f"mc{mc}" 
            print("shift ",shift)
            self.db.cursor.execute(self.GET_MACHINE_UPTIME, (shift_value, machine_no))
            machine_uptime = self.db.cursor.fetchone()
            machine_uptime = machine_uptime[0] if machine_uptime else timedelta() 

            self.db.cursor.execute(self.GET_CHECKPOINT_QUERY.format(mc=mc))
            checkpoint_data = self.db.cursor.fetchone()

            speed = 100
            cld_production = 0
            HMI_VER_OPEN_START = HMI_VER_CLOSE_END = HMI_HOZ_OPEN_START = HMI_HOZ_CLOSE_END = 0.0

            if checkpoint_data and isinstance(checkpoint_data[0], dict):
                speed = checkpoint_data[0].get("Machine_Speed_PPM", 100)
                cld_production = cld_production = checkpoint_data[0].get(json_key, 0) or 0

                
                print(f"\nCld count : {cld_production}")

                HMI_VER_OPEN_START = checkpoint_data[0].get("HMI_VER_OPEN_START", 0.0)
                HMI_VER_CLOSE_END = checkpoint_data[0].get("HMI_VER_CLOSE_END", 0.0)
                HMI_HOZ_OPEN_START = checkpoint_data[0].get("HMI_HOZ_OPEN_START", 0.0)
                HMI_HOZ_CLOSE_END = checkpoint_data[0].get("HMI_HOZ_CLOSE_END", 0.0)

            sealing_time_vertical = (HMI_VER_OPEN_START - HMI_VER_CLOSE_END) * (500 / (3 * speed)) if speed != 0 else 0.0
            sealing_time_horizontal = (HMI_HOZ_OPEN_START - HMI_HOZ_CLOSE_END) * (500 / (3 * speed)) if speed != 0 else 0.0

            status_value = 2  
            timestamp_value = None

            self.db.cursor.execute(self.GET_MACHINE_STATUS_QUERY.format(mc=mc))
            result = self.db.cursor.fetchone()
         

            if result:
                status_value, timestamp_value = result
            else :
                status_value = 2  
                timestamp_value = None

            current_time = datetime.now(self.india_timezone)
            
            if timestamp_value:
                time_difference = current_time - timestamp_value
                print(f"Current Time : {current_time}")
                print(f"DB Timestamp : {timestamp_value}")
                print(f"Time Difference : {time_difference.total_seconds()} seconds")

                if time_difference.total_seconds() > 300:
                    status_message = "Offline"
                else:
                    status_message = "Running" if status_value == 1 else "Stopped"
            else:
                print("No recent data found in the last 5 minutes.")
                status_message = "Offline"  # Default to "Offline" if no data

            print("Machine Status = ", status_message)

            self.db.cursor.execute(self.GET_MACHINE_DATA_QUERY.format(mc=mc))
            machine_row = self.db.cursor.fetchone()

            self.db.cursor.execute(self.hopper_level_query.format(mc=mc))
            hopper_data = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_STOPPAGE_TIME.format(mc=mc),(shift,))
            stoppage_minutes, stoppage_data, longest_duration , highest_count = self.db.cursor.fetchone()

            self.db.cursor.execute(self.GET_PREV_STOPPAGE_TIME.format(mc=mc))
            results = self.db.cursor.fetchall()
            
            if len(results) >= 2:
                result = results[1]
                prev_stoppage_minutes = result[0]
                prev_stoppage_data = result[1]
                prev_longest_duration = result[2]
                prev_highest_count = result[3]

            if longest_duration or prev_longest_duration:
                try:
                    longest_duration = json.loads(longest_duration)
                    prev_longest_duration = json.loads(prev_longest_duration)
                except json.JSONDecodeError:
                    longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_longest_duration = {"1": [0, ""], "2": [0, ""], "3": [0, ""]} 

            if highest_count or prev_highest_count :
                try :
                    highest_count = json.loads(highest_count)
                    prev_highest_count = json.loads(prev_highest_count)
                except json.JSONDecodeError:
                    highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}
                    prev_highest_count = {"1": [0, ""], "2": [0, ""], "3": [0, ""]}

            print("longest stoppage time ",longest_duration)
            print("prev longest stoppage time ",prev_longest_duration)

            time_difference_hours = (current_time - start_time).total_seconds() / 3600
            print("time diff = ",time_difference_hours) 
            print("cld count = ",cld_production)
            BBOEE = ((cld_production * 100) / (600 * time_difference_hours)) * 8 if time_difference_hours > 0 else 0
            BBOEE = max(0, min(BBOEE, 100))

            if machine_row :
                mc_data = {
                    "speed": max(speed if speed is not None else 100, 100),
                    "status": status_message,
                    "jamming": True,
                    "pulling": True,
                    "sealing": True,
                    "sealing_time_vertical": 166.67 if sealing_time_vertical is None else round(sealing_time_vertical, 2),
                    "sealing_time_horizontal": 259.26 if sealing_time_horizontal is None else round(sealing_time_horizontal, 2),
                    "stoppage_time":  stoppage_minutes if stoppage_minutes is not None else 0,
                    "stoppage_count": int(stoppage_data) if stoppage_data is not None else 0,
                    "longest_duration" : longest_duration,
                    "prev_stoppage_time" : prev_stoppage_minutes if prev_stoppage_minutes is not None else 0,
                    "prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,
                    "prev_longest_duration" : prev_longest_duration,
                    "highest_count" : highest_count,
                    "prev_highest_count" : prev_highest_count,
                    "cld_production": round(cld_production, 2),
                    "outgoing_quality": True,
                    "vertical_pressure": round(machine_row[3], 2) if machine_row[3] is not None else 0,
                    "vertical_rear_temp": round(machine_row[7], 2) if machine_row[7] is not None else 0,
                    "horizontal_pressure": round(machine_row[4], 2) if machine_row[4] is not None else 0,
                    "vertical_front_temp": round(machine_row[8], 2) if machine_row[8] is not None else 0,
                    "breakdown_prediction": True,
                    "hopper_level_primary": round(hopper_data[0], 2) if hopper_data[0] is not None else 0,
                    "horizontal_rear_temp": round(machine_row[9], 2) if machine_row[9] is not None else 0,
                    "horizontal_front_temp": round(machine_row[10], 2) if machine_row[10] is not None else 0,
                    "hopper_level_secondary": round(hopper_data[1], 2) if hopper_data[1] is not None else 0,
                    "productivity_prediction": True,
                    "machine_uptime" : str(machine_uptime),
                    "bboee" : round(BBOEE, 2) if BBOEE is not None else 0
                }

                setattr(self, f"last_valid_mc{mc}_data", mc_data)
            else :
                print(f"No recent data found for mc{mc}. Fetching latest record if available.")
                self.db.cursor.execute(self.QUERY_LATEST_ROW.format(mc=mc))
                latest_row = self.db.cursor.fetchone()

                if latest_row:
                    mc_data = self.get_default_machine_data(sealing_time_vertical,sealing_time_horizontal,stoppage_minutes,cld_production,machine_uptime,stoppage_data,BBOEE,longest_duration,prev_stoppage_minutes,prev_stoppage_data,prev_longest_duration,highest_count,prev_highest_count)
                    mc_data.update({
                        "vertical_pressure": round(latest_row[0], 2) if latest_row[0] is not None else 0,
                        "horizontal_pressure": round(latest_row[1], 2) if latest_row[1] is not None else 0,
                        "hopper_level_primary": round(latest_row[2], 2) if latest_row[2] is not None else 0,
                        "hopper_level_secondary": round(latest_row[3], 2) if latest_row[3] is not None else 0,
                        "vertical_rear_temp": round(latest_row[4], 2) if latest_row[4] is not None else 0,
                        "horizontal_rear_temp": round(latest_row[5], 2) if latest_row[5] is not None else 0,
                        "horizontal_front_temp": round(latest_row[6], 2) if latest_row[6] is not None else 0
                    })
                else:
                    mc_data = self.get_default_machine_data(sealing_time_vertical,sealing_time_horizontal,stoppage_minutes,cld_production,machine_uptime,stoppage_data,BBOEE,longest_duration,prev_stoppage_minutes,prev_stoppage_data,prev_longest_duration,highest_count,prev_highest_count)

            return mc_data

        except Exception as e:
            logging.error(f"Error getting machine data for machine {mc}: {e}")
            self.db.rollback()
            os._exit(1)
            #return self.get_default_machine_data(0,0,0,0)

    def get_default_machine_data(self,sealing_time_vertical,sealing_time_horizontal, stoppage_minutes,cld_production,machine_uptime,stoppage_data,BBOEE,longest_duration,prev_stoppage_minutes,prev_stoppage_data, prev_longest_duration,highest_count,prev_highest_count):
        print("Inside Get default machine data function")
        return {
            "speed": 100,
            "status": "Offline",
            "jamming": True,
            "pulling": True,
            "sealing": True,
            "sealing_time_vertical": 166.67 if sealing_time_vertical is None else round(sealing_time_vertical, 2),
            "sealing_time_horizontal": 259.26 if sealing_time_horizontal is None else round(sealing_time_horizontal, 2),
            "stoppage_time":  stoppage_minutes if stoppage_minutes is not None else 0,
            "stoppage_count": int(stoppage_data) if stoppage_data is not None else 0,
            "longest_duration" : longest_duration,
            "highest_count" : highest_count,
            "prev_highest_count":prev_highest_count,
            "prev_stoppage_time" : prev_stoppage_minutes if prev_stoppage_minutes is not None else 0,
            "prev_stoppage_count": int(prev_stoppage_data) if prev_stoppage_data is not None else 0,
            "prev_longest_duration" : prev_longest_duration,
            "cld_production": round(cld_production, 2),
            "outgoing_quality": True,
            "vertical_pressure": 0.0,
            "vertical_rear_temp": 0.0,
            "horizontal_pressure": 0.0,
            "vertical_front_temp": 0.0,
            "breakdown_prediction": True,
            "hopper_level_primary": 0.0,
            "horizontal_rear_temp": 0.0,
            "horizontal_front_temp": 0.0,
            "hopper_level_secondary": 0.0,
            "productivity_prediction": True,
            "machine_uptime" : str(machine_uptime),
            "bboee" : round(BBOEE, 2) if BBOEE is not None else 0
        }

    def update_loop3(self):
        try:
            self.db.ensure_db_connection()
            current_time = datetime.now(self.india_timezone)
            shift,json_key, start_time, end_time = self.get_shift_and_time_range(current_time)

            print("start time ",start_time)
            bulk_settling_time = self.calculate_bulk_settling_time()
            cld_production = self.calculate_cld_production(json_key)
            ega = self.calculate_ega()

            primary_tank, secondary_tank, taping_machine, case_erector, machines_data ,checkweigher = self.fetch_latest_values(start_time, end_time, json_key,shift)

            plantair, planttemperature = self.extract_latest_plant_data()
            batch_sku ,shift = self.fetch_batch_sku_and_shift()

            self.write_db.cursor.execute(self.UPDATE_LOOP3_QUERY, (
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
                round(planttemperature, 2),
                batch_sku,
                shift,
                checkweigher
            ))

            if self.write_db.cursor.rowcount == 0:
                self.insert_into_loop3(current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data, plantair, planttemperature,batch_sku,shift,checkweigher)

            self.write_db.commit()

        except Exception as e:
            error_message = str(e)
            
            if "violates check constraint" in error_message or "chunk" in error_message:
                logging.warning(f"\n\nHypertable chunk constraint violated, inserting instead. Error: {error_message}")

               
                self.db.rollback() 
                try:
                    self.insert_into_loop3(current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data, plantair, planttemperature,batch_sku,shift,checkweigher)
                except Exception as insert_error:
                    logging.error(f"\n\nError inserting loop3_overview data after hypertable error: {insert_error}")
                    self.write_db.rollback()
                    os._exit(1)

            else:
                logging.error(f"\n\nError in update_loop3_overview : {e}")
                self.write_db.rollback()
            os._exit(1)

    def insert_into_loop3(self, current_time, primary_tank, secondary_tank, bulk_settling_time, cld_production, ega, taping_machine, case_erector, machines_data, plantair, planttemperature,batch_sku,shift,checkweigher):
        try:
            self.write_db.ensure_db_connection()
            self.write_db.cursor.execute(self.INSERT_LOOP3_QUERY, (
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
                    round(planttemperature, 2),
                    batch_sku,
                    shift,
                    checkweigher
                ))
            self.write_db.commit()

        except Exception as e:
            logging.error(f"\n\n Error inserting into loop3_overview : {e}")
            self.write_db.rollback()
            os._exit(1)


def main():

    try :
        config = load_config()
        db = DatabaseConnection(config, "read")
        write_db = DatabaseConnection(config, "write")

        db.connect_db()
        write_db.connect_db()
        monitor = Loop3Monitor(db,write_db, config)

        data_flow_thread = threading.Thread(target=db.ensure_db_connection, daemon=True)
        data_flow_thread.start()

        while True:
            try :
                monitor.update_loop3()
            except Exception as e:
                logging.error(f"\nError in main loop : {e}")
                db.rollback()
                write_db.rollback
                os._exit(1)
            finally:
                logging.info("\nSleeping for 5 seconds before the next iteration...")
                time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Shutting down Loop4 monitor...")
        os._exit(1)
    finally:
        db.close_db_connection()
        write_db.close_db_connection()



if __name__ == "__main__":
    main()



