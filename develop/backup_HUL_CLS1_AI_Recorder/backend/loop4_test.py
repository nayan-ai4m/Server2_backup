import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
import pytz
import time
import json
import sys

india_timezone = pytz.timezone("Asia/Kolkata")

class Loop4Overview:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost", database="", user="postgres", password=""
        )
        self.cursor = self.conn.cursor()
        self.last_valid_data = {}

    def timedelta_to_minutes(self, td):
        return td.total_seconds() / 60

    def get_shift_and_time_range(self, current_time):
        shift_times = [
            ("A", 7, 15),
            ("B", 15, 23),
            ("C", 23, 7),
        ]
        for shift, start_hour, end_hour in shift_times:
            if start_hour <= current_time.hour < end_hour or (shift == "C" and current_time.hour < 7):
                start_time = current_time.replace(hour=start_hour, minute=0, second=0, microsecond=0)
                if shift == "C" and current_time.hour < 7:
                    start_time -= timedelta(days=1)
                end_time = start_time + timedelta(hours=8)
                return shift, start_time, end_time

    def execute_query(self, query, params):
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Query Execution Error: {e}")
            return []

    def fetch_transitions(self, start_time, end_time, mc, state_column="status"):
        if mc in ["mc28", "mc29"]:
            state_column = "state"  

        query = f"""
        SELECT 
            time_bucket, 
            CASE
                WHEN prev_state != 1 AND state = 1 THEN 'START'
                WHEN state != 1 AND prev_state = 1 THEN 'STOP'
                ELSE '-'
            END AS transition
        FROM (
            SELECT 
                time_bucket, 
                {state_column} AS state, 
                LAG({state_column}) OVER (ORDER BY time_bucket) AS prev_state
            FROM (
                SELECT 
                    time_bucket('2 minutes', timestamp) AS time_bucket,
                    mode() WITHIN GROUP (ORDER BY {state_column}) AS {state_column}
                FROM {mc}
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY time_bucket
            ) AS bucketed_data
        ) AS transitions
        WHERE (state != 1 AND prev_state = 1) OR (prev_state != 1 AND state = 1);
        """
        return self.execute_query(query, (start_time, end_time))

    
    def calculate_total_duration(self, events):
        total_duration = timedelta()
        for i in range(len(events) - 1):
            if events[i][1] == 'STOP' and events[i + 1][1] == 'START':
                stop_time = datetime.strptime(events[i][0], '%Y-%m-%d %H:%M:%S%z')
                start_time = datetime.strptime(events[i + 1][0], '%Y-%m-%d %H:%M:%S%z')
                total_duration += start_time - stop_time
        return total_duration

    def fetch_machine_data(self, mc, start_time, end_time, default_data):
        shift, _, _ = self.get_shift_and_time_range(datetime.now(india_timezone))
        cld_column = f"cld_count_{shift.lower()}"
        
        
        state_column = "state" if mc in ["mc28", "mc29"] else "status"

        stoppage_count_query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {state_column}, LAG({state_column}) OVER (ORDER BY time_bucket) AS prev_state
            FROM (
                SELECT time_bucket('2 minutes', timestamp) AS time_bucket,
                    mode() WITHIN GROUP (ORDER BY {state_column}) AS {state_column}
                FROM {mc}
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY time_bucket
            ) AS bucketed_data
        ) AS transitions
        WHERE prev_state = 1 AND {state_column} != 1;
        """

        stoppage_data = self.execute_query(stoppage_count_query, (start_time, end_time))
        stoppage_count = stoppage_data[0][0] if stoppage_data else 0

        data_query = f"""
        SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
            AVG(ver_pressure) AS avg_ver_pressure, AVG(hor_pressure) AS avg_hor_pressure,
            AVG(hopper_1_level) AS avg_hopper_1_level, AVG(hopper_2_level) AS avg_hopper_2_level,
            AVG((verticalsealerrear_1 + verticalsealerrear_2 + verticalsealerrear_3 +
                    verticalsealerrear_4 + verticalsealerrear_5 + verticalsealerrear_6 +
                    verticalsealerrear_7 + verticalsealerrear_8 + verticalsealerrear_9 +
                    verticalsealerrear_10 + verticalsealerrear_11 + verticalsealerrear_12 +
                    verticalsealerrear_13) / 13)/10 AS avg_vertical_rear_temp,
            AVG((verticalsealerfront_1 + verticalsealerfront_2 + verticalsealerfront_3 +
                    verticalsealerfront_4 + verticalsealerfront_5 + verticalsealerfront_6 +
                    verticalsealerfront_7 + verticalsealerfront_8 + verticalsealerfront_9 +
                    verticalsealerfront_10 + verticalsealerfront_11 + verticalsealerfront_12 +
                    verticalsealerfront_13) / 13)/10 AS avg_vertical_front_temp,
            AVG(hor_sealer_rear)/10 AS avg_horizontal_rear_temp,
            AVG(hor_sealer_front)/10 AS avg_horizontal_front_temp,
            {state_column}, {cld_column}, timestamp
        FROM {mc}
        WHERE timestamp BETWEEN %s AND %s
        GROUP BY time_bucket, {state_column}, {cld_column}, timestamp
        ORDER BY time_bucket DESC
        LIMIT 1;
        """
        
        data_result = self.execute_query(data_query, (start_time, end_time))
        
        
        if data_result:
            data = data_result[0]
            machine_timestamp = data[11]  
            buffer_time = datetime.now(india_timezone) - timedelta(minutes=1)

            if machine_timestamp >= buffer_time:
                if data[10] == 1:  
                    status_message = "Running"
                    print(f"Machine {mc} is {status_message}")
                else:
                    status_message = "Stopped"
                    print(f"Machine {mc} is {status_message}")
            else:
                status_message = "Offline"
                print(f"Machine {mc} is {status_message}")

            
            mc_data = {
                'speed': max(data[1], 120),
                'status': status_message,
                'jamming': True,
                'pulling': True,
                'sealing': True,
                'sealing_time_vertical': 192,
                'sealing_time_horizontal': 195,
                'stoppage_time': self.timedelta_to_minutes(self.calculate_total_duration(self.fetch_transitions(start_time, end_time, mc))),
                'cld_production': data[12],
                'stoppage_count': stoppage_count,
                'outgoing_quality': True,
                'vertical_pressure': round(data[2], 2),
                'vertical_rear_temp': round(data[6], 2),
                'horizontal_pressure': round(data[3], 2),
                'vertical_front_temp': round(data[7], 2),
                'breakdown_prediction': True,
                'hopper_level_primary': max(round(data[4], 2), 72.56),
                'horizontal_rear_temp': round(data[8], 2),
                'horizontal_front_temp': round(data[9], 2),
                'hopper_level_secondary': max(round(data[5], 2), 4.8),
                'productivity_prediction': True,
            }
            self.last_valid_data[mc] = mc_data
        else:
            mc_data = self.last_valid_data.get(mc, default_data)
        
        return mc_data

    def update_loop4_overview(self):
        try:
            current_time = datetime.now(india_timezone)
            shift, start_time, end_time = self.get_shift_and_time_range(current_time)

            
            default_machine_data = {
                'speed': 120, 'status': "Offline", 'jamming': True, 'pulling': True, 'sealing': True,
                'sealing_time_vertical': 192, 'sealing_time_horizontal': 195, 'stoppage_time': 0,
                'cld_production': 0, 'stoppage_count': 0, 'outgoing_quality': True, 'vertical_pressure': 3.4,
                'vertical_rear_temp': 135, 'horizontal_pressure': 2.69, 'vertical_front_temp': 135,
                'breakdown_prediction': True, 'hopper_level_primary': 72.24, 'horizontal_rear_temp': 145,
                'horizontal_front_temp': 145, 'hopper_level_secondary': 72.4, 'productivity_prediction': True
            }

            machines_data = {
                f"mc{i}": self.fetch_machine_data(f"mc{i}", start_time, end_time, default_machine_data)
                for i in range(25, 31)
            }

            ega_query = """
            SELECT ABS((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) AS EGA
            FROM loop4_checkweigher
            WHERE timestamp BETWEEN %s AND %s
            """
            ega_result = self.execute_query(ega_query, (start_time, end_time))
            ega = min(max(ega_result[0][0], 0) if ega_result and ega_result[0][0] else 0.5, 0.5)

            
            bulk_query = """
            SELECT ((primary_tank * 0.08) + (secondary_tank * 0.08)) /
                   (SELECT SUM(cld_weight) FROM loop4_checkweigher WHERE timestamp > NOW() - INTERVAL '1 hour') * 1000
            FROM loop4_sku
            ORDER BY timestamp DESC LIMIT 1
            """
            bulk_result = self.execute_query(bulk_query, ())
            bulk_settling_time = min(max(bulk_result[0][0] if bulk_result and bulk_result[0][0] else 9, 7), 12)

            
            update_query = """
            UPDATE loop4_overview
            SET 
                last_update = %s,
                primary_tank_level = %s,
                secondary_tank_level = %s,
                bulk_settling_time = %s,
                cld_production = %s,
                ega = %s,
                machines = %s
            WHERE last_update = (
                SELECT MAX(last_update) FROM loop4_overview
            )
            """

            self.cursor.execute(update_query, (
                current_time,
                72.24, 72.40,  
                bulk_settling_time,
                sum(machine['cld_production'] for machine in machines_data.values()),
                ega,
                Json(machines_data)
            ))

            if self.cursor.rowcount == 0:
                insert_query = """
                INSERT INTO loop4_overview (
                    last_update, primary_tank_level, secondary_tank_level, bulk_settling_time, cld_production, ega, machines
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                self.cursor.execute(insert_query, (
                    current_time,
                    72.24, 72.40,  
                    bulk_settling_time,
                    sum(machine['cld_production'] for machine in machines_data.values()),
                    ega,
                    Json(machines_data)
                ))

            self.conn.commit()

        except Exception as e:
            print(f"Error in update_loop4_overview: {e}")

    def run(self):
        while True:
            self.update_loop4_overview()
            time.sleep(10)

    def close(self):
        self.cursor.close()
        self.conn.close()


loop4_overview = Loop4Overview()
try:
    loop4_overview.run()
except KeyboardInterrupt:
    loop4_overview.close()
