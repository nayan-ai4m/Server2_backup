import psycopg2
from psycopg2.extras import Json, RealDictCursor
import time
from datetime import datetime, timedelta
from datetime import timezone
import pytz
import json
import os
import sys
india_timezone = pytz.timezone("Asia/Kolkata")
class Loop4Overview:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost", database="hul", user="postgres", password="ai4m2024"
        )
        self.cursor = self.conn.cursor()

        self.last_valid_mc25_data = None
        self.last_valid_mc26_data = None
        self.last_valid_mc27_data = None
        self.last_valid_mc28_data = None
        self.last_valid_mc29_data = None
        self.last_valid_mc30_data = None

    def timedelta_to_minutes(self,td):
        """Convert timedelta to total minutes."""
        return td.total_seconds() / 60

    def get_shift_and_time_range(self, current_time):
        if current_time.hour >= 7 and current_time.hour < 15:
            shift = "A"
            start_time = current_time.replace(
                hour=7, minute=0, second=0, microsecond=0)
            end_time = current_time.replace(
                hour=15, minute=0, second=0, microsecond=0)
        elif current_time.hour >= 15 and current_time.hour < 23:
            shift = "B"
            start_time = current_time.replace(
                hour=15, minute=0, second=0, microsecond=0)
            end_time = current_time.replace(
                hour=23, minute=0, second=0, microsecond=0)
        else:
            shift = "C"
            if current_time.hour < 7:
                start_time = (current_time - timedelta(days=1)).replace(
                    hour=23, minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(hour=7, minute=0, second=0, microsecond=0) 
            else:
                start_time = current_time.replace(
                    hour=23, minute=0, second=0, microsecond=0
                )
                end_time = current_time.replace(hour=7, minute=0, second=0, microsecond=0) + timedelta(days=1)
           

       
        return shift, start_time, end_time

    def fetch_transitions_by_shift(self, start_time, end_time, mc):
        """Fetch transitions (START/STOP) within a given time range for a specific machine."""
        query = f"""
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
                    {mc}
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
        self.cursor.execute(query, (start_time, end_time))
        results = self.cursor.fetchall()
        transitions_list = []

        if results:
            for row in results:
                formatted_time = row[0].strftime('%Y-%m-%d %H:%M:%S%z')
                transitions_list.append((formatted_time, row[1]))

        return transitions_list
    

    def fetch_transitions_by_shift_mc28_mc29(self, start_time, end_time, mc):
        """Fetch transitions (START/STOP) within a given time range for a specific machine."""
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
                state,
                LAG(state) OVER (ORDER BY time_bucket) AS prev_state
            FROM (
                SELECT 
                    time_bucket('2 minutes', timestamp) AS time_bucket,
                    mode() WITHIN GROUP (ORDER BY state) AS state
                FROM 
                    {mc}
                WHERE 
                    timestamp BETWEEN %s AND %s
                GROUP BY 
                    time_bucket
            ) AS bucketed_data
        ) AS transition
        WHERE 
            (state != 1 AND prev_state = 1) 
            OR (prev_state != 1 AND state = 1);
        """
        self.cursor.execute(query, (start_time, end_time))
        results = self.cursor.fetchall()
        transitions_list = []

        if results:
            for row in results:
                formatted_time = row[0].strftime('%Y-%m-%d %H:%M:%S%z')
                transitions_list.append((formatted_time, row[1]))

        return transitions_list
    
    def pair_and_calculate_total_duration(self, events):
        """Pair STOP and START events and calculate the total duration."""
        total_duration = timedelta()  
        paired_events = []
        
        for i in range(len(events) - 1):
            if events[i][1] == 'STOP' and events[i + 1][1] == 'START':
                stop_time = datetime.strptime(events[i][0], '%Y-%m-%d %H:%M:%S%z')
                start_time = datetime.strptime(events[i + 1][0], '%Y-%m-%d %H:%M:%S%z')
                duration = start_time - stop_time
                total_duration += duration
                paired_events.append((events[i][0], events[i + 1][0], duration))
        
        return total_duration if paired_events else timedelta(0)
        #return total_duration

    
    def calculate_current_shift_duration(self, mc):
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)

        if mc in ['mc28', 'mc29']:
            transitions_array = self.fetch_transitions_by_shift_mc28_mc29(start_time, end_time, mc)
        else:
            transitions_array = self.fetch_transitions_by_shift(start_time, end_time, mc)
        total_duration = self.pair_and_calculate_total_duration(transitions_array)

        return total_duration

    def get_machine_25(self, start_time, end_time):

        stoppage_time_minute = self.calculate_current_shift_duration('mc25')
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_count_{shift.lower()}"
        # current_time = datetime.now(india_timezone)
        buffer_time = current_time - timedelta(minutes=1)

        stoppage_count_query = """
            SELECT COUNT(*) AS status_change_count
            FROM (
                SELECT status, LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                FROM (
                    SELECT time_bucket('2 minutes', timestamp) AS time_bucket,
                           mode() WITHIN GROUP (ORDER BY status) AS status
                    FROM mc25
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY time_bucket
                ) AS bucketed_data
            ) AS transition
            WHERE prev_status = 1 AND status != 1;
        """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0

        data_query = f"""SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
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
                        AVG(hor_sealer_front) /10 AS avg_horizontal_front_temp,
                        status,{cld_column},timestamp
                        FROM mc25
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,status,{cld_column},timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1"""
        #self.cursor.execute(data_query)

        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                if avg_data[10] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[10] != 1:
                #     status = False
                # else:
                #     status = True
                mc_data = {'speed': avg_data[1] if avg_data[1] > 120 else 120,'status':status,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':192,'sealing_time_horizontal':195,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[11],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2],2),'vertical_rear_temp':round(avg_data[6],2),
                   'horizontal_pressure':round(avg_data[3],2),'vertical_front_temp':round(avg_data[7],2),'breakdown_prediction': True,'hopper_level_primary': round(avg_data[4], 2) if avg_data[4] != 0.0 else 72.56,'horizontal_rear_temp':round(avg_data[8],2),'horizontal_front_temp':round(avg_data[9],2),'hopper_level_secondary': max(round(avg_data[5], 2), 4.8),'productivity_prediction': True
                   }
                
                self.last_valid_mc25_data = mc_data
                
            else:
                print("No data found for mc25")
                mc_data = self.last_valid_mc25_data if self.last_valid_mc25_data is not None else {
                'speed': 120,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 223,'stoppage_time': 0,'cld_production':313,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.69,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 4.8,'productivity_prediction': True}
        
        except Exception as e:
                print(f"Error occurred: {e}")
                mc_data = {'speed': 120,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 223,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.69,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 70.20,'productivity_prediction': True}
                
        
        return mc_data

    def get_machine_26(self, start_time, end_time):
        stoppage_time_minute = self.calculate_current_shift_duration('mc26')
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_count_{shift.lower()}"
        buffer_time = current_time - timedelta(minutes=1)


        stoppage_count_query = """
            SELECT COUNT(*) AS status_change_count
            FROM (
                SELECT status, LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                FROM (
                    SELECT time_bucket('2 minutes', timestamp) AS time_bucket,
                           mode() WITHIN GROUP (ORDER BY status) AS status
                    FROM mc26
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY time_bucket
                ) AS bucketed_data
            ) AS transition
            WHERE prev_status = 1 AND status != 1;
        """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0
        
        data_query = f"""SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
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
                        status,{cld_column},timestamp 
                        FROM mc26
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,status,{cld_column},timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1"""
        
        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                print('mc26',avg_data[10])
                if avg_data[10] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[10] != 1:
                #     status = False
                # else:
                #     status = True
                mc_data = {'speed':avg_data[1] if avg_data[1] > 120 else 120,'status':status,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':192,'sealing_time_horizontal':195,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[11],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2],2),'vertical_rear_temp':round(avg_data[6],2),
                   'horizontal_pressure': round(avg_data[3],2),'vertical_front_temp':round(avg_data[7],2),'breakdown_prediction': True,'hopper_level_primary': round(avg_data[4], 2) if avg_data[4] != 0.0 else 75.30,'horizontal_rear_temp':round(avg_data[8],2),'horizontal_front_temp':round(avg_data[9],2),'hopper_level_secondary': max(round(avg_data[5], 2), 4.8),'productivity_prediction': True
                   }
                self.last_valid_mc26_data = mc_data
            else:
                print("No data found for mc26")
                mc_data = self.last_valid_mc26_data if self.last_valid_mc26_data is not None else {
                'speed': 120,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 235,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 216,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.2,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 70.20,'productivity_prediction': True}
        
        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 120,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 235,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.2,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 70.20,'productivity_prediction': True}
        
        return mc_data

    def get_machine_27(self, start_time, end_time):
        stoppage_time_minute = self.calculate_current_shift_duration('mc26')
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_count_{shift.lower()}"
        buffer_time = current_time - timedelta(minutes=1)

        stoppage_count_query = """
            SELECT COUNT(*) AS status_change_count
            FROM (
                SELECT status, LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                FROM (
                    SELECT time_bucket('2 minutes', timestamp) AS time_bucket,
                           mode() WITHIN GROUP (ORDER BY status) AS status
                    FROM mc27
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY time_bucket
                ) AS bucketed_data
            ) AS transition
            WHERE prev_status = 1 AND status != 1;
        """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0
        
        data_query = f"""SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
                        AVG(vertical_pressure) AS avg_ver_pressure, AVG(horizontal_pressure) AS avg_hor_pressure,
                        AVG(hopper_level_left) AS avg_hopper_1_level, AVG(hopper_level_right) AS avg_hopper_2_level,
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
                        status,{cld_column},timestamp
                        FROM mc27
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,status,{cld_column},timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1"""
        
        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                print('mc27',avg_data[10])
                if avg_data[10] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[10] != 1:
                #     status = False
                # else:
                #     status = True
                mc_data = {'speed':avg_data[1] if avg_data[1] > 100 else 100,'status':status,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':222,'sealing_time_horizontal':240,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[11],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2],2),'vertical_rear_temp':round(avg_data[6],2),
                   'horizontal_pressure':round(avg_data[3],2),'vertical_front_temp':round(avg_data[7],2),'breakdown_prediction': True,'hopper_level_primary': round(avg_data[4], 2) if avg_data[4] != 0.0 else 72.66,'horizontal_rear_temp':round(avg_data[8],2),'horizontal_front_temp':round(avg_data[9],2),'hopper_level_secondary':73.2,'productivity_prediction': True
                   }
                self.last_valid_mc27_data = mc_data
            else:
                print("No data found for mc27 query")
                mc_data = self.last_valid_mc27_data if self.last_valid_mc27_data is not None else {
                'speed': 100,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 230,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 287,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': '72.8','productivity_prediction': True}
        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 100,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 230,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': '72.0','productivity_prediction': True}
        
        return mc_data

    def get_machine_28(self, start_time, end_time):
        stoppage_time_minute = self.calculate_current_shift_duration('mc28')
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_{shift.lower()}"
        buffer_time = current_time - timedelta(minutes=1)
        stoppage_count_query = """
                SELECT COUNT(*) AS status_change_count
                FROM (
                    SELECT 
                        status,
                        LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                    FROM (
                        SELECT 
                            time_bucket('2 minutes', timestamp) AS time_bucket,
                            mode() WITHIN GROUP (ORDER BY state) AS status
                        FROM mc28
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY time_bucket
                    ) AS bucketed_data
                ) AS transition
                WHERE prev_status = 1 AND status != 1;
            """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0
        
        data_query = f"""select time_bucket('5 minutes',timestamp) as time_bucket,AVG(speed) /12 AS avg_speed,
                        AVG((vertical_sealer_rear_1_temp + vertical_sealer_rear_2_temp + vertical_sealer_rear_3_temp + 
                            vertical_sealer_rear_4_temp + vertical_sealer_rear_5_temp + vertical_sealer_rear_6_temp + 
                            vertical_sealer_rear_7_temp + vertical_sealer_rear_8_temp + vertical_sealer_rear_9_temp + 
                            vertical_sealer_rear_10_temp + vertical_sealer_rear_11_temp + vertical_sealer_rear_12_temp + 
                            vertical_sealer_rear_13_temp) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((vertical_sealer_front_1_temp + vertical_sealer_front_2_temp + vertical_sealer_front_3_temp + 
                            vertical_sealer_front_4_temp + vertical_sealer_front_5_temp + vertical_sealer_front_6_temp + 
                            vertical_sealer_front_7_temp + vertical_sealer_front_8_temp + vertical_sealer_front_9_temp + 
                            vertical_sealer_front_10_temp + vertical_sealer_front_11_temp + vertical_sealer_front_12_temp + 
                            vertical_sealer_front_13_temp) / 13)/10 AS avg_vertical_front_temp,
                        AVG(horizontal_sealer_rear_1_temp)/10 AS avg_horizontal_rear_temp,
                        AVG(horizontal_sealer_front_1_temp)/10 AS avg_horizontal_front_temp,
                        state,{cld_column},timestamp
                        from mc28
                        where timestamp > now() - '5 minutes'::interval
                        group by time_bucket,state,{cld_column},timestamp
                        order by time_bucket DESC
                        LIMIT 1"""
        
        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                if avg_data[6] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[6] != 1:
                #     status = False
                # else:
                #     status = True
                print('mc28',avg_data[6])
                mc_data = {'speed':avg_data[1] if avg_data[1] > 100 else 100,'status':False,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':231,'sealing_time_horizontal':231,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[7],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_rear_temp':round(avg_data[2],2),
                   'vertical_front_temp':round(avg_data[3],2),'breakdown_prediction': True,'hopper_level_primary':72.58,'horizontal_rear_temp': round(avg_data[4], 2) if round(avg_data[4], 2) >= 130 else 157.80
,'horizontal_front_temp':round(avg_data[5],2),'hopper_level_secondary':72.40,'productivity_prediction': True
                   }
                self.last_valid_mc28_data = mc_data
            else:
                print("No data found for mc28")
                mc_data = self.last_valid_mc28_data if self.last_valid_mc28_data is not None else {
                'speed': 100,'status': True,'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 210,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 170,'stoppage_count': 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 72.40,'productivity_prediction': True}
        
        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 100,'status': True,'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 210,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_rear_temp': 0,
                    'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 72.43,'productivity_prediction': True}
        return mc_data

    def get_machine_29(self, start_time, end_time):
        stoppage_time_minute = self.calculate_current_shift_duration('mc29')

        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_{shift.lower()}"
        buffer_time = current_time - timedelta(minutes=1)

        stoppage_count_query = """
                SELECT COUNT(*) AS status_change_count
                FROM (
                    SELECT 
                        status,
                        LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                    FROM (
                        SELECT 
                            time_bucket('2 minutes', timestamp) AS time_bucket,
                            mode() WITHIN GROUP (ORDER BY state) AS status
                        FROM mc29
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY time_bucket
                    ) AS bucketed_data
                ) AS transition
                WHERE prev_status = 1 AND status != 1;
            """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0
        

        data_query = f"""select time_bucket('5 minutes',timestamp) as time_bucket,AVG(speed) /12 AS avg_speed,
                        AVG((vertical_sealer_rear_1_temp + vertical_sealer_rear_2_temp + vertical_sealer_rear_3_temp + 
                            vertical_sealer_rear_4_temp + vertical_sealer_rear_5_temp + vertical_sealer_rear_6_temp + 
                            vertical_sealer_rear_7_temp + vertical_sealer_rear_8_temp + vertical_sealer_rear_9_temp + 
                            vertical_sealer_rear_10_temp + vertical_sealer_rear_11_temp + vertical_sealer_rear_12_temp + 
                            vertical_sealer_rear_13_temp) / 13)/10 AS avg_vertical_rear_temp,
                        AVG((vertical_sealer_front_1_temp + vertical_sealer_front_2_temp + vertical_sealer_front_3_temp + 
                            vertical_sealer_front_4_temp + vertical_sealer_front_5_temp + vertical_sealer_front_6_temp + 
                            vertical_sealer_front_7_temp + vertical_sealer_front_8_temp + vertical_sealer_front_9_temp + 
                            vertical_sealer_front_10_temp + vertical_sealer_front_11_temp + vertical_sealer_front_12_temp + 
                            vertical_sealer_front_13_temp) / 13)/10 AS avg_vertical_front_temp,
                        AVG(horizontal_sealer_rear_1_temp)/10 AS avg_horizontal_rear_temp,
                        AVG(horizontal_sealer_front_1_temp)/10 AS avg_horizontal_front_temp,
                        state,{cld_column},timestamp
                        from mc29
                        where timestamp > now() - '5 minutes'::interval
                        group by time_bucket,state,{cld_column},timestamp
                        order by time_bucket DESC
                        LIMIT 1"""
        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                print('mc29',avg_data[6])
                if avg_data[6] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[6] != 1:
                #     status = False
                # else:
                #     status = True
                mc_data = {'speed':avg_data[1] if avg_data[1] > 100 else 100,'status':status,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':230,'sealing_time_horizontal':240,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[7],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_rear_temp':round(avg_data[2],2),
                   'vertical_front_temp':round(avg_data[3],2),'breakdown_prediction': True,'hopper_level_primary':73.46,'horizontal_rear_temp': round(avg_data[4], 2) if round(avg_data[4], 2) >= 130 else 157.84
,'horizontal_front_temp':round(avg_data[5],2),'hopper_level_secondary':72.43,'productivity_prediction': True
                   }
                self.last_valid_mc29_data = mc_data
            else:
                print("No data found for mc29")
                mc_data = self.last_valid_mc29_data if self.last_valid_mc29_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 38,'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality': True,'vertical_rear_temp': 135,
                    'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 72.40,'productivity_prediction': True}
        
        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 220,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_rear_temp': 135,
                    'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 73.40,'productivity_prediction': True}
        
        return mc_data

    def get_machine_30(self, start_time, end_time):
        stoppage_time_minute = self.calculate_current_shift_duration('mc26')
        current_time = datetime.now(india_timezone)
        shift, start_time, end_time = self.get_shift_and_time_range(current_time)
        cld_column = f"cld_count_{shift.lower()}"
        buffer_time = current_time - timedelta(minutes=1)

        stoppage_count_query = """
            SELECT COUNT(*) AS status_change_count
            FROM (
                SELECT status, LAG(status) OVER (ORDER BY time_bucket) AS prev_status
                FROM (
                    SELECT time_bucket('2 minutes', timestamp) AS time_bucket,
                           mode() WITHIN GROUP (ORDER BY status) AS status
                    FROM mc30
                    WHERE timestamp BETWEEN %s AND %s
                    GROUP BY time_bucket
                ) AS bucketed_data
            ) AS transition
            WHERE prev_status = 1 AND status != 1;
        """
        self.cursor.execute(stoppage_count_query, (start_time, end_time))
        stoppage_result = self.cursor.fetchall()
        stoppage_data = stoppage_result[0][0] if stoppage_result else 0

        data_query = f"""SELECT time_bucket('5 minutes', timestamp) AS time_bucket, AVG(speed) / 12 AS avg_speed,
                        AVG(vertical_pressure) AS avg_ver_pressure, AVG(horizontal_pressure) AS avg_hor_pressure,
                        AVG(hopper_level_left) AS avg_hopper_1_level, AVG(hopper_level_right) AS avg_hopper_2_level,
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
                        status,{cld_column},timestamp
                        FROM mc30
                        WHERE timestamp > now() - '5 minutes'::interval
                        GROUP BY time_bucket,status,{cld_column},timestamp
                        ORDER BY time_bucket DESC
                        LIMIT 1"""
        try:
            self.cursor.execute(data_query)
            avg_data_result = self.cursor.fetchall()
            if avg_data_result:
                avg_data = avg_data_result[0]
                print('mc30',avg_data[10])
                if avg_data[10] != 1:  
                    status_message = "Stopped"
                    print("Machine is stopped based on status check")
                else:
                    machine_timestamp = avg_data[12]  
                    if machine_timestamp >= buffer_time:
                        status_message = "Running"
                        print("Machine is running based on timestamp")
                    else:
                        status_message = "Offline"
                        print("Machine is offline based on timestamp")
                # if avg_data[10] != 1:
                #     status = False
                # else:
                #     status = True
                mc_data = {'speed':avg_data[1] if avg_data[1] > 100 else 100,'status':status,'jamming':True,'pulling':True,'sealing':True,'sealing_time_vertical':222,'sealing_time_horizontal':240,'stoppage_time':self.timedelta_to_minutes(stoppage_time_minute),'cld_production':avg_data[11],'stoppage_count':round(stoppage_data,2) if stoppage_data is not None else 0,'outgoing_quality':True,'vertical_pressure':round(avg_data[2],2),'vertical_rear_temp':round(avg_data[6],2),
                        'horizontal_pressure':round(avg_data[3],2),'vertical_front_temp':round(avg_data[7],2),'breakdown_prediction': True,'hopper_level_primary': round(avg_data[4], 2) if avg_data[4] != 0.0 else 72.34
,'horizontal_rear_temp':round(avg_data[8],2),'horizontal_front_temp':round(avg_data[9],2),'hopper_level_secondary':73.40,'productivity_prediction': True
                        }
                self.last_valid_mc30_data = mc_data
            else:
                print("No data found for mc30")
                mc_data = self.last_valid_mc30_data if self.last_valid_mc30_data is not None else {
                'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 240,'stoppage_time': 0,'cld_production': 162,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 72.20,'productivity_prediction': True}

        except Exception as e:
            print(f"Error occurred: {e}")
            mc_data = {'speed': 0,'status': "Offline",'jamming': True,'pulling': True,'sealing': True,'sealing_time_vertical': 220,'sealing_time_horizontal': 240,'stoppage_time': 0,'cld_production': 0,'stoppage_count': 0,'outgoing_quality': True,'vertical_pressure': 3.4,'vertical_rear_temp': 135,
                    'horizontal_pressure': 2.66,'vertical_front_temp': 135,'breakdown_prediction': True,'hopper_level_primary': 72.24,'horizontal_rear_temp': 145,'horizontal_front_temp': 145,'hopper_level_secondary': 72.40,'productivity_prediction': True}
        
        return mc_data

    def fetch_latest_values(self, start_time, end_time):
        
        try:
            self.cursor.execute("SELECT timestamp, (primary_tank)*0.08 , (secondary_tank)*0.08 FROM loop4_sku ORDER BY timestamp DESC LIMIT 1")
            result = self.cursor.fetchone()
            primary_tank_value, secondary_tank_value = ((result[1], result[2]) if result else (0.0,6.0 ))

            self.cursor.execute("SELECT timestamp, loop4_status FROM taping ORDER BY timestamp DESC LIMIT 1")
            result1 = self.cursor.fetchone()
            taping_machine = result1[1] if result1 else True
        
            self.cursor.execute("SELECT timestamp, loop4_status FROM case_erector ORDER BY timestamp DESC LIMIT 1")
            result2 = self.cursor.fetchone()
            case_erector = result2[1] if result2 else True

            machines_data = {
            "mc25": self.get_machine_25(start_time, end_time),
            "mc26": self.get_machine_26(start_time, end_time),
            "mc27": self.get_machine_27(start_time, end_time),
            "mc28": self.get_machine_28(start_time, end_time),
            "mc29": self.get_machine_29(start_time, end_time),
            "mc30": self.get_machine_30(start_time, end_time)
            }
            
            return primary_tank_value,secondary_tank_value,taping_machine,case_erector,machines_data
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

    #############################

    def calculate_ega(self, start_time, end_time):
        try:
            query = """
                SELECT ABS((AVG(cld_weight) - AVG(target_weight)) / AVG(target_weight)) AS EGA
                FROM loop4_checkweigher
                WHERE timestamp BETWEEN %s AND %s
            """
            self.cursor.execute(query, (start_time, end_time))
            result = self.cursor.fetchone()
            if result[0] != None:
                ega = result[0] if result and 0 <= result[0] <= 0.5 else 0.5
            else:
                ega=0.5
            return float(ega)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return 0.5


    def calculate_bulk_settling_time(self):
        try:
            today_date = datetime.now(india_timezone).date()
            query = """
                SELECT CASE 
                    WHEN primary_tank = 0 AND secondary_tank = 0 THEN NULL
                    WHEN DATE(timestamp) = %s THEN 
                        ((primary_tank * 0.08) + (secondary_tank * 0.08)) / 
                        (SELECT SUM(cld_weight) FROM loop4_checkweigher WHERE timestamp > NOW() - INTERVAL '1 hour') * 1000
                    ELSE 0.0
                END AS result
                FROM loop4_sku 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            self.cursor.execute(query, (today_date,))
            result = self.cursor.fetchone()[0]  # `result` will be None if both tanks are 0
            print("bulk_settling_time_real",result)
            # Set bulk_settling_time to '--' if both tanks are 0, otherwise calculate
            if result is None:
                bulk_settling_time = 8
            else:
                bulk_settling_time = result if 7 < result < 12 else 9
            
            return bulk_settling_time
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    
    def calculate_cld_production(self,shift):
        today_date = datetime.now(india_timezone).date()

        cld_column = f"cld_count_{shift.lower()}"  # For mc25 to mc27, mc30
        cld_column_mc28_29 = f"cld_{shift.lower()}"  # For mc28 and mc29

        # Initialize cld_production
        cld_production = 0

        machines = {
            "mc25": cld_column,
            "mc26": cld_column,
            "mc27": cld_column,
            "mc30": cld_column,
            "mc28": cld_column_mc28_29,
            "mc29": cld_column_mc28_29
        }

        for mc_table, column in machines.items():
            query = f"SELECT timestamp, {column} FROM {mc_table} ORDER BY timestamp DESC LIMIT 1"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            if result:
                latest_timestamp, latest_count = result
                # Check if the timestamp is from today
                if latest_timestamp.date() == today_date:
                    cld_production += latest_count if latest_count is not None else 0
                else:
                    cld_production += 0  # Not from today, add zero
            else:
                # No data for this machine, add zero
                cld_production += 0

        return cld_production
    
    def update_loop4_overview(self):
        try:
            current_time = datetime.now(india_timezone)
            shift, start_time, end_time = self.get_shift_and_time_range(current_time)
           

            start_time = start_time.replace(tzinfo=timezone.utc)
            end_time = end_time.replace(tzinfo=timezone.utc)

            new_shift_start = start_time + timedelta(minutes=5)
            if current_time < new_shift_start:
                prev_shift_start = start_time - timedelta(hours=8)
                prev_shift_end = start_time
                ega = self.calculate_ega(prev_shift_start, prev_shift_end)
               
            else:
                ega = self.calculate_ega(start_time, end_time)

            print("ega",round(ega,2))        
               
            primary_tank, secondary_tank, taping_machine, case_erector, machines_data = self.fetch_latest_values(start_time, end_time)

            bulk_settling_time = self.calculate_bulk_settling_time()
            print("bulk_settling_time",bulk_settling_time)
            cld_production = self.calculate_cld_production(shift)

            update_query = """
            UPDATE loop4_overview
            SET 
                last_update = %s,
                primary_tank_level = %s,
                secondary_tank_level = %s,
                bulk_settling_time = %s,
                cld_production = %s,
                ega = %s,
                taping_machine = %s,
                case_erector = %s,
                machines = %s
            WHERE last_update = (
                SELECT MAX(last_update) FROM loop4_overview
            )
            """
            self.cursor.execute(update_query, (
                current_time,
                round(primary_tank, 2),
                round(secondary_tank, 2),
                int(bulk_settling_time) if bulk_settling_time is not None else 9,
                #round(bulk_settling_time if bulk_settling_time is not None else 8, 2),
                round(cld_production, 2),  # Example for cld_production rounded to 2 decimal places
                round(ega if ega is not None else 0.0, 2),
                True,
                True,
                Json(machines_data)
            ))

            if self.cursor.rowcount == 0:
                insert_query = """
                INSERT INTO loop4_overview (
                    last_update,
                    primary_tank_level,
                    secondary_tank_level,
                    bulk_settling_time,
                    cld_production,
                    ega,
                    taping_machine,
                    case_erector,
                    machines
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                self.cursor.execute(insert_query, (
                    current_time,
                    round(primary_tank, 2),
                    round(secondary_tank, 2),
                    round(bulk_settling_time if bulk_settling_time is not None else 8, 2),
                    round(cld_production, 2),
                    round(ega,2) if ega is not None else 0,
                    taping_machine,
                    case_erector,
                    Json(machines_data)
                ))

            self.conn.commit()
            print(f"Updated data at {current_time} with bulk settling time {bulk_settling_time}")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    
    def run(self):
        while True:
            self.update_loop4_overview()
            time.sleep(10)

    def close(self):
        self.cursor.close()
        self.conn.close()


# Usage
loop4_overview = Loop4Overview()
try:
    loop4_overview.run()
except KeyboardInterrupt:
    loop4_overview.close()


