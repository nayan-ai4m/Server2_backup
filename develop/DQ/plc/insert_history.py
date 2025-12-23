import psycopg2
from collections import defaultdict
import json
import datetime
import time
class HistoricalGraphsInserter:
    def __init__(self, table_names, host="localhost", database="hul", user="postgres", password="ai4m2024"):
        self.table_names = table_names
        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        self.cur = self.conn.cursor()
    def fetch_data_loop4(self,table_name):
        mitsubishi_query =f"""SELECT time_bucket('1 seconds',"timestamp") as time_bucket, 
                   AVG(verticalsealerfront_1)::integer AS "front1", 
                   AVG(verticalsealerfront_2)::integer AS "front2", 
                   AVG(verticalsealerfront_3)::integer AS "front3", 
                   AVG(verticalsealerfront_4)::integer AS "front4", 
                   AVG(verticalsealerfront_5)::integer AS "front5", 
                   AVG(verticalsealerfront_6)::integer AS "front6", 
                   AVG(verticalsealerfront_7)::integer AS "front7", 
                   AVG(verticalsealerfront_8)::integer AS "front8", 
                   AVG(verticalsealerfront_9)::integer AS "front9", 
                   AVG(verticalsealerfront_10)::integer AS "front10", 
                   AVG(verticalsealerfront_11)::integer AS "front11", 
                   AVG(verticalsealerfront_12)::integer AS "front12", 
                   AVG(verticalsealerfront_13)::integer AS "front13", 
                   AVG(verticalsealerrear_1)::integer AS "rear1", 
                   AVG(verticalsealerrear_2)::integer AS "rear2", 
                   AVG(verticalsealerrear_3)::integer AS "rear3", 
                   AVG(verticalsealerrear_4)::integer AS "rear4", 
                   AVG(verticalsealerrear_5)::integer AS "rear5", 
                   AVG(verticalsealerrear_6)::integer AS "rear6", 
                   AVG(verticalsealerrear_7)::integer AS "rear7", 
                   AVG(verticalsealerrear_8)::integer AS "rear8", 
                   AVG(verticalsealerrear_9)::integer AS "rear9", 
                   AVG(verticalsealerrear_10)::integer AS "rear10", 
                   AVG(verticalsealerrear_11)::integer AS "rear11", 
                   AVG(verticalsealerrear_12)::integer AS "rear12", 
                   AVG(verticalsealerrear_13)::integer AS "rear13",
                   AVG(hor_sealer_rear)::integer AS "hor_rear1", 
                   AVG(hor_sealer_front)::integer AS "hor_front1", 
                   AVG(hopper_1_level)::integer AS "hopper_1",
                   AVG(hopper_2_level)::integer AS "hopper_2"
            FROM {table_name}
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY time_bucket
            ORDER BY time_bucket DESC"""
        self.cur.execute(mitsubishi_query)

        # Fetch all rows
        rows = self.cur.fetchall()

        # Get column names
        colnames = [desc[0] for desc in self.cur.description]

        # Convert rows to a dictionary
        data = defaultdict(list)
        for row in rows:
            for col, val in zip(colnames, row):
                if col == 'time_bucket':
                    data[col].append(str(val))
                else:
                    data[col].append(val)

        return data

    def fetch_data_from_table(self, table_name):
        # Define the SQL query with dynamic table name
        query = f"""
            SELECT time_bucket('1 seconds',"timestamp") as time_bucket, 
                   AVG(ver_sealer_front_1_temp)::integer AS "front1", 
                   AVG(ver_sealer_front_2_temp)::integer AS "front2", 
                   AVG(ver_sealer_front_3_temp)::integer AS "front3", 
                   AVG(ver_sealer_front_4_temp)::integer AS "front4", 
                   AVG(ver_sealer_front_5_temp)::integer AS "front5", 
                   AVG(ver_sealer_front_6_temp)::integer AS "front6", 
                   AVG(ver_sealer_front_7_temp)::integer AS "front7", 
                   AVG(ver_sealer_front_8_temp)::integer AS "front8", 
                   AVG(ver_sealer_front_9_temp)::integer AS "front9", 
                   AVG(ver_sealer_front_10_temp)::integer AS "front10", 
                   AVG(ver_sealer_front_11_temp)::integer AS "front11", 
                   AVG(ver_sealer_front_12_temp)::integer AS "front12", 
                   AVG(ver_sealer_front_13_temp)::integer AS "front13", 
                   AVG(ver_sealer_rear_1_temp)::integer AS "rear1", 
                   AVG(ver_sealer_rear_2_temp)::integer AS "rear2", 
                   AVG(ver_sealer_rear_3_temp)::integer AS "rear3", 
                   AVG(ver_sealer_rear_4_temp)::integer AS "rear4", 
                   AVG(ver_sealer_rear_5_temp)::integer AS "rear5", 
                   AVG(ver_sealer_rear_6_temp)::integer AS "rear6", 
                   AVG(ver_sealer_rear_7_temp)::integer AS "rear7", 
                   AVG(ver_sealer_rear_8_temp)::integer AS "rear8", 
                   AVG(ver_sealer_rear_9_temp)::integer AS "rear9", 
                   AVG(ver_sealer_rear_10_temp)::integer AS "rear10", 
                   AVG(ver_sealer_rear_11_temp)::integer AS "rear11", 
                   AVG(ver_sealer_rear_12_temp)::integer AS "rear12", 
                   AVG(ver_sealer_rear_13_temp)::integer AS "rear13",
                   AVG(hor_sealer_rear_1_temp)::integer AS "hor_rear1", 
                   AVG(hor_sealer_front_1_temp)::integer AS "hor_front1", 
                   AVG(hopper_1_level)::integer AS "hopper_1",
                   AVG(hopper_2_level)::integer AS "hopper_2"
            FROM {table_name} 
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY time_bucket
            ORDER BY time_bucket DESC
        """
        
        # Execute the query
        self.cur.execute(query)

        # Fetch all rows
        rows = self.cur.fetchall()

        # Get column names
        colnames = [desc[0] for desc in self.cur.description]

        # Convert rows to a dictionary
        data = defaultdict(list)
        for row in rows:
            for col, val in zip(colnames, row):
                if col == 'time_bucket':
                    data[col].append(str(val))
                else:
                    data[col].append(val)

        return data

    def fetch_and_insert_data(self):
        all_data = {}

        # Fetch data from each table
        for table_name in self.table_names:
            data = self.fetch_data_from_table(table_name)
            all_data[table_name] = data
        mitsu = ['mc25','mc26']
        for table_name in mitsu:
            data = self.fetch_data_loop4(table_name)
            all_data[table_name] = data
        # Insert data into historical_graphs table with each table's data in separate columns
        insert_query = """INSERT INTO historical_graphs (last_updated, mc17, mc18, mc19, mc20, mc21, mc22,mc25,mc26) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s);"""
        self.cur.execute(insert_query, (
            str(datetime.datetime.now()), 
            json.dumps(all_data.get("mc17", {})),
            json.dumps(all_data.get("mc18", {})),
            json.dumps(all_data.get("mc19", {})),
            json.dumps(all_data.get("mc20", {})),
            json.dumps(all_data.get("mc21", {})),
            json.dumps(all_data.get("mc25", {})),
            json.dumps(all_data.get("mc26", {})),
            json.dumps(all_data.get("mc22", {}))
        ))

        # Commit the transaction
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

# Usage
table_names = ["mc17", "mc18", "mc19", "mc20", "mc21", "mc22"]

inserter = HistoricalGraphsInserter(table_names=table_names)

while True:
    try :
        inserter.fetch_and_insert_data()
    except Exception as e:
        print(e)
        inserter.close()

