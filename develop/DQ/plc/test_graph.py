import psycopg2
from collections import defaultdict
import json
import datetime
# Connect to your PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="hul",
    user="postgres",
    password="ai4m2024"
)

# Create a cursor object
cur = conn.cursor()

# Define the SQL query
query = """
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
           AVG(ver_sealer_rear_13_temp)::integer AS "rear13" 
    FROM mc18 
    WHERE timestamp > NOW() - INTERVAL '5 minutes'
    GROUP BY time_bucket
    ORDER BY time_bucket DESC
"""
insert_query = """INSERT INTO historical_graphs(last_updated,mc17) VALUES (%s,%s);"""


# Execute the query
cur.execute(query)

# Fetch all rows
rows = cur.fetchall()

# Get column names
colnames = [desc[0] for desc in cur.description]

# Convert rows to a dictionary
data = defaultdict(list)
for row in rows:
    for col, val in zip(colnames, row):
        if col == 'time_bucket':
            data[col].append(str(val))
        else:
            data[col].append(val)
cur.execute(insert_query, (str(datetime.datetime.now()),json.dumps(dict(data))))
# Close the cursor and connection
conn.commit()
cur.close()
conn.close()

# Print or return data dictionary

