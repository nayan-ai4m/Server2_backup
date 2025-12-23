import psycopg2
import json
from psycopg2.extras import DictCursor

# Database connection details
conn = psycopg2.connect(
    dbname="hul",
    user="postgres",
    password="ai4m2024",
    host="localhost",
    port="5432"
)

# Define the query
query = """
    SELECT time_bucket('1 seconds', "timestamp") as time_bucket, 
           AVG(ver_sealer_front_1_temp) AS "front1", AVG(ver_sealer_front_2_temp) AS "front2", 
           AVG(ver_sealer_front_3_temp) AS "front3", AVG(ver_sealer_front_4_temp) AS "front4", 
           AVG(ver_sealer_front_5_temp) AS "front5", AVG(ver_sealer_front_6_temp) AS "front6", 
           AVG(ver_sealer_front_7_temp) AS "front7", AVG(ver_sealer_front_8_temp) AS "front8", 
           AVG(ver_sealer_front_9_temp) AS "front9", AVG(ver_sealer_front_10_temp) AS "front10", 
           AVG(ver_sealer_front_11_temp) AS "front11", AVG(ver_sealer_front_12_temp) AS "front12", 
           AVG(ver_sealer_front_13_temp) AS "front13", AVG(ver_sealer_rear_1_temp) AS "rear1", 
           AVG(ver_sealer_rear_2_temp) AS "rear2", AVG(ver_sealer_rear_3_temp) AS "rear3", 
           AVG(ver_sealer_rear_4_temp) AS "rear4", AVG(ver_sealer_rear_5_temp) AS "rear5", 
           AVG(ver_sealer_rear_6_temp) AS "rear6", AVG(ver_sealer_rear_7_temp) AS "rear7", 
           AVG(ver_sealer_rear_8_temp) AS "rear8", AVG(ver_sealer_rear_9_temp) AS "rear9", 
           AVG(ver_sealer_rear_10_temp) AS "rear10", AVG(ver_sealer_rear_11_temp) AS "rear11", 
           AVG(ver_sealer_rear_12_temp) AS "rear12", AVG(ver_sealer_rear_13_temp) AS "rear13"
    FROM mc17  
    WHERE timestamp > now() - '5 minutes'::interval
    GROUP BY time_bucket
    ORDER BY time_bucket DESC
"""

with conn.cursor(cursor_factory=DictCursor) as cursor:
    cursor.execute(query)
    rows = cursor.fetchall()  # Fetch all result rows

# Convert result to JSON
#json_result = json.dumps([dict(row) for row in rows], default=str)
for record in rows:
    print(record)
# Print or retur
# Close the database connection
conn.close()

