import psycopg2
import json

with open('db_config.json', 'r') as f:
    config = json.load(f)

for db_key in ['database', 'database1', 'server_2_db']:
    db_config = config[db_key]
    print(f"Testing {db_key}: {db_config.get('host')}:{db_config.get('port', 5432)}")
    try:
        conn = psycopg2.connect(**db_config)
        print("Connected successfully!")
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
