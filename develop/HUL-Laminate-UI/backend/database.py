from fastapi import HTTPException
import psycopg2
import json
import traceback

with open("db_config.json", "r") as config_file:
    config = json.load(config_file)

DATABASE = config["database"]
DATABASE2 = config["database1"]
# SERVER2_DB = config["server_2_db"]


def get_db():
    try:
        conn = psycopg2.connect(**DATABASE)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to main database: {e}")
        print(traceback.format_exc())
        return None, None


def get_db2():
    try:
        conn2 = psycopg2.connect(**DATABASE2)
        cursor2 = conn2.cursor()
        return conn2, cursor2
    except Exception as e:
        print(f"Error connecting to secondary database: {e}")
        print(traceback.format_exc())
        return None, None


def get_server2_db():
    try:
        conn3 = psycopg2.connect(**SERVER2_DB)
        cursor3 = conn3.cursor()
        return conn3, cursor3
    except Exception as e:
        print(f"Error connecting to Server 2 database: {e}")
        print(traceback.format_exc())
        return None, None


def close_db(conn, cursor):
    """Close the main database connection and cursor."""
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def close_db2(conn2, cursor2):
    """Close the secondary database connection and cursor."""
    if cursor2:
        cursor2.close()
    if conn2:
        conn2.close()


def close_server2_db(conn3, cursor3):
    """Close the Server 2 database connection and cursor."""
    if cursor3:
        cursor3.close()
    if conn3:
        conn3.close()
