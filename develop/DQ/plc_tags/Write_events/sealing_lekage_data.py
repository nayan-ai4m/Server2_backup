import psycopg2
from psycopg2 import pool
import random

class TableMonitor:
    def __init__(self):
        self.pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,
            dbname="hul",
            user="postgres",
            password="ai4m2024",
            host="localhost"
        )
        
        self.tables = ['mc17', 'mc18', 'mc19', 'mc20', 'mc21', 'mc22']
        self.targets = {table: self.get_new_target() for table in self.tables}

    def get_new_target(self):
        return round(random.uniform(0.75, 0.9), 4)

    def get_latest_record(self, cursor, table):
        query = f"""
            SELECT status, spare2, timestamp 
            FROM {table} 
            ORDER BY timestamp DESC 
            LIMIT 1
        """
        cursor.execute(query)
        return cursor.fetchone()

    def update_spare2(self, cursor, table, new_value, timestamp):
        query = f"""
            UPDATE {table} 
            SET spare2 = %s 
            WHERE timestamp = %s
        """
        cursor.execute(query, (new_value, timestamp))
        print(f"Updated {table}: status={self.current_status}, spare2={new_value:.4f}")

    def process_table(self, cursor, table):
        result = self.get_latest_record(cursor, table)
        if not result:
            return
        
        status, spare2, timestamp = result
        self.current_status = status

        # If status is not 1, set spare2 to 0
        if status != 1:
            if spare2 != 0:
                self.update_spare2(cursor, table, 0, timestamp)
            return

        # Status is 1, handle random number logic
        current_value = spare2 if spare2 is not None else 0
        
        # If we've reached current target, get new one
        if abs(current_value - self.targets[table]) < 0.04:
            self.targets[table] = self.get_new_target()
        
        # Calculate next value moving toward target
        if current_value < self.targets[table]:
            new_value = round(min(current_value + 0.04, self.targets[table]), 4)
        else:
            new_value = round(max(current_value - 0.04, self.targets[table]), 4)

        # Update if value has changed
        if abs(new_value - current_value) >= 0.0001:
            self.update_spare2(cursor, table, new_value, timestamp)

    def run(self):
        print("Monitoring started...")
        while True:
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cursor:
                    for table in self.tables:
                        try:
                            self.process_table(cursor, table)
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            print(f"Error: {str(e)}")
            finally:
                self.pool.putconn(conn)

if __name__ == "__main__":
    monitor = TableMonitor()
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("\nStopping monitor...")
