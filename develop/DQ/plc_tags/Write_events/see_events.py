
import psycopg2
from tabulate import tabulate
from datetime import datetime

def view_events():
    # Database configuration
    db_config = {
        'dbname': 'hul',
        'user': 'postgres',
        'password': 'ai4m2024',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Create list of zones
        loop3_zones = [f"Loop3_mc{i}" for i in range(17, 31)]  # mc17 to mc22
        zone_list = ",".join([f"'{zone}'" for zone in loop3_zones])

        # Select query with ordering
        query = f"""
        SELECT 
            timestamp,
            event_id,
            zone,
            event_type,
            resolution_time,
            CASE 
                WHEN resolution_time IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (resolution_time - timestamp))/60 
                ELSE NULL 
            END as downtime_minutes
        FROM event_table 
        WHERE zone IN ({zone_list})
        ORDER BY timestamp DESC;
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            # Prepare data for tabulate
            table_data = []
            for row in rows:
                timestamp = row[0].strftime('%Y-%m-%d %H:%M:%S')
                resolution = row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else 'Not Resolved'
                downtime = f"{row[5]:.2f} mins" if row[5] else 'Ongoing'
                
                table_data.append([
                    timestamp,
                    row[1],  # event_id
                    row[2],  # zone
                    row[3],  # event_type
                    resolution,
                    downtime
                ])

            # Print results using tabulate
            headers = ['Timestamp', 'Event ID', 'Zone', 'Event Type', 'Resolution Time', 'Downtime']
            print("\nLoop3 Machine Events:")
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
            
            # Print summary
            print("\nSummary:")
            print("="*50)
            total_events = len(rows)
            resolved_events = sum(1 for row in rows if row[4] is not None)
            unresolved_events = total_events - resolved_events
            
            print(f"Total Events: {total_events}")
            print(f"Resolved Events: {resolved_events}")
            print(f"Unresolved Events: {unresolved_events}")
            
            # Summary by zone
            print("\nEvents by Zone:")
            for zone in loop3_zones:
                zone_events = sum(1 for row in rows if row[2] == zone)
                if zone_events > 0:
                    print(f"{zone}: {zone_events} events")

        else:
            print("\nNo events found for Loop3 machines.")

    except Exception as e:
        print(f"Error retrieving events: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Retrieving Loop3 machine events...")
    view_events()
