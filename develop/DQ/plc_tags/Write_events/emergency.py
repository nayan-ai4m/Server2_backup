
import psycopg2

def cleanup_events():
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

        # Create list of zones to clean
        loop3_zones = [f"Loop3_mc{i}" for i in range(17, 31)]  # mc17 to mc22
        zone_list = ",".join([f"'{zone}'" for zone in loop3_zones])

        # Delete query
        query = f"""
        DELETE FROM event_table 
        WHERE zone IN ({zone_list})
        RETURNING zone;
        """

        # Execute delete
        cursor.execute(query)
        deleted_rows = cursor.fetchall()
        
        # Commit the transaction
        conn.commit()

        # Print results
        print("\nCleanup Results:")
        print("="*50)
        for zone in loop3_zones:
            count = sum(1 for row in deleted_rows if row[0] == zone)
            print(f"Deleted {count} events from {zone}")
        print(f"\nTotal events deleted: {len(deleted_rows)}")

    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Starting cleanup of Loop3 machine events...")
    user_input = input("Are you sure you want to delete all Loop3 machine events? (yes/no): ")
    
    if user_input.lower() == 'yes':
        cleanup_events()
        print("\nCleanup completed!")
    else:
        print("\nCleanup cancelled.")
