#!/usr/bin/env python3
"""
Minimal test script to debug database query hanging issue.
Run this directly on the server to isolate the problem.
"""
import psycopg2
import sys

# Database config - same as config.json
DB_CONFIG = {
    "host": "192.168.0.185",
    "database": "hul",
    "user": "postgres",
    "password": "ai4m2024",
    "port": 5432,
    "connect_timeout": 10,
}

def test_query(table_name, machine_id):
    """Test the exact query the tower light script uses"""
    print(f"\n{'='*60}")
    print(f"Testing: {table_name} for {machine_id}")
    print('='*60, flush=True)
    
    try:
        print("1. Connecting to database...", flush=True)
        conn = psycopg2.connect(**DB_CONFIG)
        print("   ✅ Connected!", flush=True)
        
        print("2. Creating cursor...", flush=True)
        cur = conn.cursor()
        print("   ✅ Cursor created!", flush=True)
        
        # NOTE: Commenting out SET statement_timeout to test if it's the issue
        # print("3. Setting statement_timeout...", flush=True)
        # cur.execute("SET statement_timeout = '10s';")
        # print("   ✅ Timeout set!", flush=True)
        
        query = f"""
            SELECT machine_status
            FROM {table_name}
            WHERE machine_status->>'id' = %s;
        """
        
        print(f"3. Executing query...", flush=True)
        print(f"   Query: SELECT machine_status FROM {table_name} WHERE machine_status->>'id' = '{machine_id}'", flush=True)
        cur.execute(query, (machine_id,))
        print("   ✅ Query executed!", flush=True)
        
        print("4. Fetching result...", flush=True)
        result = cur.fetchone()
        print("   ✅ Result fetched!", flush=True)
        
        if result:
            machine_status = result[0]
            active_tp = machine_status.get("active_tp", {})
            active_count = sum(1 for tp_id in active_tp.keys() if tp_id != "TP01")
            print(f"\n   📊 Result: Found machine with {active_count} active TPs (excluding TP01)")
        else:
            print("\n   ⚠️  No result found for this machine_id")
        
        print("5. Closing connection...", flush=True)
        cur.close()
        conn.close()
        print("   ✅ Connection closed!", flush=True)
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"   ❌ Database connection error: {e}", flush=True)
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}", flush=True)
        return False


if __name__ == "__main__":
    print("\n" + "="*60)
    print("  Tower Light Database Query Test")
    print("  Testing if the query itself hangs")
    print("="*60)
    
    # Test both tables
    tests = [
        ("field_overview_tp_status_l3", "MC 17"),
        ("field_overview_tp_status_l4", "MC 25"),
    ]
    
    for table, machine in tests:
        success = test_query(table, machine)
        if not success:
            print(f"\n❌ Test failed for {table}")
            sys.exit(1)
    
    print("\n" + "="*60)
    print("  ✅ ALL TESTS PASSED!")
    print("="*60 + "\n")
