def check_starvation(cld_status_timestamps):
    while True:
        current_time = time.time()
        for machine, timestamp in list(cld_status_timestamps.items()):
            if current_time - timestamp >= 10:  # Check if 10 seconds passed
                query = """
                MATCH (m:Machine {name: $machine_name})
                SET m.cld_status = 2
                """
                memgraph.execute(query, {"machine_name": machine})
                print(f"Updated {machine}: CLD Status changed to 2 after 10 seconds")
                del cld_status_timestamps[machine]  # Remove from tracking
        time.sleep(1)  # Check every second