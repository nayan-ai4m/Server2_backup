import json
from kafka import KafkaConsumer
from gqlalchemy import Memgraph

# ---------- Reuse your functions ----------
def prepare_machine_data(message):
    timestamp = message.get("@timestamp")
    sensor_id = message.get("sensorId")
    objects = message.get("objects", [])
    roi = message.get("roi", {})

    machine_data = {}
    for i in roi:
        if roi[i] == 0:
            machine_data[i] = {
                "orientation": "None",
                "source_id": sensor_id,
                "num_boxes": 0,
                "cld_status": "Empty",
                "timestamp": timestamp
            }
        else:
            orientation_value = "None"
            ori_label = "None"
            for obj in objects:
                fields = obj.split("|")
                if len(fields) < 9:
                    continue
                if fields[8] == i:
                    if i == "pre_taping_l3" or i == "pre_taping_l4":
                        ori_label = fields[-1]

                    label_lower = ori_label.lower()
                    if 'bad' in label_lower:
                        orientation_value = "bad_orientation"
                        break
                    elif 'good' in label_lower and orientation_value == "None":
                        orientation_value = "good_orientation"

            machine_data[i] = {
                "orientation": orientation_value,
                "source_id": sensor_id,
                "num_boxes": roi[i],
                "cld_status": "Box Present",
                "timestamp": timestamp
            }

    return machine_data


def create_cypher_queries_l3(machine_data):
    queries = []
    for machine_name, details in machine_data.items():
        query = """
        MERGE (m:kafkaData {name: $machine_name})
        SET m.source_id = $source_id,
            m.orientation = $orientation,
            m.num_boxes = $num_boxes,
            m.cld_status = $cld_status,
            m.last_updated = $timestamp
        """
        queries.append((query, details | {"machine_name": machine_name}))
    return queries


def create_cypher_queries_l4(machine_data):
    queries = []
    for machine_name, details in machine_data.items():
        query = """
        MERGE (m:kafkaData_l4 {name: $machine_name})
        SET m.source_id = $source_id,
            m.orientation = $orientation,
            m.num_boxes = $num_boxes,
            m.cld_status = $cld_status,
            m.last_updated = $timestamp
        """
        queries.append((query, details | {"machine_name": machine_name}))
    return queries


# ---------- Kafka + Memgraph script ----------
def main():
    # Connect to Kafka
    consumer = KafkaConsumer(
        "kafkaData",  # topic name
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="memgraph-consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    # Connect to Memgraph
    memgraph = Memgraph("100.103.195.124", 7687)

    print("Listening for Kafka messages...")
    for message in consumer:
        msg = message.value
        print(f"Received: {msg}")

        # Process message into structured data
        machine_data = prepare_machine_data(msg)

        # Separate L3 and L4
        l3_data = {k: v for k, v in machine_data.items() if "l3" in k}
        l4_data = {k: v for k, v in machine_data.items() if "l4" in k}

        queries = []
        if l3_data:
            queries.extend(create_cypher_queries_l3(l3_data))
        if l4_data:
            queries.extend(create_cypher_queries_l4(l4_data))

        # Run queries in Memgraph
        for query, params in queries:
            memgraph.execute(query, params)
            print(f"Inserted into Memgraph: {params}")


if __name__ == "__main__":
    main()

