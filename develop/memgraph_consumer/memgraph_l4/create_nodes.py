import json
from gqlalchemy import Memgraph

def fetch_machine_names(db):
    return [m['name'] for m in db.execute_and_fetch("""
        MATCH (m:kafkaData_l4)
        RETURN m.name as name;
    """)]

def save_node_names():
    memgraph = Memgraph()
    machine_names = fetch_machine_names(memgraph)

    data = {"node": machine_names}

    with open("graph_relations_test.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(machine_names)} node names to graph_relations.json")

if __name__ == "__main__":
    save_node_names()

