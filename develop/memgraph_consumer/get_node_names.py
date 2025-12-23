from gqlalchemy import Memgraph

def fetch_machine_names(db):
    return [m['name'] for m in db.execute_and_fetch("""
        MATCH (m:kafkaData)
        RETURN m.name as name;
    """)]

db = Memgraph()
machine_names = fetch_machine_names(db)
print(machine_names)
