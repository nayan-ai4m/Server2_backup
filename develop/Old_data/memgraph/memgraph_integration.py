from gqlalchemy import Memgraph
import json

class zoneGraph:
    def __init__(self):
        self.db = Memgraph()
        self.define_schema()
    
    def load_json(self, filename):
        """Load JSON data from a file."""
        with open(filename, "r") as file:
            return json.load(file)
        
    def define_schema(self):
        """Define graph schema with constraints."""
        self.db.execute("""
        CREATE CONSTRAINT ON (m:zone) ASSERT m.name IS UNIQUE;
        """)
        self.db.execute("""
        CREATE INDEX ON :zone(status);
        """)
    
    def add_zone(self, name, node_type="zone", **kwargs):
        """
        Add a zone or infeed node to the graph.

        Args:
            name (str): Node name.
            node_type (str): 'zone' or 'infeed'.
            kwargs: Optional property overrides.
            ETS: Estimated Time to Starvation
        """
        query = """
        MERGE (m:zone {
            name: $name,
            source_id: $source_id,
            cld_count: $cld_count,
            orientation: $orientation,
            status: $status,
            last_updated: $last_updated
        })
        """
        params = {
            "name": name,
            "source_id": kwargs.get("source_id", ""),
            "cld_count": kwargs.get("cld_count", ""),
            "orientation": kwargs.get("orientation", ""),
            "status": kwargs.get("status", ""),
            "last_updated": kwargs.get("last_updated", "")
        }

        self.db.execute(query, params)
    
    def add_dependency(self, zone1, zone2):
        """
        Define a relationship where zone1 affects zone2.
        """
        self.db.execute("""
        MATCH (m1:zone {name: $zone1}), (m2:zone {name: $zone2})
        MERGE (m1)-[:AFFECTS]->(m2)
        """, {"zone1": zone1, "zone2": zone2})
    
    def add_machine(self, name, node_type="machine", **kwargs):
        query = """
        MERGE (m:Machine {
            name: $name,
            status: $status
        })
        """
        params = {
            "name": name,
            "status": kwargs.get("status", "ideal")
        }
        self.db.execute(query, params)
    
    def add_new_machines(self):
        new_machines = self.load_json("graph_relations.json")["new_machines"]
        dependencies = self.load_json("graph_relations.json")["dependencies"]

        for machine in new_machines:
            self.add_machine(name=machine)
        
        for machine1, machine2 in dependencies:
            self.add_dependency(machine1, machine2)

        print("New machines added and connected successfully.")
        
    def build_graph(self):
        """
        Create nodes and relationships based on the provided diagram.
        """
        nodes = self.load_json("graph_relations.json")["nodes"]
        edges = self.load_json("graph_relations.json")["edges"]
        
        # Add nodes with correct property defaults
        for node in nodes:
            if "_infeed" in node:
                self.add_zone(
                    name=node,
                    node_type="_infeed"
                )
            else:
                self.add_zone(
                    name=node,
                    node_type="zone"
                )
        
        # Add edges (dependencies)
        for zone1, zone2 in edges:
            self.add_dependency(zone1, zone2)

        print("Graph successfully created with nodes and relationships.")

    def insert_status_rules(self):
        """Insert status rules into the database from JSON."""
        truth_table = self.load_json("truth_table.json")["status_rules"]

        for rule in truth_table:
            self.db.execute("""
            MERGE (:StatusRule {vffs_status: $vffs, conveyor_status: $conveyor, cld: $cld, orientation: $orientation, status: $status})
            """, {
                "vffs": rule[0], 
                "conveyor": rule[1], 
                "cld": rule[2], 
                "orientation": rule[3], 
                "status": rule[4]
            })

        print("Status rules successfully inserted.")

if __name__ == "__main__":
    memgraph = Memgraph()
    memgraph.drop_database() 
    graph = zoneGraph()
    graph.build_graph()
    # graph.add_new_machines()
    # graph.insert_status_rules()
