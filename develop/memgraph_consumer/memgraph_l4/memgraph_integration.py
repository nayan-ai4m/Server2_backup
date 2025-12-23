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
        CREATE CONSTRAINT ON (m:zone_l4) ASSERT m.name IS UNIQUE;
        """)
        self.db.execute("""
        CREATE INDEX ON :zone_l4(status);
        """)
    
    def add_zone(self, name, **kwargs):
        """
        Add a zone or infeed node to the graph.

        Args:
            name (str): Node name.
            node_type (str): 'zone' or 'infeed'.
            kwargs: Optional property overrides.
            ETS: Estimated Time to Starvation
        """
        query = """
        MERGE (m:zone_l4 {
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
        MATCH (m1:zone_l4 {name: $zone1}), (m2:zone_l4 {name: $zone2})
        MERGE (m1)-[:AFFECTS]->(m2)
        """, {"zone1": zone1, "zone2": zone2})
        
    def build_graph(self):
        """
        Create nodes and relationships based on the provided diagram.
        """
        nodes = self.load_json("graph_relations.json")["nodes"]
        # edges = self.load_json("graph_relations.json")["edges"]
        
        # Add nodes with correct property defaults
        for node in nodes:
            self.add_zone(
                name=node
            )
        
        # # Add edges (dependencies)
        # for zone1, zone2 in edges:
        #     self.add_dependency(zone1, zone2)

        print("Graph successfully created with nodes and relationships.")

if __name__ == "__main__":
    memgraph = Memgraph()
    #memgraph.drop_database() 
    graph = zoneGraph()
    graph.build_graph()


