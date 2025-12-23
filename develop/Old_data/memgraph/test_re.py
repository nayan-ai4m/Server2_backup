import re
import json

with open("graph_relations.json", "r") as file:
    graph_relations = json.load(file)

nodes = graph_relations["nodes"]
print(nodes)

for i in nodes:
    if re.fullmatch(r"mc\d+_post_outfeed", i):
        print(i, 1)
    elif re.fullmatch(r"mc\d+_outfeed", i):
        print(i, 2)
    elif re.fullmatch(r"mc\d+_post_outfeed_L3C\d", i):
        print(i, 3)