import json

with open("check_possibility.json", "r") as file:
    check_possibility = json.load(file)

print(check_possibility["mc19_infeed"].get("timestamp"))