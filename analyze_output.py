import json

with open("output.json", "r", encoding="utf-8") as f:
    data = json.load(f)
tech_set = set()
for site in data:
    for tech in site["technologies"]:
        tech_set.add(tech["name"])
print("Număr tehnologii unice:", len(tech_set))
print("\nLista tehnologii:")
for t in sorted(tech_set):
    print("-", t)
