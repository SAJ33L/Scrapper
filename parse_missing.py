import csv
with open("output_verify_200.csv", newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

missing = []
for r in rows:
    if (r.get("DMI Sales Price (€)") in ("N/A", "")) or \
       (r.get("DentalSky Sales Price (£)") in ("N/A", "")) or \
       (r.get("Dontalia Sales Price (€)") in ("N/A", "")):
        missing.append(r)

for s in missing[:3]:
    print(f"Name: {s.get('Name')}")
    print(f"Code: {s.get('Code')}")
    print(f"Part Number: {s.get('Part Number')}")
    print(f"DMI IE: {s.get('DMI Sales Price (€)')}")
    print(f"DentalSky: {s.get('DentalSky Sales Price (£)')}")
    print(f"Dontalia: {s.get('Dontalia Sales Price (€)')}")
    print("---")
