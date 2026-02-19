import csv

with open("data/raw/mapping/players_mapping.csv", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

from collections import Counter
counts = Counter(r["sb_id"] for r in rows if r["sb_id"])
doublons = {k: v for k, v in counts.items() if v > 1}
print(f"{len(doublons)} sb_id en doublon")
for sb_id, count in list(doublons.items())[:5]:
    print(f"  sb_id={sb_id} apparaît {count} fois")