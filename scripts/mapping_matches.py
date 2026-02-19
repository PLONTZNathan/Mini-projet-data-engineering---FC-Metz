"""
build_matches_mapping.py
------------------------
Builds the match ID mapping table between :
  - StatsBomb   (match_id)
  - SkillCorner (id)

Strategy:
  1. Load teams_mapping to convert sb_id <-> sc_id
  2. For each SB match, retrieve the sc_id of both teams
  3. Find the SC match with the same teams and date

OUTPUT
------
data/raw/mapping/matches_mapping.csv
"""

import json
import csv
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent

SB_MATCHES_JSON   = ROOT / "data" / "raw" / "statsbomb"  / "matches" / "ligue1_matches_2025_2026.json"
SC_MATCHES_JSON   = ROOT / "data" / "raw" / "skillcorner" / "matches" / "ligue1_matches_2025_2026.json"
TEAMS_MAPPING_CSV = ROOT / "data" / "raw" / "mapping" / "teams_mapping.csv"

OUTPUT_DIR  = ROOT / "data" / "raw" / "mapping"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "matches_mapping.csv"

FIELDNAMES = ["sb_id", "sc_id", "date"]

# -----------------------------------------------------------------------------
# LOADERS
# -----------------------------------------------------------------------------

def load_teams_mapping():
    """Returns two dicts: sb_id -> sc_id and sc_id -> sb_id."""
    sb_to_sc = {}
    with open(TEAMS_MAPPING_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sb_to_sc[int(row["sb_id"])] = int(row["sc_id"])
    print(f"[TEAMS] {len(sb_to_sc)} teams loaded")
    return sb_to_sc


def load_statsbomb_matches():
    data = json.loads(SB_MATCHES_JSON.read_text(encoding="utf-8"))
    print(f"[SB] {len(data)} matches")
    return data


def load_skillcorner_matches():
    data = json.loads(SC_MATCHES_JSON.read_text(encoding="utf-8"))
    # Index: (home_sc_id, away_sc_id, date_str) -> match
    index = {}
    for m in data:
        date = datetime.fromisoformat(m["date_time"].replace("Z", "+00:00")).date()
        key  = (m["home_team"]["id"], m["away_team"]["id"], str(date))
        index[key] = m
    print(f"[SC] {len(data)} matches")
    return index


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("BUILD MATCHES MAPPING")
    print("=" * 60)

    sb_to_sc   = load_teams_mapping()
    sb_matches = load_statsbomb_matches()
    sc_index   = load_skillcorner_matches()

    rows      = []
    not_found = []

    for m in sb_matches:
        sb_match_id  = m["match_id"]
        date         = m["match_date"]  # "YYYY-MM-DD"

        # Get sb_id for both teams from the teams mapping
        home_sb_id = next(
            (sid for sid, name in _sb_name_to_id(sb_to_sc, m["home_team"]).items()),
            None
        )
        away_sb_id = next(
            (sid for sid, name in _sb_name_to_id(sb_to_sc, m["away_team"]).items()),
            None
        )

        # Convert to sc_id via mapping
        home_sc_id = sb_to_sc.get(home_sb_id)
        away_sc_id = sb_to_sc.get(away_sb_id)

        if home_sc_id is None or away_sc_id is None:
            not_found.append({
                "sb_id": sb_match_id,
                "reason": f"unmapped team: {m['home_team']} / {m['away_team']}",
            })
            continue

        # Look up in SC index
        key = (home_sc_id, away_sc_id, date)
        sc_match = sc_index.get(key)

        if sc_match is None:
            not_found.append({
                "sb_id": sb_match_id,
                "reason": f"match not found in SC: {m['home_team']} vs {m['away_team']} le {date}",
            })
            continue

        rows.append({
            "sb_id": sb_match_id,
            "sc_id": sc_match["id"],
            "date":  date,
        })

    # Write CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    # Display
    print(f"\n{'SB ID':<12} {'SC ID':<12} {'Date'}")
    print("-" * 38)
    for r in rows:
        print(f"{str(r['sb_id']):<12} {str(r['sc_id']):<12} {r['date']}")

    print(f"\nMatches mapped   : {len(rows)}/{len(sb_matches)}")
    if not_found:
        print(f"Not found      : {len(not_found)}")
        for nf in not_found:
            print(f"  [SB {nf['sb_id']}] {nf['reason']}")

    print(f"\nOutput : {OUTPUT_FILE}")
    print("=" * 60)


# -----------------------------------------------------------------------------
# HELPER
# -----------------------------------------------------------------------------

def _sb_name_to_id(sb_to_sc, team_name):
    """
    Returns the sb_id for a given SB team name.
    Cache is built on first call.
    """
    if not hasattr(_sb_name_to_id, "_cache"):
        # Build name -> sb_id cache from CSV
        _sb_name_to_id._cache = {}
        with open(TEAMS_MAPPING_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                _sb_name_to_id._cache[row["sb_name"]] = int(row["sb_id"])
    sid = _sb_name_to_id._cache.get(team_name)
    return {sid: team_name} if sid else {}


if __name__ == "__main__":
    main()