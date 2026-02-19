"""
check_360.py
------------
Verifie si la licence StatsBomb inclut la 360 data.
"""

import os
import json
import warnings
from pathlib import Path
from dotenv import load_dotenv
from statsbombpy import sb

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

load_dotenv()
CREDS = {
    "user":   os.getenv("STATSBOMB_USERNAME"),
    "passwd": os.getenv("STATSBOMB_PASSWORD"),
}

MATCHES_JSON = Path("data/raw/statsbomb/matches/ligue1_matches_2025_2026.json")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():

    # Charger les matchs deja recuperes
    if not MATCHES_JSON.exists():
        print("[ERREUR] Fichier matches introuvable, lance d'abord ingest_statsbomb.py")
        return

    with open(MATCHES_JSON, encoding="utf-8") as f:
        matches = json.load(f)

    print(f"{len(matches)} matchs joues disponibles")
    print("-" * 50)

    # Tester sur les 3 premiers matchs
    for match in matches[:3]:
        match_id   = match["match_id"]
        home       = match["home_team"]
        away       = match["away_team"]
        status_360 = match.get("match_status_360", "inconnu")

        print(f"\nMatch : {home} vs {away} (id={match_id})")
        print(f"  match_status_360 : {status_360}")

        if status_360 != "available":
            print(f"  -> 360 NON disponible pour ce match")
            continue

        # Tenter de recuperer les frames 360
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                frames = sb.frames(match_id=match_id, creds=CREDS)

            if frames is None or len(frames) == 0:
                print(f"  -> 360 vide ou non accessible")
            else:
                print(f"  -> 360 OK ({len(frames)} frames)")
                print(f"  -> Colonnes : {list(frames.columns)}")

        except Exception as e:
            print(f"  -> ERREUR : {e}")

    print("\n" + "-" * 50)
    print("\nResume match_status_360 sur tous les matchs :")
    from collections import Counter
    statuses = Counter(m.get("match_status_360", "inconnu") for m in matches)
    for status, count in statuses.items():
        print(f"  {status} : {count} matchs")


if __name__ == "__main__":
    main()