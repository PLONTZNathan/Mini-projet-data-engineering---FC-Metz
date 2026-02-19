"""
ingest_statsbomb.py
-------------------
StatsBomb data ingestion -- Ligue 1 2025/2026.
"""

import os
import json
import time
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

COMPETITION_NAME = "Ligue 1"
SEASON_NAME      = "2025/2026"

DATA_DIR     = Path("data/raw/statsbomb")
DIR_PLAYERS  = DATA_DIR / "players"
DIR_TEAMS    = DATA_DIR / "teams"
DIR_MATCHES  = DATA_DIR / "matches"
DIR_EVENTS   = DATA_DIR / "events"
DIR_LINEUPS  = DATA_DIR / "lineups"

PLAYERS_JSON = DIR_PLAYERS / "ligue1_players_2025_2026.json"
TEAMS_JSON   = DIR_TEAMS   / "ligue1_teams_2025_2026.json"
MATCHES_JSON = DIR_MATCHES / "ligue1_matches_2025_2026.json"

# -----------------------------------------------------------------------------
# UTILITIES
# -----------------------------------------------------------------------------

def create_dirs():
    for d in [DIR_PLAYERS, DIR_TEAMS, DIR_MATCHES, DIR_EVENTS, DIR_LINEUPS]:
        d.mkdir(parents=True, exist_ok=True)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# -----------------------------------------------------------------------------
# COMPETITION
# -----------------------------------------------------------------------------

def get_competition_id(competition_name):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        competitions = sb.competitions(creds=CREDS)
    match = competitions[competitions["competition_name"] == competition_name]
    if match.empty:
        raise RuntimeError(f"Competition '{competition_name}' non trouvee")
    competition_id = int(match["competition_id"].values[0])
    print(f"[OK] {competition_name} -> competition_id = {competition_id}")
    return competition_id


def get_season_id(competition_name, season_name):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        competitions = sb.competitions(creds=CREDS)
    match = competitions[
        (competitions["competition_name"] == competition_name) &
        (competitions["season_name"]      == season_name)
    ]
    if match.empty:
        raise RuntimeError(f"Saison '{season_name}' non trouvee pour '{competition_name}'")
    season_id = int(match["season_id"].values[0])
    print(f"[OK] {competition_name} {season_name} -> season_id = {season_id}")
    return season_id

# -----------------------------------------------------------------------------
# PLAYERS
# -----------------------------------------------------------------------------

def fetch_players(competition_id, season_id):
    print(f"\nFetching players stats...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = sb.player_season_stats(
            competition_id=competition_id,
            season_id=season_id,
            creds=CREDS
        )
    save_json(PLAYERS_JSON, json.loads(df.to_json(orient="records")))
    print(f"[OK] {len(df)} players saved -> {PLAYERS_JSON}")
    return df

# -----------------------------------------------------------------------------
# TEAMS
# -----------------------------------------------------------------------------

def fetch_teams(competition_id, season_id):
    print(f"\nFetching teams stats...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = sb.team_season_stats(
            competition_id=competition_id,
            season_id=season_id,
            creds=CREDS
        )
    save_json(TEAMS_JSON, json.loads(df.to_json(orient="records")))
    print(f"[OK] {len(df)} teams saved -> {TEAMS_JSON}")
    return df

# -----------------------------------------------------------------------------
# MATCHES
# -----------------------------------------------------------------------------

def fetch_matches(competition_id, season_id):
    print(f"\nFetching matches...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = sb.matches(
            competition_id=competition_id,
            season_id=season_id,
            creds=CREDS
        )
    total = len(df)
    df = df[df["match_status"] == "available"].copy()
    df.reset_index(drop=True, inplace=True)
    save_json(MATCHES_JSON, json.loads(df.to_json(orient="records")))
    print(f"[OK] {len(df)}/{total} matchs joues saved -> {MATCHES_JSON}")
    return df

# -----------------------------------------------------------------------------
# LINEUPS
# -----------------------------------------------------------------------------

def fetch_lineups(matches_df):
    total   = len(matches_df)
    skipped = 0
    success = 0
    errors  = []

    print(f"\nFetching lineups pour {total} matchs...")
    print("-" * 60)

    start_total = time.time()

    for i, (_, match) in enumerate(matches_df.iterrows(), start=1):
        match_id     = int(match["match_id"])
        home         = match.get("home_team", "?")
        away         = match.get("away_team", "?")
        match_date   = match.get("match_date", "?")
        match_status = match.get("match_status", "")
        output_path  = DIR_LINEUPS / f"match_{match_id}_lineups.json"

        prefix = f"[{i:>3}/{total}] {match_date} {home} vs {away} (id={match_id})"

        # Match pas joue -> skip
        if match_status != "available":
            print(f"{prefix} -> SKIP (non joue, status={match_status})")
            skipped += 1
            continue

        # Fichier deja present -> skip
        if output_path.exists():
            print(f"{prefix} -> SKIP (deja present)")
            skipped += 1
            continue

        try:
            start = time.time()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                lineups = sb.lineups(match_id=match_id, creds=CREDS)

            # lineups est un dict {team_name: DataFrame}
            # on le convertit en liste pour le JSON
            lineups_data = []
            for team_name, df in lineups.items():
                lineups_data.append({
                    "team_name": team_name,
                    "lineup": json.loads(df.to_json(orient="records"))
                })

            save_json(output_path, lineups_data)
            elapsed = time.time() - start
            nb_joueurs = sum(len(t["lineup"]) for t in lineups_data)
            print(f"{prefix} -> OK ({nb_joueurs} joueurs, {elapsed:.1f}s)")
            success += 1
            time.sleep(0.3)

        except Exception as e:
            print(f"{prefix} -> ERREUR : {e}")
            errors.append({"match_id": match_id, "error": str(e)})

    total_elapsed = time.time() - start_total
    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)

    print("-" * 60)
    print(f"\nRapport lineups :")
    print(f"  Succes  : {success}")
    print(f"  Skipped : {skipped}")
    print(f"  Erreurs : {len(errors)}")
    print(f"  Temps   : {minutes}min {seconds}s")

    if errors:
        errors_path = DIR_LINEUPS / "fetch_errors.json"
        save_json(errors_path, errors)
        print(f"  Erreurs sauvegardees -> {errors_path}")

    print(f"\n[DONE] Lineups dans {DIR_LINEUPS}")

# -----------------------------------------------------------------------------
# EVENTS
# -----------------------------------------------------------------------------

def fetch_events(matches_df):
    total   = len(matches_df)
    skipped = 0
    success = 0
    errors  = []

    print(f"\nFetching events pour {total} matchs...")
    print("-" * 60)

    start_total = time.time()

    for i, (_, match) in enumerate(matches_df.iterrows(), start=1):
        match_id     = int(match["match_id"])
        home         = match.get("home_team", "?")
        away         = match.get("away_team", "?")
        match_date   = match.get("match_date", "?")
        match_status = match.get("match_status", "")
        output_path  = DIR_EVENTS / f"match_{match_id}_events.json"

        prefix = f"[{i:>3}/{total}] {match_date} {home} vs {away} (id={match_id})"

        # Match pas joue -> skip
        if match_status != "available":
            print(f"{prefix} -> SKIP (non joue, status={match_status})")
            skipped += 1
            continue

        # Fichier deja present -> skip
        if output_path.exists():
            print(f"{prefix} -> SKIP (deja present)")
            skipped += 1
            continue

        try:
            start = time.time()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = sb.events(match_id=match_id, creds=CREDS)

            save_json(output_path, json.loads(df.to_json(orient="records")))
            elapsed = time.time() - start
            print(f"{prefix} -> OK ({len(df)} events, {elapsed:.1f}s)")
            success += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"{prefix} -> ERREUR : {e}")
            errors.append({"match_id": match_id, "error": str(e)})

    total_elapsed = time.time() - start_total
    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)

    print("-" * 60)
    print(f"\nRapport events :")
    print(f"  Succes  : {success}")
    print(f"  Skipped : {skipped}")
    print(f"  Erreurs : {len(errors)}")
    print(f"  Temps   : {minutes}min {seconds}s")

    if errors:
        errors_path = DIR_EVENTS / "fetch_errors.json"
        save_json(errors_path, errors)
        print(f"  Erreurs sauvegardees -> {errors_path}")

    print(f"\n[DONE] Events dans {DIR_EVENTS}")

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    create_dirs()

    competition_id = get_competition_id(COMPETITION_NAME)
    season_id      = get_season_id(COMPETITION_NAME, SEASON_NAME)

    fetch_players(competition_id, season_id)
    fetch_teams(competition_id, season_id)

    matches_df = fetch_matches(competition_id, season_id)


    fetch_lineups(matches_df)
    fetch_events(matches_df)


if __name__ == "__main__":
    main()