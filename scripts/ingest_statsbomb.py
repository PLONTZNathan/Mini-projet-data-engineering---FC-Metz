"""
ingest_statsbomb.py
-------------------
StatsBomb data ingestion -- Ligue 1 2025/2026.

USAGE
-----
  Re-fetch everything:
      python ingest_statsbomb.py --all

  Players + teams + matches stats only:
      python ingest_statsbomb.py --players
      python ingest_statsbomb.py --teams
      python ingest_statsbomb.py --matches

  All lineups (resumable -- skips already downloaded):
      python ingest_statsbomb.py --lineups

  All events (resumable -- skips already downloaded):
      python ingest_statsbomb.py --events

  Specific matches (force re-download):
      python ingest_statsbomb.py --lineups 3935583 3935584
      python ingest_statsbomb.py --events 3935583 3935584

  Test mode (limit number of matches):
      python ingest_statsbomb.py --all --limit 5

WHAT EACH ARGUMENT COVERS
--------------------------
  --players  → player_season_stats for the full season (one JSON file)
  --teams    → team_season_stats for the full season (one JSON file)
  --matches  → fetches and saves the matches reference file for the season
  --lineups  → one JSON file per match (resumable, or forced with IDs)
  --events   → one JSON file per match via REST API (resumable, or forced with IDs)
               Uses direct REST calls to preserve nested objects (freeze frames, tactics)
  --all      → equivalent to --players --teams --matches --lineups --events (no IDs)
"""

import os
import sys
import json
import time
import argparse
import warnings
import requests
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

ROOT        = Path(__file__).resolve().parent.parent
DATA_DIR    = ROOT / "data" / "raw" / "statsbomb"
DIR_PLAYERS = DATA_DIR / "players"
DIR_TEAMS   = DATA_DIR / "teams"
DIR_MATCHES = DATA_DIR / "matches"
DIR_EVENTS  = DATA_DIR / "events"
DIR_LINEUPS = DATA_DIR / "lineups"

PLAYERS_JSON = DIR_PLAYERS / "ligue1_players_2025_2026.json"
TEAMS_JSON   = DIR_TEAMS   / "ligue1_teams_2025_2026.json"
MATCHES_JSON = DIR_MATCHES / "ligue1_matches_2025_2026.json"

STATSBOMB_API = "https://data.statsbombservices.com/api"

# -----------------------------------------------------------------------------
# UTILITIES
# -----------------------------------------------------------------------------

def create_dirs():
    for d in [DIR_PLAYERS, DIR_TEAMS, DIR_MATCHES, DIR_EVENTS, DIR_LINEUPS]:
        d.mkdir(parents=True, exist_ok=True)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def print_separator(char="=", width=80):
    print(char * width)


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def api_get(url):
    """Authenticated REST GET. Returns (data, status_code)."""
    r = requests.get(
        url,
        auth=(CREDS["user"], CREDS["passwd"]),
        headers={"accept": "application/json"}
    )
    if r.status_code == 200:
        return r.json(), 200
    return None, r.status_code

# -----------------------------------------------------------------------------
# COMPETITION RESOLUTION  (single call)
# -----------------------------------------------------------------------------

def resolve_competition():
    """Returns (competition_id, season_id) for the configured competition/season."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        competitions = sb.competitions(creds=CREDS)

    match = competitions[
        (competitions["competition_name"] == COMPETITION_NAME) &
        (competitions["season_name"]      == SEASON_NAME)
    ]
    if match.empty:
        raise RuntimeError(
            f"'{COMPETITION_NAME} {SEASON_NAME}' non trouvee dans les competitions disponibles"
        )
    competition_id = int(match["competition_id"].values[0])
    season_id      = int(match["season_id"].values[0])
    print(f"[OK] {COMPETITION_NAME} {SEASON_NAME} "
          f"-> competition_id={competition_id}, season_id={season_id}")
    return competition_id, season_id

# -----------------------------------------------------------------------------
# PLAYERS
# -----------------------------------------------------------------------------

def fetch_players(competition_id, season_id):
    print(f"\nFetching player season stats...")
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
    print(f"\nFetching team season stats...")
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
    print(f"[OK] {len(df)}/{total} available matches saved -> {MATCHES_JSON}")
    return df

# -----------------------------------------------------------------------------
# LINEUPS
# -----------------------------------------------------------------------------

def fetch_lineups(matches_df, match_ids=None, limit=None):
    """
    match_ids=None  : resumable, skips existing files
    match_ids=[...] : force re-download for those specific IDs
    """
    force = match_ids is not None

    if match_ids:
        targets = matches_df[matches_df["match_id"].isin(match_ids)]
        if targets.empty:
            print("[WARN] None of the provided match_ids found in available matches")
            return
    else:
        targets = matches_df

    if limit:
        targets = targets.head(limit)
        print(f"[TEST MODE] Limited to first {limit} matches")

    total   = len(targets)
    skipped = 0
    success = 0
    errors  = []

    print(f"\nFetching lineups for {total} matches (force={force})...")
    print_separator("-", 60)

    start_total = time.time()

    for i, (_, match) in enumerate(targets.iterrows(), start=1):
        match_id    = int(match["match_id"])
        home        = match.get("home_team", "?")
        away        = match.get("away_team", "?")
        match_date  = match.get("match_date", "?")
        output_path = DIR_LINEUPS / f"match_{match_id}_lineups.json"

        prefix = f"[{i:>3}/{total}] {match_date} {home} vs {away} (id={match_id})"

        if output_path.exists() and not force:
            print(f"{prefix} -> SKIP (already in)")
            skipped += 1
            continue

        try:
            start = time.time()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                lineups = sb.lineups(match_id=match_id, creds=CREDS)

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
            print(f"{prefix} -> ERROR : {e}")
            errors.append({"match_id": match_id, "error": str(e)})

    _print_report("lineups", success, skipped, errors, time.time() - start_total, DIR_LINEUPS)


# -----------------------------------------------------------------------------
# EVENTS  (direct REST API to preserve nested JSON objects)
# -----------------------------------------------------------------------------

def fetch_events(matches_df, match_ids=None, limit=None):
    """
    Uses direct REST API calls instead of statsbombpy to preserve nested
    objects like shot_freeze_frame, tactics_lineup, pass_cluster_*, etc.

    match_ids=None  : resumable, skips existing files
    match_ids=[...] : force re-download for those specific IDs
    """
    force = match_ids is not None

    if match_ids:
        targets = matches_df[matches_df["match_id"].isin(match_ids)]
        if targets.empty:
            print("[WARN] None of the provided match_ids found in available matches")
            return
    else:
        targets = matches_df

    if limit:
        targets = targets.head(limit)
        print(f"[TEST MODE] Limited to first {limit} matches")

    total   = len(targets)
    skipped = 0
    success = 0
    errors  = []

    print(f"\nFetching events for {total} matches via REST API (force={force})...")
    print_separator("-", 60)

    start_total = time.time()

    for i, (_, match) in enumerate(targets.iterrows(), start=1):
        match_id    = int(match["match_id"])
        home        = match.get("home_team", "?")
        away        = match.get("away_team", "?")
        match_date  = match.get("match_date", "?")
        output_path = DIR_EVENTS / f"match_{match_id}_events.json"

        prefix = f"[{i:>3}/{total}] {match_date} {home} vs {away} (id={match_id})"

        if output_path.exists() and not force:
            print(f"{prefix} -> SKIP (already in)")
            skipped += 1
            continue

        try:
            start = time.time()
            url   = f"{STATSBOMB_API}/v8/events/{match_id}"
            data, status = api_get(url)

            if status != 200 or data is None:
                raise RuntimeError(f"HTTP {status}")

            save_json(output_path, data)
            elapsed = time.time() - start
            print(f"{prefix} -> OK ({len(data)} events, {elapsed:.1f}s)")
            success += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"{prefix} -> ERROR : {e}")
            errors.append({"match_id": match_id, "error": str(e)})

    _print_report("events", success, skipped, errors, time.time() - start_total, DIR_EVENTS)


# -----------------------------------------------------------------------------
# SHARED REPORT HELPER
# -----------------------------------------------------------------------------

def _print_report(label, success, skipped, errors, elapsed, output_dir):
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print_separator("-", 60)
    print(f"\n{label} report:")
    print(f"  Success : {success}")
    print(f"  Skipped : {skipped}")
    print(f"  Errors  : {len(errors)}")
    print(f"  Time    : {minutes}min {seconds}s")

    if errors:
        errors_path = output_dir / "fetch_errors.json"
        save_json(errors_path, errors)
        print(f"  Errors saved -> {errors_path}")

    print(f"\n[DONE] {label.capitalize()} saved in {output_dir}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="StatsBomb data ingestion -- Ligue 1 2025/2026",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest_statsbomb.py --all
  python ingest_statsbomb.py --players
  python ingest_statsbomb.py --teams
  python ingest_statsbomb.py --matches
  python ingest_statsbomb.py --lineups
  python ingest_statsbomb.py --events
  python ingest_statsbomb.py --lineups 3935583 3935584
  python ingest_statsbomb.py --events 3935583 3935584
  python ingest_statsbomb.py --all --limit 5
        """
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-fetch players + teams + matches + lineups + events"
    )
    parser.add_argument(
        "--players",
        action="store_true",
        help="Re-fetch player season stats"
    )
    parser.add_argument(
        "--teams",
        action="store_true",
        help="Re-fetch team season stats"
    )
    parser.add_argument(
        "--matches",
        action="store_true",
        help="Re-fetch the matches reference file for the season"
    )
    parser.add_argument(
        "--lineups",
        nargs="*",
        metavar="MATCH_ID",
        help="Fetch lineups. No ID = resumable. With IDs = force re-download."
    )
    parser.add_argument(
        "--events",
        nargs="*",
        metavar="MATCH_ID",
        help="Fetch events. No ID = resumable. With IDs = force re-download."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of matches to process (test mode)"
    )

    args = parser.parse_args()

    if not any([args.all, args.players, args.teams, args.matches,
                args.lineups is not None, args.events is not None]):
        parser.print_help()
        sys.exit(1)

    return args


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    create_dirs()
    args    = parse_args()
    t_start = time.time()

    print_separator("=", 80)
    print("INGEST STATSBOMB -- Ligue 1 2025/2026")
    print_separator("=", 80)

    if args.all:
        print("Mode   : ALL (players + teams + matches + lineups + events)")
    else:
        parts = []
        if args.players:
            parts.append("players")
        if args.teams:
            parts.append("teams")
        if args.matches:
            parts.append("matches")
        if args.lineups is not None:
            parts.append(f"lineups {args.lineups if args.lineups else '(all)'}")
        if args.events is not None:
            parts.append(f"events {args.events if args.events else '(all)'}")
        print(f"Mode   : {' | '.join(parts)}")

    if args.limit:
        print(f"Limit  : {args.limit} (TEST MODE)")
    print_separator("=", 80)

    # Single competition resolution call
    competition_id, season_id = resolve_competition()

    step_timings = {}

    # -- PLAYERS ---------------------------------------------------------------
    if args.all or args.players:
        print_section("STEP: PLAYERS")
        t = time.time()
        fetch_players(competition_id, season_id)
        step_timings["players"] = time.time() - t

    # -- TEAMS -----------------------------------------------------------------
    if args.all or args.teams:
        print_section("STEP: TEAMS")
        t = time.time()
        fetch_teams(competition_id, season_id)
        step_timings["teams"] = time.time() - t

    # -- MATCHES ---------------------------------------------------------------
    # Fetched standalone (--matches) OR when needed for lineups/events
    needs_matches = args.all or args.matches or args.lineups is not None or args.events is not None
    if needs_matches:
        print_section("STEP: MATCHES")
        t = time.time()
        matches_df = fetch_matches(competition_id, season_id)
        step_timings["matches"] = time.time() - t

    # -- LINEUPS ---------------------------------------------------------------
    if args.all or args.lineups is not None:
        print_section("STEP: LINEUPS")
        t = time.time()
        match_ids = [int(x) for x in args.lineups] if args.lineups else None
        fetch_lineups(matches_df, match_ids=match_ids, limit=args.limit)
        step_timings["lineups"] = time.time() - t

    # -- EVENTS ----------------------------------------------------------------
    if args.all or args.events is not None:
        print_section("STEP: EVENTS")
        t = time.time()
        match_ids = [int(x) for x in args.events] if args.events else None
        fetch_events(matches_df, match_ids=match_ids, limit=args.limit)
        step_timings["events"] = time.time() - t

    # -- FINAL SUMMARY ---------------------------------------------------------
    total = time.time() - t_start
    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    for step, duration in step_timings.items():
        print(f"  {step:<30} {duration:.2f}s  ({duration/60:.2f} min)")
    print_separator("-", 80)
    print(f"  {'TOTAL':<30} {total:.2f}s  ({total/60:.2f} min)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()