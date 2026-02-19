"""
ingest_skillcorner.py
─────────────────────
SkillCorner data ingestion — Ligue 1 2025/2026.

USAGE
─────
  Re-fetch everything:
      python ingest_skillcorner.py --all

  All players:
      python ingest_skillcorner.py --players

  Specific players (SkillCorner IDs):
      python ingest_skillcorner.py --players 14 18 202

  All matches (skip already downloaded):
      python ingest_skillcorner.py --matchs

  Specific matches (force re-download):
      python ingest_skillcorner.py --matchs 2038827 2038828

  Teams only:
      python ingest_skillcorner.py --teams

  Mix:
      python ingest_skillcorner.py --players 14 18 --matchs 2038827

  Test mode (limit number of items):
      python ingest_skillcorner.py --all --limit 10

WHAT EACH ARGUMENT COVERS
──────────────────────────
  --players  → players reference JSON + physical + off_ball_runs
                                     + on_ball_pressures + passes
               No IDs  : all players (overwrites existing files)
               With IDs: only those players (overwrites)

  --teams    → teams reference JSON
               No IDs  : all teams

  --matchs   → matches reference JSON + dynamic_events (CSV)
                                      + data_collection (JSON)
               No IDs  : all matches, SKIP if file already exists
               With IDs: only those matches, FORCE re-download

  --all      → equivalent to --players --teams --matchs (no IDs)
"""

import os
import sys
import json
import time
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv
from skillcorner.client import SkillcornerClient

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()
USERNAME = os.getenv("SKILLCORNER_USERNAME")
PASSWORD = os.getenv("SKILLCORNER_PASSWORD")

COMPETITION = {
    "area":      "FRA",
    "name":      "Ligue 1",
    "gender":    "male",
    "age_group": "adult",
    "season":    "2025/2026",
}

# Output directories
DATA_DIR         = Path("data/raw/skillcorner")
DIR_PLAYERS      = DATA_DIR / "players"
DIR_TEAMS        = DATA_DIR / "teams"
DIR_MATCHES      = DATA_DIR / "matchs"
DIR_PHYSICAL     = DATA_DIR / "physical"
DIR_OFF_BALL     = DATA_DIR / "in_possession" / "off_ball_runs"
DIR_ON_BALL      = DATA_DIR / "in_possession" / "on_ball_pressures"
DIR_PASSES       = DATA_DIR / "in_possession" / "passes"
DIR_DYN_EVENTS   = DATA_DIR / "matchs" / "dynamic_events"
DIR_DATA_COLLECT = DATA_DIR / "matchs" / "data_collection"

# Reference JSON files
PLAYERS_JSON = DIR_PLAYERS / "ligue1_players_2025_2026.json"
TEAMS_JSON   = DIR_TEAMS   / "ligue1_teams_2025_2026.json"
MATCHES_JSON = DIR_MATCHES / "ligue1_matches_2025_2026.json"


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def create_dirs():
    for d in [DIR_PLAYERS, DIR_TEAMS, DIR_MATCHES, DIR_PHYSICAL,
              DIR_OFF_BALL, DIR_ON_BALL, DIR_PASSES,
              DIR_DYN_EVENTS, DIR_DATA_COLLECT]:
        d.mkdir(parents=True, exist_ok=True)


def safe_print(text):
    try:
        print(text, end=" ", flush=True)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"), end=" ", flush=True)


def print_separator(char="=", width=80):
    print(char * width)


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def api_get(url, params=None, as_text=False):
    """Authenticated GET request. Returns (content, status_code)."""
    r = requests.get(
        url, params=params,
        auth=(USERNAME, PASSWORD),
        headers={"accept": "application/json"}
    )
    if r.status_code == 200:
        return (r.text if as_text else r.json()), 200
    return None, r.status_code


def paginate(url, extra_params=None):
    """Fetches all pages from a paginated endpoint. Returns full list."""
    items, offset, limit = [], 0, 100
    params = {**(extra_params or {}), "limit": limit}
    while True:
        params["offset"] = offset
        data, status = api_get(url, params)
        if status != 200:
            raise RuntimeError(f"API error {status} on {url}")
        items.extend(data["results"])
        print(f"   Fetched {len(items)}/{data['count']}...", end="\r", flush=True)
        if data["next"] is None:
            break
        offset += limit
    print()
    return items


# ─────────────────────────────────────────────────────────────────────────────
# EDITION RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def resolve_edition(client):
    """Finds the edition_id matching the configured competition and season."""
    comps = client.get_competitions()
    comp  = next(
        (c for c in comps
         if c.get("area")       == COMPETITION["area"]
         and c.get("name")      == COMPETITION["name"]
         and c.get("gender")    == COMPETITION["gender"]
         and c.get("age_group") == COMPETITION["age_group"]),
        None
    )
    if not comp:
        raise RuntimeError("Competition not found — check COMPETITION config")

    editions = client.get_competition_competition_editions(
        competition_id=str(comp["id"])
    )
    edition = next(
        (e for e in editions
         if e.get("season", {}).get("name") == COMPETITION["season"]),
        None
    )
    if not edition:
        raise RuntimeError(f"Season {COMPETITION['season']} not found")

    print(f"[OK] Competition ID={comp['id']}  Edition ID={edition['id']}")
    return edition["id"]


# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE DATA
# ─────────────────────────────────────────────────────────────────────────────

def refresh_players_ref(edition_id):
    print(f"\nFetching players reference list...")
    players = paginate(
        "https://skillcorner.com/api/players/",
        {"competition_edition": edition_id}
    )
    save_json(PLAYERS_JSON, players)
    print(f"[OK] {len(players)} players saved → {PLAYERS_JSON}")
    return players


def refresh_teams_ref(edition_id):
    print(f"\nFetching teams reference list...")
    teams = paginate(
        "https://skillcorner.com/api/teams/",
        {"competition_edition": edition_id}
    )
    save_json(TEAMS_JSON, teams)
    print(f"[OK] {len(teams)} teams saved → {TEAMS_JSON}")

    print(f"\nTEAMS LIST:")
    print_separator("-", 60)
    for i, t in enumerate(teams, 1):
        stadium = t.get("stadium", {})
        print(f"  {i:2}. {t.get('name')} (ID: {t.get('id')}) "
              f"— {stadium.get('name', 'N/A')}, {stadium.get('city', 'N/A')}")
    print_separator("-", 60)

    return teams


def refresh_matches_ref(edition_id):
    print(f"\nFetching matches reference list...")
    matches = paginate(
        "https://skillcorner.com/api/matches/",
        {"competition_edition": edition_id, "user": "true"}
    )
    save_json(MATCHES_JSON, matches)
    print(f"[OK] {len(matches)} matches saved → {MATCHES_JSON}")
    return matches


# ─────────────────────────────────────────────────────────────────────────────
# PLAYER DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────

PLAYER_ENDPOINTS = [
    {
        "name": "physical",
        "url":  "https://skillcorner.com/api/physical/",
        "dir":  DIR_PHYSICAL,
        "params": {
            "results": "win,lose,draw", "venue": "home,away",
            "period": "full", "possession": "all",
            "physical_check_passed": "true", "response_format": "json"
        }
    },
    {
        "name": "off_ball_runs",
        "url":  "https://skillcorner.com/api/in_possession/off_ball_runs/",
        "dir":  DIR_OFF_BALL,
        "params": {
            "results": "win,lose,draw", "venue": "home,away",
            "channel": "all", "third": "all",
            "average_per": "match", "group_by": "match,player"
        }
    },
    {
        "name": "on_ball_pressures",
        "url":  "https://skillcorner.com/api/in_possession/on_ball_pressures/",
        "dir":  DIR_ON_BALL,
        "params": {
            "results": "win,lose,draw", "venue": "home,away",
            "channel": "all", "third": "all",
            "average_per": "match", "group_by": "match,player"
        }
    },
    {
        "name": "passes",
        "url":  "https://skillcorner.com/api/in_possession/passes/",
        "dir":  DIR_PASSES,
        "params": {
            "results": "win,lose,draw", "venue": "home,away",
            "channel": "all", "third": "all",
            "average_per": "match", "group_by": "match,player"
        }
    },
]


def _fetch_player_endpoint(player, ep):
    """
    Fetch one endpoint for one player.
    Deletes the old file first (record count in filename may change).
    Returns the number of records saved, or "error" / "no_data".
    """
    pid    = player["id"]
    outdir = ep["dir"]

    # Remove old file for this player (filename encodes record count)
    for old in outdir.glob(f"player_{pid}_*"):
        old.unlink()

    data, status = api_get(ep["url"], {**ep["params"], "player": pid})

    if status != 200 or data is None:
        save_json(outdir / f"player_{pid}_0_records.json",
                  {"error": f"HTTP {status}", "player_id": pid})
        return "error"

    if (isinstance(data, list) and len(data) == 0) or \
       (isinstance(data, dict) and not data.get("results")):
        save_json(outdir / f"player_{pid}_0_records.json", data)
        return "no_data"

    n = len(data) if isinstance(data, list) else len(data.get("results", []))
    save_json(outdir / f"player_{pid}_{n}_records.json", data)
    return n


def fetch_players_data(players):
    """Fetch all player endpoints for the given list of players."""
    ep_timings = {}

    for ep in PLAYER_ENDPOINTS:
        ep_start = time.time()
        print(f"\n\nFetching {ep['name'].upper()} data for {len(players)} players...")
        print_separator("=", 80)

        ok = no_data = errors = 0

        for i, player in enumerate(players):
            pid   = player["id"]
            pname = player.get("short_name", "?")
            safe_print(f"[{i+1}/{len(players)}] {pname} (ID: {pid})...")

            result = _fetch_player_endpoint(player, ep)

            if result == "error":
                print("[ERROR]")
                errors += 1
            elif result == "no_data":
                print("[NO DATA]")
                no_data += 1
            else:
                print(f"[OK] {result} records saved")
                ok += 1

            time.sleep(0.2)

        ep_duration = time.time() - ep_start
        ep_timings[ep["name"]] = ep_duration

        print_separator("=", 80)
        print(f"\n{ep['name'].upper()} SUMMARY")
        print_separator("=", 80)
        print(f"Total players processed : {len(players)}")
        print(f"Success                 : {ok}")
        print(f"No data                 : {no_data}")
        print(f"Errors                  : {errors}")
        print(f"Output directory        : {ep['dir']}")
        print(f"Duration                : {ep_duration:.2f}s ({ep_duration/60:.2f} min)")
        print_separator("=", 80)

    return ep_timings


# ─────────────────────────────────────────────────────────────────────────────
# MATCH DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_match(match, force=False):
    """
    Fetch dynamic_events (CSV) + data_collection (JSON) for one match.
    force=True → re-downloads even if file already exists.
    """
    mid  = match["id"]
    home = match.get("home_team", {}).get("short_name", "?")
    away = match.get("away_team", {}).get("short_name", "?")
    safe_print(f"[{mid}] {home} vs {away}...")

    results = {}

    # ── dynamic_events (CSV)
    dyn_file = DIR_DYN_EVENTS / f"match_{mid}_dynamic_events.csv"
    if dyn_file.exists() and not force:
        print("[SKIP dyn_events]", end=" ")
        results["dyn"] = "skip"
    else:
        csv_text, status = api_get(
            f"https://skillcorner.com/api/match/{mid}/dynamic_events/",
            {"file_format": "csv", "ignore_dynamic_events_check": "true"},
            as_text=True
        )
        if status == 200 and csv_text:
            dyn_file.write_text(csv_text, encoding="utf-8")
            rows = csv_text.count("\n") - 1
            print(f"[OK dyn_events ~{rows} rows]", end=" ")
            results["dyn"] = "ok"
        else:
            print(f"[ERROR dyn_events HTTP {status}]", end=" ")
            results["dyn"] = "error"

    # ── data_collection (JSON)
    col_file = DIR_DATA_COLLECT / f"match_{mid}_data_collection.json"
    if col_file.exists() and not force:
        print("[SKIP data_collection]")
        results["col"] = "skip"
    else:
        data, status = api_get(
            f"https://skillcorner.com/api/match/{mid}/data_collection/"
        )
        if status == 200 and data:
            save_json(col_file, data)
            print(f"[OK data_collection status={data.get('status')} "
                  f"dyn_check={data.get('dynamic_events_check')}]")
            results["col"] = "ok"
        else:
            print(f"[ERROR data_collection HTTP {status}]")
            results["col"] = "error"

    time.sleep(0.3)
    return results


def fetch_matches_data(matches, force=False):
    """Fetch match data for the given list of matches."""
    print(f"\nFetching match data for {len(matches)} matches...")
    print_separator("=", 80)

    counters = {"dyn": {"ok": 0, "skip": 0, "error": 0},
                "col": {"ok": 0, "skip": 0, "error": 0}}

    for i, match in enumerate(matches):
        safe_print(f"[{i+1}/{len(matches)}]")
        results = _fetch_match(match, force=force)
        for key in ("dyn", "col"):
            r = results.get(key, "error")
            if r == "ok":
                counters[key]["ok"] += 1
            elif r.startswith("skip"):
                counters[key]["skip"] += 1
            else:
                counters[key]["error"] += 1

    print_separator("=", 80)
    print("\nMATCHES SUMMARY")
    print_separator("=", 80)
    print(f"Total matches processed  : {len(matches)}")
    print(f"dynamic_events  — OK: {counters['dyn']['ok']}  "
          f"Skip: {counters['dyn']['skip']}  Errors: {counters['dyn']['error']}")
    print(f"data_collection — OK: {counters['col']['ok']}  "
          f"Skip: {counters['col']['skip']}  Errors: {counters['col']['error']}")
    print(f"Output: {DIR_DYN_EVENTS}")
    print(f"        {DIR_DATA_COLLECT}")
    print_separator("=", 80)


# ─────────────────────────────────────────────────────────────────────────────
# CLI ARGUMENTS
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="SkillCorner data ingestion — Ligue 1 2025/2026",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest_skillcorner.py --all
  python ingest_skillcorner.py --players
  python ingest_skillcorner.py --players 14 18 202
  python ingest_skillcorner.py --matchs
  python ingest_skillcorner.py --matchs 2038827 2038828
  python ingest_skillcorner.py --teams
  python ingest_skillcorner.py --players 14 18 --matchs 2038827
  python ingest_skillcorner.py --all --limit 10
        """
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-fetch players + teams + matches"
    )
    parser.add_argument(
        "--players",
        nargs="*",
        metavar="ID",
        help="Re-fetch player data. No ID = all players. With IDs = only those players."
    )
    parser.add_argument(
        "--teams",
        action="store_true",
        help="Re-fetch teams."
    )
    parser.add_argument(
        "--matchs",
        nargs="*",
        metavar="ID",
        help="Re-fetch match data. No ID = skip existing files. With IDs = force re-download."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of players/matches to process (test mode)"
    )

    args = parser.parse_args()

    if not args.all and args.players is None and not args.teams and args.matchs is None:
        parser.print_help()
        sys.exit(1)

    return args


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    create_dirs()
    args = parse_args()
    t_start = time.time()

    print_separator("=", 80)
    print("INGEST SKILLCORNER — Ligue 1 2025/2026")
    print_separator("=", 80)

    # Print what will run
    if args.all:
        print("Mode    : ALL (players + teams + matches)")
    else:
        parts = []
        if args.players is not None:
            parts.append(f"players {args.players if args.players else '(all)'}")
        if args.teams:
            parts.append("teams")
        if args.matchs is not None:
            parts.append(f"matchs {args.matchs if args.matchs else '(all)'}")
        print(f"Mode    : {' | '.join(parts)}")
    if args.limit:
        print(f"Limit   : {args.limit} (TEST MODE)")
    print_separator("=", 80)

    # Connect
    print("\nConnecting to SkillCorner...")
    client = SkillcornerClient(username=USERNAME, password=PASSWORD)
    print("[OK] Connected")
    edition_id = resolve_edition(client)

    step_timings = {}

    # ── PLAYERS ──────────────────────────────────────────────────────────────
    if args.all or args.players is not None:
        print_section("STEP: PLAYERS")
        t_step = time.time()

        all_players = refresh_players_ref(edition_id)

        if args.players:
            # Explicit IDs
            ids     = {int(x) for x in args.players}
            targets = [p for p in all_players if p["id"] in ids]
            missing = ids - {p["id"] for p in targets}
            if missing:
                print(f"[WARN] IDs not found in reference: {missing}")
            print(f"Targeting {len(targets)} specific player(s)")
        else:
            targets = all_players[:args.limit] if args.limit else all_players
            if args.limit:
                print(f"[TEST MODE] Limited to first {args.limit} players")

        ep_timings = fetch_players_data(targets)
        step_timings["players"] = time.time() - t_step
        step_timings.update({f"players/{k}": v for k, v in ep_timings.items()})

    # ── TEAMS ─────────────────────────────────────────────────────────────────
    if args.all or args.teams:
        print_section("STEP: TEAMS")
        t_step = time.time()
        refresh_teams_ref(edition_id)
        step_timings["teams"] = time.time() - t_step

    # ── MATCHES ───────────────────────────────────────────────────────────────
    if args.all or args.matchs is not None:
        print_section("STEP: MATCHES")
        t_step = time.time()

        all_matches = refresh_matches_ref(edition_id)

        if args.matchs:
            # Explicit IDs → force re-download
            ids     = {int(x) for x in args.matchs}
            targets = [m for m in all_matches if m["id"] in ids]
            missing = ids - {m["id"] for m in targets}
            if missing:
                print(f"[WARN] Match IDs not found in reference: {missing}")
            print(f"Targeting {len(targets)} specific match(es) — forcing re-download")
            fetch_matches_data(targets, force=True)
        else:
            targets = all_matches[:args.limit] if args.limit else all_matches
            if args.limit:
                print(f"[TEST MODE] Limited to first {args.limit} matches")
            # No IDs → skip already-downloaded files
            fetch_matches_data(targets, force=False)

        step_timings["matches"] = time.time() - t_step

    # ── FINAL SUMMARY ─────────────────────────────────────────────────────────
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