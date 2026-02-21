# Script : process_data.py
# Location : scripts/process_data.py
#
# Entry point for all data processing tasks.
# Each entity (teams, players, matches, ...) has its own dedicated function.
# Run this script to regenerate all processed CSV files used as database tables.

import os
import json
import pandas as pd


# Project root is one level above the scripts folder
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

RAW       = os.path.join(ROOT, "data", "raw")
PROCESSED = os.path.join(ROOT, "data", "processed")


# ============================================================
# HELPERS
# ============================================================

def load_json(path):
    # Load a JSON file and always return a list
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


def save_csv(df, filename):
    # Save a dataframe to data/processed/<filename>.csv
    os.makedirs(PROCESSED, exist_ok=True)
    output_path = os.path.join(PROCESSED, filename + ".csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print("Created " + output_path)


# ============================================================
# TEAMS
# ============================================================

def load_teams_skillcorner():
    # Extract stadium city for each team
    path = os.path.join(RAW, "skillcorner", "teams", "ligue1_teams_2025_2026.json")
    rows = []
    for team in load_json(path):
        city = team.get("stadium", {}).get("city") if team.get("stadium") else None
        rows.append({"sc_id": team["id"], "city": city})
    return pd.DataFrame(rows).drop_duplicates("sc_id")


def load_teams_statsbomb():
    # Extract season metadata and all team_season_* statistics
    path = os.path.join(RAW, "statsbomb", "teams", "ligue1_teams_2025_2026.json")
    meta_fields = ["competition_name", "season_name", "team_female"]
    rows = []
    for team in load_json(path):
        row = {"sb_id": team.get("team_id") or team.get("id")}
        for field in meta_fields:
            row[field] = team.get(field)
        for key, value in team.items():
            if key.startswith("team_season_"):
                row[key] = value
        rows.append(row)
    return pd.DataFrame(rows).drop_duplicates("sb_id")


def load_teams_transfermarkt():
    # Extract club info columns
    path = os.path.join(RAW, "transfermarkt", "ligue1_teams_2025_2026.csv")
    keep = [
        "id", "team", "squad_size", "average_age", "national_team_players",
        "stadium_name", "stadium_capacity", "table_position", "years_in_league",
    ]
    df = pd.read_csv(path, usecols=lambda c: c in keep)
    return df.rename(columns={"id": "tm_id"}).drop_duplicates("tm_id")


def process_teams():
    mapping = pd.read_csv(os.path.join(RAW, "mapping", "teams_mapping.csv"))

    result = mapping.copy()
    result = result.merge(load_teams_statsbomb(),    on="sb_id", how="left")
    result = result.merge(load_teams_skillcorner(),  on="sc_id", how="left")
    result = result.merge(load_teams_transfermarkt(), on="tm_id", how="left")

    # Remove all source IDs and source name columns (handled by the mapping table)
    source_cols = ["sb_id", "sb_name", "sc_id", "sc_name", "tm_id", "tm_name"]
    result = result.drop(columns=[c for c in source_cols if c in result.columns])

    # Put descriptive columns first, then all stats at the end
    identity_cols = [
        "id", "team", "competition_name", "season_name", "team_female",
        "city", "stadium_name", "stadium_capacity",
        "squad_size", "average_age", "national_team_players",
        "table_position", "years_in_league",
    ]
    identity_cols = [c for c in identity_cols if c in result.columns]
    stat_cols = sorted([c for c in result.columns if c not in identity_cols])
    result = result[identity_cols + stat_cols]

    save_csv(result, "teams")


# ============================================================
# PLAYERS
# ============================================================

def load_players_transfermarkt():
    # Extract player info columns from Transfermarkt
    path = os.path.join(RAW, "transfermarkt", "ligue1_players_2025_2026.csv")
    keep = [
        "id", "age", "birth_place", "nationalities",
        "shirt_number", "joined_date", "contract_end", "market_value_m",
        "matches", "goals", "assists", "minutes", "yellow_cards", "red_cards",
    ]
    df = pd.read_csv(path, usecols=lambda c: c in keep)
    return df.rename(columns={"id": "tm_id"}).drop_duplicates("tm_id")


def load_players_statsbomb(teams_mapping):
    # Extract player name, team reference, season metadata and all player_season_* stats
    # teams_mapping is used to resolve the StatsBomb team_id into our internal team id
    path = os.path.join(RAW, "statsbomb", "players", "ligue1_players_2025_2026.json")

    # Build a lookup: sb team_id -> our internal team id
    sb_team_lookup = dict(zip(teams_mapping["sb_id"], teams_mapping["id"]))

    meta_fields = ["player_name", "competition_name", "season_name",
                   "birth_date", "player_female", "player_first_name",
                   "player_last_name", "player_known_name",
                   "player_weight", "player_height",
                   "primary_position", "secondary_position"]

    rows = []
    for player in load_json(path):
        row = {"sb_id": player.get("player_id") or player.get("id")}

        # Resolve StatsBomb team_id to our internal team id
        sb_team_id = player.get("team_id")
        row["team_id"] = sb_team_lookup.get(sb_team_id)

        for field in meta_fields:
            row[field] = player.get(field)

        # Dynamically add all player_season_* statistics
        for key, value in player.items():
            if key.startswith("player_season_"):
                row[key] = value

        rows.append(row)

    return pd.DataFrame(rows).drop_duplicates("sb_id")


def process_players():
    players_mapping = pd.read_csv(os.path.join(RAW, "mapping", "players_mapping.csv"))
    teams_mapping   = pd.read_csv(os.path.join(RAW, "mapping", "teams_mapping.csv"))

    result = players_mapping.copy()
    result = result.merge(load_players_statsbomb(teams_mapping), on="sb_id", how="left")
    result = result.merge(load_players_transfermarkt(),          on="tm_id", how="left")

    # Remove all source IDs, source names and match flag columns
    source_cols = [
        "sb_id", "sb_name", "sb_birth_date",
        "sc_id", "sc_name", "sc_birth_date",
        "tm_id", "tm_name", "tm_birth_date",
        "match_sb_sc", "match_sb_tm", "match_sc_tm",
    ]
    result = result.drop(columns=[c for c in source_cols if c in result.columns])

    # Put descriptive columns first, then all stats at the end
    identity_cols = [
        "id", "player_name", "player_first_name", "player_last_name", "player_known_name",
        "team_id", "competition_name", "season_name", "player_female",
        "birth_date", "age", "birth_place", "nationalities",
        "player_height", "player_weight",
        "primary_position", "secondary_position",
        "shirt_number", "joined_date", "contract_end", "market_value_m",
        "matches", "goals", "assists", "minutes", "yellow_cards", "red_cards",
    ]
    identity_cols = [c for c in identity_cols if c in result.columns]
    stat_cols = sorted([c for c in result.columns if c not in identity_cols])
    result = result[identity_cols + stat_cols]

    save_csv(result, "players")


# ============================================================
# MATCHES
# ============================================================

def load_matches_statsbomb():
    # Extract match metadata from StatsBomb
    path = os.path.join(RAW, "statsbomb", "matches", "ligue1_matches_2025_2026.json")
    rows = []
    for match in load_json(path):
        rows.append({
            "sb_id":          match.get("match_id"),
            "match_date":     match.get("match_date"),
            "kick_off":       match.get("kick_off"),
            "competition":    match.get("competition"),
            "season":         match.get("season"),
            "home_score":     match.get("home_score"),
            "away_score":     match.get("away_score"),
            "attendance":          match.get("attendance"),
            "behind_closed_doors": match.get("behind_closed_doors"),
            "neutral_ground":      match.get("neutral_ground"),
            "match_week":          match.get("match_week"),
            "competition_stage": match.get("competition_stage"),
            "stadium":        match.get("stadium"),
            "referee":        match.get("referee"),
            "home_managers":  match.get("home_managers"),
            "away_managers":  match.get("away_managers"),
        })
    return pd.DataFrame(rows).drop_duplicates("sb_id")


def load_match_skillcorner(sc_id, sc_team_lookup):
    # Load a single SkillCorner match file and extract required fields
    path = os.path.join(RAW, "skillcorner", "matches", f"match_{sc_id}.json")
    if not os.path.exists(path):
        return {}

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Duration of each period
    period_1_minutes = None
    period_2_minutes = None
    for period in data.get("match_periods", []):
        if period.get("period") == 1:
            period_1_minutes = period.get("duration_minutes")
        elif period.get("period") == 2:
            period_2_minutes = period.get("duration_minutes")

    # Resolve SkillCorner team IDs to our internal team IDs
    home_sc_id = data.get("home_team", {}).get("id")
    away_sc_id = data.get("away_team", {}).get("id")

    return {
        "period_1_minutes": period_1_minutes,
        "period_2_minutes": period_2_minutes,
        "pitch_length":     data.get("pitch_length"),
        "pitch_width":      data.get("pitch_width"),
        "home_team_id":     sc_team_lookup.get(home_sc_id),
        "away_team_id":     sc_team_lookup.get(away_sc_id),
    }


def process_matches():
    matches_mapping = pd.read_csv(os.path.join(RAW, "mapping", "matches_mapping.csv"))
    teams_mapping   = pd.read_csv(os.path.join(RAW, "mapping", "teams_mapping.csv"))

    # Build a lookup: SkillCorner team sc_id -> our internal team id
    sc_team_lookup = dict(zip(teams_mapping["sc_id"], teams_mapping["id"]))

    # Load StatsBomb matches and join on mapping
    sb_df  = load_matches_statsbomb()
    result = matches_mapping.merge(sb_df, on="sb_id", how="left")

    # Load SkillCorner data match by match and attach to result
    sc_rows = []
    for _, row in matches_mapping.iterrows():
        sc_data = load_match_skillcorner(row["sc_id"], sc_team_lookup)
        sc_data["sc_id"] = row["sc_id"]
        sc_rows.append(sc_data)

    sc_df  = pd.DataFrame(sc_rows).drop_duplicates("sc_id")
    result = result.merge(sc_df, on="sc_id", how="left")

    # Remove source ID columns (handled by the mapping table)
    source_cols = ["sb_id", "sc_id", "date"]
    result = result.drop(columns=[c for c in source_cols if c in result.columns])

    # Reorder columns
    identity_cols = [
        "id",
        "match_date", "kick_off",
        "competition", "season", "match_week", "competition_stage",
        "home_team_id", "away_team_id",
        "home_score", "away_score",
        "period_1_minutes", "period_2_minutes",
        "pitch_length", "pitch_width",
        "stadium", "referee", "home_managers", "away_managers",
        "attendance", "behind_closed_doors", "neutral_ground",
    ]
    identity_cols = [c for c in identity_cols if c in result.columns]
    remaining     = [c for c in result.columns if c not in identity_cols]
    result        = result[identity_cols + remaining]

    save_csv(result, "matches")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    process_teams()
    process_players()
    process_matches()