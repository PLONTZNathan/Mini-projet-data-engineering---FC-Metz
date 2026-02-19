"""
build_players_mapping.py
------------------------
Construit la table de correspondance des IDs joueurs entre :
  - StatsBomb  (player_id)
  - SkillCorner (id)
  - Transfermarkt (id)

Matching par : nom normalisé + date de naissance (exact puis fuzzy)

OUTPUT
------
  data/raw/mapping/players_mapping.csv
"""

import re
import csv
import json
import unicodedata
from pathlib import Path
from rapidfuzz import fuzz

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent

SB_PLAYERS_JSON = ROOT / "data" / "raw" / "statsbomb"   / "players" / "ligue1_players_2025_2026.json"
SC_PLAYERS_JSON = ROOT / "data" / "raw" / "skillcorner" / "players" / "ligue1_players_2025_2026.json"
TM_PLAYERS_CSV  = ROOT / "data" / "raw" / "transfermarkt" / "ligue1_players_2025_complete.csv"

OUTPUT_DIR = ROOT / "data" / "raw" / "mapping"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "players_mapping.csv"

FIELDNAMES = [
    "sb_id",  "sb_name",  "sb_birth_date",
    "sc_id",  "sc_name",  "sc_birth_date",
    "tm_id",  "tm_name",  "tm_birth_date",
    "match_sb_sc", "match_sb_tm", "match_sc_tm",
]

# -----------------------------------------------------------------------------
# NORMALISATION
# -----------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z ]", "", name)
    return " ".join(name.split())


def normalize_date(date_str: str) -> str:
    if not date_str:
        return ""
    date_str = str(date_str).strip()
    if re.match(r"\d{2}/\d{2}/\d{4}", date_str):
        d, m, y = date_str.split("/")
        return f"{y}-{m}-{d}"
    if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
        return date_str[:10]
    return date_str


# -----------------------------------------------------------------------------
# LOADERS
# -----------------------------------------------------------------------------

def load_statsbomb():
    with open(SB_PLAYERS_JSON, encoding="utf-8") as f:
        data = json.load(f)
    players = []
    for p in data:
        players.append({
            "sb_id":         p["player_id"],
            "sb_name":       p.get("player_name", ""),
            "sb_birth_date": normalize_date(p.get("birth_date", "")),
            "_norm_name":    normalize_name(p.get("player_name", "")),
        })
    print(f"[SB]  {len(players)} joueurs chargés")
    return players


def load_skillcorner():
    with open(SC_PLAYERS_JSON, encoding="utf-8") as f:
        data = json.load(f)
    players = []
    for p in data:
        full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        players.append({
            "sc_id":         p["id"],
            "sc_name":       p.get("short_name", full_name),
            "sc_birth_date": normalize_date(p.get("birthday", "")),
            "_norm_name":    normalize_name(full_name),
        })
    print(f"[SC]  {len(players)} joueurs chargés")
    return players


def load_transfermarkt():
    players = []
    with open(TM_PLAYERS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            players.append({
                "tm_id":         row["id"],
                "tm_name":       row["name"],
                "tm_birth_date": normalize_date(row.get("birth_date", "")),
                "_norm_name":    normalize_name(row["name"]),
            })
    print(f"[TM]  {len(players)} joueurs chargés")
    return players


# -----------------------------------------------------------------------------
# MATCHING
# -----------------------------------------------------------------------------

def name_similarity(a: str, b: str) -> int:
    """
    Similarité entre deux noms via thefuzz (distance de Levenshtein).
    - token_sort_ratio : insensible à l'ordre des mots
    - token_set_ratio  : insensible aux mots supplémentaires (noms composés longs)
    On prend le max des deux pour couvrir :
      "angel gomes" vs "adilson angel abreu de almeida gomes"  -> token_set_ratio élevé
      "pierre lees melou" vs "pierre lees-melou"               -> token_sort_ratio élevé
    Retourne un score entre 0 et 100.
    """
    return max(
        fuzz.token_sort_ratio(a, b),
        fuzz.token_set_ratio(a, b)
    )


def match_players(source, target, source_key, target_key):
    """
    1. Exact match sur (nom normalisé, date de naissance)
    2. Fuzzy nom seul >= 95 : nom très similaire sans vérification de date
    3. Fuzzy nom + date   : même date de naissance + score nom >= 60
    Retourne un dict {source_id: (target_player, method)}.
    """
    exact_index = {}
    for p in target:
        key = (p["_norm_name"], p[f"{target_key}_birth_date"])
        if key not in exact_index:
            exact_index[key] = p

    date_index = {}
    for p in target:
        d = p[f"{target_key}_birth_date"]
        date_index.setdefault(d, []).append(p)

    matched = {}
    for p in source:
        sid  = p[f"{source_key}_id"]
        name = p["_norm_name"]
        date = p[f"{source_key}_birth_date"]

        # 1. Exact match (nom + date)
        if (name, date) in exact_index:
            matched[sid] = (exact_index[(name, date)], "exact")
            continue

        # 2. Fuzzy nom seul >= 95 (nom très similaire, date non requise)
        best_score     = 0.0
        best_candidate = None
        for candidate in target:
            score = name_similarity(name, candidate["_norm_name"])
            if score > best_score:
                best_score     = score
                best_candidate = candidate
        if best_score >= 95 and best_candidate:
            matched[sid] = (best_candidate, "fuzzy_name")
            continue

        # 3. Fuzzy nom + date : même date, seuil abaissé à 60
        if date and date in date_index:
            best_score     = 0.0
            best_candidate = None
            for candidate in date_index[date]:
                score = name_similarity(name, candidate["_norm_name"])
                if score > best_score:
                    best_score     = score
                    best_candidate = candidate
            if best_score >= 60 and best_candidate:
                matched[sid] = (best_candidate, "fuzzy_date")

    return matched


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("BUILD PLAYERS MAPPING")
    print("=" * 60)

    sb_players = load_statsbomb()
    sc_players = load_skillcorner()
    tm_players = load_transfermarkt()

    sb_to_sc = match_players(sb_players, sc_players, "sb", "sc")
    sb_to_tm = match_players(sb_players, tm_players, "sb", "tm")
    sc_to_tm = match_players(sc_players, tm_players, "sc", "tm")

    print(f"\nJoueurs par source :")
    print(f"  StatsBomb    : {len(sb_players)}")
    print(f"  SkillCorner  : {len(sc_players)}")
    print(f"  Transfermarkt: {len(tm_players)}")

    exact_sb_sc = sum(1 for m, method in sb_to_sc.values() if method == "exact")
    fuzzy_sb_sc = sum(1 for m, method in sb_to_sc.values() if method == "fuzzy")
    exact_sb_tm = sum(1 for m, method in sb_to_tm.values() if method == "exact")
    fuzzy_sb_tm = sum(1 for m, method in sb_to_tm.values() if method == "fuzzy")

    print(f"\nMatching SB <-> SC : {len(sb_to_sc)}/{len(sb_players)}  (exact: {exact_sb_sc}, fuzzy: {fuzzy_sb_sc})")
    print(f"Matching SB <-> TM : {len(sb_to_tm)}/{len(sb_players)}  (exact: {exact_sb_tm}, fuzzy: {fuzzy_sb_tm})")
    print(f"Matching SC <-> TM : {len(sc_to_tm)}/{len(sc_players)}")

    # Construit les lignes
    rows    = []
    seen_sc = set()
    seen_tm = set()

    for sb in sb_players:
        sb_id      = sb["sb_id"]
        sc, _      = sb_to_sc.get(sb_id, (None, None))
        tm, _      = sb_to_tm.get(sb_id, (None, None))

        if sc:
            seen_sc.add(sc["sc_id"])
        if tm:
            seen_tm.add(tm["tm_id"])

        rows.append({
            "sb_id":         sb_id,
            "sb_name":       sb["sb_name"],
            "sb_birth_date": sb["sb_birth_date"],
            "sc_id":         sc["sc_id"]        if sc else "",
            "sc_name":       sc["sc_name"]       if sc else "",
            "sc_birth_date": sc["sc_birth_date"] if sc else "",
            "tm_id":         tm["tm_id"]         if tm else "",
            "tm_name":       tm["tm_name"]        if tm else "",
            "tm_birth_date": tm["tm_birth_date"] if tm else "",
            "match_sb_sc":   "yes" if sc else "no",
            "match_sb_tm":   "yes" if tm else "no",
            "match_sc_tm":   "",
        })

    for sc in sc_players:
        if sc["sc_id"] in seen_sc:
            continue
        tm, _ = sc_to_tm.get(sc["sc_id"], (None, None))
        if tm:
            seen_tm.add(tm["tm_id"])
        rows.append({
            "sb_id": "", "sb_name": "", "sb_birth_date": "",
            "sc_id":         sc["sc_id"],
            "sc_name":       sc["sc_name"],
            "sc_birth_date": sc["sc_birth_date"],
            "tm_id":         tm["tm_id"]         if tm else "",
            "tm_name":       tm["tm_name"]        if tm else "",
            "tm_birth_date": tm["tm_birth_date"] if tm else "",
            "match_sb_sc": "no",
            "match_sb_tm": "no",
            "match_sc_tm": "yes" if tm else "no",
        })

    for tm in tm_players:
        if tm["tm_id"] in seen_tm:
            continue
        rows.append({
            "sb_id": "", "sb_name": "", "sb_birth_date": "",
            "sc_id": "", "sc_name": "", "sc_birth_date": "",
            "tm_id":         tm["tm_id"],
            "tm_name":       tm["tm_name"],
            "tm_birth_date": tm["tm_birth_date"],
            "match_sb_sc": "no",
            "match_sb_tm": "no",
            "match_sc_tm": "no",
        })

    for row in rows:
        if row["match_sc_tm"] == "":
            row["match_sc_tm"] = "yes" if (row["sc_id"] and row["tm_id"]) else "no"

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    total     = len(rows)
    all_three = sum(1 for r in rows if r["sb_id"] and r["sc_id"] and r["tm_id"])
    sb_sc_only= sum(1 for r in rows if r["sb_id"] and r["sc_id"] and not r["tm_id"])
    sb_tm_only= sum(1 for r in rows if r["sb_id"] and r["tm_id"] and not r["sc_id"])
    sb_only   = sum(1 for r in rows if r["sb_id"] and not r["sc_id"] and not r["tm_id"])
    sc_only   = sum(1 for r in rows if r["sc_id"] and not r["sb_id"] and not r["tm_id"])
    tm_only   = sum(1 for r in rows if r["tm_id"] and not r["sb_id"] and not r["sc_id"])

    print("\n" + "=" * 60)
    print("RÉSUMÉ")
    print("=" * 60)
    print(f"Total lignes       : {total}")
    print(f"SB + SC + TM       : {all_three}")
    print(f"SB + SC seulement  : {sb_sc_only}")
    print(f"SB + TM seulement  : {sb_tm_only}")
    print(f"SB seul            : {sb_only}")
    print(f"SC seul            : {sc_only}")
    print(f"TM seul            : {tm_only}")
    print(f"\nOutput : {OUTPUT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()