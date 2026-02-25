# ⚽ Football Multi-Source Database — Ligue 1 2025/2026

A data engineering project that ingests, maps, and centralizes football data from three external sources (StatsBomb, SkillCorner, Transfermarkt) into a unified PostgreSQL database.

📊 **Interactive DB Schema**: [View on dbdiagram.io](https://dbdiagram.io/d/699b8fd2bd82f5fce272c27a)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Data Sources](#data-sources)
3. [Project Structure](#project-structure)
4. [Database Schema](#database-schema)
5. [How to Run](#how-to-run)
6. [Pipeline Reference](#pipeline-reference)
7. [Technical Choices](#technical-choices)
8. [Docker Setup](#docker-setup)

---

## Project Overview

The goal of this project is to build a clean, relational database that unifies football data from multiple providers. Each source covers a different dimension of the game:

- **StatsBomb** → event-level data (passes, shots, carries, pressures…)
- **SkillCorner** → athletic / physical tracking data
- **Transfermarkt** → contextual player data (market value, contract, age…)

The central challenge is **cross-source identity resolution**: the same player or match has a different ID in each system. This project solves that with dedicated mapping tables and a fuzzy-matching pipeline.

---

## Data Sources

| Source | Access | Data |
|---|---|---|
| **StatsBomb** | `statsbombpy` Python library + REST API | Match events, player stats, team stats |
| **SkillCorner** | `skillcorner.client` Python module | Physical tracking (distance, sprints, PPDA…) |
| **Transfermarkt** | Web scraping (`requests` + `BeautifulSoup`) | Market value, contract end, nationality, age |

---

## Project Structure

```
project/
│
├── data/
│   ├── raw/
│   │   ├── statsbomb/
│   │   │   ├── players/        # ligue1_players_2025_2026.json
│   │   │   ├── teams/          # ligue1_teams_2025_2026.json
│   │   │   ├── matches/        # ligue1_matches_2025_2026.json
│   │   │   └── events/         # match_<id>_events.json (one per match)
│   │   ├── skillcorner/
│   │   │   ├── players/
│   │   │   ├── teams/
│   │   │   └── matches/
│   │   ├── transfermarkt/
│   │   │   ├── ligue1_players_2025_2026.csv
│   │   │   └── ligue1_teams_2025_2026.csv
│   │   └── mapping/
│   │       ├── teams_mapping.csv
│   │       ├── players_mapping.csv
│   │       └── matches_mapping.csv
│   └── processed/              # Clean CSVs ready for DB injection
│
├── scripts/
│   ├── ingest_statsbomb.py
│   ├── ingest_skillcorner.py
│   ├── ingest_transfermarkt.py
│   ├── mapping_teams.py
│   ├── mapping_players.py
│   ├── mapping_matches.py
│   ├── process_data.py
│   ├── pipeline.py
│   └── db_connection.py
│
├── database/
│   ├── create_database.py
│   └── inject_processed_data_in_database.py
│
├── .env
└── README.md
```

---

## Database Schema

### Overview

The schema is organized around a central `matches` table, with `players` and `teams` as the core entities. Mapping tables handle cross-source ID resolution cleanly and separately from the business data.

```
edition
  └── matches
        ├── teams (home / away)
        ├── match_players (lineup per match)
        ├── events
        │     ├── events_shot
        │     ├── events_pass
        │     ├── events_carry
        │     ├── events_pressure
        │     └── events_ball_recovery
        └── physical

players ──── players_mapping (sb_id, sc_id, tm_id)
teams   ──── teams_mapping   (sb_id, sc_id, tm_id)
matches ──── matches_mapping (sb_id, sc_id)
```

### Core Tables

**`edition`** — Season reference  
Stores the competition and season (e.g. Ligue 1 2025/2026). Acts as the root anchor for all match data.

**`teams`** — Club reference  
One row per club. Unified across all three sources via `teams_mapping`.

**`players`** — Player reference  
The most important table. Merges biographical and contextual data from StatsBomb, SkillCorner, and Transfermarkt: name, birth date, nationality, position, market value, contract expiry, etc.

**`matches`** — Match reference  
One row per played match: date, home/away team, score, matchday, referee, stadium.

**`match_players`** — Lineups  
Junction table linking players to matches (starting XI + substitutes, with jersey numbers and minutes played).

**`events`** — StatsBomb event stream  
One row per in-game event (every pass, shot, duel, carry…). References the match and the player. Sub-tables contain type-specific attributes:

| Sub-table | Content |
|---|---|
| `events_shot` | xG, technique, body part, freeze frame |
| `events_pass` | length, angle, height, switch, cross, assist |
| `events_carry` | distance, end location, progressive |
| `events_pressure` | regain, high press |
| `events_ball_recovery` | recovery failure |

**`physical`** — SkillCorner tracking data  
Per-player per-match physical metrics: total distance, high-speed running, sprint distance, PPDA, etc.

### Mapping Tables

These tables are the backbone of cross-source identity resolution. They are kept separate from the business tables so the core data stays clean.

| Table | Keys |
|---|---|
| `teams_mapping` | `sb_id`, `sc_id`, `tm_id` |
| `players_mapping` | `sb_id`, `sc_id`, `tm_id` |
| `matches_mapping` | `sb_id`, `sc_id` |

---

## How to Run

### 1. Prerequisites

```bash
pip install -r requirements.txt
```

### 2. Environment Variables

Create a `.env` file at the project root:

```env
STATSBOMB_USERNAME=your_username
STATSBOMB_PASSWORD=your_password

SKILLCORNER_USERNAME=your_username
SKILLCORNER_PASSWORD=your_password

DB_HOST=localhost
DB_PORT=5432
DB_NAME=fc_metz
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Full Pipeline (recommended)

Run everything end-to-end (ingest → map → process → create DB → inject):

```bash
python scripts/pipeline.py --all
```

### 4. Step by Step

```bash
# Ingest data from each source
python scripts/pipeline.py --ingest statsbomb --all
python scripts/pipeline.py --ingest skillcorner --all
python scripts/pipeline.py --ingest transfermarkt --all

# Build cross-source ID mappings
python scripts/pipeline.py --mapping teams players matches

# Create database tables (drops and recreates)
python scripts/pipeline.py --create-db

# Inject processed CSVs into the database
python scripts/pipeline.py --inject
```

---

## Pipeline Reference

The `pipeline.py` orchestrator accepts flexible combinations of flags:

```bash
# Re-ingest specific events only
python scripts/pipeline.py --ingest statsbomb --events 3935583 3935584

# Test mode: limit to first N matches
python scripts/pipeline.py --ingest statsbomb --all --limit 5

# Multiple ingest blocks in one call
python scripts/pipeline.py \
  --ingest skillcorner --players 14 18 --teams \
  --ingest statsbomb --players --events 3935583

# Rebuild mapping tables only
python scripts/pipeline.py --mapping players teams
```

The events ingestion (`--events` without IDs) is **resumable**: it skips matches already downloaded. Providing specific IDs forces a re-download.

A timestamped log file is automatically saved to `data/raw/pipeline_YYYYMMDD_HHMMSS.txt` on each run.

---

## Technical Choices

### PostgreSQL

PostgreSQL was chosen for its robustness with relational data, native support for JSON columns (useful for raw event payloads), and its wide ecosystem. The schema is fully normalized — mapping tables are separate from business tables, making it easy to add a new data source without restructuring existing tables.

### Cross-Source Identity Resolution

Each source uses its own internal IDs with no shared key. The matching strategy is layered:

- **Teams**: bijective matching using the Hungarian algorithm (via `scipy.optimize.linear_sum_assignment`) on fuzzy name similarity scores — guarantees a globally optimal 1-to-1 assignment across all 18 clubs.
- **Players**: multi-pass fuzzy matching combining normalized name similarity (`rapidfuzz`) and date of birth. Passes in order: exact match → fuzzy name only (≥95) → fuzzy name + same DOB (≥45) → alias + DOB (≥95). This handles nicknames, accents, and compound names.
- **Matches**: matched by (home team sc_id, away team sc_id, date) after teams are already resolved.

### Separation of Raw / Processed Data

Raw data from all three sources is always saved to disk first (`data/raw/`) before any transformation. Processing into clean, DB-ready CSVs is a separate step (`process_data.py`). This separation means the ingestion layer can be re-run independently from the transformation layer, and raw files act as a local cache — avoiding unnecessary API calls or scraping runs. It also makes debugging easier: if a transformation produces unexpected results, the raw source files are always available for inspection without re-fetching.

### Resumable Ingestion

Event ingestion is designed to be resumable: already-downloaded match files are skipped unless specific match IDs are passed (force re-download). This makes the pipeline safe to interrupt and restart without duplicating work.

---

## Docker Setup

This project can be run entirely via Docker — no Python, PostgreSQL, or pgAdmin installation required on your machine.

### Prerequisites

Install **Docker Desktop**: https://www.docker.com/products/docker-desktop/  
Launch it and wait for the whale icon in the taskbar to be stable.

### 1. Fill in the `.env` file

Open the `.env` file and fill in your API credentials. Make sure to wrap values in single quotes to avoid issues with special characters:

```env
STATSBOMB_USERNAME='your_email'
STATSBOMB_PASSWORD='your_password'
SKILLCORNER_USERNAME='your_login'
SKILLCORNER_PASSWORD='your_password'
```

The DB section is already pre-configured for Docker — do not modify it:

```env
DB_HOST=db
DB_PORT=5432
DB_NAME=fc_metz_mini_projet
DB_USER=postgres
DB_PASSWORD=postgres
```

### 2. Start the database and pgAdmin

```bash
docker-compose up -d db pgadmin
```

### 3. Run the pipeline

```bash
docker-compose run --rm pipeline python scripts/pipeline.py --all
```

This step fetches all data, builds the mappings, creates the database schema, and injects everything. It takes approximately **30–40 minutes** depending on your internet connection. Generated data files are saved locally in the `data/` folder.

### 4. Visualize the database

1. Open **http://localhost:5050** in your browser
2. Log in with `admin@fc-metz.fr` / `admin`
3. Right-click on **Servers** → **Register** → **Server**
4. Fill in:
   - **General** tab → Name: `fc_metz`
   - **Connection** tab:
     - Host: `db`
     - Port: `5432`
     - Database: `fc_metz_mini_projet`
     - Username: `postgres`
     - Password: `postgres`
5. Click **Save** — the database is now accessible

### 5. Browse the tables

In pgAdmin, expand the tree:  
**Servers → fc_metz → Databases → fc_metz_mini_projet → Schemas → public → Tables**

To view the content of a table: right-click on it → **View/Edit Data** → **All Rows**.

### Useful commands

```bash
# Stop all containers
docker-compose down

# Stop and delete the database volume (full reset)
docker-compose down -v

# View pipeline logs
docker-compose logs -f pipeline
```
