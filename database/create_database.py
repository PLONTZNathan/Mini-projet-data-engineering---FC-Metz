# Script : create_database.py
# Location : scripts/create_database.py
#
# Creates all tables in the fc_metz PostgreSQL database.
# Imports the connection from db_connection.py.
#
# Usage:
#   python scripts/create_database.py

from db_connection import get_connection

# -----------------------------------------------------------------------------
# TABLE DEFINITIONS
# Order matters: referenced tables must be created before referencing tables
# -----------------------------------------------------------------------------

TABLES = [

    # ── Edition ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS edition (
        id          INTEGER PRIMARY KEY,
        competition VARCHAR(100) NOT NULL,
        season      VARCHAR(20)  NOT NULL
    );
    """,

    # ── Teams ─────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS teams (
        id                                          INTEGER PRIMARY KEY,
        team                                        VARCHAR(100),
        edition_id                                  INTEGER REFERENCES edition(id),
        team_female                                 BOOLEAN,
        city                                        VARCHAR(100),
        stadium_name                                VARCHAR(150),
        stadium_capacity                            INTEGER,
        squad_size                                  INTEGER,
        average_age                                 FLOAT,
        national_team_players                       INTEGER,
        table_position                              INTEGER,
        years_in_league                             INTEGER,
        team_season_matches                         INTEGER,
        team_season_gd                              INTEGER,
        team_season_xgd                             FLOAT,
        team_season_np_shots_pg                     FLOAT,
        team_season_op_shots_pg                     FLOAT,
        team_season_op_shots_outside_box_pg         FLOAT,
        team_season_sp_shots_pg                     FLOAT,
        team_season_np_xg_pg                        FLOAT,
        team_season_op_xg_pg                        FLOAT,
        team_season_sp_xg_pg                        FLOAT,
        team_season_np_xg_per_shot                  FLOAT,
        team_season_np_shot_distance                FLOAT,
        team_season_op_shot_distance                FLOAT,
        team_season_sp_shot_distance                FLOAT,
        team_season_possessions                     FLOAT,
        team_season_possession                      FLOAT,
        team_season_directness                      FLOAT,
        team_season_pace_towards_goal               FLOAT,
        team_season_gk_pass_distance                FLOAT,
        team_season_gk_long_pass_ratio              FLOAT,
        team_season_box_cross_ratio                 FLOAT,
        team_season_passes_inside_box_pg            FLOAT,
        team_season_defensive_distance              FLOAT,
        team_season_ppda                            FLOAT,
        team_season_defensive_distance_ppda         FLOAT,
        team_season_opp_passing_ratio               FLOAT,
        team_season_opp_final_third_pass_ratio      FLOAT,
        team_season_np_shots_conceded_pg            FLOAT,
        team_season_op_shots_conceded_pg            FLOAT,
        team_season_op_shots_conceded_outside_box_pg FLOAT,
        team_season_sp_shots_conceded_pg            FLOAT,
        team_season_np_xg_conceded_pg               FLOAT,
        team_season_op_xg_conceded_pg               FLOAT,
        team_season_sp_xg_conceded_pg               FLOAT,
        team_season_np_xg_per_shot_conceded         FLOAT,
        team_season_np_shot_distance_conceded       FLOAT,
        team_season_op_shot_distance_conceded       FLOAT,
        team_season_sp_shot_distance_conceded       FLOAT,
        team_season_deep_completions_conceded_pg    FLOAT,
        team_season_passes_inside_box_conceded_pg   FLOAT,
        team_season_corners_pg                      FLOAT,
        team_season_corner_xg_pg                    FLOAT,
        team_season_xg_per_corner                   FLOAT,
        team_season_free_kicks_pg                   FLOAT,
        team_season_free_kick_xg_pg                 FLOAT,
        team_season_xg_per_free_kick                FLOAT,
        team_season_direct_free_kicks_pg            FLOAT,
        team_season_direct_free_kick_xg_pg          FLOAT,
        team_season_xg_per_direct_free_kick         FLOAT,
        team_season_throw_ins_pg                    FLOAT,
        team_season_throw_in_xg_pg                  FLOAT,
        team_season_xg_per_throw_in                 FLOAT,
        team_season_ball_in_play_time               FLOAT,
        team_season_counter_attacking_shots_pg      FLOAT,
        team_season_high_press_shots_pg             FLOAT,
        team_season_shots_in_clear_pg               FLOAT,
        team_season_counter_attacking_shots_conceded_pg FLOAT,
        team_season_shots_in_clear_conceded_pg      FLOAT,
        team_season_aggressive_actions_pg           FLOAT,
        team_season_aggression                      FLOAT,
        team_season_goals_pg                        FLOAT,
        team_season_own_goals_pg                    FLOAT,
        team_season_penalty_goals_pg                FLOAT,
        team_season_goals_conceded_pg               FLOAT,
        team_season_opposition_own_goals_pg         FLOAT,
        team_season_penalty_goals_conceded_pg       FLOAT,
        team_season_gd_pg                           FLOAT,
        team_season_np_gd_pg                        FLOAT,
        team_season_xgd_pg                          FLOAT,
        team_season_np_xgd_pg                       FLOAT,
        team_season_deep_completions_pg             FLOAT,
        team_season_passing_ratio                   FLOAT,
        team_season_pressures_pg                    FLOAT,
        team_season_counterpressures_pg             FLOAT,
        team_season_pressure_regains_pg             FLOAT,
        team_season_counterpressure_regains_pg      FLOAT,
        team_season_defensive_action_regains_pg     FLOAT,
        team_season_yellow_cards_pg                 FLOAT,
        team_season_second_yellow_cards_pg          FLOAT,
        team_season_red_cards_pg                    FLOAT,
        team_season_fhalf_pressures_pg              FLOAT,
        team_season_fhalf_counterpressures_pg       FLOAT,
        team_season_fhalf_pressures_ratio           FLOAT,
        team_season_fhalf_counterpressures_ratio    FLOAT,
        team_season_crosses_into_box_pg             FLOAT,
        team_season_successful_crosses_into_box_pg  FLOAT,
        team_season_successful_box_cross_ratio      FLOAT,
        team_season_deep_progressions_pg            FLOAT,
        team_season_deep_progressions_conceded_pg   FLOAT,
        team_season_obv_pg                          FLOAT,
        team_season_obv_pass_pg                     FLOAT,
        team_season_obv_shot_pg                     FLOAT,
        team_season_obv_defensive_action_pg         FLOAT,
        team_season_obv_dribble_carry_pg            FLOAT,
        team_season_obv_gk_pg                       FLOAT,
        team_season_obv_conceded_pg                 FLOAT,
        team_season_passes_pg                       FLOAT,
        team_season_successful_passes_pg            FLOAT,
        team_season_passes_conceded_pg              FLOAT,
        team_season_successful_passes_conceded_pg   FLOAT,
        team_season_op_passes_pg                    FLOAT,
        team_season_op_passes_conceded_pg           FLOAT,
        team_season_penalties_won_pg                FLOAT,
        team_season_penalties_conceded_pg           FLOAT,
        team_season_completed_dribbles_pg           FLOAT,
        team_season_dribble_ratio                   FLOAT,
        team_season_completed_dribbles_conceded_pg  FLOAT,
        team_season_opposition_dribble_ratio        FLOAT,
        team_season_high_press_shots_conceded_pg    FLOAT,
        team_season_sp_pg                           FLOAT,
        team_season_sp_goals_pg                     FLOAT,
        team_season_sp_pg_conceded                  FLOAT,
        team_season_sp_goals_pg_conceded            FLOAT
    );
    """,

    # ── Players ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS players (
        id                                          INTEGER PRIMARY KEY,
        player_name                                 VARCHAR(150),
        player_first_name                           VARCHAR(100),
        player_last_name                            VARCHAR(100),
        player_known_name                           VARCHAR(100),
        team_id                                     INTEGER REFERENCES teams(id),
        edition_id                                  INTEGER REFERENCES edition(id),
        player_female                               BOOLEAN,
        birth_date                                  DATE,
        age                                         INTEGER,
        birth_place                                 VARCHAR(100),
        nationalities                               VARCHAR(200),
        player_height                               FLOAT,
        player_weight                               FLOAT,
        primary_position                            VARCHAR(100),
        secondary_position                          VARCHAR(100),
        shirt_number                                INTEGER,
        joined_date                                 DATE,
        contract_end                                DATE,
        market_value_m                              FLOAT,
        matches                                     INTEGER,
        goals                                       INTEGER,
        assists                                     INTEGER,
        minutes                                     INTEGER,
        yellow_cards                                INTEGER,
        red_cards                                   INTEGER,
        player_season_minutes                       FLOAT,
        player_season_np_xg_per_shot                FLOAT,
        player_season_np_xg_90                      FLOAT,
        player_season_np_shots_90                   FLOAT,
        player_season_goals_90                      FLOAT,
        player_season_npga_90                       FLOAT,
        player_season_xa_90                         FLOAT,
        player_season_key_passes_90                 FLOAT,
        player_season_op_key_passes_90              FLOAT,
        player_season_assists_90                    FLOAT,
        player_season_through_balls_90              FLOAT,
        player_season_passes_into_box_90            FLOAT,
        player_season_touches_inside_box_90         FLOAT,
        player_season_tackles_90                    FLOAT,
        player_season_interceptions_90              FLOAT,
        player_season_tackles_and_interceptions_90  FLOAT,
        player_season_padj_tackles_90               FLOAT,
        player_season_padj_interceptions_90         FLOAT,
        player_season_padj_tackles_and_interceptions_90 FLOAT,
        player_season_challenge_ratio               FLOAT,
        player_season_dribbles_90                   FLOAT,
        player_season_fouls_90                      FLOAT,
        player_season_dribbled_past_90              FLOAT,
        player_season_dispossessions_90             FLOAT,
        player_season_long_ball_ratio               FLOAT,
        player_season_long_balls_90                 FLOAT,
        player_season_clearance_90                  FLOAT,
        player_season_aerial_ratio                  FLOAT,
        player_season_aerial_wins_90                FLOAT,
        player_season_op_passes_90                  FLOAT,
        player_season_passing_ratio                 FLOAT,
        player_season_npg_90                        FLOAT,
        player_season_crosses_90                    FLOAT,
        player_season_xgchain_90                    FLOAT,
        player_season_xgbuildup_90                  FLOAT,
        player_season_pressures_90                  FLOAT,
        player_season_pressure_regains_90           FLOAT,
        player_season_deep_progressions_90          FLOAT,
        player_season_carries_90                    FLOAT,
        player_season_yellow_cards_90               FLOAT,
        player_season_red_cards_90                  FLOAT,
        player_season_obv_90                        FLOAT,
        player_season_obv_pass_90                   FLOAT,
        player_season_obv_shot_90                   FLOAT,
        player_season_obv_dribble_carry_90          FLOAT,
        player_season_appearances                   INTEGER,
        player_season_starting_appearances          INTEGER,
        player_season_average_minutes               FLOAT,
        player_season_90s_played                    FLOAT,
        player_season_most_recent_match             TIMESTAMP,
        player_season_positive_outcome_score        FLOAT,
        player_season_shots_faced_90                FLOAT,
        player_season_goals_faced_90                FLOAT,
        player_season_np_xg_faced_90                FLOAT,
        player_season_save_ratio                    FLOAT,
        player_season_gsaa_90                       FLOAT
    );
    """,

    # ── Matches ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS matches (
        id                  INTEGER PRIMARY KEY,
        match_date          DATE,
        kick_off            TIME,
        edition_id          INTEGER REFERENCES edition(id),
        match_week          INTEGER,
        competition_stage   VARCHAR(100),
        home_team_id        INTEGER REFERENCES teams(id),
        away_team_id        INTEGER REFERENCES teams(id),
        home_score          INTEGER,
        away_score          INTEGER,
        period_1_minutes    FLOAT,
        period_2_minutes    FLOAT,
        pitch_length        FLOAT,
        pitch_width         FLOAT,
        stadium             VARCHAR(150),
        referee             VARCHAR(150),
        home_managers       VARCHAR(150),
        away_managers       VARCHAR(150),
        attendance          INTEGER,
        behind_closed_doors BOOLEAN,
        neutral_ground      BOOLEAN
    );
    """,

    # ── Match Players ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS match_players (
        match_id            INTEGER REFERENCES matches(id),
        player_id           INTEGER REFERENCES players(id),
        is_home_player      BOOLEAN,
        position_group      VARCHAR(100),
        position            VARCHAR(100),
        shirt_number        INTEGER,
        start_time          TIME,
        end_time            TIME,
        minutes_played      FLOAT,
        period_1_minutes    FLOAT,
        period_2_minutes    FLOAT,
        goals               INTEGER,
        own_goals           INTEGER,
        yellow_cards        INTEGER,
        red_cards           INTEGER,
        injured             BOOLEAN,
        PRIMARY KEY (match_id, player_id)
    );
    """,

    # ── Teams Mapping ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS teams_mapping (
        id      INTEGER PRIMARY KEY REFERENCES teams(id),
        sb_id   INTEGER,
        sc_id   INTEGER,
        tm_id   INTEGER
    );
    """,

    # ── Players Mapping ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS players_mapping (
        id      INTEGER PRIMARY KEY REFERENCES players(id),
        sb_id   INTEGER,
        sc_id   INTEGER,
        tm_id   INTEGER
    );
    """,

    # ── Matches Mapping ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS matches_mapping (
        id      INTEGER PRIMARY KEY REFERENCES matches(id),
        sb_id   INTEGER,
        sc_id   INTEGER
    );
    """,

    # ── Events ────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events (
        id              UUID PRIMARY KEY,
        match_id        INTEGER,
        player_id       INTEGER,
        period          INTEGER,
        minute          INTEGER,
        second          INTEGER,
        timestamp       TIME,
        type            VARCHAR(50),
        play_pattern    VARCHAR(100),
        possession      INTEGER,
        possession_team VARCHAR(100),
        location_x      FLOAT,
        location_y      FLOAT,
        duration        FLOAT,
        under_pressure  BOOLEAN,
        obv_for_net     FLOAT,
        obv_against_net FLOAT,
        obv_total_net   FLOAT,
        FOREIGN KEY (match_id, player_id) REFERENCES match_players(match_id, player_id)
    );
    """,

    # ── Events Shot ───────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events_shot (
        event_id                        UUID PRIMARY KEY REFERENCES events(id),
        location_z                      FLOAT,
        statsbomb_xg                    FLOAT,
        shot_execution_xg               FLOAT,
        shot_execution_xg_uplift        FLOAT,
        gk_save_difficulty_xg           FLOAT,
        gk_positioning_xg_suppression   FLOAT,
        gk_shot_stopping_xg_suppression FLOAT,
        end_location_x                  FLOAT,
        end_location_y                  FLOAT,
        type                            VARCHAR(100),
        technique                       VARCHAR(100),
        outcome                         VARCHAR(100),
        body_part                       VARCHAR(100),
        first_time                      BOOLEAN,
        follows_dribble                 BOOLEAN,
        key_pass_id                     UUID,
        shot_shot_assist                BOOLEAN
    );
    """,

    # ── Events Pass ───────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events_pass (
        event_id                    UUID PRIMARY KEY REFERENCES events(id),
        recipient_id                INTEGER REFERENCES players(id),
        length                      FLOAT,
        angle                       FLOAT,
        height                      VARCHAR(100),
        end_location_x              FLOAT,
        end_location_y              FLOAT,
        pass_success_probability    FLOAT,
        body_part                   VARCHAR(100),
        type                        VARCHAR(100),
        technique                   VARCHAR(100),
        outcome                     VARCHAR(100),
        "cross"                     BOOLEAN,
        cut_back                    BOOLEAN,
        switch                      BOOLEAN,
        through_ball                BOOLEAN,
        goal_assist                 BOOLEAN,
        shot_assist                 BOOLEAN,
        assisted_shot_id            UUID,
        inswinging                  BOOLEAN,
        deflected                   BOOLEAN,
        aerial_won                  BOOLEAN,
        miscommunication            BOOLEAN,
        pass_cluster_id             INTEGER,
        pass_cluster_label          VARCHAR(100),
        pass_cluster_probability    FLOAT,
        xclaim                      FLOAT
    );
    """,

    # ── Events Carry ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events_carry (
        event_id        UUID PRIMARY KEY REFERENCES events(id),
        end_location_x  FLOAT,
        end_location_y  FLOAT
    );
    """,

    # ── Events Pressure ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events_pressure (
        event_id        UUID PRIMARY KEY REFERENCES events(id),
        counterpress    BOOLEAN
    );
    """,

    # ── Events Ball Recovery ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS events_ball_recovery (
        event_id            UUID PRIMARY KEY REFERENCES events(id),
        recovery_failure    BOOLEAN
    );
    """,

    # ── Physical ──────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS physical (
        id                              SERIAL PRIMARY KEY,
        match_id                        INTEGER,
        player_id                       INTEGER,
        match_count                     INTEGER,
        minutes_full_all                FLOAT,
        physical_check_passed           BOOLEAN,
        total_distance_full_all         FLOAT,
        total_metersperminute_full_all  FLOAT,
        running_distance_full_all       FLOAT,
        hsr_distance_full_all           FLOAT,
        hsr_count_full_all              FLOAT,
        sprint_distance_full_all        FLOAT,
        sprint_count_full_all           FLOAT,
        hi_distance_full_all            FLOAT,
        hi_count_full_all               FLOAT,
        psv99                           FLOAT,
        medaccel_count_full_all         FLOAT,
        highaccel_count_full_all        FLOAT,
        meddecel_count_full_all         FLOAT,
        highdecel_count_full_all        FLOAT,
        explacceltohsr_count_full_all   FLOAT,
        timetohsr                       FLOAT,
        timetohsrpostcod                FLOAT,
        explacceltosprint_count_full_all FLOAT,
        timetosprint                    FLOAT,
        timetosprintpostcod             FLOAT,
        cod_count_full_all              FLOAT,
        timeto505around90               FLOAT,
        timeto505around180              FLOAT,
        FOREIGN KEY (match_id, player_id) REFERENCES match_players(match_id, player_id)
    );
    """,
]


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def create_tables():
    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    for sql in TABLES:
        name = [line.strip() for line in sql.strip().splitlines()
                if line.strip().upper().startswith("CREATE TABLE")][0]
        name = name.split("EXISTS")[-1].strip().split("(")[0].strip()
        cursor.execute(sql)
        print("Created table: " + name)

    cursor.close()
    conn.close()
    print("\nAll tables created successfully.")


if __name__ == "__main__":
    create_tables()