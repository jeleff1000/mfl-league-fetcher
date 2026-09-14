"""Canonical matchup schema — complete union across Yahoo, Sleeper, ESPN.

Every matchup column from every platform normalized into one flat schema.
Fetchers return ONLY raw API data + join keys. All derived columns
(win/loss/tie, margin, rankings, etc.) are SQL enrichments.

Usage:
    from multi_league.core.canonical_matchup import (
        MATCHUP_SCHEMA, normalize_matchup_df, upload_matchups, SQL_ENRICHMENTS,
    )

    # Fetcher returns raw DataFrame
    raw_df = yahoo_matchup_fetcher(session, year=2025)

    # Normalize to canonical schema (adds missing columns as NULL, fixes types)
    canonical_df = normalize_matchup_df(raw_df, platform="yahoo")

    # Upload raw rows
    upload_matchups("kmffl", canonical_df)

    # Then run SQL enrichments
    for sql in SQL_ENRICHMENTS:
        conn.execute(sql)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from multi_league.core.join_keys import ensure_cumulative_week_column
from multi_league.core.manager_identity import hidden_manager_guid_mask
from multi_league.shared.yahoo_identity import recover_yahoo_team_key

if TYPE_CHECKING:
    import pandas as pd


# ---------------------------------------------------------------------------
# Canonical matchup schema: (column_name, duckdb_type, source)
#
# source: "api" = raw from platform API (fetcher must provide or NULL)
#         "join_key" = fetcher must build from API data
#         "sql" = SQL enrichment computes after upload
# ---------------------------------------------------------------------------

MATCHUP_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Join keys (fetcher must guarantee, correct types) ===
    ("year", "INTEGER", "api"),
    ("week", "INTEGER", "api"),
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("franchise_name", "VARCHAR", "enrichment"),
    ("manager_week", "VARCHAR", "join_key"),
    ("team_key", "VARCHAR", "api"),
    ("team_name", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    ("platform", "VARCHAR", "api"),
    # === Opponent (fetcher pairs the matchup) ===
    ("opponent", "VARCHAR", "api"),
    ("opponent_guid", "VARCHAR", "api"),
    ("opponent_team_key", "VARCHAR", "api"),
    ("opponent_franchise_id", "VARCHAR", "join_key"),
    ("matchup_id", "INTEGER", "api"),
    ("matchup_key", "VARCHAR", "join_key"),
    # === Scores (raw from API) ===
    ("team_points", "DOUBLE", "api"),
    ("opponent_points", "DOUBLE", "api"),
    ("team_projected_points", "DOUBLE", "api"),  # Yahoo only
    ("opponent_projected_points", "DOUBLE", "api"),  # Yahoo only
    # === Playoff flags ===
    # Sleeper/ESPN: fetcher determines from bracket/matchup_type API
    # Yahoo: SQL enrichment determines from week >= playoff_start_week in settings
    ("is_playoffs", "BOOLEAN", "api"),
    ("is_consolation", "BOOLEAN", "api"),
    ("winner_team_key", "VARCHAR", "api"),  # Yahoo scoreboard winner; used to trace custom playoff brackets
    # === Cross-platform fields (NULL if platform doesn't have it) ===
    ("team_logo", "VARCHAR", "api"),  # Yahoo: image_url, ESPN: logo, Sleeper: avatar
    ("division_id", "VARCHAR", "api"),  # Yahoo, Sleeper, ESPN
    ("waiver_rank", "INTEGER", "api"),  # Yahoo, Sleeper, ESPN — platform state, not calculable
    ("custom_points", "DOUBLE", "api"),  # Sleeper custom scoring override
    # === Yahoo-only API fields ===
    ("grade", "VARCHAR", "api"),  # Yahoo matchup grade (A+, B-, etc.)
    ("matchup_recap_url", "VARCHAR", "api"),  # Yahoo AI recap
    ("matchup_recap_title", "VARCHAR", "api"),  # Yahoo AI recap title
    ("url", "VARCHAR", "api"),  # Yahoo team URL
    ("felo_score", "DOUBLE", "api"),  # Yahoo Felo rating
    ("felo_tier", "VARCHAR", "api"),  # Yahoo Felo tier
    ("win_probability_api", "DOUBLE", "api"),  # Yahoo pre-game win probability
    ("week_start", "VARCHAR", "api"),  # Yahoo matchup date range
    ("week_end", "VARCHAR", "api"),  # Yahoo matchup date range
    ("auction_budget_total", "DOUBLE", "api"),  # Yahoo draft auction budget
    ("auction_budget_spent", "DOUBLE", "api"),  # Yahoo draft auction spent
    # === ESPN-only API fields ===
    ("final_playoff_seed", "INTEGER", "api"),  # ESPN: authoritative seed, supersedes our enrichment
    ("adjustment", "DOUBLE", "api"),  # ESPN: commissioner point adjustment
    ("tiebreak", "DOUBLE", "api"),  # ESPN: tiebreaker points
    # === SQL enrichments (computed after upload, NEVER in fetcher) ===
    ("win", "INTEGER", "api"),  # all fetchers provide
    ("loss", "INTEGER", "api"),  # all fetchers provide
    ("tie", "INTEGER", "api"),  # tie-aware: both teams must have points > 0
    ("margin", "DOUBLE", "api"),  # all fetchers provide
    ("total_matchup_score", "DOUBLE", "api"),  # all fetchers provide
    ("close_margin", "INTEGER", "api"),  # all fetchers provide
    ("gpa", "DOUBLE", "api"),  # from Yahoo grade (NULL for Sleeper/ESPN)
    ("league_weekly_mean", "DOUBLE", "sql"),
    ("league_weekly_median", "DOUBLE", "sql"),
    ("above_league_median", "INTEGER", "sql"),
    ("below_league_median", "INTEGER", "sql"),
    ("teams_beat_this_week", "INTEGER", "sql"),
    ("opponent_teams_beat_this_week", "INTEGER", "sql"),
    ("proj_score_error", "DOUBLE", "sql"),  # NULL if no projections
    ("abs_proj_score_error", "DOUBLE", "sql"),
    ("above_proj_score", "INTEGER", "sql"),
    ("below_proj_score", "INTEGER", "sql"),
    ("expected_spread", "DOUBLE", "sql"),
    ("expected_odds", "DOUBLE", "sql"),
    ("win_vs_spread", "INTEGER", "sql"),
    ("lose_vs_spread", "INTEGER", "sql"),
    ("underdog_wins", "INTEGER", "sql"),
    ("favorite_losses", "INTEGER", "sql"),
    ("proj_wins", "INTEGER", "sql"),
    ("proj_losses", "INTEGER", "sql"),
    ("is_championship", "BOOLEAN", "api"),  # Sleeper fetcher provides from bracket API
    ("champion", "INTEGER", "api"),  # Sleeper fetcher provides from bracket API (p=1 winner)
    ("sacko", "INTEGER", "api"),  # enrichment-derived, but must survive upload
    ("playoff_round", "VARCHAR", "sql"),
    ("playoff_round_num", "INTEGER", "sql"),
    ("playoff_week_index", "INTEGER", "sql"),  # 0-indexed week within playoffs
    ("playoff_seed", "INTEGER", "sql"),  # our enrichment; ESPN uses final_playoff_seed instead
    ("playoff_seed_to_date", "INTEGER", "sql"),
    ("postseason", "BOOLEAN", "sql"),
    ("is_bye_week", "BOOLEAN", "sql"),
    # NOTE: quarterfinal/semifinal/championship/consolation_semifinal/consolation_final
    # INTEGER flags REMOVED — replaced by playoff_round (VARCHAR) and consolation_round (VARCHAR).
    # is_championship (BOOLEAN, line 120) is the canonical championship flag.
    ("placement_rank", "INTEGER", "sql"),  # final placement (1st, 2nd, ... last)
    ("placement_game", "INTEGER", "sql"),  # championship, consolation_final, etc.
    # === Cumulative records (computed by cumulative_records enrichment) ===
    ("cumulative_week", "BIGINT", "join_key"),  # year * 100 + week join key
    ("wins_to_date", "INTEGER", "sql"),
    ("losses_to_date", "INTEGER", "sql"),
    ("ties_to_date", "INTEGER", "sql"),
    ("points_scored_to_date", "DOUBLE", "sql"),
    ("cumulative_wins", "INTEGER", "sql"),  # all-time running total
    ("cumulative_losses", "INTEGER", "sql"),
    ("cumulative_ties", "INTEGER", "sql"),
    ("win_streak", "INTEGER", "sql"),
    ("loss_streak", "INTEGER", "sql"),
    # === Season PPG (computed by manager_season_ppg enrichment) ===
    ("weekly_mean", "DOUBLE", "sql"),
    ("weekly_median", "DOUBLE", "sql"),
    ("manager_season_mean", "DOUBLE", "sql"),
    ("manager_season_median", "DOUBLE", "sql"),
    # === Rankings (computed by matchup_rankings enrichment) ===
    ("manager_all_time_ranking", "INTEGER", "sql"),
    ("manager_all_time_percentile", "DOUBLE", "sql"),
    ("manager_season_ranking", "INTEGER", "sql"),
    # === Career stats (computed by all_time_manager_stats enrichment) ===
    ("manager_all_time_gp", "INTEGER", "sql"),
    ("manager_all_time_wins", "INTEGER", "sql"),
    ("manager_all_time_losses", "INTEGER", "sql"),
    ("manager_all_time_ties", "INTEGER", "sql"),
    ("manager_all_time_win_pct", "DOUBLE", "sql"),
    # === Scoring context (computed by inflation_rate enrichment) ===
    ("inflation_rate", "DOUBLE", "sql"),
    # === LAMAR (computed by calculate_lamar enrichment) ===
    ("manager_lamar", "DOUBLE", "sql"),
    # === Simulation columns (computed by expected_record_v2 + playoff_odds_import) ===
    # --- Expected record: shuffle simulation summaries ---
    ("shuffle_avg_wins", "DOUBLE", "sim"),
    ("shuffle_avg_seed", "DOUBLE", "sim"),
    ("shuffle_avg_playoffs", "DOUBLE", "sim"),
    ("shuffle_avg_bye", "DOUBLE", "sim"),
    ("wins_vs_shuffle_wins", "DOUBLE", "sim"),
    ("seed_vs_shuffle_seed", "DOUBLE", "sim"),
    ("opp_shuffle_avg_wins", "DOUBLE", "sim"),
    ("opp_shuffle_avg_seed", "DOUBLE", "sim"),
    ("opp_shuffle_avg_playoffs", "DOUBLE", "sim"),
    ("opp_shuffle_avg_bye", "DOUBLE", "sim"),
    ("wins_vs_opp_shuffle_wins", "DOUBLE", "sim"),
    ("seed_vs_opp_shuffle_seed", "DOUBLE", "sim"),
    ("opp_pts_week_rank", "DOUBLE", "sim"),
    ("opp_pts_week_pct", "DOUBLE", "sim"),
    ("is_final_regular_week", "INTEGER", "sim"),
    # --- Expected record: shuffle win histograms (0-36 for 18-week H2H+median) ---
    *[(f"shuffle_{w}_win", "DOUBLE", "sim") for w in range(37)],
    *[(f"opp_shuffle_{w}_win", "DOUBLE", "sim") for w in range(37)],
    # --- Expected record: shuffle seed distributions (1-64) ---
    *[(f"shuffle_{s}_seed", "DOUBLE", "sim") for s in range(1, 65)],
    *[(f"opp_shuffle_{s}_seed", "DOUBLE", "sim") for s in range(1, 65)],
    # --- Playoff odds: core probabilities ---
    ("team_mu", "DOUBLE", "sim"),
    ("team_sigma", "DOUBLE", "sim"),
    ("p_playoffs", "DOUBLE", "sim"),
    ("p_bye", "DOUBLE", "sim"),
    ("p_semis", "DOUBLE", "sim"),
    ("p_final", "DOUBLE", "sim"),
    ("p_champ", "DOUBLE", "sim"),
    ("win_probability", "DOUBLE", "sim"),
    ("power_rating", "DOUBLE", "sim"),
    ("avg_seed", "DOUBLE", "sim"),
    ("exp_final_wins", "DOUBLE", "sim"),
    ("exp_final_pf", "DOUBLE", "sim"),
    ("consolation_round", "VARCHAR", "sim"),
    ("season_result", "VARCHAR", "sim"),
    # --- Playoff odds: seed probability distributions (1-64) ---
    *[(f"x{s}_seed", "DOUBLE", "sim") for s in range(1, 65)],
    # --- Playoff odds: win probability distributions (0-36 for 18-week H2H+median) ---
    *[(f"x{w}_win", "DOUBLE", "sim") for w in range(37)],
    # --- Playoff scenario columns ---
    ("p_playoffs_change", "DOUBLE", "sim"),
    ("p_playoffs_prev", "DOUBLE", "sim"),
    ("p_champ_change", "DOUBLE", "sim"),
    ("p_champ_prev", "DOUBLE", "sim"),
    ("p_bye_change", "DOUBLE", "sim"),
    ("p_bye_prev", "DOUBLE", "sim"),
    ("max_odds_swing", "DOUBLE", "sim"),
    ("is_critical_matchup", "INTEGER", "sim"),
    ("is_dramatic_win", "INTEGER", "sim"),
    ("is_dramatic_loss", "INTEGER", "sim"),
    ("drama_score", "DOUBLE", "sim"),
    ("win_probability_vs_avg", "DOUBLE", "sim"),
    ("playoff_magic_number", "INTEGER", "sim"),
    ("bye_magic_number", "INTEGER", "sim"),
    ("first_seed_magic_number", "INTEGER", "sim"),
    ("elimination_number", "INTEGER", "sim"),
    ("clinched_playoffs", "INTEGER", "sim"),
    ("clinched_bye", "INTEGER", "sim"),
    ("clinched_first_seed", "INTEGER", "sim"),
    ("eliminated_from_playoffs", "INTEGER", "sim"),
    ("eliminated_from_bye", "INTEGER", "sim"),
    # === Optimal lineup (computed by league_wide_optimal enrichment) ===
    ("optimal_points", "DOUBLE", "sql"),
    ("lineup_efficiency", "DOUBLE", "sql"),
    ("bench_points", "DOUBLE", "sql"),
    ("total_player_points", "DOUBLE", "sql"),  # sum of all rostered player fantasy_points
    (
        "starter_points",
        "DOUBLE",
        "sql",
    ),  # SUM(fantasy_points WHERE is_started=1); internal-data-consistent version of team_points
    ("players_rostered", "INTEGER", "sql"),  # count of rostered players this week
    ("players_started", "INTEGER", "sql"),  # count of started players this week
]

# Convenience lookups
RAW_COLUMNS = [name for name, _, src in MATCHUP_SCHEMA if src in ("api", "join_key")]
SQL_COLUMNS = [name for name, _, src in MATCHUP_SCHEMA if src == "sql"]
SIM_COLUMNS = [name for name, _, src in MATCHUP_SCHEMA if src == "sim"]

# Simulation families
#
# Keep the physical DDL flat, but expose explicit metadata buckets so downstream
# code can distinguish the three simulation paths:
# 1. schedule-luck shuffles
# 2. opponent-schedule / SoS shuffles
# 3. playoff Monte Carlo
SCHEDULE_LUCK_SIM_COLUMNS = [
    "shuffle_avg_wins",
    "shuffle_avg_seed",
    "shuffle_avg_playoffs",
    "shuffle_avg_bye",
    "wins_vs_shuffle_wins",
    "seed_vs_shuffle_seed",
    *[f"shuffle_{w}_win" for w in range(37)],
    *[f"shuffle_{s}_seed" for s in range(1, 65)],
]
SOS_LUCK_SIM_COLUMNS = [
    "opp_shuffle_avg_wins",
    "opp_shuffle_avg_seed",
    "opp_shuffle_avg_playoffs",
    "opp_shuffle_avg_bye",
    "wins_vs_opp_shuffle_wins",
    "seed_vs_opp_shuffle_seed",
    "opp_pts_week_rank",
    "opp_pts_week_pct",
    "is_final_regular_week",
    *[f"opp_shuffle_{w}_win" for w in range(37)],
    *[f"opp_shuffle_{s}_seed" for s in range(1, 65)],
]
LUCK_SIM_COLUMNS = [*SCHEDULE_LUCK_SIM_COLUMNS, *SOS_LUCK_SIM_COLUMNS]
PLAYOFF_SIM_COLUMNS = [
    "team_mu",
    "team_sigma",
    "p_playoffs",
    "p_bye",
    "p_semis",
    "p_final",
    "p_champ",
    "win_probability",
    "power_rating",
    "avg_seed",
    "exp_final_wins",
    "exp_final_pf",
    "consolation_round",
    "season_result",
    *[f"x{s}_seed" for s in range(1, 65)],
    *[f"x{w}_win" for w in range(37)],
    "p_playoffs_change",
    "p_playoffs_prev",
    "p_champ_change",
    "p_champ_prev",
    "p_bye_change",
    "p_bye_prev",
    "max_odds_swing",
    "is_critical_matchup",
    "is_dramatic_win",
    "is_dramatic_loss",
    "drama_score",
    "win_probability_vs_avg",
    "playoff_magic_number",
    "bye_magic_number",
    "first_seed_magic_number",
    "elimination_number",
    "clinched_playoffs",
    "clinched_bye",
    "clinched_first_seed",
    "eliminated_from_playoffs",
    "eliminated_from_bye",
]
SIM_COLUMN_FAMILIES = {
    "schedule_luck": SCHEDULE_LUCK_SIM_COLUMNS,
    "sos_luck": SOS_LUCK_SIM_COLUMNS,
    "luck": LUCK_SIM_COLUMNS,
    "playoff": PLAYOFF_SIM_COLUMNS,
}

_schedule_luck_sim_set = set(SCHEDULE_LUCK_SIM_COLUMNS)
_sos_luck_sim_set = set(SOS_LUCK_SIM_COLUMNS)
_luck_sim_set = set(LUCK_SIM_COLUMNS)
_playoff_sim_set = set(PLAYOFF_SIM_COLUMNS)
_sim_set = set(SIM_COLUMNS)
if _schedule_luck_sim_set & _sos_luck_sim_set:
    overlap = sorted(_schedule_luck_sim_set & _sos_luck_sim_set)
    raise ValueError(f"Schedule/SOS luck columns overlap: {overlap}")
if _luck_sim_set != (_schedule_luck_sim_set | _sos_luck_sim_set):
    missing = sorted(_luck_sim_set - (_schedule_luck_sim_set | _sos_luck_sim_set))
    extra = sorted((_schedule_luck_sim_set | _sos_luck_sim_set) - _luck_sim_set)
    raise ValueError(f"Luck simulation family metadata drift detected. Missing={missing}, Extra={extra}")
if _luck_sim_set & _playoff_sim_set:
    overlap = sorted(_luck_sim_set & _playoff_sim_set)
    raise ValueError(f"Luck/playoff simulation columns overlap: {overlap}")
if _luck_sim_set | _playoff_sim_set != _sim_set:
    missing = sorted(_sim_set - (_luck_sim_set | _playoff_sim_set))
    extra = sorted((_luck_sim_set | _playoff_sim_set) - _sim_set)
    raise ValueError(f"Simulation family metadata drift detected. Missing={missing}, Extra={extra}")

ALL_COLUMNS = [name for name, _, _ in MATCHUP_SCHEMA]
COLUMN_TYPES = {name: dtype for name, dtype, _ in MATCHUP_SCHEMA}


# ---------------------------------------------------------------------------
# GPA scale (Yahoo grades → numeric)
# ---------------------------------------------------------------------------
GPA_SCALE = {
    "A+": 4.0,
    "A": 3.7,
    "A-": 3.3,
    "B+": 3.0,
    "B": 2.7,
    "B-": 2.3,
    "C+": 2.0,
    "C": 1.7,
    "C-": 1.3,
    "D+": 1.0,
    "D": 0.7,
    "D-": 0.3,
    "F": 0.0,
}


# ---------------------------------------------------------------------------
# SQL enrichments — run these after uploading raw matchup data
# ---------------------------------------------------------------------------


def apply_playoff_outcomes(rows: list[dict]) -> None:
    """Derive is_championship/champion via a single-elimination walk over row dicts.

    For platforms whose API flags playoff/consolation games but names no champion
    (Fleaflicker, MFL — unlike Sleeper's bracket API). Participants of the first
    playoff week's non-consolation games are alive; each week's losers among
    alive-vs-alive games drop out; the last alive-vs-alive game is the final.
    A rematch of the same pairing in the following week is treated as a two-week
    final and decided on aggregate points.

    Mutates rows in place; expects both sides of each game as separate rows with
    is_playoffs/is_consolation/franchise_id/opponent_franchise_id/week/win/loss/
    team_points keys (the canonical fetcher row shape).
    """
    for row in rows:
        row.setdefault("is_championship", 0)
        row.setdefault("champion", 0)
    bracket = [r for r in rows if r.get("is_playoffs") and not r.get("is_consolation") and r.get("franchise_id")]
    if not bracket:
        return
    weeks = sorted({r["week"] for r in bracket})
    alive = {r["franchise_id"] for r in bracket if r["week"] == weeks[0]}
    final_rows: list[dict] = []
    for week in weeks:
        week_rows = [
            r for r in bracket
            if r["week"] == week and r["franchise_id"] in alive and r.get("opponent_franchise_id") in alive
        ]
        if not week_rows:
            continue
        final_rows = week_rows
        for r in week_rows:
            if r.get("loss") == 1:
                alive.discard(r["franchise_id"])
    if not final_rows:
        return
    final_week = final_rows[0]["week"]
    pairing = {r["franchise_id"] for r in final_rows}
    rematch = [
        r for r in bracket
        if r["week"] > final_week and r["franchise_id"] in pairing and r.get("opponent_franchise_id") in pairing
    ]
    leg_rows = final_rows + rematch
    totals: dict[str, float] = {}
    wins: dict[str, int] = {}
    for r in leg_rows:
        fid = r["franchise_id"]
        totals[fid] = totals.get(fid, 0.0) + (r.get("team_points") or 0.0)
        wins[fid] = wins.get(fid, 0) + (r.get("win") or 0)
    if rematch:
        champion_fid = max(pairing, key=lambda fid: (totals.get(fid, 0.0), wins.get(fid, 0)))
    else:
        champion_fid = max(pairing, key=lambda fid: (wins.get(fid, 0), totals.get(fid, 0.0)))
    for r in leg_rows:
        r["is_championship"] = 1
        if r["franchise_id"] == champion_fid:
            r["champion"] = 1


def get_sql_enrichments(database_name: str) -> list[str]:
    """Return ordered list of SQL statements to enrich raw matchup data.

    These run after the raw matchup table is uploaded.
    Each statement is idempotent (uses ADD COLUMN IF NOT EXISTS + UPDATE).
    """
    t = f'"{database_name}".public.matchup'

    stmts = []

    # --- Win / Loss / Tie (tie-aware: both must have points > 0) ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS win INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS loss INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS tie INTEGER;
        UPDATE {t} SET
            win  = CASE WHEN team_points > opponent_points THEN 1 ELSE 0 END,
            loss = CASE WHEN team_points < opponent_points THEN 1 ELSE 0 END,
            tie  = CASE WHEN team_points = opponent_points
                         AND team_points > 0
                         AND opponent_points > 0 THEN 1 ELSE 0 END
        WHERE team_points IS NOT NULL AND opponent_points IS NOT NULL;
    """)

    # --- Margin & total ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS margin DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS total_matchup_score DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS close_margin INTEGER;
        UPDATE {t} SET
            margin = ROUND(team_points - opponent_points, 2),
            total_matchup_score = ROUND(team_points + opponent_points, 2),
            close_margin = CASE WHEN ABS(team_points - opponent_points) <= 10 THEN 1 ELSE 0 END
        WHERE team_points IS NOT NULL AND opponent_points IS NOT NULL;
    """)

    # --- GPA from grade ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS gpa DOUBLE;
        UPDATE {t} SET gpa = CASE grade
            {" ".join(f"WHEN '{g}' THEN {v}" for g, v in GPA_SCALE.items())}
            ELSE NULL END
        WHERE grade IS NOT NULL;
    """)

    # --- League weekly stats (mean, median, teams_beat) ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS league_weekly_mean DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS league_weekly_median DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS above_league_median INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS below_league_median INTEGER;
        UPDATE {t} AS m SET
            league_weekly_mean = sub.wk_mean,
            league_weekly_median = sub.wk_median,
            above_league_median = CASE WHEN m.team_points > sub.wk_median THEN 1 ELSE 0 END,
            below_league_median = CASE WHEN m.team_points < sub.wk_median THEN 1 ELSE 0 END
        FROM (
            SELECT year, week,
                   AVG(team_points) AS wk_mean,
                   MEDIAN(team_points) AS wk_median
            FROM {t}
            WHERE team_points IS NOT NULL
            GROUP BY year, week
        ) sub
        WHERE m.year = sub.year AND m.week = sub.week;
    """)

    # --- Teams beat this week (power ranking) ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS teams_beat_this_week INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS opponent_teams_beat_this_week INTEGER;
        UPDATE {t} AS m SET
            teams_beat_this_week = sub.beaten
        FROM (
            SELECT m1.year, m1.week, m1.manager_guid,
                   SUM(CASE WHEN m2.team_points < m1.team_points THEN 1 ELSE 0 END) AS beaten
            FROM {t} m1
            CROSS JOIN {t} m2
            WHERE m1.year = m2.year AND m1.week = m2.week
              AND m1.manager_guid != m2.manager_guid
            GROUP BY m1.year, m1.week, m1.manager_guid
        ) sub
        WHERE m.year = sub.year AND m.week = sub.week AND m.manager_guid = sub.manager_guid;
        UPDATE {t} AS m SET
            opponent_teams_beat_this_week = opp.teams_beat_this_week
        FROM {t} opp
        WHERE m.year = opp.year AND m.week = opp.week AND m.opponent_guid = opp.manager_guid;
    """)

    # --- Projection-based metrics (Yahoo only, NULL if no projections) ---
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS proj_score_error DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS abs_proj_score_error DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS above_proj_score INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS below_proj_score INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS expected_spread DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS expected_odds DOUBLE;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS win_vs_spread INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS lose_vs_spread INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS underdog_wins INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS favorite_losses INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS proj_wins INTEGER;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS proj_losses INTEGER;
        UPDATE {t} SET
            proj_score_error = ROUND(team_points - team_projected_points, 2),
            abs_proj_score_error = ROUND(ABS(team_points - team_projected_points), 2),
            above_proj_score = CASE WHEN team_points > team_projected_points THEN 1 ELSE 0 END,
            below_proj_score = CASE WHEN team_points < team_projected_points THEN 1 ELSE 0 END,
            expected_spread = ROUND(team_projected_points - opponent_projected_points, 2),
            expected_odds = ROUND(1.0 / (1.0 + EXP(-(team_projected_points - opponent_projected_points) / 10.0)), 4),
            win_vs_spread = CASE WHEN (team_points - opponent_points) > (team_projected_points - opponent_projected_points) THEN 1 ELSE 0 END,
            lose_vs_spread = CASE WHEN (team_points - opponent_points) < (team_projected_points - opponent_projected_points) THEN 1 ELSE 0 END,
            underdog_wins = CASE WHEN win = 1 AND team_projected_points < opponent_projected_points THEN 1 ELSE 0 END,
            favorite_losses = CASE WHEN loss = 1 AND team_projected_points > opponent_projected_points THEN 1 ELSE 0 END,
            proj_wins = CASE WHEN team_projected_points > opponent_projected_points THEN 1 ELSE 0 END,
            proj_losses = CASE WHEN team_projected_points < opponent_projected_points THEN 1 ELSE 0 END
        WHERE team_projected_points IS NOT NULL AND opponent_projected_points IS NOT NULL;
    """)

    return stmts


# ---------------------------------------------------------------------------
# DataFrame normalization
# ---------------------------------------------------------------------------


def normalize_matchup_df(df, platform: str, league_id: str | None = None) -> pd.DataFrame:
    """Normalize a matchup DataFrame to the canonical schema.

    - Ensures all RAW_COLUMNS exist (adds as NULL if missing)
    - Drops any SQL_COLUMNS that a fetcher accidentally computed
    - Fixes column types (manager_guid → str, week → int, etc.)
    - Adds platform column

    Args:
        df: Raw DataFrame from a platform fetcher
        platform: "yahoo" | "sleeper" | "espn"

    Returns:
        DataFrame with exactly RAW_COLUMNS, correctly typed.
    """
    import pandas as pd

    db_name_cols = [c for c in ["db_name"] if df is not None and c in df.columns]
    if df is None or df.empty:
        return pd.DataFrame(columns=db_name_cols + RAW_COLUMNS)

    df = df.copy()

    # Add platform
    df["platform"] = platform
    if league_id and ("league_id" not in df.columns or df["league_id"].isna().all()):
        df["league_id"] = league_id

    # Rename platform-specific columns to canonical names
    rename_map = {
        "win_probability": "win_probability_api",
        "points": "team_points",
        "image_url": "team_logo",  # Yahoo image_url → team_logo
        "logo": "team_logo",  # ESPN logo → team_logo
        "waiver_priority": "waiver_rank",  # Yahoo waiver_priority → waiver_rank
        "waiverRank": "waiver_rank",  # ESPN waiverRank → waiver_rank
        "playoffSeed": "final_playoff_seed",  # ESPN playoffSeed → final_playoff_seed
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Yahoo scoreboard payloads can redact hidden-team team_key values as "--"
    # while still exposing the stable team slot in the team URL.
    if platform == "yahoo" and {"team_key", "url", "league_id"}.issubset(df.columns):
        repaired_team_key = df.apply(
            lambda row: recover_yahoo_team_key(
                row.get("team_key"),
                row.get("url"),
                row.get("league_id"),
            ),
            axis=1,
        )
        if repaired_team_key.notna().any():
            df["team_key"] = repaired_team_key

    # Build join keys if not present
    if "team_key" not in df.columns or df["team_key"].isna().all():
        # Yahoo: use manager_guid as team_key
        if "manager_guid" in df.columns:
            df["team_key"] = df["manager_guid"]

    if "franchise_id" not in df.columns or df["franchise_id"].isna().all():
        if "manager_guid" in df.columns:
            # franchise_id = first 8 chars of GUID (standard pattern)
            # Leave NULL for rows without a real GUID — discover_franchises
            # will assign synthetic IDs later. Converting None→'None' would
            # collapse all no-GUID managers into a single identity.
            has_guid = (
                df["manager_guid"].notna()
                & (df["manager_guid"].astype(str).str.strip() != "")
                & (df["manager_guid"].astype(str) != "None")
                & ~hidden_manager_guid_mask(df["manager_guid"])
            )
            df["franchise_id"] = None
            df.loc[has_guid, "franchise_id"] = df.loc[has_guid, "manager_guid"].astype(str).str.strip()

    # ── Franchise name disambiguation ──
    # Detect duplicate manager names (same name, different franchise_ids) and
    # disambiguate by appending the most recent team name.
    if "franchise_id" in df.columns and "manager" in df.columns and df["franchise_id"].notna().any():
        # Find manager names that map to multiple franchise_ids
        mgr_fid = df[df["franchise_id"].notna()][["manager", "franchise_id"]].drop_duplicates()
        name_counts = mgr_fid.groupby("manager")["franchise_id"].nunique()
        dupe_names = set(name_counts[name_counts > 1].index)

        if dupe_names:
            # Build disambiguated franchise_name for each franchise_id with a colliding name
            if "franchise_name" not in df.columns:
                df["franchise_name"] = None

            dupe_fid_map = (
                mgr_fid[mgr_fid["manager"].isin(dupe_names)]
                .groupby("manager", dropna=False)["franchise_id"]
                .agg(lambda series: series.dropna().unique().tolist())
                .to_dict()
            )

            for dupe_name, dupe_fids in dupe_fid_map.items():
                for fid in dupe_fids:
                    fid_mask = df["franchise_id"] == fid
                    # Get most recent team_name for this franchise
                    if "team_name" in df.columns:
                        fid_rows = df[fid_mask & df["team_name"].notna()]
                        if not fid_rows.empty and "year" in fid_rows.columns:
                            latest = fid_rows.loc[fid_rows["year"].idxmax(), "team_name"]
                        elif not fid_rows.empty:
                            latest = fid_rows.iloc[-1]["team_name"]
                        else:
                            latest = fid
                    else:
                        latest = fid
                    disambiguated = f"{dupe_name} - {latest}"
                    df.loc[fid_mask, "franchise_name"] = disambiguated
                    df.loc[fid_mask, "manager"] = disambiguated

            # Also fix opponent column to match
            if "opponent" in df.columns and "opponent_franchise_id" in df.columns:
                # Build fid → disambiguated name lookup
                fid_to_name = dict(
                    df[df["franchise_id"].notna()][["franchise_id", "manager"]]
                    .drop_duplicates(subset=["franchise_id"])
                    .values
                )
                for fid, name in fid_to_name.items():
                    opp_mask = df["opponent_franchise_id"] == fid
                    if opp_mask.any():
                        df.loc[opp_mask, "opponent"] = name

    if "manager_week" not in df.columns or df["manager_week"].isna().all():
        if all(c in df.columns for c in ["franchise_id", "year", "week"]):
            # Use franchise_id when available, fall back to manager name for hidden managers
            _mw_id = df["franchise_id"].fillna(df.get("manager", pd.Series(dtype=str))).astype(str)
            df["manager_week"] = _mw_id + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)

    if "opponent_guid" not in df.columns or df["opponent_guid"].isna().all():
        # Build opponent_guid by score-based matching (robust against duplicate names).
        # For each row, find the row in the same (year, week) whose team_points match
        # our opponent_points and vice versa, then grab their manager_guid.
        if all(c in df.columns for c in ["manager_guid", "year", "week", "team_points", "opponent_points"]):
            # Build a dict: (year, week, team_points_rounded, opp_points_rounded) -> manager_guid
            score_to_guid: dict[tuple, str] = {}
            for _, row in df.iterrows():
                tp = row.get("team_points")
                op = row.get("opponent_points")
                guid = row.get("manager_guid")
                if tp is not None and op is not None and guid and (tp != 0 or op != 0):
                    key = (row["year"], row["week"], round(float(tp), 2), round(float(op), 2))
                    score_to_guid[key] = str(guid)

            def _lookup_opp_guid(row):
                tp = row.get("opponent_points")
                op = row.get("team_points")
                if tp is None or op is None:
                    return None
                key = (row["year"], row["week"], round(float(tp), 2), round(float(op), 2))
                opp = score_to_guid.get(key)
                if opp and opp != str(row.get("manager_guid", "")):
                    return opp
                return None

            df["opponent_guid"] = df.apply(_lookup_opp_guid, axis=1)

    if "opponent_franchise_id" not in df.columns or df["opponent_franchise_id"].isna().all():
        if "opponent_guid" in df.columns:
            df["opponent_franchise_id"] = None
            has_opp_guid = (
                df["opponent_guid"].notna()
                & (df["opponent_guid"].astype(str).str.strip() != "")
                & (df["opponent_guid"].astype(str) != "None")
                & ~hidden_manager_guid_mask(df["opponent_guid"])
            )
            df.loc[has_opp_guid, "opponent_franchise_id"] = (
                df.loc[has_opp_guid, "opponent_guid"].astype(str).str.strip()
            )

    # Name-based opponent backfill — needed when score-based matching fails
    # because both team_points and opponent_points are NULL (typical of
    # externally-imported historical data where the user uploaded the
    # schedule but not the box scores). Without this, downstream transforms
    # that key off opponent_franchise_id (most notably the playoff bracket
    # tracer) silently drop those weeks. Score-based pairing wins where both
    # paths agree; name-based only fills rows still NULL.
    if all(c in df.columns for c in ["manager", "opponent", "year", "week"]):
        # Build (year, week, manager_normalized) -> (franchise_id, manager_guid)
        name_lookup: dict[tuple, dict[str, str]] = {}
        has_fid = "franchise_id" in df.columns
        has_guid = "manager_guid" in df.columns
        for _, row in df.iterrows():
            mgr = row.get("manager")
            if mgr is None or pd.isna(mgr) or not str(mgr).strip():
                continue
            yr = row.get("year")
            wk = row.get("week")
            if pd.isna(yr) or pd.isna(wk):
                continue
            key = (yr, wk, str(mgr).strip())
            entry = name_lookup.setdefault(key, {})
            if has_fid and "franchise_id" not in entry:
                fid = row.get("franchise_id")
                if pd.notna(fid) and str(fid).strip() and str(fid).lower() != "nan":
                    entry["franchise_id"] = str(fid)
            if has_guid and "manager_guid" not in entry:
                guid = row.get("manager_guid")
                if pd.notna(guid) and str(guid).strip() and str(guid).lower() != "nan":
                    entry["manager_guid"] = str(guid)

        def _opp_lookup(row, field: str):
            opp = row.get("opponent")
            if opp is None or pd.isna(opp) or not str(opp).strip():
                return None
            yr = row.get("year")
            wk = row.get("week")
            if pd.isna(yr) or pd.isna(wk):
                return None
            entry = name_lookup.get((yr, wk, str(opp).strip()))
            if entry is None:
                return None
            return entry.get(field)

        def _is_blank(v) -> bool:
            if v is None:
                return True
            try:
                if pd.isna(v):
                    return True
            except (TypeError, ValueError):
                pass
            s = str(v).strip()
            return s == "" or s.lower() in ("nan", "none")

        if has_fid and "opponent_franchise_id" in df.columns:
            null_mask = df["opponent_franchise_id"].apply(_is_blank)
            if null_mask.any():
                filled = df[null_mask].apply(lambda r: _opp_lookup(r, "franchise_id"), axis=1)
                df.loc[null_mask, "opponent_franchise_id"] = filled

        if has_guid and "opponent_guid" in df.columns:
            null_mask = df["opponent_guid"].apply(_is_blank)
            if null_mask.any():
                filled = df[null_mask].apply(lambda r: _opp_lookup(r, "manager_guid"), axis=1)
                df.loc[null_mask, "opponent_guid"] = filled

    # Add missing raw columns as NULL
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = ensure_cumulative_week_column(df)

    # Drop computed columns that shouldn't be in the raw upload
    cols_to_drop = [c for c in SQL_COLUMNS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Fix types for join keys
    for col in [
        "manager_guid",
        "franchise_id",
        "team_key",
        "opponent_team_key",
        "winner_team_key",
        "manager_week",
        "opponent_guid",
        "opponent_franchise_id",
        "league_id",
    ]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"None": None, "nan": None, "": None})

    for col in [
        "year",
        "week",
        "matchup_id",
        "waiver_priority",
        "number_of_moves",
        "number_of_trades",
        "cumulative_week",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in [
        "team_points",
        "opponent_points",
        "team_projected_points",
        "opponent_projected_points",
        "faab_balance",
        "felo_score",
        "auction_budget_total",
        "auction_budget_spent",
        "win_probability_api",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Select only canonical raw columns, in order
    return df[db_name_cols + [c for c in RAW_COLUMNS if c in df.columns]]


# ---------------------------------------------------------------------------
# DuckDB persistence
# ---------------------------------------------------------------------------


def create_matchup_table_sql(database_name: str) -> str:
    """Generate CREATE TABLE for canonical matchup table (raw + sql columns)."""
    cols = []
    for name, dtype, _ in MATCHUP_SCHEMA:
        cols.append(f'    "{name}" {dtype} NOT NULL' if name == "db_name" else f'    "{name}" {dtype}')
    col_defs = ",\n".join(cols)
    return f"""
CREATE TABLE IF NOT EXISTS "{database_name}".public.matchup (
{col_defs}
)
"""


def upload_matchups(database_name: str, df, conn=None) -> bool:
    """Upload canonical matchup DataFrame through the active DuckDB connection.

    Creates table if needed. Upserts by deleting existing year/week combos
    then inserting.

    Args:
        database_name: League database name
        df: Canonical matchup DataFrame (from normalize_matchup_df)
        conn: Optional DuckDB connection

    Returns:
        True if successful
    """
    import logging

    logger = logging.getLogger(__name__)

    if df is None or df.empty:
        logger.warning("[MATCHUP] Empty DataFrame, nothing to upload")
        return False
    df = df.copy()
    if "db_name" not in df.columns:
        df["db_name"] = database_name

    close_conn = conn is None
    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        conn.execute(f'CREATE DATABASE IF NOT EXISTS "{database_name}"')

        # Drop old-format table if schema doesn't match
        try:
            old_cols = conn.execute(
                f"SELECT column_name FROM duckdb_columns() "
                f"WHERE database_name = '{database_name}' AND table_name = 'matchup'"
            ).fetchall()
            old_col_names = {r[0] for r in old_cols}
            if old_col_names and "team_logo" not in old_col_names and "team_points" in old_col_names:
                conn.execute(f'DROP TABLE "{database_name}".public.matchup')
                logging.getLogger(__name__).info(f"[MATCHUP] Dropped old-format matchup table in {database_name}")
        except Exception:
            pass

        conn.execute(create_matchup_table_sql(database_name))

        # Delete existing data for the years we're uploading
        years = df["year"].dropna().unique().tolist()
        for yr in years:
            conn.execute(
                f'DELETE FROM "{database_name}".public.matchup WHERE year = ?',
                [int(yr)],
            )

        # Insert via DuckDB's DataFrame registration
        conn.register("_matchup_upload", df)
        insert_cols = ["db_name"] + [c for c in RAW_COLUMNS if c in df.columns]
        col_list = ", ".join([f'"{c}"' for c in insert_cols])
        conn.execute(
            f'INSERT INTO "{database_name}".public.matchup ({col_list}) SELECT {col_list} FROM _matchup_upload'
        )
        conn.unregister("_matchup_upload")

        logger.info(f"[MATCHUP] Uploaded {len(df)} rows to {database_name} ({len(years)} year(s))")
        return True

    except Exception as e:
        logger.error(f"[MATCHUP] Upload failed: {e}")
        return False
    finally:
        if close_conn:
            conn.close()
