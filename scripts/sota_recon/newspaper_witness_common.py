"""
sota_recon/newspaper_witness_common.py -- shared constants for the newspaper sidecar
witness bundle (paths, stat-name canonicalization, event-type crediting).

The bundle stores newspaper testimony verbatim: stat_name/event_type vocabularies are
NOT canonicalized at rest (that is part of the evidence). Canonicalization happens here,
read-side only, so the register script and the recon lane agree on one mapping.
"""
from __future__ import annotations

import os

from .sources import DATA_LAKE

NEWSPAPER_BUNDLE_DIR = os.path.join(
    DATA_LAKE, "curated", "witnesses", "newspaper", "20260717T065218Z_v1")
NEWSPAPER_BUNDLE_TABLES = os.path.join(NEWSPAPER_BUNDLE_DIR, "tables")

SIDECAR_TABLES = [
    "newspaper_weekly_player_stat_cells",
    "newspaper_lineup_participation",
    "newspaper_scoring_events",
    "newspaper_play_by_play_events",
    "newspaper_player_game_notes",
    "newspaper_team_game_stats",
    "newspaper_team_game_stat_claims",
    "newspaper_game_context",
    "newspaper_reviewer_source_document_notes",
    "newspaper_general_reviewer_accepted_decisions",
    "newspaper_general_reviewer_hold_decisions",
]


def sidecar_path(table: str) -> str:
    return os.path.join(NEWSPAPER_BUNDLE_TABLES, table + ".parquet").replace("\\", "/")


# --- player stat-cell stat_name -> canonical v26 atom -------------------------------------
# Only atoms with a real v26 weekly column are mapped; everything else stays raw newspaper
# vocabulary (narrative atoms are historical record, not weekly-cell witnesses). Mirrors the
# wave56 17-stat surface plus synonym variants observed in the bundle.
STAT_CELL_ATOM_MAP: dict[str, str] = {
    "carries": "carries", "rush_attempts": "carries", "rushing_attempts": "carries",
    "rushing_yards": "rushing_yards",
    "rushing_tds": "rushing_tds", "rushing_touchdowns": "rushing_tds",
    "attempts": "attempts", "pass_attempts": "attempts", "passing_attempts": "attempts",
    "completions": "completions", "pass_completions": "completions",
    "passing_completions": "completions",
    "passing_yards": "passing_yards",
    "passing_tds": "passing_tds", "passing_touchdowns": "passing_tds",
    "touchdown_passes": "passing_tds",
    "passing_interceptions": "passing_interceptions",
    "interceptions_thrown": "passing_interceptions",
    "receptions": "receptions",
    "receiving_yards": "receiving_yards", "reception_yards": "receiving_yards",
    "receiving_tds": "receiving_tds", "receiving_touchdowns": "receiving_tds",
    "pat_made": "pat_made", "extra_points_made": "pat_made", "extra_points": "pat_made",
    "points_after_touchdown": "pat_made",
    "pat_att": "pat_att",
    "fg_made": "fg_made", "field_goals_made": "fg_made", "field_goals": "fg_made",
    "fg_att": "fg_att", "field_goal_attempts": "fg_att",
    "fg_long": "fg_long",
    "def_interceptions": "def_interceptions", "defensive_interceptions": "def_interceptions",
    "def_tds": "def_tds", "defensive_touchdowns": "def_tds",
    "fumbles": "fumbles", "fumbles_lost": "fumbles_lost",
    "punts": "punts",
    "special_teams_tds": "special_teams_tds",
}

# canonical atom -> v26 weekly column (identical names in v26; listed for explicitness)
ATOM_TO_V26_COL: dict[str, str] = {a: a for a in [
    "carries", "rushing_yards", "rushing_tds", "attempts", "completions",
    "passing_yards", "passing_tds", "passing_interceptions", "receptions",
    "receiving_yards", "receiving_tds", "pat_made", "pat_att", "fg_made",
    "fg_att", "fg_long", "def_interceptions", "def_tds", "fumbles",
    "fumbles_lost", "punts", "special_teams_tds",
]}

# --- scoring event_type -> (canonical scoring bucket, credited atom for scorer) ----------
# bucket in {td, fg, pat, safety, two_pt}; None bucket = non-scoring/unclassified.
# credited atom is what the SCORING player earns; passer additionally earns passing_tds on
# pass TDs (handled in the lane via passer_NFL_player_id).
EVENT_TYPE_MAP: dict[str, tuple[str, str | None]] = {
    "rushing_touchdown": ("td", "rushing_tds"),
    "receiving_touchdown": ("td", "receiving_tds"),
    "pass_touchdown": ("td", "receiving_tds"),
    "passing_touchdown": ("td", "receiving_tds"),
    "touchdown": ("td", None),
    "touchdown_recovery": ("td", None),
    "interception_return_touchdown": ("td", "def_tds"),
    "fumble_return_touchdown": ("td", "def_tds"),
    "punt_return_touchdown": ("td", "special_teams_tds"),
    "kickoff_return_touchdown": ("td", "special_teams_tds"),
    "blocked_punt_return_touchdown": ("td", "def_tds"),
    "field_goal": ("fg", "fg_made"),
    "field_goal_made": ("fg", "fg_made"),
    "field_goal_place_kick": ("fg", "fg_made"),
    "field_goal_drop_kick": ("fg", "fg_made"),
    "drop_kick_field_goal": ("fg", "fg_made"),
    "extra_point": ("pat", "pat_made"),
    "extra_point_placement": ("pat", "pat_made"),
    "extra_point_made": ("pat", "pat_made"),
    "extra_point_good": ("pat", "pat_made"),
    "pat_made": ("pat", "pat_made"),
    "pat_made_placekick": ("pat", "pat_made"),
    "pat_kick": ("pat", "pat_made"),
    "goal_after_touchdown": ("pat", "pat_made"),
    "point_after_touchdown": ("pat", "pat_made"),
    "safety": ("safety", None),
}

# points each bucket is worth in the 1920s rulebook (TD=6 from 1912, FG=3, PAT=1, safety=2)
BUCKET_POINTS = {"td": 6, "fg": 3, "pat": 1, "safety": 2, "two_pt": 2}


def pfr_scoring_atom_case(desc_col: str) -> str:
    """SQL CASE mapping a PFR boxscore scoring description to the scorer's credited atom,
    using the GRANULAR v26 column names (atom == v26 column).

    v26 stores return TDs granularly -- fum_ret_td, def_int_ret_td, kickoff_return_tds,
    punt_return_tds -- NOT lumped into def_tds (which is a ROLLUP == def_int_ret_td +
    fum_ret_td). Comparing PFR's per-play returns against def_tds was a mapping error that
    manufactured a phantom ~1,500-cell 'gap'; e.g. Terence Newman's fumble-return TD lives
    in fum_ret_td=1, def_tds=0. Map to the granular column the play actually feeds.

    ORDER MATTERS: pass/return/field-goal patterns are tested BEFORE the rush pattern
    because a passer surname like 'Cooper Rush' contains 'rush'. The scorer is the FIRST
    link id; on a 'pass from' that first id is the RECEIVER (receiving_tds).
    """
    d = f"lower({desc_col})"
    return f"""CASE
        WHEN {d} LIKE '%pass from%' THEN 'receiving_tds'
        WHEN {d} LIKE '%interception return%' THEN 'def_int_ret_td'
        WHEN {d} LIKE '%fumble return%' OR {d} LIKE '%fumble recovery%' THEN 'fum_ret_td'
        WHEN {d} LIKE '%kickoff return%' THEN 'kickoff_return_tds'
        WHEN {d} LIKE '%punt return%' OR {d} LIKE '%missed field goal return%' THEN 'punt_return_tds'
        WHEN {d} LIKE '%field goal%' THEN 'fg_made'
        WHEN {d} LIKE '%yard rush%' OR {d} LIKE '% run %' OR {d} LIKE '%yard run%' THEN 'rushing_tds'
        ELSE NULL END"""
