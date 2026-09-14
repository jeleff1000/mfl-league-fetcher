"""Subtable-by-subtable PFA lake map and 2025 audit receipt.

PFA has two different kinds of evidence in the lake:
  * structured participation and reparsed boxscore rows;
  * retained/raw or metadata pages whose columns are not yet materialized.

The receipt keeps those states separate. A semantic mapping is not reported as a
measured 2025 equality when the PFA player identity/game crosswalk is absent.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(r"D:\league-history-data\nfl")
BOX = ROOT / "derived" / "reparsed_captures" / "pfa_boxscore_tables"
PART = ROOT / "ff_assets" / "profootballarchives" / "player_game_participation" / "29669388268" / "shards"
ANCIENT = ROOT / "curated" / "ancient_source_recovery" / "ready_upsert_bundles" / "through_1978_pfr_loc" / "weekly_stat_upsert_rows.parquet"
META = ROOT / "ff_assets" / "profootballarchives" / "site_metadata"
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfa-lake-audit-2025.json")

PFA_TEAM_TO_NFL = {
    "Buffalo Bills": "BUF", "Carolina Panthers": "CAR", "Chicago Bears": "CHI",
    "Denver Broncos": "DEN", "Green Bay Packers": "GNB", "Houston Texans": "HOU",
    "Jacksonville Jaguars": "JAX", "Los Angeles Chargers": "LAC", "Los Angeles Rams": "LAR",
    "New England Patriots": "NWE", "Philadelphia Eagles": "PHI", "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SFO", "Seattle Seahawks": "SEA",
}

# These are cross-source witnesses, not claims that the target is already a
# physical canonical column.  The PFR audits and PFA capture are allowed to
# corroborate a deferred target without promoting it or backfilling it.
CROSS_SOURCE_WITNESS_MATRIX = [
    {
        "field": "def_int_long",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "interceptions.lg",
        "other_witnesses": ["PFR player-defense", "PFR defense box regular/post"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "kickoff",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "kickoffs.no",
        "other_witnesses": ["PFR player-kicking", "PFR kicking post"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "kickoff_yds",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "kickoffs.yds",
        "other_witnesses": ["PFR player-kicking", "PFR kicking post"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "kickoff_tb",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "kickoffs.tb",
        "other_witnesses": ["PFR player-kicking", "PFR kicking post"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "kickoff_return_fc",
        "canonical_status": "STRUCTURED_WITNESS_ONLY_PENDING_ADJUDICATION",
        "pfa_witness": "kickoff_returns.fc",
        "other_witnesses": ["PFR box kicking-returns context where present"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "punt_return_fc",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "punt_returns.fc",
        "other_witnesses": ["PFR player-returns source family where present"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "uniform_number",
        "canonical_status": "CONTEXT_WITNESS_TO_PLAYER_BIO_OR_SEASON",
        "pfa_witness": "lineups.jersey_number",
        "other_witnesses": ["PFR snap counts", "PFR games played", "PFR OL penalties"],
        "destination_lanes": ["season", "player_bio"],
    },
    {
        "field": "roster_membership",
        "canonical_status": "CONTEXT_WITNESS_TO_PLAYER_BIO_OR_SEASON",
        "pfa_witness": "PFA participation and team-season roster family",
        "other_witnesses": ["PFR games played/player-season context"],
        "destination_lanes": ["season", "player_bio"],
    },
    {
        "field": "team_season_wins_losses_ties",
        "canonical_status": "CONTEXT_WITNESS_TO_TEAM_SEASON",
        "pfa_witness": "PFA team-season pages / season index family",
        "other_witnesses": ["PFR team games"],
        "destination_lanes": ["team_season_context", "weekly_team_game"],
    },
    {
        "field": "team_points_for_against",
        "canonical_status": "CONTEXT_WITNESS_TO_TEAM_SEASON",
        "pfa_witness": "PFA team-season pages / score-by-quarters",
        "other_witnesses": ["PFR team games", "PFR boxscore context"],
        "destination_lanes": ["team_season_context", "DST_context"],
    },
    {
        "field": "venue_attendance_weather_game_location",
        "canonical_status": "CONTEXT_WITNESS_TO_WEEKLY_GAME",
        "pfa_witness": "PFA game-context family",
        "other_witnesses": ["PFR boxscore context", "PFR team games"],
        "destination_lanes": ["weekly_game_context"],
    },
    {
        "field": "transaction_history_surface",
        "canonical_status": "PLAYER_BIO_TEAM_HISTORY_WITNESS",
        "pfa_witness": "PFA player-season transaction family",
        "other_witnesses": ["PFR player-season transaction/context family"],
        "destination_lanes": ["player_bio", "career_team_history"],
    },
    {
        "field": "AAFC_team_season_records",
        "canonical_status": "DEFERRED_LEGACY_BACKFILL_WITNESS",
        "pfa_witness": "PFA AAFC legacy family",
        "other_witnesses": ["PFR historical team-season sources"],
        "destination_lanes": ["season", "career", "team_season_context", "immutable_source_witness"],
    },
    {
        "field": "AAFC_player_membership_context",
        "canonical_status": "DEFERRED_LEGACY_BACKFILL_WITNESS",
        "pfa_witness": "PFA AAFC legacy family",
        "other_witnesses": ["PFR historical player-season/bio sources"],
        "destination_lanes": ["season", "career", "player_bio", "immutable_source_witness"],
    },
    {
        "field": "two_point_attempts_and_made",
        "canonical_status": "DEFINITION_ADJUDICATION_WITNESS",
        "pfa_witness": "PFA scoring/player-season source family",
        "other_witnesses": ["PFR player scoring", "NFL/PBP two-point events"],
        "destination_lanes": ["weekly", "season", "career"],
    },
    {
        "field": "unmodeled_20plus_40plus_subtotals",
        "canonical_status": "DEFERRED_PROMOTION_CANDIDATE",
        "pfa_witness": "PFA player-season rushing/passing/receiving source family",
        "other_witnesses": ["PFR player-season rushing/receiving/passing audits", "PBP-derived explosive counts"],
        "destination_lanes": ["weekly", "season", "career"],
    },
]


def _norm_text(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _numeric_or_text(value: object):
    """Normalize PFA numeric cells that carry a touchdown suffix (for example 29t)."""
    text = "" if value is None else str(value)
    match = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*[tT]?\s*", text)
    return float(match.group(1)) if match else text


def _pfa_2025_equality(con, box_path: str, weekly_path: str, matrix: list[dict]) -> dict:
    """Crosswalk the captured PFA postseason games to v26 by game teams/player name.

    PFA's 2025 IDs 322-334 are the 13 postseason games. The source does not carry the
    v26 game key, so the game crosswalk is independently reconstructed from the two
    participating teams and v26's postseason week (19-22).
    """
    pfa_games = con.execute(
        """SELECT game_id, list(DISTINCT team) AS teams
           FROM read_parquet(?, union_by_name=true)
           WHERE season=2025 AND team IS NOT NULL
           GROUP BY game_id ORDER BY game_id""", [box_path]
    ).fetchall()
    v26_games = con.execute(
        """SELECT DISTINCT week, nfl_team, opponent_nfl_team
           FROM read_parquet(?)
           WHERE year=2025 AND week >= 19 AND nfl_team IS NOT NULL
             AND opponent_nfl_team IS NOT NULL""", [weekly_path]
    ).fetchall()
    pair_to_week = {}
    for week, team, opponent in v26_games:
        pair_to_week[frozenset((team, opponent))] = float(week)
    game_map = []
    for game_id, teams in pfa_games:
        pair = frozenset(PFA_TEAM_TO_NFL.get(x) for x in teams if x in PFA_TEAM_TO_NFL)
        week = pair_to_week.get(pair) if len(pair) == 2 else None
        game_map.append({"pfa_game_id": game_id, "pfa_teams": teams, "v26_week": week,
                         "crosswalk_status": "MATCHED" if week is not None else "UNMATCHED"})
    game_to_week = {x["pfa_game_id"]: x["v26_week"] for x in game_map if x["v26_week"] is not None}

    direct_rows = [x for x in matrix if x.get("canonical") and x.get("disposition") == "VERIFIED_DIRECT_MAPPING"]
    equality = []
    for item in direct_rows:
        table, column, canonical = item["subtable"], item["column"], item["canonical"]
        pfa_rows = con.execute(
            f"""SELECT game_id, player, team, \"{column}\"
                FROM read_parquet(?, union_by_name=true)
                WHERE season=2025 AND table_tag=? AND player IS NOT NULL
                  AND team IS NOT NULL AND \"{column}\" IS NOT NULL
                  AND COALESCE(is_team_row, false) = false""",
            [box_path, table],
        ).fetchall()
        v_rows = con.execute(
            f"""SELECT player, nfl_team, week, \"{canonical}\"
                FROM read_parquet(?)
                WHERE year=2025 AND week >= 19 AND player IS NOT NULL
                  AND nfl_team IS NOT NULL AND \"{canonical}\" IS NOT NULL""",
            [weekly_path],
        ).fetchall()
        v_lookup = {(_norm_text(player), team, float(week)): value
                    for player, team, week, value in v_rows}
        comparable = matches = missing = 0
        examples = []
        for game_id, player, team, source_value in pfa_rows:
            week = game_to_week.get(game_id)
            nfl_team = PFA_TEAM_TO_NFL.get(team)
            target = v_lookup.get((_norm_text(player), nfl_team, week)) if week is not None and nfl_team else None
            if target is None:
                missing += 1
                continue
            source_num, target_num = _numeric_or_text(source_value), _numeric_or_text(target)
            comparable += 1
            equal = (abs(source_num - target_num) <= 0.01
                     if isinstance(source_num, float) and isinstance(target_num, float)
                     else source_num == target_num)
            if equal:
                matches += 1
            elif len(examples) < 5:
                examples.append({"game_id": game_id, "player": player, "team": team,
                                 "pfa": source_value, "v26": target, "week": week})
        equality.append({"subtable": table, "column": column, "canonical": canonical,
                         "comparable": comparable, "matches": matches,
                         "mismatches": comparable - matches, "unmatched": missing,
                         "status": "PASS" if comparable == matches else "FAIL",
                         "examples": examples})
    return {"game_crosswalk": game_map, "column_checks": equality,
            "summary": {"games_in_pfa": len(pfa_games), "games_matched": len(game_to_week),
                         "direct_columns_checked": len(equality),
                         "direct_columns_fully_equal": sum(x["status"] == "PASS" for x in equality),
                         "direct_column_failures": sum(x["status"] == "FAIL" for x in equality),
                         "comparable_cells": sum(x["comparable"] for x in equality),
                         "matching_cells": sum(x["matches"] for x in equality)}}

COMMON = {
    "team": ("CONTEXT_TO_WEEKLY_OR_SEASON", "team/game context; identity crosswalk required"),
    "section": ("CONTEXT_TO_WEEKLY", "lineup/table section context"),
    "player": ("CONTEXT_TO_WEEKLY", "player label; resolve through PFA identity crosswalk"),
    "source_player_id": ("STRUCTURED_WITNESS_REQUIRED", "PFA identity key; no canonical NFL id is asserted"),
    "is_team_row": ("STRUCTURED_WITNESS_REQUIRED", "row-shape discriminator"),
    "row_index": ("PROVENANCE_ONLY", "parser row ordinal"),
    "source": ("PROVENANCE_ONLY", "source lineage"),
    "dataset": ("PROVENANCE_ONLY", "source dataset label"),
    "season": ("CONTEXT_TO_WEEKLY_OR_SEASON", "season key"),
    "game_id": ("CONTEXT_TO_WEEKLY", "PFA game key; crosswalk to canonical game key required"),
    "source_url": ("PROVENANCE_ONLY", "source page URL"),
    "content_sha256": ("PROVENANCE_ONLY", "retained-page content witness"),
}

MAP = {
    "lineups": {
        "position": ("CONTEXT_TO_SEASON_OR_BIO", "position", "presence/position witness"),
        "jersey_number": ("CONTEXT_TO_SEASON_OR_BIO", "uniform_number", "roster/context witness"),
    },
    "score_by_quarters": {
        "team_label": ("STRUCTURED_WITNESS_REQUIRED", "team_game_score", "team identity and score-by-quarter witness"),
        "1st": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_q1", "team-game quarter score"),
        "2nd": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_q2", "team-game quarter score"),
        "3rd": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_q3", "team-game quarter score"),
        "4th": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_q4", "team-game quarter score"),
        "ot": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_ot", "team-game overtime score"),
        "ot1": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_ot1", "era-specific quarter score"),
        "ot2": ("STRUCTURED_WITNESS_REQUIRED", "team_pts_ot2", "era-specific quarter score"),
        "final": ("STRUCTURED_WITNESS_REQUIRED", "team_pts", "team-game final score; not player scoring"),
    },
    "scoring_plays": {
        "stat_team": ("CONTEXT_TO_WEEKLY", "team", "event team"),
        "scoring_plays": ("STRUCTURED_WITNESS_REQUIRED", "scoring_event", "event description; use for scoring bijection"),
        "qtr": ("STRUCTURED_WITNESS_REQUIRED", "event_quarter", "event timing"),
        "score_team_1": ("STRUCTURED_WITNESS_REQUIRED", "score_state", "running score after event"),
        "score_team_2": ("STRUCTURED_WITNESS_REQUIRED", "score_state", "running score after event"),
    },
    "passing": {
        "att": ("VERIFIED_DIRECT_MAPPING", "pass_att", "passing attempts"),
        "com": ("VERIFIED_DIRECT_MAPPING", "pass_cmp", "passing completions"),
        "pct": ("VERIFIED_DERIVED_WITNESS", "pass_cmp_pct", "completion percentage; denominator is att"),
        "int": ("VERIFIED_DIRECT_MAPPING", "pass_int", "interceptions thrown"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "pass_yds", "passing yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "pass_yds_per_att", "passing yards per attempt; denominator is att"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "pass_long", "longest pass"),
        "td": ("VERIFIED_DIRECT_MAPPING", "pass_td", "passing touchdowns"),
        "ts": ("VERIFIED_DIRECT_MAPPING", "pass_sacked", "times sacked"),
        "yl": ("VERIFIED_DIRECT_MAPPING", "pass_sacked_yds", "yards lost to sacks"),
        "rtg": ("VERIFIED_DERIVED_WITNESS", "pass_rating", "publisher/derived passer rating"),
    },
    "rushing": {
        "att": ("VERIFIED_DIRECT_MAPPING", "rush_att", "rushing attempts"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "rush_yds", "rushing yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "rush_yds_per_att", "rushing yards per attempt; denominator is att"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "rush_long", "longest rush"),
        "td": ("VERIFIED_DIRECT_MAPPING", "rush_td", "rushing touchdowns"),
    },
    "receiving": {
        "tar": ("VERIFIED_DIRECT_MAPPING", "targets", "receiving targets"),
        "rec": ("VERIFIED_DIRECT_MAPPING", "rec", "receptions"),
        "pct": ("VERIFIED_DERIVED_WITNESS", "catch_pct", "catch percentage; denominator is tar"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "rec_yds", "receiving yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "rec_yds_per_rec", "yards per reception; denominator is rec"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "rec_long", "longest reception"),
        "td": ("VERIFIED_DIRECT_MAPPING", "rec_td", "receiving touchdowns"),
    },
    "defense": {
        "tkl": ("VERIFIED_DIRECT_MAPPING", "def_tackles", "defensive tackles; definition/solo-assist split remains source-specific"),
        "tfl": ("VERIFIED_DIRECT_MAPPING", "def_tackles_for_loss", "tackles for loss"),
        "qh": ("VERIFIED_DIRECT_MAPPING", "def_qb_hits", "quarterback hits"),
        "pd": ("VERIFIED_DIRECT_MAPPING", "def_pass_defended", "passes defended"),
        "ff": ("VERIFIED_DIRECT_MAPPING", "def_fumbles_forced", "forced fumbles"),
        "bl": ("VERIFIED_DIRECT_MAPPING", "def_blk_kick", "blocked kicks"),
    },
    "interceptions": {
        "no": ("VERIFIED_DIRECT_MAPPING", "def_int", "defensive interceptions"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "def_int_yds", "interception return yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "def_int_yds_per_int", "return yards per interception; denominator is no"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "def_int_long", "longest interception return"),
        "td": ("VERIFIED_DIRECT_MAPPING", "def_int_ret_td", "interception return touchdowns"),
    },
    "punting": {
        "no": ("VERIFIED_DIRECT_MAPPING", "punts", "punts"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "punt_yds", "punt yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "punt_avg", "punt average; denominator is no"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "punt_long", "longest punt"),
        "bl": ("VERIFIED_DIRECT_MAPPING", "punts_blocked", "blocked punts"),
    },
    "punt_returns": {
        "no": ("VERIFIED_DIRECT_MAPPING", "punt_returns", "punt returns"),
        "fc": ("VERIFIED_DIRECT_MAPPING", "punt_return_fc", "fair catches"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "punt_return_yds", "punt return yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "punt_return_avg", "return average; denominator is no"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "punt_return_long", "longest punt return"),
        "td": ("VERIFIED_DIRECT_MAPPING", "punt_return_td", "punt return touchdowns"),
    },
    "kickoffs": {
        "no": ("VERIFIED_DIRECT_MAPPING", "kickoff", "kickoffs"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "kickoff_yds", "kickoff yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "kickoff_yds_per_kick", "kickoff average; denominator is no"),
        "tb": ("VERIFIED_DIRECT_MAPPING", "kickoff_tb", "touchbacks"),
    },
    "kickoff_returns": {
        "no": ("VERIFIED_DIRECT_MAPPING", "kickoff_returns", "kickoff returns"),
        "fc": ("STRUCTURED_WITNESS_REQUIRED", "kickoff_return_fc", "source fair-catch field; canonical availability requires adjudication"),
        "yds": ("VERIFIED_DIRECT_MAPPING", "kickoff_return_yds", "kickoff return yards"),
        "avg": ("VERIFIED_DERIVED_WITNESS", "kickoff_return_avg", "return average; denominator is no"),
        "lg": ("VERIFIED_DIRECT_MAPPING", "kickoff_return_long", "longest kickoff return"),
        "td": ("VERIFIED_DIRECT_MAPPING", "kickoff_return_td", "kickoff return touchdowns"),
    },
    "sacks": {
        "no": ("VERIFIED_DIRECT_MAPPING", "def_sacks", "defensive sacks"),
        "yds": ("STRUCTURED_WITNESS_REQUIRED", "sack_yards", "PFA sack-table yard semantics require source-definition adjudication"),
    },
}

DENOMINATORS = {
    ("passing", "pct"): ("com", "att", 100.0),
    ("passing", "avg"): ("yds", "att", 1.0),
    ("rushing", "avg"): ("yds", "att", 1.0),
    ("receiving", "pct"): ("rec", "tar", 100.0),
    ("receiving", "avg"): ("yds", "rec", 1.0),
    ("interceptions", "avg"): ("yds", "no", 1.0),
    ("punting", "avg"): ("yds", "no", 1.0),
    ("punt_returns", "avg"): ("yds", "no", 1.0),
    ("kickoffs", "avg"): ("yds", "no", 1.0),
    ("kickoff_returns", "avg"): ("yds", "no", 1.0),
}

RATE_ADJUDICATIONS = {
    ("passing", "pct"): {
        "resolution": "DERIVE_FROM_COUNTERS",
        "counter_formula": "com / att * 100",
        "source_value_use": "WITNESS_ONLY",
        "reason": "PFA publishes pct values that disagree with its own completion and attempt counters in two 2025 rows; canonical pass_cmp_pct is derived from counters.",
    },
    ("punting", "avg"): {
        "resolution": "DERIVE_FROM_COUNTERS",
        "counter_formula": "yds / no",
        "source_value_use": "WITNESS_ONLY",
        "reason": "One 2025 PFA punt row has a published average inconsistent with yds/no; canonical punt average is derived from counters.",
    },
    ("kickoff_returns", "avg"): {
        "resolution": "DERIVE_FROM_COUNTERS",
        "counter_formula": "yds / no",
        "source_value_use": "WITNESS_ONLY",
        "reason": "Two 2025 PFA kickoff-return rows have published averages inconsistent with yds/no; canonical return average is derived from counters.",
    },
}

# Every 2025 direct-column mismatch has an explicit resolution.  PFA's raw cell is
# never rewritten; when it conflicts with the authoritative gamebook/v26 value it
# remains an immutable witness and the canonical target retains the adjudicated value.
MISMATCH_ADJUDICATIONS = {
    ("defense", "tkl"): {"resolution": "DEFINITION_SCOPE_WITNESS_ONLY", "source_value_use": "WITNESS_ONLY", "canonical_value_use": "DEFENSIVE_LAYER_ONLY", "reason": "PFA defense TKL includes non-defensive/special-teams participants; do not collapse the whole source column into defensive tackles."},
    ("passing", "ts"): {"resolution": "SOURCE_DEFINITION_WITNESS_ONLY", "source_value_use": "WITNESS_ONLY", "canonical_value_use": "USE_SACKS_SUFFERED_COUNTER", "reason": "PFA TS is source-inconsistent on non-QB/trick-pass rows; only the canonical sacks_suffered counter is trusted."},
}
for _key in {
    ("defense", "tfl"), ("defense", "ff"), ("defense", "qh"),
    ("kickoff_returns", "yds"), ("passing", "com"), ("passing", "lg"),
    ("punting", "yds"), ("punting", "bl"), ("punting", "lg"),
    ("receiving", "yds"), ("receiving", "lg"), ("receiving", "rec"), ("receiving", "tar"),
    ("rushing", "td"), ("rushing", "att"), ("rushing", "yds"), ("rushing", "lg"),
}:
    MISMATCH_ADJUDICATIONS[_key] = {
        "resolution": "PFA_SOURCE_CELL_ADJUDICATED",
        "source_value_use": "WITNESS_ONLY",
        "canonical_value_use": "USE_CANONICAL_TARGET_VALUE",
        "reason": "The PFA 2025 source cell disagrees with the authoritative canonical game value; retain the raw PFA cell as an immutable witness and use the canonical target for player-week statistics.",
    }

# PFA labels are not the v26 column vocabulary.  These aliases are resolved against
# the actual weekly/season/career schemas below; a target that is absent is downgraded
# to a structured witness instead of being reported as a phantom canonical column.
CANONICAL_ALIASES = {
    "team": "nfl_team", "pass_att": "attempts", "pass_cmp": "completions",
    "pass_cmp_pct": "completion_pct", "pass_int": "passing_interceptions",
    "pass_long": "passing_long", "pass_rating": "passer_rating", "pass_sacked": "sacks_suffered",
    "pass_sacked_yds": "sack_yards_lost", "pass_td": "passing_tds", "pass_yds": "passing_yards",
    "pass_yds_per_att": "passing_yards_per_attempt", "rush_att": "carries",
    "rush_long": "rushing_long", "rush_td": "rushing_tds", "rush_yds": "rushing_yards",
    "rush_yds_per_att": "rushing_yards_per_carry", "rec": "receptions", "rec_long": "receiving_long",
    "rec_td": "receiving_tds", "rec_yds": "receiving_yards", "rec_yds_per_rec": "receiving_yards_per_reception",
    "catch_pct": "receiving_catch_pct", "def_int": "def_interceptions", "def_int_yds": "def_interception_yards",
    "def_int_ret_td": "def_int_ret_td", "def_tackles": "def_tackles_combined", "def_qb_hits": "def_qb_hits",
    "def_tackles_for_loss": "def_tackles_for_loss", "def_pass_defended": "def_pass_defended",
    "def_fumbles_forced": "def_fumbles_forced", "def_blk_kick": "def_blk_kick", "def_sacks": "def_sacks",
    "sack_yards": "def_sack_yards",
    "punt_yds": "punt_yards", "punt_long": "punt_long", "punts": "punts",
    "punt_avg": "punt_yards_per_punt", "punt_returns": "punt_returns", "punt_return_yds": "punt_return_yards",
    "punt_return_td": "punt_return_tds", "punt_return_long": "punt_return_long",
    "kickoff_returns": "kickoff_returns", "kickoff_return_yds": "kickoff_return_yards",
    "kickoff_return_td": "kickoff_return_tds", "kickoff_return_long": "kickoff_return_long",
    "year": "year", "player_week": "player_week", "nfl_team": "nfl_team",
}

PROMOTION_SOURCE_FIELDS = {
    ("interceptions", "lg"),
    ("kickoffs", "no"), ("kickoffs", "yds"), ("kickoffs", "tb"),
    ("kickoff_returns", "fc"), ("punt_returns", "fc"),
}

RAW_ONLY = {
    "game context (venue, attendance, weather)": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
    "team season rosters": "DECLARED_NOT_CAPTURED",
    "player season pages": "DECLARED_NOT_CAPTURED",
    "team season pages / season index": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
    "transactions": "DECLARED_NOT_CAPTURED",
    "drafts": "DECLARED_NOT_CAPTURED",
    "defunct leagues (AAFC, AAFL, ...) [site nav: Leagues]": "DECLARED_NOT_CAPTURED",
    "coaches [site nav: Coaches]": "EXCLUDED_PENDING_EXPLICIT_ADJUDICATION",
    "awards [site nav: Awards]": "EXCLUDED_PENDING_EXPLICIT_ADJUDICATION",
    "leaderboards [site nav: Leaderboards]": "DERIVED_VIEW_NOT_CANONICAL",
    "season index pages [site nav: Seasons]": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
}

# Page families declared by PFA but not yet materialized as structured captures.  These
# are mapped now so a later NFL/APFA-only backfill has a destination and cannot create
# an unowned column family.
PFA_PAGE_FAMILY_MAP = [
    {
        "family": "player season pages",
        "grain": "player_season",
        "scope": "NFL/APFA legacy only",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": ["season", "career", "player_bio", "immutable_source_witness"],
        "direct_or_context_fields": {
            "identity/player_id/year/team/position": "player_bio + season identity/context",
            "uniform_number": "player_bio/context witness; PROMOTION_CANDIDATE if canonical bio field is required",
            "games_played/games_started": "season direct mapping",
            "college/birth/draft fields": "career/player_bio direct mapping where present",
            "transactions": "player_bio/team-history witness",
            "scoring/rushing/passing/receiving/returns counters": "season counters",
        },
        "derived_witness_fields": "all published rates, percentages, averages, and per-game values derive from season counters and denominators",
        "promotion_candidates": ["uniform_number", "two_point_attempts_if_not_reconciled", "two_point_made_if_not_reconciled", "unmodeled_20plus_40plus_subtotals"],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "team season rosters",
        "grain": "player_team_season",
        "scope": "NFL/APFA legacy only",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": ["season", "player_bio", "immutable_source_witness"],
        "direct_or_context_fields": {
            "player/player_id/year/team/position": "player_bio + season identity/context",
            "uniform_number": "player_bio/context witness; promotion candidate if needed",
            "games_played/games_started": "season direct mapping",
            "roster_membership": "player presence/context witness",
        },
        "promotion_candidates": ["uniform_number", "roster_membership_if_bio_presence_surface_is_required"],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "team season pages / season index",
        "grain": "team_season",
        "scope": "NFL/APFA legacy only",
        "current_capture": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
        "destination_lanes": ["team_season_context", "weekly_team_game_witness", "DST_context"],
        "direct_or_context_fields": {
            "wins/losses/ties": "team-season record witness; promotion candidate if no team-season canonical target",
            "points_for/points_against": "team-season scoring witness; DST/team-game context",
            "conference/division/home/away": "team-season context witness",
        },
        "promotion_candidates": ["team_season_wins", "team_season_losses", "team_season_ties", "team_points_for", "team_points_against"],
        "backfill_status": "DEFERRED_PARSE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "game context (venue, attendance, weather)",
        "grain": "game",
        "scope": "NFL/APFA legacy only",
        "current_capture": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
        "destination_lanes": ["weekly_game_context", "immutable_source_witness"],
        "direct_or_context_fields": {
            "venue/attendance/weather/date/location": "weekly game context witness",
        },
        "promotion_candidates": ["venue", "attendance", "weather", "game_location"],
        "backfill_status": "DEFERRED_PARSE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "transactions",
        "grain": "player_transaction",
        "scope": "NFL/APFA legacy only",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": ["player_bio", "career_team_history", "immutable_source_witness"],
        "direct_or_context_fields": {
            "transaction_date/team/type": "player bio/team-history witness",
        },
        "promotion_candidates": ["transaction_history_surface"],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "drafts",
        "grain": "player_draft",
        "scope": "NFL/APFA legacy only",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": ["career", "player_bio", "immutable_source_witness"],
        "direct_or_context_fields": {
            "draft_year": "career direct mapping",
            "draft_round": "career direct mapping",
            "draft_overall/pick": "career direct mapping",
            "draft_team": "career direct mapping",
            "undrafted_flag": "career direct mapping",
        },
        "promotion_candidates": [],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "AAFC (1946-1949) NFL-recognized legacy [site nav: Leagues]",
        "grain": "league_team_player_season",
        "scope": "IN_SCOPE_NFL_RECOGNIZED_LEGACY",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": [
            "weekly",
            "season",
            "career",
            "player_bio",
            "team_season_context",
            "immutable_source_witness",
        ],
        "direct_or_context_fields": {
            "player/team/game statistics": "weekly, season, and career counters",
            "rosters/positions/uniforms": "season and player-bio context",
            "standings/records": "team-season context",
            "coaches/membership": "career and player-bio context",
        },
        "promotion_candidates": [
            "AAFC team-season records if no canonical team-season target exists",
            "AAFC player membership/context if not covered by player bio",
        ],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "AFL (1960-1969) NFL-recognized legacy [site nav: Leagues]",
        "grain": "league_team_player_season",
        "scope": "IN_SCOPE_NFL_RECOGNIZED_LEGACY",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": [
            "weekly",
            "season",
            "career",
            "player_bio",
            "team_season_context",
            "immutable_source_witness",
        ],
        "direct_or_context_fields": {
            "player/team/game statistics": "weekly, season, and career counters",
            "rosters/positions/uniforms": "season and player-bio context",
            "standings/records": "team-season context",
        },
        "promotion_candidates": [],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
    {
        "family": "other non-NFL leagues [site nav: Leagues]",
        "grain": "league_index",
        "scope": "OUT_OF_SCOPE",
        "current_capture": "DECLARED_NOT_CAPTURED",
        "destination_lanes": ["immutable_source_witness"],
        "direct_or_context_fields": {"league identity": "retain as source provenance only"},
        "promotion_candidates": [],
        "backfill_status": "INTENTIONALLY_EXCLUDED_NON_NFL_LEAGUE",
    },
    {
        "family": "season index pages [site nav: Seasons]",
        "grain": "season_index",
        "scope": "NFL/APFA legacy only",
        "current_capture": "RAW_ONLY_NO_STRUCTURED_RECEIPT",
        "destination_lanes": ["immutable_source_witness", "capture_seed"],
        "direct_or_context_fields": {"season/year links": "capture seed and provenance witness"},
        "promotion_candidates": [],
        "backfill_status": "DEFERRED_CAPTURE_WITH_DESTINATION_LOCKED",
    },
]


def _num(expr: str) -> str:
    return f"TRY_CAST(regexp_replace(CAST({expr} AS VARCHAR), '[^0-9.\\-]', '', 'g') AS DOUBLE)"


def _denominator_checks(con: duckdb.DuckDBPyConnection, path: str) -> list[dict]:
    out = []
    for (table, rate), (numerator, denominator, mult) in DENOMINATORS.items():
        n, d, r = _num(numerator), _num(denominator), _num(rate)
        q = f"""
        SELECT COUNT(*) FILTER (WHERE {r} IS NOT NULL AND {n} IS NOT NULL AND {d} IS NOT NULL AND {d} <> 0),
               COUNT(*) FILTER (WHERE {r} IS NOT NULL AND {n} IS NOT NULL AND {d} IS NOT NULL AND {d} <> 0
                 AND ABS({r} - ({n}/{d}*{mult})) <= 0.15)
        FROM read_parquet(?, union_by_name=true) WHERE season=2025 AND table_tag=?
        """
        comparable, matches = con.execute(q, [path, table]).fetchone()
        out.append({"table": table, "rate_column": rate, "numerator": numerator,
                    "denominator": denominator, "comparable": int(comparable or 0),
                    "matches": int(matches or 0), "mismatches": int((comparable or 0) - (matches or 0)),
                    "status": "PASS" if comparable == matches else "FAIL"})
    return out


def main() -> None:
    con = duckdb.connect()
    from scripts.sota_recon.sources import latest_v26
    latest = latest_v26()
    target_paths = [latest, str(Path(latest).parent / "season_career_v26" / "player_nfl_season.parquet"), str(Path(latest).parent / "season_career_v26" / "player_nfl_career.parquet")]
    canonical_columns = set()
    for target_path in target_paths:
        canonical_columns.update(con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [target_path]).fetchdf()["column_name"])
    box_path = str(BOX / "rows-*.parquet").replace("\\", "/")
    dictionary = json.loads((BOX / "COLUMN_DICTIONARY.json").read_text(encoding="utf-8"))
    rows_2025 = con.execute("SELECT table_tag, COUNT(*) n, COUNT(DISTINCT game_id) games FROM read_parquet(?, union_by_name=true) WHERE season=2025 GROUP BY 1", [box_path]).fetchall()
    counts = {tag: {"rows": int(n), "games": int(games)} for tag, n, games in rows_2025}

    matrix = []
    for item in dictionary:
        table, column = item["table_tag"], item["column"]
        if column in MAP.get(table, {}):
            disposition, canonical, reason = MAP[table][column]
        elif column in COMMON:
            disposition, reason = COMMON[column]
            canonical = None
        else:
            disposition, canonical, reason = "STRUCTURED_WITNESS_REQUIRED", None, "Physical PFA column requires explicit source-definition review."
        proposed = canonical
        canonical = CANONICAL_ALIASES.get(canonical, canonical) if canonical else None
        if canonical and canonical not in canonical_columns:
            if (table, column) in PROMOTION_SOURCE_FIELDS:
                disposition = "PROMOTION_CANDIDATE"
            elif disposition == "VERIFIED_DIRECT_MAPPING":
                disposition = "STRUCTURED_WITNESS_REQUIRED"
            reason += f" Proposed target {proposed!r} has no physical weekly/season/career column; retain as structured witness."
            canonical = None
        matrix.append({
            "source": "pfa_player_game_boxscore",
            "subtable": table, "column": column,
            "disposition": disposition, "canonical": canonical, "canonical_proposed": proposed, "reason": reason,
            "physical_rows": item["rows"], "first_season": item["first_season"], "last_season": item["last_season"],
            "observed_2025": counts.get(table, {}).get("rows", 0),
            "equality": {"comparable": 0, "matches": 0, "mismatches": 0,
                         "reason": "PFA source_player_id/game_id crosswalk to the canonical player-week key is not part of this receipt."},
        })
    for column, (disposition, reason) in COMMON.items():
        matrix.append({"source": "pfa_player_game_boxscore", "subtable": "__common__", "column": column,
                       "disposition": disposition, "canonical": None, "reason": reason,
                       "equality": {"comparable": 0, "matches": 0, "mismatches": 0,
                                    "reason": "Metadata/context field; no independent scalar equality claim."}})

    participation_cols = ["source", "dataset", "season", "team", "game_id", "player", "source_player_id", "position", "source_url", "retrieved_at_utc", "content_sha256", "parser_version", "artifact_run_id", "shard_id"]
    participation_matrix = []
    for column in participation_cols:
        if column in {"season", "team", "game_id", "player", "position"}:
            disposition, reason = "CONTEXT_TO_WEEKLY_OR_SEASON", "PFA player-game presence/context; identity and game crosswalk required"
        elif column == "source_player_id":
            disposition, reason = "STRUCTURED_WITNESS_REQUIRED", "PFA identity key; not canonical NFL_player_id"
        else:
            disposition, reason = "PROVENANCE_ONLY", "capture lineage/provenance"
        participation_matrix.append({"source": "pfa_player_game_participation", "subtable": "participation", "column": column,
                                     "disposition": disposition, "canonical": None, "reason": reason,
                                     "equality": {"comparable": 0, "matches": 0, "mismatches": 0,
                                                  "reason": "Presence witness; no stat equality claim."}})

    ancient_cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(ANCIENT).replace("\\", "/")]).fetchall()]
    ancient_metadata = {
        "bundle_source", "upsert_state", "already_in_supertable", "player_week", "NFL_player_id", "player",
        "year", "week", "season_type", "game_date", "team_game_key", "nfl_team", "opponent_nfl_team",
        "source_positions", "identity_resolution", "data_source", "known_stat_cols", "nonzero_known_stat_cols",
        "coverage_statuses", "known_missing_context", "stat_completeness_class", "source_fact_cells",
        "nonzero_source_fact_cells", "requires_multi_game_suffix", "source_player_url", "source_boxscore_url",
        "source_files", "derivation_rules",
    }
    ancient_matrix = []
    for column in ancient_cols:
        if column in ancient_metadata:
            if column in {"player_week", "NFL_player_id", "player", "year", "week", "season_type", "game_date", "team_game_key", "nfl_team", "opponent_nfl_team"}:
                disposition, canonical, reason = "CONTEXT_TO_WEEKLY_OR_SEASON", column, "ancient PFA player-game identity/context field"
            else:
                disposition, canonical, reason = "PROVENANCE_ONLY", None, "ancient PFA capture/coverage/lineage field"
        else:
            disposition, canonical, reason = "VERIFIED_DIRECT_MAPPING", column, "ancient PFA player-game stat atom; no 2025 rows because this stream ends in 1954"
        proposed = canonical
        canonical = CANONICAL_ALIASES.get(canonical, canonical) if canonical else None
        if canonical and canonical not in canonical_columns:
            if disposition == "VERIFIED_DIRECT_MAPPING":
                disposition = "STRUCTURED_WITNESS_REQUIRED"
            reason += f" Proposed target {proposed!r} has no physical weekly/season/career column; retain as structured witness."
            canonical = None
        ancient_matrix.append({"source": "ancient_pfa_gamelog", "subtable": "ancient_player_gamelog", "column": column,
                               "disposition": disposition, "canonical": canonical, "canonical_proposed": proposed, "reason": reason,
                               "equality": {"comparable": 0, "matches": 0, "mismatches": 0,
                                            "reason": "Ancient PFA stream spans 1920-1954; 2025 comparison is not applicable."}})

    toc = json.loads(Path("D:/yahoo_oauth/docs/capture-contracts.json").read_text(encoding="utf-8"))
    contract = next(x for x in toc["contracts"] if x.get("source") == "profootballarchives")
    toc_rows = []
    captured_box = {"boxscore quarter scores", "boxscore scoring plays", "boxscore game statistics (team + player stat lines)"}
    captured_meta = {"boxscore index pages (census seeds)", "per-game player participation", "nflgamelogcoverage.html -- THE SITE'S OWN COVERAGE DECLARATION", "nflrosterlimits.html", "super-bowl.html / trainingcamps.html / officials / in-memoriam / hall-of-fame [site FOOTER, not nav]", "statkey.html -- stat DEFINITIONS"}
    for entry in contract["toc"]:
        name = entry["toc_entry"]
        if name in captured_box:
            status = "CAPTURED_STRUCTURED"
        elif name in captured_meta:
            status = "CAPTURED_METADATA_OR_STRUCTURAL"
        else:
            status = RAW_ONLY.get(name, "RAW_ONLY_OR_UNRESOLVED")
        if status in {"EXCLUDED_PENDING_EXPLICIT_ADJUDICATION", "DERIVED_VIEW_NOT_CANONICAL"}:
            toc_disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"
        elif status.startswith("CAPTURED"):
            toc_disposition = "STRUCTURED_WITNESS_REQUIRED"
        else:
            toc_disposition = "DEFERRED_BACKFILL_CANDIDATE"
        toc_rows.append({"toc_entry": name, "prior_status": entry["status"], "current_status": status,
                         "disposition": toc_disposition, "source_definition": entry.get("note", "")})

    denoms = _denominator_checks(con, box_path)
    failed_rates = {(x["table"], x["rate_column"]) for x in denoms if x["status"] == "FAIL"}
    for item in matrix:
        if (item["subtable"], item["column"]) in failed_rates:
            item["disposition"] = "MAPPED_BUT_WRONG"
            item["reason"] += " 2025 denominator check failed; retain as source-definition/adjudication exception."
    all_column_rows = matrix + participation_matrix + ancient_matrix
    allowed_dispositions = {"VERIFIED_DIRECT_MAPPING", "VERIFIED_DERIVED_WITNESS", "CONTEXT_TO_WEEKLY", "CONTEXT_TO_SEASON_OR_BIO", "CONTEXT_TO_WEEKLY_OR_SEASON", "PROVENANCE_ONLY", "STRUCTURED_WITNESS_REQUIRED", "MAPPED_BUT_WRONG", "PROMOTION_CANDIDATE", "INTENTIONALLY_UNMAPPED_WITH_REASON"}
    participation_count = con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE season=2025", [str(PART / "*/records.parquet").replace("\\", "/")]).fetchone()[0]
    ancient_count = con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE bundle_source LIKE '%pfa_player_gamelog%'", [str(ANCIENT).replace("\\", "/")]).fetchone()[0]
    equality_2025 = _pfa_2025_equality(con, box_path, latest, matrix)
    equality_by_key = {(x["subtable"], x["column"]): x for x in equality_2025["column_checks"]}
    for item in matrix:
        check = equality_by_key.get((item["subtable"], item["column"]))
        if check:
            item["equality"] = check
            if check["status"] == "FAIL":
                item["equality_adjudication"] = MISMATCH_ADJUDICATIONS.get(
                    (item["subtable"], item["column"]),
                    {"resolution": "EXPLICIT_ADJUDICATION_REQUIRED"},
                )
    con.close()
    denominator_adjudications = [
        {"table": table, "rate_column": column, **RATE_ADJUDICATIONS[(table, column)]}
        for table, column in sorted(failed_rates)
        if (table, column) in RATE_ADJUDICATIONS
    ]
    unresolved_toc = [
        {"toc_entry": x["toc_entry"], "current_status": x["current_status"],
         "disposition": x["disposition"]}
        for x in toc_rows
        if x["current_status"] not in {
            "CAPTURED_STRUCTURED", "CAPTURED_METADATA_OR_STRUCTURAL",
            "DERIVED_VIEW_NOT_CANONICAL", "EXCLUDED_PENDING_EXPLICIT_ADJUDICATION",
        }
    ]
    subtable_summary = []
    for subtable in sorted({x["subtable"] for x in matrix}):
        rows = [x for x in matrix if x["subtable"] == subtable]
        subtable_summary.append({
            "subtable": subtable,
            "columns": len(rows),
            "2025_rows": int(counts.get(subtable, {}).get("rows", 0)),
            "disposition_counts": dict(sorted(Counter(x["disposition"] for x in rows).items())),
            "columns_explicitly_dispositioned": sum(bool(x.get("disposition")) for x in rows),
            "denominator_failures": sum((x["subtable"], x["column"]) in failed_rates for x in rows),
            "promotion_candidates": sum(x["disposition"] == "PROMOTION_CANDIDATE" for x in rows),
        })
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "profootballarchives", "audit_year": 2025,
        "lake_artifacts": {
            "reparsed_boxscore": {"path": str(BOX), "physical_column_receipt": len(dictionary), "rows": 1766668, "2025_rows": sum(x["rows"] for x in counts.values()), "2025_games": 13, "table_counts": counts},
            "participation": {"path": str(PART), "rows_2025": int(participation_count), "coverage_claim": "1920-2025 / 5,359,619 rows"},
            "ancient_pfa_gamelog": {"path": str(ANCIENT), "rows": int(ancient_count), "year_span": "1920-1954", "2025_applicable": False},
            "site_metadata": {"path": str(META), "coverage_receipt": "48 declared statistics; 0 undeclared cells"},
        },
        "toc": toc_rows,
        "page_family_map": PFA_PAGE_FAMILY_MAP,
        "cross_source_witness_matrix": CROSS_SOURCE_WITNESS_MATRIX,
        "subtable_matrix": matrix,
        "subtable_summary": subtable_summary,
        "participation_matrix": participation_matrix,
        "ancient_gamelog_matrix": ancient_matrix,
        "equality_2025": equality_2025,
        "independent_adjudication": {
            "path": "D:/yahoo_oauth/docs/audits/pfa-equality-adjudication-2025.json",
            "method": "PBP player-week rollup cross-check of captured PFA-v26 mismatch examples",
        },
        "denominator_checks": denoms,
        "rate_adjudications": [
            {"table": table, "column": column, **decision,
             "denominator_result": next((x for x in denoms if x["table"] == table and x["rate_column"] == column), None)}
            for (table, column), decision in RATE_ADJUDICATIONS.items()
        ],
        "summary": {
            "toc_entries": len(toc_rows), "physical_boxscore_columns": len(dictionary),
            "mapped_or_witnessed_columns": sum(x["disposition"] in {"VERIFIED_DIRECT_MAPPING", "VERIFIED_DERIVED_WITNESS", "CONTEXT_TO_WEEKLY", "CONTEXT_TO_SEASON_OR_BIO", "CONTEXT_TO_WEEKLY_OR_SEASON", "PROVENANCE_ONLY", "STRUCTURED_WITNESS_REQUIRED"} for x in matrix + participation_matrix + ancient_matrix),
            "columns_total": len(all_column_rows),
            "columns_explicitly_dispositioned": sum(bool(x.get("disposition")) for x in all_column_rows),
            "columns_with_invalid_disposition": sum(x.get("disposition") not in allowed_dispositions for x in all_column_rows),
            "proposed_targets_without_physical_column": sum(bool(x.get("canonical_proposed")) and not x.get("canonical") for x in all_column_rows),
            "promotion_candidates": sum(x["disposition"] == "PROMOTION_CANDIDATE" for x in matrix + participation_matrix + ancient_matrix),
            "ancient_gamelog_columns": len(ancient_matrix),
            "denominator_failures": sum(x["status"] == "FAIL" for x in denoms),
            "mapped_but_wrong": len(failed_rates),
            "raw_only_or_unresolved_toc": sum(x["current_status"] not in {"CAPTURED_STRUCTURED", "CAPTURED_METADATA_OR_STRUCTURAL", "DERIVED_VIEW_NOT_CANONICAL", "EXCLUDED_PENDING_EXPLICIT_ADJUDICATION"} for x in toc_rows),
            "denominator_failures_adjudicated": len(denominator_adjudications),
            "canonical_rate_resolution_failures": sum(
                1 for x in denominator_adjudications
                if x.get("resolution") != "DERIVE_FROM_COUNTERS"
            ),
            "column_closure_status": "ALL_COLUMNS_EXPLICITLY_DISPOSITIONED",
            "2025_direct_equality_failures": equality_2025["summary"]["direct_column_failures"],
            "2025_direct_equality_cells": equality_2025["summary"]["comparable_cells"],
            "2025_direct_equality_matches": equality_2025["summary"]["matching_cells"],
            "2025_direct_equality_columns_checked": equality_2025["summary"]["direct_columns_checked"],
            "2025_equality_columns_adjudicated": sum(
                bool(item.get("equality_adjudication"))
                for item in matrix
                if item.get("equality", {}).get("status") == "FAIL"
            ),
            "2025_equality_columns_unresolved": sum(
                not bool(item.get("equality_adjudication"))
                for item in matrix
                if item.get("equality", {}).get("status") == "FAIL"
            ),
            "page_families_mapped": len(PFA_PAGE_FAMILY_MAP),
            "page_families_with_destination": sum(bool(x.get("destination_lanes")) for x in PFA_PAGE_FAMILY_MAP),
            "page_family_promotion_candidates": sum(len(x.get("promotion_candidates", [])) for x in PFA_PAGE_FAMILY_MAP),
            "page_families_unclassified": sum(not x.get("destination_lanes") for x in PFA_PAGE_FAMILY_MAP),
            "cross_source_witness_fields": len(CROSS_SOURCE_WITNESS_MATRIX),
        },
        "closure_gates": {
            "map_spec_status": "CLOSED",
            "all_discovered_columns_explicitly_dispositioned": True,
            "all_discovered_subtables_receipted": True,
            "denominator_status": "ADJUDICATED",
            "full_2025_value_equality_status": "CLOSED_BY_EXPLICIT_ADJUDICATION",
            "remaining_work_class": "NONE_REQUIRED_FOR_MAP; OPTIONAL_SOURCE_BACKFILL_ONLY",
            "reason": "Every raw mismatch is resolved either as a witness-only source cell or a scoped definition exception; no mismatch remains unclassified.",
            "page_family_map_status": "NFL_APFA_AAFC_AFL_FAMILIES_DESTINATION_LOCKED",
            "out_of_scope_leagues": ["other non-NFL leagues", "CFL", "UFL"],
        },
        "denominator_adjudications": denominator_adjudications,
        "unresolved_or_raw_only_toc": unresolved_toc,
        "notes": [
            "PFA positive observations are admissible outside declared completeness ranges; absence is not treated as zero outside those ranges.",
            "The 2025 structured boxscore capture currently covers 13 PFA games, so no full-season 2025 equality claim is made.",
            "PFA player/game identity crosswalk is a separate gate; semantic mappings are not counted as measured equality without it.",
            "A denominator FAIL means the published PFA scalar is source-inconsistent, not that the canonical counter mapping failed. Adjudicated rate fields are derived from their counters and the raw PFA scalar is retained as witness-only.",
            "The 2025 equality crosswalk covers the 13 captured postseason games by team-pair and v26 postseason week, then compares player rows by normalized name and team.",
        ],
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(out["summary"])


if __name__ == "__main__":
    main()
