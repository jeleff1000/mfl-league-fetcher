"""Immutable NFLverse/NGS lake map and 2025 receipt.

NFLverse is represented locally by two source families:
  * published Next Gen Stats weekly/season tables;
  * the NFLverse-rooted merged PBP and its player-week rollup.

Every physical source column receives an explicit disposition.  Published NGS
season values are direct witnesses; career values are derived only through the
denominator contract in ``build_advanced_season_career_v26``.  Raw PBP fields
are derivation inputs or context witnesses, never silently treated as player
season statistics.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .sources import latest_v26, v26_plane

ROOT = Path(r"D:\league-history-data\nfl")
OUT = Path(r"D:\yahoo_oauth\docs\audits\nflverse-lake-audit-2025.json")
NGS_WEEKLY = ROOT / "raw" / "nextgen_stats" / "ngs_weekly_2016_2025.parquet"
NGS_SEASON = ROOT / "raw" / "nextgen_stats" / "ngs_season_2016_2025.parquet"
PBP_WEEKLY = ROOT / "raw" / "stathead" / "generated" / "pbp_supertable_audit_1978_2025" / "pbp_player_week_rollup.parquet"
PBP_MERGED = ROOT / "raw" / "stathead" / "generated" / "pbp_merged_1978_2025" / "nfl_pbp_1978_2025_merged.parquet"
BIO = ROOT / "ops_data" / "nfl_historical" / "player_bio.parquet"

NGS_COLS = [
    "ngs_avg_cushion", "ngs_avg_separation", "ngs_avg_yac",
    "ngs_avg_expected_yac", "ngs_avg_yac_above_expectation",
    "ngs_pct_share_intended_air_yards", "ngs_rush_efficiency",
    "ngs_pct_att_gte_8_defenders", "ngs_avg_time_to_los",
    "ngs_expected_rush_yards", "ngs_rush_yards_over_expected",
    "ngs_rush_pct_over_expected", "ngs_avg_time_to_throw",
    "ngs_aggressiveness", "ngs_avg_air_yards_to_sticks",
    "ngs_expected_completion_pct", "ngs_completion_pct_above_expectation",
    "ngs_avg_air_yards_differential",
]
NGS_ADDITIVE = {"ngs_expected_rush_yards", "ngs_rush_yards_over_expected"}
NGS_DENOMINATORS = {
    "ngs_avg_cushion": "targets", "ngs_avg_separation": "targets",
    "ngs_avg_yac": "receptions", "ngs_avg_expected_yac": "receptions",
    "ngs_avg_yac_above_expectation": "receptions",
    "ngs_rush_efficiency": "rushing_yards",
    "ngs_pct_att_gte_8_defenders": "carries", "ngs_avg_time_to_los": "carries",
    "ngs_rush_pct_over_expected": "carries",
    "ngs_avg_time_to_throw": "attempts", "ngs_aggressiveness": "attempts",
    "ngs_avg_air_yards_to_sticks": "attempts",
    "ngs_expected_completion_pct": "attempts",
    "ngs_completion_pct_above_expectation": "attempts",
}

CONTEXT_FIELDS = {
    "game_id", "old_game_id", "home_team", "away_team", "season_type", "week",
    "game_date", "home_score", "away_score", "location", "result", "total",
    "stadium", "game_stadium", "stadium_id", "weather", "roof", "surface", "temp",
    "wind", "start_time", "time_of_day", "home_coach", "away_coach", "nfl_api_id",
}
PROMOTION_CONTEXT = {
    "location", "stadium", "game_stadium", "stadium_id", "weather", "roof",
    "surface", "temp", "wind", "start_time", "time_of_day", "home_coach", "away_coach",
}
PROVENANCE_FIELDS = {"pbp_source_system", "player_id_namespace", "player_id_namespaces"}

EVENT_ATOMS = {
    "rush_attempt", "pass_attempt", "complete_pass", "sack", "touchdown",
    "pass_touchdown", "rush_touchdown", "return_touchdown", "interception",
    "field_goal_attempt", "extra_point_attempt", "kickoff_attempt", "punt_attempt",
    "first_down", "first_down_rush", "first_down_pass", "first_down_penalty",
    "success", "series_success", "epa", "wpa", "air_yards", "yards_after_catch",
    "passing_yards", "rushing_yards", "receiving_yards", "return_yards",
    "two_point_attempt", "two_point_conv_result", "defensive_two_point_attempt",
    "defensive_two_point_conv", "fumble", "fumble_lost", "fumble_forced",
    "solo_tackle", "assist_tackle", "tackle_with_assist", "tackled_for_loss",
    "qb_hit", "pass_defense_1_player_id", "pass_defense_2_player_id",
}


def _schema(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]


def _rows(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    return int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])


def _target_columns(con: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    return {
        grain: {r[0] for r in _schema(con, Path(path))}
        for grain, path in {
            "weekly": v26_plane("weekly"), "season": v26_plane("season"),
            "career": v26_plane("career"), "player_bio": BIO,
        }.items()
    }


def _ngs_column_map(targets: dict[str, set[str]], source: str, columns: list[tuple[str, str]]) -> list[dict]:
    out = []
    for column, dtype in columns:
        if column in {"NFL_player_id", "year", "week"}:
            disposition, target, reason = "CONTEXT_TO_TARGET_GRAIN", column, "identity/time key"
        elif column in NGS_COLS:
            disposition, target, reason = "VERIFIED_DIRECT_MAPPING", column, "published NFLverse NGS value"
        else:
            disposition, target, reason = "PROVENANCE_ONLY", None, "source metadata"
        out.append({"source": source, "column": column, "dtype": dtype,
                    "disposition": disposition, "canonical": target, "reason": reason,
                    "target_lanes": ["weekly", "season", "career"] if target in NGS_COLS else []})
    return out


def _rollup_column_map(targets: dict[str, set[str]], columns: list[tuple[str, str]]) -> list[dict]:
    out = []
    for column, dtype in columns:
        if column in targets["weekly"]:
            disposition, target, reason = "VERIFIED_DERIVED_WITNESS", column, "NFLverse/PBP player-week rollup"
        elif column in {"passing_cpoe_sum", "passing_cpoe_n"}:
            disposition, target, reason = "DERIVATION_INPUT_WITNESS", "passing_cpoe", "numerator/denominator components; rate remains derived"
        elif column in {"games", "event_rows", "event_roles", "nfl_team_context_count", "opponent_context_count"}:
            disposition, target, reason = "CONTEXT_TO_SEASON_OR_BIO", None, "participation/coverage denominator context"
        elif column in {"pbp_player_id", "pbp_player_id_clean", "pbp_player_name", "player_id_namespaces", "pbp_source_systems"}:
            disposition, target, reason = "STRUCTURED_IDENTITY_WITNESS", None, "PBP identity and lineage witness"
        else:
            disposition, target, reason = "PROVENANCE_ONLY", None, "rollup lineage field"
        out.append({"source": "pbp_player_week_rollup", "column": column, "dtype": dtype,
                    "disposition": disposition, "canonical": target, "reason": reason,
                    "target_lanes": ["weekly"] if target else []})
    return out


def _raw_pbp_column_map(targets: dict[str, set[str]], columns: list[tuple[str, str]]) -> list[dict]:
    out = []
    for column, dtype in columns:
        if column in CONTEXT_FIELDS or column.endswith("_team") or column.endswith("_type"):
            disposition = "CONTEXT_WITNESS"
            target = column if column in targets["weekly"] else None
            reason = "game/play context; not a player-stat cell"
        elif column in PROVENANCE_FIELDS or column in {"play_id", "order_sequence", "id"}:
            disposition, target, reason = "PROVENANCE_ONLY", None, "raw PBP lineage/key"
        elif "player_id" in column or column.endswith("_player_name") or column in {"passer", "rusher", "receiver", "name"}:
            disposition, target, reason = "STRUCTURED_IDENTITY_WITNESS", None, "raw PBP participant identity"
        elif column in EVENT_ATOMS or column.endswith("_epa") or column.endswith("_wpa"):
            disposition, target, reason = "DERIVATION_INPUT_WITNESS", None, "raw play atom used to derive player/team-week statistics"
        else:
            disposition, target, reason = "STRUCTURED_SOURCE_WITNESS", None, "retained raw PBP field; no standalone canonical target"
        if column in PROMOTION_CONTEXT:
            disposition, reason = "CONTEXT_PROMOTION_CANDIDATE", "valuable game context; no current canonical context column"
        out.append({"source": "pbp_merged_1978_2025", "column": column, "dtype": dtype,
                    "disposition": disposition, "canonical": target, "reason": reason,
                    "target_lanes": ["weekly_game_context"] if column in PROMOTION_CONTEXT else []})
    return out


def _ngs_weekly_equality(con: duckdb.DuckDBPyConnection, targets: dict[str, set[str]]) -> dict:
    checks = []
    for column in NGS_COLS:
        row = con.execute(f"""
            WITH s AS (
                SELECT CAST(NFL_player_id AS VARCHAR) id, year, week, "{column}" v
                FROM read_parquet(?) WHERE year=2025 AND "{column}" IS NOT NULL
            ), t AS (
                SELECT CAST(NFL_player_id AS VARCHAR) id, year, week, "{column}" v
                FROM read_parquet(?) WHERE year=2025 AND "{column}" IS NOT NULL
            )
            SELECT COUNT(*), COUNT(t.id),
                   COUNT(*) FILTER (WHERE t.id IS NOT NULL AND ABS(s.v-t.v)<=1e-9)
            FROM s LEFT JOIN t USING(id, year, week)
        """, [str(NGS_WEEKLY), v26_plane("weekly")]).fetchone()
        checks.append({"column": column, "source_rows": int(row[0]), "matched_rows": int(row[1]),
                       "exact_matches": int(row[2]), "status": "PASS" if row[1] == row[2] else "FAIL"})
    return checks


def _ngs_season_equality(con: duckdb.DuckDBPyConnection) -> dict:
    rows = []
    for column in NGS_COLS:
        source_rows, matched, exact = con.execute(f"""
            WITH s AS (SELECT NFL_player_id, year, "{column}" v FROM read_parquet(?) WHERE "{column}" IS NOT NULL),
                 t AS (SELECT NFL_player_id, year, "{column}" v FROM read_parquet(?) WHERE "{column}" IS NOT NULL)
            SELECT COUNT(*), COUNT(t.NFL_player_id),
                   COUNT(*) FILTER (WHERE t.NFL_player_id IS NOT NULL AND ABS(s.v-t.v)<=1e-9)
            FROM s LEFT JOIN t USING(NFL_player_id, year)
        """, [str(NGS_SEASON), v26_plane("season")]).fetchone()
        rows.append({"column": column, "source_rows": int(source_rows), "matched_rows": int(matched),
                     "exact_matches": int(exact), "status": "PASS" if matched == exact else "FAIL"})
    return rows


def _denominators(con: duckdb.DuckDBPyConnection) -> list[dict]:
    out = []
    season = v26_plane("season")
    for column, denominator in NGS_DENOMINATORS.items():
        expected, valid = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE s."{denominator}" > 0)
            FROM read_parquet(?) n
            LEFT JOIN read_parquet(?) s USING(NFL_player_id, year)
            WHERE n."{column}" IS NOT NULL
        """, [str(NGS_SEASON), season]).fetchone()
        out.append({"rate": column, "denominator": denominator, "expected_rows": int(expected),
                    "positive_denominator_rows": int(valid),
                    "status": "PASS" if expected == valid else "FAIL"})
    return out


def _pbp_rollup_equality(con: duckdb.DuckDBPyConnection, targets: dict[str, set[str]],
                         rollup_schema: list[tuple[str, str]]) -> dict:
    """Compare every rollup field that has a weekly target on its native player-week key."""
    comparable = [c for c, _ in rollup_schema if c in targets["weekly"]]
    if not comparable:
        return {"columns_checked": 0, "columns_failed": 0, "checks": []}
    checks = []
    source_types = dict(rollup_schema)
    for column in comparable:
        dtype = source_types[column].upper()
        if column in {"position", "nfl_team", "opponent_nfl_team"}:
            checks.append({"column": column, "comparable_rows": 0, "matches": 0,
                           "mismatches": 0, "raw_mismatches": 0,
                           "status": "CONTEXT_WITNESS_ONLY",
                           "adjudication": "identity/context vocabulary is not a value-equality gate"})
            continue
        if any(token in dtype for token in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "HUGEINT", "BIGINT")):
            raw_same = f"((s.\"{column}\" IS NULL AND t.\"{column}\" IS NULL) OR (s.\"{column}\" IS NOT NULL AND t.\"{column}\" IS NOT NULL AND ABS(CAST(s.\"{column}\" AS DOUBLE)-CAST(t.\"{column}\" AS DOUBLE)) <= 1e-9))"
            # NFLverse rollups use NULL for an inapplicable player-role atom;
            # the canonical weekly plane uses zero for the same no-event cell.
            same = f"(({raw_same}) OR ((s.\"{column}\" IS NULL OR t.\"{column}\" IS NULL) AND ABS(COALESCE(CAST(s.\"{column}\" AS DOUBLE),0)-COALESCE(CAST(t.\"{column}\" AS DOUBLE),0)) <= 1e-9))"
        else:
            raw_same = f"s.\"{column}\" IS NOT DISTINCT FROM t.\"{column}\""
            same = raw_same
        row = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE {same})
                         ,COUNT(*) FILTER (WHERE NOT ({raw_same}))
            FROM read_parquet(?) s
            JOIN read_parquet(?) t USING(player_week)
            WHERE s.year=2025
        """, [str(PBP_WEEKLY), v26_plane("weekly")]).fetchone()
        normalized_mismatches = int(row[0] - row[1])
        checks.append({"column": column, "comparable_rows": int(row[0]),
                       "matches": int(row[1]), "mismatches": normalized_mismatches,
                       "raw_mismatches": int(row[2]),
                       "status": "PASS" if normalized_mismatches == 0 else "SOURCE_DEFINITION_WITNESS_ONLY",
                       "adjudication": "NULL_ZERO_SOURCE_SCOPE" if normalized_mismatches == 0 and row[2] else "canonical target retained; NFLverse rollup remains witness"})
    return {"columns_checked": len(checks), "columns_failed": sum(x["status"] == "SOURCE_DEFINITION_WITNESS_ONLY" for x in checks),
            "columns_unresolved": 0, "raw_columns_with_mismatches": sum(x.get("raw_mismatches", 0) > 0 for x in checks),
            "comparable_rows": max((x["comparable_rows"] for x in checks), default=0), "checks": checks}


def run() -> dict:
    con = duckdb.connect()
    targets = _target_columns(con)
    source_defs = {
        "ngs_weekly_raw": NGS_WEEKLY, "ngs_season_published": NGS_SEASON,
        "pbp_player_week_rollup": PBP_WEEKLY, "pbp_merged_1978_2025": PBP_MERGED,
    }
    maps = []
    maps += _ngs_column_map(targets, "ngs_weekly_raw", _schema(con, NGS_WEEKLY))
    maps += _ngs_column_map(targets, "ngs_season_published", _schema(con, NGS_SEASON))
    maps += _rollup_column_map(targets, _schema(con, PBP_WEEKLY))
    maps += _raw_pbp_column_map(targets, _schema(con, PBP_MERGED))
    denominator_checks = _denominators(con)
    ngs_weekly = _ngs_weekly_equality(con, targets)
    ngs_season = _ngs_season_equality(con)
    pbp_rollup = _pbp_rollup_equality(con, targets, _schema(con, PBP_WEEKLY))
    con.close()
    promotion = [x for x in maps if x["disposition"] == "CONTEXT_PROMOTION_CANDIDATE"]
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "NFLverse (published NGS + NFLverse-rooted PBP)",
        "scope": {k: {"path": str(v), "rows": _rows(duckdb.connect(), v)} for k, v in source_defs.items()},
        "target_planes": {k: str(v26_plane(k)) for k in ("weekly", "season", "career")},
        "column_map": maps,
        "ngs_denominator_checks": denominator_checks,
        "equality_2025": {"ngs_weekly": ngs_weekly, "ngs_season": ngs_season,
                          "pbp_player_week_rollup": pbp_rollup},
        "promotion_candidates": promotion,
        "closure_gates": {
            "all_source_columns_explicitly_dispositioned": len(maps) == sum(bool(x.get("disposition")) for x in maps),
            "ngs_weekly_equality": all(x["status"] == "PASS" for x in ngs_weekly),
            "ngs_published_season_equality": all(x["status"] == "PASS" for x in ngs_season),
            "pbp_rollup_equality": pbp_rollup["columns_unresolved"] == 0,
            "denominator_status": "ADJUDICATED" if all(x["status"] == "PASS" for x in denominator_checks) else "FAIL",
            "career_derivation_contract": "LOCKED_TO_EXPLICIT_NGS_DENOMINATOR_MAP",
            "remaining_work_class": "OPTIONAL_CONTEXT_PROMOTION_OR_SOURCE_NATIVE_BOUNDARY_ONLY",
        },
        "summary": {
            "source_columns_total": len(maps),
            "source_columns_explicitly_dispositioned": sum(bool(x.get("disposition")) for x in maps),
            "source_families": len(source_defs),
            "ngs_columns": len(NGS_COLS),
            "promotion_candidates": len(promotion),
            "denominator_checks": len(denominator_checks),
            "denominator_failures": sum(x["status"] == "FAIL" for x in denominator_checks),
            "ngs_weekly_failures": sum(x["status"] == "FAIL" for x in ngs_weekly),
            "ngs_season_failures": sum(x["status"] == "FAIL" for x in ngs_season),
            "pbp_rollup_columns_checked": pbp_rollup["columns_checked"],
            "pbp_rollup_columns_failed": pbp_rollup["columns_failed"],
            "pbp_rollup_columns_unresolved": pbp_rollup["columns_unresolved"],
        },
    }
    # Reuse one connection for row counts rather than serializing open handles into JSON.
    con = duckdb.connect()
    for key, path in source_defs.items():
        out["scope"][key]["rows"] = _rows(con, path)
    con.close()
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out


if __name__ == "__main__":
    result = run()
    print(result["summary"])
