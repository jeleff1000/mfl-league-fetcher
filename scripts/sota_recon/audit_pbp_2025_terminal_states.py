"""Read-only PBP 2025 audit, terminal-state inventory, and promotion discovery.

This script never opens a supertable for writing.  It reads the existing NFLverse
receipt for the player-week map/equality adjudication, then profiles the raw merged
PBP at 2025 play grain to expose terminal states, pivots, low-cardinality enums,
and source columns that have no canonical target.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .sources import v26_plane

ROOT = Path(r"D:\league-history-data\nfl")
PBP = ROOT / "raw" / "stathead" / "generated" / "pbp_merged_1978_2025" / "nfl_pbp_1978_2025_merged.parquet"
ROLLUP = ROOT / "raw" / "stathead" / "generated" / "pbp_supertable_audit_1978_2025" / "pbp_player_week_rollup.parquet"
NFLVERSE_RECEIPT = Path(r"D:\yahoo_oauth\docs\audits\nflverse-lake-audit-2025.json")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-2025-terminal-state-audit.json")

TERMINAL_TOKENS = (
    "result", "transition", "play_type", "field_goal_result", "extra_point_result",
    "two_point_conv_result", "penalty_type", "roof", "surface", "location",
    "game_half", "posteam_type", "st_play_type",
)
IDENTITY_TOKENS = ("player_id", "player_name", "_team", "_player", "coach", "name", "id")
MODEL_TOKENS = ("epa", "wpa", "prob", "wp", "cpoe", "xpass", "air_yards", "yac")


def _schema(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()]


def _classify(column: str, dtype: str, mapped: dict) -> str:
    if mapped.get("disposition") == "CONTEXT_PROMOTION_CANDIDATE":
        return "PROMOTION_CANDIDATE"
    if any(token in column for token in TERMINAL_TOKENS):
        return "TERMINAL_STATE_ENUM"
    if any(token in column for token in IDENTITY_TOKENS):
        return "IDENTITY_STATE"
    if any(token in column for token in MODEL_TOKENS):
        return "MODEL_OR_RATE_STATE"
    if dtype.upper() == "BOOLEAN" or "INT" in dtype.upper() or "DOUBLE" in dtype.upper() or "DECIMAL" in dtype.upper():
        return "EVENT_ATOM_OR_COUNTER"
    return "STRUCTURED_SOURCE_STATE"


def _profile_column(con: duckdb.DuckDBPyConnection, column: str, dtype: str) -> dict:
    qcol = '"' + column.replace('"', '""') + '"'
    base = con.execute(f"""
        SELECT COUNT(*), COUNT({qcol}), COUNT(DISTINCT {qcol}),
               MIN(season) FILTER (WHERE {qcol} IS NOT NULL),
               MAX(season) FILTER (WHERE {qcol} IS NOT NULL)
        FROM read_parquet(?) WHERE season=2025
    """, [str(PBP)]).fetchone()
    rows, nonnull, distinct, first_year, last_year = base
    # Keep the pivot compact: categorical/terminal values get counts; high-cardinality
    # text and numeric columns get only a bounded sample and range.
    value_rows = con.execute(f"""
        SELECT CAST({qcol} AS VARCHAR) AS value_text, COUNT(*) n
        FROM read_parquet(?) WHERE season=2025 AND {qcol} IS NOT NULL
        GROUP BY 1 ORDER BY n DESC, value_text LIMIT 20
    """, [str(PBP)]).fetchall()
    result = {
        "column": column, "dtype": dtype, "rows_2025": int(rows),
        "nonnull_2025": int(nonnull), "null_pct_2025": round((rows - nonnull) * 100.0 / rows, 4) if rows else None,
        "distinct_2025": int(distinct), "first_nonnull_year": first_year,
        "last_nonnull_year": last_year, "top_values_2025": [{"value": v, "rows": int(n)} for v, n in value_rows],
    }
    if any(token in dtype.upper() for token in ("INT", "DOUBLE", "DECIMAL", "FLOAT", "HUGEINT")):
        lo, hi = con.execute(f"SELECT MIN({qcol}), MAX({qcol}) FROM read_parquet(?) WHERE season=2025 AND {qcol} IS NOT NULL", [str(PBP)]).fetchone()
        result["min_2025"], result["max_2025"] = lo, hi
    return result


def run() -> dict:
    receipt = json.loads(NFLVERSE_RECEIPT.read_text(encoding="utf-8"))
    source_map = {
        row["column"]: row for row in receipt["column_map"]
        if row["source"] == "pbp_merged_1978_2025"
    }
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='1500MB'")
    schema = _schema(con, PBP)
    rollup_schema = _schema(con, ROLLUP)
    raw_rows_2025 = int(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE season=2025", [str(PBP)]).fetchone()[0])
    rollup_rows_2025 = int(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE year=2025", [str(ROLLUP)]).fetchone()[0])
    duplicate_play_ids = int(con.execute("""
        SELECT COUNT(*) FROM (
            SELECT game_id, play_id, COUNT(*) n
            FROM read_parquet(?) WHERE season=2025 GROUP BY 1,2 HAVING COUNT(*) > 1
        )
    """, [str(PBP)]).fetchone()[0])
    duplicate_player_weeks = int(con.execute("""
        SELECT COUNT(*) FROM (
            SELECT player_week, COUNT(*) n
            FROM read_parquet(?) WHERE year=2025 GROUP BY 1 HAVING COUNT(*) > 1
        )
    """, [str(ROLLUP)]).fetchone()[0])

    profiles = []
    for column, dtype in schema:
        profile = _profile_column(con, column, dtype)
        profile["terminal_class"] = _classify(column, dtype, source_map.get(column, {}))
        profile["map_spec"] = source_map.get(column, {})
        profiles.append(profile)
    con.close()

    terminal_states = [x for x in profiles if x["terminal_class"] == "TERMINAL_STATE_ENUM"]
    hidden_noncanonical = [
        x for x in profiles
        if x["map_spec"].get("canonical") is None and x["nonnull_2025"] > 0
    ]
    promotions = [
        x for x in profiles
        if x["terminal_class"] == "PROMOTION_CANDIDATE" or (
            x["terminal_class"] == "TERMINAL_STATE_ENUM" and x["nonnull_2025"] > 0
            and x["distinct_2025"] <= 50 and x["map_spec"].get("canonical") is None
        )
    ]
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "NFLverse-rooted merged PBP",
        "read_only": True,
        "source_paths": {"raw_pbp": str(PBP), "player_week_rollup": str(ROLLUP), "canonical_weekly": str(v26_plane("weekly"))},
        "grain": {
            "raw_pbp": "one row per play",
            "player_week_rollup": "one row per player_week",
            "raw_rows_2025": raw_rows_2025,
            "rollup_rows_2025": rollup_rows_2025,
            "duplicate_game_play_keys_2025": duplicate_play_ids,
            "duplicate_player_week_keys_2025": duplicate_player_weeks,
        },
        "rollup_audit": receipt["equality_2025"]["pbp_player_week_rollup"],
        "terminal_state_inventory": terminal_states,
        "column_profiles": profiles,
        "hidden_noncanonical_columns_2025": hidden_noncanonical,
        "promotion_candidates": promotions,
        "summary": {
            "raw_columns": len(schema), "rollup_columns": len(rollup_schema),
            "raw_columns_profiled": len(profiles), "terminal_state_columns": len(terminal_states),
            "noncanonical_nonnull_columns": len(hidden_noncanonical),
            "promotion_candidates": len(promotions),
            "rollup_columns_checked": receipt["summary"]["pbp_rollup_columns_checked"],
            "rollup_columns_unresolved": receipt["summary"]["pbp_rollup_columns_unresolved"],
        },
        "notes": [
            "A raw PBP column with no canonical target is not dropped: it is classified as terminal-state, identity, model/rate, event atom, structured source, or promotion candidate.",
            "NULL/zero differences and source-definition disagreements are inherited from the NFLverse receipt and remain witness adjudications; canonical data is not rewritten.",
            "Top-value pivots are evidence for enum/terminal-state extraction, not a claim that every state is exhaustive outside 2025.",
        ],
    }
    OUT.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(out["summary"])


if __name__ == "__main__":
    run()
