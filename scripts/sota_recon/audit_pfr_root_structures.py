"""Audit PFR root cache/context containers without mistaking manifests for stats."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PFR = Path(r"D:\league-history-data\nfl\raw\pfr")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-root-structures-2025.json")
SCHEDULE = PFR / "cache/pfr_excel/_master_schedule_1920_2025.parquet"
MANIFEST = PFR / "context/compact_manifest.parquet"
BOX_MANIFEST = PFR / "boxscores/compact_manifest.parquet"
PLAYER_MANIFEST = PFR / "players/compact_manifest.parquet"

SCHEDULE_CONTEXT = {"year", "week", "nfl_team", "opponent_nfl_team", "franchise_id", "opponent_franchise_id", "game_date", "season_phase", "home_away"}


def audit_manifest(con: duckdb.DuckDBPyConnection, path: Path, kind: str) -> dict:
    parquet = str(path).replace("\\", "/")
    columns = list(con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parquet]).fetchdf()["column_name"])
    rows = con.execute("SELECT * FROM read_parquet(?)", [parquet]).fetchdf().to_dict("records")
    checks = []
    for row in rows:
        raw = str(row["path"])
        table_id = str(row["table_id"])
        if kind == "context":
            relative = raw.replace("ops_data\\pfr_context\\tables\\", "").replace("\\", "/")
            target = PFR / "context/tables" / relative
        elif kind == "boxscores":
            relative = raw.replace("ops_data\\pfr_boxscores\\tables\\", "").replace("\\", "/")
            target = PFR / "boxscores/tables" / relative
        else:
            target = PFR / "players/tables" / table_id / "_combined.parquet"
        exists = target.exists()
        actual = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(target).replace("\\", "/")]).fetchone()[0] if exists else None
        declared = int(row["rows"]) if row["rows"] is not None else None
        checks.append({"table_id": table_id, "path": raw, "resolved_path": str(target), "exists": exists, "declared_rows": declared, "actual_rows": actual, "row_count_matches": bool(exists and declared == actual)})
    source = f"pfr_{kind}_manifest"
    matrix = [{"source": source, "table_key": "*", "column": col, "disposition": "CONTEXT_TO_SEASON_OR_BIO", "semantic_disposition": "MANIFEST_METADATA", "canonical": None, "checks": {}, "reason": "Compact manifest metadata used to inventory underlying PFR tables; not a football statistic.", "destination_layer": "source_provenance"} for col in columns]
    return {"source": source, "physical_path": str(path.parent), "registration_status": "PHYSICAL_UNREGISTERED", "class": f"root {kind} manifest", "rows": len(rows), "columns": len(columns), "checks": {"manifest_paths": {"entries": len(checks), "existing": sum(x["exists"] for x in checks), "row_count_matches": sum(x["row_count_matches"] for x in checks), "mismatches": [x for x in checks if not x["row_count_matches"]]}}, "matrix": matrix, "path_checks": checks}


def main() -> None:
    con = duckdb.connect()
    schedule_path = str(SCHEDULE).replace("\\", "/")
    schedule_cols = list(con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [schedule_path]).fetchdf()["column_name"])
    schedule_rows = con.execute("SELECT * FROM read_parquet(?) WHERE year=2025", [schedule_path]).fetchdf().to_dict("records")
    pairs = {}
    for row in schedule_rows:
        teams = tuple(sorted((row["nfl_team"], row["opponent_nfl_team"])))
        key = (row["year"], row["week"], row["game_date"], teams)
        pairs.setdefault(key, []).append(row)
    complete = exact = 0
    for group in pairs.values():
        if len(group) != 2:
            continue
        complete += 1
        a, b = group
        exact += int(a["team_pts"] == b["opp_pts"] and a["opp_pts"] == b["team_pts"] and a["nfl_team"] == b["opponent_nfl_team"] and a["opponent_nfl_team"] == b["nfl_team"])
    schedule_matrix = []
    for col in schedule_cols:
        if col in SCHEDULE_CONTEXT:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"; semantic = "TEAM_GAME_SCHEDULE_CONTEXT"; reason = "Team-game schedule identity/context; not a player-grain supertable field."
        elif col in {"team_pts", "opp_pts", "yds_team"}:
            disposition = "PROMOTION_CANDIDATE"; semantic = "TEAM_GAME_SCALAR_CANDIDATE"; reason = "Team-game scalar candidate for a future team-game layer; not promoted into player stats."
        else:
            disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"; semantic = "SOURCE_ONLY"; reason = "Root cache field has no canonical player-stat destination."
        schedule_matrix.append({"source": "pfr_cache_schedule", "table_key": "*", "column": col, "disposition": disposition, "semantic_disposition": semantic, "canonical": None, "checks": {}, "reason": reason, "destination_layer": "team_game" if col not in SCHEDULE_CONTEXT else "team_game_context"})

    context_manifest = audit_manifest(con, MANIFEST, "context")
    box_manifest = audit_manifest(con, BOX_MANIFEST, "boxscores")
    player_manifest = audit_manifest(con, PLAYER_MANIFEST, "players")
    tables = [
        {"source": "pfr_cache_schedule", "physical_path": str(SCHEDULE.parent), "registration_status": "PHYSICAL_UNREGISTERED", "class": "root cache/team schedule", "year": 2025, "rows": len(schedule_rows), "columns": len(schedule_cols), "checks": {"reciprocal_game_pairs": {"unique_game_keys": len(pairs), "complete_pairs": complete, "exact_reciprocals": exact, "mismatches": complete - exact}, "row_denominator": {"rows": len(schedule_rows), "expected_from_complete_pairs": complete * 2, "matches": len(schedule_rows) == complete * 2}}, "matrix": schedule_matrix},
        context_manifest, box_manifest, player_manifest,
    ]
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "tables": tables, "open_blocked": 0, "notes": ["Root cache schedule is audited at team-game grain; the existing Excel schedule receipt remains the detailed source receipt.", "Root compact manifests are not stat tables; all manifest paths and declared row counts were checked against underlying parquet files.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "schedule_rows_2025": len(schedule_rows), "manifest_entries": sum(x["rows"] for x in (context_manifest, box_manifest, player_manifest)), "manifest_row_count_matches": sum(x["checks"]["manifest_paths"]["row_count_matches"] for x in (context_manifest, box_manifest, player_manifest)), "open_blocked": 0})


if __name__ == "__main__":
    main()
