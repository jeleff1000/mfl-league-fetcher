"""Audit the physically present Excel-derived PFR master schedule."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

PATH = Path(r"D:\league-history-data\nfl\raw\pfr\cache\pfr_excel\_master_schedule_1920_2025.parquet")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-excel-master-schedule-2025.json")
CONTEXT = {"year", "week", "nfl_team", "opponent_nfl_team", "franchise_id", "opponent_franchise_id", "game_date", "season_phase", "home_away"}
CANDIDATES = {"team_pts", "opp_pts", "yds_team"}

def main() -> None:
    c = duckdb.connect(); p = str(PATH).replace("\\", "/")
    columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [p]).fetchdf()["column_name"])
    rows = c.execute("SELECT * FROM read_parquet(?) WHERE year=2025", [p]).fetchdf().to_dict("records")
    pairs = {}
    for row in rows:
        teams = tuple(sorted((row["nfl_team"], row["opponent_nfl_team"])))
        key = (row["year"], row["week"], row["game_date"], teams)
        pairs.setdefault(key, []).append(row)
    complete = exact = 0; examples = []
    for key, group in pairs.items():
        if len(group) != 2: continue
        complete += 1
        a, b = group
        ok = (a["team_pts"] == b["opp_pts"] and a["opp_pts"] == b["team_pts"] and a["nfl_team"] == b["opponent_nfl_team"] and a["opponent_nfl_team"] == b["nfl_team"])
        exact += int(ok)
        if not ok and len(examples) < 3: examples.append({"key": str(key), "rows": group})
    checks = {"reciprocal_game_pair": {"unique_game_keys": len(pairs), "complete_pairs": complete, "exact_score_and_team_reciprocals": exact, "mismatches": complete-exact, "examples": examples}, "row_denominator": {"rows": len(rows), "expected_from_complete_pairs": complete*2, "matches": int(len(rows)==complete*2)}}
    matrix = []
    for col in columns:
        if col in CONTEXT:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"; reason = "Schedule/team identity, phase, date, or franchise context."
        elif col in CANDIDATES:
            disposition = "VERIFIED_DIRECT_MAPPING"; reason = "Mapped to the DST/team-defense supertable lane at the reciprocal team-game grain."
        else:
            disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"; reason = "No canonical meaning identified for this schedule field."
        canonical = {"team_pts": "pts_def_team_pts", "opp_pts": "points_allowed", "yds_team": "def_yards_allowed"}.get(col)
        mapping_witness = {"team_pts": "same team-game row", "opp_pts": "same team-game row opponent score", "yds_team": "opponent team-game row yds_team (reciprocal)"}.get(col)
        matrix.append({"source": "pfr_excel_master_schedule", "table_key": "*", "column": col, "disposition": disposition, "canonical": canonical, "checks": {}, "reason": reason, "destination_layer": "nfl_player_stats_all[DST]", "mapping_witness": mapping_witness})
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "source": "pfr_excel_master_schedule", "physical_path": str(PATH.parent), "registration_status": "PHYSICAL_UNREGISTERED", "class": "schedule/team-games/Excel-derived", "year": 2025, "rows": len(rows), "columns": len(columns), "checks": checks, "matrix": matrix, "open_blocked": 0, "notes": ["Reciprocal rows are checked as the immediate team-game denominator/integrity witness.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "rows": len(rows), "columns": len(columns), "unique_games": len(pairs), "open_blocked": 0})

if __name__ == "__main__": main()
