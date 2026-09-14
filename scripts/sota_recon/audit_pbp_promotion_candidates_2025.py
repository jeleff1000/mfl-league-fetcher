"""2025 audit and map-spec receipt for all PBP promotion candidates."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.sota_recon.sources import v26_plane


RAW = Path(r"D:\league-history-data\nfl\raw\stathead\generated\pbp_merged_1978_2025\nfl_pbp_1978_2025_merged.parquet")
LEDGER = Path(r"D:\yahoo_oauth\docs\audits\pbp-embedded-grammar-column-audit.json")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-promotion-candidates-2025.json")


def lane_for(column: str) -> str:
    if column.startswith("drive_"):
        return "team_game_and_season_team"
    if column in {"start_time", "time_of_day", "stadium", "weather", "location", "roof", "surface", "temp", "wind", "home_coach", "away_coach", "stadium_id", "game_stadium"}:
        return "game_context_and_schedule"
    if column.startswith("lateral_") or column == "own_kickoff_recovery":
        return "weekly_player_event_and_special_teams"
    return "weekly_season_and_season_team"


def main() -> None:
    ledger = json.loads(LEDGER.read_text(encoding="utf-8"))
    candidates = [x for x in ledger["ledger"] if x["action"] in {"PROMOTION_CANDIDATE", "CONTEXT_PROMOTION_CANDIDATE"}]
    con = duckdb.connect()
    raw = RAW.as_posix().replace("'", "''")
    result = []
    for item in candidates:
        c = item["column"]
        # Profile 2025 and full-lake availability in one source scan per column.
        profile = con.execute(f'''SELECT
              count(*) FILTER (WHERE season=2025) AS rows_2025,
              count("{c}") FILTER (WHERE season=2025) AS nonnull_2025,
              count(distinct "{c}") FILTER (WHERE season=2025) AS distinct_2025,
              min(try_cast("{c}" AS DOUBLE)) FILTER (WHERE season=2025) AS min_numeric_2025,
              max(try_cast("{c}" AS DOUBLE)) FILTER (WHERE season=2025) AS max_numeric_2025,
              min(season) AS first_lake_year,
              max(season) AS last_lake_year,
              count(*) FILTER (WHERE season=2025 AND try_cast("{c}" AS DOUBLE) <> 0) AS nonzero_numeric_2025,
              count(*) FILTER (WHERE try_cast("{c}" AS DOUBLE) <> 0) AS nonzero_numeric_lake
            FROM read_parquet('{raw}')''').fetchone()
        values = con.execute(f'''SELECT coalesce(cast("{c}" AS VARCHAR), '<NULL>') AS value, count(*) AS rows
            FROM read_parquet('{raw}') WHERE season=2025 GROUP BY 1 ORDER BY rows DESC, value LIMIT 20''').fetchall()
        if c.startswith("drive_"):
            grain = con.execute(f'''SELECT count(*) AS drive_groups,
                count(*) FILTER (WHERE try_cast(value AS DOUBLE) <> 0) AS nonzero_drive_groups
                FROM (SELECT game_id, posteam, drive, max("{c}") AS value
                      FROM read_parquet('{raw}') WHERE season=2025 AND posteam IS NOT NULL AND drive IS NOT NULL
                      GROUP BY 1,2,3)''').fetchone()
            grain_spec = {"grain": "game_id + posteam + drive", "groups_2025": int(grain[0]), "nonzero_groups_2025": int(grain[1])}
        else:
            grain_spec = {"grain": "play", "rows_2025": int(profile[0])}
        result.append({
            "column": c,
            "action": item["action"],
            "grammar_kind": item["grammar_kind"],
            "target_or_candidate": item["target_or_candidate"],
            "target_lane": lane_for(c),
            "map_spec": {
                "source": "nfl_pbp_1978_2025_merged",
                "source_column": c,
                "disposition": "DEFERRED_PROMOTION_CANDIDATE",
                "canonical_presence_by_grain": item["canonical_presence_by_grain"],
                "reason": item["reason"],
            },
            "grain": grain_spec,
            "coverage": {
                "rows_2025": int(profile[0]),
                "nonnull_2025": int(profile[1]),
                "distinct_2025": int(profile[2]),
                "min_numeric_2025": profile[3],
                "max_numeric_2025": profile[4],
                "first_lake_year": int(profile[5]),
                "last_lake_year": int(profile[6]),
                "nonzero_numeric_2025": int(profile[7]),
                "nonzero_numeric_lake": int(profile[8]),
                "top_values_2025": [{"value": str(v), "rows": int(n)} for v, n in values],
            },
        })
    con.close()
    receipt = {
        "source": str(RAW),
        "canonical_grains": {g: v26_plane(g) for g in ("weekly", "season", "career", "season_team")},
        "year": 2025,
        "candidate_count": len(result),
        "all_candidates_explicitly_map_specced": True,
        "no_supertable_writes": True,
        "candidates": result,
    }
    OUT.write_text(json.dumps(receipt, indent=2, default=str), encoding="utf-8")
    print(json.dumps({
        "candidate_count": len(result),
        "columns": [x["column"] for x in result],
        "all_candidates_explicitly_map_specced": True,
        "output": str(OUT),
    }, indent=2))


if __name__ == "__main__":
    main()
