"""Receipt the capture boundary for NFL.com Recent Games.

This surface is not a career subject.  Its physical rows have no season/team
key and no position-block discriminator, so a numeric field cannot be assigned
to a weekly canonical without inventing the missing lineage.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_recent_games_boundary.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def measure() -> dict:
    con = duckdb.connect()
    path = _q(SOURCE / "**/*.parquet")
    row = con.execute(f"""SELECT
        COUNT(*) n,
        COUNT(DISTINCT nflcom_slug) players,
        SUM(CASE WHEN season IS NULL OR TRIM(season)='' THEN 1 ELSE 0 END) season_blank,
        SUM(CASE WHEN team IS NULL OR TRIM(team)='' THEN 1 ELSE 0 END) team_blank,
        SUM(CASE WHEN TRY_CAST(wk AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) week_numeric,
        SUM(CASE WHEN TRIM(COALESCE(opp,''))<>'' THEN 1 ELSE 0 END) opponent_present,
        SUM(CASE WHEN regexp_matches(TRIM(COALESCE(result,'')), '^[WL]') THEN 1 ELSE 0 END) result_present,
        SUM(CASE WHEN TRY_CAST(result AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) result_numeric
      FROM read_parquet('{path}', union_by_name=true) WHERE _table='Recent Games'""").fetchone()
    keys = ("rows", "players", "season_blank", "team_blank", "week_numeric", "opponent_present", "result_present", "result_numeric")
    values = {key: int(value or 0) for key, value in zip(keys, row)}
    return {
        "measurement_only": True,
        "source_table": "Recent Games",
        "status": "PENDING_CAPTURE_REPAIR",
        "reason": "physical rows lack season/team and position-block discriminator; numeric fields are not safely addressable at weekly or career grain",
        "physical_table_values": ["Recent Games"],
        "fields_checked": values,
        "required_repair": "reparse retained HTML or re-harvest while preserving block id and season/year key",
        "no_mapping_claim": True,
        "no_state_change": True,
    }


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
