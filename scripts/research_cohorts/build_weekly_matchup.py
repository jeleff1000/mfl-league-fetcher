"""Validate and load the canonical research matchup weekly artifact.

The canonical matchup builder emits both weekly and season parquets from one
weekly primitive path. This step deliberately performs no lake/Fly reads and
exists to stop the cycle if the two grains ever diverge.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from matchup_parity import validate_weekly_season_parity


OUT_DIR = Path(os.environ.get(
    "RESEARCH_OUT_DIR",
    "D:/league-history-data/fantasy_leagues/cohort_aggregates",
))
WEEKLY = OUT_DIR / "research_matchup_weekly.parquet"
SEASON = OUT_DIR / "research_matchup_graded.parquet"
DB = OUT_DIR / "research_cohorts.duckdb"


def main() -> None:
    missing = [path for path in (WEEKLY, SEASON) if not path.exists()]
    if missing:
        names = ", ".join(str(path) for path in missing)
        raise SystemExit(
            f"missing canonical matchup artifact(s): {names}; "
            "run the matchup and grades steps first"
        )

    errors = validate_weekly_season_parity(WEEKLY, SEASON, tolerance=0.0500001)
    if errors:
        preview = "\n".join(
            f"  {e.NFL_player_id} {e.year} {e.teams}/{e.roster}/{e.ppr}/{e.td} "
            f"{e.metric}: weekly={e.weekly_value} season={e.season_value}"
            for e in errors[:20]
        )
        raise RuntimeError(
            f"weekly/season matchup parity failed ({len(errors)} fields):\n{preview}"
        )

    con = duckdb.connect(str(DB))
    try:
        con.execute(
            f"CREATE OR REPLACE TABLE matchup_weekly AS "
            f"SELECT * FROM read_parquet('{WEEKLY.as_posix()}')"
        )
        rows = con.execute("SELECT COUNT(*) FROM matchup_weekly").fetchone()[0]
    finally:
        con.close()
    print(f"[weekly-matchup] parity PASS; loaded {rows:,} local rows into {DB}")


if __name__ == "__main__":
    main()
