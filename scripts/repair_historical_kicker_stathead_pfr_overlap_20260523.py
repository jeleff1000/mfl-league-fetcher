"""Remove duplicate historical kicker rows from Stathead/PFR overlap.

The research season/career fast-path tables aggregate from
___ops.nfl_historical.nfl_player_stats_all. Historical kicker rows from
roughly 1963-1977 can appear twice for the same canonical
NFL_player_id/year/week: once from stathead_excel and once from
pfr_boxscore_weekly_insert_stage. That inflates games_played and season
totals for kickers like David Ray and Jan Stenerud.

This script targets only the unambiguous overlap:

* regular-season kicker rows
* data_source = 'stathead_excel'
* same NFL_player_id, year, and week exists from pfr_boxscore_weekly_insert_stage

Stathead-only weeks are preserved. Dry-run is the default.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402

SUPER_TABLE = "___ops.nfl_historical.nfl_player_stats_all"
STAGE_TABLE = "___ops.nfl_historical.kicker_stathead_pfr_overlap_stage_20260523"
BACKUP_TABLE = "___ops.nfl_historical.kicker_stathead_pfr_overlap_backup_20260523"


class LongFlyWriter(FlyWriter):
    """Fly writer with a longer timeout for aggregate rebuilds."""

    def __init__(self) -> None:
        super().__init__()
        self.TIMEOUT_SECONDS = int(os.environ.get("FLY_QUERY_TIMEOUT_SECONDS", "600"))


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def target_rows_sql() -> str:
    return f"""
WITH target AS (
  SELECT DISTINCT
    CAST(s.year AS INTEGER) AS year,
    CAST(s.week AS INTEGER) AS week,
    s.player_week,
    s.NFL_player_id,
    s.player,
    s.nfl_team,
    s.opponent_nfl_team,
    s.data_source,
    p.player_week AS kept_player_week,
    p.data_source AS kept_data_source,
    ROUND(COALESCE(s.fpts_4pt_half, 0), 4) AS stathead_fpts_4pt_half,
    ROUND(COALESCE(p.fpts_4pt_half, 0), 4) AS pfr_fpts_4pt_half,
    COALESCE(s.fg_made, 0) AS stathead_fg_made,
    COALESCE(p.fg_made, 0) AS pfr_fg_made,
    COALESCE(s.fg_att, 0) AS stathead_fg_att,
    COALESCE(p.fg_att, 0) AS pfr_fg_att,
    COALESCE(s.pat_made, 0) AS stathead_pat_made,
    COALESCE(p.pat_made, 0) AS pfr_pat_made,
    COALESCE(s.pat_att, 0) AS stathead_pat_att,
    COALESCE(p.pat_att, 0) AS pfr_pat_att
  FROM {SUPER_TABLE} AS s
  JOIN {SUPER_TABLE} AS p
    ON p.NFL_player_id = s.NFL_player_id
   AND CAST(p.year AS INTEGER) = CAST(s.year AS INTEGER)
   AND CAST(p.week AS INTEGER) = CAST(s.week AS INTEGER)
   AND COALESCE(p.season_type, '') = 'REG'
   AND COALESCE(p.data_source, '') = 'pfr_boxscore_weekly_insert_stage'
  WHERE s.NFL_player_id IS NOT NULL
    AND s.year IS NOT NULL
    AND s.week IS NOT NULL
    AND COALESCE(s.season_type, '') = 'REG'
    AND COALESCE(s.data_source, '') = 'stathead_excel'
    AND COALESCE(s.nfl_position, s.position) = 'K'
)
SELECT *
FROM target
"""


def remaining_overlap_sql() -> str:
    return f"""
WITH d AS (
  SELECT
    NFL_player_id,
    CAST(year AS INTEGER) AS year,
    CAST(week AS INTEGER) AS week,
    MAX(CASE WHEN COALESCE(data_source, '') = 'stathead_excel' THEN 1 ELSE 0 END) AS has_stathead_excel,
    MAX(CASE WHEN COALESCE(data_source, '') = 'pfr_boxscore_weekly_insert_stage' THEN 1 ELSE 0 END) AS has_pfr_stage
  FROM {SUPER_TABLE}
  WHERE NFL_player_id IS NOT NULL
    AND year IS NOT NULL
    AND week IS NOT NULL
    AND COALESCE(season_type, '') = 'REG'
    AND COALESCE(nfl_position, position) = 'K'
  GROUP BY NFL_player_id, CAST(year AS INTEGER), CAST(week AS INTEGER)
)
SELECT COUNT(*) AS n
FROM d
WHERE has_stathead_excel = 1
  AND has_pfr_stage = 1
"""


def fetch_count(writer: FlyWriter, sql: str) -> int:
    rows = writer.execute(sql, database="___ops")
    if not rows:
        return 0
    return int(next(iter(rows[0].values())) or 0)


def print_summary(writer: FlyWriter) -> None:
    rows = writer.execute(
        f"""
        SELECT
          COUNT(*) AS rows_to_delete,
          COUNT(DISTINCT NFL_player_id || '_' || CAST(year AS VARCHAR)) AS affected_player_seasons,
          MIN(year) AS first_year,
          MAX(year) AS last_year,
          SUM(CASE
            WHEN stathead_fg_made != pfr_fg_made
              OR stathead_fg_att != pfr_fg_att
              OR stathead_pat_made != pfr_pat_made
              OR stathead_pat_att != pfr_pat_att
            THEN 1 ELSE 0
          END) AS core_stat_conflicts
        FROM ({target_rows_sql()}) d
        """,
        database="___ops",
    )
    row = rows[0] if rows else {}
    print(
        "Detected {rows_to_delete:,} Stathead/PFR duplicate kicker rows "
        "across {affected_player_seasons:,} player-seasons "
        "({first_year}-{last_year}); core-stat conflicts={core_stat_conflicts:,}.".format(
            rows_to_delete=int(row.get("rows_to_delete") or 0),
            affected_player_seasons=int(row.get("affected_player_seasons") or 0),
            first_year=row.get("first_year") or "n/a",
            last_year=row.get("last_year") or "n/a",
            core_stat_conflicts=int(row.get("core_stat_conflicts") or 0),
        )
    )


def print_sample(writer: FlyWriter) -> None:
    rows = writer.execute(
        f"""
        SELECT *
        FROM ({target_rows_sql()}) d
        ORDER BY year, week, player
        LIMIT 16
        """,
        database="___ops",
    )
    for row in rows:
        print(
            "  {player} {year} W{week}: delete={player_week} keep={kept_player_week} "
            "fg={stathead_fg_made:g}/{stathead_fg_att:g}->{pfr_fg_made:g}/{pfr_fg_att:g} "
            "xp={stathead_pat_made:g}/{stathead_pat_att:g}->{pfr_pat_made:g}/{pfr_pat_att:g}".format(**row)
        )


def stage_targets(writer: FlyWriter) -> None:
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {STAGE_TABLE} AS
        SELECT *
        FROM ({target_rows_sql()}) d
        """,
        database="___ops",
    )


def backup_targets(writer: FlyWriter) -> int:
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {BACKUP_TABLE} AS
        SELECT s.*
        FROM {SUPER_TABLE} AS s
        JOIN {STAGE_TABLE} AS d
          ON CAST(s.year AS INTEGER) = d.year
         AND CAST(s.week AS INTEGER) = d.week
         AND s.NFL_player_id = d.NFL_player_id
         AND COALESCE(s.player_week, '') = COALESCE(d.player_week, '')
         AND COALESCE(s.data_source, '') = COALESCE(d.data_source, '')
        """,
        database="___ops",
    )
    return fetch_count(writer, f"SELECT COUNT(*) AS n FROM {BACKUP_TABLE}")


def delete_targets(writer: FlyWriter) -> None:
    writer.execute(
        f"""
        DELETE FROM {SUPER_TABLE} AS s
        USING {STAGE_TABLE} AS d
        WHERE CAST(s.year AS INTEGER) = d.year
          AND CAST(s.week AS INTEGER) = d.week
          AND s.NFL_player_id = d.NFL_player_id
          AND COALESCE(s.player_week, '') = COALESCE(d.player_week, '')
          AND COALESCE(s.data_source, '') = COALESCE(d.data_source, '')
        """,
        database="___ops",
    )


def rebuild_aggregates() -> None:
    from multi_league.data_fetchers.aggregate_nfl_stats_fly import update_aggregates

    print("\nRebuilding derived NFL season/career aggregate caches...")
    update_aggregates(year=None, rebuild_all_years=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Backup and delete the staged duplicate rows.")
    parser.add_argument(
        "--rebuild-aggregates",
        action="store_true",
        help="After --apply, rebuild derived player_nfl season/career aggregate caches.",
    )
    args = parser.parse_args()

    load_env()
    writer = LongFlyWriter()

    print_summary(writer)
    print_sample(writer)
    before = fetch_count(writer, f"SELECT COUNT(*) AS n FROM ({target_rows_sql()}) d")
    overlap_before = fetch_count(writer, remaining_overlap_sql())
    print(f"Remaining Stathead/PFR kicker overlap groups before delete: {overlap_before:,}")

    if not args.apply:
        print("\nDRY RUN only. Re-run with --apply to stage, backup, and delete these rows.")
        return 0
    if before == 0:
        print("\nNo target rows remain. Nothing to delete.")
        return 0

    print(f"\nStaging targets -> {STAGE_TABLE}")
    stage_targets(writer)
    staged = fetch_count(writer, f"SELECT COUNT(*) AS n FROM {STAGE_TABLE}")
    if staged != before:
        raise RuntimeError(f"Stage count mismatch: expected {before}, staged {staged}")

    print(f"Backing up targets -> {BACKUP_TABLE}")
    backed_up = backup_targets(writer)
    if backed_up != before:
        raise RuntimeError(f"Backup count mismatch: expected {before}, backed up {backed_up}")

    print("Deleting staged duplicate rows")
    delete_targets(writer)

    after = fetch_count(writer, f"SELECT COUNT(*) AS n FROM ({target_rows_sql()}) d")
    overlap_after = fetch_count(writer, remaining_overlap_sql())
    print(f"Remaining target rows: {after:,}")
    print(f"Remaining Stathead/PFR kicker overlap groups: {overlap_after:,}")
    if after != 0 or overlap_after != 0:
        raise RuntimeError("Expected zero remaining Stathead/PFR kicker overlap rows")

    print(f"\nDeleted {before:,} rows; backup retained at {BACKUP_TABLE}.")
    if args.rebuild_aggregates:
        rebuild_aggregates()
    else:
        print("Aggregate caches were not rebuilt. Use --rebuild-aggregates after review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
