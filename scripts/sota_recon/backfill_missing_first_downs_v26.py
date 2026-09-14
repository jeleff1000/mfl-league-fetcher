"""Fill missing player first-down cells from the raw PBP witness.

The shipped weekly rollup has no first-down columns, while the raw merged PBP does.  The
2025 release therefore has NULL player first-down cells in late weeks even though the
source evidence is present.  This is a fill-only, fail-closed repair: it never overwrites
an existing value and writes a timestamped backup before swapping the repaired release.

Run dry first, then apply:

    python -m scripts.sota_recon.backfill_missing_first_downs_v26
    python -m scripts.sota_recon.backfill_missing_first_downs_v26 --apply
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26

PBP = Path(
    r"D:/league-history-data/nfl/raw/stathead/generated/"
    r"pbp_merged_1978_2025/nfl_pbp_1978_2025_merged.parquet"
)
BIO = Path(r"D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")
TARGET = ("passing_first_downs", "receiving_first_downs", "rushing_first_downs")


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def build_rollup(con: duckdb.DuckDBPyConnection) -> None:
    """Materialize PBP first-down totals at the player-week grain."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE first_down_rollup AS
        WITH bio AS (
            SELECT pfr_id, ANY_VALUE(NFL_player_id) AS nfl_id
            FROM read_parquet('{_sql_path(BIO)}')
            WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL
            GROUP BY pfr_id
        ), raw AS (
            SELECT
                COALESCE(b.nfl_id, regexp_replace(CAST(passer_player_id AS VARCHAR), '^pfr:', '')) AS passer_id,
                COALESCE(b2.nfl_id, regexp_replace(CAST(receiver_player_id AS VARCHAR), '^pfr:', '')) AS receiver_id,
                COALESCE(b3.nfl_id, regexp_replace(CAST(rusher_player_id AS VARCHAR), '^pfr:', '')) AS rusher_id,
                CAST(season AS INTEGER) AS year,
                CAST(week AS INTEGER) AS week,
                CAST(season_type AS VARCHAR) AS season_type,
                TRY_CAST(first_down_pass AS INTEGER) AS first_down_pass,
                TRY_CAST(first_down_rush AS INTEGER) AS first_down_rush
            FROM read_parquet('{_sql_path(PBP)}') p
            LEFT JOIN bio b ON b.pfr_id = regexp_replace(CAST(p.passer_player_id AS VARCHAR), '^pfr:', '')
            LEFT JOIN bio b2 ON b2.pfr_id = regexp_replace(CAST(p.receiver_player_id AS VARCHAR), '^pfr:', '')
            LEFT JOIN bio b3 ON b3.pfr_id = regexp_replace(CAST(p.rusher_player_id AS VARCHAR), '^pfr:', '')
            WHERE season IS NOT NULL AND week IS NOT NULL
        ), rows AS (
            SELECT passer_id AS NFL_player_id, year, week, season_type,
                   SUM(CASE WHEN first_down_pass = 1 THEN 1 ELSE 0 END) AS passing_first_downs,
                   0::DOUBLE AS receiving_first_downs,
                   0::DOUBLE AS rushing_first_downs
            FROM raw WHERE passer_id IS NOT NULL AND passer_id <> ''
            GROUP BY 1, 2, 3, 4
            UNION ALL
            SELECT receiver_id, year, week, season_type, 0::DOUBLE,
                   SUM(CASE WHEN first_down_pass = 1 THEN 1 ELSE 0 END), 0::DOUBLE
            FROM raw WHERE receiver_id IS NOT NULL AND receiver_id <> ''
            GROUP BY 1, 2, 3, 4
            UNION ALL
            SELECT rusher_id, year, week, season_type, 0::DOUBLE, 0::DOUBLE,
                   SUM(CASE WHEN first_down_rush = 1 THEN 1 ELSE 0 END)
            FROM raw WHERE rusher_id IS NOT NULL AND rusher_id <> ''
            GROUP BY 1, 2, 3, 4
        )
        SELECT NFL_player_id, year, week, season_type,
               SUM(passing_first_downs) AS passing_first_downs,
               SUM(receiving_first_downs) AS receiving_first_downs,
               SUM(rushing_first_downs) AS rushing_first_downs
        FROM rows GROUP BY 1, 2, 3, 4
        """
    )


def inspect() -> dict:
    con = duckdb.connect()
    try:
        build_rollup(con)
        target = _sql_path(Path(latest_v26()))
        missing = con.execute(
            f"""SELECT COUNT(*) FROM read_parquet('{target}') v
                JOIN first_down_rollup r USING (NFL_player_id, year, week, season_type)
                WHERE v.passing_first_downs IS NULL"""
        ).fetchone()[0]
        stafford = con.execute(
            f"""SELECT SUM(passing_first_downs) FROM first_down_rollup
                WHERE NFL_player_id='00-0026498' AND year=2025 AND season_type='REG'"""
        ).fetchone()[0]
        return {"missing_passing_cells": int(missing), "stafford_2025_passing_first_downs": int(stafford)}
    finally:
        con.close()


def apply() -> dict:
    source = Path(latest_v26())
    stamp = source.parent / f"{source.stem}_pre_first_down_backfill.parquet"
    repaired = source.with_name(f"{source.stem}_first_down_repaired.parquet")
    con = duckdb.connect()
    try:
        build_rollup(con)
        columns = [row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{_sql_path(source)}')"
        ).fetchall()]
        select_columns = []
        for column in columns:
            quoted = '"' + column.replace('"', '""') + '"'
            if column in TARGET:
                select_columns.append(
                    f"CASE WHEN st.{quoted} IS NULL THEN r.{quoted} ELSE st.{quoted} END AS {quoted}"
                )
            else:
                select_columns.append(f"st.{quoted}")
        con.execute(
            f"""COPY (
                SELECT {', '.join(select_columns)}
                FROM read_parquet('{_sql_path(source)}') st
                LEFT JOIN first_down_rollup r
                  ON st.NFL_player_id=r.NFL_player_id AND st.year=r.year
                 AND st.week=r.week AND st.season_type=r.season_type
            ) TO '{_sql_path(repaired)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
    finally:
        con.close()
    shutil.copy2(source, stamp)
    repaired.replace(source)
    return {"backup": str(stamp), "target": str(source)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.apply:
        print(inspect())
        return 0
    print(apply())
    print(inspect())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
