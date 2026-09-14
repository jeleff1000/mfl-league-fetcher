#!/usr/bin/env python3
"""
Integrate validated advanced PBP atoms + composites onto the LOCAL v26 super table.

New player-week columns (each either validated 100% vs nflverse, or by_construction from
already-validated components):

  success   : pass_success, pass_success_plays, rush_success, rush_success_plays,
              rec_success, rec_success_plays
  wpa       : passing_wpa, rushing_wpa, receiving_wpa
  explosive : pass_explosive_20, rush_explosive_10, rec_explosive_20
  red-zone  : rz_pass_att, rz_pass_td, rz_carries, rz_rush_td, rz_targets, rz_rec_td
  composite : total_epa, total_wpa, scrimmage_yards, total_tds, total_touches

Join grain is (NFL_player_id, year, week) with continuous week numbering (playoffs 19-22),
which is UNIQUE in both the super table and the PBP weekly rollup; the rollup is still
SUM-aggregated to that grain defensively. EPA / air_yards / cpoe / 2pt already live in the
super table (validated) and are left untouched. Unmatched rows (no offensive PBP events)
get NULL, not 0 — absence of data is not zero.

    # validate the join on one year first (no write):
    python scripts/integrate_advanced_atoms_into_supertable.py --sample-year 2023
    # then write the augmented release:
    python scripts/integrate_advanced_atoms_into_supertable.py --write
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts.sota_recon.sources import latest_v26  # noqa: E402

DATA_LAKE = Path(r"D:\league-history-data\nfl")
DEFAULT_ROLLUP = DATA_LAKE / "raw" / "stathead" / "generated" / "pbp_weekly_rollup_1978_2025" / "pbp_player_week_rollup.parquet"

# Atoms pulled straight from the rollup (NULL where no PBP match). EPA/air_yards/cpoe/2pt
# are NOT here — they already exist and validated in the super table.
NEW_ATOMS = [
    "pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
    "rec_success", "rec_success_plays",
    "passing_wpa", "rushing_wpa", "receiving_wpa",
    "pass_explosive_20", "rush_explosive_10", "rec_explosive_20",
    "rz_pass_att", "rz_pass_td", "rz_carries", "rz_rush_td", "rz_targets", "rz_rec_td",
]

# EPA/WP-model atoms are only publicly validated for 1999+ (nflverse's win-prob/EP models
# start then). Pre-1999 the rollup emits a structural 0.0 (multi-role pivot zeros can't be
# masked at SUM time), which reads as a real "neutral" value. So the overlay masks these to
# NULL for pre-EP-era years — matching how the super table already keeps EPA NULL pre-1999.
# Model-free atoms (explosive by yards, red-zone by yardline) are NOT gated; they extend to 1978.
EP_ERA_START = 1999
EP_ERA_GATED = {
    "passing_wpa", "rushing_wpa", "receiving_wpa",
    "pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
    "rec_success", "rec_success_plays",
}

# name -> component columns; NULL only when every component is NULL, else sum of coalesced.
# s.* = super-table columns; r.* = joined rollup atoms (WPA already EP-era-gated in rollup_agg).
COMPOSITES = {
    "total_epa": ["s.passing_epa", "s.rushing_epa", "s.receiving_epa"],
    "total_wpa": ["r.passing_wpa", "r.rushing_wpa", "r.receiving_wpa"],
    "scrimmage_yards": ["s.rushing_yards", "s.receiving_yards"],
    "total_tds": ["s.passing_tds", "s.rushing_tds", "s.receiving_tds"],
    "total_touches": ["s.carries", "s.receptions"],
}


def composite_expr(parts: list[str]) -> str:
    all_null = " AND ".join(f"{p} IS NULL" for p in parts)
    summed = " + ".join(f"COALESCE({p}, 0)" for p in parts)
    return f"CASE WHEN {all_null} THEN NULL ELSE {summed} END"


def rollup_agg_sql(rollup: Path) -> str:
    # EP-era-gated atoms become NULL before the EP era; year is a grouping column so the
    # gate is valid inside the aggregate. All other atoms are plain SUMs.
    parts = []
    for a in NEW_ATOMS:
        if a in EP_ERA_GATED:
            parts.append(f"CASE WHEN CAST(year AS INTEGER) >= {EP_ERA_START} THEN SUM({a}) ELSE NULL END AS {a}")
        else:
            parts.append(f"SUM({a}) AS {a}")
    return f"""
        SELECT NFL_player_id,
               CAST(year AS INTEGER) AS year,
               CAST(week AS INTEGER) AS week,
               {", ".join(parts)}
        FROM read_parquet('{rollup.as_posix()}')
        WHERE NFL_player_id IS NOT NULL
        GROUP BY 1, 2, 3
    """


def select_sql(source: str, year_filter: int | None = None, exclude_cols: list[str] | None = None) -> str:
    atom_cols = ",\n            ".join(f"r.{a}" for a in NEW_ATOMS)
    comp_cols = ",\n            ".join(f"{composite_expr(p)} AS {name}" for name, p in COMPOSITES.items())
    where = f"WHERE CAST(s.year AS INTEGER) = {year_filter}" if year_filter is not None else ""
    # Idempotency: when re-adding onto a table that already has these columns, drop the old
    # copies from s.* so they're re-derived cleanly (no duplicate-column binder error).
    star = "s.*"
    if exclude_cols:
        star = f"s.* EXCLUDE ({', '.join(exclude_cols)})"
    # `source` may be a parquet path or a bound relation name (view/table).
    src = f"read_parquet('{source}')" if source.endswith(".parquet") else source
    return f"""
        SELECT
            {star},
            {atom_cols},
            {comp_cols}
        FROM {src} s
        LEFT JOIN rollup_agg r
          ON r.NFL_player_id = s.NFL_player_id
         AND r.year = CAST(s.year AS INTEGER)
         AND r.week = CAST(s.week AS INTEGER)
        {where}
    """


def connect(temp_dir: Path | None, mem: str, threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if temp_dir:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"PRAGMA threads={max(1, threads)}")
    return con


def sample_year(con: duckdb.DuckDBPyConnection, source: str, rollup: Path, year: int) -> None:
    con.execute(f"CREATE TEMP TABLE rollup_agg AS {rollup_agg_sql(rollup)}")
    con.execute(f"CREATE TEMP TABLE aug AS SELECT * FROM ({select_sql(source)}) WHERE year = {year}")

    total = con.execute("SELECT COUNT(*) FROM aug").fetchone()[0]
    off = con.execute(
        "SELECT COUNT(*) FROM aug WHERE COALESCE(attempts,0)+COALESCE(carries,0)+COALESCE(targets,0) > 0"
    ).fetchone()[0]
    off_matched = con.execute(
        "SELECT COUNT(*) FROM aug WHERE COALESCE(attempts,0)+COALESCE(carries,0)+COALESCE(targets,0) > 0 "
        "AND (passing_wpa IS NOT NULL OR rushing_wpa IS NOT NULL OR receiving_wpa IS NOT NULL "
        "OR pass_explosive_20 IS NOT NULL OR rush_explosive_10 IS NOT NULL OR rec_explosive_20 IS NOT NULL)"
    ).fetchone()[0]
    print(f"\n=== {year} join check ===")
    print(f"  super rows: {total:,}   offensive rows (att/car/tgt>0): {off:,}")
    print(f"  offensive rows with >=1 new atom populated: {off_matched:,}  ({off_matched/off*100:.2f}% of offensive)")

    print(f"\n  passing_wpa leaders {year} (should be elite QBs):")
    for r in con.execute(
        "SELECT player, position, ROUND(SUM(passing_wpa),2) wpa, ROUND(SUM(passing_epa),1) epa "
        "FROM aug GROUP BY 1,2 HAVING SUM(passing_wpa) IS NOT NULL ORDER BY wpa DESC LIMIT 5"
    ).fetchall():
        print(f"    {r[0]:<24} {str(r[1]):<4} wpa={r[2]:>6}  epa={r[3]}")

    print(f"\n  composite spot-check (total_epa == sum of 3 EPA components):")
    bad = con.execute(
        "SELECT COUNT(*) FROM aug WHERE total_epa IS NOT NULL AND "
        "ABS(total_epa - (COALESCE(passing_epa,0)+COALESCE(rushing_epa,0)+COALESCE(receiving_epa,0))) > 1e-6"
    ).fetchone()[0]
    print(f"    total_epa mismatches: {bad}  (must be 0)")
    print(f"  rz_carries leaders {year} (should be goal-line backs):")
    for r in con.execute(
        "SELECT player, ROUND(SUM(rz_carries),0) rzc, ROUND(SUM(rz_rush_td),0) rztd "
        "FROM aug WHERE position='RB' GROUP BY 1 HAVING SUM(rz_carries) IS NOT NULL ORDER BY rzc DESC LIMIT 5"
    ).fetchall():
        print(f"    {r[0]:<24} rz_carries={r[1]:>5}  rz_rush_td={r[2]}")


def write_release(con: duckdb.DuckDBPyConnection, source: str, rollup: Path, row_group: int) -> Path:
    # RAM-safe write in ONE pass: the source is ~1010 columns wide, so the parquet
    # writer's DEFAULT 122,880-row row group buffers ~1 GB before flushing and thrashes on
    # a low-RAM box. A small ROW_GROUP_SIZE (~20k rows -> ~160 MB) plus single-threaded
    # write keeps at most one small row group in memory; the hash-join build side is just
    # the compact rollup_agg. So one streaming COPY scans the source a single time.
    con.execute("PRAGMA threads=1")
    con.execute(f"CREATE TEMP TABLE rollup_agg AS {rollup_agg_sql(rollup)}")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    rel_dir = DATA_LAKE / "releases" / f"nfl_local_release_advstats_overlay_{stamp}_v26"
    out = rel_dir / "tables" / "nfl_player_stats_all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    print(f"streaming single-pass COPY (row_group={row_group:,}) -> {out.name} ...", flush=True)
    con.execute(
        f"COPY ({select_sql(source)}) TO '{out.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group})"
    )

    src_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{source}')").fetchone()[0]
    out_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
    assert src_rows == out_rows, f"ROW COUNT CHANGED: {src_rows} -> {out_rows} (join fan-out!)"
    ncols = len(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out.as_posix()}')").fetchall())

    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_release": source,
        "rollup_source": str(rollup),
        "rows": out_rows,
        "columns": ncols,
        "added_atoms": NEW_ATOMS,
        "added_composites": list(COMPOSITES.keys()),
        "join_grain": "(NFL_player_id, year, week)",
        "note": "Advanced-stat overlay: validated PBP atoms + composites joined onto v26 super table.",
    }
    (rel_dir / "manifest_advstats_overlay.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nwrote {out_rows:,} rows x {ncols} cols -> {out}")
    print(f"  row count preserved (no fan-out): {src_rows:,} == {out_rows:,}")
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", default=None, help="Source super table parquet (default: latest_v26)")
    p.add_argument("--rollup", type=Path, default=DEFAULT_ROLLUP)
    p.add_argument("--sample-year", type=int, default=None, help="Validate the join on one year (no write)")
    p.add_argument("--write", action="store_true", help="Write the augmented release parquet")
    p.add_argument("--temp-dir", type=Path, default=DATA_LAKE / "tmp" / "duckdb")
    p.add_argument("--memory-limit", default="1500MB")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--row-group", type=int, default=20000, help="Parquet row-group size (small = low write memory)")
    args = p.parse_args()

    source = args.source or latest_v26()
    if not args.rollup.exists():
        raise FileNotFoundError(f"Rollup not found: {args.rollup}")

    con = connect(args.temp_dir, args.memory_limit, args.threads)
    if args.sample_year:
        sample_year(con, source, args.rollup, args.sample_year)
    if args.write:
        write_release(con, source, args.rollup, args.row_group)
    if not args.sample_year and not args.write:
        print("Nothing to do: pass --sample-year YYYY and/or --write")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
