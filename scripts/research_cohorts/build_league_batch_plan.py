"""Build a deterministic, row-volume-balanced league-season batch plan.

The matchup builder's hash buckets are disjoint but can be badly unbalanced.  This
planner assigns each eligible ``(db_name, year)`` to the currently lightest batch,
using the observed player/matchup row volume as the cost.  The plan is an input
partition only; final rates still come from the additive shard assembler.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def open_planning_connection(snapshot: Path | str, corpus: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Attach only the raw tables needed to cost and partition matchup shards.

    The previous planner constructed ``LocalReader`` and rebuilt every derived ops view
    before doing this tiny census.  Planning needs no positions, bye schedules, LAMAR,
    or active-week lattice: it only needs populated league-years and player/matchup row
    counts.  Keeping this connection deliberately narrow makes planning cheap and avoids
    repeating the most expensive part of every shard run.
    """
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    paths = [(Path(snapshot), "snap")]
    if corpus and Path(corpus).exists():
        paths.append((Path(corpus), "corp"))
    for path, alias in paths:
        con.execute(f"ATTACH '{path.as_posix().replace(chr(39), chr(39) * 2)}' AS {alias} (READ_ONLY)")

    def arms(table: str, columns: tuple[str, ...]) -> str:
        selects = []
        for _, alias in paths:
            available = {row[0] for row in con.execute(f"DESCRIBE {alias}.public.{table}").fetchall()}
            if not set(columns) <= available:
                missing = sorted(set(columns) - available)
                raise RuntimeError(f"planning table {alias}.public.{table} missing {missing}")
            selects.append(
                "SELECT " + ", ".join(f'CAST("{col}" AS {"DOUBLE" if col == "fantasy_points" else "INTEGER" if col in {"year", "is_rostered"} else "VARCHAR"}) AS "{col}"' for col in columns)
                + f" FROM {alias}.public.{table}"
            )
        return " UNION ALL ".join(selects)

    con.execute("CREATE VIEW public.league_settings AS " + arms("league_settings", ("db_name", "year")))
    con.execute("CREATE VIEW public.player_fantasy AS " + arms(
        "player_fantasy", ("db_name", "year", "NFL_player_id", "fantasy_points", "is_rostered")))
    con.execute("CREATE VIEW public.matchup AS " + arms("matchup", ("db_name", "year")))
    con.execute("""CREATE TEMP TABLE _lg_has_data AS
        SELECT db_name, year,
               (COALESCE(MAX(fantasy_points), 0) <= 120
                AND COALESCE(MIN(fantasy_points), 0) >= -40) AS scoring_sane
        FROM public.player_fantasy
        GROUP BY 1, 2
        HAVING COUNT(NFL_player_id) FILTER (WHERE is_rostered = 1) >= 100
           AND COUNT(NFL_player_id) FILTER (WHERE is_rostered = 1)
               >= 0.85 * COUNT(*) FILTER (WHERE is_rostered = 1)""")
    return con


def balanced_assignments(rows: list[tuple[str, int, int]], batches: int) -> list[tuple[str, int, int, int]]:
    """Assign rows greedily by descending weight to the lightest batch."""
    if batches < 1:
        raise ValueError("batches must be positive")
    loads = [0] * batches
    out: list[tuple[str, int, int, int]] = []
    for db_name, year, weight in sorted(rows, key=lambda r: (-r[2], r[0], r[1])):
        batch = min(range(batches), key=lambda i: (loads[i], i))
        loads[batch] += int(weight)
        out.append((db_name, int(year), int(weight), batch))
    return out


def build(
    con: duckdb.DuckDBPyConnection,
    years: list[int],
    batches: int,
    output: Path,
    exclude_plans: list[Path] | None = None,
) -> dict:
    years_sql = ",".join(str(int(y)) for y in sorted(set(years)))
    rows = con.execute(f"""
        WITH eligible AS (
            SELECT ls.db_name, ls.year
            FROM public.league_settings ls
            JOIN _lg_has_data d USING (db_name, year)
            WHERE ls.year IN ({years_sql})
        ), player_rows AS (
            SELECT db_name, year, COUNT(*) AS n
            FROM public.player_fantasy
            WHERE year IN ({years_sql})
            GROUP BY 1, 2
        ), matchup_rows AS (
            SELECT db_name, year, COUNT(*) AS n
            FROM public.matchup
            WHERE year IN ({years_sql})
            GROUP BY 1, 2
        )
        SELECT e.db_name, e.year,
               CAST(GREATEST(1, COALESCE(p.n, 0) + 2 * COALESCE(m.n, 0)) AS BIGINT) AS weight
        FROM eligible e
        LEFT JOIN player_rows p USING (db_name, year)
        LEFT JOIN matchup_rows m USING (db_name, year)
        ORDER BY e.year, e.db_name
    """).fetchall()
    excluded: set[tuple[str, int]] = set()
    for plan in exclude_plans or []:
        excluded.update(
            (str(db_name), int(year))
            for db_name, year in con.execute(
                "SELECT db_name, year FROM read_parquet(?)", [str(plan)]
            ).fetchall()
        )
    if excluded:
        rows = [row for row in rows if (str(row[0]), int(row[1])) not in excluded]
        print(f"excluded {len(excluded):,} already-covered league-seasons", flush=True)
    assignments = balanced_assignments(rows, batches)
    output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "db_name": [r[0] for r in assignments],
        "year": [r[1] for r in assignments],
        "weight": [r[2] for r in assignments],
        "batch": [r[3] for r in assignments],
        "batches": [batches] * len(assignments),
    })
    pq.write_table(table, output, compression="zstd")
    matrix = [{"year": year, "batch": batch}
              for year in sorted(set(years)) for batch in range(batches)]
    summary = {
        "years": sorted(set(years)),
        "batches": batches,
        "league_years": len(assignments),
        "loads": [sum(r[2] for r in assignments if r[3] == b) for b in range(batches)],
        "matrix": matrix,
    }
    (output.parent / "matrix.json").write_text(json.dumps(matrix, separators=(",", ":")), encoding="utf-8")
    (output.parent / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", nargs="+", type=int, required=True)
    ap.add_argument("--batches", type=int, required=True)
    ap.add_argument("--snapshot", default=None, help="accepted for workflow symmetry")
    ap.add_argument(
        "--exclude-plan", action="append", type=Path, default=[],
        help="parquet plan(s) whose league-seasons are already covered and must be skipped",
    )
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    snapshot = args.snapshot or os.environ.get("RESEARCH_SNAPSHOT_PATH")
    if not snapshot:
        raise SystemExit("--snapshot or RESEARCH_SNAPSHOT_PATH is required")
    corpus = os.environ.get("RESEARCH_CORPUS_PATH")
    con = open_planning_connection(snapshot, corpus)
    summary = build(con, args.years, args.batches, args.out, args.exclude_plan)
    print(json.dumps(summary, separators=(",", ":")), flush=True)


if __name__ == "__main__":
    main()
