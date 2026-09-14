"""Turn an audited target classification into a safe outcome sidecar.

Only rows classified as ``recovered`` are writable outcome facts.  Rows with
no opponent or source discrepancies remain in the audit ledger and are never
converted into fabricated wins/losses.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def build(input_path: Path, output_path: Path, expected_recovered: int = 6789) -> dict[str, int]:
    con = duckdb.connect(str(output_path))
    try:
        con.execute("create schema public")
        con.execute(
            """
            create table public.outcome_targets as
            select cast(db_name as varchar) as db_name,
                   cast(year as integer) as year,
                   cast(week as integer) as week,
                   cast(NFL_player_id as varchar) as NFL_player_id,
                   cast(win as integer) as win,
                   cast(loss as integer) as loss,
                   cast(tie as integer) as tie,
                   cast(source_team_points as double) as team_points
            from read_parquet(?)
            where status = 'recovered'
            """,
            [str(input_path)],
        )
        status_rows = con.execute(
            "select status, count(*) from read_parquet(?) group by status order by status",
            [str(input_path)],
        ).fetchall()
        recovered = con.execute("select count(*) from public.outcome_targets").fetchone()[0]
        null_values = con.execute(
            "select count(*) from public.outcome_targets where win is null or team_points is null"
        ).fetchone()[0]
        if recovered != expected_recovered:
            raise ValueError(f"expected {expected_recovered:,} recovered rows, got {recovered}")
        if null_values:
            raise ValueError(f"recovered sidecar contains {null_values} null outcome rows")
        print({"status_counts": dict(status_rows), "recovered_rows": recovered})
        return {str(status): int(count) for status, count in status_rows}
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--expected-recovered", type=int, default=6789)
    args = ap.parse_args()
    build(args.input, args.output, args.expected_recovered)


if __name__ == "__main__":
    main()
