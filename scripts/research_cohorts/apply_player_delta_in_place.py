"""Apply exact-schema player delta rows directly to the approved corpus.

The delta is already faned out to player rows.  This intentionally does not
materialize the 269m-row base table: it joins the small delta to the existing
table on the canonical player-week identity and fills only NULL cells.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def schema(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [row[0] for row in con.execute(f"describe {relation}").fetchall()]


def key(alias: str) -> str:
    p = f"{alias}."
    return (
        "concat_ws('|', "
        f"cast({p}{q('db_name')} as varchar), "
        f"cast({p}{q('year')} as varchar), "
        f"cast({p}{q('week')} as varchar), "
        f"cast({p}{q('NFL_player_id')} as varchar), "
        f"coalesce(cast({p}{q('manager')} as varchar), '<NULL_MANAGER>'))"
    )


def apply(base: Path, sidecars: Path, pattern: str, report_path: Path) -> dict[str, object]:
    files = sorted(sidecars.rglob(pattern))
    if not files:
        raise ValueError(f"no delta files matched {pattern!r}")

    con = duckdb.connect(str(base))
    try:
        if schema(con, "public.player_fantasy") != COLUMNS:
            raise ValueError("canonical player schema is not the frozen 29-column schema")

        paths = [str(path.resolve()) for path in files]
        con.execute(
            "create or replace temp table _delta as "
            "select * from read_parquet(?, union_by_name=true)",
            [paths],
        )
        if schema(con, "_delta") != COLUMNS:
            raise ValueError("player delta schema is not the frozen 29-column schema")

        null_keys = con.execute(
            "select count(*) from _delta where db_name is null or year is null or "
            "week is null or NFL_player_id is null or manager is null"
        ).fetchone()[0]
        if null_keys:
            raise ValueError(f"delta rows missing canonical player identity: {null_keys}")

        con.execute(f"create or replace temp table _delta_keyed as select *, {key('d')} as join_key from _delta d")
        duplicate_delta = con.execute(
            "select count(*) - count(distinct join_key) from _delta_keyed"
        ).fetchone()[0]
        if duplicate_delta:
            raise ValueError(f"duplicate delta player keys: {duplicate_delta}")

        unmatched = con.execute(
            f"select count(*) from _delta_keyed d left join public.player_fantasy b "
            f"on {key('d')}={key('b')} where b.db_name is null"
        ).fetchone()[0]
        if unmatched:
            raise ValueError(f"delta rows do not match existing canonical player rows: {unmatched}")

        duplicate_base = con.execute(
            f"select count(*) from (select {key('b')} as join_key, count(*) as n "
            f"from public.player_fantasy b group by 1 having count(*) > 1)"
        ).fetchone()[0]

        conflict_by_field: dict[str, int] = {}
        improvement_by_field: dict[str, int] = {}
        for column in COLUMNS:
            s = f"d.{q(column)}"
            b = f"b.{q(column)}"
            conflict_by_field[column] = con.execute(
                f"select count(*) from _delta_keyed d join public.player_fantasy b "
                f"on {key('d')}={key('b')} where {s} is not null and {b} is not null "
                f"and {s} is distinct from {b}"
            ).fetchone()[0]
            improvement_by_field[column] = con.execute(
                f"select count(*) from _delta_keyed d join public.player_fantasy b "
                f"on {key('d')}={key('b')} where {b} is null and {s} is not null"
            ).fetchone()[0]
        conflict_total = sum(conflict_by_field.values())
        if conflict_total:
            raise ValueError(f"delta conflicts with confirmed canonical values: {conflict_total}")

        before_rows = con.execute("select count(*) from public.player_fantasy").fetchone()[0]
        con.execute("begin transaction")
        assignments = ", ".join(
            f"{q(column)} = case when b.{q(column)} is null then d.{q(column)} else b.{q(column)} end"
            for column in COLUMNS
        )
        con.execute(
            f"update public.player_fantasy as b set {assignments} from _delta_keyed as d "
            f"where {key('d')}={key('b')}"
        )
        after_rows = con.execute("select count(*) from public.player_fantasy").fetchone()[0]
        if after_rows != before_rows or schema(con, "public.player_fantasy") != COLUMNS:
            con.execute("rollback")
            raise ValueError("player row count or schema changed during delta apply")
        con.execute("commit")

        report = {
            "sidecar_files": len(files),
            "source_rows": int(con.execute("select count(*) from _delta").fetchone()[0]),
            "unmatched_rows": int(unmatched),
            "duplicate_delta_keys": int(duplicate_delta),
            "duplicate_base_keys": int(duplicate_base),
            "base_fanout_allowed": True,
            "conflict_rows": int(conflict_total),
            "improvements_by_field": {k: int(v) for k, v in improvement_by_field.items()},
            "player_rows_before": int(before_rows),
            "player_rows_after": int(after_rows),
            "schema_unchanged": True,
            "ops_untouched": True,
            "new_lineage": False,
            "join_key": ["db_name", "year", "week", "NFL_player_id", "manager"],
        }
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        return report
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sidecars", type=Path, required=True)
    parser.add_argument("--pattern", default="promotable_delta.parquet")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    apply(args.base, args.sidecars, args.pattern, args.report)


if __name__ == "__main__":
    main()
