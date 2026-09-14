"""Apply the combined outcome sidecar without replaying matchup rescues."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def validate_outcome_payload(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, object]:
    """Reject target inventories that contain no recovered outcome values."""
    relation = ".".join(q(part) for part in table.split("."))
    described = con.execute(f"describe {relation}").fetchall()
    available = {row[0] for row in described}
    value_columns = [c for c in ("win", "loss", "tie", "team_points", "opponent_points") if c in available]
    if not value_columns:
        raise ValueError("outcome payload has no recognized outcome columns")
    rows = con.execute(f"select count(*) from {relation}").fetchone()[0]
    populated = " OR ".join(f"{q(c)} IS NOT NULL" for c in value_columns)
    populated_rows = con.execute(
        f"select count(*) from {relation} where {populated}"
    ).fetchone()[0]
    if rows and not populated_rows:
        raise ValueError(
            f"outcome payload has no populated outcome values ({rows:,} target rows); "
            "this is an inventory manifest, not a recovery sidecar"
        )
    return {"rows": rows, "populated_rows": populated_rows, "value_columns": value_columns}


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def tables(con: duckdb.DuckDBPyConnection, db: str) -> list[str]:
    return [r[0] for r in con.execute(
        "select table_name from duckdb_tables() where database_name=? and schema_name='public'", [db]
    ).fetchall()]


def columns(con: duckdb.DuckDBPyConnection, db: str, table: str) -> list[str]:
    return [r[0] for r in con.execute(f"describe {db}.public.{q(table)}").fetchall()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--outcomes", type=Path, required=True)
    args = ap.parse_args()
    con = duckdb.connect(str(args.out))
    con.execute(f"attach '{args.base.resolve().as_posix().replace(chr(39), chr(39)*2)}' as base (read_only)")
    con.execute(f"attach '{args.outcomes.resolve().as_posix().replace(chr(39), chr(39)*2)}' as outcomes (read_only)")
    con.execute("create schema public")
    base_tables = set(tables(con, "base"))
    if "player_fantasy" not in base_tables:
        raise SystemExit("base lake has no player_fantasy table")

    for table in sorted(base_tables - {"player_fantasy"}):
        con.execute(f"create table public.{q(table)} as select * from base.public.{q(table)}")

    bcols = columns(con, "base", "player_fantasy")
    ocols = columns(con, "outcomes", "outcome_targets")
    payload_stats = validate_outcome_payload(con, "outcomes.public.outcome_targets")
    keys = {"db_name", "year", "week", "NFL_player_id"}
    if not keys <= set(ocols):
        raise SystemExit(f"outcome sidecar missing keys: {sorted(keys - set(ocols))}")
    value_cols = [c for c in ("win", "loss", "tie", "team_points", "opponent_points") if c in bcols and c in ocols]
    if not value_cols:
        raise SystemExit("outcome sidecar has no overlapping outcome columns")
    aggregates = ", ".join(f"max(try_cast({q(c)} as integer)) as {q(c)}" if c in {"win", "loss", "tie"}
                            else f"max({q(c)}) as {q(c)}" for c in value_cols)
    con.execute(f"""
      create or replace temp table _outcome_overlay as
      select cast(db_name as varchar) db_name, cast(year as integer) season_year,
             cast(week as integer) season_week, cast(NFL_player_id as varchar) NFL_player_id,
             {aggregates}
      from outcomes.public.outcome_targets
      group by 1,2,3,4
    """)
    select_expr = []
    for c in bcols:
        if c in value_cols:
            select_expr.append(f"coalesce(o.{q(c)}, b.{q(c)}) as {q(c)}")
        else:
            select_expr.append(f"b.{q(c)} as {q(c)}")
    con.execute(f"""
      create table public.player_fantasy as
      select {', '.join(select_expr)}
      from base.public.player_fantasy b
      left join _outcome_overlay o
        on o.db_name=cast(b.db_name as varchar)
       and o.season_year=cast(b.year as integer)
       and o.season_week=cast(b.week as integer)
       and o.NFL_player_id=cast(b.NFL_player_id as varchar)
    """)
    print({
        "outcome_targets": con.execute("select count(*) from _outcome_overlay").fetchone()[0],
        "player_fantasy_rows": con.execute("select count(*) from public.player_fantasy").fetchone()[0],
        "value_columns": value_cols,
        "payload": payload_stats,
    })
    con.close()


if __name__ == "__main__":
    main()
