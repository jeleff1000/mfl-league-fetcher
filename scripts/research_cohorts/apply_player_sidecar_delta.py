"""Apply a validated exact-schema player sidecar to a lake copy.

The sidecar is a set of complete 29-column player rows.  Existing rows are
updated only with non-null, non-conflicting values; genuinely new rows are
inserted.  This script never changes the base database in place.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


CANONICAL_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def tables(con: duckdb.DuckDBPyConnection, db: str) -> list[str]:
    return [r[0] for r in con.execute(
        "select table_name from duckdb_tables() where database_name=? and schema_name='public'",
        [db],
    ).fetchall()]


def columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [r[0] for r in con.execute(f"describe {relation}").fetchall()]


def player_key(alias: str) -> str:
    p = f"{alias}."
    platform = f"lower(nullif(trim(cast({p}platform as varchar)), ''))"
    fallback = (
        f"coalesce(nullif(trim(cast({p}NFL_player_id as varchar)), ''), "
        f"nullif(trim(cast({p}sleeper_player_id as varchar)), ''), "
        f"nullif(trim(cast({p}fleaflicker_player_id as varchar)), ''), "
        f"nullif(trim(cast({p}mfl_player_id as varchar)), ''))"
    )
    pid = (
        "case "
        f"when {platform}='sleeper' then coalesce(nullif(trim(cast({p}sleeper_player_id as varchar)), ''), {fallback}) "
        f"when {platform}='fleaflicker' then coalesce(nullif(trim(cast({p}fleaflicker_player_id as varchar)), ''), {fallback}) "
        f"when {platform}='mfl' then coalesce(nullif(trim(cast({p}mfl_player_id as varchar)), ''), {fallback}) "
        f"else {fallback} end"
    )
    return (
        "concat_ws('|', "
        f"cast({p}db_name as varchar), cast({p}year as varchar), cast({p}week as varchar), "
        f"coalesce({pid}, '<NO_PLAYER_ID>'), "
        f"coalesce(cast({p}manager as varchar), '<NULL_MANAGER>'), "
        f"coalesce(cast({p}team_key as varchar), '<NULL_TEAM_KEY>'), "
        f"coalesce(cast({p}team_name as varchar), '<NULL_TEAM_NAME>'))"
    )


def apply(base: Path, sidecar_dir: Path, out: Path, pattern: str) -> dict[str, object]:
    files = sorted(sidecar_dir.rglob(pattern))
    if not files:
        raise ValueError(f"no player sidecars matched {pattern!r}")
    if out.exists():
        out.unlink()

    con = duckdb.connect(str(out))
    esc = str(base.resolve()).replace("'", "''")
    con.execute(f"attach '{esc}' as base (read_only)")
    con.execute("create schema public")
    base_tables = tables(con, "base")
    if "player_fantasy" not in base_tables:
        raise ValueError("base lake has no public.player_fantasy")
    base_cols = columns(con, "base.public.player_fantasy")
    if base_cols != CANONICAL_COLUMNS:
        raise ValueError(f"base schema mismatch: {base_cols}")

    con.execute(
        "create or replace temp table _source as "
        "select * from read_parquet(?, union_by_name=true)",
        [[str(p) for p in files]],
    )
    source_cols = columns(con, "_source")
    if source_cols != CANONICAL_COLUMNS:
        raise ValueError(f"sidecar schema mismatch: {source_cols}")
    con.execute(f"create or replace temp table _source_keyed as select *, {player_key('s')} as join_key from _source s")
    con.execute(f"create or replace temp table _base_keyed as select *, {player_key('b')} as join_key from base.public.player_fantasy b")

    source_rows = con.execute("select count(*) from _source_keyed").fetchone()[0]
    source_keys = con.execute("select count(distinct join_key) from _source_keyed").fetchone()[0]
    if source_rows != source_keys:
        raise ValueError(f"duplicate player sidecar keys: {source_rows - source_keys}")
    base_rows = con.execute("select count(*) from _base_keyed").fetchone()[0]
    base_keys = con.execute("select count(distinct join_key) from _base_keyed").fetchone()[0]
    if base_rows != base_keys:
        raise ValueError(f"duplicate canonical player keys: {base_rows - base_keys}")

    conflict_terms = [
        f"(s.{qi(c)} is not null and b.{qi(c)} is not null and cast(s.{qi(c)} as varchar) <> cast(b.{qi(c)} as varchar))"
        for c in CANONICAL_COLUMNS
    ]
    conflicts = con.execute(
        "select count(*) from _source_keyed s join _base_keyed b using (join_key) where "
        + " or ".join(conflict_terms)
    ).fetchone()[0]
    if conflicts:
        raise ValueError(f"source conflicts with confirmed canonical values: {conflicts}")

    matched = con.execute("select count(*) from _source_keyed s join _base_keyed b using (join_key)").fetchone()[0]
    new_rows = con.execute(
        "select count(*) from _source_keyed s where not exists (select 1 from _base_keyed b where b.join_key=s.join_key)"
    ).fetchone()[0]
    improvements = con.execute(
        "select count(*) from _source_keyed s join _base_keyed b using (join_key) where "
        + " or ".join(f"b.{qi(c)} is null and s.{qi(c)} is not null" for c in CANONICAL_COLUMNS)
    ).fetchone()[0]

    for table in base_tables:
        if table == "player_fantasy":
            continue
        con.execute(f"create table public.{qi(table)} as select * from base.public.{qi(table)}")

    existing = ", ".join(
        f"case when s.{qi(c)} is not null then s.{qi(c)} else b.{qi(c)} end as {qi(c)}"
        for c in CANONICAL_COLUMNS
    )
    inserted = ", ".join(f"s.{qi(c)} as {qi(c)}" for c in CANONICAL_COLUMNS)
    con.execute(f"""
        create table public.player_fantasy as
        select {existing}
        from _base_keyed b left join _source_keyed s using (join_key)
        union all
        select {inserted}
        from _source_keyed s
        where not exists (select 1 from _base_keyed b where b.join_key=s.join_key)
    """)

    output_cols = columns(con, "public.player_fantasy")
    output_rows = con.execute("select count(*) from public.player_fantasy").fetchone()[0]
    output_keys = con.execute(f"select count(distinct {player_key('p')}) from public.player_fantasy p").fetchone()[0]
    duplicate_output = output_rows - output_keys
    missing_base = con.execute(
        f"select count(*) from (select distinct {player_key('b')} k from _base_keyed b) x "
        f"where not exists (select 1 from public.player_fantasy p where {player_key('p')}=x.k)"
    ).fetchone()[0]
    if output_cols != CANONICAL_COLUMNS:
        raise ValueError(f"output schema mismatch: {output_cols}")
    if output_rows < base_rows or duplicate_output or missing_base:
        raise ValueError(
            f"preservation failure: base_rows={base_rows}, output_rows={output_rows}, "
            f"duplicate_output={duplicate_output}, missing_base={missing_base}"
        )
    report = {
        "sidecar_files": len(files),
        "source_rows": source_rows,
        "source_unique_keys": source_keys,
        "matched_rows": matched,
        "new_player_rows": new_rows,
        "existing_rows_with_improvements": improvements,
        "base_player_rows": base_rows,
        "output_player_rows": output_rows,
        "output_unique_keys": output_keys,
        "duplicate_output_keys": duplicate_output,
        "missing_base_keys": missing_base,
        "schema_unchanged": True,
        "base_not_mutated": True,
        "new_lineage": False,
        "output_columns": output_cols,
        "join_key": ["db_name", "year", "week", "platform_player_id", "manager", "team_key", "team_name"],
    }
    out.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    con.close()
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--sidecars", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--pattern", default="promotable_player_delta*.parquet")
    args = parser.parse_args()
    apply(args.base, args.sidecars, args.out, args.pattern)


if __name__ == "__main__":
    main()
