"""Validate and promote successful MFL rescue rows into one canonical lake copy.

The source artifacts are matchup rows.  This deliberately keeps the existing table
schemas and lineage: it never adds columns, never uses bf/fallback fields, and only
fills NULL truth fields.  Player rows are updated through the existing team identity
when the canonical player table carries that identity.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_cols(con: duckdb.DuckDBPyConnection, db: str, table: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {db}.public.{qi(table)}").fetchall()]


def db_tables(con: duckdb.DuckDBPyConnection, db: str) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name='public'",
        [db],
    ).fetchall()]


def duplicate_key_count(con: duckdb.DuckDBPyConnection, relation: str, key: list[str]) -> int:
    key_sql = ", ".join(qi(column) for column in key)
    return int(con.execute(
        f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM {relation} "
        f"GROUP BY {key_sql} HAVING COUNT(*) > 1)"
    ).fetchone()[0])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sidecars", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--prefix", default="mfl_underpopulated_week_rescue_", help="Parquet filename prefix")
    ap.add_argument("--platform", choices=("mfl", "fleaflicker", "sleeper"), default=None)
    args = ap.parse_args()
    if args.out.exists():
        args.out.unlink()
    files = sorted(args.sidecars.rglob(f"{args.prefix}*.parquet"))
    if not files:
        raise SystemExit("no successful MFL parquet artifacts found")

    con = duckdb.connect(str(args.out))
    esc = lambda p: str(p.resolve()).replace("'", "''")
    con.execute(f"ATTACH '{esc(args.base)}' AS base (READ_ONLY)")
    con.execute("CREATE SCHEMA public")
    base_tables = db_tables(con, "base")
    if "matchup" not in base_tables or "player_fantasy" not in base_tables:
        raise SystemExit(f"canonical cache missing required tables: {sorted(base_tables)}")
    base_schema = {t: table_cols(con, "base", t) for t in base_tables}
    base_colsets = {t: {c for c, _ in cols} for t, cols in base_schema.items()}

    con.execute(
        "CREATE OR REPLACE TEMP TABLE _source AS "
        "SELECT DISTINCT * FROM read_parquet(?, union_by_name=true)",
        [ [str(p) for p in files] ],
    )
    source_cols = {r[0] for r in con.execute("DESCRIBE _source").fetchall()}
    forbidden_additions = {"is_playoffs_bf", "made_po_bf", "made_po"} & source_cols
    if forbidden_additions:
        raise SystemExit(f"forbidden parallel truth columns in sidecar: {sorted(forbidden_additions)}")

    # Only the existing canonical key is allowed.  Do not invent a key from names.
    key = [c for c in ("db_name", "year", "week", "manager", "franchise_id")
           if c in base_colsets["matchup"] and c in source_cols]
    source_matchup_cols = [c for c in base_colsets["matchup"] if c in source_cols]
    if not source_matchup_cols:
        raise SystemExit("MFL sidecar has no canonical matchup columns")
    con.execute("CREATE OR REPLACE TEMP TABLE _source_matchup AS SELECT * FROM _source")
    # Sleeper's source franchise_id is a large roster/team identifier, not the
    # canonical integer franchise_id used by older platform tables.  Do not use
    # it as a join key or cast it into that column; manager + team_name is the
    # stable source identity for this sidecar.
    franchise_id_incompatible = args.platform == "sleeper"
    if "franchise_id" in source_cols and "franchise_id" in base_colsets["matchup"]:
        franchise_id_incompatible = franchise_id_incompatible or con.execute(
            "SELECT COUNT(*) FROM _source_matchup "
            "WHERE franchise_id IS NOT NULL AND TRY_CAST(franchise_id AS INTEGER) IS NULL"
        ).fetchone()[0] > 0
    if franchise_id_incompatible:
        if all(c in base_colsets["matchup"] and c in source_cols
               for c in ("db_name", "year", "week", "manager", "team_name")):
            key = ["db_name", "year", "week", "manager", "team_name"]
        else:
            key = [c for c in ("db_name", "year", "week", "manager")
                   if c in base_colsets["matchup"] and c in source_cols]
    if len(key) < 4:
        raise SystemExit(f"canonical matchup key is incomplete: {key}")
    base_duplicate_keys = duplicate_key_count(con, "base.public.matchup", key)
    if base_duplicate_keys:
        raise SystemExit(
            f"duplicate canonical base matchup key count: {base_duplicate_keys}"
        )
    source_duplicate_keys = duplicate_key_count(con, "_source_matchup", key)
    if source_duplicate_keys:
        raise SystemExit(
            f"conflicting duplicate source matchup key count: {source_duplicate_keys}"
        )
    key_sql = " AND ".join(
        f"o.{qi(c)} IS NOT DISTINCT FROM b.{qi(c)}" for c in key
    )
    con.execute("CREATE OR REPLACE TEMP TABLE _source_keys AS SELECT DISTINCT "
                + ", ".join(qi(c) for c in key) + " FROM _source_matchup")

    # Copy every base table with exactly its original columns.  No schema widening.
    for table in base_tables:
        if table in {"matchup", "player_fantasy"}:
            continue
        cols = ", ".join(qi(c) for c, _ in base_schema[table])
        con.execute(f"CREATE TABLE public.{qi(table)} AS SELECT {cols} FROM base.public.{qi(table)}")

    # MFL rescue rows are newer and more complete for MFL.  A supplied source
    # value overrides the old corpus; a source NULL never erases a known fact.
    mcols = [c for c, _ in base_schema["matchup"]]
    mtypes = dict(base_schema["matchup"])
    select_existing = []
    for c in mcols:
        if c in source_cols:
            select_existing.append(
                f"CASE WHEN o.{qi(c)} IS NOT NULL THEN TRY_CAST(o.{qi(c)} AS {mtypes[c]}) "
                f"ELSE TRY_CAST(b.{qi(c)} AS {mtypes[c]}) END AS {qi(c)}"
            )
        else:
            select_existing.append(f"CAST(b.{qi(c)} AS {mtypes[c]}) AS {qi(c)}")
    source_insert = [f"TRY_CAST(o.{qi(c)} AS {mtypes[c]}) AS {qi(c)}" if c in source_cols else f"CAST(NULL AS {mtypes[c]}) AS {qi(c)}" for c in mcols]
    con.execute(f"""
        CREATE TABLE public.matchup AS
        SELECT {', '.join(select_existing)}
        FROM base.public.matchup b
        LEFT JOIN _source_matchup o ON {key_sql}
        UNION ALL
        SELECT {', '.join(source_insert)}
        FROM _source_matchup o
        WHERE NOT EXISTS (SELECT 1 FROM base.public.matchup b WHERE {key_sql})
    """)

    # Player promotion is possible only where the existing player table has a team key.
    pcols = [c for c, _ in base_schema["player_fantasy"]]
    pset = set(pcols)
    pteam = next((c for c in ("franchise_id", "manager", "manager_id", "roster_id", "team_id")
                  if c in pset and c in source_cols), None)
    if franchise_id_incompatible and "manager" in pset and "manager" in source_cols:
        pteam = "manager"
    player_outcome_cols = [c for c in ("win", "loss", "tie", "team_points", "opponent_points",
                                       "is_playoffs", "is_championship", "champion")
                           if c in pset and c in source_cols]
    player_updates = 0
    if pteam and player_outcome_cols:
        # Deduplicate source teams before joining; conflicting source values are a hard fail.
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE _player_source AS
          SELECT {', '.join(qi(c) for c in (['db_name','year','week',pteam] + player_outcome_cols))}
          FROM _source_matchup
          QUALIFY ROW_NUMBER() OVER (PARTITION BY db_name, year, week, {qi(pteam)} ORDER BY matchup_id NULLS LAST) = 1
        """)
        joins = (f"o.db_name=b.db_name AND CAST(o.year AS INTEGER)=CAST(b.year AS INTEGER) "
                 f"AND CAST(o.week AS INTEGER)=CAST(b.week AS INTEGER) "
                 f"AND o.{qi(pteam)} IS NOT DISTINCT FROM b.{qi(pteam)}")
        pselect = []
        ptypes = dict(base_schema["player_fantasy"])
        for c in pcols:
            if c in player_outcome_cols:
                pselect.append(
                    f"CASE WHEN o.{qi(c)} IS NOT NULL THEN TRY_CAST(o.{qi(c)} AS {ptypes[c]}) "
                    f"ELSE TRY_CAST(b.{qi(c)} AS {ptypes[c]}) END AS {qi(c)}"
                )
            else:
                pselect.append(f"CAST(b.{qi(c)} AS {ptypes[c]}) AS {qi(c)}")
        con.execute(f"CREATE TABLE public.player_fantasy AS SELECT {', '.join(pselect)} "
                    f"FROM base.public.player_fantasy b LEFT JOIN _player_source o ON {joins}")
        player_updates = con.execute(f"""
          SELECT COUNT(*) FROM base.public.player_fantasy b JOIN _player_source o ON {joins}
          WHERE ({' OR '.join(f'o.{qi(c)} IS NOT NULL AND o.{qi(c)} IS DISTINCT FROM b.{qi(c)}' for c in player_outcome_cols)})
        """).fetchone()[0]
    else:
        pcols_sql = ", ".join(qi(c) for c in pcols)
        con.execute(f"CREATE TABLE public.player_fantasy AS SELECT {pcols_sql} FROM base.public.player_fantasy")

    # Exact schema guard for every base table.
    output_db = con.execute("SELECT current_database()").fetchone()[0]
    for table, schema in base_schema.items():
        got = table_cols(con, output_db, table)
        if got != schema:
            raise SystemExit(f"schema drift in {table}: base={schema} output={got}")

    base_matchups = con.execute("SELECT COUNT(*) FROM base.public.matchup").fetchone()[0]
    out_matchups = con.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
    new_source_keys = con.execute(
        "SELECT COUNT(*) FROM _source_keys o WHERE NOT EXISTS "
        f"(SELECT 1 FROM base.public.matchup b WHERE {key_sql})"
    ).fetchone()[0]
    output_duplicate_keys = duplicate_key_count(con, "public.matchup", key)
    if output_duplicate_keys:
        raise SystemExit(
            f"duplicate canonical output matchup key count: {output_duplicate_keys}"
        )
    expected_matchup_rows = base_matchups + new_source_keys
    if out_matchups != expected_matchup_rows:
        raise SystemExit(
            "matchup row delta does not equal genuinely new source keys: "
            f"base={base_matchups}, new_keys={new_source_keys}, output={out_matchups}"
        )
    base_players = con.execute("SELECT COUNT(*) FROM base.public.player_fantasy").fetchone()[0]
    out_players = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    if out_players != base_players:
        raise SystemExit(
            f"player row count changed during authoritative MFL merge: base={base_players}, output={out_players}"
        )
    source_rows = con.execute("SELECT COUNT(*) FROM _source_matchup").fetchone()[0]
    report = {
        "source_artifact_files": len(files),
        "source_matchup_rows": source_rows,
        "canonical_key": key,
        "base_duplicate_matchup_keys": int(base_duplicate_keys),
        "source_duplicate_matchup_keys": int(source_duplicate_keys),
        "output_duplicate_matchup_keys": int(output_duplicate_keys),
        "base_matchup_rows": base_matchups,
        "output_matchup_rows": out_matchups,
        "new_matchup_rows": out_matchups - base_matchups,
        "new_source_matchup_keys": int(new_source_keys),
        "base_player_rows": base_players,
        "output_player_rows": out_players,
        "player_rows_improved": int(player_updates),
        "player_team_join": pteam,
        "player_outcome_columns": player_outcome_cols,
        "schema_unchanged": True,
        "lineage_unchanged": True,
    }
    args.out.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    con.close()


if __name__ == "__main__":
    main()
