"""Apply a source signal sidecar to a canonical lake copy without schema drift.

This is a candidate-builder, not a cache writer.  It updates only existing
columns, preserves the canonical player schema and player row count, and fails
closed on ambiguous team joins.  The source sidecar remains the provenance for
playoff/championship signals; ``champion`` is only filled from an explicit
title-game winner row and never cleared or overwritten with a zero.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_cols(con: duckdb.DuckDBPyConnection, db: str, table: str) -> list[str]:
    return [r[0] for r in con.execute(f"DESCRIBE {db}.public.{qi(table)}").fetchall()]


def tables(con: duckdb.DuckDBPyConnection, db: str) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name='public'", [db]
    ).fetchall()]


PLAYER_ID_COLUMNS = (
    "NFL_player_id", "sleeper_player_id", "fleaflicker_player_id",
    "mfl_player_id", "espn_player_id", "yahoo_player_id",
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sidecar", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()
    if args.out.exists():
        args.out.unlink()
    con = duckdb.connect(str(args.out))
    base_uri = str(args.base.resolve()).replace("'", "''")
    side_uri = str(args.sidecar.resolve()).replace("'", "''")
    con.execute(f"ATTACH '{base_uri}' AS base (READ_ONLY)")
    # Parquet is a file, not a DuckDB database.  Keep it read-only by exposing
    # it as a view; never attach or mutate the sidecar.
    con.execute(f"CREATE VIEW source_matchup_rescue AS SELECT * FROM read_parquet('{side_uri}')")
    con.execute("CREATE SCHEMA public")
    base_tables = set(tables(con, "base"))
    side_cols = {r[0] for r in con.execute("DESCRIBE source_matchup_rescue").fetchall()}
    required_side = {"db_name", "year", "week", "manager", "platform", "win", "team_points", "is_playoffs", "is_championship", "champion"}
    if not required_side <= side_cols:
        raise SystemExit(f"sidecar missing required signal columns: {sorted(required_side - side_cols)}")

    # Copy every non-target table exactly.  No sidecar-only columns can enter
    # the canonical schema through this path.
    for table in sorted(base_tables - {"player_fantasy", "matchup"}):
        con.execute(f"CREATE TABLE public.{qi(table)} AS SELECT * FROM base.public.{qi(table)}")

    pcols = table_cols(con, "base", "player_fantasy")
    if "player_fantasy" not in base_tables:
        raise SystemExit("base lake lacks player_fantasy")
    if "is_playoffs" not in pcols or "champion" not in pcols:
        raise SystemExit("base player_fantasy lacks canonical is_playoffs/champion columns")
    p_manager = "LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)), ''))" if "manager" in pcols else "NULL"
    p_team = "LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)), ''))" if "team_name" in pcols else "NULL"
    p_team_key = "NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), '')" if "team_key" in pcols else "NULL"
    source_manager = "LOWER(NULLIF(TRIM(CAST(manager AS VARCHAR)), ''))"
    source_team = "LOWER(NULLIF(TRIM(CAST(team_name AS VARCHAR)), ''))" if "team_name" in side_cols else "NULL"
    source_team_key = "NULLIF(TRIM(CAST(team_key AS VARCHAR)), '')" if "team_key" in side_cols else "NULL"
    player_identity = " OR ".join(
        f"NULLIF(TRIM(CAST(p.{qi(column)} AS VARCHAR)), '') IS NOT NULL"
        for column in PLAYER_ID_COLUMNS if column in pcols
    ) or "FALSE"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE _signals_raw AS
      SELECT CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
             CAST(week AS INTEGER) AS week, {source_team_key} AS source_team_key,
             {source_manager} AS manager_key, {source_team} AS team_key_name,
             MAX(CASE WHEN CAST(is_playoffs AS INTEGER)=1 THEN 1 ELSE 0 END) AS source_playoffs,
             MAX(CASE WHEN CAST(is_championship AS INTEGER)=1 AND CAST(champion AS INTEGER)=1 THEN 1 ELSE 0 END) AS source_champion,
             MAX(CAST(win AS INTEGER)) AS source_win,
             MAX(team_points) AS source_team_points,
             MAX(NULLIF(LOWER(TRIM(CAST(platform AS VARCHAR))), '')) AS source_platform
      FROM source_matchup_rescue
      GROUP BY 1,2,3,4,5,6
    """)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE _signals AS
      SELECT r.*, COUNT(DISTINCT source_team_key) OVER
        (PARTITION BY db_name, year, week, manager_key) AS manager_team_count
      FROM _signals_raw r
    """)
    identity = f"""(
      ({p_team_key} IS NOT NULL AND s.source_team_key IS NOT NULL AND {p_team_key}=s.source_team_key)
      OR ({p_manager}=s.manager_key AND {p_team}=s.team_key_name AND s.team_key_name IS NOT NULL)
      OR (
        {p_manager}=s.manager_key AND s.manager_key IS NOT NULL
        AND s.manager_team_count=1
      )
    )"""
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE _player_matches AS
      SELECT p.db_name, CAST(p.year AS INTEGER) AS year, CAST(p.week AS INTEGER) AS week,
             p.rowid AS player_rowid, COUNT(DISTINCT s.source_team_key) AS matches,
             MAX(s.source_playoffs) AS source_playoffs, MAX(s.source_champion) AS source_champion,
             MAX(s.source_win) AS source_win, MAX(s.source_team_points) AS source_team_points,
             MAX(s.source_platform) AS source_platform
      FROM base.public.player_fantasy p
      LEFT JOIN _signals s
        ON s.db_name=p.db_name AND s.year=CAST(p.year AS INTEGER) AND s.week=CAST(p.week AS INTEGER)
       AND ({identity}) AND ({player_identity})
      GROUP BY 1,2,3,4
    """)
    ambiguous = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches > 1").fetchone()[0]
    if ambiguous:
        raise SystemExit(f"ambiguous player team joins: {ambiguous}")
    matched = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches = 1").fetchone()[0]
    playoff_updates = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1 AND source_playoffs=1").fetchone()[0]
    champ_updates = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1 AND source_champion=1").fetchone()[0]
    win_updates = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1 AND source_win IS NOT NULL").fetchone()[0]
    team_points_updates = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1 AND source_team_points IS NOT NULL").fetchone()[0]
    platform_updates = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1 AND source_platform IS NOT NULL").fetchone()[0]

    expr = []
    for col in pcols:
        if col == "is_playoffs":
            expr.append(f"CASE WHEN b.{qi(col)} IS NULL AND m.source_playoffs=1 THEN 1 ELSE b.{qi(col)} END AS {qi(col)}")
        elif col == "champion":
            expr.append(f"CASE WHEN b.{qi(col)} IS NULL AND m.source_champion=1 THEN 1 ELSE b.{qi(col)} END AS {qi(col)}")
        elif col == "win":
            expr.append(f"CASE WHEN b.{qi(col)} IS NULL THEN m.source_win ELSE b.{qi(col)} END AS {qi(col)}")
        elif col == "team_points":
            expr.append(f"CASE WHEN b.{qi(col)} IS NULL THEN m.source_team_points ELSE b.{qi(col)} END AS {qi(col)}")
        elif col == "platform":
            expr.append(f"CASE WHEN b.{qi(col)} IS NULL THEN m.source_platform ELSE b.{qi(col)} END AS {qi(col)}")
        else:
            expr.append(f"b.{qi(col)} AS {qi(col)}")
    con.execute(f"""
      CREATE TABLE public.player_fantasy AS
      SELECT {', '.join(expr)}
      FROM base.public.player_fantasy b
      JOIN _player_matches m ON m.player_rowid=b.rowid
    """)

    # Matchup table: update only columns already present in the canonical table
    # and append unmatched source rows projected into that exact schema.
    if "matchup" in base_tables:
        mcols = table_cols(con, "base", "matchup")
        if not {"db_name", "year", "week"} <= set(mcols):
            raise SystemExit("base matchup lacks canonical keys")
        key_parts = ["b.db_name=s.db_name", "CAST(b.year AS INTEGER)=s.year", "CAST(b.week AS INTEGER)=s.week"]
        if "manager" in mcols and "manager" in side_cols:
            key_parts.append(f"LOWER(TRIM(CAST(b.manager AS VARCHAR)))=LOWER(TRIM(CAST(s.manager AS VARCHAR)))")
        elif "team_key" in mcols and "team_key" in side_cols:
            key_parts.append("CAST(b.team_key AS VARCHAR)=CAST(s.team_key AS VARCHAR)")
        else:
            raise SystemExit("no stable matchup identity shared by base and sidecar")
        pred = " AND ".join(key_parts)
        common = [c for c in mcols if c in side_cols]
        select_existing = []
        for c in mcols:
            if c in common:
                select_existing.append(f"COALESCE(s.{qi(c)}, b.{qi(c)}) AS {qi(c)}")
            else:
                select_existing.append(f"b.{qi(c)} AS {qi(c)}")
        insert_select = [f"s.{qi(c)} AS {qi(c)}" if c in side_cols else f"CAST(NULL AS VARCHAR) AS {qi(c)}" for c in mcols]
        con.execute(f"""
          CREATE TABLE public.matchup AS
          SELECT {', '.join(select_existing)} FROM base.public.matchup b
          LEFT JOIN source_matchup_rescue s ON {pred}
          UNION ALL
          SELECT {', '.join(insert_select)} FROM source_matchup_rescue s
          WHERE NOT EXISTS (SELECT 1 FROM base.public.matchup b WHERE {pred})
        """)
    elif "matchup" in base_tables:
        raise SystemExit("unreachable")

    # Exact-schema and cardinality gates.
    output_db = con.execute("SELECT current_database()").fetchone()[0]
    for table in base_tables:
        if table not in tables(con, output_db):
            raise SystemExit(f"output missing table {table}")
        if table_cols(con, "base", table) != table_cols(con, output_db, table):
            raise SystemExit(f"schema drift in {table}")
    base_player_rows = con.execute("SELECT COUNT(*) FROM base.public.player_fantasy").fetchone()[0]
    out_player_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    if base_player_rows != out_player_rows:
        raise SystemExit(f"player row count changed: {base_player_rows} -> {out_player_rows}")
    report = {
        "base_player_rows": int(base_player_rows), "output_player_rows": int(out_player_rows),
        "player_rows_matched_to_source_team": int(matched),
        "player_rows_playoff_signal_candidates": int(playoff_updates),
        "player_rows_championship_winner_signal_candidates": int(champ_updates),
        "player_rows_win_candidates": int(win_updates),
        "player_rows_team_points_candidates": int(team_points_updates),
        "player_rows_platform_candidates": int(platform_updates),
        "base_columns_preserved": True, "new_columns": [], "new_lineage": False,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
