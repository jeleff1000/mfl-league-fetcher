"""Apply local PBP scoring rollups to Fly ___ops super-table rows.

This is a narrow overlay for columns whose source is play-by-play detail.  It
does not insert super-table rows; it adds missing columns, zero-fills PBP-era
rows, stages the local rollup by ``player_week``, and updates existing rows.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.targets.fly_target import FlyTarget  # noqa: E402
from multi_league.data_fetchers.pbp_scoring_enrichment import PBP_ROLLUP_COLUMNS  # noqa: E402

SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
STAGE_TABLE = "public.pbp_scoring_rollup_stage_20260505"
LEAGUES_STAGE_DB = "__ops_pbp_scoring_rollup_20260505"
LEAGUES_STAGE_TABLE = "pbp_scoring_rollup_stage"
REMOTE_LEAGUES_PATH = "/data/___leagues.duckdb"
DEFAULT_ROLLUP = (
    ROOT
    / "fantasy_football_data"
    / "cache"
    / "nflverse"
    / "scoring_events"
    / "rollups"
    / "pbp_scoring_player_week_rollup.parquet"
)

UPDATE_COLUMNS = tuple(dict.fromkeys((*PBP_ROLLUP_COLUMNS, "fum_rec_yds")))
AGGREGATE_TABLES = (
    ("nfl_historical.player_nfl_season", False, True),
    ("nfl_historical.player_nfl_season_all", True, True),
    ("nfl_historical.player_nfl_career", False, False),
    ("nfl_historical.player_nfl_career_all", True, False),
)


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(float(value))


def load_rollup(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)
    if "player_week" not in df.columns:
        raise ValueError(f"{path} is missing player_week")
    for col in PBP_ROLLUP_COLUMNS:
        if col not in df.columns:
            df[col] = 0.0
    df = df[["player_week", *PBP_ROLLUP_COLUMNS]].copy()
    df["player_week"] = df["player_week"].astype("string")
    for col in PBP_ROLLUP_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.groupby("player_week", dropna=False)[list(PBP_ROLLUP_COLUMNS)].sum().reset_index()
    df["fum_rec_yds"] = df["fumble_recovery_yards_own"] + df["fumble_recovery_yards_opp"]
    df = df[df["player_week"].notna() & df["player_week"].astype(str).ne("")]
    df = df[df[list(UPDATE_COLUMNS)].fillna(0).ne(0).any(axis=1)]
    return df.reset_index(drop=True)


def execute(writer: FlyWriter, sql: str, *, dry_run: bool) -> list[dict]:
    if dry_run:
        print(sql.strip()[:1200])
        return []
    return writer.execute(sql, database="___ops")


def create_stage(writer: FlyWriter, *, dry_run: bool) -> None:
    col_defs = ",\n            ".join(f"{q_ident(col)} DOUBLE" for col in UPDATE_COLUMNS)
    sql = f"""
        CREATE SCHEMA IF NOT EXISTS public;
        DROP TABLE IF EXISTS {STAGE_TABLE};
        CREATE TABLE {STAGE_TABLE} (
            player_week VARCHAR,
            {col_defs}
        );
    """
    execute(writer, sql, dry_run=dry_run)


def write_stage_db(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.register("stage_df", df)
        conn.execute(f"CREATE TABLE public.{LEAGUES_STAGE_TABLE} AS SELECT * FROM stage_df")
        count = conn.execute(f"SELECT COUNT(*) FROM public.{LEAGUES_STAGE_TABLE}").fetchone()[0]
        print(f"[stage-db] {count:,} rows -> {path}", flush=True)
    finally:
        conn.close()


def upload_stage_db(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        print(f"[stage-db] dry-run upload {path} as {LEAGUES_STAGE_DB}", flush=True)
        return
    result = FlyTarget().merge_league(LEAGUES_STAGE_DB, path)
    print(f"[stage-db] uploaded via merge-league: {result}", flush=True)


def add_super_columns(writer: FlyWriter, *, dry_run: bool) -> None:
    statements = [
        f"ALTER TABLE {SUPER_TABLE} ADD COLUMN IF NOT EXISTS {q_ident(col)} DOUBLE;" for col in UPDATE_COLUMNS
    ]
    statements.append(
        "ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN IF NOT EXISTS pts_idp_fum_rec_yd DOUBLE;"
    )
    execute(writer, "\n".join(statements), dry_run=dry_run)


def insert_stage(writer: FlyWriter, df: pd.DataFrame, *, chunk_size: int, dry_run: bool) -> None:
    cols = ("player_week", *UPDATE_COLUMNS)
    quoted_cols = ", ".join(q_ident(col) for col in cols)
    chunks = math.ceil(len(df) / chunk_size)
    for idx in range(chunks):
        chunk = df.iloc[idx * chunk_size : (idx + 1) * chunk_size]
        values = []
        for row in chunk.itertuples(index=False):
            values.append("(" + ", ".join(sql_literal(value) for value in row) + ")")
        sql = f"INSERT INTO {STAGE_TABLE} ({quoted_cols}) VALUES\n" + ",\n".join(values) + ";"
        execute(writer, sql, dry_run=dry_run)
        print(f"[stage] {min((idx + 1) * chunk_size, len(df)):,}/{len(df):,} rows", flush=True)


def prepare_dedup_from_leagues(writer: FlyWriter, *, dry_run: bool) -> None:
    aggregate_selects = ", ".join(f"SUM(COALESCE({q_ident(col)}, 0)) AS {q_ident(col)}" for col in UPDATE_COLUMNS)
    sql = f"""
        DROP TABLE IF EXISTS {STAGE_TABLE};
        DROP TABLE IF EXISTS {STAGE_TABLE}_dedup;
        ATTACH IF NOT EXISTS '{REMOTE_LEAGUES_PATH}' AS stage_leagues (READ_ONLY);
        CREATE OR REPLACE TABLE {STAGE_TABLE}_dedup AS
        SELECT
            player_week,
            {aggregate_selects}
        FROM stage_leagues.public.{q_ident(LEAGUES_STAGE_TABLE)}
        WHERE db_name = {sql_literal(LEAGUES_STAGE_DB)}
          AND player_week IS NOT NULL
        GROUP BY player_week;
        DETACH stage_leagues;
    """
    execute(writer, sql, dry_run=dry_run)


def prepare_dedup_from_inline(writer: FlyWriter, *, dry_run: bool) -> None:
    aggregate_selects = ", ".join(f"SUM(COALESCE({q_ident(col)}, 0)) AS {q_ident(col)}" for col in UPDATE_COLUMNS)
    sql = f"""
        CREATE OR REPLACE TABLE {STAGE_TABLE}_dedup AS
        SELECT
            player_week,
            {aggregate_selects}
        FROM {STAGE_TABLE}
        WHERE player_week IS NOT NULL
        GROUP BY player_week;
    """
    execute(writer, sql, dry_run=dry_run)


def apply_updates(writer: FlyWriter, *, dry_run: bool) -> None:
    zero_sets = ",\n            ".join(f"{q_ident(col)} = 0" for col in UPDATE_COLUMNS)
    update_sets = ",\n            ".join(f"{q_ident(col)} = st.{q_ident(col)}" for col in UPDATE_COLUMNS)
    sql = f"""
        UPDATE {SUPER_TABLE}
        SET
            {zero_sets}
        WHERE year >= 1999;

        UPDATE {SUPER_TABLE} AS s
        SET
            {update_sets}
        FROM {STAGE_TABLE}_dedup AS st
        WHERE s.player_week = st.player_week;

        UPDATE {SUPER_TABLE}
        SET pts_idp_fum_rec_yd = COALESCE(fumble_recovery_yards_own, 0) + COALESCE(fumble_recovery_yards_opp, 0)
        WHERE year >= 1999;
    """
    execute(writer, sql, dry_run=dry_run)


def verify(writer: FlyWriter) -> list[dict]:
    sql = f"""
        SELECT
            COUNT(*) AS rows_1999_plus,
            SUM(CASE WHEN completions_50plus <> 0 THEN 1 ELSE 0 END) AS pass_cmp_50plus_rows,
            SUM(CASE WHEN receptions_0_4 <> 0 THEN 1 ELSE 0 END) AS rec_0_4_rows,
            SUM(CASE WHEN special_teams_tackles_solo <> 0 THEN 1 ELSE 0 END) AS st_tkl_solo_rows,
            SUM(CASE WHEN fumble_recovery_yards_opp <> 0 THEN 1 ELSE 0 END) AS fum_opp_rows,
            ROUND(SUM(COALESCE(completions_50plus, 0)), 4) AS pass_cmp_50plus,
            ROUND(SUM(COALESCE(special_teams_tackles_solo, 0)), 4) AS st_tkl_solo,
            ROUND(SUM(COALESCE(fumble_recovery_yards_own, 0)), 4) AS fum_own_yds,
            ROUND(SUM(COALESCE(fumble_recovery_yards_opp, 0)), 4) AS fum_opp_yds
        FROM {SUPER_TABLE}
        WHERE year >= 1999
    """
    return writer.execute(sql, database="___ops")


def aggregate_stage_name(table: str) -> str:
    return f"public.{table.split('.')[-1]}_pbp_scoring_stage_20260505"


def add_aggregate_columns(writer: FlyWriter, *, dry_run: bool) -> None:
    statements: list[str] = []
    for table, _, _ in AGGREGATE_TABLES:
        statements.extend(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {q_ident(col)} DOUBLE;" for col in UPDATE_COLUMNS
        )
    execute(writer, "\n".join(statements), dry_run=dry_run)


def apply_aggregate_updates(writer: FlyWriter, *, dry_run: bool) -> None:
    agg_selects = ",\n            ".join(
        f"ROUND(SUM(COALESCE({q_ident(col)}, 0)), 4) AS {q_ident(col)}" for col in UPDATE_COLUMNS
    )

    for table, include_playoffs, season in AGGREGATE_TABLES:
        stage = aggregate_stage_name(table)
        phase = "season_type IN ('REG', 'POST')" if include_playoffs else "season_type = 'REG'"
        key_select = "NFL_player_id, CAST(year AS INTEGER) AS year" if season else "NFL_player_id"
        group_by = "NFL_player_id, year" if season else "NFL_player_id"
        join_sql = (
            "st.NFL_player_id = t.NFL_player_id AND st.year = t.year"
            if season
            else "st.NFL_player_id = t.NFL_player_id"
        )
        table_cols = [
            row["column_name"]
            for row in writer.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_catalog = '___ops'
                  AND table_schema = 'nfl_historical'
                  AND table_name = {sql_literal(table.split('.')[-1])}
                ORDER BY ordinal_position
                """,
                database="___ops",
            )
        ]
        base_cols = [col for col in table_cols if col not in UPDATE_COLUMNS]
        selects = [f"t.{q_ident(col)}" for col in base_cols]
        selects.extend(f"COALESCE(st.{q_ident(col)}, 0) AS {q_ident(col)}" for col in UPDATE_COLUMNS)
        sql = f"""
            CREATE OR REPLACE TABLE {stage} AS
            SELECT
                {key_select},
                {agg_selects}
            FROM {SUPER_TABLE}
            WHERE NFL_player_id IS NOT NULL
              AND year IS NOT NULL
              AND week IS NOT NULL
              AND {phase}
            GROUP BY {group_by};

            CREATE OR REPLACE TABLE {table} AS
            SELECT
                {', '.join(selects)}
            FROM {table} AS t
            LEFT JOIN {stage} AS st
              ON {join_sql};
        """
        print(f"[aggregate] {table}", flush=True)
        execute(writer, sql, dry_run=dry_run)


def verify_aggregates(writer: FlyWriter) -> list[dict]:
    metric_selects = ",\n            ".join(
        f"ROUND(SUM(COALESCE({q_ident(col)}, 0)), 4) AS {q_ident(col)}"
        for col in (
            "completions_50plus",
            "receptions_0_4",
            "special_teams_tackles_solo",
            "fumble_recovery_yards_own",
            "fumble_recovery_yards_opp",
            "fum_rec_yds",
        )
    )
    branches = []
    for table, _, _ in AGGREGATE_TABLES:
        branches.append(
            f"""
            SELECT
                {sql_literal(table)} AS table_name,
                COUNT(*) AS rows,
                {metric_selects}
            FROM {table}
            """
        )
    return writer.execute("\nUNION ALL\n".join(branches), database="___ops")


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply PBP scoring rollup columns to Fly ___ops.")
    parser.add_argument("--rollup", type=Path, default=DEFAULT_ROLLUP)
    parser.add_argument("--chunk-size", type=int, default=2500)
    parser.add_argument("--stage-db", type=Path, default=ROOT / "tmp" / "pbp_scoring_rollup_stage.duckdb")
    parser.add_argument("--transport", choices=("merge-league", "inline"), default="merge-league")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-aggregates", action="store_true")
    parser.add_argument("--only-aggregates", action="store_true")
    args = parser.parse_args()

    load_env()
    writer = FlyWriter()

    if not args.only_aggregates:
        df = load_rollup(args.rollup)
        print(f"[rollup] rows={len(df):,} path={args.rollup}", flush=True)

        add_super_columns(writer, dry_run=args.dry_run)
        if args.transport == "merge-league":
            write_stage_db(df, args.stage_db)
            upload_stage_db(args.stage_db, dry_run=args.dry_run)
            prepare_dedup_from_leagues(writer, dry_run=args.dry_run)
        else:
            create_stage(writer, dry_run=args.dry_run)
            insert_stage(writer, df, chunk_size=args.chunk_size, dry_run=args.dry_run)
            prepare_dedup_from_inline(writer, dry_run=args.dry_run)
        apply_updates(writer, dry_run=args.dry_run)

        if not args.dry_run:
            print("[verify] weekly super table", flush=True)
            print(verify(writer), flush=True)

    if not args.skip_aggregates:
        print("[rollup] applying targeted season/career aggregate columns", flush=True)
        apply_aggregate_updates(writer, dry_run=args.dry_run)

        if not args.dry_run:
            print("[verify] aggregate tables", flush=True)
            print(verify_aggregates(writer), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
