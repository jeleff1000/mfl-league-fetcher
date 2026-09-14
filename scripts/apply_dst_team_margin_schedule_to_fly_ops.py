"""Apply DST team score/margin columns to Fly ___ops tables.

Builds a tiny local DuckDB stage from existing game schedules, matches those
games to existing DEF rows by team/opponent/week, pre-aggregates the values for
the NFL season/career rollup tables, then applies simple SQL joins in Fly.
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
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402
from multi_league.core.targets.fly_target import FlyTarget  # noqa: E402
from multi_league.data_fetchers.defense_stats import NFLVERSE_TO_SUPER_TABLE_TEAM_MAP  # noqa: E402
from nfl_data.nfl_franchises import get_nfl_franchise_number  # noqa: E402
from nfl_data.team_margin import TEAM_MARGIN_COLUMNS, team_margin_values  # noqa: E402

SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
LEAGUES_STAGE_DB = "__ops_dst_team_margin_20260505"
REMOTE_LEAGUES_PATH = "/data/___leagues.duckdb"

WEEKLY_STAGE_TABLE = "dst_team_margin_weekly_stage"
SEASON_STAGE_TABLE = "dst_team_margin_season_stage"
SEASON_ALL_STAGE_TABLE = "dst_team_margin_season_all_stage"
CAREER_STAGE_TABLE = "dst_team_margin_career_stage"
CAREER_ALL_STAGE_TABLE = "dst_team_margin_career_all_stage"

OPS_STAGE_TABLES = {
    WEEKLY_STAGE_TABLE: "public.dst_team_margin_weekly_stage_20260505",
    SEASON_STAGE_TABLE: "public.dst_team_margin_season_stage_20260505",
    SEASON_ALL_STAGE_TABLE: "public.dst_team_margin_season_all_stage_20260505",
    CAREER_STAGE_TABLE: "public.dst_team_margin_career_stage_20260505",
    CAREER_ALL_STAGE_TABLE: "public.dst_team_margin_career_all_stage_20260505",
}

DEFAULT_MASTER_SCHEDULE = ROOT / "fantasy_football_data" / "cache" / "pfr_excel" / "_master_schedule_1920_2025.parquet"
DEFAULT_PBP_CACHE = ROOT / "fantasy_football_data" / "cache" / "nflverse"
DEFAULT_STAGE_DB = ROOT / "tmp" / "dst_team_margin_stage.duckdb"

AGGREGATE_TARGETS = (
    ("nfl_historical.player_nfl_season", SEASON_STAGE_TABLE, True),
    ("nfl_historical.player_nfl_season_all", SEASON_ALL_STAGE_TABLE, True),
    ("nfl_historical.player_nfl_career", CAREER_STAGE_TABLE, False),
    ("nfl_historical.player_nfl_career_all", CAREER_ALL_STAGE_TABLE, False),
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


def q_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def parquet_literal(path: Path) -> str:
    return "'" + path.resolve().as_posix().replace("'", "''") + "'"


def normalize_code(value: object) -> str:
    if value is None or pd.isna(value):
        return "NON_NFL"
    text = str(value).strip().upper()
    return text if text else "NON_NFL"


def normalize_nflverse_schedule_code(value: object, year: object) -> str:
    """Normalize nflverse's current-franchise codes to super-table era codes."""
    code = normalize_code(value)
    try:
        season = int(year)
    except (TypeError, ValueError):
        season = 0

    if code in {"LA", "LAR"}:
        return "STL" if 1995 <= season <= 2015 else "LAR"
    if code == "LV":
        return "OAK" if season <= 2019 else "LV"
    if code in {"LAC", "SD"}:
        return "SDG" if season <= 2016 else "LAC"
    if code == "JAC":
        return "JAX"
    if code == "ARZ":
        return "ARI"
    return NFLVERSE_TO_SUPER_TABLE_TEAM_MAP.get(code, code)


def franchise_key(franchise_number: object, abbrev: object) -> str:
    try:
        if franchise_number is not None and not pd.isna(franchise_number):
            return f"F:{int(float(franchise_number))}"
    except (TypeError, ValueError):
        pass
    return f"C:{normalize_code(abbrev)}"


def franchise_number_for_code(code: object, year: object) -> int | None:
    if code is None or pd.isna(code):
        return None
    try:
        season = int(year)
    except (TypeError, ValueError):
        return None
    return get_nfl_franchise_number(str(code), season)


def load_historical_schedule(master_schedule: Path) -> pd.DataFrame:
    if not master_schedule.exists():
        raise FileNotFoundError(f"Canonical schedule parquet not found: {master_schedule}")
    sql = f"""
        SELECT
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week,
            UPPER(COALESCE(season_phase, 'REG')) AS season_type,
            nfl_team,
            opponent_nfl_team,
            CAST(franchise_id AS DOUBLE) AS nfl_franchise_number,
            CAST(opponent_franchise_id AS DOUBLE) AS opponent_nfl_franchise_number,
            CAST(team_pts AS DOUBLE) AS team_pts,
            CAST(opp_pts AS DOUBLE) AS opp_pts,
            src_schedule
        FROM read_parquet({parquet_literal(master_schedule)})
        WHERE year < 1999
          AND week IS NOT NULL
          AND team_pts IS NOT NULL
          AND opp_pts IS NOT NULL
          AND nfl_team IS NOT NULL
    """
    return duckdb.connect().execute(sql).fetchdf()


def load_pbp_schedule(pbp_cache: Path, start_year: int = 1999) -> pd.DataFrame:
    files = sorted(
        p
        for p in pbp_cache.glob("nflverse_pbp_*.parquet")
        if p.stem.rsplit("_", 1)[-1].isdigit() and int(p.stem.rsplit("_", 1)[-1]) >= start_year
    )
    if not files:
        return pd.DataFrame(
            columns=[
                "year",
                "week",
                "season_type",
                "nfl_team",
                "opponent_nfl_team",
                "nfl_franchise_number",
                "opponent_nfl_franchise_number",
                "team_pts",
                "opp_pts",
                "src_schedule",
            ]
        )

    relation = "read_parquet([" + ", ".join(parquet_literal(path) for path in files) + "])"
    sql = f"""
        WITH games AS (
            SELECT DISTINCT
                CAST(season AS INTEGER) AS year,
                CAST(week AS INTEGER) AS week,
                UPPER(COALESCE(season_type, 'REG')) AS season_type,
                home_team,
                away_team,
                CAST(home_score AS DOUBLE) AS home_score,
                CAST(away_score AS DOUBLE) AS away_score
            FROM {relation}
            WHERE season >= {int(start_year)}
              AND week IS NOT NULL
              AND home_team IS NOT NULL
              AND away_team IS NOT NULL
              AND home_score IS NOT NULL
              AND away_score IS NOT NULL
        )
        SELECT
            year,
            week,
            season_type,
            home_team AS nfl_team,
            away_team AS opponent_nfl_team,
            home_score AS team_pts,
            away_score AS opp_pts,
            'nflverse_pbp' AS src_schedule
        FROM games
        UNION ALL
        SELECT
            year,
            week,
            season_type,
            away_team AS nfl_team,
            home_team AS opponent_nfl_team,
            away_score AS team_pts,
            home_score AS opp_pts,
            'nflverse_pbp' AS src_schedule
        FROM games
    """
    df = duckdb.connect().execute(sql).fetchdf()
    df["nfl_team"] = [normalize_nflverse_schedule_code(team, year) for team, year in zip(df["nfl_team"], df["year"])]
    df["opponent_nfl_team"] = [
        normalize_nflverse_schedule_code(team, year) for team, year in zip(df["opponent_nfl_team"], df["year"])
    ]
    df["nfl_franchise_number"] = [
        franchise_number_for_code(team, year) for team, year in zip(df["nfl_team"], df["year"])
    ]
    df["opponent_nfl_franchise_number"] = [
        franchise_number_for_code(team, year) for team, year in zip(df["opponent_nfl_team"], df["year"])
    ]
    return df


def add_team_margin_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        for col in TEAM_MARGIN_COLUMNS:
            df[col] = pd.Series(dtype="float64")
        return df

    values = [team_margin_values(row.team_pts, row.opp_pts) for row in df.itertuples(index=False)]
    value_df = pd.DataFrame(values, index=df.index)
    return pd.concat([df.reset_index(drop=True), value_df.reset_index(drop=True)], axis=1)


def load_def_rows() -> pd.DataFrame:
    reader = FlyReader()
    rows = reader.query(
        f"""
        SELECT
            player_week,
            NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week,
            UPPER(COALESCE(season_type, 'REG')) AS season_type,
            nfl_team,
            opponent_nfl_team,
            nfl_franchise_number,
            opponent_nfl_franchise_number,
            player
        FROM {SUPER_TABLE}
        WHERE NFL_player_id LIKE 'DEF-%'
          AND year IS NOT NULL
          AND week IS NOT NULL
          AND nfl_team IS NOT NULL
        """,
        database="___ops",
    )
    return pd.DataFrame(rows)


def build_weekly_stage(
    schedule: pd.DataFrame, def_rows: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    key_cols = ["year", "week", "season_type", "_team_key", "_opponent_key"]

    schedule = schedule.copy()
    schedule["year"] = pd.to_numeric(schedule["year"], errors="coerce").astype("Int64")
    schedule["week"] = pd.to_numeric(schedule["week"], errors="coerce").astype("Int64")
    schedule["season_type"] = schedule["season_type"].fillna("REG").astype(str).str.upper()
    schedule["nfl_team"] = schedule["nfl_team"].map(normalize_code)
    schedule["opponent_nfl_team"] = schedule["opponent_nfl_team"].map(normalize_code)
    schedule["_team_key"] = [
        franchise_key(fid, team) for fid, team in zip(schedule["nfl_franchise_number"], schedule["nfl_team"])
    ]
    schedule["_opponent_key"] = [
        franchise_key(fid, team)
        for fid, team in zip(schedule["opponent_nfl_franchise_number"], schedule["opponent_nfl_team"])
    ]
    schedule = schedule.dropna(subset=["year", "week"])
    schedule = schedule.drop_duplicates(key_cols, keep="last")

    def_rows = def_rows.copy()
    def_rows["year"] = pd.to_numeric(def_rows["year"], errors="coerce").astype("Int64")
    def_rows["week"] = pd.to_numeric(def_rows["week"], errors="coerce").astype("Int64")
    def_rows["season_type"] = def_rows["season_type"].fillna("REG").astype(str).str.upper()
    def_rows["nfl_team"] = def_rows["nfl_team"].map(normalize_code)
    def_rows["opponent_nfl_team"] = def_rows["opponent_nfl_team"].map(normalize_code)
    def_rows["_team_key"] = [
        franchise_key(fid, team) for fid, team in zip(def_rows["nfl_franchise_number"], def_rows["nfl_team"])
    ]
    def_rows["_opponent_key"] = [
        franchise_key(fid, team)
        for fid, team in zip(def_rows["opponent_nfl_franchise_number"], def_rows["opponent_nfl_team"])
    ]
    def_rows = def_rows.dropna(subset=["player_week", "NFL_player_id", "year", "week"])

    value_cols = ["team_pts", "opp_pts", *TEAM_MARGIN_COLUMNS]
    matched = def_rows.merge(schedule[key_cols + value_cols], on=key_cols, how="inner")
    matched = matched.drop_duplicates("player_week", keep="last")

    # Early historical rows occasionally disagree on REG/POST labelling while
    # still sharing the exact season/week/team/opponent identity. Fill those
    # from an unambiguous relaxed match and preserve the super-table season_type.
    unmatched_def = def_rows[~def_rows["player_week"].isin(matched["player_week"])].copy()
    if not unmatched_def.empty:
        relaxed_keys = ["year", "week", "_team_key", "_opponent_key"]
        schedule_relaxed = schedule[relaxed_keys + value_cols].copy()
        duplicate_relaxed = schedule_relaxed.duplicated(relaxed_keys, keep=False)
        schedule_relaxed = schedule_relaxed[~duplicate_relaxed]
        relaxed = unmatched_def.merge(schedule_relaxed, on=relaxed_keys, how="inner")
        if not relaxed.empty:
            matched = pd.concat([matched, relaxed], ignore_index=True).drop_duplicates("player_week", keep="last")

    unmatched_def = def_rows[~def_rows["player_week"].isin(matched["player_week"])].copy()
    unmatched_def = unmatched_def[unmatched_def["season_type"].eq("POST")]
    if not unmatched_def.empty:
        playoff_keys = ["year", "_team_key", "_opponent_key"]
        playoff_schedule = schedule[schedule["season_type"].eq("POST")][playoff_keys + value_cols].copy()
        duplicate_playoff = playoff_schedule.duplicated(playoff_keys, keep=False)
        playoff_schedule = playoff_schedule[~duplicate_playoff]
        playoff_relaxed = unmatched_def.merge(playoff_schedule, on=playoff_keys, how="inner")
        if not playoff_relaxed.empty:
            matched = pd.concat([matched, playoff_relaxed], ignore_index=True).drop_duplicates(
                "player_week", keep="last"
            )

    weekly_cols = [
        "player_week",
        "NFL_player_id",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
        "team_pts",
        "opp_pts",
        *TEAM_MARGIN_COLUMNS,
    ]
    weekly = matched[weekly_cols].copy()
    for col in TEAM_MARGIN_COLUMNS:
        weekly[col] = pd.to_numeric(weekly[col], errors="coerce").fillna(0.0)

    matched_pairs = matched[["year", "week", "season_type", "_team_key", "_opponent_key"]].drop_duplicates()
    schedule_missing = schedule.merge(matched_pairs, on=key_cols, how="left", indicator=True)
    schedule_missing = schedule_missing[schedule_missing["_merge"] == "left_only"].drop(columns=["_merge"])
    def_missing = def_rows[~def_rows["player_week"].isin(matched["player_week"])].copy()
    def_missing = def_missing.merge(schedule[key_cols].drop_duplicates(), on=key_cols, how="left", indicator=True)
    def_missing = def_missing[def_missing["_merge"] == "left_only"].drop(columns=["_merge"])
    return weekly.reset_index(drop=True), schedule_missing.reset_index(drop=True), def_missing.reset_index(drop=True)


def rollup_stage(weekly: pd.DataFrame, *, include_postseason: bool, season_level: bool) -> pd.DataFrame:
    data = weekly.copy()
    if not include_postseason:
        data = data[data["season_type"].eq("REG")]
    keys = ["NFL_player_id", "year"] if season_level else ["NFL_player_id"]
    grouped = data.groupby(keys, dropna=False)[list(TEAM_MARGIN_COLUMNS)].sum().reset_index()
    for col in TEAM_MARGIN_COLUMNS:
        grouped[col] = grouped[col].round(4)
    return grouped


def build_stage_tables(master_schedule: Path, pbp_cache: Path) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    historical = load_historical_schedule(master_schedule)
    modern = load_pbp_schedule(pbp_cache)
    schedule = pd.concat([historical, modern], ignore_index=True)
    schedule = add_team_margin_columns(schedule)

    def_rows = load_def_rows()
    weekly, schedule_missing, def_missing = build_weekly_stage(schedule, def_rows)

    tables = {
        WEEKLY_STAGE_TABLE: weekly,
        SEASON_STAGE_TABLE: rollup_stage(weekly, include_postseason=False, season_level=True),
        SEASON_ALL_STAGE_TABLE: rollup_stage(weekly, include_postseason=True, season_level=True),
        CAREER_STAGE_TABLE: rollup_stage(weekly, include_postseason=False, season_level=False),
        CAREER_ALL_STAGE_TABLE: rollup_stage(weekly, include_postseason=True, season_level=False),
    }
    summary = {
        "schedule_rows": len(schedule),
        "historical_schedule_rows": len(historical),
        "pbp_schedule_rows": len(modern),
        "def_rows": len(def_rows),
        "weekly_stage_rows": len(weekly),
        "schedule_unmatched_rows": len(schedule_missing),
        "def_unmatched_rows": len(def_missing),
    }
    if not schedule_missing.empty:
        year_counts = schedule_missing.groupby("year").size().sort_values(ascending=False).head(8)
        print("[match] top schedule rows without DEF match:", year_counts.to_dict(), flush=True)
    if not def_missing.empty:
        year_counts = def_missing.groupby("year").size().sort_values(ascending=False).head(8)
        print("[match] top DEF rows without schedule match:", year_counts.to_dict(), flush=True)
    return tables, summary


def write_stage_db(tables: dict[str, pd.DataFrame], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        for table, df in tables.items():
            conn.register("_stage_df", df)
            conn.execute(f"CREATE OR REPLACE TABLE public.{q_ident(table)} AS SELECT * FROM _stage_df")
            conn.unregister("_stage_df")
            count = conn.execute(f"SELECT COUNT(*) FROM public.{q_ident(table)}").fetchone()[0]
            print(f"[stage-db] {table}: {count:,} rows", flush=True)
    finally:
        conn.close()
    print(f"[stage-db] wrote {path}", flush=True)


def execute(writer: FlyWriter, sql: str, *, dry_run: bool) -> list[dict]:
    if dry_run:
        print(sql.strip()[:2200], flush=True)
        return []
    return writer.execute(sql, database="___ops")


def upload_stage_db(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        print(f"[stage-db] dry-run upload {path} as {LEAGUES_STAGE_DB}", flush=True)
        return
    result = FlyTarget().merge_league(LEAGUES_STAGE_DB, path)
    print(f"[stage-db] uploaded via merge-league: {result}", flush=True)


def add_target_columns(writer: FlyWriter, *, dry_run: bool) -> None:
    statements = [
        f"ALTER TABLE {SUPER_TABLE} ADD COLUMN IF NOT EXISTS {q_ident(col)} DOUBLE;" for col in TEAM_MARGIN_COLUMNS
    ]
    for table, _, _ in AGGREGATE_TARGETS:
        statements.extend(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {q_ident(col)} DOUBLE;" for col in TEAM_MARGIN_COLUMNS
        )
    execute(writer, "\n".join(statements), dry_run=dry_run)


def prepare_ops_stage_from_leagues(writer: FlyWriter, *, dry_run: bool) -> None:
    table_cols = {
        WEEKLY_STAGE_TABLE: (
            "player_week",
            "NFL_player_id",
            "year",
            "week",
            "season_type",
            "nfl_team",
            "opponent_nfl_team",
            "team_pts",
            "opp_pts",
            *TEAM_MARGIN_COLUMNS,
        ),
        SEASON_STAGE_TABLE: ("NFL_player_id", "year", *TEAM_MARGIN_COLUMNS),
        SEASON_ALL_STAGE_TABLE: ("NFL_player_id", "year", *TEAM_MARGIN_COLUMNS),
        CAREER_STAGE_TABLE: ("NFL_player_id", *TEAM_MARGIN_COLUMNS),
        CAREER_ALL_STAGE_TABLE: ("NFL_player_id", *TEAM_MARGIN_COLUMNS),
    }
    statements = [f"DROP TABLE IF EXISTS {stage};" for stage in OPS_STAGE_TABLES.values()]
    statements.append(f"ATTACH IF NOT EXISTS '{REMOTE_LEAGUES_PATH}' AS stage_leagues (READ_ONLY);")
    for source_table, stage_table in OPS_STAGE_TABLES.items():
        cols = ", ".join(q_ident(col) for col in table_cols[source_table])
        statements.append(
            f"""
            CREATE OR REPLACE TABLE {stage_table} AS
            SELECT {cols}
            FROM stage_leagues.public.{q_ident(source_table)}
            WHERE db_name = {q_literal(LEAGUES_STAGE_DB)};
            """
        )
    statements.append("DETACH stage_leagues;")
    execute(writer, "\n".join(statements), dry_run=dry_run)


def update_table_with_stage(table: str, stage_table: str, join_sql: str, *, dry_run: bool, writer: FlyWriter) -> None:
    set_sql = ",\n            ".join(f"{q_ident(col)} = COALESCE(st.{q_ident(col)}, 0)" for col in TEAM_MARGIN_COLUMNS)
    zero_sql = ",\n            ".join(f"{q_ident(col)} = COALESCE({q_ident(col)}, 0)" for col in TEAM_MARGIN_COLUMNS)
    sql = f"""
        UPDATE {table} AS t
        SET
            {set_sql}
        FROM {stage_table} AS st
        WHERE {join_sql}
          AND t.NFL_player_id LIKE 'DEF-%';

        UPDATE {table}
        SET
            {zero_sql}
        WHERE NFL_player_id LIKE 'DEF-%';
    """
    execute(writer, sql, dry_run=dry_run)


def apply_weekly_updates(writer: FlyWriter, *, dry_run: bool) -> None:
    update_table_with_stage(
        SUPER_TABLE,
        OPS_STAGE_TABLES[WEEKLY_STAGE_TABLE],
        "t.player_week = st.player_week",
        dry_run=dry_run,
        writer=writer,
    )


def apply_aggregate_updates(writer: FlyWriter, *, dry_run: bool) -> None:
    for target_table, stage_key, season_level in AGGREGATE_TARGETS:
        join_sql = (
            "t.NFL_player_id = st.NFL_player_id AND t.year = st.year"
            if season_level
            else "t.NFL_player_id = st.NFL_player_id"
        )
        update_table_with_stage(
            target_table,
            OPS_STAGE_TABLES[stage_key],
            join_sql,
            dry_run=dry_run,
            writer=writer,
        )


def verify(writer: FlyWriter) -> tuple[list[dict], list[dict]]:
    weekly = writer.execute(
        f"""
        SELECT
            year,
            week,
            player,
            nfl_team,
            opponent_nfl_team,
            points_allowed,
            pts_def_team_pts,
            pts_def_team_margin,
            pts_def_team_win,
            pts_def_team_loss,
            pts_def_team_win_margin_25p,
            pts_def_team_loss_margin_25p
        FROM {SUPER_TABLE}
        WHERE year = 2025
          AND week = 3
          AND NFL_player_id LIKE 'DEF-%'
          AND nfl_team IN ('NO', 'SEA')
        ORDER BY nfl_team
        """,
        database="___ops",
    )
    metric_cols = ",\n            ".join(
        f"ROUND(SUM(COALESCE({q_ident(col)}, 0)), 4) AS {q_ident(col)}"
        for col in (
            "pts_def_team_win",
            "pts_def_team_loss",
            "pts_def_team_tie",
            "pts_def_team_pts",
            "pts_def_team_margin",
        )
    )
    branches = [
        f"""
        SELECT {q_literal('weekly')} AS table_name, COUNT(*) AS rows, {metric_cols}
        FROM {SUPER_TABLE}
        WHERE NFL_player_id LIKE 'DEF-%'
        """
    ]
    for table, _, _ in AGGREGATE_TARGETS:
        branches.append(
            f"SELECT {q_literal(table)} AS table_name, COUNT(*) AS rows, {metric_cols} FROM {table} WHERE NFL_player_id LIKE 'DEF-%'"
        )
    totals = writer.execute("\nUNION ALL\n".join(branches), database="___ops")
    return weekly, totals


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply DST team result/score/margin columns to Fly ___ops.")
    parser.add_argument("--master-schedule", type=Path, default=DEFAULT_MASTER_SCHEDULE)
    parser.add_argument("--pbp-cache", type=Path, default=DEFAULT_PBP_CACHE)
    parser.add_argument("--stage-db", type=Path, default=DEFAULT_STAGE_DB)
    parser.add_argument("--machine-id", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-prepare", action="store_true")
    args = parser.parse_args()

    load_env()
    if not args.skip_build:
        tables, summary = build_stage_tables(args.master_schedule, args.pbp_cache)
        print(f"[summary] {summary}", flush=True)
        write_stage_db(tables, args.stage_db)
    if args.build_only:
        return 0

    writer = FlyWriter(machine_id=args.machine_id or os.environ.get("FLY_PRIMARY_MACHINE_ID") or None)
    add_target_columns(writer, dry_run=args.dry_run)
    if not args.skip_upload:
        upload_stage_db(args.stage_db, dry_run=args.dry_run)
    if not args.skip_prepare:
        prepare_ops_stage_from_leagues(writer, dry_run=args.dry_run)
    apply_weekly_updates(writer, dry_run=args.dry_run)
    apply_aggregate_updates(writer, dry_run=args.dry_run)

    if not args.dry_run:
        sample, totals = verify(writer)
        print("[verify] 2025 W3 NO/SEA:", sample, flush=True)
        print("[verify] totals:", totals, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
