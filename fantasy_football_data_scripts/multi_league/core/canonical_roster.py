"""Canonical roster (player_fantasy) schema — all platforms normalized.

Fetchers return raw API data + join keys only. NFL_player_id resolution,
nfl_team correction, and all advanced metrics are SQL enrichments.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from multi_league.core.join_keys import build_manager_week_series, ensure_cumulative_week_column
from multi_league.core.manager_identity import hidden_manager_guid_mask
from multi_league.core.roster_slots import resolve as resolve_position


# ---------------------------------------------------------------------------
# Schema: (column_name, duckdb_type, source)
#   "api"      = raw from platform API
#   "join_key" = fetcher builds from API data
#   "sql"      = SQL enrichment after upload
# ---------------------------------------------------------------------------

ROSTER_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Identity (fetcher provides) ===
    ("year", "INTEGER", "api"),
    ("week", "INTEGER", "api"),
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("manager_week", "VARCHAR", "join_key"),
    ("team_key", "VARCHAR", "api"),
    ("team_name", "VARCHAR", "api"),
    ("platform", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    # === Player identity (fetcher provides raw, enrichments resolve canonical) ===
    ("player", "VARCHAR", "api"),
    ("position", "VARCHAR", "api"),  # NFL position from API
    ("fantasy_position", "VARCHAR", "api"),  # roster slot (QB, RB, FLEX, BN, IR, TAXI)
    ("eligible_positions", "VARCHAR", "api"),  # comma-separated eligible slots
    ("nfl_team_api", "VARCHAR", "api"),  # raw team from API (unreliable for Yahoo historical)
    # === Platform player IDs (one populated, others NULL) ===
    ("yahoo_player_id", "VARCHAR", "api"),
    ("sleeper_player_id", "VARCHAR", "api"),
    ("espn_player_id", "VARCHAR", "api"),
    ("fleaflicker_player_id", "VARCHAR", "api"),
    ("mfl_player_id", "VARCHAR", "api"),
    # === Scoring (fetcher provides raw) ===
    ("fantasy_points", "DOUBLE", "api"),  # actual points from API
    ("projected_points", "DOUBLE", "api"),  # ESPN 2019+ only, NULL otherwise
    ("is_started", "INTEGER", "api"),  # 1=starter, 0=bench/IR/TAXI
    ("is_rostered", "INTEGER", "api"),  # 1=real manager roster, 0=unrostered/free agent
    # === SQL enrichments (computed after upload) ===
    ("NFL_player_id", "VARCHAR", "sql"),  # from player_bio via platform ID
    ("player_week", "VARCHAR", "sql"),  # {NFL_player_id}_{year}_{week}
    ("nfl_team", "VARCHAR", "sql"),  # from super_table (correct per-week team)
    ("cumulative_week", "BIGINT", "join_key"),  # year * 100 + week
]

# Convenience lookups
RAW_COLUMNS = [name for name, _, src in ROSTER_SCHEMA if src in ("api", "join_key")]
SQL_COLUMNS = [name for name, _, src in ROSTER_SCHEMA if src == "sql"]
ALL_COLUMNS = [name for name, _, _ in ROSTER_SCHEMA]
COLUMN_TYPES = {name: dtype for name, dtype, _ in ROSTER_SCHEMA}

# Non-starter roster slots
BENCH_SLOTS = {"BN", "IR", "IL", "TAXI", "RESERVE", "RES", "COVID", "PUP", "INJ", "NA"}
UNROSTERED_MANAGER_VALUES = {"unrostered", "fa", "free agent", "waivers", "", "none", "nan"}


# ---------------------------------------------------------------------------
# DataFrame normalization
# ---------------------------------------------------------------------------


def normalize_roster_df(df, platform: str, league_id: str | None = None) -> pd.DataFrame:
    """Normalize a roster DataFrame to canonical schema.

    - Renames platform-specific columns to canonical names
    - Builds join keys (franchise_id, manager_week)
    - Computes is_started from fantasy_position
    - Adds missing columns as NULL
    - Drops any SQL columns a fetcher accidentally computed
    """
    import pandas as pd

    db_name_cols = [c for c in ["db_name"] if df is not None and c in df.columns]
    if df is None or df.empty:
        return pd.DataFrame(columns=db_name_cols + RAW_COLUMNS)

    df = df.copy()
    df["platform"] = platform
    if league_id and ("league_id" not in df.columns or df["league_id"].isna().all()):
        df["league_id"] = league_id

    platform_id_col = {
        "sleeper": "sleeper_player_id",
        "espn": "espn_player_id",
        "fleaflicker": "fleaflicker_player_id",
    }.get((platform or "").lower(), "yahoo_player_id")

    # Platform-specific renames
    rename_map = {
        # Yahoo
        "player_name": "player",
        "yahoo_position": "position",
        "player_id": platform_id_col,
        "player_key": "yahoo_player_id",  # fallback
        "image_url": "team_logo",
        "nfl_team": "nfl_team_api",
        "manager_name": "manager",
        # Sleeper
        "points": "fantasy_points",
        "nfl_position": "position",
        # ESPN
        "espn_player_id_original": "espn_player_id",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # If nfl_team exists but nfl_team_api doesn't, rename
    if "nfl_team" in df.columns and "nfl_team_api" not in df.columns:
        df = df.rename(columns={"nfl_team": "nfl_team_api"})

    # Ensure platform IDs are VARCHAR (not float/int)
    for col in ["yahoo_player_id", "sleeper_player_id", "espn_player_id", "fleaflicker_player_id", "mfl_player_id"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(r"\.0$", "", regex=True).replace({"None": None, "nan": None, "": None})
            )

    # Build join keys
    if "franchise_id" not in df.columns:
        df["franchise_id"] = None
    else:
        hidden_franchise = hidden_manager_guid_mask(df["franchise_id"])
        if hidden_franchise.any():
            df.loc[hidden_franchise, "franchise_id"] = None
    if "manager_guid" in df.columns:
        missing_franchise = df["franchise_id"].isna() | (df["franchise_id"].astype(str).str.strip() == "")
        valid_guid = ~hidden_manager_guid_mask(df["manager_guid"])
        fill_mask = missing_franchise & valid_guid
        df.loc[fill_mask, "franchise_id"] = (
            df.loc[fill_mask, "manager_guid"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"None": None, "nan": None, "": None})
        )

    # Normalize fantasy_position to canonical names (FLX, SUPER_FLEX, etc.)
    if "fantasy_position" in df.columns:
        df["fantasy_position"] = df["fantasy_position"].apply(lambda x: resolve_position(x) if pd.notna(x) else x)

    # Compute is_started from fantasy_position
    if "is_started" not in df.columns or df["is_started"].isna().all():
        if "fantasy_position" in df.columns:
            df["is_started"] = df["fantasy_position"].apply(lambda x: 0 if str(x).upper() in BENCH_SLOTS else 1)

    # Preserve explicit roster status from the fetcher, but infer it from manager
    # whenever the upstream payload omitted it. This keeps bench_lamar and other
    # roster-only enrichments deterministic across platforms.
    if "manager" in df.columns:
        manager_norm = df["manager"].astype(str).str.strip().str.lower()
        inferred_is_rostered = (~manager_norm.isin(UNROSTERED_MANAGER_VALUES)).astype("Int64")
        if "is_rostered" not in df.columns:
            df["is_rostered"] = inferred_is_rostered
        else:
            df["is_rostered"] = df["is_rostered"].where(df["is_rostered"].notna(), inferred_is_rostered)
    elif "is_rostered" not in df.columns:
        df["is_rostered"] = 1

    preserved_sql_cols: list[str] = []

    # Sleeper team defenses use team abbreviations as player IDs and won't resolve
    # through player_bio, so synthesize canonical DEF IDs before upload.
    if (platform or "").lower() == "sleeper":
        if "NFL_player_id" not in df.columns:
            df["NFL_player_id"] = None
        if all(col in df.columns for col in ["year", "week"]):
            try:
                from nfl_data.nfl_franchises import get_def_player_id

                def is_def_row(row) -> bool:
                    pos = str(row.get("position", "")).upper()
                    if pos in {"DEF", "DST", "D/ST"}:
                        return True
                    sleeper_id = str(row.get("sleeper_player_id", "")).strip().upper()
                    return (
                        sleeper_id.isalpha()
                        and 2 <= len(sleeper_id) <= 3
                        and "DST" in str(row.get("player", "")).upper()
                    )

                def build_def_id(row):
                    team = row.get("nfl_team_api") or row.get("sleeper_player_id")
                    year_val = row.get("year")
                    if pd.isna(team) or pd.isna(year_val):
                        return None
                    return get_def_player_id(str(team).strip().upper(), int(year_val))

                dst_mask = df.apply(is_def_row, axis=1)
                if dst_mask.any():
                    missing_dst_ids = dst_mask & df["NFL_player_id"].isna()
                    if missing_dst_ids.any():
                        df.loc[missing_dst_ids, "NFL_player_id"] = df.loc[missing_dst_ids].apply(build_def_id, axis=1)

                    df["player_week"] = df.get("player_week")
                    missing_player_week = dst_mask & (
                        df["player_week"].isna() | (df["player_week"].astype(str).str.strip() == "")
                    )
                    if missing_player_week.any():
                        df.loc[missing_player_week, "player_week"] = (
                            df.loc[missing_player_week, "NFL_player_id"].astype(str)
                            + "_"
                            + df.loc[missing_player_week, "year"].astype("Int64").astype(str)
                            + "_"
                            + df.loc[missing_player_week, "week"].astype("Int64").astype(str)
                        )
            except Exception:
                pass

    if "NFL_player_id" in df.columns:
        preserved_sql_cols.append("NFL_player_id")
    if "player_week" in df.columns:
        preserved_sql_cols.append("player_week")
    # Add missing raw columns as NULL
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = ensure_cumulative_week_column(df)
    if "manager_week" in df.columns:
        missing_manager_week = (
            df["manager_week"].isna()
            | df["manager_week"].astype(str).str.strip().isin({"", "None", "nan", "<NA>"})
            | df["manager_week"].astype(str).str.startswith("None_")
            | df["manager_week"].astype(str).str.startswith("nan_")
        )
        if missing_manager_week.any():
            df.loc[missing_manager_week, "manager_week"] = build_manager_week_series(df).loc[missing_manager_week]

    # Drop SQL columns a fetcher may have computed
    cols_to_drop = [c for c in SQL_COLUMNS if c in df.columns and c not in preserved_sql_cols]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # Fix types
    for col in ["is_started", "is_rostered"]:
        if col in df.columns:
            bool_map = {
                True: 1,
                False: 0,
                "true": 1,
                "false": 0,
                "True": 1,
                "False": 0,
            }
            df[col] = df[col].apply(lambda value, _bool_map=bool_map: _bool_map.get(value, value))

    for col in ["year", "week", "is_started", "is_rostered", "cumulative_week"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["fantasy_points", "projected_points"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["manager_guid", "franchise_id", "team_key", "manager_week", "league_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"None": None, "nan": None, "": None})

    return df[db_name_cols + [c for c in RAW_COLUMNS if c in df.columns] + preserved_sql_cols]


# ---------------------------------------------------------------------------
# SQL enrichments
# ---------------------------------------------------------------------------


def get_sql_enrichments(database_name: str) -> list[str]:
    """SQL enrichments for player_fantasy — run after upload.

    Order matters:
    1. NFL_player_id resolution (from player_bio)
    2. player_week construction
    3. nfl_team from super_table
    4. team_projected_points → matchup table (from player projections)
    """
    t = f'"{database_name}".public.player_fantasy'
    bio = "___ops.nfl_historical.player_bio"
    nfl = "___ops.nfl_historical.nfl_player_stats_all"
    mt = f'"{database_name}".public.matchup'

    stmts = []

    # 1. NFL_player_id from player_bio — match on platform ID
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS "NFL_player_id" VARCHAR;

        UPDATE {t} SET "NFL_player_id" = bio."NFL_player_id"
        FROM {bio} bio
        WHERE {t}.yahoo_player_id IS NOT NULL
          AND {t}.yahoo_player_id = CAST(CAST(bio.yahoo_player_id AS INTEGER) AS VARCHAR)
          AND COALESCE({t}."NFL_player_id", '') <> bio."NFL_player_id";

        UPDATE {t} SET "NFL_player_id" = bio."NFL_player_id"
        FROM {bio} bio
        WHERE {t}.sleeper_player_id IS NOT NULL
          AND {t}.sleeper_player_id = CAST(CAST(bio.sleeper_player_id AS INTEGER) AS VARCHAR)
          AND COALESCE({t}."NFL_player_id", '') <> bio."NFL_player_id";

        UPDATE {t} SET "NFL_player_id" = bio."NFL_player_id"
        FROM {bio} bio
        WHERE {t}.espn_player_id IS NOT NULL
          AND {t}.espn_player_id = CAST(bio.espn_id AS VARCHAR)
          AND COALESCE({t}."NFL_player_id", '') <> bio."NFL_player_id";
    """)

    # 2. player_week join key
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS player_week VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS player VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS fantasy_position VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS manager_week VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS sleeper_player_id VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS yahoo_player_id VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS espn_player_id VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS fleaflicker_player_id VARCHAR;
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS mfl_player_id VARCHAR;
        UPDATE {t} SET player_week = "NFL_player_id" || '_' || year || '_' || week
        WHERE "NFL_player_id" IS NOT NULL;

        UPDATE {t}
        SET player_week = 'UNMAPPED_'
            || sha256(concat_ws(
                chr(31),
                COALESCE(sleeper_player_id, ''),
                COALESCE(yahoo_player_id, ''),
                COALESCE(espn_player_id, ''),
                COALESCE(fleaflicker_player_id, ''),
                COALESCE(mfl_player_id, ''),
                COALESCE(player, ''),
                COALESCE(manager_week, ''),
                COALESCE(fantasy_position, '')
            ))
            || '_' || year || '_' || week
        WHERE NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
          AND year IS NOT NULL
          AND week IS NOT NULL;
    """)

    # 3. nfl_team from super_table (correct per-week team, not Yahoo's last-team-only)
    stmts.append(f"""
        ALTER TABLE {t} ADD COLUMN IF NOT EXISTS nfl_team VARCHAR;
        UPDATE {t} SET nfl_team = nfl.nfl_team
        FROM (SELECT DISTINCT player_week, nfl_team FROM {nfl} WHERE nfl_team IS NOT NULL) nfl
        WHERE {t}.player_week = nfl.player_week
          AND {t}.player_week IS NOT NULL;
    """)

    # 4. team_projected_points → matchup table (ESPN/any platform with player projections)
    # Two separate updates: team projections, then opponent projections
    stmts.append(f"""
        UPDATE {mt} SET team_projected_points = sub.proj
        FROM (
            SELECT year, week, manager_guid, ROUND(SUM(projected_points), 2) AS proj
            FROM {t}
            WHERE is_started = 1 AND projected_points IS NOT NULL
            GROUP BY year, week, manager_guid
        ) sub
        WHERE {mt}.year = sub.year AND {mt}.week = sub.week
          AND {mt}.manager_guid = sub.manager_guid
          AND {mt}.team_projected_points IS NULL;

        UPDATE {mt} SET opponent_projected_points = sub.proj
        FROM (
            SELECT year, week, manager_guid, ROUND(SUM(projected_points), 2) AS proj
            FROM {t}
            WHERE is_started = 1 AND projected_points IS NOT NULL
            GROUP BY year, week, manager_guid
        ) sub
        WHERE {mt}.year = sub.year AND {mt}.week = sub.week
          AND {mt}.opponent_guid = sub.manager_guid
          AND {mt}.opponent_projected_points IS NULL;
    """)

    return stmts


# ---------------------------------------------------------------------------
# DuckDB persistence
# ---------------------------------------------------------------------------


def create_roster_table_sql(database_name: str) -> str:
    """CREATE TABLE for canonical player_fantasy."""
    cols = []
    for name, dtype, _ in ROSTER_SCHEMA:
        cols.append(f'    "{name}" {dtype} NOT NULL' if name == "db_name" else f'    "{name}" {dtype}')
    return f"""
CREATE TABLE IF NOT EXISTS "{database_name}".public.player_fantasy (
{",".join(chr(10) + c for c in cols)}
)
"""


def upload_rosters(database_name: str, df, conn=None) -> bool:
    """Upload canonical roster DataFrame through the active DuckDB connection.

    Creates table if needed. Upserts by deleting existing year data
    then inserting.
    """
    import logging

    logger = logging.getLogger(__name__)

    if df is None or df.empty:
        logger.warning("[ROSTER] Empty DataFrame, nothing to upload")
        return False
    df = df.copy()
    if "db_name" not in df.columns:
        df["db_name"] = database_name

    close_conn = conn is None
    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        conn.execute(f'CREATE DATABASE IF NOT EXISTS "{database_name}"')

        # Drop old-format table if schema doesn't match
        try:
            old_cols = conn.execute(
                f"SELECT column_name FROM duckdb_columns() "
                f"WHERE database_name = '{database_name}' AND table_name = 'player_fantasy'"
            ).fetchall()
            old_col_names = {r[0] for r in old_cols}
            if old_col_names and "nfl_team_api" not in old_col_names and "fantasy_points" in old_col_names:
                conn.execute(f'DROP TABLE "{database_name}".public.player_fantasy')
                logger.info(f"[ROSTER] Dropped old-format player_fantasy table in {database_name}")
        except Exception:
            pass

        conn.execute(create_roster_table_sql(database_name))

        # Delete existing data for years we're uploading
        years = df["year"].dropna().unique().tolist()
        for yr in years:
            conn.execute(
                f'DELETE FROM "{database_name}".public.player_fantasy WHERE year = ?',
                [int(yr)],
            )

        # Insert
        conn.register("_roster_upload", df)
        insert_cols = ["db_name"] + [c for c in RAW_COLUMNS if c in df.columns]
        col_list = ", ".join([f'"{c}"' for c in insert_cols])
        conn.execute(
            f'INSERT INTO "{database_name}".public.player_fantasy ({col_list}) SELECT {col_list} FROM _roster_upload'
        )
        conn.unregister("_roster_upload")

        logger.info(f"[ROSTER] Uploaded {len(df)} rows to {database_name} ({len(years)} year(s))")
        return True

    except Exception as e:
        logger.error(f"[ROSTER] Upload failed: {e}")
        return False
    finally:
        if close_conn:
            conn.close()
