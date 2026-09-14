"""Canonical schedule schema — all platforms normalized.

Schedule is the season week-by-week structure: who plays who, when are
playoffs, what's the matchup order. Most data overlaps with matchup table
but schedule is the authoritative source for playoff structure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from multi_league.core.join_keys import (
    build_manager_week_series,
    ensure_cumulative_week_column,
    manager_identity_series,
)
from multi_league.core.manager_identity import hidden_manager_guid_mask


SCHEDULE_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Identity ===
    ("year", "INTEGER", "api"),
    ("week", "INTEGER", "api"),
    ("cumulative_week", "BIGINT", "join_key"),
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("franchise_name", "VARCHAR", "enrichment"),
    ("team_name", "VARCHAR", "api"),
    ("platform", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    # === Composite keys ===
    ("manager_week", "VARCHAR", "join_key"),
    ("manager_year", "VARCHAR", "join_key"),
    # === Opponent ===
    ("opponent", "VARCHAR", "api"),
    ("opponent_guid", "VARCHAR", "api"),
    ("opponent_franchise_id", "VARCHAR", "join_key"),
    ("opponent_week", "VARCHAR", "api"),
    ("opponent_year", "VARCHAR", "api"),
    # === Scores ===
    ("team_points", "DOUBLE", "api"),
    ("opponent_points", "DOUBLE", "api"),
    ("win", "INTEGER", "api"),
    ("loss", "INTEGER", "api"),
    # === Postseason flags ===
    ("is_playoffs", "INTEGER", "api"),
    ("is_consolation", "INTEGER", "api"),
    ("postseason", "INTEGER", "enrichment"),
    ("playoff_round", "VARCHAR", "enrichment"),
    ("playoff_round_num", "INTEGER", "enrichment"),
    ("playoff_week_index", "INTEGER", "enrichment"),
    # === Bracket flags ===
    ("quarterfinal", "INTEGER", "enrichment"),
    ("semifinal", "INTEGER", "enrichment"),
    ("championship", "INTEGER", "enrichment"),
    ("consolation_round", "VARCHAR", "enrichment"),
    ("consolation_semifinal", "INTEGER", "enrichment"),
    ("consolation_final", "INTEGER", "enrichment"),
    ("placement_game", "INTEGER", "enrichment"),
    # === Awards ===
    ("champion", "INTEGER", "enrichment"),
    ("sacko", "INTEGER", "enrichment"),
    ("placement_rank", "INTEGER", "enrichment"),
]

RAW_COLUMNS = [name for name, _, src in SCHEDULE_SCHEMA if src in ("api", "join_key")]
SQL_COLUMNS = [name for name, _, src in SCHEDULE_SCHEMA if src == "sql"]
ALL_COLUMNS = [name for name, _, _ in SCHEDULE_SCHEMA]
COLUMN_TYPES = {name: dtype for name, dtype, _ in SCHEDULE_SCHEMA}


def normalize_schedule_df(df, platform: str, league_id: str | None = None) -> pd.DataFrame:
    """Normalize a schedule DataFrame to canonical schema."""
    import pandas as pd

    db_name_cols = [c for c in ["db_name"] if df is not None and c in df.columns]
    if df is None or df.empty:
        return pd.DataFrame(columns=db_name_cols + RAW_COLUMNS)

    df = df.copy()
    df["platform"] = platform

    rename_map = {
        "manager_name": "manager",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})

    # Join keys
    for id_col in ["franchise_id", "opponent_franchise_id"]:
        if id_col in df.columns:
            hidden_id = hidden_manager_guid_mask(df[id_col])
            if hidden_id.any():
                df.loc[hidden_id, id_col] = None

    if "franchise_id" not in df.columns or df["franchise_id"].isna().all():
        if "manager_guid" in df.columns:
            df["franchise_id"] = None
            valid_guid = ~hidden_manager_guid_mask(df["manager_guid"])
            df.loc[valid_guid, "franchise_id"] = df.loc[valid_guid, "manager_guid"].astype(str).str.strip()

    if "opponent_franchise_id" not in df.columns or df["opponent_franchise_id"].isna().all():
        if "opponent_guid" in df.columns:
            df["opponent_franchise_id"] = None
            valid_opp_guid = ~hidden_manager_guid_mask(df["opponent_guid"])
            df.loc[valid_opp_guid, "opponent_franchise_id"] = (
                df.loc[valid_opp_guid, "opponent_guid"].astype(str).str.strip()
            )

    if "manager_week" not in df.columns or df["manager_week"].isna().all():
        if all(c in df.columns for c in ["franchise_id", "year", "week"]):
            df["manager_week"] = (
                df["franchise_id"].astype(str) + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)
            )

    if "manager_year" not in df.columns or df["manager_year"].isna().all():
        if all(c in df.columns for c in ["franchise_id", "year"]):
            df["manager_year"] = df["franchise_id"].astype(str) + "_" + df["year"].astype(str)

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

    if "manager_year" in df.columns and "year" in df.columns:
        missing_manager_year = (
            df["manager_year"].isna()
            | df["manager_year"].astype(str).str.strip().isin({"", "None", "nan", "<NA>"})
            | df["manager_year"].astype(str).str.startswith("None_")
            | df["manager_year"].astype(str).str.startswith("nan_")
        )
        if missing_manager_year.any():
            identity = manager_identity_series(df)
            year_values = pd.to_numeric(df["year"], errors="coerce").round().astype("Int64").astype("string")
            manager_year = (identity + "_" + year_values).astype("string")
            df.loc[missing_manager_year, "manager_year"] = manager_year.loc[missing_manager_year]

    cols_to_drop = [c for c in SQL_COLUMNS if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    for col in ["year", "week", "cumulative_week"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df[db_name_cols + [c for c in RAW_COLUMNS if c in df.columns]]


def create_schedule_table_sql(database_name: str) -> str:
    cols = [f'    "{name}" {dtype}' for name, dtype, _ in SCHEDULE_SCHEMA]
    cols[0] = '    "db_name" VARCHAR NOT NULL'
    return f'CREATE TABLE IF NOT EXISTS "{database_name}".public.schedule (\n' + ",\n".join(cols) + "\n)"


# Canonical schedule upload is handled by
# ``LocalLeagueDB.upload_to_fly()`` (DELETE WHERE db_name -> INSERT).
