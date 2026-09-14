"""Canonical transaction schema — all platforms normalized."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from multi_league.core.join_keys import ensure_cumulative_week_column
from multi_league.core.manager_identity import hidden_manager_guid_mask
from multi_league.core.trade_utils import duplicate_trade_rows

TRANSACTION_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Identity ===
    ("transaction_id", "VARCHAR", "api"),
    ("transaction_sequence", "INTEGER", "api"),  # order within same transaction_id (add=0, drop=1)
    ("year", "INTEGER", "api"),
    ("week", "INTEGER", "api"),
    ("timestamp", "BIGINT", "api"),  # unix timestamp (seconds)
    ("transaction_datetime", "VARCHAR", "api"),  # human readable: 2025-09-02 02:23:48
    ("status", "VARCHAR", "api"),  # successful, complete, failed, vetoed
    ("transaction_type", "VARCHAR", "api"),  # add, drop, add/drop, trade, trade_pick, commish
    ("platform", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    # === Week date range ===
    ("week_start", "VARCHAR", "api"),  # date string: 2025-09-04
    ("week_end", "VARCHAR", "api"),  # date string: 2025-09-08
    # === Manager (who made the transaction) ===
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("team_name", "VARCHAR", "api"),
    # === Player (NULL for trade_pick type) ===
    ("player", "VARCHAR", "api"),  # player name, or "2025 Round 2 Pick" for traded picks
    ("position", "VARCHAR", "api"),
    ("nfl_team_api", "VARCHAR", "api"),
    ("yahoo_player_id", "VARCHAR", "api"),
    ("sleeper_player_id", "VARCHAR", "api"),
    ("espn_player_id", "VARCHAR", "api"),
    ("fleaflicker_player_id", "VARCHAR", "api"),
    # === Transaction details ===
    ("faab_bid", "DOUBLE", "api"),  # FAAB spent on waiver claim
    ("faab_traded", "DOUBLE", "api"),  # FAAB dollars exchanged in trade (Sleeper)
    ("source_type", "VARCHAR", "api"),  # waivers, freeagents, team
    ("destination", "VARCHAR", "api"),  # team, waivers, freeagents
    ("waiver_seq", "INTEGER", "api"),  # waiver priority at time of claim (Sleeper)
    ("notes", "VARCHAR", "api"),  # metadata notes (Sleeper: "claimed by another")
    ("is_estimated", "BOOLEAN", "api"),  # ESPN pre-2019 estimated data flag
    # === Trade source/destination (full manager info, NULL for non-trades) ===
    ("source_manager", "VARCHAR", "api"),  # trade: who sent the player
    ("source_manager_guid", "VARCHAR", "api"),
    ("source_team_name", "VARCHAR", "api"),
    ("source_franchise_id", "VARCHAR", "join_key"),
    ("trade_direction", "VARCHAR", "api"),  # trade perspective: received or sent
    ("destination_manager", "VARCHAR", "api"),  # trade: who received the player
    ("destination_manager_guid", "VARCHAR", "api"),
    ("destination_team_name", "VARCHAR", "api"),
    ("destination_franchise_id", "VARCHAR", "join_key"),
    # === Sleeper traded draft picks (NULL for non-pick trades) ===
    ("traded_pick_season", "INTEGER", "api"),  # which season's pick was traded
    ("traded_pick_round", "INTEGER", "api"),  # which round
    ("traded_pick_original_owner", "VARCHAR", "api"),  # who originally owned the pick
    # === ESPN-specific ===
    ("trade_accept_date", "BIGINT", "api"),  # ESPN trade acceptance timestamp
    # === SQL enrichments ===
    ("NFL_player_id", "VARCHAR", "sql"),
    ("nfl_team", "VARCHAR", "sql"),
    ("transaction_lamar", "DOUBLE", "sql"),
    ("trade_grade", "VARCHAR", "sql"),
    ("trade_asset_lamar", "DOUBLE", "sql"),
    ("trade_net_lamar", "DOUBLE", "sql"),
    # === Join key ===
    ("cumulative_week", "BIGINT", "join_key"),  # year * 100 + week
    # === SQL enrichments: player performance context (Group 3) ===
    ("points_at_transaction", "DOUBLE", "sql"),  # cumulative season pts BEFORE transaction week
    ("lamar_at_transaction", "DOUBLE", "sql"),
    ("ppg_before_transaction", "DOUBLE", "sql"),
    ("weeks_before", "INTEGER", "sql"),
    ("ppg_after_transaction", "DOUBLE", "sql"),
    ("total_points_after_4wks", "DOUBLE", "sql"),
    ("weeks_after", "INTEGER", "sql"),
    # ROS managed (while on manager's roster)
    ("total_points_ros_managed", "DOUBLE", "sql"),
    ("ppg_ros_managed", "DOUBLE", "sql"),
    ("weeks_ros_managed", "INTEGER", "sql"),
    ("player_lamar_ros_managed", "DOUBLE", "sql"),
    ("manager_lamar_ros_managed", "DOUBLE", "sql"),
    ("manager_lamar_per_game_managed", "DOUBLE", "sql"),
    ("replacement_ppg_ros_managed", "DOUBLE", "sql"),
    # ROS total (all games regardless of roster)
    ("total_points_ros_total", "DOUBLE", "sql"),
    ("ppg_ros_total", "DOUBLE", "sql"),
    ("weeks_ros_total", "INTEGER", "sql"),
    ("player_lamar_ros_total", "DOUBLE", "sql"),
    ("player_lamar_per_game_total", "DOUBLE", "sql"),
    ("manager_lamar_ros_total", "DOUBLE", "sql"),
    ("replacement_ppg_ros_total", "DOUBLE", "sql"),
    # === SQL enrichments: transaction scoring (Group 4) ===
    ("fa_lamar_ros", "DOUBLE", "sql"),
    ("transaction_score", "DOUBLE", "sql"),
    ("transaction_grade", "VARCHAR", "sql"),
    ("score_percentile", "DOUBLE", "sql"),
    # === SQL enrichments: trade grading (Group 5) ===
    ("trade_percentile", "DOUBLE", "sql"),
    # === SQL enrichments: engagement metrics (Group 6) ===
    ("faab_value_tier", "VARCHAR", "sql"),
    ("timing_category", "VARCHAR", "sql"),
    ("pickup_type", "VARCHAR", "sql"),
    # === SQL enrichments: draft pick conveyances (Group 7) ===
    ("conveyed_player", "VARCHAR", "sql"),
    ("conveyed_lamar", "DOUBLE", "sql"),
    ("conveyed_fantasy_points", "DOUBLE", "sql"),
    ("conveyed_year", "INTEGER", "sql"),
    ("is_conveyed", "BOOLEAN", "sql"),
]

RAW_COLUMNS = [name for name, _, src in TRANSACTION_SCHEMA if src in ("api", "join_key")]
SQL_COLUMNS = [name for name, _, src in TRANSACTION_SCHEMA if src == "sql"]
ALL_COLUMNS = [name for name, _, _ in TRANSACTION_SCHEMA]
COLUMN_TYPES = {name: dtype for name, dtype, _ in TRANSACTION_SCHEMA}


def _has_populated_trade_destination_columns(df) -> bool:
    """Detect raw trade rows that still need per-party expansion."""
    if df is None or df.empty or "transaction_type" not in df.columns:
        return False

    trade_mask = df["transaction_type"].fillna("").astype(str).str.lower().isin({"trade", "trade_pick"})
    if not trade_mask.any():
        return False

    indicator_cols = [
        "destination_manager",
        "destination_manager_guid",
        "destination_team_name",
        "destination_franchise_id",
    ]
    for col in indicator_cols:
        if col not in df.columns:
            continue
        values = df.loc[trade_mask, col]
        if values.notna().any() and values.astype(str).str.strip().ne("").any():
            return True
    return False


def normalize_transaction_df(df, platform: str, league_id: str | None = None) -> pd.DataFrame:
    """Normalize a transaction DataFrame to canonical schema."""
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

    rename_map = {
        "player_name": "player",
        "nfl_team": "nfl_team_api",
        "player_id": platform_id_col,
        "player_key": "yahoo_player_id",
        "human_readable_timestamp": "transaction_datetime",
        "partner": "source_manager",
        "partner_guid": "source_manager_guid",
        "partner_franchise_id": "source_franchise_id",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    if "nfl_team" in df.columns and "nfl_team_api" not in df.columns:
        df = df.rename(columns={"nfl_team": "nfl_team_api"})

    # Convert timestamp to numeric, normalize ms → seconds FIRST
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        ts = df["timestamp"].dropna()
        if not ts.empty and ts.iloc[0] > 1e12:  # milliseconds → seconds
            df["timestamp"] = (df["timestamp"] / 1000).round().astype("Int64")

    # Build transaction_datetime from normalized timestamp
    if "transaction_datetime" not in df.columns or df["transaction_datetime"].isna().all():
        if "timestamp" in df.columns:
            import datetime

            df["transaction_datetime"] = df["timestamp"].apply(
                lambda ts: datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                if pd.notna(ts) and ts
                else None
            )

    # Platform IDs to VARCHAR
    for col in ["yahoo_player_id", "sleeper_player_id", "espn_player_id", "fleaflicker_player_id", "transaction_id"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace(r"\.0$", "", regex=True).replace({"None": None, "nan": None, "": None})
            )

    # Join keys
    for id_col in ["franchise_id", "source_franchise_id", "destination_franchise_id"]:
        if id_col in df.columns:
            hidden_id = hidden_manager_guid_mask(df[id_col])
            if hidden_id.any():
                df.loc[hidden_id, id_col] = None

    def _fill_guid_backed_id(id_col: str, guid_col: str) -> None:
        if id_col not in df.columns:
            df[id_col] = None
        if guid_col in df.columns:
            missing_id = df[id_col].isna() | (df[id_col].astype(str).str.strip() == "")
            valid_guid = ~hidden_manager_guid_mask(df[guid_col])
            fill_mask = missing_id & valid_guid
            df.loc[fill_mask, id_col] = (
                df.loc[fill_mask, guid_col]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
                .replace({"None": None, "nan": None, "": None})
            )

    _fill_guid_backed_id("franchise_id", "manager_guid")
    _fill_guid_backed_id("source_franchise_id", "source_manager_guid")
    _fill_guid_backed_id("destination_franchise_id", "destination_manager_guid")

    if _has_populated_trade_destination_columns(df):
        df = duplicate_trade_rows(df)
        for id_col in ["franchise_id", "source_franchise_id", "destination_franchise_id"]:
            if id_col in df.columns:
                hidden_id = hidden_manager_guid_mask(df[id_col])
                if hidden_id.any():
                    df.loc[hidden_id, id_col] = None

    # Normalize DEF player names: "Bears" → "Bears DST" (canonical format)
    if "position" in df.columns and "player" in df.columns:
        from multi_league.data_fetchers.shared.clean_names import normalize_def_player_name

        def_mask = df["position"].fillna("").astype(str).str.upper().isin({"DEF", "DST", "D/ST"})
        for idx in df.index[def_mask]:
            normalized, _ = normalize_def_player_name(str(df.at[idx, "player"]))
            if normalized:
                df.at[idx, "player"] = normalized

    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = ensure_cumulative_week_column(df)

    if "transaction_id" in df.columns and "transaction_sequence" in df.columns:
        seq = pd.to_numeric(df["transaction_sequence"], errors="coerce")
        tx_id = df["transaction_id"].astype("string").str.strip()
        duplicate_sequence = (
            tx_id.notna()
            & tx_id.ne("")
            & seq.notna()
            & pd.DataFrame({"transaction_id": tx_id, "transaction_sequence": seq}).duplicated(
                ["transaction_id", "transaction_sequence"], keep=False
            )
        )
        if seq.isna().any() or duplicate_sequence.any():
            ordering_cols = [
                col
                for col in (
                    "transaction_id",
                    "timestamp",
                    "transaction_datetime",
                    "transaction_type",
                    "manager",
                    "player",
                    "yahoo_player_id",
                    "sleeper_player_id",
                    "espn_player_id",
                    "fleaflicker_player_id",
                    "source_manager",
                    "destination_manager",
                )
                if col in df.columns
            ]
            ordered = df.assign(_original_order=range(len(df))).sort_values(
                ordering_cols + ["_original_order"], kind="mergesort", na_position="last"
            )
            reassigned = ordered.groupby("transaction_id", dropna=False).cumcount()
            df.loc[ordered.index, "transaction_sequence"] = reassigned.to_numpy()

    preserved_sql_cols = [c for c in ["NFL_player_id"] if c in df.columns]
    cols_to_drop = [c for c in SQL_COLUMNS if c in df.columns and c not in preserved_sql_cols]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    for col in ["year", "week", "timestamp", "cumulative_week"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if "faab_bid" in df.columns:
        df["faab_bid"] = pd.to_numeric(df["faab_bid"], errors="coerce")

    return df[db_name_cols + [c for c in RAW_COLUMNS if c in df.columns] + preserved_sql_cols]


def create_transaction_table_sql(database_name: str) -> str:
    cols = [f'    "{name}" {dtype}' for name, dtype, _ in TRANSACTION_SCHEMA]
    cols[0] = '    "db_name" VARCHAR NOT NULL'
    return f'CREATE TABLE IF NOT EXISTS "{database_name}".public.transactions (\n' + ",\n".join(cols) + "\n)"


# Canonical transaction upload is handled by
# ``LocalLeagueDB.upload_to_fly()`` (DELETE WHERE db_name -> INSERT).
