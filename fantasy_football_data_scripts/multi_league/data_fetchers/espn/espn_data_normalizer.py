"""
ESPN Data Normalizer

Converts ESPN fetcher output to canonical schema for multi-tenant compatibility.
This ensures ESPN data can flow through the same transformation pipeline as
Yahoo and Sleeper data.

Canonical Tables:
- player_fantasy.parquet: Player-week statistics
- matchup.parquet: Weekly matchup results
- draft.parquet: Draft picks and keepers
- transactions.parquet: Trades, pickups, drops

Column Mappings:
- espn_player_id -> player_id (with 'espn_' prefix retained for tracing)
- player stays as player
- position stays as position
- fantasy_points stays as fantasy_points (already named correctly by fetchers)
- fantasy_position stays as fantasy_position
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

# Reuse team/franchise mappings from Sleeper normalizer
from ..sleeper.sleeper_data_normalizer import (
    ABBREV_TO_FRANCHISE_ID,
    normalize_def_records,
)


# Canonical position normalization for ESPN data
# ESPN uses "D/ST" while our schema uses "DEF"
ESPN_POSITION_NORMALIZE = {
    "D/ST": "DEF",
    "DST": "DEF",
    "DF": "DEF",
    "DEF": "DEF",
    "QB": "QB",
    "RB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
}


def _normalize_position_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Normalize a position column: D/ST→DEF, strip whitespace, upper-case."""
    if col not in df.columns:
        return df
    df[col] = df[col].fillna("").astype(str).str.upper().str.strip().replace(ESPN_POSITION_NORMALIZE)
    # Replace empty strings with None
    df.loc[df[col] == "", col] = None
    return df


def log(msg: str):
    logger.info(msg)
    print(msg)


# Cache for ESPN ID -> NFL_player_id mapping
_ESPN_NFL_MAP_CACHE: dict[str, str] | None = None


def get_espn_nfl_map() -> dict[str, str]:
    """
    Load ESPN ID -> NFL_player_id mapping from MotherDuck.

    Returns dict: espn_id -> NFL_player_id
    """
    global _ESPN_NFL_MAP_CACHE

    if _ESPN_NFL_MAP_CACHE is not None:
        return _ESPN_NFL_MAP_CACHE

    # Backend-aware: Fly path uses DATABASE_READ_TOKEN inside FlyReader.
    # Only fail-closed on missing MOTHERDUCK_TOKEN when not on Fly.
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend != "fly":
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            _ESPN_NFL_MAP_CACHE = {}
            return _ESPN_NFL_MAP_CACHE

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        df = reader.query_df(
            """
            SELECT DISTINCT espn_id, NFL_player_id
            FROM nfl_historical.player_bio
            WHERE espn_id IS NOT NULL AND espn_id != ''
              AND NFL_player_id IS NOT NULL AND NFL_player_id != ''
            """,
            database="___ops",
        )

        _ESPN_NFL_MAP_CACHE = dict(zip(df["espn_id"].astype(str), df["NFL_player_id"]))
        logger.info(f"Loaded {len(_ESPN_NFL_MAP_CACHE):,} ESPN->NFL mappings from player_bio")
        return _ESPN_NFL_MAP_CACHE

    except Exception as e:
        logger.warning(f"Failed to load ESPN-NFL mapping: {e}")
        _ESPN_NFL_MAP_CACHE = {}
        return _ESPN_NFL_MAP_CACHE


# =============================================================================
# ESPN Slot Map (for reference / shared usage)
# =============================================================================

ESPN_SLOT_MAP = {
    0: "QB",
    2: "RB",
    4: "WR",
    6: "TE",
    16: "DEF",
    17: "K",
    20: "BN",
    21: "IR",
    23: "FLEX",
    7: "OP",
}


# =============================================================================
# Normalization Functions
# =============================================================================


def normalize_player_data(df: pd.DataFrame, league_id: str, platform: str = "espn") -> pd.DataFrame:
    """
    Normalize ESPN roster data to canonical player_fantasy.parquet schema.

    Args:
        df: DataFrame from fetch_espn_rosters()
        league_id: League identifier
        platform: Platform identifier

    Returns:
        DataFrame with canonical column names
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} player rows for league {league_id}")

    result = df.copy()

    # Add platform and league identifiers
    result["platform"] = platform
    result["league_id"] = str(league_id)

    # Keep espn_player_id for tracing
    if "espn_player_id" in result.columns:
        result["espn_player_id_original"] = result["espn_player_id"]
        # Map to generic player_id
        result["player_id"] = result["espn_player_id"].astype(str)

    # Rename nfl_team -> team for canonical schema
    if "nfl_team" in result.columns and "team" not in result.columns:
        result["team"] = result["nfl_team"]

    # Normalize fantasy_position values (clean up SLOT_* and string values from ESPN API)
    if "fantasy_position" in result.columns:
        POSITION_CLEANUP = {
            "SLOT_K": "K",
            "SLOT_BE": "BN",
            "SLOT_BN": "BN",
            "SLOT_BENCH": "BN",
            "SLOT_TE": "TE",
            "SLOT_WR": "WR",
            "SLOT_RB": "RB",
            "SLOT_QB": "QB",
            "SLOT_DEF": "DEF",
            "SLOT_IR": "IR",
            "SLOT_ER": "IR",
            "SLOT_FLEX": "FLEX",
            "SLOT_OP": "OP",
            "SLOT_RB/WR/TE": "FLEX",
            "SLOT_RB/WR": "FLEX",
            "SLOT_WR/TE": "FLEX",
            "SLOT_D/ST": "DEF",
            "SLOT_DST": "DEF",
            "BE": "BN",
            "BENCH": "BN",
            "D/ST": "DEF",
            "DST": "DEF",
            "RB/WR/TE": "FLEX",
            "RB/WR": "FLEX",
            "WR/TE": "FLEX",
        }
        result["fantasy_position"] = result["fantasy_position"].apply(
            lambda x: POSITION_CLEANUP.get(str(x).upper().strip(), x) if pd.notna(x) else x
        )

    # Set is_started based on fantasy_position
    NON_STARTER_POSITIONS = {"BN", "IR", "TAXI"}
    if "fantasy_position" in result.columns:
        result["is_started"] = ~result["fantasy_position"].fillna("BN").isin(NON_STARTER_POSITIONS)

    # Add missing canonical columns with defaults
    canonical_defaults = {
        "projected_points": None,
        "is_rostered": True,
    }
    for col, default in canonical_defaults.items():
        if col not in result.columns:
            result[col] = default

    # Ensure types
    result = _ensure_player_types(result)

    # Normalize positions (D/ST → DEF) before creating aliases
    result = _normalize_position_column(result, "position")
    result = _normalize_position_column(result, "fantasy_position")

    # Normalize granular IDP positions (CB→DB, DT→DL, FS→DB, FB→RB, etc.)
    if "position" in result.columns:
        from multi_league.core.roster_slots import normalize_position

        result["position"] = result["position"].apply(lambda p: normalize_position(p) if isinstance(p, str) else p)

    # Add Yahoo-compatible column aliases (player_id for platform-agnostic joins)
    if "yahoo_player_id" not in result.columns:
        if "player_id" in result.columns:
            result["yahoo_player_id"] = result["player_id"].astype(str)
        elif "espn_player_id" in result.columns:
            result["yahoo_player_id"] = result["espn_player_id"].astype(str)
    if "position" in result.columns and "yahoo_position" not in result.columns:
        result["yahoo_position"] = result["position"].where(result["position"].notna(), None)
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]
    if "fantasy_position" in result.columns and "roster_position" not in result.columns:
        result["roster_position"] = result["fantasy_position"]

    # Ensure position columns are string type for DuckDB compatibility
    for col in ["position", "yahoo_position", "fantasy_position", "roster_position"]:
        if col in result.columns:
            result[col] = result[col].astype("object")

    # Add NFL_player_id from ESPN mapping
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    if "espn_player_id" in result.columns:
        espn_map = get_espn_nfl_map()
        if espn_map:
            mask = result["NFL_player_id"].isna()
            if mask.any():
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "espn_player_id"].apply(
                    lambda x: espn_map.get(str(x), None) if pd.notna(x) else None
                )

    # Normalize DEF records
    result = normalize_def_records(result)

    # Normalize DST NFL_player_id to franchise ID format
    if "NFL_player_id" in result.columns:
        _normalize_dst_ids(result)

    # Build composite keys
    _add_composite_keys(result)

    # Ensure fantasy_points is never NULL for rostered players
    if "fantasy_points" in result.columns:
        null_mask = result["fantasy_points"].isna()
        if null_mask.any():
            result.loc[null_mask, "fantasy_points"] = 0.0
            log(f"  Filled {null_mask.sum()} NULL fantasy_points with 0")

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_matchup_data(df: pd.DataFrame, league_id: str, platform: str = "espn") -> pd.DataFrame:
    """
    Normalize ESPN matchup data to canonical matchup.parquet schema.
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} matchup rows for league {league_id}")

    result = df.copy()

    result["platform"] = platform
    result["league_id"] = str(league_id)

    # Rename team_points -> points (canonical name)
    if "team_points" in result.columns and "points" not in result.columns:
        result["points"] = result["team_points"]
    # Keep team_points alias
    if "points" in result.columns and "team_points" not in result.columns:
        result["team_points"] = result["points"]

    # Add franchise_id placeholder (set by discover_franchises)
    if "franchise_id" not in result.columns:
        result["franchise_id"] = None

    # Add missing canonical columns
    canonical_defaults = {
        "is_playoffs": False,
        "is_consolation": False,
        "is_championship": False,
        "optimal_points": None,
        "points_left_on_bench": None,
    }
    for col, default in canonical_defaults.items():
        if col not in result.columns:
            result[col] = default

    # Add cumulative_week
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)

    # Add year_week
    if "year_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["year_week"] = result["year"].astype(str) + "_" + result["week"].astype(str)

    # Create matchup_key
    if "matchup_key" not in result.columns:
        result["matchup_key"] = (
            result["league_id"].astype(str)
            + "_"
            + result["year"].astype(str)
            + "_"
            + result["week"].astype(str)
            + "_"
            + result["matchup_id"].astype(str)
        )

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_draft_data(df: pd.DataFrame, league_id: str, platform: str = "espn") -> pd.DataFrame:
    """
    Normalize ESPN draft data to canonical draft.parquet schema.
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} draft rows for league {league_id}")

    result = df.copy()

    result["platform"] = platform
    result["league_id"] = str(league_id)

    # Keep espn_player_id for tracing
    if "espn_player_id" in result.columns:
        result["espn_player_id_original"] = result["espn_player_id"]
        result["player_id"] = result["espn_player_id"].astype(str)

    # Rename nfl_team -> team
    if "nfl_team" in result.columns and "team" not in result.columns:
        result["team"] = result["nfl_team"]

    # Add pick_number alias
    if "pick" in result.columns and "pick_number" not in result.columns:
        result["pick_number"] = result["pick"]

    # Add keeper_cost alias
    if "cost" in result.columns and "keeper_cost" not in result.columns:
        result["keeper_cost"] = result["cost"]

    # Normalize positions (D/ST → DEF) before creating aliases
    result = _normalize_position_column(result, "position")

    # Add Yahoo-compatible aliases
    if "yahoo_player_id" not in result.columns:
        if "player_id" in result.columns:
            result["yahoo_player_id"] = result["player_id"].astype(str)
        elif "espn_player_id" in result.columns:
            result["yahoo_player_id"] = result["espn_player_id"].astype(str)
    if "position" in result.columns and "yahoo_position" not in result.columns:
        result["yahoo_position"] = result["position"].where(result["position"].notna(), None)
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]

    # Ensure position columns are string type for DuckDB compatibility
    for col in ["position", "yahoo_position"]:
        if col in result.columns:
            result[col] = result[col].astype("object")

    # Add NFL_player_id from ESPN mapping
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    if "espn_player_id" in result.columns:
        espn_map = get_espn_nfl_map()
        if espn_map:
            mask = result["NFL_player_id"].isna()
            if mask.any():
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "espn_player_id"].apply(
                    lambda x: espn_map.get(str(x), None) if pd.notna(x) else None
                )
                mapped_count = result["NFL_player_id"].notna().sum()
                log(f"  Mapped {mapped_count} NFL_player_ids from ESPN->NFL mapping")

    # Add missing canonical columns
    canonical_defaults = {
        "keeper_year": None,
        "original_draft_round": None,
        "season_points": None,
        "season_lamar": None,
        "draft_grade": None,
        "value_tier": None,
        "pick_value": None,
        "surplus_value": None,
    }
    for col, default in canonical_defaults.items():
        if col not in result.columns:
            result[col] = default

    # Normalize D/ST in player names (position already normalized above)
    if "player" in result.columns:
        result["player"] = result["player"].astype(str).str.replace(" D/ST", " DST", regex=False)

    # Deduplicate by (player_id, year)
    if "player_id" in result.columns and "year" in result.columns:
        before = len(result)
        result = result.drop_duplicates(subset=["player_id", "year"], keep="first")
        after = len(result)
        if before != after:
            log(f"  Removed {before - after} duplicate rows")

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_transaction_data(df: pd.DataFrame, league_id: str, platform: str = "espn") -> pd.DataFrame:
    """
    Normalize ESPN transaction data to canonical transactions.parquet schema.
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} transaction rows for league {league_id}")

    result = df.copy()

    result["platform"] = platform
    result["league_id"] = str(league_id)

    # Trade row expansion (duplicate_trade_rows) is handled canonically by
    # normalize_transaction_df() in canonical_transaction.py — not here.

    # Keep espn_player_id for tracing
    if "espn_player_id" in result.columns:
        result["espn_player_id_original"] = result["espn_player_id"]
        result["player_id"] = result["espn_player_id"].astype(str)

    # Rename nfl_team -> team
    if "nfl_team" in result.columns and "team" not in result.columns:
        result["team"] = result["nfl_team"]

    # Add franchise_id placeholder
    if "franchise_id" not in result.columns:
        result["franchise_id"] = None

    # Add missing canonical columns
    canonical_defaults = {
        "faab_remaining": None,
        "points_after_acquisition": None,
        "lamar_after_acquisition": None,
        "acquisition_value": None,
        "transaction_sequence": 0,
        "is_keeper_status": 0,
        "kept_next_year": 0,
    }
    for col, default in canonical_defaults.items():
        if col not in result.columns:
            result[col] = default

    # Add faab_bid alias
    if "faab_spent" in result.columns and "faab_bid" not in result.columns:
        result["faab_bid"] = result["faab_spent"]

    # Add transaction_datetime from timestamp
    if "transaction_datetime" not in result.columns and "timestamp" in result.columns:
        try:
            result["transaction_datetime"] = pd.to_datetime(
                pd.to_numeric(result["timestamp"], errors="coerce"), unit="ms", errors="coerce"
            )
        except Exception:
            result["transaction_datetime"] = None

    # Add player_key for compatibility
    if "player_key" not in result.columns and "player_id" in result.columns:
        result["player_key"] = result["player_id"].apply(lambda x: f"espn.p.{x}" if pd.notna(x) else None)

    # Add Yahoo-compatible aliases
    if "yahoo_player_id" not in result.columns:
        if "player_id" in result.columns:
            result["yahoo_player_id"] = result["player_id"].astype(str)
        elif "espn_player_id" in result.columns:
            result["yahoo_player_id"] = result["espn_player_id"].astype(str)
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]

    # Add NFL_player_id from ESPN mapping
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    if "espn_player_id" in result.columns:
        espn_map = get_espn_nfl_map()
        if espn_map:
            mask = result["NFL_player_id"].isna()
            if mask.any():
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "espn_player_id"].apply(
                    lambda x: espn_map.get(str(x), None) if pd.notna(x) else None
                )

    # Add cumulative_week
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)

    log(f"  Normalized columns: {list(result.columns)}")
    return result


# =============================================================================
# Full Pipeline Normalization
# =============================================================================


def normalize_all_espn_data(
    player_df: pd.DataFrame | None,
    matchup_df: pd.DataFrame | None,
    draft_df: pd.DataFrame | None,
    transaction_df: pd.DataFrame | None,
    league_id: str,
    platform: str = "espn",
) -> dict:
    """
    Normalize all ESPN data to canonical schemas.

    Returns:
        Dict with normalized DataFrames: player, matchup, draft, transactions
    """
    log(f"\n{'='*60}")
    log(f"Normalizing all ESPN data for league {league_id}")
    log(f"{'='*60}")

    result = {}

    if player_df is not None and not player_df.empty:
        result["player"] = normalize_player_data(player_df, league_id, platform)
    else:
        result["player"] = pd.DataFrame()

    if matchup_df is not None and not matchup_df.empty:
        result["matchup"] = normalize_matchup_data(matchup_df, league_id, platform)
    else:
        result["matchup"] = pd.DataFrame()

    if draft_df is not None and not draft_df.empty:
        result["draft"] = normalize_draft_data(draft_df, league_id, platform)
    else:
        result["draft"] = pd.DataFrame()

    if transaction_df is not None and not transaction_df.empty:
        result["transactions"] = normalize_transaction_data(transaction_df, league_id, platform)
    else:
        result["transactions"] = pd.DataFrame()

    log("\nNormalization complete:")
    for table, tdf in result.items():
        log(f"  {table}: {len(tdf)} rows")

    return result


# =============================================================================
# Internal Helpers
# =============================================================================


def _ensure_player_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure proper data types for player data."""
    type_map = {
        "year": "Int64",
        "week": "Int64",
        "fantasy_points": "float64",
        "projected_points": "float64",
        "is_rostered": "boolean",
        "is_started": "boolean",
    }
    for col, dtype in type_map.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError):
                pass
    return df


def _normalize_dst_ids(result: pd.DataFrame):
    """Normalize D/ST NFL_player_id to franchise ID format."""
    pos_col = "position" if "position" in result.columns else "yahoo_position"
    if pos_col not in result.columns:
        return

    dst_mask = result[pos_col].fillna("").astype(str).str.upper().isin(["DEF", "DST", "D/ST"])
    if not dst_mask.any():
        return

    team_col = "nfl_team" if "nfl_team" in result.columns else "team"
    if team_col not in result.columns:
        return

    for idx in result[dst_mask].index:
        nfl_id = result.at[idx, "NFL_player_id"]
        if pd.notna(nfl_id) and str(nfl_id).startswith("DEF-"):
            # Check if it's already in numeric format
            suffix = str(nfl_id).replace("DEF-", "")
            if suffix.isdigit():
                continue  # Already in correct format

        # Use team abbreviation to build franchise ID
        team_abbr = str(result.at[idx, team_col]).upper().strip()
        franchise_id = ABBREV_TO_FRANCHISE_ID.get(team_abbr)
        if franchise_id:
            result.at[idx, "NFL_player_id"] = f"DEF-{franchise_id}"
            result.at[idx, "position"] = "DEF"


def _add_composite_keys(result: pd.DataFrame):
    """Add player_week, player_year, cumulative_week composite keys."""
    # player_week
    if "player_week" not in result.columns:
        if "NFL_player_id" in result.columns:
            result["player_week"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip()
                + "_"
                + result["year"].astype(str)
                + "_"
                + result["week"].astype(str)
            )

    # player_year
    if "player_year" not in result.columns:
        if "NFL_player_id" in result.columns:
            result["player_year"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip() + "_" + result["year"].astype(str)
            )

    # cumulative_week
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)


# =============================================================================
# Validation
# =============================================================================


def validate_canonical_schema(df: pd.DataFrame, table_type: str) -> list:
    """Validate that a DataFrame has required canonical columns."""
    required_columns = {
        "player": ["league_id", "year", "week", "player_id", "player", "position", "manager"],
        "matchup": ["league_id", "year", "week", "manager", "points", "opponent"],
        "draft": ["league_id", "year", "pick_number", "player_id", "player", "manager", "round"],
        "transactions": ["league_id", "transaction_id", "transaction_type", "player_id", "manager"],
    }

    if table_type not in required_columns:
        return [f"Unknown table type: {table_type}"]

    issues = []
    for col in required_columns[table_type]:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")

    return issues
