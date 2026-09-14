"""
Sleeper Data Normalizer

Converts Sleeper fetcher output to canonical schema for multi-tenant compatibility.
This ensures Sleeper data can flow through the same transformation pipeline as Yahoo data.

Canonical Tables (from docs/data-dictionary/canonical-tables.md):
- player.parquet: Player-week statistics
- matchup.parquet: Weekly matchup results
- draft.parquet: Draft picks and keepers
- transactions.parquet: Trades, pickups, drops

Column Mappings:
- sleeper_player_id → player_id (with 'sleeper_' prefix retained for tracing)
- player → player_name (alias)
- yahoo_position → position
- points → fantasy_points
- fantasy_position stays as-is (roster_position alias added for backward compat)
- pick stays as-is (pick_number alias added for backward compat)
- cost stays as-is (keeper_cost alias added for backward compat)
- manager → manager (unchanged)
- manager_guid → manager_guid (unchanged, uses Sleeper user_id)

Usage:
    from sleeper_data_normalizer import (
        normalize_player_data,
        normalize_matchup_data,
        normalize_draft_data,
        normalize_transaction_data,
    )

    # Convert Sleeper rosters to canonical player format
    canonical_player = normalize_player_data(sleeper_rosters_df, league_id)

    # Convert Sleeper matchups to canonical format
    canonical_matchup = normalize_matchup_data(sleeper_matchups_df, league_id)
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Valid NFL team abbreviations for DST normalization
# Super table uses franchise ID format (DEF-3 for PHI, DEF-19 for NE, etc.)
VALID_NFL_TEAMS = {
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GB",
    "HOU",
    "IND",
    "JAX",
    "JAC",
    "KC",
    "LA",
    "LAC",
    "LAR",
    "LV",
    "MIA",
    "MIN",
    "NE",
    "NO",
    "NYG",
    "NYJ",
    "OAK",
    "PHI",
    "PIT",
    "SD",
    "SEA",
    "SF",
    "STL",
    "TB",
    "TEN",
    "WAS",
    "WSH",
}

# Franchise ID to team abbreviation mapping (for reference - used by nfl_franchises.py)
# DEF player_week format: DEF-{franchise_id}_{year}_{week} (e.g., DEF-3_2024_10 for Eagles)
FRANCHISE_ID_TO_ABBREV = {
    1: "DAL",
    2: "NYG",
    3: "PHI",
    4: "WAS",  # NFC East
    5: "CHI",
    6: "DET",
    7: "GB",
    8: "MIN",  # NFC North
    9: "ATL",
    10: "CAR",
    11: "NO",
    12: "TB",  # NFC South
    13: "ARI",
    14: "LAR",
    15: "SF",
    16: "SEA",  # NFC West
    17: "BUF",
    18: "MIA",
    19: "NE",
    20: "NYJ",  # AFC East
    21: "BAL",
    22: "CIN",
    23: "CLE",
    24: "PIT",  # AFC North
    25: "HOU",
    26: "IND",
    27: "JAX",
    28: "TEN",  # AFC South
    29: "DEN",
    30: "KC",
    31: "LV",
    32: "LAC",  # AFC West
}

# Reverse mapping: team abbreviation -> franchise ID (for DEF normalization)
ABBREV_TO_FRANCHISE_ID = {v: k for k, v in FRANCHISE_ID_TO_ABBREV.items()}
# Add common variations
ABBREV_TO_FRANCHISE_ID.update(
    {
        "LA": 14,  # Rams (ambiguous but default to Rams)
        "JAC": 27,  # Jaguars alternate
        "WSH": 4,  # Washington alternate
        "OAK": 31,  # Raiders (Oakland)
        "SD": 32,  # Chargers (San Diego)
        "STL": 14,  # Rams (St. Louis)
    }
)

# Cache for sleeper_nfl_player_map lookup
_SLEEPER_NFL_MAP_CACHE: dict[str, str] | None = None


def log(msg: str):
    """Simple logging function."""
    logger.info(msg)
    print(msg)


def get_sleeper_nfl_map() -> dict[str, str]:
    """
    Load Sleeper-to-NFL player ID mapping.

    Uses the shared unified mapping (player_bio + legacy maps).

    Returns dict: sleeper_player_id -> NFL_player_id
    Caches result in memory to avoid repeated database calls.
    """
    global _SLEEPER_NFL_MAP_CACHE

    if _SLEEPER_NFL_MAP_CACHE is not None:
        return _SLEEPER_NFL_MAP_CACHE

    from multi_league.data_fetchers.shared.nfl_player_mapping import get_sleeper_to_nfl_map

    _SLEEPER_NFL_MAP_CACHE = get_sleeper_to_nfl_map()
    logger.info(f"Loaded {len(_SLEEPER_NFL_MAP_CACHE):,} sleeper->NFL mappings (player_bio + legacy)")
    return _SLEEPER_NFL_MAP_CACHE


def lookup_nfl_player_id(sleeper_player_id: str) -> str | None:
    """
    Look up NFL_player_id for a Sleeper player ID.

    Uses the sleeper_nfl_player_map table in MotherDuck.

    Args:
        sleeper_player_id: Sleeper player ID (e.g., "4046")

    Returns:
        NFL_player_id (e.g., "00-0033873") or None if not found
    """
    sleeper_map = get_sleeper_nfl_map()
    return sleeper_map.get(str(sleeper_player_id))


# =============================================================================
# Column Mappings: Sleeper → Canonical
# =============================================================================

PLAYER_COLUMN_MAP = {
    # Sleeper column → Canonical column
    "sleeper_player_id": "player_id",  # Keep sleeper_ version too
    # Note: 'player' stays as 'player' - standardized column name across all platforms
    # Note: yahoo_position and nfl_position both map to 'position'
    # but only one should be renamed (handled in normalize function)
    "points": "fantasy_points",
    # 'fantasy_position' stays as 'fantasy_position' - required by player_stats_v2.py for is_started
    "nfl_team": "team",
    "team_key": "team_key",  # Sleeper roster_id as string
    "manager": "manager",
    "manager_guid": "manager_guid",
    "year": "year",
    "week": "week",
}

MATCHUP_COLUMN_MAP = {
    "manager": "manager",
    "manager_guid": "manager_guid",
    "team_name": "team_name",
    "team_key": "team_key",
    "team_points": "points",
    "opponent": "opponent",
    "opponent_points": "opponent_points",
    "margin": "margin",
    "win": "win",
    "loss": "loss",
    "tie": "tie",
    "week": "week",
    "year": "year",
    "matchup_id": "matchup_id",
    "teams_beat_this_week": "teams_beat_this_week",
    "league_weekly_mean": "league_weekly_mean",
    "above_league_median": "above_league_median",
}

DRAFT_COLUMN_MAP = {
    "sleeper_player_id": "player_id",
    # Note: 'player' stays as 'player' - standardized column name across all platforms
    "yahoo_position": "position",
    "nfl_team": "team",
    # 'pick' stays as 'pick' - required by draft_value_metrics_v3.py
    # 'cost' stays as 'cost' - required by draft_value_metrics_v3.py
    "round": "round",
    "manager": "manager",
    "manager_guid": "manager_guid",
    "team_key": "team_key",
    "is_keeper_status": "is_keeper",
    "draft_type": "draft_type",
    "year": "year",
}

TRANSACTION_COLUMN_MAP = {
    "transaction_id": "transaction_id",
    "sleeper_player_id": "player_id",
    # Note: 'player' stays as 'player' - standardized column name across all platforms
    "nfl_team": "team",
    "manager": "manager",
    "manager_guid": "manager_guid",
    "team_name": "team_name",
    "transaction_type": "transaction_type",
    "faab_bid": "faab_spent",
    "source_type": "source_team",
    "destination": "destination_team",
    "week": "week",
    "year": "year",
    "timestamp": "timestamp",
    "status": "status",
}


# =============================================================================
# DEF (Team Defense) Normalization
# =============================================================================


def normalize_def_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DEF (team defense) records by generating NFL_player_id and fixing player_week.

    Sleeper DEF records come with:
    - position = 'DEF'
    - nfl_team = team abbreviation (e.g., 'GB', 'TB')
    - player_week = '{abbrev}_{year}_{week}' (e.g., 'GB_2021_1') - WRONG FORMAT

    This function converts to super_table format:
    - NFL_player_id = 'DEF-{franchise_id}' (e.g., 'DEF-7' for Packers)
    - player_week = 'DEF-{franchise_id}_{year}_{week}' (e.g., 'DEF-7_2021_1')

    Args:
        df: DataFrame with Sleeper roster data

    Returns:
        DataFrame with normalized DEF records
    """
    if df.empty:
        return df

    # Find DEF position column
    pos_col = None
    for col in ["position", "yahoo_position", "nfl_position"]:
        if col in df.columns:
            pos_col = col
            break

    if pos_col is None:
        return df

    # Find DEF rows by position OR by sleeper_player_id being a team abbreviation
    def_mask = df[pos_col].astype(str).str.upper().isin(["DEF", "DST", "D/ST"])

    # Safety net: detect DEF by team abbreviation in sleeper_player_id_original
    # Handles edge case where position wasn't set correctly (e.g., historical teams)
    for id_col in ["sleeper_player_id_original", "sleeper_player_id", "player_id"]:
        if id_col in df.columns:
            abbrev_mask = df[id_col].astype(str).str.upper().str.strip().isin(ABBREV_TO_FRANCHISE_ID.keys())
            if abbrev_mask.any():
                new_defs = abbrev_mask & ~def_mask
                if new_defs.any():
                    # Fix position for these rows too
                    df.loc[new_defs, pos_col] = "DEF"
                    log(f"[DEF] Detected {new_defs.sum()} additional DEF rows from team abbreviation in {id_col}")
                def_mask = def_mask | abbrev_mask
            break

    if not def_mask.any():
        return df

    def_count = def_mask.sum()
    log(f"[DEF] Normalizing {def_count} DEF records...")

    df = df.copy()

    # Ensure columns exist
    if "NFL_player_id" not in df.columns:
        df["NFL_player_id"] = None
    if "player_week" not in df.columns:
        df["player_week"] = None

    # Find team column
    team_col = None
    for col in ["nfl_team", "team", "nfl_team_abbr"]:
        if col in df.columns:
            team_col = col
            break

    if team_col is None:
        log("[DEF] Warning: No team column found, cannot normalize DEF records")
        return df

    # Generate NFL_player_id for DEF records
    def get_def_nfl_id(team_abbr):
        if pd.isna(team_abbr):
            return None
        abbr_upper = str(team_abbr).upper().strip()
        franchise_id = ABBREV_TO_FRANCHISE_ID.get(abbr_upper)
        if franchise_id:
            return f"DEF-{franchise_id}"
        # Return None if not found - don't use abbrev format
        return None

    # Only update DEF rows that are missing NFL_player_id or have wrong format
    needs_update = def_mask & (df["NFL_player_id"].isna() | (~df["NFL_player_id"].astype(str).str.startswith("DEF-")))

    if needs_update.any():
        df.loc[needs_update, "NFL_player_id"] = df.loc[needs_update, team_col].apply(get_def_nfl_id)
        log(f"[DEF] Generated NFL_player_id for {needs_update.sum()} DEF records")

    # Fix player_week format for DEF records
    # Current: {abbrev}_{year}_{week} (e.g., 'GB_2021_1')
    # Target: DEF-{franchise_id}_{year}_{week} (e.g., 'DEF-7_2021_1')
    if "year" in df.columns and "week" in df.columns:
        # Check for DEF records with wrong player_week format
        wrong_pw_mask = def_mask & (df["player_week"].isna() | (~df["player_week"].astype(str).str.startswith("DEF-")))

        if wrong_pw_mask.any():
            # Generate correct player_week using NFL_player_id
            df.loc[wrong_pw_mask, "player_week"] = (
                df.loc[wrong_pw_mask, "NFL_player_id"].astype(str)
                + "_"
                + df.loc[wrong_pw_mask, "year"].astype(int).astype(str)
                + "_"
                + df.loc[wrong_pw_mask, "week"].astype(int).astype(str)
            )
            log(f"[DEF] Fixed player_week format for {wrong_pw_mask.sum()} DEF records")

    return df


# =============================================================================
# Normalization Functions
# =============================================================================


def normalize_player_data(df: pd.DataFrame, league_id: str, platform: str = "sleeper") -> pd.DataFrame:
    """
    Normalize Sleeper roster data to canonical player.parquet schema.

    Args:
        df: DataFrame from fetch_sleeper_rosters()
        league_id: League identifier for league_id column
        platform: Platform identifier (default: 'sleeper')

    Returns:
        DataFrame with canonical column names
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} player rows for league {league_id}")

    result = df.copy()

    # Add platform and league identifiers
    result["platform"] = platform
    result["league_id"] = league_id

    # Handle position column: use yahoo_position if available, else nfl_position
    # Then drop the source columns to avoid duplicates
    if "yahoo_position" in result.columns:
        result["position"] = result["yahoo_position"]
        result = result.drop(columns=["yahoo_position"], errors="ignore")
    elif "nfl_position" in result.columns:
        result["position"] = result["nfl_position"]

    # Drop nfl_position if it exists (we've already handled position)
    if "nfl_position" in result.columns and "position" in result.columns:
        result = result.drop(columns=["nfl_position"], errors="ignore")

    # Normalize granular IDP positions (CB→DB, DT→DL, FS→DB, FB→RB, etc.)
    if "position" in result.columns:
        from multi_league.core.roster_slots import normalize_position

        result["position"] = result["position"].apply(lambda p: normalize_position(p) if isinstance(p, str) else p)

    # Rename columns to canonical names
    rename_map = {}
    for sleeper_col, canonical_col in PLAYER_COLUMN_MAP.items():
        if sleeper_col in result.columns and sleeper_col != canonical_col:
            rename_map[sleeper_col] = canonical_col

    # Keep sleeper_player_id as additional column for tracing
    if "sleeper_player_id" in result.columns:
        result["sleeper_player_id_original"] = result["sleeper_player_id"]

    result = result.rename(columns=rename_map)

    # Add missing canonical columns with defaults
    canonical_defaults = {
        "projected_points": None,
        "is_rostered": True,
        "is_started": None,  # Will be set based on fantasy_position != 'BN'
    }

    for col, default in canonical_defaults.items():
        if col not in result.columns:
            result[col] = default

    # Set is_started based on fantasy_position (roster slot)
    # fantasy_position is the canonical name, roster_position is an alias
    # Non-starter positions: BN (Bench), IR (Injured Reserve), TAXI (Taxi/practice squad)
    NON_STARTER_POSITIONS = {"BN", "IR", "IR+", "TAXI"}
    pos_col = "fantasy_position" if "fantasy_position" in result.columns else "roster_position"
    if pos_col in result.columns:
        result["is_started"] = ~result[pos_col].isin(NON_STARTER_POSITIONS)

    # Ensure proper types
    result = _ensure_player_types(result)

    # Add Yahoo-compatible column aliases for UI compatibility
    # The Streamlit UI expects yahoo_player_id and yahoo_position columns
    if "player_id" in result.columns and "yahoo_player_id" not in result.columns:
        # Cast to string to handle mixed types (numeric for players, string like 'NE' for DEF)
        result["yahoo_player_id"] = result["player_id"].astype(str)
    if "position" in result.columns and "yahoo_position" not in result.columns:
        result["yahoo_position"] = result["position"]
    # 'player' is the canonical column name - add player_name alias for backward compatibility
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]

    # roster_position alias for fantasy_position (backward compatibility)
    if "fantasy_position" in result.columns and "roster_position" not in result.columns:
        result["roster_position"] = result["fantasy_position"]

    # Add NFL_player_id column (transformations expect this for NFL data joins)
    # Priority: 1) gsis_id from API, 2) sleeper_nfl_player_map lookup, 3) player_id fallback
    if "NFL_player_id" not in result.columns:
        if "gsis_id" in result.columns and result["gsis_id"].notna().any():
            # Use gsis_id directly where available
            # CRITICAL: Strip whitespace - Sleeper API sometimes has leading spaces in gsis_id
            result["NFL_player_id"] = result["gsis_id"].astype(str).str.strip().replace({"nan": None, "": None})
        else:
            # Don't fallback to player_id - this causes duplicates!
            # Raw sleeper player_id (e.g., "12462") is NOT a valid NFL_player_id
            # Leave as None so sleeper_nfl_merge.py uses the SLP- prefix fallback consistently
            result["NFL_player_id"] = None

    # Fill missing NFL_player_id using sleeper_nfl_player_map
    if "player_id" in result.columns:
        sleeper_map = get_sleeper_nfl_map()
        if sleeper_map:
            # Only lookup for rows where NFL_player_id is missing or equals player_id
            mask = result["NFL_player_id"].isna() | (result["NFL_player_id"] == result["player_id"])
            if mask.any():
                # CRITICAL: Return None if not in map, NOT the raw sleeper ID
                # Returning raw sleeper ID causes duplicates because player_week is built inconsistently:
                # - Path 1: NFL_player_id = "12462" → player_week = "12462_2025_1"
                # - Path 2: NFL_player_id = None → player_week = "SLP-12462_2025_1"
                # Leaving NFL_player_id as None ensures sleeper_nfl_merge.py uses the SLP- fallback consistently
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "player_id"].apply(
                    lambda x: sleeper_map.get(str(x), None) if pd.notna(x) else None
                )

    # Ensure league_id is string (some transformations expect string)
    if "league_id" in result.columns:
        result["league_id"] = result["league_id"].astype(str)

    # Ensure is_rostered is proper boolean (not int)
    if "is_rostered" in result.columns:
        result["is_rostered"] = result["is_rostered"].fillna(True).astype(bool)

    # CRITICAL: Normalize DEF (team defense) records BEFORE other DST handling
    # DEF records from Sleeper have NFL_player_id = None - we need to generate it
    # from nfl_team (e.g., 'GB' -> 'DEF-7') so subsequent DST normalization works
    result = normalize_def_records(result)

    # Normalize DST NFL_player_id to franchise ID format (DEF-3, DEF-19, etc.)
    # This ensures DST IDs match the format used in the NFL super table
    # IMPORTANT: Super table uses franchise ID format (DEF-3 for PHI), NOT abbreviations (DEF-PHI)
    if "NFL_player_id" in result.columns:

        def normalize_dst_id(row):
            """Convert DST NFL_player_id to franchise ID format (DEF-3 for Eagles)."""
            # Import here to avoid circular imports
            from nfl_data.nfl_franchises import get_def_player_id

            nfl_id = row.get("NFL_player_id")
            if pd.isna(nfl_id):
                return nfl_id

            nfl_id_str = str(nfl_id).upper().strip()
            position = str(row.get("yahoo_position", row.get("nfl_position", row.get("position", "")))).upper()
            year = row.get("year")

            # Case 1: Already in DEF-X format
            if nfl_id_str.startswith("DEF-"):
                team_part = nfl_id_str.replace("DEF-", "")
                # If it's a franchise ID (number), keep as-is - already in correct format
                if team_part.isdigit():
                    return nfl_id_str
                # If it's a team abbreviation, convert to franchise ID format
                if team_part in VALID_NFL_TEAMS:
                    if pd.notna(year):
                        try:
                            return get_def_player_id(team_part, int(year))
                        except (ValueError, TypeError):
                            pass
                    return nfl_id_str
                return nfl_id

            # Case 2: Just team abbreviation (e.g., "NO", "NE") for DEF position
            # Sleeper uses team abbreviation as player_id for DST - convert to franchise ID format
            if position == "DEF" and nfl_id_str in VALID_NFL_TEAMS:
                if pd.notna(year):
                    try:
                        return get_def_player_id(nfl_id_str, int(year))
                    except (ValueError, TypeError):
                        pass
                return f"DEF-{nfl_id_str}"

            return nfl_id

        # Detect DST rows by position OR by NFL_player_id pattern
        pos_col = "yahoo_position" if "yahoo_position" in result.columns else "nfl_position"
        if pos_col not in result.columns:
            pos_col = "position"

        if pos_col in result.columns:
            # DST by position
            dst_by_position = result[pos_col].fillna("").astype(str).str.upper() == "DEF"
        else:
            dst_by_position = pd.Series(False, index=result.index)

        # DST by NFL_player_id pattern - catch:
        # 1. Just team abbreviation without DEF- prefix (e.g., "NE")
        # 2. Franchise ID format (e.g., "DEF-19") that needs conversion
        # 3. DEF-{team_abbrev} format (e.g., "DEF-BUF") that needs normalization
        nfl_ids_upper = result["NFL_player_id"].fillna("").astype(str).str.upper()
        dst_by_abbrev = nfl_ids_upper.isin(VALID_NFL_TEAMS)
        dst_by_franchise_id = nfl_ids_upper.str.match(r"^DEF-\d+$")
        # Check for DEF-{team_abbrev} format (e.g., "DEF-BUF", "DEF-NO")
        dst_by_def_abbrev = nfl_ids_upper.str.replace("DEF-", "", regex=False).isin(VALID_NFL_TEAMS)

        # Combine: normalize any row that's DST by position OR needs normalization
        dst_mask = dst_by_position | dst_by_abbrev | dst_by_franchise_id | dst_by_def_abbrev

        if dst_mask.any():
            log(f"  Normalizing {dst_mask.sum()} DST NFL_player_ids to franchise ID format...")
            result.loc[dst_mask, "NFL_player_id"] = result.loc[dst_mask].apply(normalize_dst_id, axis=1)

            # CRITICAL: Regenerate player_week for DST rows after normalization
            # player_week may have been built earlier (in sleeper_nfl_merge.py) with empty/wrong NFL_player_id
            # We must rebuild it using the normalized NFL_player_id (DEF-{franchise_id})
            if "player_week" in result.columns and "year" in result.columns and "week" in result.columns:
                result.loc[dst_mask, "player_week"] = (
                    result.loc[dst_mask, "NFL_player_id"].fillna("").astype(str).str.strip()
                    + "_"
                    + result.loc[dst_mask, "year"].astype(str)
                    + "_"
                    + result.loc[dst_mask, "week"].astype(str)
                )
                log(f"  Regenerated player_week for {dst_mask.sum()} DST rows")

    # Add player_week composite key for joining with NFL data
    # MUST use NFL_player_id (already populated from sleeper_nfl_player_map above)
    # This is the join key for super_table which uses NFL_player_id format (00-XXXXXXX)
    if "player_week" not in result.columns:
        if "NFL_player_id" in result.columns:
            # Use NFL_player_id for compatibility with super_table joins
            # Strip whitespace to ensure clean join keys
            result["player_week"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip()
                + "_"
                + result["year"].astype(str)
                + "_"
                + result["week"].astype(str)
            )
        elif "player_id" in result.columns:
            # Fallback to player_id if NFL_player_id not available
            result["player_week"] = (
                result["player_id"].fillna("").astype(str).str.strip()
                + "_"
                + result["year"].astype(str)
                + "_"
                + result["week"].astype(str)
            )

    # Add player_year composite key for season aggregations
    # Use NFL_player_id for consistency with player_week
    if "player_year" not in result.columns:
        if "NFL_player_id" in result.columns:
            result["player_year"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip() + "_" + result["year"].astype(str)
            )
        elif "player_id" in result.columns:
            result["player_year"] = (
                result["player_id"].fillna("").astype(str).str.strip() + "_" + result["year"].astype(str)
            )

    # Add cumulative_week for joins (year * 100 + week)
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)

    # =========================================================================
    # Ensure fantasy_points is never NULL for rostered players
    # =========================================================================
    # Players who were rostered but didn't play in the NFL game will have
    # NULL fantasy_points from the NFL merge. Fill from Sleeper's 'points'
    # column, then default to 0 for any remaining NULLs.
    if "fantasy_points" in result.columns:
        # Handle potential duplicate columns from merge
        fp_col = result["fantasy_points"]
        if isinstance(fp_col, pd.DataFrame):
            # Multiple fantasy_points columns - take the first one that has values
            fp_col = fp_col.iloc[:, 0]
            result = result.loc[:, ~result.columns.duplicated()]

        null_mask = fp_col.isna()
        null_count = null_mask.sum()
        if null_count > 0:
            # Fill from 'points' column if it still exists
            if "points" in result.columns:
                result.loc[null_mask, "fantasy_points"] = result.loc[null_mask, "points"]
                null_mask = result["fantasy_points"].isna()
                null_count = null_mask.sum()

            # Default remaining NULLs to 0 (player rostered but didn't play)
            if null_count > 0:
                result.loc[null_mask, "fantasy_points"] = 0.0
                log(f"  Filled {null_count} NULL fantasy_points with 0 (rostered but no NFL stats)")

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_matchup_data(df: pd.DataFrame, league_id: str, platform: str = "sleeper") -> pd.DataFrame:
    """
    Normalize Sleeper matchup data to canonical matchup.parquet schema.

    The canonical schema uses home/away perspective, but Sleeper uses
    single-row-per-manager format. This function converts to the expected format.

    Args:
        df: DataFrame from fetch_sleeper_matchups()
        league_id: League identifier
        platform: Platform identifier

    Returns:
        DataFrame with canonical column names (home/away format)
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} matchup rows for league {league_id}")

    result = df.copy()

    # Add platform and league identifiers
    result["platform"] = platform
    result["league_id"] = league_id

    # Rename to home/away format
    # Current format: manager, opponent (one row per manager per week)
    # Target format: home_manager, away_manager (one row per matchup)

    # For now, keep the single-row format but add canonical column names
    result = result.rename(
        columns={
            "manager": "manager",
            "team_points": "points",
            "opponent": "opponent",
            "opponent_points": "opponent_points",
            "team_name": "team_name",
        }
    )

    # Add team_points back as an alias for points (Yahoo compatibility)
    # The rename above converts team_points -> points, but downstream code expects both
    if "points" in result.columns and "team_points" not in result.columns:
        result["team_points"] = result["points"]

    # Add franchise_id placeholder (will be set by discover_franchises transformation)
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

    # Add cumulative_week (year * 100 + week) for Yahoo compatibility
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)

    # Add year_week for Yahoo compatibility
    if "year_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["year_week"] = result["year"].astype(str) + "_" + result["week"].astype(str)

    # Ensure league_id is string
    if "league_id" in result.columns:
        result["league_id"] = result["league_id"].astype(str)

    # Create matchup_key
    if "matchup_key" not in result.columns:
        result["matchup_key"] = (
            result["league_id"]
            + "_"
            + result["year"].astype(str)
            + "_"
            + result["week"].astype(str)
            + "_"
            + result["matchup_id"].astype(str)
        )

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_draft_data(df: pd.DataFrame, league_id: str, platform: str = "sleeper") -> pd.DataFrame:
    """
    Normalize Sleeper draft data to canonical draft.parquet schema.

    Args:
        df: DataFrame from fetch_sleeper_draft()
        league_id: League identifier
        platform: Platform identifier

    Returns:
        DataFrame with canonical column names
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} draft rows for league {league_id}")

    result = df.copy()

    # Add platform and league identifiers
    result["platform"] = platform
    result["league_id"] = league_id

    # Rename columns
    rename_map = {}
    for sleeper_col, canonical_col in DRAFT_COLUMN_MAP.items():
        if sleeper_col in result.columns and sleeper_col != canonical_col:
            rename_map[sleeper_col] = canonical_col

    # Keep sleeper_player_id for tracing
    if "sleeper_player_id" in result.columns:
        result["sleeper_player_id_original"] = result["sleeper_player_id"]

    result = result.rename(columns=rename_map)

    # Add pick_number alias for backward compatibility (pick stays as canonical)
    if "pick" in result.columns and "pick_number" not in result.columns:
        result["pick_number"] = result["pick"]

    # Add keeper_cost alias for backward compatibility (cost stays as canonical)
    if "cost" in result.columns and "keeper_cost" not in result.columns:
        result["keeper_cost"] = result["cost"]

    # Add pick_in_round if not present
    if "pick_in_round" not in result.columns and "pick" in result.columns:
        # Calculate pick in round based on total teams
        # Sleeper uses roster_id or manager, Yahoo uses team_key
        team_col = (
            "roster_id"
            if "roster_id" in result.columns
            else ("manager" if "manager" in result.columns else ("team_key" if "team_key" in result.columns else None))
        )
        num_teams = result[team_col].nunique() if team_col else 10
        num_teams = num_teams or 10  # Fallback if 0
        result["pick_in_round"] = ((result["pick"] - 1) % num_teams) + 1

    # Add franchise_id placeholder
    if "franchise_id" not in result.columns:
        result["franchise_id"] = None

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

    # Add Yahoo-compatible column aliases for UI compatibility
    if "player_id" in result.columns and "yahoo_player_id" not in result.columns:
        # Cast to string to handle mixed types (numeric for players, string like 'NE' for DEF)
        result["yahoo_player_id"] = result["player_id"].astype(str)
    if "position" in result.columns and "yahoo_position" not in result.columns:
        result["yahoo_position"] = result["position"]
    # 'player' is the canonical column name - add player_name alias for backward compatibility
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]

    # Add NFL_player_id column using sleeper_nfl_player_map lookup
    # CRITICAL: Must look up the actual NFL_player_id, not just copy player_id (which is Sleeper ID)
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    if "player_id" in result.columns:
        sleeper_map = get_sleeper_nfl_map()
        if sleeper_map:
            # Look up NFL_player_id for each row where it's missing
            mask = result["NFL_player_id"].isna()
            if mask.any():
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "player_id"].apply(
                    lambda x: sleeper_map.get(str(x).replace(".0", ""), None) if pd.notna(x) else None
                )
                mapped_count = result["NFL_player_id"].notna().sum()
                log(f"  Mapped {mapped_count} NFL_player_ids from sleeper_nfl_player_map")

    # Ensure league_id is string
    if "league_id" in result.columns:
        result["league_id"] = result["league_id"].astype(str)

    # CRITICAL: Ensure sleeper_player_id_original is a clean string (no .0 float suffix)
    # This is needed for joins with sleeper_nfl_player_map which uses string IDs
    if "sleeper_player_id_original" in result.columns:
        result["sleeper_player_id_original"] = (
            result["sleeper_player_id_original"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .replace({"nan": None, "None": None, "": None})
        )

    # CRITICAL: Normalize DEF player names to "Team DST" format
    # This catches cases where the fetcher returned full team names like "Baltimore Ravens"
    if "player" in result.columns:
        pos_col = (
            "position"
            if "position" in result.columns
            else ("yahoo_position" if "yahoo_position" in result.columns else None)
        )

        if pos_col:
            from .sleeper_player_cache import SleeperPlayerCache

            def_mask = result[pos_col].fillna("").astype(str).str.upper().isin(["DEF", "DST", "D/ST"])
            needs_fix = def_mask & ~result["player"].astype(str).str.contains("DST", case=False, na=False)

            if needs_fix.any():
                log(f"  Normalizing {needs_fix.sum()} DEF player names to 'Team DST' format...")

                def normalize_def_name(name):
                    """Convert full team name to 'Team DST' format."""
                    if pd.isna(name):
                        return name
                    name_str = str(name)
                    # Check if already in DST format
                    if "DST" in name_str.upper():
                        return name_str
                    # Try to find in full name mapping
                    short = SleeperPlayerCache.NFL_TEAM_SHORT_NAMES.get(name_str)
                    if short:
                        return f"{short} DST"
                    # Fallback: take last word of name
                    parts = name_str.split()
                    if parts:
                        return f"{parts[-1]} DST"
                    return name_str

                result.loc[needs_fix, "player"] = result.loc[needs_fix, "player"].apply(normalize_def_name)

    # CRITICAL: Deduplicate by (player_id, year) - draft should have one row per player per year
    # This prevents duplicate rows from file concatenation issues or multiple runs
    dedup_key = None
    if "player_id" in result.columns and "year" in result.columns:
        dedup_key = ["player_id", "year"]
    elif "yahoo_player_id" in result.columns and "year" in result.columns:
        dedup_key = ["yahoo_player_id", "year"]

    if dedup_key:
        before_dedup = len(result)
        result = result.drop_duplicates(subset=dedup_key, keep="first")
        after_dedup = len(result)
        if before_dedup != after_dedup:
            log(f"  Removed {before_dedup - after_dedup} duplicate rows (kept {after_dedup})")

    log(f"  Normalized columns: {list(result.columns)}")
    return result


def normalize_transaction_data(df: pd.DataFrame, league_id: str, platform: str = "sleeper") -> pd.DataFrame:
    """
    Normalize Sleeper transaction data to canonical transactions.parquet schema.

    Args:
        df: DataFrame from fetch_sleeper_transactions()
        league_id: League identifier
        platform: Platform identifier

    Returns:
        DataFrame with canonical column names
    """
    if df.empty:
        return df

    log(f"Normalizing {len(df)} transaction rows for league {league_id}")

    result = df.copy()

    # Add platform and league identifiers
    result["platform"] = platform
    result["league_id"] = league_id

    # Rename columns
    rename_map = {}
    for sleeper_col, canonical_col in TRANSACTION_COLUMN_MAP.items():
        if sleeper_col in result.columns and sleeper_col != canonical_col:
            rename_map[sleeper_col] = canonical_col

    # Keep sleeper_player_id for tracing
    if "sleeper_player_id" in result.columns:
        result["sleeper_player_id_original"] = result["sleeper_player_id"]

    result = result.rename(columns=rename_map)

    # Trade row expansion (duplicate_trade_rows) is handled canonically by
    # normalize_transaction_df() in canonical_transaction.py — not here.

    # Add franchise_id placeholder
    if "franchise_id" not in result.columns:
        result["franchise_id"] = None

    # Handle trade_pick rows specially - no real player to look up
    if "transaction_type" in result.columns:
        pick_trade_mask = result["transaction_type"] == "trade_pick"
        if pick_trade_mask.any():
            # Set position to PICK for draft pick trades
            if "position" not in result.columns:
                result["position"] = None
            result.loc[pick_trade_mask, "position"] = "PICK"
            # NFL_player_id should be None (no real player)
            if "NFL_player_id" not in result.columns:
                result["NFL_player_id"] = None
            result.loc[pick_trade_mask, "NFL_player_id"] = None
            log(f"  Marked {pick_trade_mask.sum()} trade_pick rows with position='PICK'")

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

    # Add faab_bid as alias for faab_spent (Yahoo compatibility)
    if "faab_spent" in result.columns and "faab_bid" not in result.columns:
        result["faab_bid"] = result["faab_spent"]

    # Add destination column (Yahoo compatibility)
    if "destination" not in result.columns:
        if "destination_team" in result.columns:
            # Map destination_team to destination type
            result["destination"] = result["destination_team"].apply(
                lambda x: "waivers" if pd.isna(x) or x == "" else "team"
            )
        else:
            result["destination"] = "team"

    # Add source_type column (Yahoo compatibility)
    if "source_type" not in result.columns:
        if "source_team" in result.columns:
            result["source_type"] = result["source_team"].apply(
                lambda x: "freeagents" if pd.isna(x) or x == "" else "team"
            )
        else:
            result["source_type"] = "freeagents"

    # Add transaction_datetime from timestamp (Yahoo compatibility)
    if "transaction_datetime" not in result.columns and "timestamp" in result.columns:
        try:
            # Convert to numeric first to avoid FutureWarning with string timestamps
            result["transaction_datetime"] = pd.to_datetime(
                pd.to_numeric(result["timestamp"], errors="coerce"), unit="ms", errors="coerce"
            )
        except Exception:
            result["transaction_datetime"] = None

    # Add player_key for Yahoo compatibility
    if "player_key" not in result.columns and "player_id" in result.columns:
        result["player_key"] = result["player_id"].apply(lambda x: f"sleeper.p.{x}" if pd.notna(x) else None)

    # Add Yahoo-compatible column aliases for UI compatibility
    if "player_id" in result.columns and "yahoo_player_id" not in result.columns:
        # Cast to string to handle mixed types (numeric for players, string like 'NE' for DEF)
        result["yahoo_player_id"] = result["player_id"].astype(str)
    # 'player' is the canonical column name - add player_name alias for backward compatibility
    if "player" in result.columns and "player_name" not in result.columns:
        result["player_name"] = result["player"]

    # Add NFL_player_id column using sleeper_nfl_player_map lookup
    # CRITICAL: Must look up the actual NFL_player_id, not just copy player_id (which is Sleeper ID)
    if "NFL_player_id" not in result.columns:
        result["NFL_player_id"] = None

    if "player_id" in result.columns:
        sleeper_map = get_sleeper_nfl_map()
        if sleeper_map:
            # Look up NFL_player_id for each row where it's missing
            mask = result["NFL_player_id"].isna()
            if mask.any():
                result.loc[mask, "NFL_player_id"] = result.loc[mask, "player_id"].apply(
                    lambda x: sleeper_map.get(str(x).replace(".0", ""), None) if pd.notna(x) else None
                )
                mapped_count = result["NFL_player_id"].notna().sum()
                log(f"  Mapped {mapped_count} NFL_player_ids from sleeper_nfl_player_map")

    # Ensure league_id is string
    if "league_id" in result.columns:
        result["league_id"] = result["league_id"].astype(str)

    # CRITICAL: Ensure sleeper_player_id_original is a clean string (no .0 float suffix)
    # This is needed for joins with sleeper_nfl_player_map which uses string IDs
    if "sleeper_player_id_original" in result.columns:
        result["sleeper_player_id_original"] = (
            result["sleeper_player_id_original"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .replace({"nan": None, "None": None, "": None})
        )

    # Add cumulative_week for transaction joins (year * 100 + week)
    if "cumulative_week" not in result.columns and "year" in result.columns and "week" in result.columns:
        result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(int)

    log(f"  Normalized columns: {list(result.columns)}")
    return result


# =============================================================================
# Type Enforcement
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

    # CRITICAL: Ensure sleeper_player_id_original is a clean string (no .0 float suffix)
    # This is needed for joins with sleeper_nfl_player_map which uses string IDs
    if "sleeper_player_id_original" in df.columns:
        df["sleeper_player_id_original"] = (
            df["sleeper_player_id_original"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .replace({"nan": None, "None": None, "": None})
        )

    return df


# =============================================================================
# Full Pipeline Normalization
# =============================================================================


def normalize_all_sleeper_data(
    player_df: pd.DataFrame | None,
    matchup_df: pd.DataFrame | None,
    draft_df: pd.DataFrame | None,
    transaction_df: pd.DataFrame | None,
    league_id: str,
    platform: str = "sleeper",
) -> dict:
    """
    Normalize all Sleeper data to canonical schemas.

    Args:
        player_df: DataFrame from fetch_sleeper_rosters()
        matchup_df: DataFrame from fetch_sleeper_matchups()
        draft_df: DataFrame from fetch_sleeper_draft()
        transaction_df: DataFrame from fetch_sleeper_transactions()
        league_id: League identifier
        platform: Platform identifier

    Returns:
        Dict with normalized DataFrames:
        {
            'player': normalized_player_df,
            'matchup': normalized_matchup_df,
            'draft': normalized_draft_df,
            'transactions': normalized_transaction_df,
        }
    """
    log(f"\n{'='*60}")
    log(f"Normalizing all Sleeper data for league {league_id}")
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
    for table, df in result.items():
        log(f"  {table}: {len(df)} rows")

    return result


# =============================================================================
# Validation
# =============================================================================


def validate_canonical_schema(df: pd.DataFrame, table_type: str) -> list:
    """
    Validate that a DataFrame has required canonical columns.

    Args:
        df: DataFrame to validate
        table_type: One of 'player', 'matchup', 'draft', 'transactions'

    Returns:
        List of validation issues (empty if valid)
    """
    required_columns = {
        "player": ["league_id", "year", "week", "player_id", "player", "position", "manager"],
        "matchup": ["league_id", "year", "week", "manager", "points", "opponent"],
        "draft": ["league_id", "year", "pick_number", "player_id", "player", "manager", "round"],
        "transactions": ["league_id", "transaction_id", "transaction_type", "player_id", "manager"],
    }

    if table_type not in required_columns:
        return [f"Unknown table type: {table_type}"]

    issues = []
    required = required_columns[table_type]

    for col in required:
        if col not in df.columns:
            issues.append(f"Missing required column: {col}")

    return issues
