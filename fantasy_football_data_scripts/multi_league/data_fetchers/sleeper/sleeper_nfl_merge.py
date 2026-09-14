"""
Sleeper-NFL Data Merger

Merges Sleeper roster data with NFLverse player stats.
Similar to yahoo_nfl_merge_v3.py but optimized for Sleeper's player ID format.

Key differences from Yahoo merge:
- Sleeper provides gsis_id for many players (direct NFL match)
- Sleeper player_id is numeric (no parsing needed)
- Name matching is fallback when gsis_id unavailable
- DST (Defense/Special Teams) merged via team-based matching

Matching Strategy for Individual Players:
1. Layer 1: Use gsis_id if available (direct NFL match)
2. Layer 2: Full normalized name + position + team + week
3. Layer 3: Last name 1:1 fallback (when unambiguous)

DST (Defense/Special Teams) Handling:
- DST players (position='DEF') are separated before merge
- Matched to NFLverse team defense stats by team abbreviation + year + week
- Provides detailed defensive stats: sacks, INTs, fumbles, points allowed, etc.

Usage:
    from sleeper_nfl_merge import merge_sleeper_nfl, merge_dst_with_nfl
    from sleeper_player_cache import SleeperPlayerCache

    # Merge with DST stats
    merged = merge_sleeper_nfl(sleeper_df, nfl_df, player_cache, nfl_defense_df=nfl_defense_df)
"""

import logging
from dataclasses import dataclass, field

import pandas as pd

from .sleeper_player_cache import SleeperPlayerCache
from .sleeper_data_normalizer import get_sleeper_nfl_map
from ..shared.nfl_player_mapping import get_sleeper_to_headshot_map
from ..shared.name_utils import normalize_name as _shared_normalize_name, apply_name_aliases

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function."""
    logger.info(msg)
    print(msg)


@dataclass
class MergeConfig:
    """Configuration for the merge process."""

    # Fantasy positions to include
    fantasy_positions: set[str] = field(
        default_factory=lambda: {"QB", "RB", "WR", "TE", "K", "DEF", "FLEX", "SUPER_FLEX", "REC_FLEX"}
    )

    # Flex position mappings
    flex_positions: dict[str, list[str]] = field(
        default_factory=lambda: {
            "FLEX": ["RB", "WR", "TE"],
            "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
            "REC_FLEX": ["WR", "TE"],
        }
    )

    # Name normalization aliases
    name_aliases: dict[str, str] = field(
        default_factory=lambda: {
            "hollywood brown": "marquise brown",
            "scotty miller": "scott miller",
            "mitch trubisky": "mitchell trubisky",
            "bill belichick": "william belichick",
        }
    )


def normalize_name(name: str, config: MergeConfig | None = None) -> str:
    """Normalize a player name for matching. Delegates to shared name_utils."""
    result = _shared_normalize_name(name)
    if config and config.name_aliases:
        result = apply_name_aliases(result, config.name_aliases)
    return result


def extract_last_name(name: str) -> str:
    """
    Extract last name for matching.

    Handles compound last names (St. Brown, Van Jefferson, etc.)

    Args:
        name: Normalized player name

    Returns:
        Last name portion
    """
    if not name:
        return ""

    parts = name.split()
    if not parts:
        return ""

    # Handle compound prefixes
    compound_prefixes = {"st", "de", "la", "van", "von", "mc", "mac", "o"}

    if len(parts) >= 3 and parts[-2].lower() in compound_prefixes:
        return " ".join(parts[-2:])

    return parts[-1]


def build_match_key(name: str, position: str, team: str, year: int, week: int) -> str:
    """
    Build a match key for player matching.

    Args:
        name: Normalized player name
        position: Position (QB, RB, etc.)
        team: NFL team abbreviation
        year: Season year
        week: Week number

    Returns:
        Match key string
    """
    return f"{name}|{position.upper()}|{team.upper()}|{year}|{week}"


def build_gsis_mapping(player_cache: SleeperPlayerCache) -> dict[str, str]:
    """
    Build mapping from Sleeper player_id to NFL gsis_id.

    Args:
        player_cache: Loaded SleeperPlayerCache

    Returns:
        Dict mapping sleeper_player_id -> gsis_id
    """
    mapping = {}

    for player_id in player_cache._players:
        gsis_id = player_cache.get_player_gsis_id(player_id)
        if gsis_id:
            mapping[player_id] = gsis_id

    log(f"Built gsis_id mapping: {len(mapping)} players with GSIS IDs")
    return mapping


def detect_def_scoring_column(scoring_settings: dict | None = None) -> str:
    """
    Detect the correct pts_def_* column based on league scoring settings.

    Sleeper's DEF scoring in the API includes IDP aggregate stats (all tackles,
    sacks, INTs from individual defensive players). We need to use NFLverse's
    team defense stats instead, which are in pts_def_* columns.

    Args:
        scoring_settings: League scoring settings dict from Sleeper API.
            If None, defaults to pts_def_std.

    Returns:
        Column name to use: 'pts_def_std' or 'pts_def_ya'.
        (pts_def_high removed 2026-04-30 — KMFFL-specific high-reward + penalty-only
        scoring is now computed per-league from league_settings, not from super_table.)
    """
    if not scoring_settings:
        return "pts_def_std"  # Safe default

    # pts_def_high path removed 2026-04-30 (KMFFL-specific). Leagues with
    # high-reward + penalty-only DEF scoring will compute it via their own
    # per-league pipeline driven by league_settings.
    if scoring_settings.get("yds_allow", 0) != 0:
        return "pts_def_ya"
    return "pts_def_std"


def merge_dst_with_nfl(sleeper_df: pd.DataFrame, nfl_defense_df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Merge DST (Defense/Special Teams) rows with NFLverse defense stats.

    DST players don't have individual gsis_ids - they match to team defensive stats.

    Args:
        sleeper_df: DataFrame with Sleeper roster data (DST rows only)
            Required columns: nfl_team, year, week
        nfl_defense_df: DataFrame with NFLverse team defense stats
            Required columns: nfl_team (or team), year, week
        verbose: If True, print merge statistics

    Returns:
        DataFrame with DST rows merged with NFL defense stats
    """
    if sleeper_df.empty:
        return sleeper_df

    if nfl_defense_df.empty:
        if verbose:
            log("NFL defense DataFrame is empty, returning DST rows unmerged")
        return sleeper_df

    if verbose:
        log(f"Merging {len(sleeper_df)} DST rows with NFL defense stats")

    # Standardize team abbreviations for matching
    TEAM_ABBREVIATION_FIXES = {
        "LA": "LAR",  # Los Angeles Rams
        "STL": "LAR",  # St. Louis Rams → LA Rams
        "SD": "LAC",  # San Diego → LA Chargers
        "OAK": "LV",  # Oakland → Las Vegas
        "WSH": "WAS",  # Washington aliases
        "JAC": "JAX",  # Jacksonville aliases
    }

    result = sleeper_df.copy()

    # Normalize team abbreviation in Sleeper data
    if "nfl_team" in result.columns:
        result["_match_team"] = result["nfl_team"].replace(TEAM_ABBREVIATION_FIXES).str.upper()
    else:
        log("Warning: nfl_team column missing from DST data")
        return result

    # Prepare NFL defense data for merge
    nfl_defense = nfl_defense_df.copy()

    # Find team column in NFL defense data
    team_col = "nfl_team" if "nfl_team" in nfl_defense.columns else "team"
    if team_col not in nfl_defense.columns:
        log("Warning: No team column found in NFL defense data (tried: nfl_team, team)")
        return result

    nfl_defense["_match_team"] = nfl_defense[team_col].replace(TEAM_ABBREVIATION_FIXES).str.upper()

    # Merge on team + year + week
    merge_cols = ["_match_team", "year", "week"]

    # Only include columns that exist
    available_merge_cols = [c for c in merge_cols if c in nfl_defense.columns]

    if len(available_merge_cols) < 3:
        log(f"Warning: Missing merge columns. Available: {available_merge_cols}")
        return result

    # Select relevant columns from NFL defense (avoid column conflicts)
    nfl_cols_to_merge = [c for c in nfl_defense.columns if c not in result.columns or c in available_merge_cols]
    nfl_defense_subset = nfl_defense[nfl_cols_to_merge].drop_duplicates(subset=available_merge_cols, keep="first")

    # Perform merge
    merged = result.merge(nfl_defense_subset, on=available_merge_cols, how="left", suffixes=("", "_def"))

    # Count matches
    if "_match_team" in merged.columns:
        merged = merged.drop(columns=["_match_team"], errors="ignore")

    if verbose:
        # Check how many rows got defense stats
        defense_cols = [
            c
            for c in merged.columns
            if c.endswith("_def")
            or c
            in [
                "sacks",
                "interceptions",
                "fumbles_recovered",
                "defensive_td",
                "points_allowed",
                "yards_allowed",
                "pass_defended",
            ]
        ]
        if defense_cols:
            matched = merged[defense_cols[0]].notna().sum()
            log(f"  DST merge: {matched}/{len(merged)} rows matched with defense stats")

    return merged


def merge_sleeper_nfl(
    sleeper_df: pd.DataFrame,
    nfl_df: pd.DataFrame,
    player_cache: SleeperPlayerCache,
    config: MergeConfig | None = None,
    verbose: bool = True,
    nfl_defense_df: pd.DataFrame | None = None,
    scoring_settings: dict | None = None,
) -> pd.DataFrame:
    """
    Merge Sleeper roster data with NFLverse stats.

    Uses a 3-layer matching strategy:
    1. GSIS ID direct match (most reliable)
    2. Full name + position + team + week
    3. Last name 1:1 fallback

    For DST players (position='DEF'), uses team-based matching with NFL defense stats.

    Args:
        sleeper_df: DataFrame with Sleeper roster data
            Required columns: sleeper_player_id, year, week
        nfl_df: DataFrame with NFLverse player stats
            Required columns: gsis_id or player_name, position, team, year, week
        player_cache: Loaded SleeperPlayerCache
        config: Optional merge configuration
        verbose: If True, print merge statistics
        nfl_defense_df: Optional DataFrame with NFL team defense stats
            If provided, DST players will be merged with this data

    Returns:
        Merged DataFrame with both Sleeper and NFL data
    """
    if sleeper_df.empty:
        log("Sleeper DataFrame is empty")
        return sleeper_df

    if nfl_df.empty:
        log("NFL DataFrame is empty")
        return sleeper_df

    config = config or MergeConfig()

    if verbose:
        log(f"\n{'='*60}")
        log("Merging Sleeper + NFL data")
        log(f"{'='*60}")
        log(f"Sleeper rows: {len(sleeper_df)}")
        log(f"NFL rows: {len(nfl_df)}")

    # =========================================================================
    # DST Handling Strategy
    # =========================================================================
    # DST players (position='DEF') can be matched via NFL_player_id since both
    # Sleeper and NFL stats use format like "DEF-CHI", "DEF-SF", etc.
    # Only separate DST rows if explicit nfl_defense_df is provided for richer stats.

    # Determine position column
    pos_col = "yahoo_position" if "yahoo_position" in sleeper_df.columns else "nfl_position"
    if pos_col not in sleeper_df.columns:
        pos_col = None

    merged_dst = pd.DataFrame()

    # Only separate DST for special handling if defense-specific data is provided
    if nfl_defense_df is not None and not nfl_defense_df.empty and pos_col:
        dst_mask = sleeper_df[pos_col].str.upper() == "DEF"
        sleeper_dst = sleeper_df[dst_mask].copy()
        sleeper_non_dst = sleeper_df[~dst_mask].copy()

        if verbose and len(sleeper_dst) > 0:
            log(f"Separated {len(sleeper_dst)} DST rows for team-based merge with defense data")

        merged_dst = merge_dst_with_nfl(sleeper_dst, nfl_defense_df, verbose=verbose)

        # Continue with non-DST rows
        if sleeper_non_dst.empty:
            if verbose:
                log("No non-DST rows to merge")
            return merged_dst if not merged_dst.empty else sleeper_df
        sleeper_df = sleeper_non_dst
    else:
        # No special defense data - DST will be matched via NFL_player_id like other players
        if verbose and pos_col:
            dst_count = (sleeper_df[pos_col].str.upper() == "DEF").sum() if pos_col in sleeper_df.columns else 0
            if dst_count > 0:
                log(f"Including {dst_count} DST rows in standard NFL_player_id matching")

    # =========================================================================
    # DEF Direct Matching: Team abbreviation → DEF-{franchise_id}
    # =========================================================================
    # DEF entries are deterministic: every team plays every week. We have
    # ABBREV_TO_FRANCHISE_ID to map team abbreviations to franchise IDs.
    # Set their gsis_id directly so they merge via Layer 1 — no name matching.
    from .sleeper_data_normalizer import ABBREV_TO_FRANCHISE_ID

    sleeper_df = sleeper_df.copy()

    if pos_col and pos_col in sleeper_df.columns:
        def_mask = sleeper_df[pos_col].astype(str).str.upper().isin(["DEF", "DST", "D/ST"])
    else:
        def_mask = pd.Series(False, index=sleeper_df.index)

    # Also catch rows where position is 'Unknown' but sleeper_player_id is a team abbreviation
    if "sleeper_player_id" in sleeper_df.columns:
        abbrev_mask = (
            sleeper_df["sleeper_player_id"].astype(str).str.upper().str.strip().isin(ABBREV_TO_FRANCHISE_ID.keys())
        )
        def_mask = def_mask | abbrev_mask

    if def_mask.any():
        # Get team abbreviation from sleeper_player_id (e.g., 'TB', 'OAK')
        team_col_for_def = "nfl_team" if "nfl_team" in sleeper_df.columns else "team"
        if team_col_for_def in sleeper_df.columns:
            def_teams = sleeper_df.loc[def_mask, team_col_for_def].astype(str).str.upper().str.strip()
        else:
            def_teams = sleeper_df.loc[def_mask, "sleeper_player_id"].astype(str).str.upper().str.strip()

        def_nfl_ids = def_teams.map(
            lambda t: f"DEF-{ABBREV_TO_FRANCHISE_ID[t]}" if t in ABBREV_TO_FRANCHISE_ID else None
        )
        sleeper_df.loc[def_mask, "_def_nfl_id"] = def_nfl_ids

        matched_def = def_nfl_ids.notna().sum()
        if verbose and matched_def > 0:
            log(f"DEF direct match: {matched_def} DEF rows mapped to franchise IDs (bypassing name matching)")
    else:
        sleeper_df["_def_nfl_id"] = None

    # Build GSIS mapping from player cache
    gsis_mapping = build_gsis_mapping(player_cache)

    # Add gsis_id to Sleeper data
    # Priority: 1) DEF direct match, 2) cache mapping, 3) NFL_player_id, 4) sleeper_nfl_player_map
    sleeper_df["_cache_gsis"] = sleeper_df["sleeper_player_id"].map(gsis_mapping)

    # Get NFL_player_id either from existing column or from sleeper_nfl_player_map
    if "NFL_player_id" in sleeper_df.columns:
        # Clean existing NFL_player_id - remove 'None' strings and whitespace
        clean_nfl_id = sleeper_df["NFL_player_id"].astype(str).str.strip()
        clean_nfl_id = clean_nfl_id.replace({"None": None, "nan": None, "": None, "NaN": None})
    else:
        # NFL_player_id not in data - look it up from sleeper_nfl_player_map
        # This happens when merge is called before normalize_player_data
        sleeper_nfl_map = get_sleeper_nfl_map()
        if sleeper_nfl_map:
            # Get player_id column (could be 'sleeper_player_id', 'player_id', or just 'player_id')
            id_col = "sleeper_player_id" if "sleeper_player_id" in sleeper_df.columns else "player_id"
            if id_col in sleeper_df.columns:
                clean_nfl_id = sleeper_df[id_col].astype(str).map(sleeper_nfl_map)
                if verbose:
                    mapped_count = clean_nfl_id.notna().sum()
                    log(f"Looked up {mapped_count} NFL_player_ids from sleeper_nfl_player_map")
            else:
                clean_nfl_id = pd.Series([None] * len(sleeper_df), index=sleeper_df.index)
        else:
            clean_nfl_id = pd.Series([None] * len(sleeper_df), index=sleeper_df.index)

    # Use DEF direct match first, then cache gsis_id, then NFL_player_id fallback
    sleeper_df["gsis_id"] = sleeper_df["_def_nfl_id"].fillna(sleeper_df["_cache_gsis"]).fillna(clean_nfl_id)

    if verbose:
        def_direct = sleeper_df["_def_nfl_id"].notna().sum()
        cache_count = (sleeper_df["_def_nfl_id"].isna() & sleeper_df["_cache_gsis"].notna()).sum()
        nfl_id_fill = (sleeper_df["_def_nfl_id"].isna() & sleeper_df["_cache_gsis"].isna() & clean_nfl_id.notna()).sum()
        log(
            f"GSIS ID sources: {def_direct} DEF direct, {cache_count} from cache, {nfl_id_fill} from NFL_player_id fallback"
        )

    sleeper_df = sleeper_df.drop(columns=["_cache_gsis", "_def_nfl_id"])

    # Track match statistics
    stats = {
        "total": len(sleeper_df),
        "gsis_match": 0,
        "name_match": 0,
        "last_name_match": 0,
        "unmatched": 0,
    }

    # =========================================================================
    # Layer 1: GSIS ID Direct Match
    # =========================================================================
    # Check for gsis_id column in NFL data (could be 'gsis_id' or 'NFL_player_id')
    nfl_gsis_col = None
    if "gsis_id" in nfl_df.columns:
        nfl_gsis_col = "gsis_id"
    elif "NFL_player_id" in nfl_df.columns:
        # Super table uses NFL_player_id which is the gsis format (00-XXXXXXX)
        nfl_gsis_col = "NFL_player_id"

    if nfl_gsis_col:
        # Strip whitespace from gsis_id (Sleeper data sometimes has leading spaces)
        sleeper_df = sleeper_df.copy()
        sleeper_df["gsis_id"] = (
            sleeper_df["gsis_id"].astype(str).str.strip().replace({"nan": None, "": None, "None": None})
        )

        # Rows with GSIS IDs
        sleeper_with_gsis = sleeper_df[sleeper_df["gsis_id"].notna()].copy()
        sleeper_without_gsis = sleeper_df[sleeper_df["gsis_id"].isna()].copy()

        if not sleeper_with_gsis.empty:
            # Prepare NFL data for merge
            nfl_for_merge = nfl_df.copy()
            if nfl_gsis_col != "gsis_id":
                # Create gsis_id alias for merge
                nfl_for_merge["gsis_id"] = nfl_for_merge[nfl_gsis_col]
            # Strip whitespace from NFL gsis_id too
            nfl_for_merge["gsis_id"] = nfl_for_merge["gsis_id"].astype(str).str.strip()

            # Merge on gsis_id + year + week
            merged_gsis = sleeper_with_gsis.merge(
                nfl_for_merge, on=["gsis_id", "year", "week"], how="left", suffixes=("", "_nfl")
            )

            # Count matches - check for any NFL column that's not null
            # Look for columns that came from NFL (stat columns)
            nfl_stat_cols = [
                c
                for c in merged_gsis.columns
                if c.endswith("_nfl")
                or c in ["passing_yards", "rushing_yards", "receiving_yards", "receptions", "targets"]
            ]
            if nfl_stat_cols:
                matched_mask = merged_gsis[nfl_stat_cols].notna().any(axis=1)
                stats["gsis_match"] = matched_mask.sum()
            else:
                stats["gsis_match"] = 0

            if verbose:
                log(f"Layer 1 (GSIS via {nfl_gsis_col}): {stats['gsis_match']} matches")
        else:
            merged_gsis = pd.DataFrame()
    else:
        sleeper_without_gsis = sleeper_df.copy()
        merged_gsis = pd.DataFrame()

    # =========================================================================
    # Layer 2: Full Name + Position + Team Match
    # =========================================================================
    if not sleeper_without_gsis.empty:
        # Build match keys for Sleeper data
        sleeper_without_gsis["_normalized_name"] = sleeper_without_gsis["player"].apply(
            lambda x: normalize_name(x, config)
        )

        # Get position and team from cache
        sleeper_without_gsis["_position"] = sleeper_without_gsis["sleeper_player_id"].apply(
            lambda x: player_cache.get_player_position(str(x))
        )
        sleeper_without_gsis["_team"] = sleeper_without_gsis["sleeper_player_id"].apply(
            lambda x: player_cache.get_player_team(str(x))
        )

        sleeper_without_gsis["_match_key"] = sleeper_without_gsis.apply(
            lambda r: build_match_key(
                r["_normalized_name"], r["_position"] or "", r["_team"] or "", r["year"], r["week"]
            ),
            axis=1,
        )

        # Build match keys for NFL data
        nfl_df = nfl_df.copy()

        # Handle different column names in NFL data
        name_col = "player_name" if "player_name" in nfl_df.columns else "player"
        pos_col = "position" if "position" in nfl_df.columns else "pos"
        team_col = "team" if "team" in nfl_df.columns else "recent_team"

        nfl_df["_normalized_name"] = nfl_df[name_col].apply(lambda x: normalize_name(x, config) if pd.notna(x) else "")

        nfl_df["_match_key"] = nfl_df.apply(
            lambda r: build_match_key(
                r["_normalized_name"], str(r.get(pos_col, "")), str(r.get(team_col, "")), r["year"], r["week"]
            ),
            axis=1,
        )

        # Create lookup dict for NFL data (drop duplicates to ensure unique index)
        # Keep first occurrence - if a player has multiple entries for same week, use first
        nfl_dedup = nfl_df.drop_duplicates(subset=["_match_key"], keep="first")
        nfl_lookup = nfl_dedup.set_index("_match_key").to_dict("index")

        # Match by key
        matched_rows = []
        unmatched_rows = []

        for idx, row in sleeper_without_gsis.iterrows():
            key = row["_match_key"]
            if key in nfl_lookup:
                # Found match
                nfl_row = nfl_lookup[key]
                combined = {**row.to_dict(), **{f"{k}_nfl": v for k, v in nfl_row.items() if not k.startswith("_")}}
                matched_rows.append(combined)
                stats["name_match"] += 1
            else:
                unmatched_rows.append(row.to_dict())

        if verbose:
            log(f"Layer 2 (Name): {stats['name_match']} matches")

        # =====================================================================
        # Layer 3: Last Name 1:1 Match (for remaining unmatched)
        # =====================================================================
        if unmatched_rows:
            # Build last name index for NFL data
            last_name_index: dict[str, list[str]] = {}
            for key, row in nfl_lookup.items():
                last = extract_last_name(row["_normalized_name"])
                pos = str(row.get(pos_col, "")).upper()
                year = row.get("year")
                week = row.get("week")
                lookup_key = f"{last}|{pos}|{year}|{week}"

                if lookup_key not in last_name_index:
                    last_name_index[lookup_key] = []
                last_name_index[lookup_key].append(key)

            # Try last name matching
            final_unmatched = []
            for row in unmatched_rows:
                last = extract_last_name(row["_normalized_name"])
                pos = row.get("_position", "").upper()
                year = row.get("year")
                week = row.get("week")
                lookup_key = f"{last}|{pos}|{year}|{week}"

                candidates = last_name_index.get(lookup_key, [])

                if len(candidates) == 1:
                    # Unique match
                    nfl_key = candidates[0]
                    nfl_row = nfl_lookup[nfl_key]
                    combined = {**row, **{f"{k}_nfl": v for k, v in nfl_row.items() if not k.startswith("_")}}
                    matched_rows.append(combined)
                    stats["last_name_match"] += 1
                else:
                    final_unmatched.append(row)

            if verbose:
                log(f"Layer 3 (Last Name): {stats['last_name_match']} matches")

            unmatched_rows = final_unmatched

        # Combine results
        merged_name = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
        unmatched_df = pd.DataFrame(unmatched_rows) if unmatched_rows else pd.DataFrame()
    else:
        merged_name = pd.DataFrame()
        unmatched_df = pd.DataFrame()

    # =========================================================================
    # Combine All Results
    # =========================================================================
    result_parts = []

    if not merged_gsis.empty:
        result_parts.append(merged_gsis)
    if not merged_name.empty:
        result_parts.append(merged_name)
    if not unmatched_df.empty:
        result_parts.append(unmatched_df)

    if result_parts:
        result = pd.concat(result_parts, ignore_index=True)
    else:
        result = sleeper_df.copy()

    # =========================================================================
    # Ensure ALL rows have valid NFL_player_id and player_week
    # =========================================================================
    # CRITICAL: Players who were rostered but didn't play in the NFL game won't
    # have a match in the super_table. We must preserve their Sleeper data with
    # a valid player_week key so they're not lost in downstream processing.
    #
    # For unmatched rows:
    # - NFL_player_id comes from gsis_id (already set from sleeper_nfl_player_map)
    # - player_week = NFL_player_id_year_week
    # - fantasy_points = Sleeper's 'points' (0 for players who didn't play)

    if "gsis_id" in result.columns:
        # Ensure NFL_player_id is set from gsis_id for all rows
        if "NFL_player_id" not in result.columns:
            result["NFL_player_id"] = None

        # Fill missing NFL_player_id from gsis_id
        mask = result["NFL_player_id"].isna() | (result["NFL_player_id"] == "") | (result["NFL_player_id"] == "None")
        if mask.any():
            result.loc[mask, "NFL_player_id"] = result.loc[mask, "gsis_id"]
            if verbose:
                log(f"  Filled {mask.sum()} missing NFL_player_id values from gsis_id")

    # Ensure player_week is set for all rows
    if "year" in result.columns and "week" in result.columns:
        if "player_week" not in result.columns:
            result["player_week"] = None

        # Build player_week from NFL_player_id + year + week
        # IMPORTANT: Only build when NFL_player_id is NOT NULL to avoid malformed keys like '_2021_5'
        id_col = "NFL_player_id" if "NFL_player_id" in result.columns else "gsis_id"
        if id_col in result.columns:
            # Only process rows that need player_week AND have a valid ID
            needs_player_week = (
                result["player_week"].isna() | (result["player_week"] == "") | (result["player_week"] == "None")
            )
            has_valid_id = result[id_col].notna() & (result[id_col].astype(str).str.strip() != "")
            mask = needs_player_week & has_valid_id
            if mask.any():
                result.loc[mask, "player_week"] = (
                    result.loc[mask, id_col].astype(str).str.strip()
                    + "_"
                    + result.loc[mask, "year"].astype(str)
                    + "_"
                    + result.loc[mask, "week"].astype(str)
                )
                if verbose:
                    log(f"  Built {mask.sum()} missing player_week values")

            # For rows with missing NFL_player_id, try sleeper_player_id as fallback.
            # Set BOTH player_week AND NFL_player_id to the SLP-prefixed placeholder
            # so downstream joins and validators can treat these rows consistently.
            still_missing = (
                result["player_week"].isna() | (result["player_week"] == "") | (result["player_week"] == "None")
            )
            if still_missing.any() and "sleeper_player_id" in result.columns:
                has_sleeper_id = result["sleeper_player_id"].notna() & (
                    result["sleeper_player_id"].astype(str).str.strip() != ""
                )
                fallback_mask = still_missing & has_sleeper_id
                if fallback_mask.any():
                    # Use SLP- prefix (with dash) to match validator pattern: [A-Za-z0-9\-]+_\d{4}_\d+
                    placeholder_id = "SLP-" + result.loc[fallback_mask, "sleeper_player_id"].astype(str).str.strip()
                    result.loc[fallback_mask, "NFL_player_id"] = placeholder_id
                    result.loc[fallback_mask, "player_week"] = (
                        placeholder_id
                        + "_"
                        + result.loc[fallback_mask, "year"].astype(str)
                        + "_"
                        + result.loc[fallback_mask, "week"].astype(str)
                    )
                    if verbose:
                        log(
                            f"  Built {fallback_mask.sum()} player_week + NFL_player_id values using sleeper_player_id fallback"
                        )

    # Ensure fantasy_points is populated from NFL data or Sleeper points
    # Priority: fantasy_points_nfl (from NFL merge) > fantasy_points > points (from Sleeper) > 0
    #
    # After merge, NFL columns get _nfl suffix. The original 'fantasy_points' column
    # may be from Sleeper (likely NULL/0) or may not exist. We need to:
    # 1. Copy fantasy_points_nfl to fantasy_points where available
    # 2. Fall back to Sleeper's 'points' column
    # 3. Default remaining to 0

    # Step 1: Ensure fantasy_points column exists
    if "fantasy_points" not in result.columns:
        result["fantasy_points"] = None

    # Step 2: Copy from fantasy_points_nfl where available (NFL data takes priority)
    if "fantasy_points_nfl" in result.columns:
        nfl_fp_available = result["fantasy_points_nfl"].notna() & (result["fantasy_points_nfl"] != 0)
        if nfl_fp_available.any():
            result.loc[nfl_fp_available, "fantasy_points"] = result.loc[nfl_fp_available, "fantasy_points_nfl"]
            if verbose:
                log(f"  Copied {nfl_fp_available.sum()} fantasy_points from NFL data")

    # Step 3: Fill remaining NULLs from Sleeper's 'points' column
    # IMPORTANT: Exclude DEF positions - Sleeper 'points' includes IDP aggregate stats
    # which inflates DEF scores to 100+. DEF will get correct pts_def_std later.
    null_fp_mask = result["fantasy_points"].isna() | (result["fantasy_points"] == 0)
    if null_fp_mask.any():
        if "points" in result.columns:
            # Exclude DEF positions from Sleeper points fill
            is_def = result["position"] == "DEF" if "position" in result.columns else False
            sleeper_pts_available = null_fp_mask & result["points"].notna() & (result["points"] != 0) & ~is_def
            if sleeper_pts_available.any():
                result.loc[sleeper_pts_available, "fantasy_points"] = result.loc[sleeper_pts_available, "points"]
                if verbose:
                    log(f"  Filled {sleeper_pts_available.sum()} fantasy_points from Sleeper points (excluding DEF)")

    # Step 4: Default remaining NULLs to 0 (player rostered but didn't play)
    final_null_mask = result["fantasy_points"].isna()
    if final_null_mask.any():
        result.loc[final_null_mask, "fantasy_points"] = 0.0
        if verbose:
            log(f"  Defaulted {final_null_mask.sum()} remaining fantasy_points to 0")

    # Clean up temporary columns
    temp_cols = [c for c in result.columns if c.startswith("_")]
    result = result.drop(columns=temp_cols, errors="ignore")

    stats["unmatched"] = stats["total"] - stats["gsis_match"] - stats["name_match"] - stats["last_name_match"]

    # =========================================================================
    # Add DST rows back to result
    # =========================================================================
    if not merged_dst.empty:
        result = pd.concat([result, merged_dst], ignore_index=True)
        stats["dst_rows"] = len(merged_dst)
        # Update total to include DST rows
        stats["total"] = stats["total"] + stats["dst_rows"]

    # =========================================================================
    # DEF scoring: trust Sleeper API's per-player points.
    #
    # Historical note: an older version of this code self-computed DEF points
    # from pts_def_* modular columns, on the theory that Sleeper's per-player
    # "points" field for DEF rows aggregated IDP stats and was therefore
    # unreliable. That workaround is stale — empirical testing against the
    # live Sleeper API (/league/{id}/matchups/{week}) shows that Sleeper
    # returns clean, integer-valued team DEF scores even in leagues with both
    # team DEF and IDP slots (verified on spqr_dynasty 2024 weeks 6/10, 1 DEF
    # + 8 IDP slots). Self-computing introduced drift because the recomputed
    # value occasionally differed from Sleeper's authoritative score by 1-2
    # points per DEF, cascading into optimal_gte_team_points failures across
    # ~219 Sleeper leagues.
    #
    # Sleeper's team_points (matchup API) and per-player DEF points (player
    # API) are consistent, so we trust both. Unrostered DEF scoring for
    # optimal lineup calculations is handled separately via expand_to_all_nfl
    # using league-specific def_multipliers.

    # =========================================================================
    # Fill missing headshot URLs from NFL merge and sleeper_nfl_player_map
    # =========================================================================
    # Step 1: Ensure headshot_url column exists
    if "headshot_url" not in result.columns:
        result["headshot_url"] = None

    # Step 2: Copy from headshot_url_nfl (created by merge suffix) where available
    # This happens when NFL data has headshot_url and Sleeper data does too (gets _nfl suffix)
    if "headshot_url_nfl" in result.columns:
        nfl_headshot_available = result["headshot_url_nfl"].notna()
        if nfl_headshot_available.any():
            result.loc[nfl_headshot_available, "headshot_url"] = result.loc[nfl_headshot_available, "headshot_url_nfl"]
            if verbose:
                log(f"  Copied {nfl_headshot_available.sum()} headshot URLs from NFL data")
        # Drop the _nfl suffix column to avoid duplication
        result = result.drop(columns=["headshot_url_nfl"], errors="ignore")

    # Step 3: Fill remaining missing headshots from sleeper_nfl_player_map
    if result["headshot_url"].isna().any():
        headshot_map = get_sleeper_to_headshot_map()
        if headshot_map:
            # Determine which column to use for lookup
            id_col = "sleeper_player_id" if "sleeper_player_id" in result.columns else "player_id"
            if id_col in result.columns:
                # Fill missing headshots from mapping
                missing_mask = result["headshot_url"].isna()
                if missing_mask.any():
                    result.loc[missing_mask, "headshot_url"] = result.loc[missing_mask, id_col].apply(
                        lambda x: headshot_map.get(str(x)) if pd.notna(x) else None
                    )
                    filled_count = missing_mask.sum() - result["headshot_url"].isna().sum()
                    if verbose and filled_count > 0:
                        log(f"  Filled {filled_count} missing headshot URLs from sleeper_nfl_player_map")

    # =========================================================================
    # Deduplicate columns (can happen from merge/concat operations)
    # =========================================================================
    if result.columns.duplicated().any():
        dup_cols = result.columns[result.columns.duplicated()].tolist()
        if verbose:
            log(f"  Deduplicating {len(dup_cols)} duplicate columns: {dup_cols[:5]}...")

        # Keep first occurrence of each column
        seen = set()
        keep_indices = []
        for i, col in enumerate(result.columns):
            if col not in seen:
                seen.add(col)
                keep_indices.append(i)
        result = result.iloc[:, keep_indices]

    # =========================================================================
    # Final dedup: one row per (year, week, NFL_player_id).
    # The Sleeper API can return the same player on two managers' rosters in a
    # single week when a mid-week trade occurs. Keep the started copy (or the
    # first if neither is started) so downstream optimal-lineup and validator
    # checks see exactly one owner per player per week.
    # =========================================================================
    cross_mgr_dedup = ["year", "week", "NFL_player_id"]
    if all(c in result.columns for c in cross_mgr_dedup):
        before_count = len(result)
        if "is_started" in result.columns:
            result = result.sort_values(cross_mgr_dedup + ["is_started"], ascending=[True, True, True, False])
        result = result.drop_duplicates(subset=cross_mgr_dedup, keep="first")
        after_count = len(result)
        if before_count > after_count and verbose:
            log(
                f"  Deduplicated {before_count - after_count} cross-manager duplicate rows by (year, week, NFL_player_id)"
            )

    # Also dedup within same manager (different merge paths can produce dupes)
    same_mgr_dedup = ["year", "week", "manager", "NFL_player_id"]
    if all(c in result.columns for c in same_mgr_dedup):
        before_count = len(result)
        result = result.drop_duplicates(subset=same_mgr_dedup, keep="first")
        after_count = len(result)
        if before_count > after_count and verbose:
            log(
                f"  Deduplicated {before_count - after_count} same-manager duplicate rows by (year, week, manager, NFL_player_id)"
            )

    if verbose:
        log("\nMerge Summary:")
        log(f"  Total rows: {stats['total']}")
        log(f"  GSIS matches: {stats['gsis_match']} ({100*stats['gsis_match']/stats['total']:.1f}%)")
        log(f"  Name matches: {stats['name_match']} ({100*stats['name_match']/stats['total']:.1f}%)")
        log(f"  Last name matches: {stats['last_name_match']} ({100*stats['last_name_match']/stats['total']:.1f}%)")
        if "dst_rows" in stats:
            log(f"  DST rows (team merge): {stats['dst_rows']} ({100*stats['dst_rows']/stats['total']:.1f}%)")
        log(f"  Unmatched: {stats['unmatched']} ({100*stats['unmatched']/stats['total']:.1f}%)")

    return result
