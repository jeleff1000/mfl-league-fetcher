#!/usr/bin/env python3
"""
Update NFL Super Table with Latest NFLverse Data.

This script fetches the latest week's data from NFLverse and updates
the super table in MotherDuck. Run this ONCE before league updates
so all leagues pull from the same fresh data.

TWO-TRACK ARCHITECTURE:
- Track 1 (this script): Updates universal NFL super table
- Track 2: Creates league-specific fantasy tables (per-platform importers)

The player_week column serves as the primary join key between tracks:
    player_week = "{NFL_player_id}_{year}_{week}"

Usage:
    python update_nfl_super_table.py --year 2024
    python update_nfl_super_table.py --year 2024 --week 14
    python update_nfl_super_table.py --auto  # Auto-detect current week

Callable from pipeline:
    from update_nfl_super_table import update_super_table
    update_super_table(year=2024, week=14)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.data_fetchers.kicking_stat_guards import sanitize_non_kicker_kicking_leaks
from multi_league.data_fetchers.pbp_scoring_enrichment import enrich_with_pbp_scoring_stats
from multi_league.data_fetchers.player_identity_guards import (
    apply_known_context_identity_rebuilds,
    apply_known_identity_repairs,
    apply_known_special_teams_identity_repairs,
    apply_known_stat_family_splits,
)

# Import the existing NFLverse fetchers
from multi_league.data_fetchers.nfl_offense_stats import fetch_nflverse_player_stats
from multi_league.data_fetchers.defense_stats import process_one_year as fetch_nflverse_defense_stats
from multi_league.data_fetchers.fantasy_points_calculator import (
    calculate_all_fantasy_points,
    calculate_all_ranks,
    calculate_composite_fantasy_points,
    calculate_season_position_ranks,
    calculate_season_flex_ranks,
    calculate_alltime_position_ranks,
    calculate_alltime_flex_ranks,
    SEASON_POSITION_RANK_COLUMNS,
    SEASON_FLEX_RANK_COLUMNS,
    ALLTIME_POSITION_RANK_COLUMNS,
    ALLTIME_FLEX_RANK_COLUMNS,
)

from multi_league.transformations.player.modules.ppg_precompute import calculate_all_ppg_metrics

# Import aggregate functions for season/career tables
from multi_league.data_fetchers.aggregate_nfl_stats import update_aggregates

# Import franchise-based DEF ID functions
try:
    from nfl_data.nfl_franchises import get_def_player_id, get_def_display_name

    FRANCHISE_FUNCTIONS_AVAILABLE = True
except ImportError:
    FRANCHISE_FUNCTIONS_AVAILABLE = False
    get_def_player_id = None
    get_def_display_name = None


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [NFL-UPDATE] {msg}")


def generate_player_week(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate player_week composite key for joining tracks.

    Format: {NFL_player_id}_{year}_{week}
    Example: 00-0023459_2024_5 (Patrick Mahomes, 2024, Week 5)
             DEF-30_2024_5 (Jacksonville DEF, 2024, Week 5)

    This key is the PRIMARY JOIN between:
    - Track 1: NFL super table (universal stats)
    - Track 2: League tables (fantasy context)
    """
    if "NFL_player_id" not in df.columns:
        log("[WARN] NFL_player_id column missing, cannot generate player_week")
        return df

    df["player_week"] = df["NFL_player_id"].astype(str) + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)

    log(f"  Generated player_week for {len(df):,} rows")
    return df


def synthesize_missing_players(current_week_df: pd.DataFrame, year: int, week: int) -> pd.DataFrame:
    """
    Synthesize placeholder rows for players missing from current week's NFLverse data.

    NFLverse only includes players who recorded stats. Players on BYE, injured,
    or with 0 stats don't appear. This function creates placeholder rows so
    Yahoo roster players can still join to the super table.

    Strategy:
    1. Get all known players from the super table (current year, previous weeks)
    2. Find players missing from current week's data
    3. Create placeholder rows with basic info (id, name, team, position) and null stats

    Args:
        current_week_df: DataFrame with current week's NFLverse data
        year: NFL season year
        week: Current week number

    Returns:
        DataFrame with synthesized placeholder rows for missing players
    """
    import duckdb
    import os

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")

    if backend == "fly":
        from multi_league.core.db_reader import get_reader

        log(f"[SYNTH] Synthesizing missing players for {year} week {week} (Fly.io)...")
        try:
            reader = get_reader()
            # Use reader for the query portion - see below for full synthesis logic
            # For now, fall through to MotherDuck if token available
            if not token:
                log("[SYNTH] No MOTHERDUCK_TOKEN, skipping synthesis")
                return pd.DataFrame()
        except Exception:
            if not token:
                log("[SYNTH] No MOTHERDUCK_TOKEN, skipping synthesis")
                return pd.DataFrame()

    if not token:
        log("[SYNTH] No MOTHERDUCK_TOKEN, skipping synthesis")
        return pd.DataFrame()

    log(f"[SYNTH] Synthesizing missing players for {year} week {week}...")

    try:
        conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

        # Get all unique players from this year's previous weeks
        # Use the most recent week's data for each player (for current team/position)
        known_players = conn.execute(f"""
            WITH ranked AS (
                SELECT
                    NFL_player_id,
                    player,
                    nfl_team,
                    nfl_position,
                    week,
                    ROW_NUMBER() OVER (PARTITION BY NFL_player_id ORDER BY week DESC) as rn
                FROM nfl_historical.nfl_player_stats_all
                WHERE year = {year}
                  AND week < {week}
                  AND NFL_player_id IS NOT NULL
                  AND data_source = 'nflverse'
            )
            SELECT NFL_player_id, player, nfl_team, nfl_position
            FROM ranked
            WHERE rn = 1
        """).fetchdf()

        conn.close()

        if known_players.empty:
            log("[SYNTH] No known players from previous weeks")
            return pd.DataFrame()

        log(f"[SYNTH] Found {len(known_players):,} known players from previous weeks")

        # Get current week's player IDs
        current_ids = set()
        if "NFL_player_id" in current_week_df.columns:
            current_ids = set(current_week_df["NFL_player_id"].dropna().unique())

        # Find missing players
        missing_mask = ~known_players["NFL_player_id"].isin(current_ids)
        missing_players = known_players[missing_mask].copy()

        if missing_players.empty:
            log("[SYNTH] No missing players to synthesize")
            return pd.DataFrame()

        log(f"[SYNTH] Creating {len(missing_players):,} placeholder rows for missing players")

        # Create placeholder rows
        missing_players["year"] = year
        missing_players["week"] = week
        missing_players["data_source"] = "synthesized"
        missing_players["data_quality_flag"] = "placeholder"
        missing_players["last_updated"] = datetime.now().isoformat()

        # Generate player_week key
        missing_players["player_week"] = (
            missing_players["NFL_player_id"].astype(str) + "_" + str(year) + "_" + str(week)
        )

        return missing_players

    except Exception as e:
        log(f"[SYNTH] Error synthesizing players: {e}")
        return pd.DataFrame()


def get_current_nfl_week() -> tuple[int, int]:
    """
    Get current NFL year and week from NFLverse schedule.

    Returns:
        Tuple of (year, week)
    """
    try:
        # Try to get from NFLverse schedule
        url = "https://github.com/nflverse/nflverse-data/releases/download/schedules/schedules.parquet"
        schedule = pd.read_parquet(url)

        today = datetime.now().date()

        # Find games that have been played
        schedule["gameday"] = pd.to_datetime(schedule["gameday"]).dt.date
        past_games = schedule[schedule["gameday"] <= today]

        if past_games.empty:
            return get_current_nfl_season_year(), 1

        # Get the most recent completed week
        latest = past_games.sort_values("gameday", ascending=False).iloc[0]
        year = int(latest["season"])
        week = int(latest["week"])

        log(f"NFLverse schedule: latest game was {year} week {week}")
        return year, week

    except Exception as e:
        log(f"Could not get week from schedule: {e}")
        # Fallback to date-based estimate
        today = datetime.now()
        year = today.year
        # Rough estimate: NFL season starts early September
        if today.month >= 9:
            week = min((today.day // 7) + 1 + (today.month - 9) * 4, 18)
        else:
            week = 1
        return year, week


def fetch_and_combine_nfl_data(
    year: int,
    week: int = None,
    *,
    synthesize_missing: bool = True,
) -> pd.DataFrame:
    """
    Fetch offense and defense stats and combine them.

    Args:
        year: NFL season year
        week: Specific week (None = all available weeks)

    Returns:
        Combined DataFrame with offense + defense stats
    """
    log(f"Fetching NFLverse data for {year}" + (f" week {week}" if week else " (all weeks)"))

    # Fetch offense stats
    log("Fetching offense stats...")
    offense_df = fetch_nflverse_player_stats(year, use_cache=False)
    log(f"  Got {len(offense_df):,} offense records")

    # Fetch defense stats
    log("Fetching defense stats...")
    defense_df = fetch_nflverse_defense_stats(year, use_cache=False)
    log(f"  Got {len(defense_df):,} defense records")

    # Filter by week if specified
    if week:
        offense_df = offense_df[offense_df["week"] == week]
        defense_df = defense_df[defense_df["week"] == week]
        log(f"  Filtered to week {week}: {len(offense_df):,} offense, {len(defense_df):,} defense")

    # Standardize column names to match super table schema
    # CRITICAL: Must include player_display_name -> player and position -> nfl_position
    # Otherwise player names and positions will be NULL in the super table
    #
    # IMPORTANT: NFLverse has both player_display_name (full name like "Josh Allen")
    # and player_name (abbreviated like "J.Allen"). We ONLY want player_display_name.
    # Drop player_name entirely - it's useless abbreviated data.
    for df in [offense_df, defense_df]:
        if "player_name" in df.columns:
            df.drop(columns=["player_name"], inplace=True)

    rename_map = {
        "season": "year",
        "team": "nfl_team",
        "opponent_team": "opponent_nfl_team",
        "player_id": "NFL_player_id",
        "player_display_name": "player",  # NFLverse full name -> player
        "position": "nfl_position",  # NFLverse position column
    }
    offense_df = offense_df.rename(columns={k: v for k, v in rename_map.items() if k in offense_df.columns})
    defense_df = defense_df.rename(columns={k: v for k, v in rename_map.items() if k in defense_df.columns})
    raw_offense_for_kicking_guard = offense_df.copy()

    offense_df = enrich_with_pbp_scoring_stats(offense_df, year, week=week, use_cache=False)

    # Ensure position column exists AND is populated from nfl_position
    # Critical: Some records have nfl_position but NULL position, causing LAMAR calculation to fail
    if "nfl_position" in offense_df.columns:
        if "position" not in offense_df.columns:
            offense_df["position"] = offense_df["nfl_position"]
        else:
            # Fill NULL position values from nfl_position
            offense_df["position"] = offense_df["position"].fillna(offense_df["nfl_position"])
    if "nfl_position" in defense_df.columns:
        if "position" not in defense_df.columns:
            defense_df["position"] = defense_df["nfl_position"]
        else:
            # Fill NULL position values from nfl_position
            defense_df["position"] = defense_df["position"].fillna(defense_df["nfl_position"])

    # Normalize IDP positions to fantasy categories (DB/LB/DL)
    # This is the universal standard for IDP fantasy leagues
    IDP_POSITION_MAP = {
        # Defensive backs
        "SAF": "DB",
        "CB": "DB",
        "S": "DB",
        "FS": "DB",
        "SS": "DB",
        # Linebackers
        "ILB": "LB",
        "OLB": "LB",
        "MLB": "LB",
        # Defensive line
        "DE": "DL",
        "DT": "DL",
        "NT": "DL",
        "ED": "DL",
        "EDGE": "DL",
    }
    for df in [offense_df, defense_df]:
        if "nfl_position" in df.columns:
            df["nfl_position"] = df["nfl_position"].replace(IDP_POSITION_MAP)
        if "position" in df.columns:
            df["position"] = df["position"].replace(IDP_POSITION_MAP)

    # Combine offense and defense
    # Offense = individual player stats (QBs, RBs, WRs, etc.)
    # Defense = team DST stats (DEF-XX)
    # These are SEPARATE record types with different NFL_player_id formats,
    # so we CONCAT them instead of merging (LEFT JOIN would discard DST rows)
    if not defense_df.empty:
        log(f"Combining {len(offense_df):,} offense + {len(defense_df):,} defense records via CONCAT")
        # Remove duplicate columns before concat (can cause InvalidIndexError)
        if offense_df.columns.duplicated().any():
            log("  Removing duplicate columns from offense_df...")
            offense_df = offense_df.loc[:, ~offense_df.columns.duplicated()]
        if defense_df.columns.duplicated().any():
            log("  Removing duplicate columns from defense_df...")
            defense_df = defense_df.loc[:, ~defense_df.columns.duplicated()]
        combined = pd.concat([offense_df, defense_df], ignore_index=True)
        log(f"Combined: {len(combined):,} records")
    else:
        combined = offense_df
        log("No defense data, using offense only")

    # ==========================================================================
    # CRITICAL: Convert NFLverse's MODERN abbreviations to HISTORICAL ones
    # ==========================================================================
    # NFLverse has a data quality issue where they use current abbreviations
    # even for historical years (e.g., 'LA' for 2014 Rams when they were 'STL').
    # We need to REVERSE this to preserve accurate historical data.
    #
    # Mapping: NFLverse modern → historical (based on year)
    # - LA (NFLverse) → STL for 1995-2015 (Rams in St. Louis)
    # - LAC (NFLverse) → SD for 1961-2016 (Chargers in San Diego)
    # - LV (NFLverse) → OAK for 1960-2019 (Raiders in Oakland)
    # ==========================================================================
    def get_historical_abbrev(modern_abbrev, year):
        """Convert abbreviations to Stathead-style canonical abbreviations.

        Returns canonical abbreviations based on franchise_eras table.
        These match the Stathead data source style (RAI, RAM, SDG, GNB, NWE, etc.).

        Stathead Canonical Abbreviations (key differences from common abbreviations):
        - Packers: GNB (not GB)
        - Saints: NOR (not NO)
        - 49ers: SFO (not SF)
        - Buccaneers: TAM (not TB)
        - Patriots: NWE (not NE) for 1971+
        - Chiefs: KAN (not KC) for 1963+
        - Chargers: SDG (not SD) for 1961-2016
        - Raiders: RAI (not LA) for 1982-1994
        - Rams: RAM (not LAR) for 1946-1994
        """
        if pd.isna(modern_abbrev) or pd.isna(year):
            return modern_abbrev

        year = int(year)
        abbrev = str(modern_abbrev).upper()

        # === STATIC FRANCHISE CONVERSIONS (Stathead style) ===
        # These teams never moved but use Stathead-specific abbreviations

        # Packers (franchise_id=7): GB -> GNB
        if abbrev == "GB":
            return "GNB"

        # Saints (franchise_id=11): NO -> NOR
        if abbrev == "NO":
            return "NOR"

        # 49ers (franchise_id=15): SF -> SFO
        if abbrev == "SF":
            return "SFO"

        # Buccaneers (franchise_id=12): TB -> TAM
        if abbrev == "TB":
            return "TAM"

        # === RELOCATED FRANCHISE CONVERSIONS ===

        # === CARDINALS (franchise_id=13) ===
        # CRD(1920-1959) -> STL(1960-1987) -> PHO(1988-1993) -> ARI(1994+)
        # Note: CRD used for Chicago Cardinals era to distinguish from Bears CHI
        if (
            abbrev in ("ARI", "PHO", "CRD", "SLC")
            or (abbrev == "STL" and year <= 1987)
            or (abbrev == "CHI" and year <= 1959)
        ):
            # Check if this is Cardinals (not Bears CHI or Rams STL)
            is_cardinals = abbrev in ("ARI", "PHO", "CRD", "SLC")
            if abbrev == "STL" and year <= 1987:
                is_cardinals = True  # Cardinals were in STL 1960-1987
            if abbrev == "CHI" and year <= 1959:
                # Could be Cardinals or Bears - both were in Chicago
                # Bears franchise started 1920, Cardinals started 1920
                # Can't distinguish from abbreviation alone, skip CHI conversion
                pass

            if is_cardinals:
                if year <= 1959:
                    return "CRD"  # Chicago Cardinals (Stathead uses CRD to avoid Bears CHI conflict)
                elif year <= 1987:
                    return "STL"  # St. Louis Cardinals
                elif year <= 1993:
                    return "PHO"  # Phoenix Cardinals
                else:
                    return "ARI"  # Arizona Cardinals

        # === RAMS (franchise_id=14) ===
        # CLE(1936-1945) -> RAM(1946-1994) -> STL(1995-2015) -> LAR(2016+)
        # Note: Stathead uses RAM for 1946-1994 era, not LAR
        if (
            abbrev in ("LAR", "RAM", "LA")
            or (abbrev == "CLE" and year <= 1945)
            or (abbrev == "STL" and 1995 <= year <= 2015)
        ):
            is_rams = abbrev in ("LAR", "RAM")
            if abbrev == "CLE" and year <= 1945:
                is_rams = True  # Cleveland Rams
            if abbrev == "STL" and 1995 <= year <= 2015:
                is_rams = True  # St. Louis Rams
            if abbrev == "LA":
                # NFLverse uses LA for Rams across all years
                # Chargers only used LA in 1960 (NFLverse normalizes to LAC)
                if year <= 1959 or year >= 2016 or (1961 <= year <= 2015):
                    is_rams = True

            if is_rams:
                if year <= 1945:
                    return "CLE"  # Cleveland Rams
                elif year <= 1994:
                    return "RAM"  # Los Angeles Rams (first stint) - Stathead canonical
                elif year <= 2015:
                    return "STL"  # St. Louis Rams
                else:
                    return "LAR"  # Los Angeles Rams (second stint)

        # === PATRIOTS (franchise_id=19) ===
        # BOS(1960-1970) -> NWE(1971+)
        # Note: Stathead uses NWE, not NE
        if abbrev in ("NE", "NWE", "BOS"):
            # BOS could also be early Commanders (1932-1936) - check year
            if abbrev == "BOS" and year <= 1936:
                pass  # This is Commanders, not Patriots
            else:
                if year <= 1970:
                    return "BOS"  # Boston Patriots
                else:
                    return "NWE"  # New England Patriots - Stathead canonical

        # === COLTS (franchise_id=26) ===
        # BAL(1953-1983) -> IND(1984+)
        # Note: BAL after 1996 is Ravens (franchise_id=21)
        if abbrev in ("IND", "CLT") or (abbrev == "BAL" and year <= 1983):
            is_colts = abbrev in ("IND", "CLT")
            if abbrev == "BAL" and year <= 1983:
                is_colts = True  # Baltimore Colts

            if is_colts:
                if year <= 1983:
                    return "BAL"  # Baltimore Colts
                else:
                    return "IND"  # Indianapolis Colts

        # === TITANS/OILERS (franchise_id=28) ===
        # HOU(1960-1996) -> TEN(1997+)
        # Note: HOU after 2002 is Texans (franchise_id=25)
        # Note: NYT is New York Titans (franchise_id=20, Jets), NOT this franchise
        if abbrev in ("TEN", "OTI", "HST") or (abbrev == "HOU" and year <= 1996):
            is_titans = abbrev in ("TEN", "OTI", "HST")
            if abbrev == "HOU" and year <= 1996:
                is_titans = True  # Houston Oilers

            if is_titans:
                if year <= 1996:
                    return "HOU"  # Houston Oilers
                else:
                    return "TEN"  # Tennessee Titans

        # === JETS (franchise_id=20) ===
        # NYT(1960-1962) -> NYJ(1963+)
        # Note: NYT = New York Titans (AFL), became Jets in 1963
        if abbrev in ("NYJ", "NYT"):
            if year <= 1962:
                return "NYT"  # New York Titans
            else:
                return "NYJ"  # New York Jets

        # === LIONS (franchise_id=6) ===
        # PRT(1930-1933) -> DET(1934+)
        # Note: PRT = Portsmouth Spartans, became Detroit Lions
        if abbrev in ("DET", "PRT"):
            if year <= 1933:
                return "PRT"  # Portsmouth Spartans
            else:
                return "DET"  # Detroit Lions

        # === CHIEFS (franchise_id=30) ===
        # DAL(1960-1962) -> KAN(1963+)
        # Note: Stathead uses KAN, not KC
        if abbrev in ("KC", "KAN") or (abbrev == "DAL" and year <= 1962):
            is_chiefs = abbrev in ("KC", "KAN")
            if abbrev == "DAL" and year <= 1962:
                is_chiefs = True  # Dallas Texans

            if is_chiefs:
                if year <= 1962:
                    return "DAL"  # Dallas Texans
                else:
                    return "KAN"  # Kansas City Chiefs - Stathead canonical

        # === RAIDERS (franchise_id=31) ===
        # OAK(1960-1981) -> RAI(1982-1994) -> OAK(1995-2019) -> LV(2020+)
        # Note: Stathead uses RAI for LA era, not LA
        if abbrev in ("LV", "OAK", "RAI", "LA"):
            # Check if LA is Raiders (1982-1994) vs Rams
            if abbrev == "LA" and 1982 <= year <= 1994:
                # During 1982-1994, LA could be Raiders or Rams
                # Rams stayed in LA 1946-1994, Raiders were in LA 1982-1994
                # If we're dealing with a DEF record or explicit Raiders context, use RAI
                # Otherwise default to RAM for Rams
                pass  # Can't determine without more context, let caller handle
            else:
                if year <= 1981:
                    return "OAK"  # Oakland Raiders (first stint)
                elif year <= 1994:
                    return "RAI"  # Los Angeles Raiders - Stathead canonical
                elif year <= 2019:
                    return "OAK"  # Oakland Raiders (second stint)
                else:
                    return "LV"  # Las Vegas Raiders

        # === CHARGERS (franchise_id=32) ===
        # LAC(1960) -> SDG(1961-2016) -> LAC(2017+)
        # Note: Stathead uses SDG for San Diego era, not SD
        if abbrev in ("LAC", "SD", "SDG"):
            if year == 1960:
                return "LAC"  # Los Angeles Chargers (first year only)
            elif year <= 2016:
                return "SDG"  # San Diego Chargers - Stathead canonical
            else:
                return "LAC"  # Los Angeles Chargers

        # === COMMANDERS (franchise_id=4) ===
        # BOS(1932-1936) -> WAS(1937+)
        if abbrev in ("WAS", "WSH") or (abbrev == "BOS" and year <= 1936):
            if abbrev == "BOS" and year <= 1936:
                return "BOS"  # Boston Braves/Redskins
            return "WAS"  # Washington

        # === Other standardizations ===
        if abbrev == "JAC":
            return "JAX"

        return modern_abbrev

    log("  Converting NFLverse modern abbreviations to historical...")
    if "nfl_team" in combined.columns and "year" in combined.columns:
        combined["nfl_team"] = combined.apply(lambda row: get_historical_abbrev(row["nfl_team"], row["year"]), axis=1)
    if "opponent_nfl_team" in combined.columns and "year" in combined.columns:
        combined["opponent_nfl_team"] = combined.apply(
            lambda row: get_historical_abbrev(row["opponent_nfl_team"], row["year"]), axis=1
        )

    # Add metadata
    combined["data_source"] = "nflverse"
    combined["data_quality_flag"] = "ok"
    combined["last_updated"] = datetime.now().isoformat()

    # Normalize DEF records with franchise-based IDs and display names
    # This ensures historical relocations are handled (e.g., CHI/STL/PHO/ARI → DEF-13)
    pos_col = "position" if "position" in combined.columns else "nfl_position"
    if FRANCHISE_FUNCTIONS_AVAILABLE and (
        pos_col in combined.columns or "nfl_position" in combined.columns or "NFL_player_id" in combined.columns
    ):
        def_mask = pd.Series(False, index=combined.index)
        if pos_col in combined.columns:
            def_mask |= combined[pos_col].fillna("").astype(str).str.upper().isin({"DEF", "DST", "D/ST"})
        if "nfl_position" in combined.columns:
            def_mask |= combined["nfl_position"].fillna("").astype(str).str.upper().isin({"DEF", "DST", "D/ST"})
        if "NFL_player_id" in combined.columns:
            def_mask |= combined["NFL_player_id"].fillna("").astype(str).str.upper().str.startswith("DEF-")
        if def_mask.any():
            log(f"  Normalizing {def_mask.sum():,} DEF records with franchise-based IDs...")

            # Update NFL_player_id for DEF rows
            combined.loc[def_mask, "NFL_player_id"] = combined.loc[def_mask].apply(
                lambda row: get_def_player_id(row["nfl_team"], row["year"])
                if pd.notna(row.get("nfl_team")) and pd.notna(row.get("year"))
                else row.get("NFL_player_id", f"DEF-{row.get('nfl_team', 'UNK')}"),
                axis=1,
            )

            # Update player display name for DEF rows
            if "player" in combined.columns:
                combined.loc[def_mask, "player"] = combined.loc[def_mask].apply(
                    lambda row: get_def_display_name(row["nfl_team"], row["year"])
                    if pd.notna(row.get("nfl_team")) and pd.notna(row.get("year"))
                    else f"{row.get('nfl_team', 'Unknown')} DST",
                    axis=1,
                )

    combined = apply_known_identity_repairs(combined, log_fn=log)
    combined = apply_known_stat_family_splits(combined, log_fn=log)
    combined = apply_known_special_teams_identity_repairs(combined, log_fn=log)
    combined = apply_known_context_identity_rebuilds(combined, log_fn=log)

    # Generate player_week join key (critical for two-track architecture)
    combined = generate_player_week(combined)

    # Synthesize missing players (BYE weeks, 0 stats, etc.)
    # Only for single-week updates where we can compare to previous weeks
    if week and synthesize_missing:
        synthesized = synthesize_missing_players(combined, year, week)
        if not synthesized.empty:
            combined = pd.concat([combined, synthesized], ignore_index=True)
            log(f"  Added {len(synthesized):,} synthesized rows, total: {len(combined):,}")

    # Ensure DEF records exist for all teams that played this week
    combined = ensure_def_records(combined, year, week)

    combined = sanitize_non_kicker_kicking_leaks(
        combined,
        source_df=raw_offense_for_kicking_guard,
        min_year=1999,
        log_fn=log,
    )

    # Calculate pre-calculated fantasy points by category
    log("Calculating pre-calculated fantasy points...")
    combined = calculate_all_fantasy_points(combined)
    log(f"  Added pts_* columns for {len(combined):,} rows")

    # Calculate composite fantasy points and pre-computed ranks
    # These enable optimal lineup calculations via rank lookups instead of runtime sorting
    log("Calculating composite fantasy points and ranks...")
    combined = calculate_all_ranks(combined)
    log(f"  Added fpts_* and rank_* columns for {len(combined):,} rows")

    # Calculate pre-computed PPG metrics (season, alltime, rolling, consistency)
    # These enable fast league imports by avoiding runtime groupby/sort operations
    log("Calculating pre-computed PPG metrics...")
    combined = calculate_all_ppg_metrics(combined)
    log(f"  Added ppg_*, rolling_*, consistency_* columns for {len(combined):,} rows")

    return combined


def ensure_def_records(df: pd.DataFrame, year: int, week: int = None) -> pd.DataFrame:
    """
    Ensure DEF records exist for all NFL teams that played in the given week(s).

    NFLverse player stats don't include team defense. This function adds
    DEF rows for each team using franchise-based IDs.

    Args:
        df: Current combined DataFrame
        year: NFL season year
        week: Specific week (None = all weeks in df)

    Returns:
        DataFrame with DEF records added
    """
    if not FRANCHISE_FUNCTIONS_AVAILABLE:
        log("[DEF] Franchise functions not available, skipping DEF synthesis")
        return df

    log("[DEF] Ensuring DEF records exist...")

    # Get teams that played (from existing data)
    if "nfl_team" not in df.columns:
        log("[DEF] No nfl_team column, skipping DEF synthesis")
        return df

    # Get unique team/year/week combinations
    if week:
        weeks_to_process = [week]
    else:
        weeks_to_process = df["week"].dropna().unique().tolist()

    # Get teams that actually have source data in each week. Do not use all
    # season teams for every week; that creates missed-playoff DST shells in
    # postseason weeks. Also exclude synthesized placeholder players so bye
    # teams do not fabricate DEF rows.
    team_source_mask = df["nfl_team"].notna()
    if "data_quality_flag" in df.columns:
        team_source_mask &= ~df["data_quality_flag"].astype(str).str.lower().isin({"placeholder", "def_placeholder"})
    if "data_source" in df.columns:
        team_source_mask &= df["data_source"].astype(str).str.lower() != "synthesized"
    played_source = df.loc[team_source_mask, ["week", "nfl_team"]].copy()
    played_source["week_num"] = pd.to_numeric(played_source["week"], errors="coerce").astype("Int64")

    # Check which DEF records already exist (by franchise ID + week, not team abbrev)
    existing_def = set()
    if "NFL_player_id" in df.columns:
        def_rows = df[df["NFL_player_id"].astype(str).str.startswith("DEF-")]
        for _, row in def_rows.iterrows():
            existing_def.add((str(row.get("NFL_player_id")), int(row.get("week", 0))))

    # Create missing DEF records (one per franchise per week)
    new_def_rows = []
    for wk in weeks_to_process:
        seen_franchise_ids = set()
        teams_for_week = played_source.loc[played_source["week_num"] == int(wk), "nfl_team"].dropna().unique()
        for team in teams_for_week:
            if pd.isna(team):
                continue
            def_id = get_def_player_id(team, year)
            # Skip if we already have this franchise ID for this week
            if def_id in seen_franchise_ids:
                continue
            seen_franchise_ids.add(def_id)

            if (def_id, int(wk)) not in existing_def:
                new_def_rows.append(
                    {
                        "NFL_player_id": def_id,
                        "player": get_def_display_name(team, year),
                        "nfl_team": team,
                        "nfl_position": "DEF",
                        "position": "DEF",
                        "year": year,
                        "week": int(wk),
                        "data_source": "synthesized",
                        "data_quality_flag": "def_placeholder",
                        "last_updated": datetime.now().isoformat(),
                        "player_week": f"{def_id}_{year}_{int(wk)}",
                    }
                )

    if new_def_rows:
        log(f"[DEF] Adding {len(new_def_rows)} DEF records")
        def_df = pd.DataFrame(new_def_rows)

        # Ensure no duplicate columns before concat (can happen with complex merges)
        if df.columns.duplicated().any():
            log("[DEF] WARNING: Main df has duplicate columns, deduplicating...")
            df = df.loc[:, ~df.columns.duplicated()]

        # Reset index to avoid concat issues
        df = df.reset_index(drop=True)
        def_df = def_df.reset_index(drop=True)

        df = pd.concat([df, def_df], ignore_index=True)
    else:
        log("[DEF] All DEF records already exist")

    return df


def update_motherduck_super_table(df: pd.DataFrame, year: int, week: int = None):
    """Stub: super_table refresh is broken pending the Sep 2026 unified rebuild.

    See memory entry 'NFL ops rebuild plan' for the replacement design.
    """
    raise NotImplementedError(
        "super_table refresh broken since MotherDuck dropped 2026-04-24. "
        "The pre-existing Fly path here would have nuked all of ___ops by calling "
        "FlyTarget.replace_database with a single-table file. Will be rebuilt as a "
        "unified build_full_ops.py before next NFL season — see memory entry "
        "'NFL ops rebuild plan'."
    )


def recalculate_season_and_alltime_ranks(year: int):
    """
    Recalculate season and all-time rank columns after weekly update.

    After adding a new week's data, the season ranks for that year and the
    all-time ranks for all players need to be recalculated since aggregates changed.

    Strategy:
    1. Season ranks: Fetch all data for `year`, recalculate, update that year only
    2. All-time ranks: Process by position group to avoid memory issues,
       fetch all history, recalculate, update all rows

    Args:
        year: NFL season year to update season ranks for
    """
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[RANKS] Fly.io backend: rank recalculation handled by super table rebuild workflow")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[RANKS] No MOTHERDUCK_TOKEN, skipping rank recalculation")
        return

    log(f"[RANKS] Recalculating season and all-time ranks after {year} weekly update...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # =====================================================================
        # STEP 1: Recalculate SEASON ranks for current year
        # =====================================================================
        log(f"[RANKS] Step 1: Recalculating season ranks for {year}...")

        # Fetch all data for this year
        year_df = conn.execute(f"""
            SELECT *
            FROM nfl_historical.nfl_player_stats_all
            WHERE year = {year}
        """).fetchdf()

        if year_df.empty:
            log(f"[RANKS] No data for {year}, skipping")
            conn.close()
            return

        log(f"[RANKS]   Loaded {len(year_df):,} rows for {year}")

        # Ensure composite points exist
        year_df = calculate_composite_fantasy_points(year_df)

        # Calculate season ranks
        year_df = calculate_season_position_ranks(year_df)
        year_df = calculate_season_flex_ranks(year_df)

        # Update season rank columns in MotherDuck
        season_cols = SEASON_POSITION_RANK_COLUMNS + SEASON_FLEX_RANK_COLUMNS
        available_season_cols = [c for c in season_cols if c in year_df.columns]

        if available_season_cols:
            log(f"[RANKS]   Updating {len(available_season_cols)} season rank columns...")
            conn.register("season_ranked", year_df[["player_week"] + available_season_cols])

            set_clauses = [f"{col} = r.{col}" for col in available_season_cols]
            update_sql = f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {', '.join(set_clauses)}
                FROM season_ranked r
                WHERE t.player_week = r.player_week
                  AND t.year = {year}
            """
            conn.execute(update_sql)
            conn.unregister("season_ranked")
            log(f"[RANKS]   Season ranks updated for {year}")

        # =====================================================================
        # STEP 2: Recalculate ALL-TIME ranks (requires full history)
        # =====================================================================
        log("[RANKS] Step 2: Recalculating all-time ranks...")

        # Define position groups to process separately (reduce memory usage)
        position_groups = [
            ("QB", ["QB"]),
            ("RB", ["RB"]),
            ("WR", ["WR"]),
            ("TE", ["TE"]),
            ("K", ["K"]),
            ("DEF", ["DEF"]),
            ("LB", ["LB", "ILB", "OLB", "MLB"]),
            ("DL", ["DL", "DE", "DT", "NT", "ED"]),
            ("DB", ["DB", "CB", "S", "SS", "FS", "SAF"]),
        ]

        for group_name, positions in position_groups:
            pos_list = "', '".join(positions)

            # Fetch all history for this position group
            pos_df = conn.execute(f"""
                SELECT *
                FROM nfl_historical.nfl_player_stats_all
                WHERE nfl_position IN ('{pos_list}')
            """).fetchdf()

            if pos_df.empty:
                continue

            log(f"[RANKS]   {group_name}: Processing {len(pos_df):,} rows...")

            # Ensure composite points exist
            pos_df = calculate_composite_fantasy_points(pos_df)

            # Calculate all-time ranks for this position group
            pos_df = calculate_alltime_position_ranks(pos_df)
            pos_df = calculate_alltime_flex_ranks(pos_df)

            # Get available all-time columns
            alltime_cols = ALLTIME_POSITION_RANK_COLUMNS + ALLTIME_FLEX_RANK_COLUMNS
            available_alltime_cols = [c for c in alltime_cols if c in pos_df.columns and pos_df[c].notna().any()]

            if available_alltime_cols:
                conn.register("alltime_ranked", pos_df[["player_week"] + available_alltime_cols])

                set_clauses = [f"{col} = r.{col}" for col in available_alltime_cols]
                update_sql = f"""
                    UPDATE nfl_historical.nfl_player_stats_all t
                    SET {', '.join(set_clauses)}
                    FROM alltime_ranked r
                    WHERE t.player_week = r.player_week
                """
                conn.execute(update_sql)
                conn.unregister("alltime_ranked")

            log(f"[RANKS]   {group_name}: All-time ranks updated")

        # Also process FLEX combinations (RB/WR/TE, QB/RB/WR/TE, IDP)
        flex_groups = [
            ("FLEX", ["RB", "WR", "TE"]),
            ("SFLEX", ["QB", "RB", "WR", "TE"]),
            ("IDP", ["LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "DB", "CB", "S", "SS", "FS", "SAF"]),
        ]

        for group_name, positions in flex_groups:
            pos_list = "', '".join(positions)

            pos_df = conn.execute(f"""
                SELECT *
                FROM nfl_historical.nfl_player_stats_all
                WHERE nfl_position IN ('{pos_list}')
            """).fetchdf()

            if pos_df.empty:
                continue

            log(f"[RANKS]   {group_name}: Processing {len(pos_df):,} rows for flex ranks...")

            pos_df = calculate_composite_fantasy_points(pos_df)
            pos_df = calculate_alltime_flex_ranks(pos_df)

            # Get flex-specific columns
            flex_cols = [c for c in ALLTIME_FLEX_RANK_COLUMNS if c in pos_df.columns and pos_df[c].notna().any()]

            if flex_cols:
                conn.register("flex_ranked", pos_df[["player_week"] + flex_cols])

                set_clauses = [f"{col} = r.{col}" for col in flex_cols]
                update_sql = f"""
                    UPDATE nfl_historical.nfl_player_stats_all t
                    SET {', '.join(set_clauses)}
                    FROM flex_ranked r
                    WHERE t.player_week = r.player_week
                """
                conn.execute(update_sql)
                conn.unregister("flex_ranked")

            log(f"[RANKS]   {group_name}: Flex ranks updated")

        log("[RANKS] All rank recalculations complete")

    except Exception as e:
        log(f"[RANKS] ERROR during rank recalculation: {e}")
        raise

    finally:
        conn.close()


def update_super_table(year: int = None, week: int = None, auto: bool = False) -> int:
    """
    Callable function for pipeline integration (Track 1).

    Updates the NFL super table with latest NFLverse data.
    Call this ONCE before running league-specific Track 2.

    Args:
        year: NFL season year (default: current year or auto-detected)
        week: Specific week to update (None = all available weeks)
        auto: Auto-detect current year/week from NFLverse schedule

    Returns:
        Number of records updated

    Example:
        from update_nfl_super_table import update_super_table

        # In weekly_update_v2.py Track 1:
        count = update_super_table(auto=True)
        print(f"Updated {count} records in super table")
    """
    # Determine year and week
    if auto:
        year, week = get_current_nfl_week()
        log(f"Auto-detected: {year} week {week}")
    else:
        year = year or get_current_nfl_season_year()

    log(f"[TRACK 1] Updating NFL super table for {year}" + (f" week {week}" if week else " (all weeks)"))

    # Fetch latest data
    df = fetch_and_combine_nfl_data(year, week)

    if df.empty:
        log("[TRACK 1] No data fetched, nothing to update")
        return 0

    # Update MotherDuck
    update_motherduck_super_table(df, year, week)

    # Recalculate season and all-time ranks (they depend on full year/history)
    # Only do this for single-week updates; full year updates already have correct ranks
    if week:
        recalculate_season_and_alltime_ranks(year)

    log(f"[TRACK 1] SUCCESS - Super table updated with {len(df):,} records")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Update NFL Super Table with latest NFLverse data")
    parser.add_argument("--year", type=int, help="NFL season year (default: current)")
    parser.add_argument("--week", type=int, help="Specific week to update (default: all available)")
    parser.add_argument("--auto", action="store_true", help="Auto-detect current week from NFLverse")
    parser.add_argument("--backfill-all", action="store_true", help="Backfill all years (1999-current)")
    parser.add_argument("--start-year", type=int, default=1999, help="Start year for backfill (default: 1999)")

    args = parser.parse_args()

    # Handle backfill-all mode
    if args.backfill_all:
        return backfill_all_years(args.start_year)

    # Determine year and week
    if args.auto:
        year, week = get_current_nfl_week()
        log(f"Auto-detected: {year} week {week}")
    else:
        year = args.year or get_current_nfl_season_year()
        week = args.week

    log(f"Updating NFL super table for {year}" + (f" week {week}" if week else " (all weeks)"))

    # Ensure PPG columns exist in MotherDuck schema (one-time migration, safe to call multiple times)
    ensure_ppg_columns_exist()

    # Ensure new flex rank columns exist (W/R, R/T flex)
    ensure_rank_columns_exist()

    # Ensure new scoring columns exist (5pt pass TD, return yards, TE premium)
    ensure_scoring_columns_exist()

    # Fetch latest data
    df = fetch_and_combine_nfl_data(year, week)

    if df.empty:
        log("No data fetched, nothing to update")
        return 1

    # Update MotherDuck
    update_motherduck_super_table(df, year, week)

    # Recalculate season and all-time ranks (they depend on full year/history)
    # Only do this for single-week updates; full year updates already have correct ranks
    if week:
        recalculate_season_and_alltime_ranks(year)
        # Also recalculate PPG metrics for the year
        recalculate_ppg_metrics(year)

    # Compute research LAMAR columns (depends on rank + pts columns)
    log("Computing research LAMAR columns...")
    from multi_league.data_fetchers.research_lamar import enrich_research_lamar
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("Fly.io backend: research LAMAR computed during super table rebuild")
    else:
        md_conn = duckdb.connect("md:")
        enrich_research_lamar(md_conn, year=year)
        md_conn.close()

    # Update aggregate tables (season/career with and without playoffs)
    # This populates player_nfl_season, player_nfl_season_all, player_nfl_career, player_nfl_career_all
    log("Updating aggregate tables (season/career)...")
    update_aggregates(year=year)

    log(f"[SUCCESS] Super table updated with {len(df):,} records")
    return 0


def backfill_all_years(start_year: int = 1999):
    """
    Backfill all years from start_year to current year.

    This processes each year sequentially to populate PPG columns
    for the entire historical dataset.

    Args:
        start_year: First year to backfill (default: 1999, when NFLverse data starts)

    Returns:
        0 on success, 1 on failure
    """
    current_year = get_current_nfl_season_year()
    years = list(range(start_year, current_year + 1))

    log("=" * 60)
    log(f"BACKFILL ALL YEARS: {start_year} to {current_year} ({len(years)} years)")
    log("=" * 60)

    # Ensure PPG columns exist first (one-time migration)
    ensure_ppg_columns_exist()

    success_count = 0
    fail_count = 0

    for i, year in enumerate(years, 1):
        log("")
        log(f"[{i}/{len(years)}] Processing year {year}...")

        try:
            # Fetch data for this year
            df = fetch_and_combine_nfl_data(year, week=None)

            if df.empty:
                log(f"  [SKIP] No data for {year}")
                continue

            # Update MotherDuck
            update_motherduck_super_table(df, year, week=None)

            log(f"  [OK] {year}: {len(df):,} records")
            success_count += 1

        except Exception as e:
            # 404 errors for future years are expected, not failures
            if "404" in str(e) and year > get_current_nfl_season_year() - 1:
                log(f"  [SKIP] {year}: No data available yet")
            else:
                log(f"  [FAIL] {year}: {e}")
                fail_count += 1
            continue

    # After all years are loaded, update aggregate tables once
    log("")
    log("=" * 60)
    log("Updating aggregate tables (season/career) for all years...")
    log("=" * 60)
    update_aggregates(year=None)  # None = all years

    log("")
    log("=" * 60)
    log(f"BACKFILL COMPLETE: {success_count} succeeded, {fail_count} failed")
    log("=" * 60)

    return 0 if fail_count == 0 else 1


def ensure_ppg_columns_exist():
    """
    Add PPG columns to all MotherDuck NFL tables.

    This is a one-time migration to add pre-computed PPG columns.
    Safe to call multiple times - only adds columns that don't exist.

    Tables updated:
    - nfl_player_stats_all: All 42 PPG columns (weekly data)
    - player_nfl_season, player_nfl_season_all: 24 columns (season aggregates)
    - player_nfl_career, player_nfl_career_all: 6 columns (career aggregates)
    """
    import duckdb
    from fantasy_points_calculator import PPG_COLUMNS

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[PPG SCHEMA] Fly.io backend: schema managed by super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[PPG SCHEMA] No MOTHERDUCK_TOKEN, skipping schema update")
        return

    # Define which columns go to which table type
    # Weekly: all 42 columns
    weekly_columns = PPG_COLUMNS

    # Season: ppg_season_*, consistency_*, weighted_ppg_*, avg_pts_next_year_* (24 columns)
    season_columns = [
        c
        for c in PPG_COLUMNS
        if (
            c.startswith("ppg_season_")
            or c.startswith("consistency_")
            or c.startswith("weighted_ppg_")
            or c.startswith("avg_pts_next_year_")
        )
    ]

    # Career: ppg_alltime_* (6 columns)
    career_columns = [c for c in PPG_COLUMNS if c.startswith("ppg_alltime_")]

    # Table configurations: (table_name, columns_to_add)
    tables = [
        ("nfl_historical.nfl_player_stats_all", weekly_columns),
        ("nfl_historical.player_nfl_season", season_columns),
        ("nfl_historical.player_nfl_season_all", season_columns),
        ("nfl_historical.player_nfl_career", career_columns),
        ("nfl_historical.player_nfl_career_all", career_columns),
    ]

    log("[PPG SCHEMA] Adding PPG columns to all NFL tables...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        total_added = 0

        for table_name, columns in tables:
            # Extract schema and table for information_schema query
            schema_name = table_name.split(".")[0]
            table_only = table_name.split(".")[1]

            # Check if table exists
            table_exists = (
                conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema_name}' AND table_name = '{table_only}'
            """).fetchone()[0]
                > 0
            )

            if not table_exists:
                log(f"  [SKIP] {table_name} - table does not exist")
                continue

            # Get existing columns
            existing_cols = set(
                conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_only}'
            """)
                .fetchdf()["column_name"]
                .tolist()
            )

            # Add missing columns
            added = 0
            for col in columns:
                if col not in existing_cols:
                    conn.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {col} DOUBLE
                    """)
                    added += 1

            if added > 0:
                log(f"  [OK] {table_name}: Added {added} columns")
                total_added += added
            else:
                log(f"  [OK] {table_name}: All {len(columns)} columns exist")

        log(f"[PPG SCHEMA] Complete - added {total_added} total columns across all tables")

    except Exception as e:
        log(f"[PPG SCHEMA] ERROR: {e}")
        raise

    finally:
        conn.close()


def ensure_scoring_columns_exist():
    """
    Add new scoring columns to MotherDuck NFL tables.

    This is a migration to add 5pt passing TD, return yards, and TE premium columns.
    Safe to call multiple times - only adds columns that don't exist.

    New columns:
    - pts_pass_5pt, pts_ret_yds, pts_rec_tep: Category columns
    - fpts_5pt_0ppr, fpts_5pt_half, fpts_5pt_ppr: 5pt composite columns
    - fpts_4pt_tep, fpts_5pt_tep, fpts_6pt_tep: TE Premium composite columns
    - rank_qb_5pt, rank_te_tep: Position rank columns
    - rank_sflex_5pt_*, rank_flex_tep, rank_recflex_tep: Flex rank columns
    """
    import duckdb
    from fantasy_points_calculator import (
        PRECALC_COLUMNS,
        COMPOSITE_POINTS_COLUMNS,
        POSITION_RANK_COLUMNS,
        FLEX_RANK_COLUMNS,
        SEASON_POSITION_RANK_COLUMNS,
        SEASON_FLEX_RANK_COLUMNS,
        ALLTIME_POSITION_RANK_COLUMNS,
        ALLTIME_FLEX_RANK_COLUMNS,
    )

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[SCORING SCHEMA] Fly.io backend: schema managed by super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[SCORING SCHEMA] No MOTHERDUCK_TOKEN, skipping schema update")
        return

    # Combine all columns that need to be added
    all_columns = (
        PRECALC_COLUMNS
        + COMPOSITE_POINTS_COLUMNS
        + POSITION_RANK_COLUMNS
        + FLEX_RANK_COLUMNS
        + SEASON_POSITION_RANK_COLUMNS
        + SEASON_FLEX_RANK_COLUMNS
        + ALLTIME_POSITION_RANK_COLUMNS
        + ALLTIME_FLEX_RANK_COLUMNS
    )

    # Table configuration
    table_name = "nfl_historical.nfl_player_stats_all"

    log("[SCORING SCHEMA] Adding scoring columns to NFL tables...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # Get existing columns
        existing_cols = set(
            conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'nfl_historical' AND table_name = 'nfl_player_stats_all'
        """)
            .fetchdf()["column_name"]
            .tolist()
        )

        # Add missing columns
        added = 0
        for col in all_columns:
            if col not in existing_cols:
                # Rank columns are INTEGER, point columns are DOUBLE
                col_type = "INTEGER" if col.startswith("rank_") else "DOUBLE"
                conn.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS {col} {col_type}
                """)
                added += 1

        if added > 0:
            log(f"  [OK] {table_name}: Added {added} columns")
        else:
            log(f"  [OK] {table_name}: All columns exist")

        log(f"[SCORING SCHEMA] Complete - added {added} total columns")

    except Exception as e:
        log(f"[SCORING SCHEMA] ERROR: {e}")
        raise

    finally:
        conn.close()


def ensure_rank_columns_exist():
    """
    Add new flex rank columns to MotherDuck NFL tables.

    This is a one-time migration to add W/R and R/T flex rank columns.
    Safe to call multiple times - only adds columns that don't exist.
    """
    import duckdb
    from fantasy_points_calculator import FLEX_RANK_COLUMNS, SEASON_FLEX_RANK_COLUMNS, ALLTIME_FLEX_RANK_COLUMNS

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[RANK SCHEMA] Fly.io backend: schema managed by super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[RANK SCHEMA] No MOTHERDUCK_TOKEN, skipping schema update")
        return

    # Define which columns go to which table
    # Weekly: all flex rank columns
    weekly_columns = FLEX_RANK_COLUMNS

    # Season: season flex rank columns
    season_columns = SEASON_FLEX_RANK_COLUMNS

    # All-time: alltime flex rank columns
    alltime_columns = ALLTIME_FLEX_RANK_COLUMNS

    # Table configurations: (table_name, columns_to_add)
    tables = [
        ("nfl_historical.nfl_player_stats_all", weekly_columns + season_columns + alltime_columns),
    ]

    log("[RANK SCHEMA] Adding flex rank columns to NFL tables...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        total_added = 0

        for table_name, columns in tables:
            # Extract schema and table for information_schema query
            schema_name = table_name.split(".")[0]
            table_only = table_name.split(".")[1]

            # Check if table exists
            table_exists = (
                conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema_name}' AND table_name = '{table_only}'
            """).fetchone()[0]
                > 0
            )

            if not table_exists:
                log(f"  [SKIP] {table_name} - table does not exist")
                continue

            # Get existing columns
            existing_cols = set(
                conn.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_only}'
            """)
                .fetchdf()["column_name"]
                .tolist()
            )

            # Add missing columns
            added = 0
            for col in columns:
                if col not in existing_cols:
                    conn.execute(f"""
                        ALTER TABLE {table_name}
                        ADD COLUMN IF NOT EXISTS {col} DOUBLE
                    """)
                    added += 1

            if added > 0:
                log(f"  [OK] {table_name}: Added {added} columns")
                total_added += added
            else:
                log(f"  [OK] {table_name}: All {len(columns)} columns exist")

        log(f"[RANK SCHEMA] Complete - added {total_added} total columns across all tables")

    except Exception as e:
        log(f"[RANK SCHEMA] ERROR: {e}")
        raise

    finally:
        conn.close()


def recalculate_ppg_metrics(year: int = None):
    """
    Recalculate PPG metrics in MotherDuck after weekly update.

    After adding a new week's data, the season and alltime PPG values
    for all players in that year need to be recalculated.

    Args:
        year: NFL season year to update (None = all years)
    """
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[PPG] Fly.io backend: PPG metrics recalculated during super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[PPG] No MOTHERDUCK_TOKEN, skipping PPG recalculation")
        return

    log("[PPG] Recalculating PPG metrics" + (f" for {year}" if year else " for all years") + "...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # For each scoring variant, recalculate season and alltime PPG
        # Includes 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TE Premium variants
        variants = [
            ("4pt", "0ppr", "fpts_4pt_0ppr"),
            ("4pt", "half", "fpts_4pt_half"),
            ("4pt", "ppr", "fpts_4pt_ppr"),
            ("5pt", "0ppr", "fpts_5pt_0ppr"),
            ("5pt", "half", "fpts_5pt_half"),
            ("5pt", "ppr", "fpts_5pt_ppr"),
            ("6pt", "0ppr", "fpts_6pt_0ppr"),
            ("6pt", "half", "fpts_6pt_half"),
            ("6pt", "ppr", "fpts_6pt_ppr"),
            ("4pt", "tep", "fpts_4pt_tep"),
            ("5pt", "tep", "fpts_5pt_tep"),
            ("6pt", "tep", "fpts_6pt_tep"),
        ]

        for td, ppr, fpts_col in variants:
            season_col = f"ppg_season_{td}_{ppr}"
            alltime_col = f"ppg_alltime_{td}_{ppr}"

            # Update season PPG
            year_filter = f"WHERE year = {year}" if year else ""
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {season_col} = ROUND(season_avg.avg_pts, 2)
                FROM (
                    SELECT NFL_player_id, year, AVG({fpts_col}) as avg_pts
                    FROM nfl_historical.nfl_player_stats_all
                    {year_filter}
                    GROUP BY NFL_player_id, year
                ) season_avg
                WHERE t.NFL_player_id = season_avg.NFL_player_id
                  AND t.year = season_avg.year
                  {f"AND t.year = {year}" if year else ""}
            """)

            # Update alltime PPG (only if updating all years or this affects all-time)
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {alltime_col} = ROUND(alltime_avg.avg_pts, 2)
                FROM (
                    SELECT NFL_player_id, AVG({fpts_col}) as avg_pts
                    FROM nfl_historical.nfl_player_stats_all
                    GROUP BY NFL_player_id
                ) alltime_avg
                WHERE t.NFL_player_id = alltime_avg.NFL_player_id
                  {f"AND t.year = {year}" if year else ""}
            """)

            log(f"  Updated {season_col}, {alltime_col}")

        log("[PPG] PPG recalculation complete")

    except Exception as e:
        log(f"[PPG] ERROR during PPG recalculation: {e}")
        raise

    finally:
        conn.close()


def calculate_rolling_totals():
    """
    Calculate rolling_total columns (cumulative sum per player per season) in MotherDuck.

    This uses SQL window functions for fast batch calculation across all historical data.
    """
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[ROLLING] Fly.io backend: rolling totals calculated during super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[ROLLING] No MOTHERDUCK_TOKEN, skipping rolling total calculation")
        return

    log("[ROLLING] Calculating rolling_total columns in MotherDuck...")
    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # Includes 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TE Premium variants
        variants = [
            ("4pt", "0ppr", "fpts_4pt_0ppr"),
            ("4pt", "half", "fpts_4pt_half"),
            ("4pt", "ppr", "fpts_4pt_ppr"),
            ("5pt", "0ppr", "fpts_5pt_0ppr"),
            ("5pt", "half", "fpts_5pt_half"),
            ("5pt", "ppr", "fpts_5pt_ppr"),
            ("6pt", "0ppr", "fpts_6pt_0ppr"),
            ("6pt", "half", "fpts_6pt_half"),
            ("6pt", "ppr", "fpts_6pt_ppr"),
            ("4pt", "tep", "fpts_4pt_tep"),
            ("5pt", "tep", "fpts_5pt_tep"),
            ("6pt", "tep", "fpts_6pt_tep"),
        ]

        for td, ppr, fpts_col in variants:
            rolling_col = f"rolling_total_{td}_{ppr}"
            log(f"  Calculating {rolling_col}...")

            # Use window function to calculate cumulative sum per player per season
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {rolling_col} = ROUND(cumsum.rolling_sum, 2)
                FROM (
                    SELECT
                        player_week,
                        SUM({fpts_col}) OVER (
                            PARTITION BY NFL_player_id, year
                            ORDER BY week
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) as rolling_sum
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                ) cumsum
                WHERE t.player_week = cumsum.player_week
            """)

            log(f"  [OK] {rolling_col}")

        # Add rolling_total for DEF and K (needed for tie-breaking in rankings)
        special_positions = [
            ("rolling_total_def", "pts_def_std", "DEF"),
            ("rolling_total_k", "pts_k_yds", "K"),
        ]

        for rolling_col, pts_col, position in special_positions:
            log(f"  Calculating {rolling_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {rolling_col} = ROUND(cumsum.rolling_sum, 2)
                FROM (
                    SELECT
                        player_week,
                        SUM({pts_col}) OVER (
                            PARTITION BY NFL_player_id, year
                            ORDER BY week
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) as rolling_sum
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {pts_col} IS NOT NULL
                    AND nfl_position = '{position}'
                ) cumsum
                WHERE t.player_week = cumsum.player_week
            """)
            log(f"  [OK] {rolling_col}")

        log("[ROLLING] Rolling total calculation complete")

    except Exception as e:
        log(f"[ROLLING] ERROR: {e}")
        raise

    finally:
        conn.close()


def backfill_all_ppg_columns():
    """
    Backfill ALL 120 PPG columns for ALL years in super_table.

    This calculates PPG metrics from the existing fpts_* columns which are
    100% populated for all years (1920-present).

    Scoring variants: 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TEP = 12 variants
    Metrics per variant: 8 (ppg_season, ppg_alltime, rolling_total, rolling_3,
                           rolling_5, weighted_ppg, consistency, avg_pts_next_year)
    Total: 12 × 10 = 120 columns (rolling_total is tiebreaker, counts as 1 of 10)

    Metrics calculated:
    - ppg_season: Season average points per game
    - ppg_alltime: Career average points per game
    - rolling_total: Cumulative sum per season
    - rolling_3: 3-game rolling average
    - rolling_5: 5-game rolling average
    - weighted_ppg: Exponentially weighted moving average (approx)
    - consistency: Coefficient of variation (std/mean) per season
    - avg_pts_next_year: What player averaged the following season
    """
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("[PPG-BACKFILL] Fly.io backend: PPG columns backfilled during super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        log("[PPG-BACKFILL] No MOTHERDUCK_TOKEN")
        return

    log("[PPG-BACKFILL] ============================================================")
    log("[PPG-BACKFILL] BACKFILLING ALL 120 PPG COLUMNS FOR ALL YEARS")
    log("[PPG-BACKFILL] ============================================================")

    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # Includes 4pt/5pt/6pt pass TD × 0ppr/half/ppr + TE Premium variants
        variants = [
            ("4pt", "0ppr", "fpts_4pt_0ppr"),
            ("4pt", "half", "fpts_4pt_half"),
            ("4pt", "ppr", "fpts_4pt_ppr"),
            ("5pt", "0ppr", "fpts_5pt_0ppr"),
            ("5pt", "half", "fpts_5pt_half"),
            ("5pt", "ppr", "fpts_5pt_ppr"),
            ("6pt", "0ppr", "fpts_6pt_0ppr"),
            ("6pt", "half", "fpts_6pt_half"),
            ("6pt", "ppr", "fpts_6pt_ppr"),
            ("4pt", "tep", "fpts_4pt_tep"),
            ("5pt", "tep", "fpts_5pt_tep"),
            ("6pt", "tep", "fpts_6pt_tep"),
        ]

        for idx, (td, ppr, fpts_col) in enumerate(variants):
            log(f"[PPG-BACKFILL] Processing variant {idx+1}/{len(variants)}: {td} TD, {ppr}...")

            # 1. Season PPG
            season_col = f"ppg_season_{td}_{ppr}"
            log(f"  [1/8] {season_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {season_col} = ROUND(calc.avg_pts, 2)
                FROM (
                    SELECT NFL_player_id, year, AVG({fpts_col}) as avg_pts
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                    GROUP BY NFL_player_id, year
                ) calc
                WHERE t.NFL_player_id = calc.NFL_player_id
                  AND t.year = calc.year
            """)

            # 2. Alltime PPG
            alltime_col = f"ppg_alltime_{td}_{ppr}"
            log(f"  [2/8] {alltime_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {alltime_col} = ROUND(calc.avg_pts, 2)
                FROM (
                    SELECT NFL_player_id, AVG({fpts_col}) as avg_pts
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                    GROUP BY NFL_player_id
                ) calc
                WHERE t.NFL_player_id = calc.NFL_player_id
            """)

            # 3. Rolling Total (cumulative sum per season)
            rolling_total_col = f"rolling_total_{td}_{ppr}"
            log(f"  [3/8] {rolling_total_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {rolling_total_col} = ROUND(calc.rolling_sum, 2)
                FROM (
                    SELECT player_week,
                           SUM({fpts_col}) OVER (
                               PARTITION BY NFL_player_id, year
                               ORDER BY week
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                           ) as rolling_sum
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                ) calc
                WHERE t.player_week = calc.player_week
            """)

            # 4. Rolling 3-game average
            rolling_3_col = f"rolling_3_{td}_{ppr}"
            log(f"  [4/8] {rolling_3_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {rolling_3_col} = ROUND(calc.rolling_avg, 2)
                FROM (
                    SELECT player_week,
                           AVG({fpts_col}) OVER (
                               PARTITION BY NFL_player_id
                               ORDER BY year, week
                               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                           ) as rolling_avg
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                ) calc
                WHERE t.player_week = calc.player_week
            """)

            # 5. Rolling 5-game average
            rolling_5_col = f"rolling_5_{td}_{ppr}"
            log(f"  [5/8] {rolling_5_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {rolling_5_col} = ROUND(calc.rolling_avg, 2)
                FROM (
                    SELECT player_week,
                           AVG({fpts_col}) OVER (
                               PARTITION BY NFL_player_id
                               ORDER BY year, week
                               ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                           ) as rolling_avg
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                ) calc
                WHERE t.player_week = calc.player_week
            """)

            # 6. Weighted PPG (approximation using recent games weighted higher)
            # DuckDB doesn't have EWM, so use weighted average of last 5 games
            weighted_col = f"weighted_ppg_{td}_{ppr}"
            log(f"  [6/8] {weighted_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {weighted_col} = ROUND(calc.weighted_avg, 2)
                FROM (
                    SELECT player_week,
                           -- Weighted average: most recent game weighted 5x, then 4x, 3x, 2x, 1x
                           (
                               COALESCE(LAG({fpts_col}, 0) OVER w * 5, 0) +
                               COALESCE(LAG({fpts_col}, 1) OVER w * 4, 0) +
                               COALESCE(LAG({fpts_col}, 2) OVER w * 3, 0) +
                               COALESCE(LAG({fpts_col}, 3) OVER w * 2, 0) +
                               COALESCE(LAG({fpts_col}, 4) OVER w * 1, 0)
                           ) / NULLIF(
                               (CASE WHEN LAG({fpts_col}, 0) OVER w IS NOT NULL THEN 5 ELSE 0 END) +
                               (CASE WHEN LAG({fpts_col}, 1) OVER w IS NOT NULL THEN 4 ELSE 0 END) +
                               (CASE WHEN LAG({fpts_col}, 2) OVER w IS NOT NULL THEN 3 ELSE 0 END) +
                               (CASE WHEN LAG({fpts_col}, 3) OVER w IS NOT NULL THEN 2 ELSE 0 END) +
                               (CASE WHEN LAG({fpts_col}, 4) OVER w IS NOT NULL THEN 1 ELSE 0 END)
                           , 0) as weighted_avg
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                    WINDOW w AS (PARTITION BY NFL_player_id ORDER BY year, week)
                ) calc
                WHERE t.player_week = calc.player_week
            """)

            # 7. Consistency (coefficient of variation = std/mean per season)
            consistency_col = f"consistency_{td}_{ppr}"
            log(f"  [7/8] {consistency_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {consistency_col} = ROUND(calc.cv, 3)
                FROM (
                    SELECT NFL_player_id, year,
                           CASE WHEN AVG({fpts_col}) > 0
                                THEN STDDEV({fpts_col}) / AVG({fpts_col})
                                ELSE 0
                           END as cv
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                    GROUP BY NFL_player_id, year
                ) calc
                WHERE t.NFL_player_id = calc.NFL_player_id
                  AND t.year = calc.year
            """)

            # 8. Avg Points Next Year (forward-looking)
            next_year_col = f"avg_pts_next_year_{td}_{ppr}"
            log(f"  [8/8] {next_year_col}...")
            conn.execute(f"""
                UPDATE nfl_historical.nfl_player_stats_all t
                SET {next_year_col} = ROUND(next_season.avg_pts, 2)
                FROM (
                    -- Get each player's season average
                    SELECT NFL_player_id, year, AVG({fpts_col}) as avg_pts
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE {fpts_col} IS NOT NULL
                    GROUP BY NFL_player_id, year
                ) next_season
                WHERE t.NFL_player_id = next_season.NFL_player_id
                  AND t.year = next_season.year - 1  -- Current year's rows get NEXT year's average
            """)

            log(f"  [OK] Variant {td}_{ppr} complete")

        log("[PPG-BACKFILL] ============================================================")
        log("[PPG-BACKFILL] ALL 48 PPG COLUMNS BACKFILLED SUCCESSFULLY")
        log("[PPG-BACKFILL] ============================================================")

    except Exception as e:
        log(f"[PPG-BACKFILL] ERROR: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    # Check for --backfill-ppg flag
    if "--backfill-ppg" in sys.argv:
        backfill_all_ppg_columns()
        sys.exit(0)

    sys.exit(main())
