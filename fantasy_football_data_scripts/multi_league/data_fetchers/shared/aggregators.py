#!/usr/bin/env python3
"""
Data Aggregators

Functions for aggregating and normalizing weekly/yearly data files into
canonical parquet outputs. These operations are commonly needed for both
initial imports and incremental updates.

Extracted from initial_import_v2.py for modularity and reusability.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
import pandas as pd

_verbose = "--verbose" in sys.argv

# Import data normalization utilities from the same package

# Try to import LeagueContext
try:
    from multi_league.core.league_context import LeagueContext
except ImportError:
    LeagueContext = None


def _load_ctx(context_path: str):
    """Load league context from JSON file."""
    if LeagueContext is None:
        raise ImportError("LeagueContext not available")
    return LeagueContext.load(context_path)


# Required columns for matchup data
REQUIRED_MATCHUP_COLS = [
    "year",
    "week",
    "manager",
    "opponent",
    "team_name",
    "opponent_team",
    "team_points",
    "opponent_points",
    "is_playoffs",
    "is_consolation",
    "manager_week",
]

OPTIONAL_MATCHUP_COLS_DEFAULTS = {
    "is_playoffs": False,
    "is_consolation": False,
}

# NFL team abbreviation to DST player name mapping
# Used to normalize defense transactions to match app nomenclature ({Nickname} DST)
NFL_TEAM_TO_DST = {
    "ARI": "Cardinals DST",
    "ATL": "Falcons DST",
    "BAL": "Ravens DST",
    "BUF": "Bills DST",
    "CAR": "Panthers DST",
    "CHI": "Bears DST",
    "CIN": "Bengals DST",
    "CLE": "Browns DST",
    "DAL": "Cowboys DST",
    "DEN": "Broncos DST",
    "DET": "Lions DST",
    "GB": "Packers DST",
    "HOU": "Texans DST",
    "IND": "Colts DST",
    "JAC": "Jaguars DST",
    "JAX": "Jaguars DST",
    "KC": "Chiefs DST",
    "LAC": "Chargers DST",
    "LAR": "Rams DST",
    "LV": "Raiders DST",
    "MIA": "Dolphins DST",
    "MIN": "Vikings DST",
    "NE": "Patriots DST",
    "NO": "Saints DST",
    "NYG": "Giants DST",
    "NYJ": "Jets DST",
    "OAK": "Raiders DST",
    "PHI": "Eagles DST",
    "PIT": "Steelers DST",
    "SD": "Chargers DST",
    "SEA": "Seahawks DST",
    "SF": "49ers DST",
    "STL": "Rams DST",
    "TB": "Buccaneers DST",
    "TEN": "Titans DST",
    "WAS": "Commanders DST",
    "WSH": "Commanders DST",
}

# City name to DST mapping (for staging data that uses city names like "Atlanta", "Buffalo")
CITY_TO_DST = {
    "Atlanta": "Falcons DST",
    "Buffalo": "Bills DST",
    "Baltimore": "Ravens DST",
    "Carolina": "Panthers DST",
    "Chicago": "Bears DST",
    "Cincinnati": "Bengals DST",
    "Cleveland": "Browns DST",
    "Dallas": "Cowboys DST",
    "Denver": "Broncos DST",
    "Detroit": "Lions DST",
    "Green Bay": "Packers DST",
    "Houston": "Texans DST",
    "Indianapolis": "Colts DST",
    "Jacksonville": "Jaguars DST",
    "Kansas City": "Chiefs DST",
    "Las Vegas": "Raiders DST",
    "Miami": "Dolphins DST",
    "Minnesota": "Vikings DST",
    "New England": "Patriots DST",
    "New Orleans": "Saints DST",
    "New York Giants": "Giants DST",
    "New York Jets": "Jets DST",
    "Philadelphia": "Eagles DST",
    "Pittsburgh": "Steelers DST",
    "San Francisco": "49ers DST",
    "Seattle": "Seahawks DST",
    "Tampa Bay": "Buccaneers DST",
    "Tennessee": "Titans DST",
    "Washington": "Commanders DST",
    "Arizona": "Cardinals DST",
    "Los Angeles Rams": "Rams DST",
    "Los Angeles Chargers": "Chargers DST",
    "Oakland": "Raiders DST",
    "San Diego": "Chargers DST",
    "St. Louis": "Rams DST",
}

# Team nickname to DST (for Yahoo data that uses nicknames like "Falcons", "Bills")
NICKNAME_TO_DST = {
    "Falcons": "Falcons DST",
    "Bills": "Bills DST",
    "Ravens": "Ravens DST",
    "Panthers": "Panthers DST",
    "Bears": "Bears DST",
    "Bengals": "Bengals DST",
    "Browns": "Browns DST",
    "Cowboys": "Cowboys DST",
    "Broncos": "Broncos DST",
    "Lions": "Lions DST",
    "Packers": "Packers DST",
    "Texans": "Texans DST",
    "Colts": "Colts DST",
    "Jaguars": "Jaguars DST",
    "Chiefs": "Chiefs DST",
    "Raiders": "Raiders DST",
    "Dolphins": "Dolphins DST",
    "Vikings": "Vikings DST",
    "Patriots": "Patriots DST",
    "Saints": "Saints DST",
    "Giants": "Giants DST",
    "Jets": "Jets DST",
    "Eagles": "Eagles DST",
    "Steelers": "Steelers DST",
    "49ers": "49ers DST",
    "Seahawks": "Seahawks DST",
    "Buccaneers": "Buccaneers DST",
    "Titans": "Titans DST",
    "Commanders": "Commanders DST",
    "Cardinals": "Cardinals DST",
    "Rams": "Rams DST",
    "Chargers": "Chargers DST",
    "Redskins": "Commanders DST",
}

# Full team name to DST (for Sleeper data that uses full names like "Arizona Cardinals")
FULL_NAME_TO_DST = {
    "Arizona Cardinals": "Cardinals DST",
    "Atlanta Falcons": "Falcons DST",
    "Baltimore Ravens": "Ravens DST",
    "Buffalo Bills": "Bills DST",
    "Carolina Panthers": "Panthers DST",
    "Chicago Bears": "Bears DST",
    "Cincinnati Bengals": "Bengals DST",
    "Cleveland Browns": "Browns DST",
    "Dallas Cowboys": "Cowboys DST",
    "Denver Broncos": "Broncos DST",
    "Detroit Lions": "Lions DST",
    "Green Bay Packers": "Packers DST",
    "Houston Texans": "Texans DST",
    "Indianapolis Colts": "Colts DST",
    "Jacksonville Jaguars": "Jaguars DST",
    "Kansas City Chiefs": "Chiefs DST",
    "Las Vegas Raiders": "Raiders DST",
    "Los Angeles Chargers": "Chargers DST",
    "Los Angeles Rams": "Rams DST",
    "Miami Dolphins": "Dolphins DST",
    "Minnesota Vikings": "Vikings DST",
    "New England Patriots": "Patriots DST",
    "New Orleans Saints": "Saints DST",
    "New York Giants": "Giants DST",
    "New York Jets": "Jets DST",
    "Philadelphia Eagles": "Eagles DST",
    "Pittsburgh Steelers": "Steelers DST",
    "San Francisco 49ers": "49ers DST",
    "Seattle Seahawks": "Seahawks DST",
    "Tampa Bay Buccaneers": "Buccaneers DST",
    "Tennessee Titans": "Titans DST",
    "Washington Commanders": "Commanders DST",
    # Historical names
    "Oakland Raiders": "Raiders DST",
    "San Diego Chargers": "Chargers DST",
    "St. Louis Rams": "Rams DST",
    "Washington Redskins": "Commanders DST",
    "Washington Football Team": "Commanders DST",
}

# Common name suffixes - NOTE: super_table naming is inconsistent
# Some players have suffixes (Odell Beckham Jr., Calvin Austin III), others don't (Melvin Gordon)
# This function does NOT auto-strip suffixes due to this inconsistency
# Instead, headshot matching is done case-insensitively with fuzzy fallbacks in the UI

# Known name mappings for common variations (Yahoo/Sleeper -> super_table)
PLAYER_NAME_MAPPINGS = {
    "Stephen Hauschka": "Steven Hauschka",
    # Add more as needed
}


def normalize_player_name_suffixes(df: pd.DataFrame, log: Callable = print) -> pd.DataFrame:
    """
    Normalize player names using known mappings.

    NOTE: This function does NOT auto-strip suffixes (Jr., II, III, etc.) because
    the super_table naming is inconsistent - some players have suffixes in their
    canonical name (e.g., "Odell Beckham Jr.", "Calvin Austin III") while others
    don't (e.g., "Melvin Gordon" not "Melvin Gordon III").

    Instead, this applies known name mappings for common variations.

    Args:
        df: DataFrame with player column
        log: Logging function

    Returns:
        DataFrame with normalized player names
    """
    if "player" not in df.columns:
        return df

    total_normalized = 0
    for old_name, new_name in PLAYER_NAME_MAPPINGS.items():
        mask = df["player"] == old_name
        if mask.any():
            df.loc[mask, "player"] = new_name
            total_normalized += mask.sum()

    if total_normalized > 0:
        log(f"[normalize_names] Applied {total_normalized:,} known name mappings")

    return df


def normalize_dst_player_names(df: pd.DataFrame, log: Callable = print) -> pd.DataFrame:
    """
    Normalize defense player names to match app nomenclature ({Nickname} DST).

    Handles multiple cases:
    1. If position column exists: Use position='DEF' mask with nfl_team mapping
    2. If position is missing: Pattern match player names against city/nickname mappings

    Args:
        df: DataFrame with player column (position and nfl_team optional)
        log: Logging function

    Returns:
        DataFrame with normalized DST player names
    """
    if "player" not in df.columns:
        return df

    # PATH 1: If we have position column, use existing mask logic (most reliable)
    if "position" in df.columns and "nfl_team" in df.columns:
        dst_mask = df["position"] == "DEF"
        if dst_mask.any():
            df.loc[dst_mask, "player"] = (
                df.loc[dst_mask, "nfl_team"].map(NFL_TEAM_TO_DST).fillna(df.loc[dst_mask, "player"])
            )
            mapped_count = df.loc[dst_mask, "player"].str.contains("DST", na=False).sum()
            if mapped_count > 0:
                log(f"[normalize_dst] Normalized {mapped_count:,} DST player names via position/nfl_team")
        return df

    # PATH 2: No position column - detect DST by pattern matching player names
    # This handles transaction data where position column is not available
    total_mapped = 0

    # Try full team names first (e.g., "Arizona Cardinals" -> "Cardinals DST")
    # This is the most specific match, used by Sleeper
    fullname_mask = df["player"].isin(FULL_NAME_TO_DST.keys())
    if fullname_mask.any():
        df.loc[fullname_mask, "player"] = df.loc[fullname_mask, "player"].map(FULL_NAME_TO_DST)
        if "position" in df.columns:
            df.loc[fullname_mask, "position"] = "DEF"
        total_mapped += fullname_mask.sum()
        log(f"[normalize_dst] Normalized {fullname_mask.sum():,} DST names from full team names")

    # Try city names (e.g., "Atlanta" -> "Falcons DST")
    city_mask = df["player"].isin(CITY_TO_DST.keys())
    if city_mask.any():
        df.loc[city_mask, "player"] = df.loc[city_mask, "player"].map(CITY_TO_DST)
        if "position" in df.columns:
            df.loc[city_mask, "position"] = "DEF"
        total_mapped += city_mask.sum()
        log(f"[normalize_dst] Normalized {city_mask.sum():,} DST names from city names")

    # Try nicknames (e.g., "Falcons" -> "Falcons DST")
    nickname_mask = df["player"].isin(NICKNAME_TO_DST.keys())
    if nickname_mask.any():
        df.loc[nickname_mask, "player"] = df.loc[nickname_mask, "player"].map(NICKNAME_TO_DST)
        if "position" in df.columns:
            df.loc[nickname_mask, "position"] = "DEF"
        total_mapped += nickname_mask.sum()
        log(f"[normalize_dst] Normalized {nickname_mask.sum():,} DST names from team nicknames")

    return df


def ensure_fantasy_points_alias(db, log: Callable[..., None] = print) -> str:
    """
    Ensure player_fantasy table has a 'fantasy_points' column as an alias
    to the actual points column (which varies by scoring type).

    Args:
        db: LocalLeagueDB instance (required)
        log: Logging function (default: print)

    Returns:
        Description of what was done

    Raises:
        ValueError: If player_fantasy table is empty or no suitable points column found
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    conn = db.connect()
    cols = [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'player_fantasy'"
        ).fetchall()
    ]
    if not cols:
        raise ValueError("player_fantasy table not found or has no columns")

    if "fantasy_points" in cols:
        log("[POST] fantasy_points column already exists in player_fantasy")
        return "already_exists"

    candidates = [
        "fantasy_points_total",
        "fantasy_points_ppr",
        "fpts",
        "points",
        "fantasy_points_std",
    ]
    chosen = None
    for c in candidates:
        if c in cols:
            chosen = c
            break

    if chosen is None:
        raise ValueError("No suitable points column found to alias to 'fantasy_points'")

    conn.execute("ALTER TABLE public.player_fantasy ADD COLUMN fantasy_points DOUBLE")
    conn.execute(f"UPDATE public.player_fantasy SET fantasy_points = {chosen}")
    log(f"[POST] Added fantasy_points alias from {chosen} in player_fantasy table")
    return f"aliased_from_{chosen}"
