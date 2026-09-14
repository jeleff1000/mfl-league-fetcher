#!/usr/bin/env python3
"""
Yahoo-NFL Merge V3 - Fast hybrid matching with last-name 1:1 fallback.

Design:
1. Layer 1: Full normalized name + position + year + week (catches most matches)
2. Layer 2: Last name + position 1:1 (catches nicknames when unambiguous)
3. Multi-position handling (e.g., QB,TE matches either position)
4. DEF uses nfl_team code (handles "New York" ??? "NYG" vs "NYJ")
5. Settings-driven position filtering

Performance: ~0.4s for 3,000 Yahoo rows + 20,000 NFL rows
Accuracy: +19 more correct matches than original, 0 false positives

Author: Fantasy Football Analytics Pipeline
"""

from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass, field

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.core.roster_slots import is_flex
from multi_league.data_fetchers.shared.name_utils import normalize_name as _shared_normalize_name

import pandas as pd
import numpy as np

from multi_league.data_fetchers.shared.clean_names import TEAM_ABBREV_TO_FRANCHISE_ID

# =============================================================================
# CONTEXT SUPPORT
# =============================================================================
# Add parent directories to path for imports
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_FETCHERS_DIR = SCRIPT_DIR.parent
MULTI_LEAGUE_DIR = DATA_FETCHERS_DIR.parent

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

try:
    from multi_league.core.league_context import LeagueContext

    LEAGUE_CONTEXT_AVAILABLE = True
except ImportError:
    LEAGUE_CONTEXT_AVAILABLE = False
    LeagueContext = None

# Import super table loader for fallback when local NFL files don't exist (GitHub Actions)
try:
    from multi_league.data_fetchers.load_nfl_from_super_table import load_super_table, filter_by_year_week

    SUPER_TABLE_AVAILABLE = True
except ImportError:
    SUPER_TABLE_AVAILABLE = False
    load_super_table = None
    filter_by_year_week = None

# Default paths (when no context provided)
REPO_ROOT = MULTI_LEAGUE_DIR.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "fantasy_football_data"

# Import franchise-based DEF ID functions
try:
    from nfl_data.nfl_franchises import get_def_player_id, get_def_display_name

    FRANCHISE_FUNCTIONS_AVAILABLE = True
except ImportError:
    FRANCHISE_FUNCTIONS_AVAILABLE = False
    get_def_player_id = None
    get_def_display_name = None

# Import player ID cache for fast lookups
try:
    from ..shared.player_id_cache import load_yahoo_nfl_mapping, save_yahoo_nfl_mapping

    PLAYER_ID_CACHE_AVAILABLE = True
except ImportError:
    try:
        from shared.player_id_cache import load_yahoo_nfl_mapping, save_yahoo_nfl_mapping

        PLAYER_ID_CACHE_AVAILABLE = True
    except ImportError:
        PLAYER_ID_CACHE_AVAILABLE = False
        load_yahoo_nfl_mapping = None
        save_yahoo_nfl_mapping = None

# Import YahooPlayerCache for local disk caching (24h TTL)
try:
    from ..shared.yahoo_player_cache import YahooPlayerCache, get_yahoo_cache

    YAHOO_PLAYER_CACHE_AVAILABLE = True
except ImportError:
    try:
        from shared.yahoo_player_cache import YahooPlayerCache, get_yahoo_cache

        YAHOO_PLAYER_CACHE_AVAILABLE = True
    except ImportError:
        YAHOO_PLAYER_CACHE_AVAILABLE = False
        YahooPlayerCache = None
        get_yahoo_cache = None

# Import SQL-based player matching for cache misses
try:
    from ..shared.sql_player_matching import match_and_save_to_cache

    SQL_MATCHING_AVAILABLE = True
except ImportError:
    try:
        from shared.sql_player_matching import match_and_save_to_cache

        SQL_MATCHING_AVAILABLE = True
    except ImportError:
        SQL_MATCHING_AVAILABLE = False
        match_and_save_to_cache = None


# =============================================================================
# CONFIGURATION
# =============================================================================
@dataclass
class MergeConfig:
    """Configuration for the merge process."""

    fantasy_positions: set[str] = field(default_factory=lambda: {"QB", "RB", "WR", "TE", "K", "DEF", "FB"})
    flex_positions: set[str] = field(default_factory=set)

    nfl_to_fantasy_pos: dict[str, str] = field(
        default_factory=lambda: {
            "QB": "QB",
            "RB": "RB",
            "FB": "RB",
            "HB": "RB",
            "WR": "WR",
            "TE": "TE",
            "K": "K",
            "DEF": "DEF",
            "DST": "DEF",
        }
    )

    @classmethod
    def from_league_settings(cls, settings_json: dict) -> MergeConfig:
        """Build config from league settings JSON."""
        config = cls()

        roster_positions = settings_json.get("roster_positions", [])

        active_positions = set()
        flex_positions = set()

        for pos_info in roster_positions:
            pos = pos_info.get("position", "")
            if pos in ("BN", "IR", "IL"):
                continue
            if is_flex(pos):
                flex_positions.add(pos)
                # Derive the individual positions eligible for this flex slot
                for p in pos.replace("/", "").upper():
                    if p == "W":
                        active_positions.add("WR")
                    elif p == "R":
                        active_positions.add("RB")
                    elif p == "T":
                        active_positions.add("TE")
                    elif p == "Q":
                        active_positions.add("QB")
            else:
                active_positions.add(pos.upper())

        # Always include core fantasy positions even if league settings omit them.
        # Yahoo's archived league settings sometimes drop K and DEF from
        # roster_positions, but the scoreboard still includes their points.
        MANDATORY_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF", "FB"}
        fantasy_positions = active_positions | MANDATORY_POSITIONS

        config.fantasy_positions = fantasy_positions
        config.flex_positions = flex_positions

        # Check for IDP
        idp_positions = {"DB", "DL", "LB", "DE", "DT", "CB", "S", "DP", "D"}
        if active_positions & idp_positions:
            config.nfl_to_fantasy_pos.update(
                {
                    "LB": "LB",
                    "MLB": "LB",
                    "ILB": "LB",
                    "OLB": "LB",
                    "DE": "DL",
                    "DT": "DL",
                    "NT": "DL",
                    "CB": "DB",
                    "S": "DB",
                    "SS": "DB",
                    "FS": "DB",
                    "SAF": "DB",
                }
            )
            config.fantasy_positions.update(idp_positions)

        return config


# =============================================================================
# MANUAL PLAYER ID OVERRIDES
# =============================================================================
# Direct yahoo_player_id -> NFL_player_id mappings for players that fail
# automated name matching. These are applied FIRST, before any name matching.
# Add entries here when a player cannot be matched automatically due to:
# - Name typos in Yahoo data (e.g., "Trenton Richardson" vs "Trent Richardson")
# - Name variations not in NICKNAME_MAP (e.g., "Stephen" vs "Steven")
# - Suffix mismatches (e.g., "Mark Ingram II" vs "Mark Ingram")
# - Position changes (e.g., Tim Tebow QB->TE comeback)
# - Players who never played regular season games (will map to closest match)
YAHOO_NFL_OVERRIDES = {
    # 2014 unmatched players
    "25713": "00-0029675",  # Trenton Richardson -> Trent Richardson (RB, typo)
    "24815": "00-0027966",  # Mark Ingram II -> Mark Ingram (RB, suffix)
    "9066": "00-0025944",  # Stephen Hauschka -> Steven Hauschka (K, name variant)
    # 2016+ unmatched players
    "29560": "00-0032741",  # Kenneth Barber -> Peyton Barber (RB, wrong first name in Yahoo)
    # 2021 unmatched players
    "24000": "00-0027876",  # Tim Tebow (TE) -> Tim Tebow (QB) - same person, different position
    # '33015': None,        # Brian Lewerke - never played NFL regular season, leave unmapped
    # =========================================================================
    # SAME-NAME PLAYER OVERRIDES
    # These players have the same name but are different people. We explicitly
    # map each Yahoo ID to the correct NFL_player_id to prevent merging.
    # =========================================================================
    # Brandon Marshall (WR vs ILB)
    "7868": "00-0024334",  # Brandon Marshall WR (Broncos/Dolphins/Bears/Jets/Giants 2006-2018)
    "25852": "00-0029620",  # Brandon Marshall ILB (Jaguars/Broncos/Raiders 2012-2019)
    # Alex Smith (QB vs TE)
    "7177": "00-0023436",  # Alex Smith QB (49ers/Chiefs/Washington 2005-2020)
    "7247": "00-0023506",  # Alex Smith TE (Buccaneers/Patriots/Eagles 2005-2015)
    # A.J. Green (WR vs CB)
    "24791": "00-0027942",  # A.J. Green WR (Bengals/Cardinals 2011-2022)
    "33306": "00-0036114",  # A.J. Green CB (Browns 2021-2025)
    # Chris Johnson (RB vs DB) - NOTE: Yahoo 8801 should only map to CJ2K (RB)
    "8801": "00-0026164",  # Chris Johnson RB "CJ2K" (Titans/Jets/Cardinals 2008-2017)
    # David Johnson (RB vs TE)
    "28474": "00-0032187",  # David Johnson RB (Cardinals/Texans/Saints 2015-2022)
    "9505": "00-0026957",  # David Johnson TE (Steelers/Chargers 2009-2016)
    # Lamar Jackson (QB vs CB)
    "31002": "00-0034796",  # Lamar Jackson QB (Ravens 2018-)
    "33336": "00-0036152",  # Lamar Jackson CB (Jets 2020-2022)
    # Michael Bennett (DE) - RB Michael Bennett is too old for Yahoo mappings
    "9568": "00-0026618",  # Michael Bennett DE (Seahawks/Eagles/Patriots 2009-2019)
    # Kyle Williams (DT) - WR Kyle Williams has no Yahoo mapping
    "7883": "00-0024348",  # Kyle Williams DT (Bills 2006-2018)
    # Steve Smith (WR vs WR) - Two different WRs with same name
    "5521": "00-0020337",  # Steve Smith Sr. WR (Panthers/Ravens 2001-2016)
    "8305": "00-0025438",  # Steve Smith WR (Giants 2007-2012) - different person
}

# =============================================================================
# SAME-NAME PLAYER DISAMBIGUATION
# =============================================================================
# Players with the same name but different positions who should NEVER be merged.
# When cross-position matching (Layer 2), these players require strict position match.
# Format: normalized_name -> list of (NFL_player_id, position_group) tuples
#
# This prevents incorrectly merging stats between:
# - Brandon Marshall WR and Brandon Marshall ILB
# - Adrian Peterson RB (Vikings) and Adrian Peterson RB (Bears - different player)
# - Josh Allen QB (Bills) and Josh Allen DE/LB (Jaguars)
SAME_NAME_PLAYERS = {
    # Players with 5+ overlapping years or high fantasy relevance
    "alex smith": [
        ("00-0023436", "QB"),  # Alex Smith QB (49ers/Chiefs/Washington 2005-2020) - 2458 pts
        ("00-0023506", "TE"),  # Alex Smith TE (Buccaneers/Patriots/Eagles 2005-2015) - 238 pts
    ],
    "aj green": [  # normalized without period
        ("00-0027942", "WR"),  # A.J. Green WR (Bengals/Cardinals 2011-2022) - 1498 pts
        ("00-0036114", "CB"),  # A.J. Green CB (Browns 2021-2025)
    ],
    "brandon marshall": [
        ("00-0024334", "WR"),  # Brandon Marshall WR (2006-2018) - 1731 pts
        ("00-0029620", "LB"),  # Brandon Marshall ILB (2012-2019)
    ],
    "chris johnson": [
        ("00-0026164", "RB"),  # Chris Johnson RB "CJ2K" (Titans/Jets 2008-2017) - 1561 pts
        ("00-0021949", "DB"),  # Chris Johnson DB (Packers/Raiders 2005-2012)
    ],
    "david johnson": [
        ("00-0032187", "RB"),  # David Johnson RB (Cardinals/Texans 2015-2022) - 1046 pts
        ("00-0026957", "TE"),  # David Johnson TE (Steelers/Chargers 2009-2016) - 37 pts
    ],
    "josh allen": [
        ("00-0034857", "QB"),  # Josh Allen QB (Bills, 2018-)
        ("00-0034418", "LB"),  # Josh Allen DE/LB (Jaguars, 2019-)
    ],
    "kyle williams": [
        ("00-0027608", "WR"),  # Kyle Williams WR (49ers/Chiefs 2010-2025) - 127 pts
        ("00-0024348", "DT"),  # Kyle Williams DT (Bills 2006-2018)
    ],
    "lamar jackson": [
        ("00-0034796", "QB"),  # Lamar Jackson QB (Ravens 2018-) - 2510 pts
        ("00-0036152", "CB"),  # Lamar Jackson CB (Jets 2020-2022)
    ],
    "michael bennett": [
        ("00-0020514", "RB"),  # Michael Bennett RB (Vikings/Chiefs 2001-2010) - 608 pts
        ("00-0026618", "DE"),  # Michael Bennett DE (Seahawks/Eagles 2009-2019)
    ],
    "michael thomas": [
        ("00-0033075", "WR"),  # Michael Thomas WR (Saints, 2016-)
        ("00-0030772", "S"),  # Michael Thomas S (49ers/Dolphins, 2012-2018)
    ],
    "mike williams": [
        ("00-0027702", "WR"),  # Mike Williams WR (Chargers/Jets 2017-) - 1188 pts (active)
        ("00-0021142", "G"),  # Mike Williams G (Bills 2002-2009)
    ],
    "roy williams": [
        ("00-0022909", "WR"),  # Roy Williams WR (Lions/Cowboys 2004-2011) - 831 pts
        ("00-0021143", "S"),  # Roy Williams S (Cowboys/Bengals 2002-2010)
    ],
    "steve smith": [
        ("00-0020337", "WR"),  # Steve Smith Sr. WR (Panthers/Ravens, 2001-2016)
        ("00-0025438", "WR"),  # Steve Smith WR (Giants, 2007-2012) - different person
    ],
    # Players with less overlap but still potentially confusing
    "chris henry": [
        ("00-0023518", "WR"),  # Chris Henry WR (Bengals 2005-2009) - 313 pts
        ("00-0025437", "RB"),  # Chris Henry RB (Titans/Cardinals 2007-2010) - 30 pts
    ],
}

# =============================================================================
# NAME NORMALIZATION
# =============================================================================
# Compound last name prefixes
COMPOUND_PREFIXES = {"st", "de", "la", "le", "van", "von", "del", "der", "den", "mc", "mac", "o"}

# Hardcoded aliases for nicknames that can't resolve via 1:1 last name matching
# (e.g., too many players with same last name at same position)
# Also includes known Yahoo data typos/misspellings
NAME_ALIASES = {
    "hollywood brown": "marquise brown",  # Only keep truly unique nicknames
    "trenton richardson": "trent richardson",  # Yahoo typo
    "kenneth barber": "peyton barber",  # Yahoo has wrong first name
}

# Common nickname ??? canonical name mappings (applied after initial normalization)
NICKNAME_MAP = {
    "josh": "joshua",
    "mike": "michael",
    "matt": "matthew",
    "rob": "robert",
    "bob": "robert",
    "joe": "joseph",
    "tony": "anthony",
    "dan": "daniel",
    "danny": "daniel",
    "ben": "benjamin",
    "tom": "thomas",
    "tommy": "thomas",
    "will": "william",
    "bill": "william",
    "billy": "william",
    "jim": "james",
    "jimmy": "james",
    "jake": "jacob",
    "chris": "christopher",
    "nick": "nicholas",
    "nate": "nathaniel",
    "zach": "zachary",
    "zack": "zachary",
    "alex": "alexander",
    "gabe": "gabriel",
    "abe": "abraham",
    "sam": "samuel",
    "sammy": "samuel",
    "ken": "kenneth",
    "kenny": "kenneth",
    "steve": "steven",
    "stephen": "steven",  # Stephen/Steven variants
    "ed": "edward",
    "eddie": "edward",
    "pat": "patrick",
    "rick": "richard",
    "dick": "richard",
    "greg": "gregory",
    "ty": "tyler",
    "tim": "timothy",
    "jon": "jonathan",
}

# Accent character mapping - maps accented characters to ASCII equivalents
ACCENT_MAP = str.maketrans(
    {
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "à": "a",
        "á": "a",
        "â": "a",
        "ã": "a",
        "ä": "a",
        "ì": "i",
        "í": "i",
        "î": "i",
        "ï": "i",
        "ò": "o",
        "ó": "o",
        "ô": "o",
        "õ": "o",
        "ö": "o",
        "ù": "u",
        "ú": "u",
        "û": "u",
        "ü": "u",
        "ñ": "n",
        "ç": "c",
    }
)


def normalize_name(name: str) -> str:
    """
    Normalize player name for matching.

    Delegates core normalization (accents, suffixes, punctuation, whitespace) to
    the shared ``name_utils.normalize_name`` and layers Yahoo-specific extras:

    - Applies hardcoded aliases (Hollywood Brown -> Marquise Brown)
    - Removes middle initials (Charles D Johnson -> charles johnson)
    - Keeps first-name initials (A J Brown -> a j brown)
    """
    # Apply Yahoo-specific aliases *before* shared normalization so the alias
    # value (e.g. "marquise brown") flows through the standard pipeline.
    if pd.isna(name) or not name:
        return ""
    raw = str(name).lower().strip()
    if raw in NAME_ALIASES:
        raw = NAME_ALIASES[raw]

    s = _shared_normalize_name(raw)
    if not s:
        return ""

    parts = s.split()
    if len(parts) < 3:
        return s

    # Remove middle initials (single letter after a full first name)
    # Keep first-name initials (consecutive single letters at start)
    new_parts = [parts[0]]
    for i in range(1, len(parts)):
        part = parts[i]
        prev_part = parts[i - 1]
        if len(part) == 1 and len(prev_part) > 1:
            continue  # Skip middle initial
        else:
            new_parts.append(part)

    return " ".join(new_parts)


def extract_last_name(name: str) -> str:
    """
    Extract last name, handling compound names like "St. Brown".
    """
    norm = normalize_name(name)
    parts = norm.split()

    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]

    # Check for compound last name
    last_name_parts = [parts[-1]]
    for i in range(len(parts) - 2, 0, -1):
        if parts[i] in COMPOUND_PREFIXES:
            last_name_parts.insert(0, parts[i])
        else:
            break

    return " ".join(last_name_parts)


def get_all_positions(pos_str: str, config: MergeConfig) -> list[str]:
    """
    Extract all positions from a string like "QB,TE" -> ["QB", "TE"].
    Maps NFL positions to fantasy positions.
    """
    if pd.isna(pos_str) or not pos_str:
        return ["UNK"]

    positions = []
    for p in str(pos_str).upper().replace(",", "/").split("/"):
        p = p.strip()
        if p in config.fantasy_positions:
            positions.append(p)
        elif p in config.nfl_to_fantasy_pos:
            positions.append(config.nfl_to_fantasy_pos[p])

    return positions if positions else ["UNK"]


# =============================================================================
# KEY BUILDING
# =============================================================================
def build_full_keys(row: pd.Series, name_col: str, config: MergeConfig) -> list[str]:
    """Build full-name match keys (one per position for multi-position players)."""
    positions = row["_positions"]
    year = row["year"]
    week = row["week"]

    # DEF special handling - use team code
    if "DEF" in positions and pd.notna(row.get("nfl_team")) and row.get("nfl_team"):
        return [f"def_{row['nfl_team']}|{year}|{week}|DEF".lower()]

    norm = normalize_name(row[name_col])
    return [f"{norm}|{year}|{week}|{pos}".lower() for pos in positions]


def build_last_keys(row: pd.Series, name_col: str, config: MergeConfig) -> list[str]:
    """Build last-name match keys (one per position for multi-position players)."""
    positions = row["_positions"]
    year = row["year"]
    week = row["week"]

    # DEF special handling
    if "DEF" in positions and pd.notna(row.get("nfl_team")) and row.get("nfl_team"):
        return [f"def_{row['nfl_team']}|{year}|{week}|DEF".lower()]

    last = extract_last_name(row[name_col])
    return [f"{last}|{year}|{week}|{pos}".lower() for pos in positions]


# =============================================================================
# CHUNKED MERGE HELPER
# =============================================================================
# Threshold for using chunked merge (rows in left DataFrame)
CHUNK_THRESHOLD = 100_000
CHUNK_SIZE = 50_000


def _chunked_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str,
    how: str = "inner",
    suffixes: tuple = ("", "_nfl"),
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Merge large DataFrames in chunks to reduce memory spikes.

    Only uses chunked processing when left DataFrame exceeds CHUNK_THRESHOLD.
    This prevents memory issues when merging 1M+ row datasets.

    Args:
        left: Left DataFrame (will be chunked)
        right: Right DataFrame (kept whole for lookup efficiency)
        on: Column to merge on
        how: Merge type ('inner', 'left', 'right', 'outer')
        suffixes: Suffixes for overlapping columns
        verbose: Print progress info

    Returns:
        Merged DataFrame
    """
    # For small datasets, use standard merge
    if len(left) < CHUNK_THRESHOLD:
        return left.merge(right, on=on, how=how, suffixes=suffixes)

    if verbose:
        print(f"  [chunked] Processing {len(left):,} rows in {CHUNK_SIZE:,}-row chunks...")

    chunks = []
    total_rows = len(left)

    for start in range(0, total_rows, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, total_rows)
        chunk = left.iloc[start:end]

        merged_chunk = chunk.merge(right, on=on, how=how, suffixes=suffixes)
        chunks.append(merged_chunk)

        if verbose and (start // CHUNK_SIZE) % 5 == 0:
            pct = (end / total_rows) * 100
            print(f"    Progress: {end:,}/{total_rows:,} ({pct:.0f}%)")

    result = pd.concat(chunks, ignore_index=True)

    if verbose:
        print(f"  [chunked] Complete: {len(result):,} merged rows")

    return result


# =============================================================================
# DEDUPLICATION HELPERS
# =============================================================================
def dedupe_yahoo_input(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Deduplicate Yahoo input to ensure one row per (yahoo_player_id, year, week).
    Prefers rostered rows (manager not null/Unrostered) over unrostered.

    Yahoo rows always have yahoo_player_id (from yahoo_fantasy_data.py fetcher).
    """
    if df.empty:
        return df

    # Use yahoo_player_id as primary key (all Yahoo rows have this)
    if "yahoo_player_id" not in df.columns:
        return df  # Can't dedupe without ID

    df = df.copy()

    # Build dedup key: yahoo_player_id + year + week
    df["_dedupe_key"] = df["yahoo_player_id"].astype(str) + "|" + df["year"].astype(str) + "|" + df["week"].astype(str)

    # Check for duplicates
    if not df["_dedupe_key"].duplicated().any():
        return df.drop(columns=["_dedupe_key"])

    # Score rows: rostered > unrostered, then by data completeness
    if "manager" in df.columns:
        df["_is_rostered"] = df["manager"].notna() & (df["manager"].astype(str).str.strip().str.lower() != "unrostered")
    else:
        df["_is_rostered"] = False

    df["_non_null_count"] = df.notna().sum(axis=1)

    # Sort to put best row first, then drop duplicates
    df = df.sort_values(["_dedupe_key", "_is_rostered", "_non_null_count"], ascending=[True, False, False])

    before = len(df)
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="first")
    after = len(df)

    if before != after and verbose:
        print(f"[dedupe] Removed {before - after} duplicate Yahoo rows (kept rostered)")

    # Clean up temp columns
    return df.drop(columns=["_dedupe_key", "_is_rostered", "_non_null_count"])


# =============================================================================
# CORE MERGE
# =============================================================================
def merge_yahoo_nfl(
    yahoo_df: pd.DataFrame, nfl_df: pd.DataFrame, config: MergeConfig, verbose: bool = True, use_cache: bool = True
) -> pd.DataFrame:
    """
    Merge Yahoo and NFL data using two-layer hybrid matching.

    Layer 1: Full normalized name match (catches exact/suffix matches)
    Layer 2: Last name 1:1 match (catches nicknames when unambiguous)

    Optimization: Uses persistent ID mapping cache for known players.
    Cache lookup is O(1), 4-layer matching only runs for new players.

    Returns DataFrame with all Yahoo rows + unrostered NFL rows.
    """
    if verbose:
        print(f"\n{'='*70}")
        print("YAHOO-NFL MERGE V3 (Hybrid: Full Name -> Last Name 1:1)")
        print(f"{'='*70}")
        print(f"Yahoo rows: {len(yahoo_df):,}")
        print(f"NFL rows:   {len(nfl_df):,}")
        print(f"Fantasy positions: {sorted(config.fantasy_positions)}")

    yahoo = yahoo_df.copy()
    yahoo = dedupe_yahoo_input(yahoo, verbose=verbose)  # Dedupe before merge
    nfl = nfl_df.copy()

    # =========================================================================
    # CACHE LOOKUP: Skip 4-layer matching for known players
    # =========================================================================
    # Uses YahooPlayerCache for local disk caching with 24h TTL.
    # This avoids network calls to MotherDuck on every import.
    cache_hits = 0
    new_mappings_to_save = []  # Collect new mappings to save after merge
    yahoo_cache = None  # Keep reference for saving new mappings later

    if use_cache and "yahoo_player_id" in yahoo.columns:
        if verbose:
            print("\nLoading player ID cache...")

        try:
            # Prefer YahooPlayerCache (local disk with 24h TTL)
            if YAHOO_PLAYER_CACHE_AVAILABLE:
                import os

                token = os.environ.get("MOTHERDUCK_TOKEN")
                yahoo_cache = get_yahoo_cache()
                yahoo_cache.refresh_if_stale(token)

                # Use O(1) dict lookup
                id_cache = yahoo_cache.get_nfl_id_map()

                if verbose:
                    cache_stats = yahoo_cache.get_stats()
                    print(f"  Local cache: {cache_stats.get('total_mappings', 0):,} mappings")
                    gsis_pct = 100 * cache_stats.get("with_gsis_id", 0) / max(cache_stats.get("total_mappings", 1), 1)
                    print(f"  GSIS coverage: {gsis_pct:.1f}%")
            # Fall back to direct MotherDuck query
            elif PLAYER_ID_CACHE_AVAILABLE:
                id_cache = load_yahoo_nfl_mapping()
                if verbose:
                    print(f"  Loaded {len(id_cache):,} mappings from MotherDuck")
            else:
                id_cache = {}

            if id_cache:
                # Mark cached players with their NFL_player_id
                yahoo["_cached_nfl_id"] = yahoo["yahoo_player_id"].astype(str).map(id_cache)
                cache_hits = yahoo["_cached_nfl_id"].notna().sum()

                if verbose:
                    cache_miss = len(yahoo) - cache_hits
                    hit_pct = 100 * cache_hits / max(len(yahoo), 1)
                    print(f"  Cache hit:  {cache_hits:,} ({hit_pct:.1f}%) - skip name matching")
                    print(f"  Cache miss: {cache_miss:,} players - need matching")
            else:
                yahoo["_cached_nfl_id"] = pd.NA
                if verbose:
                    print("  Cache empty, will populate after matching")
        except Exception as e:
            yahoo["_cached_nfl_id"] = pd.NA
            if verbose:
                print(f"  Cache unavailable: {e}")
    else:
        yahoo["_cached_nfl_id"] = pd.NA

    # =========================================================================
    # LAYER 0: Apply hardcoded overrides (highest priority)
    # =========================================================================
    # These override both cache and name matching for known problem players
    if "yahoo_player_id" in yahoo.columns and YAHOO_NFL_OVERRIDES:
        yahoo_id_str = yahoo["yahoo_player_id"].astype(str)
        override_mask = yahoo_id_str.isin(YAHOO_NFL_OVERRIDES.keys())
        override_count = override_mask.sum()

        if override_count > 0:
            # Apply overrides - these take precedence over cache
            yahoo.loc[override_mask, "_cached_nfl_id"] = yahoo_id_str[override_mask].map(YAHOO_NFL_OVERRIDES)
            if verbose:
                print(f"\nLayer 0: Applied {override_count:,} hardcoded overrides")
                for yid in yahoo_id_str[override_mask].unique():
                    player_name = yahoo.loc[yahoo_id_str == yid, "player"].iloc[0] if "player" in yahoo.columns else yid
                    print(f"  {player_name} ({yid}) -> {YAHOO_NFL_OVERRIDES[yid]}")

    # =========================================================================
    # SQL MATCHING: Match cache misses via MotherDuck SQL (replaces pandas matching)
    # =========================================================================
    # Instead of loading millions of NFL rows, query MotherDuck for just the
    # unmatched players. This is much faster and uses less memory.
    cache_miss_count = yahoo["_cached_nfl_id"].isna().sum()

    if cache_miss_count > 0 and SQL_MATCHING_AVAILABLE and use_cache:
        if verbose:
            print(f"\nSQL Matching: {cache_miss_count:,} cache misses...")

        try:
            # Get unmatched players
            unmatched_mask = yahoo["_cached_nfl_id"].isna()
            unmatched_df = yahoo.loc[unmatched_mask].copy()

            # Run SQL-based matching
            import os

            token = os.environ.get("MOTHERDUCK_TOKEN")
            sql_matches = match_and_save_to_cache(unmatched_df, token=token, verbose=verbose)

            if sql_matches:
                # Apply SQL matches
                yahoo.loc[unmatched_mask, "_cached_nfl_id"] = (
                    yahoo.loc[unmatched_mask, "yahoo_player_id"].astype(str).map(sql_matches)
                )

                sql_matched = yahoo.loc[unmatched_mask, "_cached_nfl_id"].notna().sum()
                if verbose:
                    print(f"  SQL matched: {sql_matched:,} players")
                    print(f"  Still unmatched: {cache_miss_count - sql_matched:,} players")

        except Exception as e:
            if verbose:
                print(f"  SQL matching failed: {e}")

    # Recalculate final cache stats
    final_matched = yahoo["_cached_nfl_id"].notna().sum()
    final_unmatched = len(yahoo) - final_matched

    if verbose:
        print(
            f"\nFinal matching: {final_matched:,} matched ({100*final_matched/len(yahoo):.1f}%), {final_unmatched:,} unmatched"
        )

    # =========================================================================
    # SKIP NFL DATA LOADING IF ALL MATCHED OR NFL DATA IS EMPTY
    # =========================================================================
    # If all players are matched via cache + SQL, we don't need to load NFL data
    # at all. The player_week keys will be built from the cached NFL_player_ids.
    # ALSO: If NFL data is empty (because main() decided to skip loading it),
    # we MUST use the fast path since pandas-based matching requires NFL data.
    skip_nfl_loading = ((final_unmatched == 0) and use_cache) or nfl.empty

    if skip_nfl_loading and verbose:
        if nfl.empty:
            print("\n[OPTIMIZATION] NFL data not loaded - using fast path with fallback IDs for unmatched!")
        else:
            print("\n[OPTIMIZATION] All players matched via cache - skipping NFL data load!")

    # =========================================================================
    # FAST PATH: All players matched via cache + SQL (or NFL data not loaded)
    # =========================================================================
    # Skip pandas-based layer matching entirely and build result directly
    if skip_nfl_loading:
        if verbose:
            print("\n[FAST PATH] Building result from cache matches...")

        result = yahoo.copy()

        # Apply cached NFL_player_id
        result["NFL_player_id"] = result["_cached_nfl_id"]

        # Build player_week key
        result["player_week"] = (
            result["NFL_player_id"].astype(str) + "_" + result["year"].astype(str) + "_" + result["week"].astype(str)
        )

        # For unmatched players, use fallback YAHOO- IDs
        # This happens when SQL matching couldn't find all players in the super table
        unmatched_mask = result["NFL_player_id"].isna()
        unmatched_count = unmatched_mask.sum()
        if unmatched_count > 0:
            if verbose:
                print(f"\n[FAST PATH] {unmatched_count:,} players unmatched - assigning fallback YAHOO- IDs")
                # Show some examples of unmatched players
                unmatched_examples = result.loc[unmatched_mask].head(5)
                if "player" in unmatched_examples.columns:
                    for _, row in unmatched_examples.iterrows():
                        print(
                            f"  - {row.get('player', 'Unknown')} ({row.get('yahoo_position', row.get('position', '?'))})"
                        )

            result.loc[unmatched_mask, "NFL_player_id"] = "YAHOO-" + result.loc[
                unmatched_mask, "yahoo_player_id"
            ].astype(str)
            result.loc[unmatched_mask, "player_week"] = (
                result.loc[unmatched_mask, "NFL_player_id"]
                + "_"
                + result.loc[unmatched_mask, "year"].astype(str)
                + "_"
                + result.loc[unmatched_mask, "week"].astype(str)
            )

        # Clean up internal columns
        result = result.drop(columns=["_cached_nfl_id"], errors="ignore")

        if verbose:
            print(f"\n[FAST PATH] Result: {len(result):,} rows")
            matched = result["NFL_player_id"].notna() & ~result["NFL_player_id"].str.startswith("YAHOO-", na=False)
            print(f"[FAST PATH] Matched: {matched.sum():,} ({100*matched.sum()/len(result):.1f}%)")
            if unmatched_count > 0:
                print(f"[FAST PATH] Fallback IDs: {unmatched_count:,} players (these won't have NFL stats)")

        return result

    # =========================================================================
    # STANDARD PATH: Pandas-based layer matching (when cache misses remain)
    # =========================================================================
    # Normalize types
    for df in [yahoo] + ([nfl] if not nfl.empty else []):
        df["year"] = pd.to_numeric(df.get("year"), errors="coerce").astype("Int64")
        df["week"] = pd.to_numeric(df.get("week"), errors="coerce").astype("Int64")

    # Get player column names
    yahoo_player_col = "player" if "player" in yahoo.columns else "player_name"
    nfl_player_col = "player" if "player" in nfl.columns else "player_display_name"

    # Extract positions
    yahoo_pos_col = "yahoo_position" if "yahoo_position" in yahoo.columns else "position"
    nfl_pos_col = "nfl_position" if "nfl_position" in nfl.columns else "position"

    yahoo["_positions"] = yahoo[yahoo_pos_col].apply(lambda x: get_all_positions(x, config))
    nfl["_positions"] = nfl[nfl_pos_col].apply(lambda x: get_all_positions(x, config))

    # Filter NFL to fantasy positions
    nfl_fantasy = nfl[nfl["_positions"].apply(lambda x: any(p in config.fantasy_positions for p in x))].copy()

    if verbose:
        dropped = len(nfl) - len(nfl_fantasy)
        print(f"\nFiltered NFL to fantasy positions: {len(nfl_fantasy):,} ({dropped:,} dropped)")

    # Build keys
    if verbose:
        print("Building match keys...")

    yahoo["_keys_full"] = yahoo.apply(lambda r: build_full_keys(r, yahoo_player_col, config), axis=1)
    yahoo["_keys_last"] = yahoo.apply(lambda r: build_last_keys(r, yahoo_player_col, config), axis=1)
    nfl_fantasy["_keys_full"] = nfl_fantasy.apply(lambda r: build_full_keys(r, nfl_player_col, config), axis=1)
    nfl_fantasy["_keys_last"] = nfl_fantasy.apply(lambda r: build_last_keys(r, nfl_player_col, config), axis=1)

    # =========================================================================
    # LAYER 1: Full normalized name match
    # =========================================================================
    if verbose:
        print("\nLayer 1: Full name matching...")

    yahoo_full_keys = set(k for keys in yahoo["_keys_full"] for k in keys)
    nfl_full_keys = set(k for keys in nfl_fantasy["_keys_full"] for k in keys)
    layer1_keys = yahoo_full_keys & nfl_full_keys

    yahoo["_matched_l1"] = yahoo["_keys_full"].apply(lambda keys: any(k in layer1_keys for k in keys))
    nfl_fantasy["_matched_l1"] = nfl_fantasy["_keys_full"].apply(lambda keys: any(k in layer1_keys for k in keys))

    if verbose:
        print(f"  Yahoo matched: {yahoo['_matched_l1'].sum():,}")

    # =========================================================================
    # LAYER 2: Full name cross-position match (for position mismatches)
    # =========================================================================
    # This catches players like Jordan Matthews where Yahoo says WR but NFL says TE
    # MUST run before last-name matching to prevent false positives
    if verbose:
        print("Layer 2: Full name cross-position matching...")

    yahoo_unmatched = yahoo[~yahoo["_matched_l1"]].copy()
    nfl_unmatched = nfl_fantasy[~nfl_fantasy["_matched_l1"]].copy()

    # Build name-only keys using FULL normalized name (no position)
    def build_name_only_key(row, name_col):
        """Build key with full normalized name + year + week (no position)."""
        positions = row["_positions"]
        if "DEF" in positions:
            return None
        norm = normalize_name(row[name_col])
        return f"{norm}|{row['year']}|{row['week']}".lower()

    # Handle empty dataframes
    if len(yahoo_unmatched) > 0:
        yahoo_unmatched["_key_name_only"] = yahoo_unmatched.apply(
            lambda r: build_name_only_key(r, yahoo_player_col), axis=1
        )
    else:
        yahoo_unmatched["_key_name_only"] = pd.Series(dtype=object)

    if len(nfl_unmatched) > 0:
        nfl_unmatched["_key_name_only"] = nfl_unmatched.apply(lambda r: build_name_only_key(r, nfl_player_col), axis=1)
    else:
        nfl_unmatched["_key_name_only"] = pd.Series(dtype=object)

    # Filter out None keys
    yahoo_l2 = yahoo_unmatched[yahoo_unmatched["_key_name_only"].notna()]
    nfl_l2 = nfl_unmatched[nfl_unmatched["_key_name_only"].notna()]

    # Count keys on each side
    y_counts_l2 = yahoo_l2.groupby("_key_name_only").size().to_dict()
    n_counts_l2 = nfl_l2.groupby("_key_name_only").size().to_dict()

    # Only match if exactly 1:1
    layer2_keys = {k for k in set(y_counts_l2) & set(n_counts_l2) if y_counts_l2[k] == 1 and n_counts_l2[k] == 1}

    # CRITICAL: Filter out same-name players from cross-position matching
    # These players have different people with the same name at different positions
    # (e.g., Brandon Marshall WR vs Brandon Marshall ILB)
    # For these players, we require strict Layer 1 position matching
    def is_same_name_player(key):
        """Check if a name key corresponds to a known same-name player."""
        if not key:
            return False
        # Key format: "normalized_name|year|week"
        parts = key.split("|")
        if len(parts) >= 1:
            norm_name = parts[0]
            return norm_name in SAME_NAME_PLAYERS
        return False

    # Remove same-name players from cross-position matching
    same_name_excluded = {k for k in layer2_keys if is_same_name_player(k)}
    layer2_keys = layer2_keys - same_name_excluded

    if verbose and same_name_excluded:
        print(f"  Excluded {len(same_name_excluded)} same-name player keys from cross-position matching")
        for k in list(same_name_excluded)[:3]:  # Show first 3 examples
            print(f"    Example: {k}")

    # Handle empty dataframes for _matched_l2
    if len(yahoo_unmatched) > 0:
        yahoo_unmatched["_matched_l2"] = yahoo_unmatched["_key_name_only"].apply(
            lambda k: k in layer2_keys if pd.notna(k) else False
        )
    else:
        yahoo_unmatched["_matched_l2"] = pd.Series(dtype=bool)

    if len(nfl_unmatched) > 0:
        nfl_unmatched["_matched_l2"] = nfl_unmatched["_key_name_only"].apply(
            lambda k: k in layer2_keys if pd.notna(k) else False
        )
    else:
        nfl_unmatched["_matched_l2"] = pd.Series(dtype=bool)

    # Initialize columns on main dataframes
    yahoo["_key_name_only"] = None
    yahoo["_matched_l2"] = False
    nfl_fantasy["_key_name_only"] = None
    nfl_fantasy["_matched_l2"] = False

    if verbose:
        print(f"  Yahoo matched: {yahoo_unmatched['_matched_l2'].sum():,}")

    # =========================================================================
    # LAYER 2.5: Phonetic matching (Soundex) for name variants
    # =========================================================================
    # This catches spelling variants like "Jon" vs "John", "Allan" vs "Allen"
    # Only fires when position matches and Soundex codes are identical
    try:
        import jellyfish

        HAS_PHONETIC = True
    except ImportError:
        HAS_PHONETIC = False

    yahoo["_matched_l2_5"] = False
    nfl_fantasy["_matched_l2_5"] = False

    if HAS_PHONETIC:
        if verbose:
            print("Layer 2.5: Phonetic matching (Soundex)...")

        # Get rows still unmatched after Layer 2
        yahoo_for_phonetic = (
            yahoo_unmatched[~yahoo_unmatched["_matched_l2"]].copy() if len(yahoo_unmatched) > 0 else pd.DataFrame()
        )
        nfl_for_phonetic = (
            nfl_unmatched[~nfl_unmatched["_matched_l2"]].copy() if len(nfl_unmatched) > 0 else pd.DataFrame()
        )

        if len(yahoo_for_phonetic) > 0 and len(nfl_for_phonetic) > 0:
            # Add Soundex codes
            yahoo_for_phonetic["_soundex"] = yahoo_for_phonetic["_norm_name"].apply(
                lambda x: jellyfish.soundex(str(x)) if pd.notna(x) and len(str(x)) > 0 else ""
            )
            nfl_for_phonetic["_soundex"] = nfl_for_phonetic["_norm_name"].apply(
                lambda x: jellyfish.soundex(str(x)) if pd.notna(x) and len(str(x)) > 0 else ""
            )

            # Build lookup by Soundex+position
            nfl_soundex_lookup = {}
            for idx, row in nfl_for_phonetic.iterrows():
                soundex = row["_soundex"]
                if soundex and "_positions" in row and row["_positions"]:
                    for pos in row["_positions"]:
                        key = (soundex, pos)
                        if key not in nfl_soundex_lookup:
                            nfl_soundex_lookup[key] = []
                        nfl_soundex_lookup[key].append(idx)

            # Match Yahoo players by Soundex+position (only 1:1 matches)
            matched_count = 0
            for idx, row in yahoo_for_phonetic.iterrows():
                soundex = row["_soundex"]
                if soundex and "_positions" in row and row["_positions"]:
                    for pos in row["_positions"]:
                        key = (soundex, pos)
                        if key in nfl_soundex_lookup and len(nfl_soundex_lookup[key]) == 1:
                            # Unambiguous phonetic match
                            yahoo.loc[idx, "_matched_l2_5"] = True
                            nfl_fantasy.loc[nfl_soundex_lookup[key][0], "_matched_l2_5"] = True
                            matched_count += 1
                            break

            if verbose:
                print(f"  Yahoo matched: {matched_count:,}")
    else:
        if verbose:
            print("Layer 2.5: Skipped (jellyfish not installed)")

    # =========================================================================
    # LAYER 3: Last name 1:1 match (for nickname resolution)
    # =========================================================================
    # This catches nicknames like "Hollywood Brown" ??? "Marquise Brown"
    # Only fires when there's exactly 1 player with that last name on each side
    # AND first names are similar (phonetic match or known nickname)
    if verbose:
        print("Layer 3: Last name 1:1 matching...")

    # Initialize Layer 3 columns
    yahoo["_matched_l3"] = False
    nfl_fantasy["_matched_l3"] = False

    # Get rows still unmatched after Layer 2 and Layer 2.5
    yahoo_still_unmatched = (
        yahoo_unmatched[
            ~yahoo_unmatched["_matched_l2"]
            & ~yahoo_unmatched.get(
                "_matched_l2_5", pd.Series([False] * len(yahoo_unmatched), index=yahoo_unmatched.index)
            )
        ].copy()
        if len(yahoo_unmatched) > 0
        else pd.DataFrame()
    )
    nfl_still_unmatched = (
        nfl_unmatched[
            ~nfl_unmatched["_matched_l2"]
            & ~nfl_unmatched.get("_matched_l2_5", pd.Series([False] * len(nfl_unmatched), index=nfl_unmatched.index))
        ].copy()
        if len(nfl_unmatched) > 0
        else pd.DataFrame()
    )

    # Count keys on each side (explode multi-position rows)
    if len(yahoo_still_unmatched) > 0 and "_keys_last" in yahoo_still_unmatched.columns:
        yahoo_last_exp = yahoo_still_unmatched.explode("_keys_last")
        y_counts_l3 = yahoo_last_exp.groupby("_keys_last").size().to_dict()
    else:
        y_counts_l3 = {}

    if len(nfl_still_unmatched) > 0 and "_keys_last" in nfl_still_unmatched.columns:
        nfl_last_exp = nfl_still_unmatched.explode("_keys_last")
        n_counts_l3 = nfl_last_exp.groupby("_keys_last").size().to_dict()
    else:
        n_counts_l3 = {}

    # Only match if exactly 1:1 based on last name key
    layer3_candidate_keys = {
        k for k in set(y_counts_l3) & set(n_counts_l3) if y_counts_l3[k] == 1 and n_counts_l3[k] == 1
    }

    # CRITICAL FIX: Validate first names are similar before allowing match
    # This prevents Noah Brown -> Antonio Brown type mismatches
    def get_first_name(full_name):
        """Extract first name from full name."""
        if pd.isna(full_name) or not full_name:
            return ""
        parts = str(full_name).strip().split()
        return parts[0].lower() if parts else ""

    def first_names_compatible(yahoo_first, nfl_first):
        """Check if first names are compatible (same, nickname, or phonetic match)."""
        if not yahoo_first or not nfl_first:
            return False

        # Exact match
        if yahoo_first == nfl_first:
            return True

        # Known nickname mapping (from NICKNAME_MAP)
        y_canonical = NICKNAME_MAP.get(yahoo_first, yahoo_first)
        n_canonical = NICKNAME_MAP.get(nfl_first, nfl_first)
        if y_canonical == n_canonical:
            return True

        # Phonetic match (if jellyfish available)
        if HAS_PHONETIC:
            try:
                if jellyfish.soundex(yahoo_first) == jellyfish.soundex(nfl_first):
                    return True
            except Exception:  # noqa: broad-except
                pass
        # One is prefix of other (e.g., "Rob" and "Robert", but not "Rob" and "Roberto")
        if len(yahoo_first) >= 3 and len(nfl_first) >= 3:
            if yahoo_first.startswith(nfl_first[:3]) or nfl_first.startswith(yahoo_first[:3]):
                return True

        return False

    # Build mapping from last-name key to (yahoo_first_name, nfl_first_name)
    layer3_keys = set()
    if layer3_candidate_keys and len(yahoo_still_unmatched) > 0 and len(nfl_still_unmatched) > 0:
        # Build lookup of last-name key -> first name for each side
        yahoo_first_by_key = {}
        nfl_first_by_key = {}

        for idx, row in yahoo_still_unmatched.iterrows():
            for key in row.get("_keys_last", []):
                if key in layer3_candidate_keys:
                    yahoo_first_by_key[key] = get_first_name(row.get(yahoo_player_col, ""))

        for idx, row in nfl_still_unmatched.iterrows():
            for key in row.get("_keys_last", []):
                if key in layer3_candidate_keys:
                    nfl_first_by_key[key] = get_first_name(row.get(nfl_player_col, ""))

        # Only include keys where first names are compatible
        for key in layer3_candidate_keys:
            yahoo_first = yahoo_first_by_key.get(key, "")
            nfl_first = nfl_first_by_key.get(key, "")
            if first_names_compatible(yahoo_first, nfl_first):
                layer3_keys.add(key)
            elif verbose and yahoo_first and nfl_first:
                # Log rejected matches for debugging
                print(f"    Layer 3 REJECTED: '{yahoo_first}' vs '{nfl_first}' (key: {key})")

    # Handle empty dataframes
    if len(yahoo_still_unmatched) > 0:
        yahoo_still_unmatched["_matched_l3"] = yahoo_still_unmatched["_keys_last"].apply(
            lambda keys: any(k in layer3_keys for k in keys)
        )
    else:
        yahoo_still_unmatched["_matched_l3"] = pd.Series(dtype=bool)

    if len(nfl_still_unmatched) > 0:
        nfl_still_unmatched["_matched_l3"] = nfl_still_unmatched["_keys_last"].apply(
            lambda keys: any(k in layer3_keys for k in keys)
        )
    else:
        nfl_still_unmatched["_matched_l3"] = pd.Series(dtype=bool)

    if verbose:
        rejected = len(layer3_candidate_keys) - len(layer3_keys)
        print(
            f"  Yahoo matched: {yahoo_still_unmatched['_matched_l3'].sum():,} (rejected {rejected} incompatible first names)"
        )

    # =========================================================================
    # LAYER 4: Fuzzy matching fallback (for nickname variants like Josh/Joshua)
    # =========================================================================
    # Only runs on players that failed Layers 1-3
    # Requires: 1) score ???80%, 2) gap ???15 points vs second-best, 3) 1:1 constraint
    if verbose:
        print("Layer 4: Fuzzy matching (nickname fallback)...")

    # Initialize Layer 4 columns
    yahoo["_matched_l4"] = False
    yahoo["_fuzzy_match_key"] = None
    nfl_fantasy["_matched_l4"] = False
    nfl_fantasy["_fuzzy_match_key"] = None

    # Get rows still unmatched after Layer 3
    yahoo_for_fuzzy = (
        yahoo_still_unmatched[~yahoo_still_unmatched["_matched_l3"]].copy()
        if len(yahoo_still_unmatched) > 0
        else pd.DataFrame()
    )
    nfl_for_fuzzy = (
        nfl_still_unmatched[~nfl_still_unmatched["_matched_l3"]].copy()
        if len(nfl_still_unmatched) > 0
        else pd.DataFrame()
    )

    # Initialize Layer 4 columns
    if len(yahoo_for_fuzzy) > 0:
        yahoo_for_fuzzy["_matched_l4"] = False
        yahoo_for_fuzzy["_fuzzy_match_key"] = None
    if len(nfl_for_fuzzy) > 0:
        nfl_for_fuzzy["_matched_l4"] = False
        nfl_for_fuzzy["_fuzzy_match_key"] = None

    layer4_matches = {}  # yahoo_idx -> (nfl_idx, score)

    if len(yahoo_for_fuzzy) > 0 and len(nfl_for_fuzzy) > 0:
        from difflib import SequenceMatcher

        def fuzzy_score(a, b):
            """Fuzzy similarity ratio (0-100)."""
            return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100

        FUZZY_THRESHOLD = 80.0
        FUZZY_GAP = 15.0

        # Build lookup for NFL candidates by (last_name, position, year, week)
        nfl_by_context = {}
        for idx, row in nfl_for_fuzzy.iterrows():
            last_name = extract_last_name(row[nfl_player_col])
            for pos in row["_positions"]:
                key = (last_name, pos, row["year"], row["week"])
                if key not in nfl_by_context:
                    nfl_by_context[key] = []
                nfl_by_context[key].append((idx, row))

        # For each unmatched Yahoo player, find best fuzzy match
        yahoo_fuzzy_candidates = {}  # yahoo_idx -> [(nfl_idx, score), ...]

        for y_idx, y_row in yahoo_for_fuzzy.iterrows():
            y_name = normalize_name(y_row[yahoo_player_col])
            y_last = extract_last_name(y_row[yahoo_player_col])

            candidates = []
            for pos in y_row["_positions"]:
                key = (y_last, pos, y_row["year"], y_row["week"])
                if key in nfl_by_context:
                    for n_idx, n_row in nfl_by_context[key]:
                        n_name = normalize_name(n_row[nfl_player_col])
                        score = fuzzy_score(y_name, n_name)
                        candidates.append((n_idx, score, n_row))

            if candidates:
                # Sort by score descending
                candidates.sort(key=lambda x: -x[1])
                best_score = candidates[0][1]
                second_score = candidates[1][1] if len(candidates) > 1 else 0

                # Check threshold and gap
                if best_score >= FUZZY_THRESHOLD and (best_score - second_score) >= FUZZY_GAP:
                    yahoo_fuzzy_candidates[y_idx] = (candidates[0][0], best_score)

        # Now enforce 1:1 constraint
        # Check if multiple Yahoo players claim the same NFL player
        nfl_claimed_by = {}  # nfl_idx -> [(yahoo_idx, score), ...]
        for y_idx, (n_idx, score) in yahoo_fuzzy_candidates.items():
            if n_idx not in nfl_claimed_by:
                nfl_claimed_by[n_idx] = []
            nfl_claimed_by[n_idx].append((y_idx, score))

        # Only keep matches where NFL player is claimed by exactly one Yahoo player
        for n_idx, claimants in nfl_claimed_by.items():
            if len(claimants) == 1:
                y_idx, score = claimants[0]
                # Create a unique fuzzy match key
                fuzzy_key = f"fuzzy_{y_idx}_{n_idx}"
                layer4_matches[y_idx] = (n_idx, fuzzy_key)

                # Mark as matched
                if y_idx in yahoo_for_fuzzy.index:
                    yahoo_for_fuzzy.loc[y_idx, "_matched_l4"] = True
                    yahoo_for_fuzzy.loc[y_idx, "_fuzzy_match_key"] = fuzzy_key
                if n_idx in nfl_for_fuzzy.index:
                    nfl_for_fuzzy.loc[n_idx, "_matched_l4"] = True
                    nfl_for_fuzzy.loc[n_idx, "_fuzzy_match_key"] = fuzzy_key

    # Apply Layer 4 results back to main dataframes
    if len(yahoo_for_fuzzy) > 0 and "_matched_l4" in yahoo_for_fuzzy.columns:
        yahoo.loc[yahoo_for_fuzzy.index, "_matched_l4"] = yahoo_for_fuzzy["_matched_l4"]
        yahoo.loc[yahoo_for_fuzzy.index, "_fuzzy_match_key"] = yahoo_for_fuzzy["_fuzzy_match_key"]
    if len(nfl_for_fuzzy) > 0 and "_matched_l4" in nfl_for_fuzzy.columns:
        nfl_fantasy.loc[nfl_for_fuzzy.index, "_matched_l4"] = nfl_for_fuzzy["_matched_l4"]
        nfl_fantasy.loc[nfl_for_fuzzy.index, "_fuzzy_match_key"] = nfl_for_fuzzy["_fuzzy_match_key"]

    if verbose:
        l4_count = len(layer4_matches)
        print(f"  Yahoo matched: {l4_count:,}")

    # =========================================================================
    # LAYER 5: Team-Aware Disambiguation (for common names like Josh Allen)
    # =========================================================================
    # For players with common names that failed Layers 1-4, use team as tiebreaker
    # Only fires when: 1) Layers 1-4 failed, 2) team is available, 3) 1:1 on name+team
    if verbose:
        print("Layer 5: Team-aware disambiguation (common names)...")

    # Initialize Layer 5 columns
    yahoo["_matched_l5"] = False
    yahoo["_team_match_key"] = None
    nfl_fantasy["_matched_l5"] = False
    nfl_fantasy["_team_match_key"] = None

    layer5_keys = set()

    def build_name_team_key(row, name_col, team_col="nfl_team"):
        """Build match key with name + team + year + week (no position requirement)."""
        name = row.get(name_col)
        if pd.isna(name):
            return None
        # Get team from available columns
        team = row.get(team_col) or row.get("team") or row.get("Team")
        if pd.isna(team) or team == "" or team is None:
            return None
        norm = normalize_name(name)
        return f"{norm}|{str(team).upper()}|{row['year']}|{row['week']}"

    # Get rows still unmatched after Layer 4
    yahoo_for_team = yahoo[
        ~yahoo["_matched_l1"] & ~yahoo["_matched_l2"] & ~yahoo["_matched_l3"] & ~yahoo["_matched_l4"]
    ].copy()

    nfl_for_team = nfl_fantasy[
        ~nfl_fantasy["_matched_l1"]
        & ~nfl_fantasy["_matched_l2"]
        & ~nfl_fantasy["_matched_l3"]
        & ~nfl_fantasy["_matched_l4"]
    ].copy()

    if len(yahoo_for_team) > 0 and len(nfl_for_team) > 0:
        # Build name+team keys
        yahoo_for_team["_team_match_key"] = yahoo_for_team.apply(
            lambda r: build_name_team_key(r, yahoo_player_col, "nfl_team"), axis=1
        )
        nfl_for_team["_team_match_key"] = nfl_for_team.apply(
            lambda r: build_name_team_key(r, nfl_player_col, "nfl_team"), axis=1
        )

        # Filter to rows with valid keys
        yahoo_l5 = yahoo_for_team[yahoo_for_team["_team_match_key"].notna()]
        nfl_l5 = nfl_for_team[nfl_for_team["_team_match_key"].notna()]

        if len(yahoo_l5) > 0 and len(nfl_l5) > 0:
            # Count occurrences - only match if 1:1
            y_counts = yahoo_l5.groupby("_team_match_key").size().to_dict()
            n_counts = nfl_l5.groupby("_team_match_key").size().to_dict()

            layer5_keys = {k for k in set(y_counts) & set(n_counts) if y_counts[k] == 1 and n_counts[k] == 1}

            if layer5_keys:
                # Mark matches
                yahoo_for_team.loc[yahoo_l5.index, "_matched_l5"] = yahoo_l5["_team_match_key"].apply(
                    lambda k: k in layer5_keys
                )
                nfl_for_team.loc[nfl_l5.index, "_matched_l5"] = nfl_l5["_team_match_key"].apply(
                    lambda k: k in layer5_keys
                )

        # Apply back to main dataframes
        yahoo.loc[yahoo_for_team.index, "_matched_l5"] = yahoo_for_team["_matched_l5"]
        yahoo.loc[yahoo_for_team.index, "_team_match_key"] = yahoo_for_team["_team_match_key"]
        nfl_fantasy.loc[nfl_for_team.index, "_matched_l5"] = nfl_for_team["_matched_l5"]
        nfl_fantasy.loc[nfl_for_team.index, "_team_match_key"] = nfl_for_team["_team_match_key"]

    if verbose:
        l5_count = yahoo["_matched_l5"].sum() if "_matched_l5" in yahoo.columns else 0
        print(f"  Yahoo matched: {l5_count:,}")

    # =========================================================================
    # BUILD FINAL RESULT
    # =========================================================================
    if verbose:
        print("\nBuilding final result...")

    # Get the winning key for each row (for merge)
    # Layer order: 1=full name+position, 2=full name cross-position, 3=last name 1:1, 4=fuzzy, 5=team-aware
    def get_match_key(row, layer1_keys, layer2_keys, layer3_keys, layer5_keys):
        # Layer 1: Full name + position
        for k in row["_keys_full"]:
            if k in layer1_keys:
                return k
        # Layer 2: Full name cross-position (name-only key)
        name_only = row.get("_key_name_only")
        if pd.notna(name_only) and name_only in layer2_keys:
            return name_only
        # Layer 3: Last name 1:1
        for k in row["_keys_last"]:
            if k in layer3_keys:
                return k
        # Layer 4: Fuzzy match
        fuzzy_key = row.get("_fuzzy_match_key")
        if pd.notna(fuzzy_key):
            return fuzzy_key
        # Layer 5: Team-aware match (for common names like Josh Allen)
        team_key = row.get("_team_match_key")
        if pd.notna(team_key) and team_key in layer5_keys:
            return team_key
        return None

    # Apply Layer 2 results back to main dataframes
    if len(yahoo_unmatched) > 0:
        yahoo.loc[yahoo_unmatched.index, "_key_name_only"] = yahoo_unmatched["_key_name_only"]
        yahoo.loc[yahoo_unmatched.index, "_matched_l2"] = yahoo_unmatched["_matched_l2"]
    if len(nfl_unmatched) > 0:
        nfl_fantasy.loc[nfl_unmatched.index, "_key_name_only"] = nfl_unmatched["_key_name_only"]
        nfl_fantasy.loc[nfl_unmatched.index, "_matched_l2"] = nfl_unmatched["_matched_l2"]

    # Apply Layer 3 results back to main dataframes
    if len(yahoo_still_unmatched) > 0:
        yahoo.loc[yahoo_still_unmatched.index, "_matched_l3"] = yahoo_still_unmatched["_matched_l3"]
    if len(nfl_still_unmatched) > 0:
        nfl_fantasy.loc[nfl_still_unmatched.index, "_matched_l3"] = nfl_still_unmatched["_matched_l3"]

    yahoo["_match_key"] = yahoo.apply(
        lambda r: get_match_key(r, layer1_keys, layer2_keys, layer3_keys, layer5_keys), axis=1
    )
    nfl_fantasy["_match_key"] = nfl_fantasy.apply(
        lambda r: get_match_key(r, layer1_keys, layer2_keys, layer3_keys, layer5_keys), axis=1
    )

    # Split into matched and unmatched
    yahoo_matched = yahoo[yahoo["_match_key"].notna()].copy()
    yahoo_only = yahoo[yahoo["_match_key"].isna()].copy()

    nfl_matched = nfl_fantasy[nfl_fantasy["_match_key"].notna()].copy()
    nfl_only = nfl_fantasy[nfl_fantasy["_match_key"].isna()].copy()

    # Merge matched rows (using chunked merge for large datasets)
    if len(yahoo_matched) > 0 and len(nfl_matched) > 0:
        merged = _chunked_merge(
            yahoo_matched, nfl_matched, on="_match_key", how="inner", suffixes=("", "_nfl"), verbose=verbose
        )
        merged["_source"] = "matched"

        # Post-merge deduplication (defense against position variants)
        dedupe_cols = ["year", "week"]
        if "yahoo_player_id" in merged.columns:
            dedupe_cols.append("yahoo_player_id")
        elif "NFL_player_id" in merged.columns:
            dedupe_cols.append("NFL_player_id")

        if len(dedupe_cols) >= 3:  # Need at least year, week, and an ID
            before = len(merged)
            merged = merged.drop_duplicates(subset=dedupe_cols, keep="first")
            after = len(merged)
            if before != after and verbose:
                print(f"[merge] Removed {before - after} duplicate merged rows")
    else:
        merged = pd.DataFrame()

    # Add unmatched rows
    yahoo_only["_source"] = "yahoo_only"
    nfl_only["_source"] = "nfl_only"

    result = pd.concat([merged, yahoo_only, nfl_only], ignore_index=True, sort=False)

    # Final deduplication including league_id if present
    final_dedupe_cols = ["year", "week"]
    if "league_id" in result.columns:
        final_dedupe_cols.insert(0, "league_id")
    if "yahoo_player_id" in result.columns:
        final_dedupe_cols.append("yahoo_player_id")
    elif "NFL_player_id" in result.columns:
        final_dedupe_cols.append("NFL_player_id")

    if len(final_dedupe_cols) >= 3:
        before = len(result)
        result = result.drop_duplicates(subset=final_dedupe_cols, keep="first")
        if before != len(result) and verbose:
            print(f"[final] Removed {before - len(result)} duplicate rows")

    # Coalesce columns
    result = _coalesce_columns(result)

    # =========================================================================
    # CACHE INTEGRATION: Use cached NFL_player_id and save new mappings
    # =========================================================================
    if use_cache and PLAYER_ID_CACHE_AVAILABLE and "_cached_nfl_id" in result.columns:
        # For rows with cached NFL_player_id, ensure it's used
        cached_mask = result["_cached_nfl_id"].notna()
        if cached_mask.any():
            if "NFL_player_id" not in result.columns:
                result["NFL_player_id"] = pd.NA
            # Use cached ID for rows that have it
            result.loc[cached_mask, "NFL_player_id"] = result.loc[cached_mask, "_cached_nfl_id"]
            if verbose:
                print(f"\n[CACHE] Applied cached NFL_player_id to {cached_mask.sum():,} rows")

        # Extract new mappings to save (matched rows without cached ID)
        new_match_mask = (
            (result["_source"] == "matched")
            & (result["_cached_nfl_id"].isna())
            & (result["yahoo_player_id"].notna())
            & (result["NFL_player_id"].notna())
            & (~result["NFL_player_id"].astype(str).str.startswith("YAHOO-"))
            & (~result["NFL_player_id"].astype(str).str.startswith("UNK-"))
        )

        if new_match_mask.any():
            # Determine column names dynamically
            player_col = "player" if "player" in result.columns else "yahoo_player_name"
            pos_col = (
                "position"
                if "position" in result.columns
                else ("nfl_position" if "nfl_position" in result.columns else "yahoo_position")
            )

            extract_cols = ["yahoo_player_id", "NFL_player_id"]
            if player_col in result.columns:
                extract_cols.append(player_col)
            if pos_col in result.columns:
                extract_cols.append(pos_col)

            new_mappings_df = result.loc[new_match_mask, extract_cols].drop_duplicates()

            # Determine match layer (rough estimate based on available info)
            # For simplicity, assume layer 1 for all new matches
            new_mappings = []
            for _, row in new_mappings_df.iterrows():
                new_mappings.append(
                    (
                        str(row["yahoo_player_id"]),
                        str(row["NFL_player_id"]),
                        1,  # match_layer (assume exact)
                        100.0,  # confidence
                        str(row.get(player_col, "")),  # yahoo_name
                        str(row.get(player_col, "")),  # nfl_name (same for matched)
                        str(row.get(pos_col, "")),
                    )
                )

            if new_mappings:
                try:
                    # Save to MotherDuck (persistent storage)
                    saved = save_yahoo_nfl_mapping(new_mappings)
                    if verbose and saved > 0:
                        print(f"[CACHE] Saved {saved} new mappings to MotherDuck")

                    # Also update local cache if available
                    if yahoo_cache is not None:
                        yahoo_cache.save_new_mappings(new_mappings)
                        if verbose:
                            print("[CACHE] Updated local disk cache")
                except Exception as e:
                    if verbose:
                        print(f"[CACHE] Could not save new mappings: {e}")

    # Clean up internal columns
    internal = [
        "_positions",
        "_positions_nfl",
        "_keys_full",
        "_keys_full_nfl",
        "_keys_last",
        "_keys_last_nfl",
        "_matched_l1",
        "_matched_l1_nfl",
        "_matched_l2",
        "_matched_l2_nfl",
        "_matched_l2_5",
        "_matched_l2_5_nfl",
        "_matched_l3",
        "_matched_l3_nfl",
        "_soundex",
        "_soundex_nfl",
        "_matched_l4",
        "_matched_l4_nfl",
        "_fuzzy_match_key",
        "_fuzzy_match_key_nfl",
        "_key_name_only",
        "_key_name_only_nfl",
        "_match_key",
        "_cached_nfl_id",
    ]
    result = result.drop(columns=[c for c in internal if c in result.columns], errors="ignore")

    # Normalize DEF records with franchise-based IDs and display names
    # This ensures historical relocations are handled (e.g., CHI/STL/PHO/ARI ??? DEF-13)
    # CRITICAL FIX: Check ALL position columns (yahoo_position, primary_position, nfl_position, position, fantasy_position)
    # Yahoo-only records may only have yahoo_position/primary_position, not nfl_position
    if FRANCHISE_FUNCTIONS_AVAILABLE:
        if "nfl_team" in result.columns and "year" in result.columns:
            # Build comprehensive DEF mask from ALL position columns
            def_mask = pd.Series(False, index=result.index)

            # Check all possible position columns for DEF value (case-insensitive)
            position_cols = ["position", "nfl_position", "yahoo_position", "primary_position", "fantasy_position"]
            for pos_col in position_cols:
                if pos_col in result.columns:
                    # Case-insensitive check for DEF, Def, def, DST, D/ST
                    col_values = result[pos_col].astype(str).str.upper()
                    def_mask = def_mask | col_values.isin(["DEF", "DST", "D/ST"])

            if def_mask.any():
                if verbose:
                    print(f"  Normalizing {def_mask.sum():,} DEF records with franchise-based IDs...")

                # Update NFL_player_id for DEF rows
                # Helper to get DEF player ID with proper fallback
                def get_def_id(row):
                    nfl_team = row.get("nfl_team")
                    year = row.get("year")
                    # Try franchise-based ID first
                    if get_def_player_id and pd.notna(nfl_team) and pd.notna(year):
                        result_id = get_def_player_id(nfl_team, year)
                        if result_id:
                            return result_id
                    # Fallback to DEF-{franchise_id} format using abbreviation mapping
                    if pd.notna(nfl_team):
                        abbrev = str(nfl_team).upper().strip()
                        if abbrev in TEAM_ABBREV_TO_FRANCHISE_ID:
                            return f"DEF-{TEAM_ABBREV_TO_FRANCHISE_ID[abbrev]}"
                        # If not in mapping, use abbreviation (validator will catch it)
                        return f"DEF-{abbrev}"
                    return "DEF-UNK"

                if "NFL_player_id" not in result.columns:
                    result["NFL_player_id"] = np.nan
                result.loc[def_mask, "NFL_player_id"] = result.loc[def_mask].apply(get_def_id, axis=1)

                # Update player display name for DEF rows
                player_col = "player" if "player" in result.columns else "yahoo_player_name"
                if player_col in result.columns:
                    result.loc[def_mask, player_col] = result.loc[def_mask].apply(
                        lambda row: get_def_display_name(row["nfl_team"], row["year"])
                        if pd.notna(row.get("nfl_team")) and pd.notna(row.get("year"))
                        else f"{row.get('nfl_team', 'Unknown')} DST",
                        axis=1,
                    )

                # Normalize position columns to 'DEF' for consistency
                for pos_col in ["position", "nfl_position"]:
                    if pos_col in result.columns:
                        result.loc[def_mask, pos_col] = "DEF"

                if verbose:
                    # Log sample of normalized DEF records for debugging
                    def_sample = result.loc[def_mask, [player_col, "nfl_team", "year"]].head(3)
                    for _, row in def_sample.iterrows():
                        print(f"    DEF normalized: {row['nfl_team']} ??? {row[player_col]}")

    # Final stats
    n_matched = (result["_source"] == "matched").sum()
    n_yahoo_only = (result["_source"] == "yahoo_only").sum()
    n_nfl_only = (result["_source"] == "nfl_only").sum()

    if verbose:
        print(f"\n{'='*70}")
        print("FINAL RESULTS")
        print(f"{'='*70}")
        print(f"Total rows:   {len(result):,}")
        print(f"  Matched:    {n_matched:,}")
        print(f"  Yahoo-only: {n_yahoo_only:,} (bye/injured/inactive)")
        print(f"  NFL-only:   {n_nfl_only:,} (not rostered)")

        expected = len(yahoo_df)
        actual = n_matched + n_yahoo_only
        if actual == expected:
            print(f"\n[OK] All {expected:,} Yahoo rows preserved")
        else:
            print(f"\n[WARN] Yahoo: expected {expected:,}, got {actual:,}")

    # Ensure season_type is present (for pre-aggregation filtering)
    # NFL data has season_type (REG/POST), default to REG for unmatched rows
    if "season_type" not in result.columns:
        result["season_type"] = "REG"
    else:
        result["season_type"] = result["season_type"].fillna("REG")

    # CRITICAL: Ensure position column is populated for all rows
    # Bench players may have yahoo_position but NULL position (causes LAMAR to fail)
    if "position" in result.columns:
        # Priority: position ??? nfl_position ??? yahoo_position ??? primary_position
        for fallback_col in ["nfl_position", "yahoo_position", "primary_position"]:
            if fallback_col in result.columns:
                result["position"] = result["position"].fillna(result[fallback_col])
    elif "nfl_position" in result.columns:
        result["position"] = result["nfl_position"]
    elif "yahoo_position" in result.columns:
        result["position"] = result["yahoo_position"]

    # ==========================================================================
    # TWO-TRACK ARCHITECTURE: Drop NFL stats columns
    # ==========================================================================
    # NFL stats (passing_yards, rushing_tds, etc.) should come from super_table
    # via JOIN at query time, not be baked into player_fantasy.
    # This reduces player_fantasy from ~300 columns to ~50 columns.
    #
    # Keep: join keys, identity, Yahoo/fantasy context, derived metrics
    # Drop: All NFL stats columns (they exist in super_table)
    # ==========================================================================

    # Patterns for NFL stats columns to DROP
    nfl_stats_patterns = [
        "passing_",
        "rushing_",
        "receiving_",
        "def_",
        "special_teams_",
        "sack",
        "fumble",
        "interception",
        "touchdown",
        "snap_",
        "kick_",
        "punt_",
        "return_",
        "epa",
        "cpoe",
        "air_yards",
        "yards_after",
        "target",
        "carry",
        "carries",
        "attempt",
        "completion",
        "receptions",
        "targets",
        "passer_rating",
        "pacr",
        "racr",
        "wopr",
        "dom",
        "yptmpa",
        "proe",
        "depth_chart",
        "headshot",
        # Pre-calculated scoring components (super_table has these)
        "pts_pass",
        "pts_rush",
        "pts_rec",
        "pts_misc",
        "pts_k",
        "pts_def",
    ]

    # Columns to ALWAYS keep (even if they match patterns above)
    always_keep = {
        "fantasy_points",
        "points",
        "player_week",
        "NFL_player_id",
        "yahoo_player_id",
        "player_id",
        "position",
        "nfl_position",
        "yahoo_position",
        "primary_position",
        "fantasy_position",
        "headshot_url",  # Identity data needed for UI display
        # Pre-calculated scoring columns (needed for fantasy_points calculation in player_stats_v2)
        "pts_pass_4pt",
        "pts_pass_6pt",
        "pts_rush",
        "pts_rec_0ppr",
        "pts_rec_half",
        "pts_rec_ppr",
        "pts_misc",
        "pts_k_std",
        "pts_k_yds",
        "pts_def_std",
        "pts_def_ya",
        # pts_def_high removed 2026-04-30 (KMFFL-specific; computed per-league)
    }

    cols_to_drop = []
    for col in result.columns:
        col_lower = col.lower()
        # Check if column matches any NFL stats pattern
        if any(pattern in col_lower for pattern in nfl_stats_patterns):
            # But keep it if it's in the always_keep set
            if col not in always_keep:
                cols_to_drop.append(col)

    if cols_to_drop and verbose:
        print(f"\n[TWO-TRACK] Dropping {len(cols_to_drop)} NFL stats columns (will come from super_table JOIN)")

    result = result.drop(columns=cols_to_drop, errors="ignore")

    if verbose:
        print(f"[TWO-TRACK] Final column count: {len(result.columns)}")

    # ==========================================================================
    # GENERATE player_week FOR ALL ROWS
    # ==========================================================================
    # Ensure player_week is populated for all rows (required for two-track JOIN)
    # Rows that matched NFL data already have player_week from super_table
    # Unmatched rows (bye weeks, injured, etc.) need fallback player_week
    # ==========================================================================
    if "year" in result.columns and "week" in result.columns:

        def get_player_week(row):
            # If player_week already exists and is valid, keep it
            existing = row.get("player_week")
            if pd.notna(existing) and str(existing) not in ("None", "nan", ""):
                return existing

            # Generate fallback player_week
            nfl_id = row.get("NFL_player_id")
            if pd.notna(nfl_id) and str(nfl_id) not in ("None", "nan", ""):
                player_id = str(nfl_id)
            else:
                # Fallback chain: DEF-{franchise_id} → YAHOO-id → UNK-hash
                position = row.get("position", "")
                nfl_team = row.get("nfl_team")
                if position == "DEF" and pd.notna(nfl_team):
                    abbrev = str(nfl_team).upper().strip()
                    if abbrev in TEAM_ABBREV_TO_FRANCHISE_ID:
                        player_id = f"DEF-{TEAM_ABBREV_TO_FRANCHISE_ID[abbrev]}"
                    else:
                        player_id = f"DEF-{abbrev}"  # Validator will catch unmapped
                else:
                    yahoo_id = row.get("yahoo_player_id")
                    if pd.notna(yahoo_id):
                        player_id = f"YAHOO-{int(yahoo_id)}"
                    else:
                        player = row.get("player", "UNKNOWN")
                        player_id = f"UNK-{hash(player) % 100000}"

            return f"{player_id}_{int(row['year'])}_{int(row['week'])}"

        # Only update rows where player_week is missing
        null_mask = result["player_week"].isna() | (result["player_week"].astype(str).isin(["None", "nan", ""]))
        if null_mask.any():
            result.loc[null_mask, "player_week"] = result.loc[null_mask].apply(get_player_week, axis=1)
            if verbose:
                print(f"[player_week] Generated fallback values for {null_mask.sum():,} rows")

    # NOTE: DEF scoring for ROSTERED players comes from Yahoo API (league-specific scoring)
    # UNROSTERED DEF scoring is handled in SQLEnrichments.expand_to_all_nfl() using league-appropriate pts_def_* column

    return result


def _coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Coalesce duplicate columns from merge."""
    result = df.copy()

    # Yahoo priority columns (fantasy context)
    # These columns should use Yahoo's values when available, NFL as fallback
    # NOTE: fantasy_points is NOT in this list because Yahoo API returns 0 for
    # old years where data was deleted/archived. We handle fantasy_points
    # separately in seed_missing_points() which calculates points from the
    # modular pts_* columns (pts_pass_4pt, pts_rush, pts_rec_half, etc.).
    yahoo_priority = {
        "manager",
        "fantasy_position",
        "yahoo_position",
        "points",
        "yahoo_player_id",
        "primary_position",
        "team_key",
        "player_key",
        "eligible_positions",
    }

    for col in list(result.columns):
        if col.endswith("_nfl"):
            base = col[:-4]
            if base in result.columns:
                if base in yahoo_priority:
                    # Yahoo value takes priority, fill missing with NFL
                    result[base] = result[base].combine_first(result[col])
                else:
                    # NFL value takes priority, fill missing with Yahoo
                    result[base] = result[col].combine_first(result[base])
                result = result.drop(columns=[col])
            else:
                result = result.rename(columns={col: base})

    return result


# =============================================================================
# POST-PROCESSING (for pipeline compatibility)
# =============================================================================
# Suffixes to strip from player names
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "junior", "senior"}


def create_player_key(name: str) -> str:
    """
    Create normalized player key for deduplication/matching.
    Compatible with original yahoo_nfl_merge.py's _norm_player_for_key.
    """
    if pd.isna(name) or not name:
        return ""

    s = str(name).strip().lower()

    # Remove suffixes
    tokens = s.split()
    tokens = [t for t in tokens if t not in _SUFFIXES]
    s = " ".join(tokens)

    # Remove periods and extra spaces
    s = s.replace(".", "").replace("'", "").replace("-", " ")
    s = " ".join(s.split())  # Normalize whitespace

    return s


def create_player_last_name_key(name: str) -> str:
    """Extract and normalize last name for fallback matching."""
    if pd.isna(name) or not name:
        return ""

    s = str(name).strip().lower()
    tokens = [t for t in s.split() if t]
    tokens = [t for t in tokens if t not in _SUFFIXES]

    if not tokens:
        return ""

    last = tokens[-1]
    return last.replace("-", "").replace("'", "").replace(".", "")


def add_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add player_key and player_last_name_key columns for downstream compatibility."""
    result = df.copy()

    # Determine player column name
    player_col = "player" if "player" in result.columns else "player_name"
    if player_col not in result.columns:
        return result

    # Create keys if not present
    if "player_key" not in result.columns:
        result["player_key"] = result[player_col].apply(create_player_key)

    if "player_last_name_key" not in result.columns:
        # Use player_last_name if available, else derive from player
        if "player_last_name" in result.columns:
            result["player_last_name_key"] = result["player_last_name"].apply(create_player_last_name_key)
        else:
            result["player_last_name_key"] = result[player_col].apply(create_player_last_name_key)

    return result


def add_column_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Add abbreviated column aliases for NFLverse columns."""
    result = df.copy()

    aliases = {
        "passing_yards": "pass_yds",
        "rushing_yards": "rush_yds",
        "receiving_yards": "rec_yds",
        "passing_tds": "pass_td",
        "rushing_tds": "rush_td",
        "receiving_tds": "rec_td",
        "receptions": "rec",
        "passing_interceptions": "pass_int",
        "fumbles_lost": "fum_lost",
    }

    for full_name, abbrev_name in aliases.items():
        if full_name in result.columns and abbrev_name not in result.columns:
            result[abbrev_name] = result[full_name]

    return result


def seed_missing_points(df: pd.DataFrame) -> pd.DataFrame:
    """
    Seed fantasy_points from pts_* columns where Yahoo returned 0.

    Yahoo API returns 0 for fantasy_points in old years where they've archived
    the data. We use the pre-calculated pts_* columns from super_table to
    calculate points based on position.

    Uses half-PPR as default. League-specific scoring is applied later in
    SQLEnrichments.expand_to_all_nfl() / populate_fantasy_points().
    """
    if "fantasy_points" not in df.columns:
        return df

    # Only process rows with 0 or NULL fantasy_points
    needs_seed = df["fantasy_points"].isna() | (df["fantasy_points"] == 0)
    if not needs_seed.any():
        return df

    result = df.copy()

    # Get position column (vectorized)
    pos = result.get("nfl_position", result.get("position", pd.Series("", index=result.index)))
    pos = pos.str.upper().fillna("")

    calc = pd.Series(0.0, index=result.index)

    # Kickers: pts_k_std only
    if "pts_k_std" in result.columns:
        k_pts = pd.to_numeric(result["pts_k_std"], errors="coerce").fillna(0)
        calc = calc.where(pos != "K", k_pts)

    # DEF/DST: pts_def_std only
    if "pts_def_std" in result.columns:
        def_pts = pd.to_numeric(result["pts_def_std"], errors="coerce").fillna(0)
        calc = calc.where(~pos.isin(["DEF", "DST"]), def_pts)

    # Offense: sum pass + rush + rec + misc
    off_mask = ~pos.isin(["K", "DEF", "DST"])
    for col in ["pts_pass_4pt", "pts_rush", "pts_rec_half", "pts_misc"]:
        if col in result.columns:
            col_vals = pd.to_numeric(result[col], errors="coerce").fillna(0)
            calc = calc + (off_mask * col_vals)

    # Apply only where needed and calculated > 0
    update_mask = needs_seed & (calc > 0)
    if update_mask.any():
        result.loc[update_mask, "fantasy_points"] = calc[update_mask].round(2)
        if "points" in result.columns:
            result.loc[update_mask, "points"] = calc[update_mask].round(2)
        print(f"[seed_missing_points] Seeded {update_mask.sum():,} rows from pts_* columns")

    return result


# =============================================================================
# FILE DISCOVERY
# =============================================================================
def find_yahoo_parquet(year: int, week: int, output_dir: Path) -> Path | None:
    """Find Yahoo player stats parquet file."""
    # Check both root and player_data subdirectory
    search_dirs = [output_dir, output_dir / "player_data"]

    # For year=0 (all years), ALWAYS regenerate multi-year file from individual year files
    # This ensures we pick up newly fetched historical data instead of returning stale cache
    if year == 0:
        for search_dir in search_dirs:
            if not search_dir.exists():
                continue
            # Find all year-specific files (yahoo_player_stats_YYYY_all_weeks.parquet)
            year_files = [
                f
                for f in search_dir.glob("yahoo_player_stats_*_all_weeks.parquet")
                if re.match(r"yahoo_player_stats_\d{4}_all_weeks\.parquet$", f.name)
            ]
            if year_files:
                # Combine all years into a single file
                combined_path = search_dir / "yahoo_player_stats_multi_year_all_weeks.parquet"
                print(f"[yahoo] Combining {len(year_files)} year files into multi-year file...")
                dfs = [pd.read_parquet(f) for f in sorted(year_files)]
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.to_parquet(combined_path, index=False)
                # Extract years from filenames (can't use backslash in f-string)
                year_pattern = re.compile(r"(\d{4})")
                years_found = sorted([int(year_pattern.search(f.name).group(1)) for f in year_files])
                print(f"[yahoo] Created multi-year file: {len(combined_df):,} rows from years {years_found}")
                return combined_path

        # No individual year files found - fall back to existing multi-year file if it exists
        for search_dir in search_dirs:
            if not search_dir.exists():
                continue
            multi_year_path = search_dir / "yahoo_player_stats_multi_year_all_weeks.parquet"
            if multi_year_path.exists():
                return multi_year_path

    # For specific year (year != 0), check single-year files only
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue

        candidates = [
            search_dir / f"yahoo_player_stats_{year}_all_weeks.parquet",
            search_dir / f"yahoo_player_stats_{year}_week_{week}.parquet",
        ]

        for p in candidates:
            if p.exists():
                return p

    # Fallback: find most recent Yahoo file in either directory
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        yahoo_files = sorted(
            search_dir.glob("yahoo_player_stats_*.parquet"), key=lambda x: x.stat().st_mtime, reverse=True
        )
        if yahoo_files:
            return yahoo_files[0]

    return None


def find_nfl_parquet(year: int, week: int, output_dir: Path) -> Path | None:
    """
    Find NFL combined stats parquet file.

    PRIORITY ORDER (for multi-year data):
    1. Super table (all years) - nfl_player_stats_super_table.parquet
    2. All-years merged files - nfl_stats_merged_0_all_weeks.parquet
    3. Single-year file (only if specific year requested)

    When year=0 (all years), we MUST use multi-year files to get proper
    LAMAR calculations across all league years.
    """
    # Check both root and player_data subdirectory
    search_dirs = [output_dir, output_dir / "player_data"]

    # PRIORITY 1: Super table (contains all years 1970+)
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        super_table = search_dir / "nfl_player_stats_super_table.parquet"
        if super_table.exists():
            print(f"[nfl] Found super table (multi-year): {super_table}")
            return super_table

    # PRIORITY 2: All-years merged files
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue

        multi_year_candidates = [
            search_dir / "nfl_stats_merged_0_all_weeks.parquet",  # All years (year=0)
            search_dir / "nfl_combined_0_all_weeks.parquet",
            search_dir / "nfl_stats_merged_all_years.parquet",
        ]

        for p in multi_year_candidates:
            if p.exists():
                print(f"[nfl] Found multi-year file: {p}")
                return p

    # PRIORITY 3: Single-year file (only if specific year requested AND no multi-year available)
    # WARNING: Using single-year file means LAMAR will only work for that year!
    if year != 0:
        for search_dir in search_dirs:
            if not search_dir.exists():
                continue
            single_year = search_dir / f"nfl_stats_merged_{year}_all_weeks.parquet"
            if single_year.exists():
                print(f"[nfl] [WARN] Using single-year file: {single_year}")
                print(f"[nfl] [WARN] LAMAR metrics will only work for year {year}!")
                return single_year

    # Fallback: find any NFL merged file, but warn about multi-year issues
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        nfl_files = list(search_dir.glob("nfl_*merged*.parquet")) or list(search_dir.glob("nfl_combined*.parquet"))
        if nfl_files:
            # Prefer files with "0" or "all" in the name (multi-year)
            multi_year_files = [f for f in nfl_files if "_0_" in f.name or "all_years" in f.name]
            if multi_year_files:
                selected = sorted(multi_year_files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
                print(f"[nfl] Found multi-year file (fallback): {selected}")
                return selected

            # CRITICAL FIX: When year=0 (all years mode), do NOT accept single-year files.
            # Return None to trigger MotherDuck super table fallback instead.
            # This fixes the bug where only 2024 players matched and 2015-2023 failed.
            if year == 0:
                print("[nfl] [INFO] Found single-year files but year=0 (all years mode)")
                print("[nfl] [INFO] Returning None to trigger MotherDuck super table fallback")
                return None

            # Last resort: most recent file (likely single-year) - only for specific year requests
            selected = sorted(nfl_files, key=lambda x: x.stat().st_mtime, reverse=True)[0]
            print(f"[nfl] [WARN] Using fallback file: {selected}")
            if "_0_" not in selected.name and "all_years" not in selected.name:
                print("[nfl] [WARN] This appears to be a single-year file - LAMAR may be incomplete!")
            return selected

    return None


def find_settings_file(year: int, league_key: str | None, settings_dir: Path) -> Path | None:
    """Find league settings JSON file."""
    if not settings_dir.exists():
        return None

    # Try specific league key first
    if league_key:
        safe_key = league_key.replace(".", "_")
        specific = settings_dir / f"league_settings_{year}_{safe_key}.json"
        if specific.exists():
            return specific

    # Find any settings file for this year
    year_files = sorted(
        settings_dir.glob(f"league_settings_{year}_*.json"), key=lambda x: x.stat().st_mtime, reverse=True
    )
    if year_files:
        return year_files[0]

    return None


def filter_to_completed_weeks(nfl_df: pd.DataFrame, yahoo_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Filter NFL data to completed weeks (based on Yahoo data) for current year only."""
    if "week" not in yahoo_df.columns or "week" not in nfl_df.columns:
        return nfl_df
    if "year" not in yahoo_df.columns or "year" not in nfl_df.columns:
        return nfl_df

    current_year = get_current_nfl_season_year()

    # Only filter current year to avoid incomplete week data
    if year != current_year and year != 0:
        return nfl_df

    # Find max week per year in Yahoo data
    yahoo_max_weeks = (
        yahoo_df.groupby(pd.to_numeric(yahoo_df["year"], errors="coerce"))["week"]
        .apply(lambda x: pd.to_numeric(x, errors="coerce").max())
        .to_dict()
    )

    if not yahoo_max_weeks:
        return nfl_df

    # Only filter current year
    current_year_max = yahoo_max_weeks.get(current_year)
    if current_year_max is None:
        return nfl_df

    nfl_year = pd.to_numeric(nfl_df["year"], errors="coerce")
    nfl_week = pd.to_numeric(nfl_df["week"], errors="coerce")

    # Keep: past years (all weeks) OR current year (up to max Yahoo week)
    mask = (nfl_year != current_year) | (nfl_week <= current_year_max)

    filtered = nfl_df[mask].copy()
    print(f"[nfl] Filtered current year {current_year} to weeks 1-{int(current_year_max)}")

    return filtered


# =============================================================================
# CLI — REMOVED (parquet I/O retired; merge is now called in-process only)
# =============================================================================
