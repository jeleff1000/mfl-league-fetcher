#!/usr/bin/env python3
"""
NFL Franchise Numbers - Stable identifiers for franchise lineages.

This module provides nfl_franchise_number mappings that handle:
- Team relocations (OAK → LV, STL → LAR, SD → LAC)
- Team renames (Washington's various names)
- Historical abbreviation conflicts (BAL = Colts pre-1984, Ravens 1996+)

Usage:
    from nfl_franchises import get_nfl_franchise_number

    nfl_franchise_number = get_nfl_franchise_number("OAK", 1985)  # 31 (Raiders)
    nfl_franchise_number = get_nfl_franchise_number("LV", 2023)   # 31 (Raiders)

`get_franchise_id` remains as a compatibility alias for older pipeline modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# =============================================================================
# Franchise Definitions
# =============================================================================
# Each franchise has a unique ID that persists through relocations/renames

FRANCHISES = {
    # NFC East
    1: {"name": "Dallas Cowboys", "current_abbrev": "DAL", "founded": 1960},
    2: {"name": "New York Giants", "current_abbrev": "NYG", "founded": 1925},
    3: {"name": "Philadelphia Eagles", "current_abbrev": "PHI", "founded": 1933},
    4: {"name": "Washington Commanders", "current_abbrev": "WAS", "founded": 1932},
    # NFC North
    5: {"name": "Chicago Bears", "current_abbrev": "CHI", "founded": 1920},
    6: {"name": "Detroit Lions", "current_abbrev": "DET", "founded": 1930},
    7: {"name": "Green Bay Packers", "current_abbrev": "GB", "founded": 1921},
    8: {"name": "Minnesota Vikings", "current_abbrev": "MIN", "founded": 1961},
    # NFC South
    9: {"name": "Atlanta Falcons", "current_abbrev": "ATL", "founded": 1966},
    10: {"name": "Carolina Panthers", "current_abbrev": "CAR", "founded": 1995},
    11: {"name": "New Orleans Saints", "current_abbrev": "NO", "founded": 1967},
    12: {"name": "Tampa Bay Buccaneers", "current_abbrev": "TB", "founded": 1976},
    # NFC West
    13: {"name": "Arizona Cardinals", "current_abbrev": "ARI", "founded": 1920},  # CHI → STL → PHO → ARI
    14: {"name": "Los Angeles Rams", "current_abbrev": "LAR", "founded": 1936},  # CLE → LA → STL → LA
    15: {"name": "San Francisco 49ers", "current_abbrev": "SF", "founded": 1946},
    16: {"name": "Seattle Seahawks", "current_abbrev": "SEA", "founded": 1976},
    # AFC East
    17: {"name": "Buffalo Bills", "current_abbrev": "BUF", "founded": 1960},
    18: {"name": "Miami Dolphins", "current_abbrev": "MIA", "founded": 1966},
    19: {"name": "New England Patriots", "current_abbrev": "NE", "founded": 1960},  # BOS → NE
    20: {"name": "New York Jets", "current_abbrev": "NYJ", "founded": 1960},
    # AFC North
    21: {"name": "Baltimore Ravens", "current_abbrev": "BAL", "founded": 1996},  # Expansion (not Colts)
    22: {"name": "Cincinnati Bengals", "current_abbrev": "CIN", "founded": 1968},
    23: {"name": "Cleveland Browns", "current_abbrev": "CLE", "founded": 1946},  # Suspended 1996-98
    24: {"name": "Pittsburgh Steelers", "current_abbrev": "PIT", "founded": 1933},
    # AFC South
    25: {"name": "Houston Texans", "current_abbrev": "HOU", "founded": 2002},  # Expansion (not Oilers)
    26: {"name": "Indianapolis Colts", "current_abbrev": "IND", "founded": 1953},  # BAL → IND
    27: {"name": "Jacksonville Jaguars", "current_abbrev": "JAX", "founded": 1995},
    28: {"name": "Tennessee Titans", "current_abbrev": "TEN", "founded": 1960},  # HOU Oilers → TEN
    # AFC West
    29: {"name": "Denver Broncos", "current_abbrev": "DEN", "founded": 1960},
    30: {"name": "Kansas City Chiefs", "current_abbrev": "KC", "founded": 1960},  # DAL Texans → KC
    31: {"name": "Las Vegas Raiders", "current_abbrev": "LV", "founded": 1960},  # OAK → LA → OAK → LV
    32: {"name": "Los Angeles Chargers", "current_abbrev": "LAC", "founded": 1960},  # LA → SD → LA
}

# =============================================================================
# Franchise Nicknames - For display purposes (e.g., "Ravens DST")
# =============================================================================

FRANCHISE_NICKNAMES = {
    # NFC East
    1: "Cowboys",  # Dallas Cowboys
    2: "Giants",  # New York Giants
    3: "Eagles",  # Philadelphia Eagles
    4: "Commanders",  # Washington Commanders
    # NFC North
    5: "Bears",  # Chicago Bears
    6: "Lions",  # Detroit Lions
    7: "Packers",  # Green Bay Packers
    8: "Vikings",  # Minnesota Vikings
    # NFC South
    9: "Falcons",  # Atlanta Falcons
    10: "Panthers",  # Carolina Panthers
    11: "Saints",  # New Orleans Saints
    12: "Buccaneers",  # Tampa Bay Buccaneers
    # NFC West
    13: "Cardinals",  # Arizona Cardinals (CHI→STL→PHO→ARI)
    14: "Rams",  # Los Angeles Rams (CLE→LA→STL→LAR)
    15: "49ers",  # San Francisco 49ers
    16: "Seahawks",  # Seattle Seahawks
    # AFC East
    17: "Bills",  # Buffalo Bills
    18: "Dolphins",  # Miami Dolphins
    19: "Patriots",  # New England Patriots (BOS→NE)
    20: "Jets",  # New York Jets
    # AFC North
    21: "Ravens",  # Baltimore Ravens (expansion 1996)
    22: "Bengals",  # Cincinnati Bengals
    23: "Browns",  # Cleveland Browns
    24: "Steelers",  # Pittsburgh Steelers
    # AFC South
    25: "Texans",  # Houston Texans (expansion 2002)
    26: "Colts",  # Indianapolis Colts (BAL→IND)
    27: "Jaguars",  # Jacksonville Jaguars
    28: "Titans",  # Tennessee Titans (HOU Oilers→TEN)
    # AFC West
    29: "Broncos",  # Denver Broncos
    30: "Chiefs",  # Kansas City Chiefs (DAL Texans→KC)
    31: "Raiders",  # Las Vegas Raiders (OAK→LA→OAK→LV)
    32: "Chargers",  # Los Angeles Chargers (LA→SD→LAC)
}

# =============================================================================
# Franchise Nickname History - Year-aware nicknames for teams that changed names
# =============================================================================
# Format: franchise_id -> [(nickname, start_year, end_year_or_None), ...]
# Only teams that changed nicknames need entries here.
# Teams NOT listed use the current nickname from FRANCHISE_NICKNAMES for all years.

FRANCHISE_NICKNAME_HISTORY = {
    # Washington (franchise 4): Braves -> Redskins -> Football Team -> Commanders
    4: [("Braves", 1932, 1932), ("Redskins", 1933, 2019), ("Football Team", 2020, 2021), ("Commanders", 2022, None)],
    # Chicago Bears (franchise 5): Staleys -> Bears
    5: [("Staleys", 1920, 1921), ("Bears", 1922, None)],
    # Detroit Lions (franchise 6): Spartans -> Lions
    6: [("Spartans", 1930, 1933), ("Lions", 1934, None)],
    # New York Jets (franchise 20): Titans -> Jets
    20: [("Titans", 1960, 1962), ("Jets", 1963, None)],
    # Tennessee Titans (franchise 28): Oilers -> Titans
    28: [("Oilers", 1960, 1998), ("Titans", 1999, None)],
    # Kansas City Chiefs (franchise 30): Texans -> Chiefs
    30: [("Texans", 1960, 1962), ("Chiefs", 1963, None)],
}

# =============================================================================
# Defunct Franchise Names - For historical teams no longer in NFL
# =============================================================================
# Maps abbreviation -> full nickname for display as "Nickname DST"

DEFUNCT_FRANCHISE_NAMES = {
    # Major defunct franchises (1920s-1950s) - NO lineage to current teams
    # NOTE: NYT (Titans->Jets) and PRT (Spartans->Lions) have lineage, not defunct
    "AKR": "Akron Pros",  # 1920-1926 (became Indians)
    "BCL": "Brooklyn Lions",  # 1926 (1 season)
    "BDA": "Boston/NY Bulldogs",  # 1929, 1944-1949
    "BKN": "Brooklyn Dodgers",  # 1930-1943 (merged with Yanks)
    "BRL": "Brooklyn Tigers",  # 1944 (merger year)
    "CAN": "Canton Bulldogs",  # 1920-1923, 1925-1926
    "CHH": "Chicago Hornets",  # AAFC 1949 (not NFL)
    "CHR": "Cincinnati Reds",  # 1933-1934
    "CHT": "Chicago Tigers",  # 1920 (1 season)
    "CLI": "Cleveland Indians",  # 1931 (1 season)
    "COL": "Columbus Panhandles",  # 1920-1926
    "DAY": "Dayton Triangles",  # 1920-1929
    "DTX": "Dallas Texans",  # 1952 (1 season, not AFL Texans)
    "DUL": "Duluth Eskimos",  # 1926-1927
    "EVN": "Evansville Crimson Giants",  # 1921-1922
    "FRN": "Frankford Yellow Jackets",  # 1924-1931
    "HAM": "Hammond Pros",  # 1920-1926
    "HRT": "Hartford Blues",  # 1926 (1 season)
    "KEN": "Kenosha Maroons",  # 1924 (1 season)
    "LAB": "Los Angeles Buccaneers",  # 1926 (1 season)
    "LAD": "Los Angeles Dons",  # AAFC 1946-1949
    "LOU": "Louisville Brecks",  # 1921-1923
    "MIL": "Milwaukee Badgers",  # 1922-1926
    "MUN": "Muncie Flyers",  # 1920-1921
    "NYB": "New York Bulldogs",  # 1949 (1 season)
    "NYY": "New York Yanks",  # 1950-1951
    "OOR": "Oorang Indians",  # 1922-1923
    "POT": "Pottsville Maroons",  # 1925-1928
    "PRV": "Providence Steam Roller",  # 1925-1931
    "RAC": "Racine Legion",  # 1922-1924
    "RCH": "Rochester Jeffersons",  # 1920-1925
    "RII": "Rock Island Independents",  # 1920-1926
    "SIS": "Staten Island Stapletons",  # 1929-1932
    "TOL": "Toledo Maroons",  # 1922-1923
    "TON": "Tonawanda Kardex",  # 1921 (1 game)
    "TOR": "Orange Tornadoes",  # 1929-1930 (Orange/Newark Tornadoes)
}

# List of all defunct team abbreviations (for filter expansion)
# These are teams in the super table with no lineage to current franchises
DEFUNCT_TEAM_ABBREVS = list(DEFUNCT_FRANCHISE_NAMES.keys())

# =============================================================================
# Franchise History - Abbreviation mappings by year range
# =============================================================================
# Format: (franchise_id, start_year, end_year or None for ongoing)

FRANCHISE_HISTORY: list[tuple[int, str, int, int | None]] = [
    # Arizona Cardinals relocations
    # NOTE: Super table normalizes to ARI for all Cardinals history
    (13, "ARI", 1920, None),  # Arizona Cardinals (super table normalized)
    (13, "CRD", 1920, 1959),  # Chicago Cardinals (Stathead abbreviation)
    (13, "SLC", 1960, 1987),  # St. Louis Cardinals
    (13, "STL", 1960, 1987),  # St. Louis Cardinals (alternate)
    (13, "PHO", 1988, 1993),  # Phoenix Cardinals
    # Los Angeles Rams relocations
    (14, "CLE", 1936, 1945),  # Cleveland Rams (conflicts with Browns)
    (14, "LA", 1946, None),  # Los Angeles Rams (NFLverse uses LA for all Rams years)
    (14, "LAR", 1946, None),  # Los Angeles Rams (super table normalized)
    (14, "RAM", 1946, 1994),  # Rams abbreviation
    (14, "STL", 1995, 2015),  # St. Louis Rams
    # Las Vegas Raiders relocations
    # Note: Super table normalizes to LV for Raiders history
    (31, "OAK", 1960, 2019),  # Oakland Raiders
    (31, "RAI", 1982, 1994),  # Raiders abbreviation (LA era)
    (31, "LV", 1960, None),  # Las Vegas Raiders (super table normalized)
    (31, "LVR", 2020, None),  # Las Vegas Raiders (PFR/FantasyPros-style alternate)
    # Los Angeles Chargers relocations
    # Note: Super table normalizes to LAC for all Chargers history
    (32, "LAC", 1960, None),  # Los Angeles Chargers (super table normalized)
    (32, "LA", 1960, 1960),  # Los Angeles Chargers (1 year)
    (32, "SD", 1961, 2016),  # San Diego Chargers
    (32, "SDG", 1961, 2016),  # San Diego Chargers (PFR)
    # Indianapolis Colts relocation
    (26, "BAL", 1953, 1983),  # Baltimore Colts
    (26, "CLT", 1953, 1983),  # Colts abbreviation
    (26, "IND", 1984, None),  # Indianapolis Colts
    # Tennessee Titans relocations/renames
    (28, "HOU", 1960, 1998),  # Houston Oilers/Tennessee Oilers source alias
    (28, "OTI", 1960, 1998),  # Oilers/Titans abbreviation
    (28, "HST", 1960, 1996),  # Houston abbreviation
    (28, "TEN", 1997, None),  # Tennessee (Oilers 97-98, Titans 99+)
    # New England Patriots rename
    (19, "BOS", 1960, 1970),  # Boston Patriots
    (19, "NE", 1971, None),  # New England Patriots
    (19, "NWE", 1971, None),  # New England (PFR)
    # Kansas City Chiefs relocation
    # PFR/source data uses DTX for the AFL Dallas Texans. Keep DAL reserved for
    # the Dallas Cowboys, which also began play in 1960.
    (30, "DTX", 1960, 1962),  # Dallas Texans (AFL, era-appropriate)
    (30, "KC", 1963, None),  # Kansas City Chiefs
    (30, "KAN", 1963, None),  # Kansas City (PFR)
    # Washington name changes (same location)
    (4, "BOS", 1932, 1936),  # Boston Braves/Redskins
    (4, "WAS", 1937, None),  # Washington (various names)
    # Teams with stable abbreviations (no relocations in NFL era)
    (1, "DAL", 1960, None),  # Dallas Cowboys
    (2, "NYG", 1925, None),  # New York Giants
    (3, "PHI", 1933, None),  # Philadelphia Eagles
    (5, "CHI", 1920, None),  # Chicago Bears (Staleys 1920-21, Bears 1922+)
    (6, "PRT", 1930, 1933),  # Portsmouth Spartans (became Lions)
    (6, "DET", 1934, None),  # Detroit Lions
    (7, "GB", 1921, None),  # Green Bay Packers
    (7, "GNB", 1921, None),  # Green Bay (PFR)
    (8, "MIN", 1961, None),  # Minnesota Vikings
    (9, "ATL", 1966, None),  # Atlanta Falcons
    (10, "CAR", 1995, None),  # Carolina Panthers
    (11, "NO", 1967, None),  # New Orleans Saints
    (11, "NOR", 1967, None),  # New Orleans (PFR)
    (12, "TB", 1976, None),  # Tampa Bay Buccaneers
    (12, "TAM", 1976, None),  # Tampa Bay (PFR)
    (15, "SF", 1946, None),  # San Francisco 49ers
    (15, "SFO", 1946, None),  # San Francisco (PFR)
    (16, "SEA", 1976, None),  # Seattle Seahawks
    (17, "BUF", 1960, None),  # Buffalo Bills
    (18, "MIA", 1966, None),  # Miami Dolphins
    (20, "NYT", 1960, 1962),  # New York Titans (AFL, became Jets)
    (20, "NYJ", 1963, None),  # New York Jets
    (21, "BAL", 1996, None),  # Baltimore Ravens
    (22, "CIN", 1968, None),  # Cincinnati Bengals
    (23, "CLE", 1946, 1995),  # Cleveland Browns (original)
    (23, "CLE", 1999, None),  # Cleveland Browns (expansion)
    (24, "PIT", 1933, None),  # Pittsburgh Steelers
    (25, "HOU", 2002, None),  # Houston Texans
    (27, "JAX", 1995, None),  # Jacksonville Jaguars
    (27, "JAC", 1995, None),  # Jacksonville (alternate)
    (29, "DEN", 1960, None),  # Denver Broncos
]

# =============================================================================
# Build lookup dictionary
# =============================================================================


def _build_abbrev_lookup() -> dict[str, list[tuple[int, int, int | None]]]:
    """Build abbreviation -> [(franchise_id, start_year, end_year), ...] lookup."""
    lookup = {}
    for franchise_id, abbrev, start_year, end_year in FRANCHISE_HISTORY:
        abbrev_upper = abbrev.upper()
        if abbrev_upper not in lookup:
            lookup[abbrev_upper] = []
        lookup[abbrev_upper].append((franchise_id, start_year, end_year))
    return lookup


ABBREV_LOOKUP = _build_abbrev_lookup()

# Current abbreviation to franchise (for simple lookups)
CURRENT_ABBREV_TO_FRANCHISE = {v["current_abbrev"]: k for k, v in FRANCHISES.items()}

# =============================================================================
# Public API
# =============================================================================


def get_franchise_id(abbrev: str, year: int) -> int | None:
    """
    Get NFL franchise lineage number for a team abbreviation in a given year.

    Compatibility name for older code. Prefer `get_nfl_franchise_number` in new
    code so these NFL lineage numbers are not confused with fantasy league
    `franchise_id` values.

    Handles historical relocations:
        get_franchise_id('OAK', 1985) -> 31 (Raiders)
        get_franchise_id('LV', 2023)  -> 31 (Raiders)
        get_franchise_id('BAL', 1980) -> 26 (Colts)
        get_franchise_id('BAL', 2000) -> 21 (Ravens)

    Args:
        abbrev: Team abbreviation (case-insensitive)
        year: Season year

    Returns:
        NFL franchise number (1-32) or None if not found
    """
    abbrev_upper = abbrev.upper() if abbrev else None
    if not abbrev_upper or abbrev_upper not in ABBREV_LOOKUP:
        # Try current abbreviation as fallback
        return CURRENT_ABBREV_TO_FRANCHISE.get(abbrev_upper)

    candidates = ABBREV_LOOKUP[abbrev_upper]

    for franchise_id, start_year, end_year in candidates:
        if start_year <= year and (end_year is None or year <= end_year):
            return franchise_id

    # Fallback: return most recent match
    if candidates:
        return candidates[-1][0]

    return None


def get_nfl_franchise_number(abbrev: str, year: int) -> int | None:
    """Return the stable numeric NFL franchise lineage code.

    This is the preferred public name for new code. `get_franchise_id` remains
    as a compatibility alias for older pipeline modules.
    """
    return get_franchise_id(abbrev, year)


def get_franchise_info(nfl_franchise_number: int) -> dict | None:
    """Get franchise information by NFL franchise number."""
    return FRANCHISES.get(nfl_franchise_number)


def get_current_abbrev(nfl_franchise_number: int) -> str | None:
    """Get current abbreviation for an NFL franchise number."""
    info = FRANCHISES.get(nfl_franchise_number)
    return info["current_abbrev"] if info else None


def add_nfl_franchise_number_column(
    df: pd.DataFrame,
    abbrev_col: str = "nfl_team",
    year_col: str = "year",
    output_col: str = "nfl_franchise_number",
) -> pd.DataFrame:
    """
    Add an NFL franchise lineage-number column to a DataFrame.

    Args:
        df: DataFrame with team abbreviations and years
        abbrev_col: Name of abbreviation column
        year_col: Name of year column
        output_col: Name of output column

    Returns:
        DataFrame with `output_col` added
    """
    import pandas as pd

    df = df.copy()
    df[output_col] = df.apply(
        lambda row: get_nfl_franchise_number(row[abbrev_col], row[year_col])
        if pd.notna(row[abbrev_col]) and pd.notna(row[year_col])
        else None,
        axis=1,
    )
    return df


def add_franchise_id_column(df: pd.DataFrame, abbrev_col: str = "nfl_team", year_col: str = "year") -> pd.DataFrame:
    """
    Add a legacy `franchise_id` column to a DataFrame.

    Compatibility wrapper for older code. Prefer `add_nfl_franchise_number_column`
    in new code.

    Args:
        df: DataFrame with team abbreviations and years
        abbrev_col: Name of abbreviation column
        year_col: Name of year column

    Returns:
        DataFrame with `franchise_id` column added
    """
    return add_nfl_franchise_number_column(
        df,
        abbrev_col=abbrev_col,
        year_col=year_col,
        output_col="franchise_id",
    )


def get_franchise_nickname(franchise_id: int, year: int = None) -> str | None:
    """
    Get the nickname for a franchise by ID, optionally era-accurate.

    When year is provided and the franchise has entries in FRANCHISE_NICKNAME_HISTORY,
    returns the historically accurate nickname for that year.
    When year is omitted, returns the current nickname (backward compatible).

    Args:
        franchise_id: Franchise ID (1-32)
        year: Optional season year for era-accurate nicknames

    Returns:
        Nickname string (e.g., "Ravens", "Redskins", "Oilers") or None if not found
    """
    if year is not None and franchise_id in FRANCHISE_NICKNAME_HISTORY:
        entries = FRANCHISE_NICKNAME_HISTORY[franchise_id]
        for nickname, start_year, end_year in entries:
            if start_year <= year and (end_year is None or year <= end_year):
                return nickname
        # Year is outside all ranges — use earliest known nickname to avoid anachronisms
        # (e.g., franchise 4 in 1921 → "Braves" not "Commanders")
        if entries:
            return entries[0][0]
    return FRANCHISE_NICKNAMES.get(franchise_id)


def get_nickname_from_abbrev(abbrev: str, year: int) -> str | None:
    """
    Get franchise nickname from team abbreviation and year.

    Args:
        abbrev: Team abbreviation (e.g., "BAL", "STL", "OAK")
        year: Season year

    Returns:
        Nickname string (e.g., "Ravens", "Cardinals") or None if not found
    """
    franchise_id = get_franchise_id(abbrev, year)
    if franchise_id:
        return get_franchise_nickname(franchise_id, year)
    return None


def get_def_player_id(abbrev: str, year: int) -> str:
    """
    Get franchise-based DEF NFL_player_id.

    This ensures all historical team abbreviations map to a single franchise ID:
    - CHI/STL/PHO/ARI Cardinals → DEF-13
    - CLE/LA/STL/LAR Rams → DEF-14
    - OAK/LV Raiders → DEF-31

    Args:
        abbrev: Team abbreviation
        year: Season year

    Returns:
        DEF player ID in format "DEF-{franchise_id}" (e.g., "DEF-13")
    """
    franchise_id = get_franchise_id(abbrev, year)
    if franchise_id:
        return f"DEF-{franchise_id}"
    # Fallback for unknown teams
    return f"DEF-{abbrev.upper() if abbrev else 'UNK'}"


def generate_def_franchise_case_sql(team_col: str = "nfl_team", year_col: str = "year") -> str:
    """
    Generate a SQL CASE expression that maps (nfl_team, year) to DEF-{franchise_id}.

    Uses FRANCHISE_HISTORY to produce year-aware mappings for abbreviations shared
    by multiple franchises (BAL, HOU, CLE, DAL, BOS, STL, LA), and simple mappings
    for unambiguous abbreviations.

    Example output fragment:
        CASE
            WHEN nfl_team = 'BAL' AND year <= 1983 THEN 'DEF-26'
            WHEN nfl_team = 'BAL' AND year >= 1996 THEN 'DEF-21'
            WHEN nfl_team = 'CHI' THEN 'DEF-5'
            ...
            ELSE 'DEF-' || nfl_team
        END

    Args:
        team_col: SQL column name for team abbreviation (default: "nfl_team")
        year_col: SQL column name for year (default: "year")

    Returns:
        SQL CASE expression string (without trailing alias)
    """
    from collections import defaultdict

    # Step 1: Group FRANCHISE_HISTORY entries by abbreviation
    abbrev_entries: dict = defaultdict(list)
    for franchise_id, abbrev, start_year, end_year in FRANCHISE_HISTORY:
        abbrev_entries[abbrev.upper()].append((franchise_id, start_year, end_year))

    # Step 2: Find abbreviations used by multiple distinct franchises
    abbrev_franchise_ids: dict = defaultdict(set)
    for abbrev, entries in abbrev_entries.items():
        for franchise_id, _, _ in entries:
            abbrev_franchise_ids[abbrev].add(franchise_id)
    conflicting = {abbrev for abbrev, fids in abbrev_franchise_ids.items() if len(fids) > 1}

    # Step 3: Build WHEN clauses
    when_clauses = []
    handled_simple: set = set()  # Track non-conflicting abbrevs we've already emitted

    for abbrev, entries in sorted(abbrev_entries.items()):
        if abbrev in conflicting:
            # Conflicting abbreviation: emit year-range WHEN for each franchise
            for franchise_id, start_year, end_year in entries:
                if end_year is None:
                    when_clauses.append(
                        f"WHEN {team_col} = '{abbrev}' AND {year_col} >= {start_year} " f"THEN 'DEF-{franchise_id}'"
                    )
                else:
                    when_clauses.append(
                        f"WHEN {team_col} = '{abbrev}' AND {year_col} BETWEEN {start_year} AND {end_year} "
                        f"THEN 'DEF-{franchise_id}'"
                    )
        else:
            # Non-conflicting: one WHEN per abbreviation (deduplicated)
            if abbrev not in handled_simple:
                franchise_id = entries[0][0]  # All entries share the same franchise
                when_clauses.append(f"WHEN {team_col} = '{abbrev}' THEN 'DEF-{franchise_id}'")
                handled_simple.add(abbrev)

    # Step 4: Assemble CASE expression with ELSE fallback
    clauses_sql = "\n                ".join(when_clauses)
    return f"""CASE
                {clauses_sql}
                ELSE 'DEF-' || {team_col}
            END"""


def get_def_display_name(abbrev: str, year: int) -> str:
    """
    Get 'Nickname DST' display name for defense.

    Args:
        abbrev: Team abbreviation
        year: Season year

    Returns:
        Display name in format "{Nickname} DST" (e.g., "Ravens DST", "Cardinals DST")
    """
    nickname = get_nickname_from_abbrev(abbrev, year)
    if nickname:
        return f"{nickname} DST"

    # Check defunct franchise names
    abbrev_upper = abbrev.upper() if abbrev else None
    if abbrev_upper and abbrev_upper in DEFUNCT_FRANCHISE_NAMES:
        return f"{DEFUNCT_FRANCHISE_NAMES[abbrev_upper]} DST"

    # Fallback for unknown teams
    return f"{abbrev_upper if abbrev_upper else 'UNK'} DST"


def get_all_historical_abbrevs(current_abbrev: str) -> list[str]:
    """
    Get all historical abbreviations for a franchise (simple list, no year info).

    NOTE: For filtering, use get_franchise_sql_filter() instead which handles
    year-based disambiguation for conflicting abbreviations like BAL/HOU.

    Args:
        current_abbrev: Current franchise abbreviation (e.g., "IND", "TEN", "BAL")

    Returns:
        List of all abbreviations that map to this franchise
    """
    # Get franchise ID from current abbreviation
    franchise_id = CURRENT_ABBREV_TO_FRANCHISE.get(current_abbrev.upper())
    if not franchise_id:
        return [current_abbrev.upper()]

    # Collect all abbreviations that map to this franchise
    abbrevs = set()
    for fid, abbrev, _start_year, _end_year in FRANCHISE_HISTORY:
        if fid == franchise_id:
            abbrevs.add(abbrev.upper())

    return list(abbrevs) if abbrevs else [current_abbrev.upper()]


def get_franchise_sql_conditions(current_abbrev: str, team_col: str = "nfl_team", year_col: str = "year") -> list[str]:
    """
    Get SQL conditions for filtering by franchise with year-based disambiguation.

    Handles conflicting abbreviations (BAL, HOU, DAL, etc.) by including year constraints.

    Example for IND (Colts):
        Returns: ["nfl_team = 'IND'", "nfl_team = 'CLT'", "(nfl_team = 'BAL' AND year BETWEEN 1953 AND 1983)"]

    Example for BAL (Ravens):
        Returns: ["(nfl_team = 'BAL' AND year >= 1996)"]

    Args:
        current_abbrev: Current franchise abbreviation (e.g., "IND", "TEN", "BAL")
        team_col: Name of the team column in SQL (default: "nfl_team")
        year_col: Name of the year column in SQL (default: "year")

    Returns:
        List of SQL conditions to OR together
    """
    # Get franchise ID from current abbreviation
    franchise_id = CURRENT_ABBREV_TO_FRANCHISE.get(current_abbrev.upper())
    if not franchise_id:
        return [f"{team_col} = '{current_abbrev.upper()}'"]

    # Build a set of abbreviations that are used by MULTIPLE franchises (conflicts)
    # This handles cases like BAL (Colts 1953-1983, Ravens 1996+) and HOU (Oilers 1960-1996, Texans 2002+)
    abbrev_to_franchises: dict[str, list[int]] = {}
    for fid, abbrev, _start_year, _end_year in FRANCHISE_HISTORY:
        abbrev_upper = abbrev.upper()
        if abbrev_upper not in abbrev_to_franchises:
            abbrev_to_franchises[abbrev_upper] = []
        if fid not in abbrev_to_franchises[abbrev_upper]:
            abbrev_to_franchises[abbrev_upper].append(fid)

    # Abbreviations with multiple franchises need year constraints
    conflicting_abbrevs = {abbrev for abbrev, fids in abbrev_to_franchises.items() if len(fids) > 1}

    # Collect SQL conditions for each abbreviation used by this franchise
    conditions = []
    for fid, abbrev, start_year, end_year in FRANCHISE_HISTORY:
        if fid == franchise_id:
            abbrev_upper = abbrev.upper()

            # Check if this abbreviation is used by multiple franchises
            is_conflicting = abbrev_upper in conflicting_abbrevs

            if is_conflicting:
                # Add year constraint for conflicting abbreviations
                if end_year is None:
                    # Ongoing - use >= start_year
                    conditions.append(f"({team_col} = '{abbrev_upper}' AND {year_col} >= {start_year})")
                else:
                    # Historical range
                    conditions.append(
                        f"({team_col} = '{abbrev_upper}' AND {year_col} BETWEEN {start_year} AND {end_year})"
                    )
            else:
                # No conflict - simple equality (but avoid duplicates)
                simple_cond = f"{team_col} = '{abbrev_upper}'"
                if simple_cond not in conditions:
                    conditions.append(simple_cond)

    # Deduplicate conditions
    return list(dict.fromkeys(conditions)) if conditions else [f"{team_col} = '{current_abbrev.upper()}'"]


def get_franchise_filter_abbrevs() -> dict[str, list[str]]:
    """
    Get a mapping of current franchise abbreviations to all their historical abbreviations.

    IMPORTANT: Excludes abbreviations that are currently used by a DIFFERENT franchise
    to avoid matching wrong teams.

    Used by UI filters to expand franchise selections to historical teams.

    Returns:
        Dict mapping current abbrev -> list of historical abbrevs (excluding conflicts)
        e.g., {"IND": ["IND", "CLT"], "TEN": ["TEN", "OTI", "HST"], ...}
    """
    # Get set of ALL current franchise abbreviations (to exclude conflicts)
    all_current_abbrevs = set(CURRENT_ABBREV_TO_FRANCHISE.keys())

    result = {}
    for franchise_id, info in FRANCHISES.items():
        current = info["current_abbrev"]
        abbrevs = set()
        for fid, abbrev, _start_year, _end_year in FRANCHISE_HISTORY:
            if fid == franchise_id:
                abbrev_upper = abbrev.upper()
                # Include if it's this franchise's current abbrev OR not another's current
                if abbrev_upper == current or abbrev_upper not in all_current_abbrevs:
                    abbrevs.add(abbrev_upper)
        result[current] = list(abbrevs) if abbrevs else [current]
    return result


# =============================================================================
# City History - Year-accurate city names for relocated franchises
# =============================================================================
# Only franchises that relocated need entries here.
# Others derive city from FRANCHISES[fid]["name"] (everything before the last word).

CITY_HISTORY: dict[int, list[tuple[str, int, int | None]]] = {
    # (city, start_year, end_year_or_None)
    4: [("Boston", 1932, 1936), ("Washington", 1937, None)],
    5: [("Decatur", 1920, 1920), ("Chicago", 1921, None)],
    6: [("Portsmouth", 1930, 1933), ("Detroit", 1934, None)],
    13: [("Chicago", 1920, 1959), ("St. Louis", 1960, 1987), ("Phoenix", 1988, 1993), ("Arizona", 1994, None)],
    14: [
        ("Cleveland", 1936, 1945),
        ("Los Angeles", 1946, 1994),
        ("St. Louis", 1995, 2015),
        ("Los Angeles", 2016, None),
    ],
    19: [("Boston", 1960, 1970), ("New England", 1971, None)],
    20: [("New York", 1960, None)],
    26: [("Baltimore", 1953, 1983), ("Indianapolis", 1984, None)],
    28: [("Houston", 1960, 1996), ("Tennessee", 1997, None)],
    30: [("Dallas", 1960, 1962), ("Kansas City", 1963, None)],
    31: [("Oakland", 1960, 1981), ("Los Angeles", 1982, 1994), ("Oakland", 1995, 2019), ("Las Vegas", 2020, None)],
    32: [("Los Angeles", 1960, 1960), ("San Diego", 1961, 2016), ("Los Angeles", 2017, None)],
}


def get_franchise_city(franchise_id: int, year: int) -> str | None:
    """
    Get era-correct city name for a franchise in a given year.

    For franchises with a CITY_HISTORY entry, returns the historically accurate city.
    For stable franchises, derives city from the franchise name (everything before
    the last word, e.g., "Dallas Cowboys" -> "Dallas").

    Args:
        franchise_id: Franchise ID (1-32)
        year: Season year

    Returns:
        City name string or None if not found
    """
    if franchise_id in CITY_HISTORY:
        for city, start, end in CITY_HISTORY[franchise_id]:
            if start <= year and (end is None or year <= end):
                return city
    info = FRANCHISES.get(franchise_id)
    if info:
        name = info["name"]
        return name.rsplit(" ", 1)[0] if " " in name else None
    return None


# =============================================================================
# NFLverse Abbreviation Display Mapping
# =============================================================================
# NFLverse uses 3-letter canonical abbreviations for some teams that differ
# from the common display abbreviations used in fantasy contexts.

NFLVERSE_TO_DISPLAY: dict[str, str] = {
    "GNB": "GB",  # Green Bay Packers
    "NWE": "NE",  # New England Patriots
    "SFO": "SF",  # San Francisco 49ers
    "SDG": "SD",  # San Diego Chargers (historical)
    "KAN": "KC",  # Kansas City Chiefs
    "NOR": "NO",  # New Orleans Saints
    "TAM": "TB",  # Tampa Bay Buccaneers
    "OTI": "TEN",  # Tennessee Oilers/Titans (historical)
    "HST": "HOU",  # Houston Oilers (historical)
    "CLT": "IND",  # Indianapolis Colts (historical)
    "CRD": "ARI",  # Arizona Cardinals (historical Stathead)
    "RAM": "LAR",  # Los Angeles Rams (historical)
    "SLC": "STL",  # St. Louis Cardinals (historical)
    "PHO": "ARI",  # Phoenix Cardinals (historical)
    "RAI": "OAK",  # Los Angeles Raiders (historical)
}


def get_display_abbrev(nflverse_abbrev: str) -> str:
    """
    Convert NFLverse canonical abbreviation to display abbreviation.

    NFLverse uses 3-letter codes (GNB, NWE, SFO, etc.) that differ from
    common display abbreviations (GB, NE, SF, etc.) used in most fantasy
    contexts. This function normalizes them.

    Args:
        nflverse_abbrev: NFLverse abbreviation (e.g., "GNB", "NWE", "DAL")

    Returns:
        Display abbreviation (e.g., "GB", "NE", "DAL")
    """
    return NFLVERSE_TO_DISPLAY.get(nflverse_abbrev, nflverse_abbrev)


def create_franchise_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create franchise dimension tables for database.

    Returns:
        (franchises_df, franchise_history_df)
    """
    import pandas as pd

    # Main franchise table
    franchises_df = pd.DataFrame(
        [
            {
                "franchise_id": fid,
                "franchise_name": info["name"],
                "current_abbrev": info["current_abbrev"],
                "founded_year": info["founded"],
            }
            for fid, info in FRANCHISES.items()
        ]
    )

    # History table
    history_df = pd.DataFrame(
        [
            {"franchise_id": franchise_id, "abbrev": abbrev, "start_year": start_year, "end_year": end_year}
            for franchise_id, abbrev, start_year, end_year in FRANCHISE_HISTORY
        ]
    )

    return franchises_df, history_df


# =============================================================================
# CLI for testing
# =============================================================================

if __name__ == "__main__":
    print("NFL Franchise Key Tests")
    print("=" * 50)

    # Test cases
    tests = [
        ("OAK", 1985, 31, "Raiders in Oakland"),
        ("LV", 2023, 31, "Raiders in Las Vegas"),
        ("LA", 1990, 14, "Rams in LA (NFLverse normalized)"),
        ("LA", 2020, 14, "Rams in LA (2016+)"),
        ("STL", 2010, 14, "Rams in St. Louis"),
        ("STL", 1985, 13, "Cardinals in St. Louis"),
        ("BAL", 1980, 26, "Colts in Baltimore"),
        ("BAL", 2000, 21, "Ravens in Baltimore"),
        ("HOU", 1990, 28, "Oilers in Houston"),
        ("HOU", 2010, 25, "Texans in Houston"),
        ("SD", 2015, 32, "Chargers in San Diego"),
        ("LAC", 2020, 32, "Chargers in LA"),
        ("JAX", 2020, 27, "Jaguars"),
        ("JAC", 2001, 27, "Jaguars (alternate abbrev)"),
    ]

    all_passed = True
    for abbrev, year, expected, desc in tests:
        result = get_franchise_id(abbrev, year)
        status = "PASS" if result == expected else "FAIL"
        if result != expected:
            all_passed = False
        print(f"  {status}: {abbrev} {year} -> {result} (expected {expected}) - {desc}")

    print(f"\n{'All tests passed!' if all_passed else 'Some tests failed!'}")

    # Show franchise tables
    print("\n" + "=" * 50)
    print("Franchise Table Preview:")
    franchises, history = create_franchise_tables()
    print(franchises.head(10).to_string())
