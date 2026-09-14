#!/usr/bin/env python3
"""
Pre-clean player and team names before merge to improve matching.

This script normalizes names in Yahoo and NFL data files to handle:
- Punctuation (T.Y. → ty, C.J. → cj)
- Suffixes (Jr, Sr, III, etc.)
- Apostrophes (Le'Veon → leveon)
- Team name variations (Los Angeles → LA)
- DEF/DST player names (New Orleans Saints → Saints DST)
"""

import pandas as pd

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

# =============================================================================
# DEF/DST Name Normalization
# =============================================================================
# Maps full team names and variations to (nickname, franchise_id)
# This handles Yahoo's format of "New Orleans Saints" -> "Saints DST"

TEAM_NAME_TO_DST: dict = {
    # NFC East
    "dallas cowboys": ("Cowboys", 1),
    "cowboys": ("Cowboys", 1),
    "dal": ("Cowboys", 1),
    "new york giants": ("Giants", 2),
    "giants": ("Giants", 2),
    "ny giants": ("Giants", 2),
    "nyg": ("Giants", 2),
    "philadelphia eagles": ("Eagles", 3),
    "eagles": ("Eagles", 3),
    "phi": ("Eagles", 3),
    "washington commanders": ("Commanders", 4),
    "washington redskins": ("Commanders", 4),
    "washington football team": ("Commanders", 4),
    "commanders": ("Commanders", 4),
    "redskins": ("Commanders", 4),
    "was": ("Commanders", 4),
    "wsh": ("Commanders", 4),
    # NFC North
    "chicago bears": ("Bears", 5),
    "bears": ("Bears", 5),
    "chi": ("Bears", 5),
    "detroit lions": ("Lions", 6),
    "lions": ("Lions", 6),
    "det": ("Lions", 6),
    "green bay packers": ("Packers", 7),
    "packers": ("Packers", 7),
    "gb": ("Packers", 7),
    "gnb": ("Packers", 7),
    "minnesota vikings": ("Vikings", 8),
    "vikings": ("Vikings", 8),
    "min": ("Vikings", 8),
    # NFC South
    "atlanta falcons": ("Falcons", 9),
    "falcons": ("Falcons", 9),
    "atl": ("Falcons", 9),
    "carolina panthers": ("Panthers", 10),
    "panthers": ("Panthers", 10),
    "car": ("Panthers", 10),
    "new orleans saints": ("Saints", 11),
    "saints": ("Saints", 11),
    "no": ("Saints", 11),
    "nor": ("Saints", 11),
    "tampa bay buccaneers": ("Buccaneers", 12),
    "buccaneers": ("Buccaneers", 12),
    "bucs": ("Buccaneers", 12),
    "tb": ("Buccaneers", 12),
    "tam": ("Buccaneers", 12),
    # NFC West
    "arizona cardinals": ("Cardinals", 13),
    "cardinals": ("Cardinals", 13),
    "ari": ("Cardinals", 13),
    "az": ("Cardinals", 13),
    "los angeles rams": ("Rams", 14),
    "la rams": ("Rams", 14),
    "rams": ("Rams", 14),
    "st. louis rams": ("Rams", 14),
    "st louis rams": ("Rams", 14),
    "lar": ("Rams", 14),
    "la": ("Rams", 14),
    "stl": ("Rams", 14),
    "san francisco 49ers": ("49ers", 15),
    "san francisco niners": ("49ers", 15),
    "49ers": ("49ers", 15),
    "niners": ("49ers", 15),
    "sf": ("49ers", 15),
    "sfo": ("49ers", 15),
    "seattle seahawks": ("Seahawks", 16),
    "seahawks": ("Seahawks", 16),
    "sea": ("Seahawks", 16),
    # AFC East
    "buffalo bills": ("Bills", 17),
    "bills": ("Bills", 17),
    "buf": ("Bills", 17),
    "miami dolphins": ("Dolphins", 18),
    "dolphins": ("Dolphins", 18),
    "mia": ("Dolphins", 18),
    "new england patriots": ("Patriots", 19),
    "patriots": ("Patriots", 19),
    "pats": ("Patriots", 19),
    "ne": ("Patriots", 19),
    "nwe": ("Patriots", 19),
    "new york jets": ("Jets", 20),
    "jets": ("Jets", 20),
    "ny jets": ("Jets", 20),
    "nyj": ("Jets", 20),
    # AFC North
    "baltimore ravens": ("Ravens", 21),
    "ravens": ("Ravens", 21),
    "bal": ("Ravens", 21),
    "rav": ("Ravens", 21),
    "cincinnati bengals": ("Bengals", 22),
    "bengals": ("Bengals", 22),
    "cin": ("Bengals", 22),
    "cleveland browns": ("Browns", 23),
    "browns": ("Browns", 23),
    "cle": ("Browns", 23),
    "pittsburgh steelers": ("Steelers", 24),
    "steelers": ("Steelers", 24),
    "pit": ("Steelers", 24),
    # AFC South
    "houston texans": ("Texans", 25),
    "texans": ("Texans", 25),
    "hou": ("Texans", 25),
    "htx": ("Texans", 25),
    "indianapolis colts": ("Colts", 26),
    "colts": ("Colts", 26),
    "ind": ("Colts", 26),
    "clt": ("Colts", 26),
    "jacksonville jaguars": ("Jaguars", 27),
    "jaguars": ("Jaguars", 27),
    "jags": ("Jaguars", 27),
    "jax": ("Jaguars", 27),
    "jac": ("Jaguars", 27),
    "tennessee titans": ("Titans", 28),
    "titans": ("Titans", 28),
    "ten": ("Titans", 28),
    "oti": ("Titans", 28),
    # AFC West
    "denver broncos": ("Broncos", 29),
    "broncos": ("Broncos", 29),
    "den": ("Broncos", 29),
    "kansas city chiefs": ("Chiefs", 30),
    "chiefs": ("Chiefs", 30),
    "kc chiefs": ("Chiefs", 30),
    "kc": ("Chiefs", 30),
    "kan": ("Chiefs", 30),
    "las vegas raiders": ("Raiders", 31),
    "oakland raiders": ("Raiders", 31),
    "raiders": ("Raiders", 31),
    "lv": ("Raiders", 31),
    "lvr": ("Raiders", 31),
    "oak": ("Raiders", 31),
    "rai": ("Raiders", 31),
    "los angeles chargers": ("Chargers", 32),
    "la chargers": ("Chargers", 32),
    "san diego chargers": ("Chargers", 32),
    "chargers": ("Chargers", 32),
    "lac": ("Chargers", 32),
    "sd": ("Chargers", 32),
    "sdg": ("Chargers", 32),
}

# Reverse mapping: abbreviation -> franchise_id (for quick lookup)
TEAM_ABBREV_TO_FRANCHISE_ID: dict = {
    # Standard 2-3 letter abbreviations
    "DAL": 1,
    "NYG": 2,
    "PHI": 3,
    "WAS": 4,
    "WSH": 4,
    "CHI": 5,
    "DET": 6,
    "GB": 7,
    "GNB": 7,
    "MIN": 8,
    "ATL": 9,
    "CAR": 10,
    "NO": 11,
    "NOR": 11,
    "TB": 12,
    "TAM": 12,
    "ARI": 13,
    "AZ": 13,
    "LA": 14,
    "LAR": 14,
    "STL": 14,
    "SF": 15,
    "SFO": 15,
    "SEA": 16,
    "BUF": 17,
    "MIA": 18,
    "NE": 19,
    "NWE": 19,
    "NYJ": 20,
    "BAL": 21,
    "RAV": 21,
    "CIN": 22,
    "CLE": 23,
    "PIT": 24,
    "HOU": 25,
    "HTX": 25,
    "IND": 26,
    "CLT": 26,
    "JAX": 27,
    "JAC": 27,
    "TEN": 28,
    "OTI": 28,
    "DEN": 29,
    "KC": 30,
    "KAN": 30,
    "LV": 31,
    "LVR": 31,
    "OAK": 31,
    "RAI": 31,
    "LAC": 32,
    "SD": 32,
    "SDG": 32,
}


def normalize_def_player_name(player_name: str) -> tuple[str | None, str | None]:
    """
    Normalize a DEF/DST player name to standard 'Nickname DST' format.

    Handles Yahoo's full team names (e.g., "New Orleans Saints") and converts
    them to the format used in the NFL super_table (e.g., "Saints DST").

    Args:
        player_name: Raw player name (e.g., "New Orleans Saints", "Saints DST", "NO")

    Returns:
        Tuple of (normalized_name, nfl_player_id) or (None, None) if not a DEF
        - normalized_name: "Saints DST" format
        - nfl_player_id: "DEF-{franchise_id}" format (e.g., "DEF-11")

    Examples:
        >>> normalize_def_player_name("New Orleans Saints")
        ('Saints DST', 'DEF-11')
        >>> normalize_def_player_name("Los Angeles Chargers")
        ('Chargers DST', 'DEF-32')
        >>> normalize_def_player_name("Patrick Mahomes")
        (None, None)
    """
    if not player_name or pd.isna(player_name):
        return None, None

    name_lower = str(player_name).lower().strip()

    # Already in DST format?
    if name_lower.endswith(" dst"):
        # Extract nickname and look it up
        nickname = name_lower[:-4].strip()
        for key, (nick, fid) in TEAM_NAME_TO_DST.items():
            if nick.lower() == nickname:
                return f"{nick} DST", f"DEF-{fid}"
        # Return as-is if we can't map it
        return player_name, None

    # Check if it's a known team name
    if name_lower in TEAM_NAME_TO_DST:
        nickname, franchise_id = TEAM_NAME_TO_DST[name_lower]
        return f"{nickname} DST", f"DEF-{franchise_id}"

    # Not a DEF player
    return None, None


def is_def_player(player_name: str) -> bool:
    """
    Check if a player name represents a DEF/DST.

    Args:
        player_name: Player name to check

    Returns:
        True if this is a defense/DST player
    """
    normalized, _ = normalize_def_player_name(player_name)
    return normalized is not None


def normalize_player_name_for_join(player_name: str, position: str | None = None) -> tuple[str, str | None]:
    """
    Normalize a player name for joining with NFL data.

    For DEF players, converts to "Nickname DST" format.
    For regular players, returns the name unchanged.

    Args:
        player_name: Raw player name
        position: Optional position hint (DEF, DST, D/ST triggers DEF normalization)

    Returns:
        Tuple of (normalized_name, nfl_player_id or None)
    """
    if not player_name or pd.isna(player_name):
        return str(player_name) if player_name else "", None

    # Check position hint first
    if position:
        pos_upper = str(position).upper()
        if pos_upper in ("DEF", "DST", "D/ST"):
            normalized, nfl_id = normalize_def_player_name(player_name)
            if normalized:
                return normalized, nfl_id

    # Try to detect DEF from name
    normalized, nfl_id = normalize_def_player_name(player_name)
    if normalized:
        return normalized, nfl_id

    # Not a DEF - return unchanged
    return player_name, None


def normalize_manager_name(nickname: str, overrides: dict = None, team_name_fallback: str = None) -> str:
    """
    Normalize manager name with optional overrides and team_name fallback.

    This function handles:
    1. Empty/None nicknames → use team_name_fallback
    2. --hidden-- managers → use team_name_fallback
    3. Overrides from LeagueContext (e.g., {"Team Name": "Real Name"})
    4. Title case normalization

    Args:
        nickname: Raw manager nickname from Yahoo API
        overrides: Dict mapping old names to new names (from LeagueContext.manager_name_overrides)
        team_name_fallback: Team name to use if manager is hidden/unavailable

    Returns:
        Normalized manager name

    Example:
        >>> normalize_manager_name("--hidden--", {"You Are A Pirate": "Ezra"}, "You Are A Pirate")
        'Ezra'
        >>> normalize_manager_name("--hidden--", None, "My Team Name")
        'My Team Name'
        >>> normalize_manager_name("JohnDoe", {"JohnDoe": "John Doe"}, None)
        'John Doe'
    """
    # Build case-insensitive override lookup (handles apostrophe casing like "Newton'S Law" vs "Newton's Law")
    override_lookup = {}
    if overrides:
        for k, v in overrides.items():
            override_lookup[k.lower()] = v

    # Handle empty/None nickname
    if not nickname:
        if team_name_fallback:
            fallback = str(team_name_fallback).strip()
            # Check if there's an override for this team name (case-insensitive)
            if fallback.lower() in override_lookup:
                return override_lookup[fallback.lower()]
            return fallback.title()
        return "Unknown"

    s = str(nickname).strip()

    # Check overrides using case-insensitive matching
    if override_lookup:
        if s.lower() in override_lookup:
            return override_lookup[s.lower()]
        # ALSO check team_name fallback for overrides - this catches cases where
        # the nickname differs from the team name but we want to normalize to team name
        if team_name_fallback:
            fallback = str(team_name_fallback).strip()
            if fallback.lower() in override_lookup:
                return override_lookup[fallback.lower()]

    # Handle --hidden-- managers by using team name fallback
    if s == "--hidden--":
        if team_name_fallback:
            fallback = str(team_name_fallback).strip()
            # Check if there's an override for this team name (case-insensitive)
            if fallback.lower() in override_lookup:
                return override_lookup[fallback.lower()]
            return fallback.title()
        return "Unknown"

    # Return title case if no override applies
    return s.title()


# Alias for backwards compatibility - other modules can import either name
norm_manager = normalize_manager_name


def normalize_team_name(team):
    """Normalize team names to handle variations."""
    if not team or pd.isna(team):
        return team

    s = str(team).upper().strip()

    team_mappings = {
        "LOS ANGELES RAMS": "LAR",
        "LA RAMS": "LAR",
        "LOS ANGELES CHARGERS": "LAC",
        "LA CHARGERS": "LAC",
        "LOS ANGELES": "LA",
        "NEW YORK JETS": "NYJ",
        "NY JETS": "NYJ",
        "NEW YORK GIANTS": "NYG",
        "NY GIANTS": "NYG",
        "NEW YORK": "NY",
    }

    return team_mappings.get(s, team)
