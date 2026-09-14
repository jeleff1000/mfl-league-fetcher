"""
Build Player ID Bridge - Unified Player IDs for Historical + NFLverse Data

Creates a unified player ID system for the super table:
1. Players who bridge to NFLverse (career spans 1998+1999): Use NFLverse player_id
2. Historical-only players (career ended pre-1999): Create "HIST-{kaggle_id}"

This ensures every player in the super table has a consistent, unique ID that can be
used for career-spanning analytics (e.g., Walter Payton = HIST-walterpayton01).

Matching Strategy for bridged players:
1. Layer 1: Exact normalized name + position + team (highest confidence)
2. Layer 2: Exact normalized name + position (team may have changed)
3. Layer 3: Last name 1:1 + position + team (catches nicknames)
4. Layer 4: Fuzzy name + position + team (catches spelling variations)

Usage:
    python build_player_id_bridge.py
    python build_player_id_bridge.py --output bridge.parquet
    python build_player_id_bridge.py --apply  # Apply bridge to historical data
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from difflib import SequenceMatcher
import pandas as pd

# Add parent for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from multi_league.data_fetchers.shared.name_utils import normalize_name

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

HISTORICAL_FILE = REPO_ROOT / "data" / "historical" / "historical_player_formatted.parquet"
NFLVERSE_CACHE = SCRIPT_DIR / "fantasy_football_data" / "cache" / "nflverse"
OUTPUT_DIR = REPO_ROOT / "data" / "bridge"

# Years where players could span both datasets
OVERLAP_YEARS_HISTORICAL = list(range(1970, 1999))  # Last year in Kaggle
OVERLAP_YEARS_NFLVERSE = list(range(1999, 2010))  # First years in NFLverse (career overlap window)

# Matching thresholds
FUZZY_THRESHOLD = 85.0  # Minimum fuzzy score
FUZZY_GAP = 10.0  # Minimum gap vs second-best match


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ============================================================================
# Name Normalization (from yahoo_nfl_merge_v3.py)
# ============================================================================

COMPOUND_PREFIXES = {"st", "de", "la", "le", "van", "von", "del", "der", "den", "mc", "mac", "o"}

ACCENT_MAP = str.maketrans(
    {
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "á": "a",
        "à": "a",
        "â": "a",
        "ä": "a",
        "ã": "a",
        "í": "i",
        "ì": "i",
        "î": "i",
        "ï": "i",
        "ó": "o",
        "ò": "o",
        "ô": "o",
        "ö": "o",
        "õ": "o",
        "ú": "u",
        "ù": "u",
        "û": "u",
        "ü": "u",
        "ñ": "n",
        "ç": "c",
    }
)


# normalize_name is imported from multi_league.data_fetchers.shared.name_utils


def extract_last_name(name: str) -> str:
    """Extract last name, handling compound names."""
    norm = normalize_name(name)
    parts = norm.split()

    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]

    last_name_parts = [parts[-1]]
    for i in range(len(parts) - 2, 0, -1):
        if parts[i] in COMPOUND_PREFIXES:
            last_name_parts.insert(0, parts[i])
        else:
            break

    return " ".join(last_name_parts)


def normalize_position(pos: str) -> str:
    """Normalize position to fantasy-relevant categories."""
    if pd.isna(pos) or not pos:
        return "UNK"

    pos = str(pos).upper().strip()

    # Map to standard fantasy positions
    pos_map = {
        "QB": "QB",
        "RB": "RB",
        "FB": "RB",
        "HB": "RB",
        "WR": "WR",
        "SE": "WR",
        "FL": "WR",
        "TE": "TE",
        "K": "K",
        "PK": "K",
    }

    return pos_map.get(pos, pos)


def fuzzy_score(a: str, b: str) -> float:
    """Calculate fuzzy similarity score (0-100)."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


# ============================================================================
# Headshot URL Generation
# ============================================================================

# NFL.com placeholder image file size (bytes)
# When an NFLverse headshot URL returns this exact size, it's a generic silhouette
NFL_PLACEHOLDER_SIZE = 382225


def is_placeholder_headshot(url: str, timeout: float = 5.0) -> bool:
    """
    Check if an NFL.com headshot URL is a placeholder (silhouette) image.

    The NFL.com CDN uses a specific placeholder image for players without photos.
    This placeholder has a consistent file size of 382,225 bytes.

    Args:
        url: The headshot URL to check
        timeout: Request timeout in seconds

    Returns:
        True if the URL points to a placeholder, False if it's a real photo
    """
    if not url or pd.isna(url):
        return True

    try:
        import requests

        r = requests.head(url, timeout=timeout)
        if r.status_code == 200:
            size = int(r.headers.get("content-length", 0))
            return size == NFL_PLACEHOLDER_SIZE
    except Exception:
        pass

    # If we can't check, assume it's not a placeholder
    return False


def generate_pfr_player_id(name: str, disambiguator: int = 0) -> str:
    """
    Generate a Pro Football Reference style player ID.

    PFR format: Last4 + First2 + 2-digit number
    Example: Walter Payton -> PaytWa00
    """
    parts = normalize_name(name).split()
    if len(parts) < 2:
        return None

    first_name = parts[0]
    last_name = parts[-1]

    # Take first 4 chars of last name (or pad if shorter)
    last_part = (last_name[:4].capitalize() + "xxxx")[:4]
    # Take first 2 chars of first name
    first_part = (first_name[:2].capitalize() + "xx")[:2]
    # Add disambiguator
    num_part = f"{disambiguator:02d}"

    return f"{last_part}{first_part}{num_part}"


def generate_headshot_url(name: str, player_id: str = None, source: str = "pfr") -> str | None:
    """
    Generate a headshot URL for a historical player.

    Sources:
    - 'pfr': Pro Football Reference (best coverage for historical players)
    - 'espn': ESPN (limited historical coverage)

    Args:
        name: Player name
        player_id: Known player ID (e.g., from Kaggle or PFR)
        source: Which source to use

    Returns:
        URL string or None
    """
    if source == "pfr":
        # PFR headshot URL pattern
        # https://www.pro-football-reference.com/req/20180910/images/headshots/PaytWa00_2022.jpg
        pfr_id = generate_pfr_player_id(name)
        if pfr_id:
            return f"https://www.pro-football-reference.com/req/20180910/images/headshots/{pfr_id}_2022.jpg"

    elif source == "espn":
        # ESPN uses numeric IDs, harder to derive without lookup
        return None

    return None


def get_headshot_url_for_player(
    name: str,
    kaggle_id: str = None,
    nflverse_id: str = None,
    nflverse_headshot: str = None,
    check_placeholder: bool = True,
) -> str | None:
    """
    Get the best available headshot URL for a player.

    Priority:
    1. NFLverse headshot (if bridged, available, AND not a placeholder)
    2. Pro Football Reference (for historical players or placeholder fallback)

    Args:
        name: Player name
        kaggle_id: Kaggle player ID
        nflverse_id: NFLverse player ID (if bridged)
        nflverse_headshot: NFLverse headshot URL (if available)
        check_placeholder: Whether to verify NFLverse URL isn't a placeholder (slower)

    Returns:
        Best available headshot URL
    """
    # If we have NFLverse headshot, check if it's real (not a placeholder)
    if nflverse_headshot and pd.notna(nflverse_headshot):
        if check_placeholder:
            # Check if it's the NFL.com placeholder silhouette
            if not is_placeholder_headshot(nflverse_headshot):
                return nflverse_headshot
            # It's a placeholder, fall through to PFR
        else:
            return nflverse_headshot

    # Try to generate PFR URL as fallback
    pfr_url = generate_headshot_url(name, kaggle_id, source="pfr")
    if pfr_url:
        return pfr_url

    return None


def replace_placeholder_headshots(
    df: pd.DataFrame,
    name_col: str = "player",
    headshot_col: str = "headshot_url",
    batch_size: int = 50,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Replace placeholder headshot URLs with PFR fallback URLs in a DataFrame.

    This function checks each headshot URL and replaces NFL.com placeholder
    images (382,225 bytes) with Pro Football Reference URLs.

    Args:
        df: DataFrame with player data
        name_col: Column containing player names
        headshot_col: Column containing headshot URLs
        batch_size: Number of URLs to check between progress updates
        show_progress: Whether to print progress updates

    Returns:
        DataFrame with placeholder headshots replaced
    """
    if headshot_col not in df.columns:
        log(f"Warning: {headshot_col} column not found, skipping placeholder replacement")
        return df

    if name_col not in df.columns:
        # Try common alternatives
        for alt in ["player_display_name", "player_name", "name"]:
            if alt in df.columns:
                name_col = alt
                break
        else:
            log("Warning: Could not find name column, skipping placeholder replacement")
            return df

    df = df.copy()

    # Get unique headshot URLs to check (avoid duplicate requests)
    unique_urls = df[headshot_col].dropna().unique()
    nfl_urls = [url for url in unique_urls if "static.www.nfl.com" in str(url)]

    if not nfl_urls:
        log("No NFL.com headshot URLs found to check")
        return df

    log(f"Checking {len(nfl_urls):,} unique NFL.com headshot URLs for placeholders...")

    # Check which URLs are placeholders
    import requests

    placeholder_urls = set()
    checked = 0

    for url in nfl_urls:
        try:
            r = requests.head(url, timeout=5)
            if r.status_code == 200:
                size = int(r.headers.get("content-length", 0))
                if size == NFL_PLACEHOLDER_SIZE:
                    placeholder_urls.add(url)
        except Exception:
            pass

        checked += 1
        if show_progress and checked % batch_size == 0:
            log(f"  Checked {checked:,}/{len(nfl_urls):,} URLs, found {len(placeholder_urls):,} placeholders")

    log(f"  Found {len(placeholder_urls):,} placeholder URLs out of {len(nfl_urls):,} checked")

    if not placeholder_urls:
        log("No placeholders found, headshots are all real")
        return df

    # Replace placeholders with PFR URLs
    replaced = 0
    for idx, row in df.iterrows():
        if row[headshot_col] in placeholder_urls:
            pfr_url = generate_headshot_url(row[name_col], source="pfr")
            if pfr_url:
                df.at[idx, headshot_col] = pfr_url
                replaced += 1

    log(f"  Replaced {replaced:,} placeholder headshots with PFR URLs")

    return df


# ============================================================================
# Data Loading
# ============================================================================


def load_historical_players(historical_file: Path) -> pd.DataFrame:
    """Load unique players from historical data."""
    log(f"Loading historical data from: {historical_file}")

    if not historical_file.exists():
        raise FileNotFoundError(f"Historical file not found: {historical_file}")

    df = pd.read_parquet(historical_file)
    log(f"Loaded {len(df):,} historical records")

    # Get unique players with their career info
    # Group by player_id to get career span
    players = (
        df.groupby("NFL_player_id")
        .agg(
            {
                "player": "first",
                "nfl_position": "first",
                "nfl_team": lambda x: x.dropna().iloc[-1] if len(x.dropna()) > 0 else None,  # Last known team
                "year": ["min", "max"],
            }
        )
        .reset_index()
    )

    players.columns = ["kaggle_player_id", "player", "position", "last_team", "first_year", "last_year"]

    # Filter to players who played close to 1999 (potential overlap)
    # Players who last played 1995-1998 could still be in NFLverse 1999+
    players = players[players["last_year"] >= 1995]

    log(f"Found {len(players):,} historical players with last_year >= 1995")

    return players


def load_nflverse_players(cache_dir: Path, years: list[int] = None) -> pd.DataFrame:
    """Load unique players from NFLverse cached data."""
    if years is None:
        years = list(range(1999, 2010))  # First decade of NFLverse

    all_players = []

    for year in years:
        cache_file = cache_dir / f"nflverse_player_stats_{year}.parquet"
        if cache_file.exists():
            try:
                df = pd.read_parquet(cache_file)
                if "player_id" in df.columns:
                    df = df.rename(columns={"player_id": "NFL_player_id"})
                all_players.append(df)
                log(f"  Loaded {len(df):,} records from {year}")
            except Exception as e:
                log(f"  Warning: Could not load {year}: {e}")

    if not all_players:
        raise RuntimeError("No NFLverse data found in cache")

    combined = pd.concat(all_players, ignore_index=True)
    log(f"Total NFLverse records: {len(combined):,}")

    # Determine player name column
    name_col = "player_display_name" if "player_display_name" in combined.columns else "player_name"
    if name_col not in combined.columns:
        name_col = "player"

    # Determine position column
    pos_col = "position" if "position" in combined.columns else "nfl_position"

    # Determine team column
    team_col = "recent_team" if "recent_team" in combined.columns else "team"
    if team_col not in combined.columns:
        team_col = "nfl_team"

    # Determine headshot column
    headshot_col = "headshot_url" if "headshot_url" in combined.columns else None

    # Get unique players
    agg_dict = {
        name_col: "first",
        pos_col: "first",
        team_col: lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,  # First known team
        "season" if "season" in combined.columns else "year": ["min", "max"],
    }
    if headshot_col:
        agg_dict[headshot_col] = "first"

    players = combined.groupby("NFL_player_id").agg(agg_dict).reset_index()

    if headshot_col:
        players.columns = [
            "nflverse_player_id",
            "player",
            "position",
            "first_team",
            "first_year",
            "last_year",
            "headshot_url",
        ]
    else:
        players.columns = ["nflverse_player_id", "player", "position", "first_team", "first_year", "last_year"]
        players["headshot_url"] = None

    # Filter to players who appeared in early NFLverse years (potential overlap)
    players = players[players["first_year"] <= 2005]

    log(f"Found {len(players):,} NFLverse players with first_year <= 2005")

    return players


# ============================================================================
# Matching Engine
# ============================================================================


def build_player_bridge(
    historical: pd.DataFrame, nflverse: pd.DataFrame, check_placeholders: bool = False
) -> pd.DataFrame:
    """
    Build bridge mapping kaggle_player_id -> nflverse_player_id.

    Uses multi-layer matching:
    1. Exact name + position + team
    2. Exact name + position
    3. Last name 1:1 + position + team
    4. Fuzzy name + position + team

    Args:
        historical: Historical player DataFrame
        nflverse: NFLverse player DataFrame
        check_placeholders: If True, verify NFLverse headshots aren't placeholders (slower)
    """
    log("\nBuilding player ID bridge...")
    log(f"Historical candidates: {len(historical):,}")
    log(f"NFLverse candidates: {len(nflverse):,}")

    # Add normalized columns
    historical = historical.copy()
    nflverse = nflverse.copy()

    historical["norm_name"] = historical["player"].apply(normalize_name)
    historical["norm_pos"] = historical["position"].apply(normalize_position)
    historical["last_name"] = historical["player"].apply(extract_last_name)

    nflverse["norm_name"] = nflverse["player"].apply(normalize_name)
    nflverse["norm_pos"] = nflverse["position"].apply(normalize_position)
    nflverse["last_name"] = nflverse["player"].apply(extract_last_name)

    matches = []
    matched_historical = set()
    matched_nflverse = set()

    # =========================================================================
    # Layer 1: Exact name + position + team (1:1 constraint)
    # =========================================================================
    log("\nLayer 1: Exact name + position + team (1:1)...")

    # Build lookup: (norm_name, norm_pos, team) -> list of players
    hist_by_key_l1 = {}
    for _, h_row in historical.iterrows():
        if h_row["kaggle_player_id"] in matched_historical:
            continue
        key = (h_row["norm_name"], h_row["norm_pos"], h_row["last_team"])
        if key not in hist_by_key_l1:
            hist_by_key_l1[key] = []
        hist_by_key_l1[key].append(h_row)

    nfl_by_key_l1 = {}
    for _, n_row in nflverse.iterrows():
        if n_row["nflverse_player_id"] in matched_nflverse:
            continue
        key = (n_row["norm_name"], n_row["norm_pos"], n_row["first_team"])
        if key not in nfl_by_key_l1:
            nfl_by_key_l1[key] = []
        nfl_by_key_l1[key].append(n_row)

    # Only match where exactly 1:1
    for key in set(hist_by_key_l1.keys()) & set(nfl_by_key_l1.keys()):
        if len(hist_by_key_l1[key]) == 1 and len(nfl_by_key_l1[key]) == 1:
            h_row = hist_by_key_l1[key][0]
            n_row = nfl_by_key_l1[key][0]

            if h_row["kaggle_player_id"] in matched_historical:
                continue
            if n_row["nflverse_player_id"] in matched_nflverse:
                continue

            # Get headshot: prefer NFLverse (if real), fallback to PFR
            nfl_headshot = n_row["headshot_url"] if "headshot_url" in n_row.index else None
            headshot = get_headshot_url_for_player(
                h_row["player"],
                h_row["kaggle_player_id"],
                n_row["nflverse_player_id"],
                nfl_headshot,
                check_placeholder=check_placeholders,
            )
            matches.append(
                {
                    "kaggle_player_id": h_row["kaggle_player_id"],
                    "nflverse_player_id": n_row["nflverse_player_id"],
                    "kaggle_name": h_row["player"],
                    "nflverse_name": n_row["player"],
                    "position": h_row["norm_pos"],
                    "team": h_row["last_team"],
                    "match_layer": 1,
                    "match_type": "exact_name_pos_team",
                    "confidence": 100.0,
                    "headshot_url": headshot,
                }
            )
            matched_historical.add(h_row["kaggle_player_id"])
            matched_nflverse.add(n_row["nflverse_player_id"])

    log(f"  Matched: {len(matches):,}")

    # =========================================================================
    # Layer 2: Exact name + position (1:1 constraint, no team requirement)
    # =========================================================================
    log("\nLayer 2: Exact name + position (1:1)...")
    layer2_start = len(matches)

    # Build lookup: (norm_name, norm_pos) -> list of players
    hist_by_key_l2 = {}
    for _, h_row in historical.iterrows():
        if h_row["kaggle_player_id"] in matched_historical:
            continue
        key = (h_row["norm_name"], h_row["norm_pos"])
        if key not in hist_by_key_l2:
            hist_by_key_l2[key] = []
        hist_by_key_l2[key].append(h_row)

    nfl_by_key_l2 = {}
    for _, n_row in nflverse.iterrows():
        if n_row["nflverse_player_id"] in matched_nflverse:
            continue
        key = (n_row["norm_name"], n_row["norm_pos"])
        if key not in nfl_by_key_l2:
            nfl_by_key_l2[key] = []
        nfl_by_key_l2[key].append(n_row)

    # Only match where exactly 1:1
    for key in set(hist_by_key_l2.keys()) & set(nfl_by_key_l2.keys()):
        if len(hist_by_key_l2[key]) == 1 and len(nfl_by_key_l2[key]) == 1:
            h_row = hist_by_key_l2[key][0]
            n_row = nfl_by_key_l2[key][0]

            if h_row["kaggle_player_id"] in matched_historical:
                continue
            if n_row["nflverse_player_id"] in matched_nflverse:
                continue

            nfl_headshot = n_row["headshot_url"] if "headshot_url" in n_row.index else None
            headshot = get_headshot_url_for_player(
                h_row["player"],
                h_row["kaggle_player_id"],
                n_row["nflverse_player_id"],
                nfl_headshot,
                check_placeholder=check_placeholders,
            )
            matches.append(
                {
                    "kaggle_player_id": h_row["kaggle_player_id"],
                    "nflverse_player_id": n_row["nflverse_player_id"],
                    "kaggle_name": h_row["player"],
                    "nflverse_name": n_row["player"],
                    "position": h_row["norm_pos"],
                    "team": f"{h_row['last_team']} -> {n_row['first_team']}",
                    "match_layer": 2,
                    "match_type": "exact_name_pos",
                    "confidence": 95.0,
                    "headshot_url": headshot,
                }
            )
            matched_historical.add(h_row["kaggle_player_id"])
            matched_nflverse.add(n_row["nflverse_player_id"])

    log(f"  Matched: {len(matches) - layer2_start:,}")

    # =========================================================================
    # Layer 3: Last name 1:1 + position + team
    # =========================================================================
    log("\nLayer 3: Last name 1:1 + position + team...")
    layer3_start = len(matches)

    # Build last name lookup for NFLverse
    nfl_by_last_pos_team = {}
    for _, n_row in nflverse.iterrows():
        if n_row["nflverse_player_id"] in matched_nflverse:
            continue
        key = (n_row["last_name"], n_row["norm_pos"], n_row["first_team"])
        if key not in nfl_by_last_pos_team:
            nfl_by_last_pos_team[key] = []
        nfl_by_last_pos_team[key].append(n_row)

    for _, h_row in historical.iterrows():
        if h_row["kaggle_player_id"] in matched_historical:
            continue

        key = (h_row["last_name"], h_row["norm_pos"], h_row["last_team"])
        candidates = nfl_by_last_pos_team.get(key, [])

        # Only match if exactly 1:1
        if len(candidates) == 1:
            n_row = candidates[0]
            if n_row["nflverse_player_id"] not in matched_nflverse:
                nfl_headshot = n_row["headshot_url"] if "headshot_url" in n_row.index else None
                headshot = get_headshot_url_for_player(
                    h_row["player"],
                    h_row["kaggle_player_id"],
                    n_row["nflverse_player_id"],
                    nfl_headshot,
                    check_placeholder=check_placeholders,
                )
                matches.append(
                    {
                        "kaggle_player_id": h_row["kaggle_player_id"],
                        "nflverse_player_id": n_row["nflverse_player_id"],
                        "kaggle_name": h_row["player"],
                        "nflverse_name": n_row["player"],
                        "position": h_row["norm_pos"],
                        "team": h_row["last_team"],
                        "match_layer": 3,
                        "match_type": "last_name_pos_team",
                        "confidence": 90.0,
                        "headshot_url": headshot,
                    }
                )
                matched_historical.add(h_row["kaggle_player_id"])
                matched_nflverse.add(n_row["nflverse_player_id"])

    log(f"  Matched: {len(matches) - layer3_start:,}")

    # =========================================================================
    # Layer 4: Fuzzy name + position + team (1:1 constraint)
    # =========================================================================
    log("\nLayer 4: Fuzzy name + position + team (1:1)...")
    layer4_start = len(matches)

    # Build lookup by position + team for both sides
    hist_by_pos_team = {}
    for _, h_row in historical.iterrows():
        if h_row["kaggle_player_id"] in matched_historical:
            continue
        key = (h_row["norm_pos"], h_row["last_team"])
        if key not in hist_by_pos_team:
            hist_by_pos_team[key] = []
        hist_by_pos_team[key].append(h_row)

    nfl_by_pos_team = {}
    for _, n_row in nflverse.iterrows():
        if n_row["nflverse_player_id"] in matched_nflverse:
            continue
        key = (n_row["norm_pos"], n_row["first_team"])
        if key not in nfl_by_pos_team:
            nfl_by_pos_team[key] = []
        nfl_by_pos_team[key].append(n_row)

    # Find fuzzy matches, then enforce 1:1
    fuzzy_candidates = []  # [(h_row, n_row, score), ...]

    for key in set(hist_by_pos_team.keys()) & set(nfl_by_pos_team.keys()):
        hist_list = hist_by_pos_team[key]
        nfl_list = nfl_by_pos_team[key]

        for h_row in hist_list:
            if h_row["kaggle_player_id"] in matched_historical:
                continue

            best_match = None
            best_score = 0
            second_score = 0

            for n_row in nfl_list:
                if n_row["nflverse_player_id"] in matched_nflverse:
                    continue
                score = fuzzy_score(h_row["norm_name"], n_row["norm_name"])
                if score > best_score:
                    second_score = best_score
                    best_score = score
                    best_match = n_row
                elif score > second_score:
                    second_score = score

            if best_match is not None and best_score >= FUZZY_THRESHOLD and (best_score - second_score) >= FUZZY_GAP:
                fuzzy_candidates.append((h_row, best_match, best_score))

    # Enforce 1:1: check that each nflverse player is only claimed by one historical player
    nfl_claims = {}  # nflverse_id -> [(h_row, score), ...]
    for h_row, n_row, score in fuzzy_candidates:
        nfl_id = n_row["nflverse_player_id"]
        if nfl_id not in nfl_claims:
            nfl_claims[nfl_id] = []
        nfl_claims[nfl_id].append((h_row, n_row, score))

    for nfl_id, claims in nfl_claims.items():
        if len(claims) == 1:
            h_row, n_row, score = claims[0]
            if (
                h_row["kaggle_player_id"] not in matched_historical
                and n_row["nflverse_player_id"] not in matched_nflverse
            ):
                nfl_headshot = n_row["headshot_url"] if "headshot_url" in n_row.index else None
                headshot = get_headshot_url_for_player(
                    h_row["player"],
                    h_row["kaggle_player_id"],
                    n_row["nflverse_player_id"],
                    nfl_headshot,
                    check_placeholder=check_placeholders,
                )
                matches.append(
                    {
                        "kaggle_player_id": h_row["kaggle_player_id"],
                        "nflverse_player_id": n_row["nflverse_player_id"],
                        "kaggle_name": h_row["player"],
                        "nflverse_name": n_row["player"],
                        "position": h_row["norm_pos"],
                        "team": h_row["last_team"],
                        "match_layer": 4,
                        "match_type": f"fuzzy_{score:.1f}",
                        "confidence": score,
                        "headshot_url": headshot,
                    }
                )
                matched_historical.add(h_row["kaggle_player_id"])
                matched_nflverse.add(n_row["nflverse_player_id"])

    log(f"  Matched: {len(matches) - layer4_start:,}")

    # =========================================================================
    # Summary
    # =========================================================================
    bridge_df = pd.DataFrame(matches)

    log(f"\n{'='*60}")
    log("BRIDGE SUMMARY")
    log(f"{'='*60}")
    log(f"Total matches: {len(bridge_df):,}")
    log(f"  Layer 1 (exact name+pos+team): {len(bridge_df[bridge_df['match_layer']==1]):,}")
    log(f"  Layer 2 (exact name+pos):      {len(bridge_df[bridge_df['match_layer']==2]):,}")
    log(f"  Layer 3 (last name 1:1):       {len(bridge_df[bridge_df['match_layer']==3]):,}")
    log(f"  Layer 4 (fuzzy):               {len(bridge_df[bridge_df['match_layer']==4]):,}")
    log(f"\nUnmatched historical: {len(historical) - len(matched_historical):,}")

    # =========================================================================
    # Add entries for unmatched historical players (HIST-xxx IDs)
    # =========================================================================
    log("\nCreating IDs for historical-only players...")

    for _, h_row in historical.iterrows():
        if h_row["kaggle_player_id"] in matched_historical:
            continue

        # Create a HIST-xxx ID for this player
        hist_id = f"HIST-{h_row['kaggle_player_id']}"

        # Generate PFR headshot URL for historical-only players
        headshot = generate_headshot_url(h_row["player"], source="pfr")

        matches.append(
            {
                "kaggle_player_id": h_row["kaggle_player_id"],
                "nflverse_player_id": None,  # No NFLverse match
                "unified_player_id": hist_id,  # New unified ID
                "kaggle_name": h_row["player"],
                "nflverse_name": None,
                "position": h_row["norm_pos"],
                "team": h_row["last_team"],
                "match_layer": 0,  # 0 = no match (historical only)
                "match_type": "historical_only",
                "confidence": 100.0,  # We're certain this is the right ID for this player
                "headshot_url": headshot,
            }
        )

    # Add unified_player_id to all matched entries
    bridge_df = pd.DataFrame(matches)

    # For bridged players, unified_player_id = nflverse_player_id
    if "unified_player_id" not in bridge_df.columns:
        bridge_df["unified_player_id"] = None
    bridge_df.loc[bridge_df["nflverse_player_id"].notna(), "unified_player_id"] = bridge_df.loc[
        bridge_df["nflverse_player_id"].notna(), "nflverse_player_id"
    ]

    log("\nFinal bridge:")
    log(f"  Total entries: {len(bridge_df):,}")
    log(f"  Bridged to NFLverse: {bridge_df['nflverse_player_id'].notna().sum():,}")
    log(f"  Historical-only (HIST-xxx): {bridge_df['nflverse_player_id'].isna().sum():,}")

    return bridge_df


def apply_bridge_to_historical(historical_file: Path, bridge_df: pd.DataFrame, output_file: Path) -> pd.DataFrame:
    """
    Apply the bridge to historical data, replacing kaggle IDs with unified IDs.

    Args:
        historical_file: Path to historical parquet
        bridge_df: Bridge DataFrame with kaggle_player_id -> unified_player_id mapping
        output_file: Path to save updated historical data

    Returns:
        Updated historical DataFrame
    """
    log("\nApplying bridge to historical data...")
    log(f"Loading: {historical_file}")

    df = pd.read_parquet(historical_file)
    original_count = len(df)
    log(f"Loaded {original_count:,} records")

    # Create lookup: kaggle_player_id -> unified_player_id
    bridge_lookup = dict(zip(bridge_df["kaggle_player_id"], bridge_df["unified_player_id"]))

    # Apply mapping
    df["unified_player_id"] = df["NFL_player_id"].map(bridge_lookup)

    # Count results
    mapped = df["unified_player_id"].notna().sum()
    bridged = df["unified_player_id"].str.startswith("00-", na=False).sum()  # NFLverse IDs start with 00-
    hist_only = df["unified_player_id"].str.startswith("HIST-", na=False).sum()

    log("\nMapping results:")
    log(f"  Total records: {len(df):,}")
    log(f"  Mapped: {mapped:,} ({100*mapped/len(df):.1f}%)")
    log(f"    -> Bridged to NFLverse: {bridged:,}")
    log(f"    -> Historical-only: {hist_only:,}")
    log(f"  Unmapped: {len(df) - mapped:,}")

    # Save
    log(f"\nSaving to: {output_file}")
    df.to_parquet(output_file, index=False)

    return df


# ============================================================================
# Lookup Functions for External Use
# ============================================================================

# Global bridge cache (loaded once on first use)
_BRIDGE_CACHE: pd.DataFrame | None = None
_BRIDGE_BY_NAME_POS_TEAM: dict[tuple[str, str, str], str] | None = None
_BRIDGE_BY_NAME_POS: dict[tuple[str, str], str] | None = None


def load_bridge_cache() -> pd.DataFrame:
    """Load bridge data into memory cache."""
    global _BRIDGE_CACHE
    if _BRIDGE_CACHE is None:
        bridge_path = OUTPUT_DIR / "player_id_bridge.parquet"
        if bridge_path.exists():
            _BRIDGE_CACHE = pd.read_parquet(bridge_path)
        else:
            _BRIDGE_CACHE = pd.DataFrame()
    return _BRIDGE_CACHE


def get_unified_id_for_player(name: str, position: str = None, team: str = None, year: int = None) -> str | None:
    """
    Look up unified player ID from bridge, prioritizing career continuity.

    For players who span both historical (pre-1999) and NFLverse (1999+) eras,
    this returns the NFLverse ID for ALL their records (career continuity).

    Args:
        name: Player name
        position: Position (QB, RB, WR, etc.)
        team: Team abbreviation
        year: Year (used for context but not strict matching)

    Returns:
        Unified player ID if found:
        - NFLverse ID (e.g., '00-0023459') for players bridged to NFLverse
        - HIST-xxx ID for historical-only players
        - None if no match found
    """
    global _BRIDGE_BY_NAME_POS_TEAM, _BRIDGE_BY_NAME_POS

    bridge = load_bridge_cache()
    if bridge.empty:
        return None

    # Build lookup dictionaries on first use
    if _BRIDGE_BY_NAME_POS_TEAM is None:
        _BRIDGE_BY_NAME_POS_TEAM = {}
        _BRIDGE_BY_NAME_POS = {}

        for _, row in bridge.iterrows():
            norm_name = normalize_name(row["kaggle_name"])
            norm_pos = normalize_position(row["position"])
            team_val = row["team"] if pd.notna(row["team"]) else ""
            unified_id = row["unified_player_id"]

            # Index by name + position + team
            key_full = (norm_name, norm_pos, team_val)
            if key_full not in _BRIDGE_BY_NAME_POS_TEAM:
                _BRIDGE_BY_NAME_POS_TEAM[key_full] = unified_id

            # Index by name + position (for team-agnostic matching)
            key_np = (norm_name, norm_pos)
            if key_np not in _BRIDGE_BY_NAME_POS:
                _BRIDGE_BY_NAME_POS[key_np] = unified_id

    # Normalize inputs
    norm_name = normalize_name(name)
    norm_pos = normalize_position(position) if position else "UNK"
    team = team if team else ""

    # Try exact match with team
    key_full = (norm_name, norm_pos, team)
    if key_full in _BRIDGE_BY_NAME_POS_TEAM:
        return _BRIDGE_BY_NAME_POS_TEAM[key_full]

    # Try match without team
    key_np = (norm_name, norm_pos)
    if key_np in _BRIDGE_BY_NAME_POS:
        return _BRIDGE_BY_NAME_POS[key_np]

    return None


def get_nflverse_id_for_player(name: str, position: str = None, team: str = None, year: int = None) -> str | None:
    """
    Look up NFLverse player ID from bridge (career continuity).

    Unlike get_unified_id_for_player, this only returns NFLverse IDs
    (not HIST-xxx IDs). Use this when you specifically need career
    continuity for players who have NFLverse data.

    Args:
        name: Player name
        position: Position (QB, RB, WR, etc.)
        team: Team abbreviation
        year: Year (used for context)

    Returns:
        NFLverse player ID (e.g., '00-0023459') if player is bridged, else None
    """
    unified_id = get_unified_id_for_player(name, position, team, year)

    # Only return NFLverse IDs (those starting with digits, not HIST-)
    if unified_id and not unified_id.startswith("HIST-"):
        return unified_id

    return None


# ============================================================================
# Main
# ============================================================================


def load_all_historical_players(historical_file: Path) -> pd.DataFrame:
    """Load ALL unique players from historical data (not just overlap candidates)."""
    log(f"Loading all historical players from: {historical_file}")

    if not historical_file.exists():
        raise FileNotFoundError(f"Historical file not found: {historical_file}")

    df = pd.read_parquet(historical_file)
    log(f"Loaded {len(df):,} historical records")

    # Get unique players with their career info
    players = (
        df.groupby("NFL_player_id")
        .agg(
            {
                "player": "first",
                "nfl_position": "first",
                "nfl_team": lambda x: x.dropna().iloc[-1] if len(x.dropna()) > 0 else None,
                "year": ["min", "max"],
            }
        )
        .reset_index()
    )

    players.columns = ["kaggle_player_id", "player", "position", "last_team", "first_year", "last_year"]

    log(f"Found {len(players):,} unique historical players")

    return players


def main():
    parser = argparse.ArgumentParser(description="Build Player ID Bridge")
    parser.add_argument("--historical", type=str, help="Path to historical parquet")
    parser.add_argument("--cache-dir", type=str, help="Path to NFLverse cache directory")
    parser.add_argument("--output", type=str, help="Output path for bridge parquet")
    parser.add_argument(
        "--nflverse-years", type=str, default="1999-2010", help="NFLverse year range to search (default: 1999-2010)"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Apply bridge to historical data (adds unified_player_id column)"
    )
    parser.add_argument(
        "--skip-nflverse", action="store_true", help="Skip NFLverse matching (just create HIST-xxx IDs for all players)"
    )
    parser.add_argument(
        "--check-placeholders",
        action="store_true",
        help="Check NFLverse headshots for placeholder images and use PFR fallback (slower)",
    )

    args = parser.parse_args()

    historical_file = Path(args.historical) if args.historical else HISTORICAL_FILE
    cache_dir = Path(args.cache_dir) if args.cache_dir else NFLVERSE_CACHE
    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse year range
    if "-" in args.nflverse_years:
        start, end = args.nflverse_years.split("-")
        nflverse_years = list(range(int(start), int(end) + 1))
    else:
        nflverse_years = [int(args.nflverse_years)]

    log("=" * 60)
    log("Player ID Bridge Builder")
    log("=" * 60)
    log(f"Historical file: {historical_file}")
    log(f"NFLverse cache: {cache_dir}")
    log(f"NFLverse years: {nflverse_years[0]}-{nflverse_years[-1]}")
    log(f"Skip NFLverse: {args.skip_nflverse}")
    log(f"Check placeholders: {args.check_placeholders}")
    log(f"Apply to historical: {args.apply}")
    log("")

    # Load ALL historical players (not just overlap candidates)
    historical_all = load_all_historical_players(historical_file)

    # Also load overlap candidates for bridging
    historical_overlap = historical_all[historical_all["last_year"] >= 1995].copy()
    log(f"Overlap candidates (last_year >= 1995): {len(historical_overlap):,}")

    if args.skip_nflverse:
        # Just create HIST-xxx IDs for all players
        log("\nSkipping NFLverse matching (--skip-nflverse)")
        bridge_entries = []
        for _, h_row in historical_all.iterrows():
            hist_id = f"HIST-{h_row['kaggle_player_id']}"
            headshot = generate_headshot_url(h_row["player"], source="pfr")
            bridge_entries.append(
                {
                    "kaggle_player_id": h_row["kaggle_player_id"],
                    "nflverse_player_id": None,
                    "unified_player_id": hist_id,
                    "kaggle_name": h_row["player"],
                    "nflverse_name": None,
                    "position": normalize_position(h_row["position"]),
                    "team": h_row["last_team"],
                    "match_layer": 0,
                    "match_type": "historical_only",
                    "confidence": 100.0,
                    "headshot_url": headshot,
                }
            )
        bridge = pd.DataFrame(bridge_entries)
        log(f"Created {len(bridge):,} HIST-xxx IDs")
    else:
        # Try to load NFLverse data for matching
        try:
            nflverse = load_nflverse_players(cache_dir, nflverse_years)

            # Build bridge with matching
            if args.check_placeholders:
                log("\nChecking NFLverse headshots for placeholders (this may take a while)...")
            bridge = build_player_bridge(historical_overlap, nflverse, check_placeholders=args.check_placeholders)

            # Add entries for historical players NOT in overlap candidates
            historical_non_overlap = historical_all[historical_all["last_year"] < 1995]
            log(f"\nAdding IDs for pre-1995 players: {len(historical_non_overlap):,}")

            non_overlap_entries = []
            for _, h_row in historical_non_overlap.iterrows():
                hist_id = f"HIST-{h_row['kaggle_player_id']}"
                headshot = generate_headshot_url(h_row["player"], source="pfr")
                non_overlap_entries.append(
                    {
                        "kaggle_player_id": h_row["kaggle_player_id"],
                        "nflverse_player_id": None,
                        "unified_player_id": hist_id,
                        "kaggle_name": h_row["player"],
                        "nflverse_name": None,
                        "position": normalize_position(h_row["position"]),
                        "team": h_row["last_team"],
                        "match_layer": 0,
                        "match_type": "historical_only",
                        "confidence": 100.0,
                        "headshot_url": headshot,
                    }
                )

            if non_overlap_entries:
                bridge = pd.concat([bridge, pd.DataFrame(non_overlap_entries)], ignore_index=True)

        except Exception as e:
            log(f"\nWarning: Could not load NFLverse data: {e}")
            log("Falling back to HIST-xxx IDs for all players")

            bridge_entries = []
            for _, h_row in historical_all.iterrows():
                hist_id = f"HIST-{h_row['kaggle_player_id']}"
                headshot = generate_headshot_url(h_row["player"], source="pfr")
                bridge_entries.append(
                    {
                        "kaggle_player_id": h_row["kaggle_player_id"],
                        "nflverse_player_id": None,
                        "unified_player_id": hist_id,
                        "kaggle_name": h_row["player"],
                        "nflverse_name": None,
                        "position": normalize_position(h_row["position"]),
                        "team": h_row["last_team"],
                        "match_layer": 0,
                        "match_type": "historical_only",
                        "confidence": 100.0,
                        "headshot_url": headshot,
                    }
                )
            bridge = pd.DataFrame(bridge_entries)

    # Save bridge
    output_path = Path(args.output) if args.output else output_dir / "player_id_bridge.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bridge.to_parquet(output_path, index=False)
    log(f"\nSaved bridge to: {output_path}")

    # Also save CSV for inspection
    csv_path = output_path.with_suffix(".csv")
    bridge.to_csv(csv_path, index=False)
    log(f"Saved CSV to: {csv_path}")

    # Summary
    log(f"\n{'='*60}")
    log("FINAL BRIDGE SUMMARY")
    log(f"{'='*60}")
    log(f"Total players: {len(bridge):,}")
    bridged = bridge["nflverse_player_id"].notna().sum()
    hist_only = bridge["nflverse_player_id"].isna().sum()
    log(f"  Bridged to NFLverse: {bridged:,}")
    log(f"  Historical-only (HIST-xxx): {hist_only:,}")

    # Show sample bridged players
    bridged_df = bridge[bridge["nflverse_player_id"].notna()]
    if len(bridged_df) > 0:
        log("\nSample bridged players:")
        for _, row in bridged_df.head(10).iterrows():
            nfl_name = row["nflverse_name"] if pd.notna(row["nflverse_name"]) else "N/A"
            log(f"  {row['kaggle_name']:25s} -> {nfl_name:25s} [{row['position']}] L{row['match_layer']}")

    # Show sample historical-only players
    hist_df = bridge[bridge["nflverse_player_id"].isna()]
    if len(hist_df) > 0:
        log("\nSample historical-only players:")
        for _, row in hist_df.head(10).iterrows():
            log(f"  {row['kaggle_name']:25s} -> {row['unified_player_id']} [{row['position']}]")

    # Apply to historical data if requested
    if args.apply:
        output_historical = historical_file.parent / "historical_player_unified.parquet"
        apply_bridge_to_historical(historical_file, bridge, output_historical)

    log("\nDone!")


if __name__ == "__main__":
    main()
