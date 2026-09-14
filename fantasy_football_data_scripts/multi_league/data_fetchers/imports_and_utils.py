"""
Shared utilities and configuration for the fantasy football data pipeline.

This module provides a handful of helper functions and configuration values
that are used throughout the various scripts in this repository.  It aims to
centralize common functionality (like name cleaning and manual mapping) and
provide sane defaults for OAuth credentials, league selection and output
directories.  All configuration values can be overridden via environment
variables or by passing explicit arguments to functions.

Key functions exported:

* :func:`clean_name` – normalize a player's name by stripping suffixes and
  fixing accented characters.
* :func:`apply_manual_mapping` – apply custom corrections to player names.
* :func:`get_oauth` – obtain a valid OAuth2 session for Yahoo! Fantasy APIs.
* :func:`get_league_id` – retrieve the appropriate league ID for a given
  season.  Defaults to the most recent league (index ``-1``) but may be
  overridden.

Configuration is resolved in the following order (highest priority first):

#. Function argument overrides.
#. Environment variables.
#. Built‑in defaults.

The following environment variables are recognised:

``YAHOO_OAUTH_FILE``
    Path to the OAuth JSON configuration used to authenticate against the
    Yahoo! Fantasy Sports APIs.  If unspecified the library defaults to
    ``~/.yahoo_oauth.json``.  See the `yahoo_oauth` package documentation
    for details on the expected format.

``YAHOO_LEAGUE_INDEX``
    Zero‑based index used to select a league from the list returned by
    ``yahoo_fantasy_api.Game(...).league_ids(year=...)``.  Negative
    numbers index from the end of the list.  The default is ``-1``, which
    picks the most recent league for the given year.

``FANTASY_OUTPUT_DIR``
    Absolute or relative path to the directory where processed data
    files should be written.  If unspecified, a ``data`` folder next to
    this module will be created and used.  Any intermediate
    directories will be created automatically.

The functions in this module are intentionally lightweight and only import
heavy third‑party libraries when they are needed.  This helps improve
startup time for scripts that only need the name normalisation logic.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from yahoo_oauth import OAuth2
import yahoo_fantasy_api as yfa

__all__ = [
    "DEFAULT_OAUTH_FILE",
    "DEFAULT_LEAGUE_INDEX",
    "DEFAULT_OUTPUT_DIR",
    "clean_name",
    "apply_manual_mapping",
    "get_oauth",
    "get_league_id",
]

###############################################################################
# Configuration
###############################################################################

# Location of the OAuth credential file used for Yahoo! Fantasy API
# DEFAULT: Use context-based OAuth instead of hardcoded path
DEFAULT_OAUTH_FILE: str | None = os.environ.get("YAHOO_OAUTH_FILE")

# Index of the league to select from the league list.  Negative values
# follow Python indexing semantics (e.g. ``-1`` picks the most recent).
try:
    DEFAULT_LEAGUE_INDEX: int = int(os.environ.get("YAHOO_LEAGUE_INDEX", "-1"))
except ValueError:
    DEFAULT_LEAGUE_INDEX = -1

# Root directory for writing output files.  Falls back to a local "data"
# directory relative to this module if not provided.
_output_root_env = os.environ.get("FANTASY_OUTPUT_DIR")
if _output_root_env:
    DEFAULT_OUTPUT_DIR = Path(_output_root_env).expanduser().resolve()
else:
    DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

###############################################################################
# Name normalisation and manual mapping
###############################################################################


def clean_name(name: str) -> str:
    """Return a normalised player name.

    This helper strips out punctuation, generational suffixes (e.g. Jr, Sr,
    II, III) and converts accented characters to their closest ASCII
    equivalents.  It also capitalises each word in the name.  Use this
    function before attempting to merge across disparate data sources.

    Parameters
    ----------
    name: str
        The raw player name to clean.

    Returns
    -------
    str
        The cleaned and capitalised name.
    """
    if not isinstance(name, str):
        return ""
    # Convert accented e/è/é/ê/ë characters to 'e'
    name = re.sub(r"[èéêëÈÉÊË]", "e", name)
    # Remove punctuation and common generational suffixes
    pattern = r"[.\-']|(\bjr\b\.?)|(\bsr\b\.?)|(\bII\b)|(\bIII\b)"
    cleaned_name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()
    # Capitalise each word
    capitalised = " ".join(word.capitalize() for word in cleaned_name.split())
    return capitalised


# Manual overrides for specific player names.  These mappings correct
# inconsistencies between data sources.  All keys are normalised to lower
# case for ease of lookup.
_MANUAL_MAPPING: dict[str, str] = {
    "hollywood brown": "Marquise Brown",
    "will fuller v": "Will Fuller",
    "jeff wilson": "Jeffery Wilson",
    "willie snead iv": "Willie Snead",
    "charles johnson": "Charles D Johnson",
    "kenneth barber": "Peyton Barber",
    "rodney smith": "Rod Smith",
    "bisi johnson": "Olabisi Johnson",
    "chris herndon": "Christopher Herndon",
    "scotty miller": "Scott Miller",
    "trenton richardson": "Trent Richardson",
    # Edge case: Taysom Hill (Yahoo: TE, NFL: QB) - normalize to standard name
    "taysom hill": "Taysom Hill",
}


def apply_manual_mapping(name: str) -> str:
    """Apply a manual mapping to a player name if one exists.

    Parameters
    ----------
    name: str
        The name to potentially replace with a corrected version.

    Returns
    -------
    str
        The mapped name if found, otherwise the original name.
    """
    try:
        key = name.lower()
    except Exception:
        return name
    return _MANUAL_MAPPING.get(key, name)


###############################################################################
# OAuth and league selection helpers
###############################################################################


def load_league_settings(year: int, league_key: str, settings_dir: Path) -> dict[str, Any]:
    """
    Load comprehensive league settings from unified JSON file.

    This replaces the old separate files (dst_scoring, full_scoring, roster_positions)
    with one unified settings loader.

    Parameters
    ----------
    year: int
        Season year
    league_key: str
        Yahoo league key (e.g., "449.l.198278")
    settings_dir: Path
        Directory containing league settings files

    Returns
    -------
    Dict[str, Any]
        Dictionary containing:
        - year: int
        - league_key: str
        - league_metadata: dict with num_teams, start_week, end_week
        - scoring_rules: list of scoring rule dicts
        - dst_scoring: dict of DST scoring values
        - roster_positions: list of roster position dicts

    Raises
    ------
    FileNotFoundError
        If settings file doesn't exist for the specified year/league
    """
    import json

    safe_lk = league_key.replace(".", "_")
    settings_file = settings_dir / f"league_settings_{year}_{safe_lk}.json"

    if not settings_file.exists():
        raise FileNotFoundError(
            f"League settings not found: {settings_file}\n"
            f"Run yahoo_fantasy_data.py for year {year} to generate settings."
        )

    with settings_file.open("r", encoding="utf-8") as f:
        settings = json.load(f)

    return settings


def get_oauth(oauth_file: str | None = None, oauth_credentials: dict[str, Any] | None = None) -> OAuth2:
    """Return a valid OAuth2 session for the Yahoo! Fantasy API.

    Supports both file-based and embedded OAuth credentials for multi-league contexts.

    Parameters
    ----------
    oauth_file: Optional[str]
        Path to the OAuth credential JSON file.
    oauth_credentials: Optional[Dict[str, Any]]
        Embedded OAuth credentials dict (access_token, consumer_key, consumer_secret, etc.)

    Returns
    -------
    OAuth2
        A ready‑to‑use OAuth session.

    Raises
    ------
    ValueError
        If neither oauth_file nor oauth_credentials is provided.
    """
    # Try embedded credentials first (for multi-league support)
    if oauth_credentials:
        import tempfile
        import json as _json

        # Write credentials to temp file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            _json.dump(oauth_credentials, f)
            temp_path = f.name

        try:
            oauth = OAuth2(None, None, from_file=str(temp_path))
            if not oauth.token_is_valid():
                oauth.refresh_access_token()
            return oauth
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_path)
            except Exception:  # noqa: broad-except
                pass
    # Fall back to file-based OAuth
    path = oauth_file or DEFAULT_OAUTH_FILE
    if not path:
        raise ValueError("Either oauth_file or oauth_credentials must be provided")

    # Construct the OAuth2 session from the provided file
    oauth = OAuth2(None, None, from_file=str(path))
    if not oauth.token_is_valid():
        oauth.refresh_access_token()
    return oauth


def get_league_id(
    year: int,
    *,
    league_index: int | None = None,
    oauth: OAuth2 | None = None,
    game_code: str = "nfl",
    ensure: bool = True,
) -> str:
    """Retrieve a league ID for a given season.

    Yahoo! Fantasy managers may belong to multiple leagues in a given season.
    This helper abstracts away the process of choosing which league to use.

    Parameters
    ----------
    year: int
        The season year for which to fetch league identifiers.
    league_index: Optional[int], optional
        The index of the league to select from the list returned by
        ``yahoo_fantasy_api.Game(...).league_ids()``.  If ``None``
        (default), the function uses ``DEFAULT_LEAGUE_INDEX``.  Negative
        numbers count from the end of the list.
    oauth: Optional[OAuth2], optional
        An OAuth2 session to reuse.  If ``None`` one will be created
        automatically via :func:`get_oauth`.
    game_code: str, optional
        The Yahoo! Fantasy game code.  "nfl" is the default.  See the
        documentation for ``yahoo_fantasy_api`` for other valid values.
    ensure: bool, optional
        If ``True`` (default) this function will raise a :class:`ValueError`
        if no leagues are returned.  If ``False``, the function returns
        ``None`` when no leagues exist.

    Returns
    -------
    str
        The league identifier.

    Raises
    ------
    ValueError
        If ``ensure`` is ``True`` and no league IDs are found.
    """
    session = oauth or get_oauth(oauth_file=None)
    game = yfa.Game(session, game_code)
    league_ids = game.league_ids(year=year)
    if not league_ids:
        if ensure:
            raise ValueError(f"No league IDs found for year {year} (game_code='{game_code}').")
        return None
    idx = league_index if league_index is not None else DEFAULT_LEAGUE_INDEX
    # Normalise negative indices
    if idx < 0:
        idx = len(league_ids) + idx
    # Clamp to valid range
    idx = max(0, min(idx, len(league_ids) - 1))
    return league_ids[idx]
