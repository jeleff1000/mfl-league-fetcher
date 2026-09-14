"""
Sleeper Player Cache - Manages the ~5MB player database.

Sleeper returns numeric player_id (e.g., "4046") vs Yahoo's "461.p.33376" format.

This cache provides:
- Fast lookup: player_id -> {name, position, team}
- Reverse lookup: (name, position) -> player_id
- Persistence between runs (24-hour TTL)
- Lazy loading (only downloads when needed)
"""

import json
import logging
import unicodedata
import re
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SleeperPlayerCache:
    """
    Cache for Sleeper player database.

    The Sleeper player database is ~5MB and contains all NFL players.
    This cache stores it locally with a 24-hour TTL to avoid repeated downloads.

    Usage:
        cache = SleeperPlayerCache(cache_dir=Path("./cache"))

        # Initialize (downloads if needed)
        cache.refresh_if_stale(client)

        # Lookups
        name = cache.get_player_name("4046")  # -> "Patrick Mahomes"
        position = cache.get_player_position("4046")  # -> "QB"
        team = cache.get_player_team("4046")  # -> "KC"

        # Reverse lookup
        player_id = cache.lookup_by_name("Patrick Mahomes", "QB")  # -> "4046"
    """

    CACHE_TTL_HOURS = 24
    CACHE_FILENAME = "sleeper_players.json"
    METADATA_FILENAME = "sleeper_players_metadata.json"

    def __init__(self, cache_dir: Path):
        """
        Initialize the player cache.

        Args:
            cache_dir: Directory to store cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_file = self.cache_dir / self.CACHE_FILENAME
        self.metadata_file = self.cache_dir / self.METADATA_FILENAME

        self._players: dict[str, dict[str, Any]] = {}
        self._name_index: dict[str, list[str]] = {}  # normalized_name -> list of player_ids
        self._loaded = False
        self._last_refresh: datetime | None = None

    def _ensure_cache_dir(self):
        """Create cache directory if it doesn't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _load_metadata(self) -> dict[str, Any] | None:
        """Load cache metadata (last refresh time, etc.)."""
        if not self.metadata_file.exists():
            return None

        try:
            with open(self.metadata_file, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load cache metadata: {e}")
            return None

    def _save_metadata(self):
        """Save cache metadata."""
        self._ensure_cache_dir()

        metadata = {
            "last_refresh": datetime.now().isoformat(),
            "player_count": len(self._players),
        }

        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    def _is_stale(self) -> bool:
        """Check if cache is stale (older than TTL)."""
        metadata = self._load_metadata()
        if metadata is None:
            return True

        last_refresh_str = metadata.get("last_refresh")
        if not last_refresh_str:
            return True

        try:
            last_refresh = datetime.fromisoformat(last_refresh_str)
            age = datetime.now() - last_refresh
            return age > timedelta(hours=self.CACHE_TTL_HOURS)
        except Exception:
            return True

    def _load_from_disk(self) -> bool:
        """Load cached player data from disk."""
        if not self.cache_file.exists():
            return False

        try:
            with open(self.cache_file, encoding="utf-8") as f:
                self._players = json.load(f)

            self._build_name_index()
            self._loaded = True
            logger.info(f"Loaded {len(self._players)} players from cache")
            return True

        except Exception as e:
            logger.warning(f"Failed to load player cache from disk: {e}")
            return False

    def _save_to_disk(self):
        """Save player data to disk."""
        self._ensure_cache_dir()

        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self._players, f)

        self._save_metadata()
        logger.info(f"Saved {len(self._players)} players to cache")

    def refresh_if_stale(self, client: "SleeperAPIClient") -> bool:
        """
        Download player database if stale (>24h old).

        Args:
            client: SleeperAPIClient instance

        Returns:
            True if cache was refreshed, False if using existing cache
        """
        # Try to load from disk first
        if not self._loaded:
            self._load_from_disk()

        # Check if refresh is needed
        if self._loaded and not self._is_stale():
            logger.debug("Player cache is fresh, skipping refresh")
            return False

        # Download fresh data
        logger.info("Downloading Sleeper player database (~5MB)...")
        try:
            self._players = client.get_all_players("nfl")

            if not self._players:
                logger.error("Failed to download player database (empty response)")
                return False

            self._build_name_index()
            self._save_to_disk()
            self._loaded = True
            self._last_refresh = datetime.now()

            logger.info(f"Downloaded {len(self._players)} players")
            return True

        except Exception as e:
            logger.error(f"Failed to download player database: {e}")

            # Fall back to cached data if available
            if self._loaded:
                logger.warning("Using stale cache data")
                return False

            raise

    def force_refresh(self, client: "SleeperAPIClient") -> bool:
        """
        Force a cache refresh regardless of staleness.

        Args:
            client: SleeperAPIClient instance

        Returns:
            True if refresh succeeded
        """
        logger.info("Force refreshing player cache...")
        self._players = {}
        self._name_index = {}
        self._loaded = False

        return self.refresh_if_stale(client)

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a player name for lookup.

        - Lowercase
        - Remove accents (é -> e)
        - Remove punctuation
        - Remove suffixes (Jr, Sr, III, IV, V)
        - Collapse whitespace

        Args:
            name: Player name

        Returns:
            Normalized name
        """
        if not name:
            return ""

        # Lowercase
        name = name.lower()

        # Remove accents
        name = unicodedata.normalize("NFKD", name)
        name = "".join(c for c in name if not unicodedata.combining(c))

        # Remove suffixes
        suffixes = [" jr", " sr", " iii", " iv", " v", " ii", " jr.", " sr."]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

        # Remove punctuation (keep spaces)
        name = re.sub(r"[^\w\s]", "", name)

        # Collapse whitespace
        name = " ".join(name.split())

        return name

    def _build_name_index(self):
        """Build normalized name -> player_id index for reverse lookups."""
        self._name_index = {}

        for player_id, player in self._players.items():
            full_name = player.get("full_name", "")
            if not full_name:
                first = player.get("first_name", "")
                last = player.get("last_name", "")
                full_name = f"{first} {last}".strip()

            if not full_name:
                continue

            normalized = self._normalize_name(full_name)
            if normalized not in self._name_index:
                self._name_index[normalized] = []
            self._name_index[normalized].append(player_id)

        logger.debug(f"Built name index with {len(self._name_index)} unique names")

    def get_player(self, player_id: str) -> dict[str, Any] | None:
        """
        Get full player info by ID.

        Args:
            player_id: Sleeper player_id

        Returns:
            Player dict with all fields, or None if not found
        """
        return self._players.get(str(player_id))

    # Valid NFL team abbreviations for DST detection
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

    # Team abbreviation to full name mapping
    TEAM_ABBREV_TO_NAME = {
        "ARI": "Arizona Cardinals",
        "ATL": "Atlanta Falcons",
        "BAL": "Baltimore Ravens",
        "BUF": "Buffalo Bills",
        "CAR": "Carolina Panthers",
        "CHI": "Chicago Bears",
        "CIN": "Cincinnati Bengals",
        "CLE": "Cleveland Browns",
        "DAL": "Dallas Cowboys",
        "DEN": "Denver Broncos",
        "DET": "Detroit Lions",
        "GB": "Green Bay Packers",
        "HOU": "Houston Texans",
        "IND": "Indianapolis Colts",
        "JAX": "Jacksonville Jaguars",
        "JAC": "Jacksonville Jaguars",
        "KC": "Kansas City Chiefs",
        "LA": "Los Angeles Rams",
        "LAC": "Los Angeles Chargers",
        "LAR": "Los Angeles Rams",
        "LV": "Las Vegas Raiders",
        "MIA": "Miami Dolphins",
        "MIN": "Minnesota Vikings",
        "NE": "New England Patriots",
        "NO": "New Orleans Saints",
        "NYG": "New York Giants",
        "NYJ": "New York Jets",
        "OAK": "Oakland Raiders",
        "PHI": "Philadelphia Eagles",
        "PIT": "Pittsburgh Steelers",
        "SD": "San Diego Chargers",
        "SEA": "Seattle Seahawks",
        "SF": "San Francisco 49ers",
        "STL": "St. Louis Rams",
        "TB": "Tampa Bay Buccaneers",
        "TEN": "Tennessee Titans",
        "WAS": "Washington Commanders",
        "WSH": "Washington Commanders",
    }

    # NFL team name to short name mapping for DST normalization
    NFL_TEAM_SHORT_NAMES = {
        "Arizona Cardinals": "Cardinals",
        "Atlanta Falcons": "Falcons",
        "Baltimore Ravens": "Ravens",
        "Buffalo Bills": "Bills",
        "Carolina Panthers": "Panthers",
        "Chicago Bears": "Bears",
        "Cincinnati Bengals": "Bengals",
        "Cleveland Browns": "Browns",
        "Dallas Cowboys": "Cowboys",
        "Denver Broncos": "Broncos",
        "Detroit Lions": "Lions",
        "Green Bay Packers": "Packers",
        "Houston Texans": "Texans",
        "Indianapolis Colts": "Colts",
        "Jacksonville Jaguars": "Jaguars",
        "Kansas City Chiefs": "Chiefs",
        "Las Vegas Raiders": "Raiders",
        "Los Angeles Chargers": "Chargers",
        "Los Angeles Rams": "Rams",
        "Miami Dolphins": "Dolphins",
        "Minnesota Vikings": "Vikings",
        "New England Patriots": "Patriots",
        "New Orleans Saints": "Saints",
        "New York Giants": "Giants",
        "New York Jets": "Jets",
        "Philadelphia Eagles": "Eagles",
        "Pittsburgh Steelers": "Steelers",
        "San Francisco 49ers": "49ers",
        "Seattle Seahawks": "Seahawks",
        "Tampa Bay Buccaneers": "Buccaneers",
        "Tennessee Titans": "Titans",
        "Washington Commanders": "Commanders",
        # Legacy names
        "Oakland Raiders": "Raiders",
        "San Diego Chargers": "Chargers",
        "St. Louis Rams": "Rams",
        "Washington Redskins": "Commanders",
        "Washington Football Team": "Commanders",
    }

    def get_player_name(self, player_id: str) -> str:
        """
        Get display name for player ID.

        For DEF positions, normalizes team names to "Team DST" format
        (e.g., "Los Angeles Chargers" -> "Chargers DST").

        Args:
            player_id: Sleeper player_id

        Returns:
            Player full name, or "Unknown" if not found
        """
        player = self.get_player(player_id)
        if not player:
            return "Unknown"

        position = player.get("position", "")
        full_name = (
            player.get("full_name", "") or f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
        )

        # Normalize DST names to match Yahoo format (e.g., "Chargers DST")
        if position == "DEF" and full_name:
            short_name = self.NFL_TEAM_SHORT_NAMES.get(full_name)
            if short_name:
                return f"{short_name} DST"
            # Fallback: try to extract team name from end
            # e.g., "Los Angeles Chargers" -> "Chargers DST"
            parts = full_name.split()
            if parts:
                return f"{parts[-1]} DST"

        if full_name:
            return full_name

        first = player.get("first_name", "")
        last = player.get("last_name", "")
        return f"{first} {last}".strip() or "Unknown"

    def get_player_position(self, player_id: str) -> str:
        """
        Get primary position for player ID.

        Args:
            player_id: Sleeper player_id

        Returns:
            Position string (QB, RB, WR, TE, K, DEF), or "Unknown"
        """
        player = self.get_player(player_id)
        if not player:
            return "Unknown"

        return player.get("position", "Unknown")

    def get_player_team(self, player_id: str) -> str:
        """
        Get current NFL team for player ID.

        Args:
            player_id: Sleeper player_id

        Returns:
            Team abbreviation (KC, SF, etc.), or "FA" for free agents
        """
        player = self.get_player(player_id)
        if not player:
            return "Unknown"

        return player.get("team") or "FA"

    def get_player_gsis_id(self, player_id: str) -> str | None:
        """
        Get NFL GSIS ID for player (if available).

        GSIS IDs enable direct matching with NFLverse data.

        Args:
            player_id: Sleeper player_id

        Returns:
            GSIS ID string, or None if not available
        """
        player = self.get_player(player_id)
        if not player:
            return None

        return player.get("gsis_id")

    def lookup_by_name(self, name: str, position: str | None = None, team: str | None = None) -> str | None:
        """
        Reverse lookup: find player_id from name.

        If multiple players match, uses position and team to disambiguate.

        Args:
            name: Player name (will be normalized)
            position: Optional position filter (QB, RB, etc.)
            team: Optional team filter (KC, SF, etc.)

        Returns:
            player_id if found and unique, None otherwise
        """
        normalized = self._normalize_name(name)

        candidates = self._name_index.get(normalized, [])
        if not candidates:
            return None

        # Filter by position if provided
        if position:
            position = position.upper()
            candidates = [
                pid for pid in candidates if self._players.get(pid, {}).get("position", "").upper() == position
            ]

        # Filter by team if provided
        if team:
            team = team.upper()
            candidates = [pid for pid in candidates if (self._players.get(pid, {}).get("team") or "").upper() == team]

        # Return if unique match
        if len(candidates) == 1:
            return candidates[0]

        # Multiple matches - return None (ambiguous)
        if len(candidates) > 1:
            logger.debug(f"Ambiguous name lookup for '{name}': {len(candidates)} matches")
            return None

        return None

    def lookup_all_by_name(self, name: str) -> list[str]:
        """
        Find all player_ids matching a name.

        Args:
            name: Player name (will be normalized)

        Returns:
            List of player_ids (may be empty)
        """
        normalized = self._normalize_name(name)
        return self._name_index.get(normalized, [])

    def get_all_players_at_position(self, position: str) -> list[str]:
        """
        Get all player_ids at a specific position.

        Args:
            position: Position (QB, RB, WR, TE, K, DEF)

        Returns:
            List of player_ids
        """
        position = position.upper()
        return [pid for pid, player in self._players.items() if player.get("position", "").upper() == position]

    def search_players(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        Search for players by name prefix.

        Args:
            query: Search query (name prefix)
            limit: Maximum results to return

        Returns:
            List of player dicts matching the query
        """
        query_normalized = self._normalize_name(query)
        if not query_normalized:
            return []

        results = []
        for name, player_ids in self._name_index.items():
            if name.startswith(query_normalized):
                for pid in player_ids:
                    player = self._players.get(pid)
                    if player:
                        results.append(player)
                    if len(results) >= limit:
                        return results

        return results

    @property
    def player_count(self) -> int:
        """Get number of players in cache."""
        return len(self._players)

    @property
    def is_loaded(self) -> bool:
        """Check if cache has been loaded."""
        return self._loaded

    def clear(self):
        """Clear the cache (both memory and disk)."""
        self._players = {}
        self._name_index = {}
        self._loaded = False
        self._last_refresh = None

        if self.cache_file.exists():
            self.cache_file.unlink()
        if self.metadata_file.exists():
            self.metadata_file.unlink()

        logger.info("Player cache cleared")
