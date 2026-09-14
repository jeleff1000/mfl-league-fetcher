#!/usr/bin/env python3
"""
Yahoo Player Cache - Local disk cache for Yahoo-to-NFL player mappings.

Provides fast O(1) lookups for yahoo_player_id -> NFL_player_id mappings
with automatic refresh from MotherDuck when cache is stale (>24h old).

This cache eliminates repeated network calls to MotherDuck for every import,
while keeping data fresh with a 24-hour TTL.

Usage:
    cache = YahooPlayerCache(cache_dir=Path("./cache"))

    # Initialize (loads from disk or MotherDuck if needed)
    cache.refresh_if_stale(token)

    # Fast lookups
    nfl_id = cache.get_nfl_player_id("24791")  # -> "00-0027942"
    name = cache.get_player_name("24791")       # -> "A.J. Green"

    # Reverse lookup
    yahoo_id = cache.lookup_by_name("Patrick Mahomes", "QB")

    # Save new mappings discovered during import
    cache.save_new_mappings(new_mappings_df)

Author: Fantasy Football Analytics Pipeline
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class YahooPlayerCache:
    """
    Local disk cache for Yahoo-to-NFL player ID mappings.

    The mapping data is sourced from MotherDuck ___ops.public.yahoo_nfl_player_map.
    This cache stores it locally with a 24-hour TTL to avoid repeated network calls.

    Key features:
    - O(1) lookup: yahoo_player_id -> NFL_player_id
    - Reverse lookup: (name, position) -> yahoo_player_id
    - Automatic refresh from MotherDuck when stale
    - Persistence between runs
    - Thread-safe loading

    Attributes:
        CACHE_TTL_HOURS: Time-to-live in hours before cache is considered stale
        CACHE_FILENAME: Name of the main cache file
        METADATA_FILENAME: Name of the metadata file
    """

    CACHE_TTL_HOURS = 24
    CACHE_FILENAME = "yahoo_players.json"
    METADATA_FILENAME = "yahoo_players_metadata.json"

    def __init__(self, cache_dir: Path):
        """
        Initialize the Yahoo player cache.

        Args:
            cache_dir: Directory to store cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_file = self.cache_dir / self.CACHE_FILENAME
        self.metadata_file = self.cache_dir / self.METADATA_FILENAME

        # In-memory cache: yahoo_player_id -> player info dict
        self._players: dict[str, dict[str, Any]] = {}
        # Reverse index: normalized_name -> list of yahoo_player_ids
        self._name_index: dict[str, list[str]] = {}
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
            logger.warning(f"Failed to load Yahoo cache metadata: {e}")
            return None

    def _save_metadata(self):
        """Save cache metadata."""
        self._ensure_cache_dir()

        metadata = {
            "last_refresh": datetime.now().isoformat(),
            "player_count": len(self._players),
            "cache_version": "1.0",
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
            logger.info(f"Loaded {len(self._players):,} Yahoo player mappings from cache")
            return True

        except Exception as e:
            logger.warning(f"Failed to load Yahoo player cache from disk: {e}")
            return False

    def _save_to_disk(self):
        """Save player data to disk."""
        self._ensure_cache_dir()

        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self._players, f)

        self._save_metadata()
        logger.info(f"Saved {len(self._players):,} Yahoo player mappings to cache")

    def _load_from_motherduck(self, token: str) -> bool:
        """
        Load player mappings from MotherDuck.

        Args:
            token: MotherDuck authentication token

        Returns:
            True if load succeeded, False otherwise
        """
        try:
            from multi_league.core.db_reader import get_reader

            # Backend-aware: Fly path uses DATABASE_READ_TOKEN, MotherDuck path
            # needs MOTHERDUCK_TOKEN. Only fail-closed on the latter.
            backend = os.environ.get("DATABASE_BACKEND", "fly")
            if backend != "fly":
                token = token or os.environ.get("MOTHERDUCK_TOKEN")
                if not token:
                    logger.warning("No MotherDuck token available, cannot refresh cache")
                    return False

            reader = get_reader()

            # Check if table exists (FlyReader requires explicit database; legacy
            # MotherDuck path was implicit so callers omitted it — pass ___ops).
            tables = reader.query(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'yahoo_nfl_player_map'
                """,
                database="___ops",
            )

            if not tables:
                logger.warning("yahoo_nfl_player_map table does not exist in MotherDuck")
                return False

            # Fetch all mappings
            df = reader.query_df(
                """
                SELECT
                    yahoo_player_id,
                    NFL_player_id,
                    yahoo_name,
                    nfl_name,
                    position,
                    headshot_url,
                    gsis_id,
                    match_layer,
                    match_confidence,
                    is_manual_override
                FROM public.yahoo_nfl_player_map
                """,
                database="___ops",
            )

            # Convert to player dict format
            self._players = {}
            for _, row in df.iterrows():
                yahoo_id = str(row["yahoo_player_id"])
                self._players[yahoo_id] = {
                    "yahoo_player_id": yahoo_id,
                    "NFL_player_id": row["NFL_player_id"],
                    "yahoo_name": row.get("yahoo_name", ""),
                    "nfl_name": row.get("nfl_name", ""),
                    "position": row.get("position", ""),
                    "headshot_url": row.get("headshot_url"),
                    "gsis_id": row.get("gsis_id"),
                    "match_layer": row.get("match_layer"),
                    "match_confidence": row.get("match_confidence"),
                    "is_manual_override": row.get("is_manual_override", False),
                }

            logger.info(f"Loaded {len(self._players):,} mappings from MotherDuck")
            return True

        except Exception as e:
            logger.error(f"Failed to load from MotherDuck: {e}")
            return False

    def refresh_if_stale(self, token: str = None) -> bool:
        """
        Load player data from MotherDuck if cache is stale (>24h old).

        Args:
            token: MotherDuck authentication token

        Returns:
            True if cache was refreshed, False if using existing cache
        """
        # Try to load from disk first
        if not self._loaded:
            self._load_from_disk()

        # Check if refresh is needed
        if self._loaded and not self._is_stale():
            logger.debug("Yahoo player cache is fresh, skipping refresh")
            return False

        # Download fresh data from MotherDuck
        logger.info("Refreshing Yahoo player cache from MotherDuck...")
        try:
            if not self._load_from_motherduck(token):
                # Fall back to cached data if available
                if self._loaded:
                    logger.warning("Using stale cache data")
                    return False
                return False

            self._build_name_index()
            self._save_to_disk()
            self._loaded = True
            self._last_refresh = datetime.now()

            return True

        except Exception as e:
            logger.error(f"Failed to refresh Yahoo player cache: {e}")

            # Fall back to cached data if available
            if self._loaded:
                logger.warning("Using stale cache data")
                return False

            raise

    def force_refresh(self, token: str = None) -> bool:
        """
        Force a cache refresh regardless of staleness.

        Args:
            token: MotherDuck authentication token

        Returns:
            True if refresh succeeded
        """
        logger.info("Force refreshing Yahoo player cache...")
        self._players = {}
        self._name_index = {}
        self._loaded = False

        return self.refresh_if_stale(token)

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a player name for lookup.

        - Lowercase
        - Remove accents (e.g., e -> e)
        - Remove punctuation
        - Remove suffixes (Jr, Sr, III, IV, V, II)
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

        # Remove suffixes (process longer ones first)
        suffixes = [" iii", " iv", " ii", " jr", " sr", " v", " jr.", " sr."]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

        # Remove punctuation (keep spaces)
        name = re.sub(r"[^\w\s]", "", name)

        # Collapse whitespace
        name = " ".join(name.split())

        return name

    def _build_name_index(self):
        """Build normalized name -> yahoo_player_id index for reverse lookups."""
        self._name_index = {}

        for yahoo_id, player in self._players.items():
            # Use yahoo_name for indexing (what Yahoo calls the player)
            name = player.get("yahoo_name", "")
            if not name:
                name = player.get("nfl_name", "")

            if not name:
                continue

            normalized = self._normalize_name(name)
            if normalized not in self._name_index:
                self._name_index[normalized] = []
            self._name_index[normalized].append(yahoo_id)

        logger.debug(f"Built Yahoo name index with {len(self._name_index)} unique names")

    # =========================================================================
    # LOOKUP METHODS
    # =========================================================================

    def get_player(self, yahoo_player_id: str) -> dict[str, Any] | None:
        """
        Get full player info by Yahoo player ID.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            Player dict with all fields, or None if not found
        """
        return self._players.get(str(yahoo_player_id))

    def get_nfl_player_id(self, yahoo_player_id: str) -> str | None:
        """
        Get NFL player ID for a Yahoo player ID.

        This is the primary lookup method - O(1) dict access.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            NFL_player_id string, or None if not in cache
        """
        player = self.get_player(yahoo_player_id)
        if not player:
            return None
        return player.get("NFL_player_id")

    def get_gsis_id(self, yahoo_player_id: str) -> str | None:
        """
        Get GSIS ID for a Yahoo player ID (if available).

        GSIS IDs enable direct matching with NFLverse data.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            GSIS ID string, or None if not available
        """
        player = self.get_player(yahoo_player_id)
        if not player:
            return None
        return player.get("gsis_id")

    def get_player_name(self, yahoo_player_id: str) -> str:
        """
        Get display name for a Yahoo player ID.

        Returns yahoo_name if available, falls back to nfl_name.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            Player name, or "Unknown" if not found
        """
        player = self.get_player(yahoo_player_id)
        if not player:
            return "Unknown"

        return player.get("yahoo_name") or player.get("nfl_name") or "Unknown"

    def get_player_position(self, yahoo_player_id: str) -> str:
        """
        Get position for a Yahoo player ID.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            Position string, or "Unknown" if not found
        """
        player = self.get_player(yahoo_player_id)
        if not player:
            return "Unknown"
        return player.get("position", "Unknown")

    def get_headshot_url(self, yahoo_player_id: str) -> str | None:
        """
        Get headshot URL for a Yahoo player ID.

        Args:
            yahoo_player_id: Yahoo player ID

        Returns:
            Headshot URL, or None if not available
        """
        player = self.get_player(yahoo_player_id)
        if not player:
            return None
        return player.get("headshot_url")

    def lookup_by_name(self, name: str, position: str | None = None) -> str | None:
        """
        Reverse lookup: find yahoo_player_id from name.

        If multiple players match, uses position to disambiguate.

        Args:
            name: Player name (will be normalized)
            position: Optional position filter (QB, RB, etc.)

        Returns:
            yahoo_player_id if found and unique, None otherwise
        """
        normalized = self._normalize_name(name)

        candidates = self._name_index.get(normalized, [])
        if not candidates:
            return None

        # Filter by position if provided
        if position:
            position = position.upper()
            candidates = [
                yid for yid in candidates if self._players.get(yid, {}).get("position", "").upper() == position
            ]

        # Return if unique match
        if len(candidates) == 1:
            return candidates[0]

        # Multiple matches - return None (ambiguous)
        if len(candidates) > 1:
            logger.debug(f"Ambiguous Yahoo name lookup for '{name}': {len(candidates)} matches")
            return None

        return None

    def lookup_all_by_name(self, name: str) -> list[str]:
        """
        Find all yahoo_player_ids matching a name.

        Args:
            name: Player name (will be normalized)

        Returns:
            List of yahoo_player_ids (may be empty)
        """
        normalized = self._normalize_name(name)
        return self._name_index.get(normalized, [])

    # =========================================================================
    # BULK OPERATIONS
    # =========================================================================

    def get_nfl_id_map(self) -> dict[str, str]:
        """
        Get full mapping dict: yahoo_player_id -> NFL_player_id.

        Useful for bulk operations with pandas .map().

        Returns:
            Dict mapping yahoo_player_id to NFL_player_id
        """
        return {
            yahoo_id: player.get("NFL_player_id")
            for yahoo_id, player in self._players.items()
            if player.get("NFL_player_id")
        }

    def get_unmapped_ids(self, yahoo_ids: list[str]) -> list[str]:
        """
        Return yahoo_ids that are NOT in the cache.

        Args:
            yahoo_ids: List of yahoo_player_ids to check

        Returns:
            List of yahoo_ids not in cache
        """
        yahoo_ids_set = {str(yid) for yid in yahoo_ids}
        cached_ids = set(self._players.keys())
        return list(yahoo_ids_set - cached_ids)

    def save_new_mappings(self, mappings: list[tuple[str, str, int, float, str, str, str]], token: str = None) -> int:
        """
        Save new mappings to both local cache and MotherDuck.

        Args:
            mappings: List of (yahoo_player_id, NFL_player_id, match_layer,
                      confidence, yahoo_name, nfl_name, position) tuples
            token: MotherDuck token

        Returns:
            Number of new mappings saved
        """
        if not mappings:
            return 0

        # Update local cache
        new_count = 0
        for mapping in mappings:
            yahoo_id = str(mapping[0])
            if yahoo_id not in self._players:
                self._players[yahoo_id] = {
                    "yahoo_player_id": yahoo_id,
                    "NFL_player_id": mapping[1],
                    "match_layer": mapping[2],
                    "match_confidence": mapping[3],
                    "yahoo_name": mapping[4],
                    "nfl_name": mapping[5],
                    "position": mapping[6],
                }
                new_count += 1

        # Rebuild name index
        if new_count > 0:
            self._build_name_index()
            self._save_to_disk()

        # Try to persist to MotherDuck
        try:
            from .player_id_cache import save_yahoo_nfl_mapping

            save_yahoo_nfl_mapping(mappings, token)
        except Exception as e:
            logger.warning(f"Failed to persist new mappings to MotherDuck: {e}")

        return new_count

    # =========================================================================
    # STATISTICS
    # =========================================================================

    @property
    def player_count(self) -> int:
        """Get number of players in cache."""
        return len(self._players)

    @property
    def is_loaded(self) -> bool:
        """Check if cache has been loaded."""
        return self._loaded

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict with cache statistics
        """
        if not self._loaded:
            return {"loaded": False}

        # Count by match layer
        layer_counts = {}
        gsis_count = 0
        for player in self._players.values():
            layer = player.get("match_layer")
            if layer is not None:
                layer_counts[layer] = layer_counts.get(layer, 0) + 1
            if player.get("gsis_id"):
                gsis_count += 1

        return {
            "loaded": True,
            "total_mappings": len(self._players),
            "with_gsis_id": gsis_count,
            "by_layer": layer_counts,
            "name_index_size": len(self._name_index),
        }

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

        logger.info("Yahoo player cache cleared")


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_default_cache: YahooPlayerCache | None = None


def get_yahoo_cache(cache_dir: Path = None) -> YahooPlayerCache:
    """
    Get a shared YahooPlayerCache instance.

    Args:
        cache_dir: Optional cache directory. If not provided, uses default.

    Returns:
        YahooPlayerCache instance
    """
    global _default_cache

    if _default_cache is None:
        if cache_dir is None:
            # Default to a cache directory in the user's data folder
            cache_dir = Path(__file__).parent.parent.parent.parent / "cache"
        _default_cache = YahooPlayerCache(cache_dir)

    return _default_cache


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Yahoo Player Cache Manager")
    parser.add_argument("--refresh", action="store_true", help="Force refresh cache from MotherDuck")
    parser.add_argument("--stats", action="store_true", help="Show cache statistics")
    parser.add_argument("--lookup", type=str, help="Lookup NFL ID for a Yahoo player ID")
    parser.add_argument("--cache-dir", type=str, default="./cache", help="Cache directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    cache = YahooPlayerCache(Path(args.cache_dir))
    token = os.environ.get("MOTHERDUCK_TOKEN")

    if args.refresh:
        cache.force_refresh(token)
        print(f"Cache refreshed: {cache.player_count:,} mappings")

    elif args.lookup:
        cache.refresh_if_stale(token)
        nfl_id = cache.get_nfl_player_id(args.lookup)
        if nfl_id:
            player = cache.get_player(args.lookup)
            print(f"Yahoo ID {args.lookup} -> NFL ID {nfl_id}")
            print(f"  Name: {player.get('yahoo_name', 'Unknown')}")
            print(f"  Position: {player.get('position', 'Unknown')}")
            if player.get("gsis_id"):
                print(f"  GSIS ID: {player.get('gsis_id')}")
        else:
            print(f"Yahoo ID {args.lookup} not found in cache")

    elif args.stats:
        cache.refresh_if_stale(token)
        stats = cache.get_stats()
        print("\n=== Yahoo Player Cache Statistics ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    else:
        # Default: show count
        cache.refresh_if_stale(token)
        print(f"Yahoo cache contains {cache.player_count:,} mappings")
