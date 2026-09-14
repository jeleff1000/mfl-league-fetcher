"""
NFL Player ID Mapping

Unified interface for resolving platform-specific player IDs to NFL_player_id.
Uses ___ops.nfl_historical.player_bio as the single source of truth.

Legacy mapping tables (yahoo_nfl_player_map, sleeper_nfl_player_map,
espn_nfl_player_map) are deprecated. Their data has been backfilled into
player_bio via migrations/backfill_player_bio_platform_ids.sql.

The NFL_player_id (GSIS ID format: 00-XXXXXXX) is the universal join key
for the NFL super table (___ops.nfl_historical.nfl_player_stats_all).

Usage:
    from data_fetchers.shared import (
        get_yahoo_to_nfl_map,
        get_sleeper_to_nfl_map,
        resolve_nfl_player_id,
    )

    # Get full mapping dicts
    yahoo_map = get_yahoo_to_nfl_map()  # {yahoo_id: nfl_id}
    sleeper_map = get_sleeper_to_nfl_map()  # {sleeper_id: nfl_id}

    # Resolve single ID
    nfl_id = resolve_nfl_player_id('12345', 'yahoo')
"""

import logging
import os

logger = logging.getLogger(__name__)

# Module-level caches
_YAHOO_MAP: dict[str, str] | None = None
_SLEEPER_MAP: dict[str, str] | None = None
_ESPN_MAP: dict[str, str] | None = None
_YAHOO_HEADSHOT_MAP: dict[str, str] | None = None
_SLEEPER_HEADSHOT_MAP: dict[str, str] | None = None
_BIO_HEADSHOT_MAP: dict[str, str] | None = None

# player_bio caches (canonical source)
_BIO_YAHOO_MAP: dict[str, str] | None = None
_BIO_SLEEPER_MAP: dict[str, str] | None = None
_BIO_ESPN_MAP: dict[str, str] | None = None


def _load_player_bio_maps() -> None:
    """Load all platform ID -> NFL_player_id mappings from player_bio (canonical source).

    player_bio has yahoo_player_id, sleeper_player_id, and espn_id columns.
    This is the single source of truth for cross-platform player identity.
    """
    global _BIO_YAHOO_MAP, _BIO_SLEEPER_MAP, _BIO_ESPN_MAP, _BIO_HEADSHOT_MAP

    if _BIO_YAHOO_MAP is not None:
        return  # Already loaded

    # Backend-aware: Fly path uses DATABASE_READ_TOKEN (handled inside FlyReader),
    # MotherDuck path needs MOTHERDUCK_TOKEN. Fail-closed only on the latter.
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend != "fly":
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            _BIO_YAHOO_MAP = {}
            _BIO_SLEEPER_MAP = {}
            _BIO_ESPN_MAP = {}
            _BIO_HEADSHOT_MAP = {}
            return

    try:
        from multi_league.core.runtime_mode import is_corpus_mode

        query = """
                SELECT NFL_player_id, yahoo_player_id, sleeper_player_id, espn_id, headshot_url
                FROM nfl_historical.player_bio
                WHERE NFL_player_id IS NOT NULL
                """
        if is_corpus_mode():
            from pathlib import Path

            import duckdb

            ops_path = Path(os.environ.get("OPS_CACHE_PATH", ""))
            if not ops_path.is_file():
                raise FileNotFoundError(f"Corpus ops cache is missing: {ops_path}")
            conn = duckdb.connect(str(ops_path), read_only=True)
            try:
                df = conn.execute(query).fetchdf()
            finally:
                conn.close()
        else:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
            df = reader.query_df(query, database="___ops")

        # Yahoo: yahoo_player_id (DOUBLE) -> NFL_player_id
        yahoo_mask = df["yahoo_player_id"].notna()
        _BIO_YAHOO_MAP = dict(
            zip(
                df.loc[yahoo_mask, "yahoo_player_id"].astype(int).astype(str),
                df.loc[yahoo_mask, "NFL_player_id"],
            )
        )

        # Sleeper: sleeper_player_id (DOUBLE) -> NFL_player_id
        sleeper_mask = df["sleeper_player_id"].notna()
        _BIO_SLEEPER_MAP = dict(
            zip(
                df.loc[sleeper_mask, "sleeper_player_id"].astype(int).astype(str),
                df.loc[sleeper_mask, "NFL_player_id"],
            )
        )

        # ESPN: espn_id (VARCHAR) -> NFL_player_id
        espn_mask = df["espn_id"].notna() & (df["espn_id"].str.strip() != "")
        _BIO_ESPN_MAP = dict(
            zip(
                df.loc[espn_mask, "espn_id"].astype(str),
                df.loc[espn_mask, "NFL_player_id"],
            )
        )

        # Headshots: NFL_player_id -> headshot_url
        hs_mask = df["headshot_url"].notna() & (df["headshot_url"].str.strip() != "")
        _BIO_HEADSHOT_MAP = dict(
            zip(
                df.loc[hs_mask, "NFL_player_id"],
                df.loc[hs_mask, "headshot_url"],
            )
        )

        logger.info(
            f"[player_bio] Loaded {len(_BIO_YAHOO_MAP):,} Yahoo, "
            f"{len(_BIO_SLEEPER_MAP):,} Sleeper, {len(_BIO_ESPN_MAP):,} ESPN mappings"
        )

    except Exception as e:
        logger.warning(f"Failed to load player_bio mappings: {e}")
        _BIO_YAHOO_MAP = {}
        _BIO_SLEEPER_MAP = {}
        _BIO_ESPN_MAP = {}
        _BIO_HEADSHOT_MAP = {}


def get_yahoo_to_nfl_map() -> dict[str, str]:
    """
    Get Yahoo player ID to NFL_player_id mapping.

    Uses player_bio as the single source of truth.

    Returns:
        Dict mapping yahoo_player_id -> NFL_player_id
    """
    global _YAHOO_MAP
    if _YAHOO_MAP is None:
        _load_player_bio_maps()
        _YAHOO_MAP = dict(_BIO_YAHOO_MAP or {})
        logger.info(f"[Yahoo] Loaded {len(_YAHOO_MAP):,} mappings from player_bio")
    return _YAHOO_MAP


def get_sleeper_to_nfl_map() -> dict[str, str]:
    """
    Get Sleeper player ID to NFL_player_id mapping.

    Uses player_bio as the single source of truth.

    Returns:
        Dict mapping sleeper_player_id -> NFL_player_id
    """
    global _SLEEPER_MAP
    if _SLEEPER_MAP is None:
        _load_player_bio_maps()
        _SLEEPER_MAP = dict(_BIO_SLEEPER_MAP or {})
        logger.info(f"[Sleeper] Loaded {len(_SLEEPER_MAP):,} mappings from player_bio")
    return _SLEEPER_MAP


def get_espn_to_nfl_map() -> dict[str, str]:
    """
    Get ESPN player ID to NFL_player_id mapping.

    Uses player_bio as the single source of truth.

    Returns:
        Dict mapping espn_id -> NFL_player_id
    """
    global _ESPN_MAP
    if _ESPN_MAP is None:
        _load_player_bio_maps()
        _ESPN_MAP = dict(_BIO_ESPN_MAP or {})
        logger.info(f"[ESPN] Loaded {len(_ESPN_MAP):,} mappings from player_bio")
    return _ESPN_MAP


def get_yahoo_to_headshot_map() -> dict[str, str]:
    """
    Get Yahoo player ID to headshot_url mapping from player_bio.

    Returns:
        Dict mapping yahoo_player_id -> headshot_url
    """
    global _YAHOO_HEADSHOT_MAP
    if _YAHOO_HEADSHOT_MAP is None:
        _load_player_bio_maps()
        # Build yahoo_player_id -> headshot_url from player_bio
        _YAHOO_HEADSHOT_MAP = {}
        if _BIO_YAHOO_MAP and _BIO_HEADSHOT_MAP:
            for yahoo_id, nfl_id in (_BIO_YAHOO_MAP or {}).items():
                headshot = (_BIO_HEADSHOT_MAP or {}).get(nfl_id)
                if headshot:
                    _YAHOO_HEADSHOT_MAP[yahoo_id] = headshot
        logger.info(f"Built {len(_YAHOO_HEADSHOT_MAP):,} Yahoo headshots from player_bio")
    return _YAHOO_HEADSHOT_MAP


def get_sleeper_to_headshot_map() -> dict[str, str]:
    """
    Get Sleeper player ID to headshot_url mapping from player_bio.

    Returns:
        Dict mapping sleeper_player_id -> headshot_url
    """
    global _SLEEPER_HEADSHOT_MAP
    if _SLEEPER_HEADSHOT_MAP is None:
        _load_player_bio_maps()
        # Build sleeper_player_id -> headshot_url from player_bio
        _SLEEPER_HEADSHOT_MAP = {}
        if _BIO_SLEEPER_MAP and _BIO_HEADSHOT_MAP:
            for sleeper_id, nfl_id in (_BIO_SLEEPER_MAP or {}).items():
                headshot = (_BIO_HEADSHOT_MAP or {}).get(nfl_id)
                if headshot:
                    _SLEEPER_HEADSHOT_MAP[sleeper_id] = headshot
        logger.info(f"Built {len(_SLEEPER_HEADSHOT_MAP):,} Sleeper headshots from player_bio")
    return _SLEEPER_HEADSHOT_MAP


def get_bio_headshot_map() -> dict[str, str]:
    """Get NFL_player_id -> headshot_url mapping from player_bio."""
    _load_player_bio_maps()
    return _BIO_HEADSHOT_MAP or {}


def resolve_headshot_url(
    player_id: str,
    platform: str,
) -> str | None:
    """
    Resolve a platform-specific player ID to headshot_url.

    Tries player_bio first (via NFL_player_id), then falls back to
    platform-specific headshot maps.

    Args:
        player_id: Platform-specific player ID
        platform: 'yahoo', 'sleeper', or 'espn'

    Returns:
        headshot_url or None if not found
    """
    if not player_id:
        return None

    player_id_str = str(player_id)

    # Try resolving to NFL_player_id first, then get headshot from bio
    nfl_id = resolve_nfl_player_id(player_id_str, platform)
    if nfl_id:
        bio_hs = get_bio_headshot_map().get(nfl_id)
        if bio_hs:
            return bio_hs

    # Fall back to platform-specific headshot maps
    if platform == "yahoo":
        return get_yahoo_to_headshot_map().get(player_id_str)
    elif platform == "sleeper":
        return get_sleeper_to_headshot_map().get(player_id_str)
    else:
        return None


def resolve_nfl_player_id(
    player_id: str,
    platform: str,
) -> str | None:
    """
    Resolve a platform-specific player ID to NFL_player_id.

    Args:
        player_id: Platform-specific player ID
        platform: 'yahoo' or 'sleeper'

    Returns:
        NFL_player_id (GSIS format) or None if not found

    Examples:
        >>> resolve_nfl_player_id('12345', 'yahoo')
        '00-0033873'

        >>> resolve_nfl_player_id('4046', 'sleeper')
        '00-0033873'
    """
    if not player_id:
        return None

    player_id_str = str(player_id)

    if platform == "yahoo":
        return get_yahoo_to_nfl_map().get(player_id_str)
    elif platform == "sleeper":
        return get_sleeper_to_nfl_map().get(player_id_str)
    elif platform == "espn":
        return get_espn_to_nfl_map().get(player_id_str)
    else:
        logger.warning(f"Unknown platform: {platform}")
        return None


def resolve_many_nfl_player_ids(
    player_ids: list,
    platform: str,
) -> dict[str, str | None]:
    """
    Resolve multiple player IDs to NFL_player_ids.

    More efficient than calling resolve_nfl_player_id repeatedly
    as it only loads the mapping once.

    Args:
        player_ids: List of platform-specific player IDs
        platform: 'yahoo' or 'sleeper'

    Returns:
        Dict mapping input IDs to NFL_player_ids (None if not found)
    """
    if platform == "yahoo":
        mapping = get_yahoo_to_nfl_map()
    elif platform == "sleeper":
        mapping = get_sleeper_to_nfl_map()
    elif platform == "espn":
        mapping = get_espn_to_nfl_map()
    else:
        return {str(pid): None for pid in player_ids}

    return {str(pid): mapping.get(str(pid)) for pid in player_ids}


def clear_player_id_cache():
    """
    Clear cached player ID mappings.

    Call this to force a refresh from MotherDuck on next access.
    """
    global _YAHOO_MAP, _SLEEPER_MAP, _ESPN_MAP
    global _YAHOO_HEADSHOT_MAP, _SLEEPER_HEADSHOT_MAP, _BIO_HEADSHOT_MAP
    global _BIO_YAHOO_MAP, _BIO_SLEEPER_MAP, _BIO_ESPN_MAP
    _YAHOO_MAP = None
    _SLEEPER_MAP = None
    _ESPN_MAP = None
    _YAHOO_HEADSHOT_MAP = None
    _SLEEPER_HEADSHOT_MAP = None
    _BIO_HEADSHOT_MAP = None
    _BIO_YAHOO_MAP = None
    _BIO_SLEEPER_MAP = None
    _BIO_ESPN_MAP = None
    logger.info("Cleared player ID and headshot mapping caches")


def get_mapping_stats() -> dict[str, int]:
    """
    Get statistics about loaded mappings.

    Returns:
        Dict with counts of loaded mappings per platform
    """
    return {
        "yahoo": len(get_yahoo_to_nfl_map()),
        "sleeper": len(get_sleeper_to_nfl_map()),
        "espn": len(get_espn_to_nfl_map()),
        "yahoo_headshots": len(get_yahoo_to_headshot_map()),
        "sleeper_headshots": len(get_sleeper_to_headshot_map()),
    }
