"""
Yahoo Data Normalizer

Converts Yahoo fetcher output to canonical schema for multi-tenant compatibility.
This ensures Yahoo data matches the same schema as Sleeper data for downstream
transformations.

Yahoo fetchers have historically NOT had an explicit normalizer - the column names
were treated as canonical. This module makes normalization explicit and ensures
parity with Sleeper's normalization approach.

Column Mappings:
- manager_name → manager
- player_name → player
- yahoo_position → position (primary)
- primary_position → position (fallback)
- fantasy_points stays as-is
- fantasy_position stays as-is
- is_keeper_status → is_keeper
- is_keeper_cost → is_keeper (fallback)

Usage:
    from yahoo_normalizer import (
        normalize_player_data,
        normalize_matchup_data,
        normalize_draft_data,
        normalize_transaction_data,
    )

    # Convert Yahoo rosters to canonical player format
    canonical_player = normalize_player_data(yahoo_rosters_df, league_id)
"""

import logging

import pandas as pd

from ..base.base_normalizer import BaseNormalizer
from ..shared.nfl_player_mapping import get_yahoo_to_headshot_map

logger = logging.getLogger(__name__)

# Cache for yahoo_nfl_player_map lookup
_YAHOO_NFL_MAP_CACHE: dict[str, str] | None = None


def log(msg: str):
    """Simple logging function."""
    logger.info(msg)
    print(msg)


def get_yahoo_nfl_map() -> dict[str, str]:
    """
    Load Yahoo-to-NFL player ID mapping from MotherDuck.

    Returns dict: yahoo_player_id -> NFL_player_id

    Caches result in memory to avoid repeated database calls.
    """
    global _YAHOO_NFL_MAP_CACHE

    if _YAHOO_NFL_MAP_CACHE is not None:
        return _YAHOO_NFL_MAP_CACHE

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        df = reader.query_df(
            """
            SELECT DISTINCT CAST(yahoo_player_id AS VARCHAR) as yahoo_player_id, NFL_player_id
            FROM nfl_historical.player_bio
            WHERE yahoo_player_id IS NOT NULL
              AND NFL_player_id IS NOT NULL AND NFL_player_id != ''
            """,
            database="___ops",
        )

        _YAHOO_NFL_MAP_CACHE = dict(zip(df["yahoo_player_id"].astype(str), df["NFL_player_id"]))
        logger.info(f"Loaded {len(_YAHOO_NFL_MAP_CACHE):,} yahoo->NFL mappings from player_bio")
        return _YAHOO_NFL_MAP_CACHE

    except Exception as e:
        logger.warning(f"Failed to load yahoo_nfl_player_map: {e}")
        _YAHOO_NFL_MAP_CACHE = {}
        return _YAHOO_NFL_MAP_CACHE


def lookup_nfl_player_id(yahoo_player_id: str) -> str | None:
    """
    Look up NFL_player_id for a Yahoo player ID.

    Uses the yahoo_nfl_player_map table in MotherDuck.

    Args:
        yahoo_player_id: Yahoo player ID

    Returns:
        NFL_player_id or None if not found
    """
    yahoo_map = get_yahoo_nfl_map()
    return yahoo_map.get(str(yahoo_player_id))


# =============================================================================
# Column Mappings: Yahoo → Canonical
# =============================================================================

PLAYER_COLUMN_MAP = {
    # Yahoo column → Canonical column
    "manager_name": "manager",
    "player_name": "player",
    # Position handling: yahoo_position is preferred, primary_position is fallback
    # Handled explicitly in normalize function
    "yahoo_position": "position",
    "primary_position": "position",  # Fallback
    # fantasy_points stays as-is (already canonical)
    # fantasy_position stays as-is (required by player_stats_v2.py for is_started)
    "nfl_team": "team",
    "team_key": "team_key",
    "manager_guid": "manager_guid",
    "year": "year",
    "week": "week",
}

MATCHUP_COLUMN_MAP = {
    "manager_name": "manager",
    "manager": "manager",  # May already be canonical
    "manager_guid": "manager_guid",
    "team_name": "team_name",
    "team_key": "team_key",
    "team_points": "points",
    "opponent": "opponent",
    "opponent_points": "opponent_points",
    "margin": "margin",
    "win": "win",
    "loss": "loss",
    "tie": "tie",
    "week": "week",
    "year": "year",
    # Yahoo-specific columns (keep as-is, Sleeper sets to NULL)
    "grade": "grade",
    "gpa": "gpa",
    "matchup_recap_url": "matchup_recap_url",
    "matchup_recap_title": "matchup_recap_title",
    "team_projected_points": "team_projected_points",
    "opponent_projected_points": "opponent_projected_points",
}

DRAFT_COLUMN_MAP = {
    "manager_name": "manager",
    "player_name": "player",
    "yahoo_position": "position",
    "primary_position": "position",  # Fallback
    "nfl_team": "team",
    # pick stays as-is (required by draft_value_metrics_v3.py)
    # cost stays as-is (required by draft_value_metrics_v3.py)
    # round stays as-is
    "manager_guid": "manager_guid",
    "team_key": "team_key",
    # Yahoo has is_keeper_status and is_keeper_cost - normalize to is_keeper
    "is_keeper_status": "is_keeper",
    "is_keeper_cost": "is_keeper",
    "draft_type": "draft_type",
    "year": "year",
    # Yahoo-specific ADP columns (Sleeper sets to NULL)
    "avg_pick": "avg_pick",
    "avg_round": "avg_round",
    "avg_cost": "avg_cost",
    "percent_drafted": "percent_drafted",
    "preseason_avg_pick": "preseason_avg_pick",
    "preseason_avg_round": "preseason_avg_round",
    "preseason_avg_cost": "preseason_avg_cost",
    "preseason_percent_drafted": "preseason_percent_drafted",
}

TRANSACTION_COLUMN_MAP = {
    "transaction_id": "transaction_id",
    "yahoo_player_id": "yahoo_player_id",  # Keep Yahoo ID
    "player_name": "player",
    "manager_name": "manager",
    "manager_guid": "manager_guid",
    "team_name": "team_name",
    "transaction_type": "transaction_type",
    "faab_bid": "faab_spent",
    "source_team": "source_team",
    "destination_team": "destination_team",
    "week": "week",
    "year": "year",
    "timestamp": "timestamp",
    "status": "status",
}


class YahooNormalizer(BaseNormalizer):
    """
    Normalizer for Yahoo Fantasy data.

    Converts Yahoo fetcher output to canonical schema, ensuring parity
    with Sleeper data for downstream transformations.
    """

    @property
    def platform(self) -> str:
        return "yahoo"

    def normalize_player_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize Yahoo roster data to canonical player.parquet schema.

        Args:
            df: DataFrame from YahooRosterFetcher
            league_id: League identifier for league_id column

        Returns:
            DataFrame with canonical column names
        """
        if df.empty:
            return df

        log(f"Normalizing {len(df)} Yahoo player rows for league {league_id}")

        result = df.copy()

        # Add platform and league identifiers
        result = self._add_platform_identifier(result, league_id)

        # Handle position column: prefer yahoo_position over primary_position
        if "yahoo_position" in result.columns:
            result["position"] = result["yahoo_position"]
        elif "primary_position" in result.columns:
            result["position"] = result["primary_position"]

        # Normalize granular IDP positions (CB→DB, DT→DL, FS→DB, FB→RB, etc.)
        if "position" in result.columns:
            from multi_league.core.roster_slots import normalize_position

            result["position"] = result["position"].apply(lambda p: normalize_position(p) if isinstance(p, str) else p)

        # Build rename map (only for columns that need renaming)
        rename_map = {}
        for yahoo_col, canonical_col in PLAYER_COLUMN_MAP.items():
            if yahoo_col in result.columns and yahoo_col != canonical_col:
                # Don't overwrite if canonical already exists
                if canonical_col not in result.columns:
                    rename_map[yahoo_col] = canonical_col

        result = result.rename(columns=rename_map)

        # Ensure yahoo_player_id is preserved
        if "yahoo_player_id" not in result.columns and "player_id" in result.columns:
            result["yahoo_player_id"] = result["player_id"]

        # Add NFL_player_id from mapping table
        if "yahoo_player_id" in result.columns and "NFL_player_id" not in result.columns:
            yahoo_map = get_yahoo_nfl_map()
            if yahoo_map:
                result["NFL_player_id"] = result["yahoo_player_id"].astype(str).map(lambda x: yahoo_map.get(x, None))
            else:
                result["NFL_player_id"] = None

        # Add headshot_url from mapping table (fallback for when super_table join fails)
        # Always ensure headshot_url column exists (even if we can't fill it)
        if "headshot_url" not in result.columns:
            result["headshot_url"] = None

        # Try to fill missing headshots from mapping table
        if "yahoo_player_id" in result.columns and result["headshot_url"].isna().any():
            headshot_map = get_yahoo_to_headshot_map()
            if headshot_map:
                missing_mask = result["headshot_url"].isna()
                if missing_mask.any():
                    result.loc[missing_mask, "headshot_url"] = (
                        result.loc[missing_mask, "yahoo_player_id"].astype(str).map(lambda x: headshot_map.get(x, None))
                    )

        # Set is_started based on fantasy_position (roster slot)
        result = self._compute_is_started(result)

        # Add is_rostered (Yahoo fetcher only gets rostered players)
        if "is_rostered" not in result.columns:
            result["is_rostered"] = True

        # Add composite keys
        result = self._add_composite_keys(result)

        # Add backward compatibility aliases
        result = self._add_backward_compatibility_aliases(result)

        # Ensure sleeper_player_id is NULL for Yahoo data
        if "sleeper_player_id" not in result.columns:
            result["sleeper_player_id"] = None

        self._log_normalization_summary(
            "player_fantasy",
            len(df),
            len(result),
            rename_map,
        )

        return result

    def normalize_matchup_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize Yahoo matchup data to canonical matchup.parquet schema.

        Args:
            df: DataFrame from fetch_yahoo_matchups
            league_id: League identifier

        Returns:
            DataFrame with canonical column names
        """
        if df.empty:
            return df

        log(f"Normalizing {len(df)} Yahoo matchup rows for league {league_id}")

        result = df.copy()

        # Add platform and league identifiers
        result = self._add_platform_identifier(result, league_id)

        # Build rename map
        rename_map = {}
        for yahoo_col, canonical_col in MATCHUP_COLUMN_MAP.items():
            if yahoo_col in result.columns and yahoo_col != canonical_col:
                if canonical_col not in result.columns:
                    rename_map[yahoo_col] = canonical_col

        result = result.rename(columns=rename_map)

        # Add team_points alias if we renamed to points
        if "points" in result.columns and "team_points" not in result.columns:
            result["team_points"] = result["points"]

        self._log_normalization_summary(
            "matchup",
            len(df),
            len(result),
            rename_map,
        )

        return result

    def normalize_draft_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize Yahoo draft data to canonical draft.parquet schema.

        Args:
            df: DataFrame from fetch_yahoo_draft
            league_id: League identifier

        Returns:
            DataFrame with canonical column names
        """
        if df.empty:
            return df

        log(f"Normalizing {len(df)} Yahoo draft rows for league {league_id}")

        result = df.copy()

        # Add platform and league identifiers
        result = self._add_platform_identifier(result, league_id)

        # Handle position column
        if "yahoo_position" in result.columns:
            result["position"] = result["yahoo_position"]
        elif "primary_position" in result.columns:
            result["position"] = result["primary_position"]

        # Handle is_keeper - Yahoo has is_keeper_status and is_keeper_cost
        # Consolidate to single is_keeper boolean
        if "is_keeper" not in result.columns:
            if "is_keeper_status" in result.columns:
                # is_keeper_status is typically a boolean or 0/1
                result["is_keeper"] = result["is_keeper_status"].fillna(False).astype(bool)
            elif "is_keeper_cost" in result.columns:
                # is_keeper_cost > 0 indicates keeper
                result["is_keeper"] = result["is_keeper_cost"].fillna(0) > 0

        # Build rename map
        rename_map = {}
        for yahoo_col, canonical_col in DRAFT_COLUMN_MAP.items():
            if yahoo_col in result.columns and yahoo_col != canonical_col:
                if canonical_col not in result.columns:
                    rename_map[yahoo_col] = canonical_col

        result = result.rename(columns=rename_map)

        # Add NFL_player_id from mapping
        if "yahoo_player_id" in result.columns and "NFL_player_id" not in result.columns:
            yahoo_map = get_yahoo_nfl_map()
            if yahoo_map:
                result["NFL_player_id"] = result["yahoo_player_id"].astype(str).map(lambda x: yahoo_map.get(x, None))
            else:
                result["NFL_player_id"] = None

        # Add backward compatibility aliases
        if "player" in result.columns and "player_name" not in result.columns:
            result["player_name"] = result["player"]
        if "pick" in result.columns and "pick_number" not in result.columns:
            result["pick_number"] = result["pick"]
        if "cost" in result.columns and "keeper_cost" not in result.columns:
            result["keeper_cost"] = result["cost"]

        # Ensure sleeper_player_id is NULL for Yahoo data
        if "sleeper_player_id" not in result.columns:
            result["sleeper_player_id"] = None

        self._log_normalization_summary(
            "draft",
            len(df),
            len(result),
            rename_map,
        )

        return result

    def normalize_transaction_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize Yahoo transaction data to canonical transactions.parquet schema.

        Args:
            df: DataFrame from fetch_yahoo_transactions
            league_id: League identifier

        Returns:
            DataFrame with canonical column names
        """
        if df.empty:
            return df

        log(f"Normalizing {len(df)} Yahoo transaction rows for league {league_id}")

        result = df.copy()

        # Add platform and league identifiers
        result = self._add_platform_identifier(result, league_id)

        # Build rename map
        rename_map = {}
        for yahoo_col, canonical_col in TRANSACTION_COLUMN_MAP.items():
            if yahoo_col in result.columns and yahoo_col != canonical_col:
                if canonical_col not in result.columns:
                    rename_map[yahoo_col] = canonical_col

        result = result.rename(columns=rename_map)

        # Trade row expansion (duplicate_trade_rows) is handled canonically by
        # normalize_transaction_df() in canonical_transaction.py — not here.

        # Add NFL_player_id from mapping
        if "yahoo_player_id" in result.columns and "NFL_player_id" not in result.columns:
            yahoo_map = get_yahoo_nfl_map()
            if yahoo_map:
                result["NFL_player_id"] = result["yahoo_player_id"].astype(str).map(lambda x: yahoo_map.get(x, None))
            else:
                result["NFL_player_id"] = None

        # Add backward compatibility alias
        if "player" in result.columns and "player_name" not in result.columns:
            result["player_name"] = result["player"]

        # Ensure sleeper_player_id is NULL for Yahoo data
        if "sleeper_player_id" not in result.columns:
            result["sleeper_player_id"] = None

        self._log_normalization_summary(
            "transactions",
            len(df),
            len(result),
            rename_map,
        )

        return result


# =============================================================================
# Module-level convenience functions
# =============================================================================

# Singleton normalizer instance
_normalizer: YahooNormalizer | None = None


def _get_normalizer() -> YahooNormalizer:
    """Get or create singleton normalizer instance."""
    global _normalizer
    if _normalizer is None:
        _normalizer = YahooNormalizer()
    return _normalizer


def normalize_player_data(
    df: pd.DataFrame,
    league_id: str,
) -> pd.DataFrame:
    """
    Normalize Yahoo roster data to canonical schema.

    Convenience function that uses singleton normalizer.

    Args:
        df: DataFrame from YahooRosterFetcher
        league_id: League identifier

    Returns:
        Normalized DataFrame
    """
    return _get_normalizer().normalize_player_data(df, league_id)


def normalize_matchup_data(
    df: pd.DataFrame,
    league_id: str,
) -> pd.DataFrame:
    """
    Normalize Yahoo matchup data to canonical schema.

    Convenience function that uses singleton normalizer.

    Args:
        df: DataFrame from fetch_yahoo_matchups
        league_id: League identifier

    Returns:
        Normalized DataFrame
    """
    return _get_normalizer().normalize_matchup_data(df, league_id)


def normalize_draft_data(
    df: pd.DataFrame,
    league_id: str,
) -> pd.DataFrame:
    """
    Normalize Yahoo draft data to canonical schema.

    Convenience function that uses singleton normalizer.

    Args:
        df: DataFrame from fetch_yahoo_draft
        league_id: League identifier

    Returns:
        Normalized DataFrame
    """
    return _get_normalizer().normalize_draft_data(df, league_id)


def normalize_transaction_data(
    df: pd.DataFrame,
    league_id: str,
) -> pd.DataFrame:
    """
    Normalize Yahoo transaction data to canonical schema.

    Convenience function that uses singleton normalizer.

    Args:
        df: DataFrame from fetch_yahoo_transactions
        league_id: League identifier

    Returns:
        Normalized DataFrame
    """
    return _get_normalizer().normalize_transaction_data(df, league_id)
