"""
ESPN Fantasy Football Data Fetchers

This package provides data fetchers for ESPN fantasy football leagues,
parallel to the Sleeper and Yahoo data fetchers. All output schemas
match canonical formats for downstream compatibility.

Modules:
    espn_context: Configuration dataclass
    espn_api_client: ESPN API wrapper (library + raw API for trades)
    espn_league_settings: League settings fetch/save/load
    espn_draft: Draft picks data
    espn_matchups: Weekly matchup scores
    espn_rosters: Weekly roster/player data
    espn_transactions: Transaction history (waivers, trades, roster-diff)
    espn_schedules: Season schedule metadata
    espn_nfl_merge: Merge with NFLverse stats
    espn_data_normalizer: Canonical schema normalization

Usage:
    from .espn_context import ESPNContext
    from .espn_api_client import ESPNAPIClient
    from .espn_rosters import fetch_espn_rosters
    from .espn_matchups import fetch_espn_matchups
    from .espn_draft import fetch_espn_draft
    from .espn_transactions import fetch_espn_transactions
    from .espn_nfl_merge import merge_espn_nfl
"""

from .espn_context import ESPNContext, create_espn_context, load_espn_context, build_manager_names
from .espn_api_client import ESPNAPIClient, ESPNAPIError
from .espn_nfl_merge import merge_espn_nfl, MergeConfig
from .espn_draft import fetch_espn_draft, fetch_all_espn_drafts
from .espn_matchups import fetch_espn_matchups, fetch_all_espn_matchups
from .espn_rosters import fetch_espn_rosters, fetch_all_espn_rosters
from .espn_transactions import fetch_espn_transactions, fetch_all_espn_transactions
from .espn_schedules import fetch_espn_schedule, fetch_all_espn_schedules
from .espn_league_settings import (
    fetch_espn_settings,
    save_espn_settings,
    load_espn_settings,
    fetch_and_save_all_settings,
)
from .espn_data_normalizer import (
    normalize_player_data,
    normalize_matchup_data,
    normalize_draft_data,
    normalize_transaction_data,
    normalize_all_espn_data,
    validate_canonical_schema,
)

__all__ = [
    # Core classes
    "ESPNContext",
    "ESPNAPIClient",
    "ESPNAPIError",
    # Context helpers
    "create_espn_context",
    "load_espn_context",
    "build_manager_names",
    # Data fetchers
    "fetch_espn_draft",
    "fetch_all_espn_drafts",
    "fetch_espn_matchups",
    "fetch_all_espn_matchups",
    "fetch_espn_rosters",
    "fetch_all_espn_rosters",
    "fetch_espn_transactions",
    "fetch_all_espn_transactions",
    "fetch_espn_schedule",
    "fetch_all_espn_schedules",
    # Merge
    "merge_espn_nfl",
    "MergeConfig",
    # League Settings
    "fetch_espn_settings",
    "save_espn_settings",
    "load_espn_settings",
    "fetch_and_save_all_settings",
    # Data Normalization
    "normalize_player_data",
    "normalize_matchup_data",
    "normalize_draft_data",
    "normalize_transaction_data",
    "normalize_all_espn_data",
    "validate_canonical_schema",
]
