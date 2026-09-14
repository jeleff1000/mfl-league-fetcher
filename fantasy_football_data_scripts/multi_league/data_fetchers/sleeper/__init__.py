"""
Sleeper Fantasy Football Data Fetchers

This package provides data fetchers for Sleeper fantasy football leagues,
parallel to the Yahoo data fetchers. All output schemas match Yahoo formats
for downstream compatibility.

Modules:
    sleeper_api_client: HTTP client for Sleeper API
    sleeper_player_cache: Player database caching (~5MB)
    sleeper_context: Configuration dataclass
    sleeper_rosters: Weekly roster/player data
    sleeper_transactions: Transaction history
    sleeper_traded_picks: Traded draft picks data
    sleeper_draft: Draft picks data
    sleeper_matchups: Weekly matchup scores
    sleeper_schedules: Season schedule data
    sleeper_nfl_merge: Merge with NFLverse stats

Usage:
    from .sleeper_context import SleeperContext
    from .sleeper_api_client import SleeperAPIClient
    from .sleeper_rosters import fetch_sleeper_rosters
    from .sleeper_transactions import fetch_sleeper_transactions
    from .sleeper_draft import fetch_sleeper_draft
    from .sleeper_matchups import fetch_sleeper_matchups
    from .sleeper_schedules import fetch_sleeper_schedule
    from .sleeper_nfl_merge import merge_sleeper_nfl
"""

from .sleeper_api_client import SleeperAPIClient, SleeperAPIConfig, SleeperAPIError
from .sleeper_player_cache import SleeperPlayerCache
from .sleeper_context import (
    SleeperContext,
    create_sleeper_context,
    load_sleeper_context,
    discover_league_history,
    season_has_matchup_data,
)
from .sleeper_rosters import fetch_sleeper_rosters, fetch_all_sleeper_rosters
from .sleeper_transactions import fetch_sleeper_transactions, fetch_all_sleeper_transactions
from .sleeper_traded_picks import fetch_sleeper_traded_picks
from .sleeper_draft import fetch_sleeper_draft, fetch_all_sleeper_drafts
from .sleeper_matchups import fetch_sleeper_matchups, fetch_all_sleeper_matchups
from .sleeper_schedules import fetch_sleeper_schedule, fetch_all_sleeper_schedules
from .sleeper_nfl_merge import merge_sleeper_nfl, MergeConfig
from .sleeper_league_settings import (
    fetch_sleeper_settings,
    save_sleeper_settings,
    fetch_and_save_all_settings,
    load_sleeper_settings,
)

# Keeper rules inference (optional - may not exist in all deployments)
try:
    from .sleeper_keeper_rules import (
        infer_keeper_rules,
        get_keeper_rule_description,
    )
except ImportError:
    infer_keeper_rules = None
    get_keeper_rule_description = None
from .sleeper_data_normalizer import (
    normalize_player_data,
    normalize_matchup_data,
    normalize_draft_data,
    normalize_transaction_data,
    normalize_all_sleeper_data,
    validate_canonical_schema,
)

__all__ = [
    # Core classes
    "SleeperAPIClient",
    "SleeperAPIConfig",
    "SleeperAPIError",
    "SleeperPlayerCache",
    "SleeperContext",
    # Context helpers
    "create_sleeper_context",
    "load_sleeper_context",
    "discover_league_history",
    "season_has_matchup_data",
    # Data fetchers
    "fetch_sleeper_rosters",
    "fetch_all_sleeper_rosters",
    "fetch_sleeper_transactions",
    "fetch_all_sleeper_transactions",
    "fetch_sleeper_traded_picks",
    "fetch_sleeper_draft",
    "fetch_all_sleeper_drafts",
    "fetch_sleeper_matchups",
    "fetch_all_sleeper_matchups",
    "fetch_sleeper_schedule",
    "fetch_all_sleeper_schedules",
    # Merge
    "merge_sleeper_nfl",
    "MergeConfig",
    # League Settings
    "fetch_sleeper_settings",
    "save_sleeper_settings",
    "fetch_and_save_all_settings",
    "load_sleeper_settings",
    # Keeper Rules Inference
    "infer_keeper_rules",
    "get_keeper_rule_description",
    # Data Normalization
    "normalize_player_data",
    "normalize_matchup_data",
    "normalize_draft_data",
    "normalize_transaction_data",
    "normalize_all_sleeper_data",
    "validate_canonical_schema",
]
