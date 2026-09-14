"""
Yahoo Fantasy Football Data Fetchers

This package provides data fetchers for Yahoo Fantasy Football leagues.
All output schemas are normalized to match the canonical format for
downstream compatibility with transformations.

Modules:
    yahoo_rosters: Weekly roster/player data (from yahoo_fantasy_data.py)
    yahoo_matchups: Weekly matchup scores (from weekly_matchup_data_v2.py)
    yahoo_draft: Draft picks data (from draft_data_v2.py)
    yahoo_transactions: Transaction history (from transactions_v2.py)
    yahoo_nfl_merge: Merge with NFLverse stats
    yahoo_normalizer: Convert Yahoo output to canonical schema

Usage:
    from data_fetchers.yahoo import YahooNormalizer

    normalizer = YahooNormalizer()
    canonical_df = normalizer.normalize_player_data(yahoo_df, league_id)
"""

from .yahoo_normalizer import (
    YahooNormalizer,
    normalize_player_data,
    normalize_matchup_data,
    normalize_draft_data,
    normalize_transaction_data,
)

__all__ = [
    "YahooNormalizer",
    "normalize_player_data",
    "normalize_matchup_data",
    "normalize_draft_data",
    "normalize_transaction_data",
]

# Future exports (after file moves):
# from .yahoo_rosters import YahooRosterFetcher, fetch_yahoo_rosters
# from .yahoo_matchups import fetch_yahoo_matchups
# from .yahoo_draft import fetch_yahoo_draft
# from .yahoo_transactions import fetch_yahoo_transactions
# from .yahoo_nfl_merge import merge_yahoo_nfl
