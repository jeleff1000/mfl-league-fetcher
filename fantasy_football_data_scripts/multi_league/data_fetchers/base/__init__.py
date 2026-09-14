"""
Base classes and canonical column definitions for data fetchers.

This module provides the foundation for platform-agnostic data fetching:
- CanonicalPlayerColumns, CanonicalMatchupColumns, etc.: Standard column names
- BaseNormalizer: Abstract base class for platform normalizers
- ContextProtocol: Interface for LeagueContext/SleeperContext compatibility

Usage:
    from data_fetchers.base import (
        CanonicalPlayerColumns,
        BaseNormalizer,
        PLAYER_REQUIRED_COLUMNS,
    )
"""

from .canonical_columns import (
    CanonicalPlayerColumns,
    CanonicalMatchupColumns,
    CanonicalDraftColumns,
    CanonicalTransactionColumns,
    PLAYER_REQUIRED_COLUMNS,
    MATCHUP_REQUIRED_COLUMNS,
    DRAFT_REQUIRED_COLUMNS,
    TRANSACTION_REQUIRED_COLUMNS,
)
from .base_normalizer import BaseNormalizer
from .base_context import ContextProtocol

__all__ = [
    # Column definitions
    "CanonicalPlayerColumns",
    "CanonicalMatchupColumns",
    "CanonicalDraftColumns",
    "CanonicalTransactionColumns",
    # Required columns lists
    "PLAYER_REQUIRED_COLUMNS",
    "MATCHUP_REQUIRED_COLUMNS",
    "DRAFT_REQUIRED_COLUMNS",
    "TRANSACTION_REQUIRED_COLUMNS",
    # Base classes
    "BaseNormalizer",
    "ContextProtocol",
]
