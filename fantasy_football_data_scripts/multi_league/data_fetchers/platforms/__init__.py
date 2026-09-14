"""
Platform Abstraction Layer

This module provides a unified interface for working with different
fantasy football platforms (Yahoo, Sleeper, ESPN, etc.).

Key Components:
    - BasePlatformClient: Abstract base class for API clients
    - BaseFetcher: Abstract base classes for data fetchers
    - PlatformRegistry: Singleton for registering platform implementations
    - Factory functions: get_platform(), get_client(), get_normalizer()

Usage:
    from data_fetchers.platforms import get_platform, get_client, get_normalizer

    # Auto-detect platform and get appropriate client
    platform = get_platform(ctx)
    client = get_client(ctx, oauth=oauth)  # For Yahoo
    client = get_client(ctx)  # For Sleeper (no auth)

    # Get normalizer for the platform
    normalizer = get_normalizer(ctx)

Adding a New Platform:
    1. Create platform_client.py implementing BasePlatformClient
    2. Create platform_normalizer.py implementing BaseNormalizer
    3. Register in platform/__init__.py or platform's __init__.py
"""

from .base_client import BasePlatformClient, PlatformConfig
from .base_fetcher import (
    BaseFetcher,
    BaseRosterFetcher,
    BaseMatchupFetcher,
    BaseDraftFetcher,
    BaseTransactionFetcher,
)
from .registry import (
    registry,
    PlatformRegistry,
    get_platform,
    get_client,
    get_normalizer,
    get_fetcher,
)

# Track if platforms have been registered
_platforms_registered = False


def _register_platforms():
    """
    Register all known platforms.

    Called lazily on first use to avoid circular imports.
    """
    global _platforms_registered
    if _platforms_registered:
        return

    _platforms_registered = True

    # Register Yahoo
    try:
        from ..yahoo.yahoo_client import YahooPlatformClient
        from ..yahoo.yahoo_normalizer import YahooNormalizer
        from ..base.base_context import is_yahoo_context

        registry.register(
            platform_name="yahoo",
            client_class=YahooPlatformClient,
            normalizer_class=YahooNormalizer,
            context_detector=is_yahoo_context,
        )
    except ImportError as e:
        import logging

        logging.debug(f"Yahoo platform not available: {e}")

    # Register Sleeper
    try:
        from ..sleeper.sleeper_client import SleeperPlatformClient
        from ..sleeper.sleeper_data_normalizer import SleeperDataNormalizer
        from ..base.base_context import is_sleeper_context

        registry.register(
            platform_name="sleeper",
            client_class=SleeperPlatformClient,
            normalizer_class=SleeperDataNormalizer,
            context_detector=is_sleeper_context,
        )
    except ImportError as e:
        import logging

        logging.debug(f"Sleeper platform not available: {e}")


def ensure_platforms_registered():
    """Ensure platforms are registered before use."""
    _register_platforms()


# Override the convenience functions to ensure registration
_original_get_platform = get_platform
_original_get_client = get_client
_original_get_normalizer = get_normalizer
_original_get_fetcher = get_fetcher


def get_platform(ctx):
    """Get platform name from context."""
    ensure_platforms_registered()
    return _original_get_platform(ctx)


def get_client(ctx_or_platform, **kwargs):
    """Get appropriate client for context or platform."""
    ensure_platforms_registered()
    return _original_get_client(ctx_or_platform, **kwargs)


def get_normalizer(ctx_or_platform):
    """Get appropriate normalizer for context or platform."""
    ensure_platforms_registered()
    return _original_get_normalizer(ctx_or_platform)


def get_fetcher(ctx, fetcher_type, client):
    """Get appropriate fetcher for context."""
    ensure_platforms_registered()
    return _original_get_fetcher(ctx, fetcher_type, client)


__all__ = [
    # Base classes
    "BasePlatformClient",
    "PlatformConfig",
    "BaseFetcher",
    "BaseRosterFetcher",
    "BaseMatchupFetcher",
    "BaseDraftFetcher",
    "BaseTransactionFetcher",
    # Registry
    "registry",
    "PlatformRegistry",
    # Factory functions
    "get_platform",
    "get_client",
    "get_normalizer",
    "get_fetcher",
    # Utility
    "ensure_platforms_registered",
]
