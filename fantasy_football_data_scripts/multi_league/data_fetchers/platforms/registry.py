"""
Platform Registry

Singleton registry for platform implementations.
Allows registration of new platforms without modifying core code.
Each platform registers its client, normalizer, and context detector.
"""

from typing import Any, TYPE_CHECKING
from collections.abc import Callable
import logging

if TYPE_CHECKING:
    from .base_client import BasePlatformClient
    from ..base.base_normalizer import BaseNormalizer

logger = logging.getLogger(__name__)


class PlatformRegistry:
    """
    Singleton registry for platform implementations.

    Allows registration of new platforms without modifying core code.
    Each platform registers its client, normalizer, and context classes.

    Example:
        # Register a new platform
        registry.register(
            platform_name='espn',
            client_class=ESPNPlatformClient,
            normalizer_class=ESPNNormalizer,
            context_detector=is_espn_context,
        )

        # Get client for a context
        platform = get_platform(ctx)
        client = get_client(platform, oauth=oauth)
        normalizer = get_normalizer(platform)
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._platforms: dict[str, dict[str, Any]] = {}
            cls._instance._initialized = False
        return cls._instance

    def register(
        self,
        platform_name: str,
        client_class: type["BasePlatformClient"],
        normalizer_class: type["BaseNormalizer"],
        context_detector: Callable[[Any], bool],
        fetcher_classes: dict[str, type] | None = None,
    ):
        """
        Register a platform implementation.

        Args:
            platform_name: Platform identifier ('yahoo', 'sleeper', 'espn')
            client_class: Platform API client class
            normalizer_class: Platform normalizer class
            context_detector: Function to detect if context is for this platform
            fetcher_classes: Optional dict of fetcher type -> class
                            e.g., {'roster': SleeperRosterFetcher}
        """
        logger.info(f"Registering platform: {platform_name}")
        self._platforms[platform_name] = {
            "client": client_class,
            "normalizer": normalizer_class,
            "detector": context_detector,
            "fetchers": fetcher_classes or {},
        }

    def unregister(self, platform_name: str):
        """Remove a platform registration."""
        if platform_name in self._platforms:
            del self._platforms[platform_name]
            logger.info(f"Unregistered platform: {platform_name}")

    def detect_platform(self, ctx: Any) -> str:
        """
        Auto-detect platform from context.

        Args:
            ctx: Context object (LeagueContext, SleeperContext, etc.)

        Returns:
            Platform name string

        Raises:
            ValueError: If context doesn't match any registered platform
        """
        # Check explicit platform attribute first
        if hasattr(ctx, "platform"):
            platform = getattr(ctx, "platform", None)
            if platform and platform in self._platforms:
                return platform

        # Try each registered detector
        for name, info in self._platforms.items():
            try:
                if info["detector"](ctx):
                    return name
            except Exception as e:
                logger.debug(f"Detector for {name} failed: {e}")
                continue

        raise ValueError(
            f"Unknown platform - context doesn't match any registered platform. "
            f"Available platforms: {list(self._platforms.keys())}"
        )

    def get_client_class(self, platform: str) -> type["BasePlatformClient"]:
        """Get platform client class."""
        if platform not in self._platforms:
            raise ValueError(f"Unknown platform: {platform}. Available: {self.available_platforms}")
        return self._platforms[platform]["client"]

    def get_client(self, platform: str, **kwargs) -> "BasePlatformClient":
        """
        Get platform client instance.

        Args:
            platform: Platform name
            **kwargs: Arguments to pass to client constructor

        Returns:
            Instantiated platform client
        """
        client_class = self.get_client_class(platform)
        return client_class(**kwargs)

    def get_normalizer_class(self, platform: str) -> type["BaseNormalizer"]:
        """Get platform normalizer class."""
        if platform not in self._platforms:
            raise ValueError(f"Unknown platform: {platform}. Available: {self.available_platforms}")
        return self._platforms[platform]["normalizer"]

    def get_normalizer(self, platform: str) -> "BaseNormalizer":
        """
        Get platform normalizer instance.

        Args:
            platform: Platform name

        Returns:
            Instantiated normalizer
        """
        normalizer_class = self.get_normalizer_class(platform)
        return normalizer_class()

    def get_fetcher_class(self, platform: str, fetcher_type: str) -> type | None:
        """
        Get specific fetcher class for platform.

        Args:
            platform: Platform name
            fetcher_type: Type of fetcher ('roster', 'matchup', 'draft', 'transaction')

        Returns:
            Fetcher class or None if not registered
        """
        if platform not in self._platforms:
            return None
        fetchers = self._platforms[platform].get("fetchers", {})
        return fetchers.get(fetcher_type)

    def get_fetcher(self, platform: str, fetcher_type: str, ctx: Any, client: "BasePlatformClient") -> Any | None:
        """
        Get instantiated fetcher for platform.

        Args:
            platform: Platform name
            fetcher_type: Type of fetcher
            ctx: Context object
            client: Platform client

        Returns:
            Instantiated fetcher or None
        """
        fetcher_class = self.get_fetcher_class(platform, fetcher_type)
        if fetcher_class:
            return fetcher_class(ctx, client)
        return None

    def is_registered(self, platform: str) -> bool:
        """Check if a platform is registered."""
        return platform in self._platforms

    @property
    def available_platforms(self) -> list:
        """List registered platforms."""
        return list(self._platforms.keys())

    def __repr__(self) -> str:
        return f"PlatformRegistry(platforms={self.available_platforms})"


# Global registry instance
registry = PlatformRegistry()


# Convenience functions
def get_platform(ctx: Any) -> str:
    """
    Get platform name from context.

    Args:
        ctx: Context object

    Returns:
        Platform name string
    """
    return registry.detect_platform(ctx)


def get_client(ctx_or_platform: Any, **kwargs) -> "BasePlatformClient":
    """
    Get appropriate client for context or platform.

    Args:
        ctx_or_platform: Context object or platform name string
        **kwargs: Arguments for client constructor

    Returns:
        Platform client instance
    """
    if isinstance(ctx_or_platform, str):
        platform = ctx_or_platform
    else:
        platform = get_platform(ctx_or_platform)
    return registry.get_client(platform, **kwargs)


def get_normalizer(ctx_or_platform: Any) -> "BaseNormalizer":
    """
    Get appropriate normalizer for context or platform.

    Args:
        ctx_or_platform: Context object or platform name string

    Returns:
        Normalizer instance
    """
    if isinstance(ctx_or_platform, str):
        platform = ctx_or_platform
    else:
        platform = get_platform(ctx_or_platform)
    return registry.get_normalizer(platform)


def get_fetcher(ctx: Any, fetcher_type: str, client: "BasePlatformClient") -> Any | None:
    """
    Get appropriate fetcher for context.

    Args:
        ctx: Context object
        fetcher_type: Type of fetcher ('roster', 'matchup', 'draft', 'transaction')
        client: Platform client

    Returns:
        Fetcher instance or None
    """
    platform = get_platform(ctx)
    return registry.get_fetcher(platform, fetcher_type, ctx, client)
