"""
Consolidated retry and rate limiting utilities.

Replaces platform-specific retry logic in yahoo_rosters.py, sleeper_api_client.py.
Adds retry capability to espn_api_client.py (which had none).

This module re-exports the canonical retry_with_backoff decorator from
multi_league.utils.retry and adds a RateLimiter class for enforcing
per-platform rate limits.

Usage:
    from multi_league.data_fetchers.shared.retry_utils import (
        retry_with_backoff,
        RateLimiter,
        DEFAULT_TIMEOUT,
    )

    # Decorator usage (same as utils.retry)
    @retry_with_backoff(
        max_retries=3,
        retry_exceptions=(ConnectionError, TimeoutError),
    )
    def fetch_data():
        return requests.get(url, timeout=DEFAULT_TIMEOUT).json()

    # Rate limiter usage
    limiter = RateLimiter(requests_per_minute=1000)
    for item in items:
        limiter.wait()
        process(item)
"""

import logging
import time
import threading

# Re-export the canonical retry decorator so callers only need one import
from ...utils.retry import retry_with_backoff, RetryContext, retry_api_call  # noqa: F401

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_INITIAL_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0


class RateLimiter:
    """
    Sliding-window rate limiter. Thread-safe for use with ThreadPoolExecutor.

    Enforces both:
    - A maximum number of requests per minute (sliding window)
    - A minimum gap between consecutive requests

    Args:
        requests_per_minute: Maximum requests allowed per 60-second window.
            0 or negative disables rate limiting.

    Usage:
        limiter = RateLimiter(requests_per_minute=1000)
        for url in urls:
            limiter.wait()
            requests.get(url)
    """

    def __init__(self, requests_per_minute: int = 0):
        self._rpm = requests_per_minute
        self._min_interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
        self._last_request = 0.0
        self._window: list = []  # Timestamps of recent requests
        self._lock = threading.Lock()

    def wait(self) -> None:
        """Block until a request is allowed under the rate limit."""
        if self._rpm <= 0:
            return

        with self._lock:
            now = time.monotonic()
            window_start = now - 60.0

            # Prune timestamps outside the 1-minute window
            self._window = [t for t in self._window if t > window_start]

            # If at capacity, wait until the oldest request falls out
            if len(self._window) >= self._rpm:
                oldest = min(self._window)
                wait_time = oldest + 60.0 - now + 0.1  # small buffer
                if wait_time > 0:
                    logger.debug(f"Rate limit reached ({self._rpm}/min), waiting {wait_time:.1f}s")
                    time.sleep(wait_time)

            # Enforce minimum gap between consecutive requests
            elapsed = time.monotonic() - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)

            self._last_request = time.monotonic()
            self._window.append(self._last_request)

    @property
    def requests_per_minute(self) -> int:
        """Current rate limit setting."""
        return self._rpm
