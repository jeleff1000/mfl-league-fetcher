"""
Retry utilities for handling transient failures.

Provides decorators and context managers for retrying operations
that may fail due to network issues, rate limits, or other transient errors.
"""

import time
import random
import logging
import functools
from typing import TypeVar
from collections.abc import Callable

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Default exceptions to retry on
DEFAULT_RETRY_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,  # Includes network errors
)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_exceptions: tuple[type[Exception], ...] = DEFAULT_RETRY_EXCEPTIONS,
    on_retry: Callable[[Exception, int], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (not including initial attempt)
        base_delay: Initial delay in seconds between retries
        max_delay: Maximum delay in seconds between retries
        exponential_base: Base for exponential backoff calculation
        jitter: Add random jitter to delay to prevent thundering herd
        retry_exceptions: Tuple of exception types to retry on
        on_retry: Optional callback called on each retry with (exception, attempt_number)

    Returns:
        Decorated function that will retry on specified exceptions

    Example:
        @retry_with_backoff(max_retries=3)
        def fetch_data():
            return requests.get(url).json()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exception = e

                    if attempt >= max_retries:
                        logger.error(f"{func.__name__} failed after {max_retries + 1} attempts: {e}")
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base**attempt), max_delay)

                    # Add jitter (up to 25% of delay)
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    if on_retry:
                        on_retry(e, attempt + 1)

                    time.sleep(delay)

            # Should never reach here, but just in case
            raise last_exception

        return wrapper

    return decorator


class RetryContext:
    """
    Context manager for retry logic with progress tracking.

    Example:
        with RetryContext(max_retries=3, operation="upload") as ctx:
            for attempt in ctx:
                try:
                    result = do_upload()
                    ctx.success(result)
                except ConnectionError as e:
                    ctx.retry(e)
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        operation: str = "operation",
        retry_exceptions: tuple[type[Exception], ...] = DEFAULT_RETRY_EXCEPTIONS,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.operation = operation
        self.retry_exceptions = retry_exceptions
        self._attempt = 0
        self._result = None
        self._success = False
        self._last_exception = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and isinstance(exc_val, self.retry_exceptions):
            # Don't suppress retry exceptions
            return False
        return False

    def __iter__(self):
        return self

    def __next__(self) -> int:
        if self._success:
            raise StopIteration

        if self._attempt > self.max_retries:
            if self._last_exception:
                raise self._last_exception
            raise StopIteration

        attempt = self._attempt
        self._attempt += 1
        return attempt

    def success(self, result=None):
        """Mark the operation as successful."""
        self._success = True
        self._result = result

    def retry(self, exception: Exception):
        """Mark that we should retry due to an exception."""
        self._last_exception = exception

        if self._attempt > self.max_retries:
            logger.error(f"{self.operation} failed after {self.max_retries + 1} attempts: {exception}")
            raise exception

        delay = min(self.base_delay * (2 ** (self._attempt - 1)), self.max_delay)
        delay = delay * (0.75 + random.random() * 0.5)  # Add jitter

        logger.warning(
            f"{self.operation} failed (attempt {self._attempt}/{self.max_retries + 1}): {exception}. "
            f"Retrying in {delay:.1f}s..."
        )

        time.sleep(delay)

    @property
    def result(self):
        return self._result

    @property
    def attempts(self) -> int:
        return self._attempt


def retry_api_call(func: Callable[..., T], *args, max_retries: int = 3, **kwargs) -> T:
    """
    Simple retry wrapper for API calls.

    Args:
        func: Function to call
        *args: Positional arguments for func
        max_retries: Maximum retry attempts
        **kwargs: Keyword arguments for func

    Returns:
        Result of successful function call

    Raises:
        Last exception if all retries fail
    """
    import requests  # Import here to avoid circular imports

    retry_exceptions = (
        ConnectionError,
        TimeoutError,
        OSError,
        requests.exceptions.RequestException,
    )

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except retry_exceptions as e:
            last_exception = e
            if attempt >= max_retries:
                raise

            delay = min(1.0 * (2**attempt), 30.0)
            delay = delay * (0.75 + random.random() * 0.5)

            logger.warning(f"API call failed (attempt {attempt + 1}): {e}. Retrying in {delay:.1f}s...")
            time.sleep(delay)

    raise last_exception
