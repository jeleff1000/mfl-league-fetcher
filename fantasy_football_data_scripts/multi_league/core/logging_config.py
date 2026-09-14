#!/usr/bin/env python3
"""
Centralized Logging Configuration for Fantasy Football Data Pipeline

This module provides a unified logging system that replaces scattered print statements
with structured, professional logging. It maintains backward compatibility with the
existing log() function signature while adding modern logging features.

Features:
- Timestamp-prefixed output matching existing format
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Progress tracking for batch operations
- Script lifecycle logging (start/end with timing)
- Optional file logging to {data_directory}/logs/
- Clean, professional console output

Usage:
    from multi_league.core.logging_config import get_logger, log

    # Simple drop-in replacement for print()
    log("Processing data...")

    # Full logger for more control
    logger = get_logger(__name__)
    logger.info("Starting import")
    logger.debug("Debug details", extra={"year": 2024})
    logger.error("Failed", exc_info=True)

    # Progress tracking
    logger.progress(50, 100, "Processing weeks")

    # Script lifecycle
    logger.script_start("initial_import_v2")
    # ... work ...
    logger.script_end("initial_import_v2", success=True, duration=120.5)
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any
from collections.abc import Callable


# =============================================================================
# Third-party noise suppression
# =============================================================================

_noisy_loggers_suppressed = False


def suppress_noisy_loggers() -> None:
    """Suppress DEBUG spam from third-party libraries (yahoo_oauth, urllib3, etc.).

    Safe to call multiple times — only applies once.

    The yahoo_oauth library hardcodes ``logger.setLevel(DEBUG)`` and adds its
    own StreamHandler at import time, so we must force-import it first, then
    strip its handler and raise the level.
    """
    global _noisy_loggers_suppressed
    if _noisy_loggers_suppressed:
        return

    # Force-import so the library's module-level logging setup runs first
    try:
        import yahoo_oauth.oauth  # noqa: F401
    except ImportError:
        pass

    for name in ("yahoo_oauth", "urllib3", "requests", "oauthlib"):
        lgr = logging.getLogger(name)
        lgr.setLevel(logging.WARNING)
        lgr.handlers.clear()  # remove library-added handlers
        lgr.propagate = False  # don't bubble up to root

    _noisy_loggers_suppressed = True


# =============================================================================
# Custom Formatter
# =============================================================================


class PipelineFormatter(logging.Formatter):
    """
    Custom formatter that produces clean, timestamped output.

    Format: [2024-12-21 10:30:45] [INFO] Message
    """

    LEVEL_COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def __init__(self, use_colors: bool = True):
        super().__init__()
        self.use_colors = use_colors and sys.stdout.isatty()

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname

        if self.use_colors and level in self.LEVEL_COLORS:
            level_str = f"{self.LEVEL_COLORS[level]}{level:8}{self.RESET}"
        else:
            level_str = f"{level:8}"

        # Format the base message
        message = record.getMessage()

        # Add extra context if present (but not standard logging extras)
        extra_items = []
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "exc_info",
                "exc_text",
                "thread",
                "threadName",
                "message",
                "taskName",
            ):
                extra_items.append(f"{key}={value}")

        if extra_items:
            message = f"{message} ({', '.join(extra_items)})"

        return f"[{timestamp}] [{level_str}] {message}"


# =============================================================================
# Pipeline Logger Class
# =============================================================================


class PipelineLogger(logging.Logger):
    """
    Extended logger with pipeline-specific methods.

    Adds:
    - progress(): Log progress with percentage
    - script_start(): Log script start with context
    - script_end(): Log script completion with timing
    - section(): Create visual section headers
    """

    def progress(self, current: int, total: int, message: str = "Progress") -> None:
        """
        Log progress with percentage.

        Args:
            current: Current item number
            total: Total items
            message: Description of what's being processed

        Example:
            logger.progress(50, 100, "Processing weeks")
            # Output: [2024-12-21 10:30:45] [INFO    ] [50%] Processing weeks (50/100)
        """
        if total > 0:
            pct = (current / total) * 100
            self.info(f"[{pct:3.0f}%] {message} ({current}/{total})")
        else:
            self.info(f"{message} ({current}/?)")

    def script_start(self, script_name: str, **context: Any) -> float:
        """
        Log script start with context.

        Args:
            script_name: Name of the script starting
            **context: Additional context (year, week, etc.)

        Returns:
            Start time for use with script_end()

        Example:
            start = logger.script_start("yahoo_fantasy_data", year=2024)
        """
        ctx_str = ", ".join(f"{k}={v}" for k, v in context.items()) if context else ""
        if ctx_str:
            self.info(f"[START] {script_name} ({ctx_str})")
        else:
            self.info(f"[START] {script_name}")
        return time.time()

    def script_end(
        self, script_name: str, success: bool = True, duration: float | None = None, start_time: float | None = None
    ) -> None:
        """
        Log script completion with status and timing.

        Args:
            script_name: Name of the script that completed
            success: Whether the script succeeded
            duration: Duration in seconds (if known)
            start_time: Start time from script_start() (used if duration not provided)

        Example:
            logger.script_end("yahoo_fantasy_data", success=True, start_time=start)
        """
        if duration is None and start_time is not None:
            duration = time.time() - start_time

        status = "OK" if success else "FAIL"
        if duration is not None:
            if duration >= 60:
                time_str = f"{duration / 60:.1f}m"
            else:
                time_str = f"{duration:.1f}s"
            self.info(f"[{status}] {script_name} completed in {time_str}")
        else:
            self.info(f"[{status}] {script_name}")

    def section(self, title: str, width: int = 60) -> None:
        """
        Log a section header for visual organization.

        Args:
            title: Section title
            width: Total width of the header line

        Example:
            logger.section("Phase 1: Data Fetchers")
        """
        padding = width - len(title) - 4
        left_pad = padding // 2
        right_pad = padding - left_pad
        self.info("=" * width)
        self.info(f"{'=' * left_pad} {title} {'=' * right_pad}")
        self.info("=" * width)


# =============================================================================
# Logger Factory
# =============================================================================

# Register our custom logger class
logging.setLoggerClass(PipelineLogger)

# Global registry for loggers
_loggers: dict[str, PipelineLogger] = {}

# Default log level
_default_level = logging.INFO


def set_log_level(level: int | str) -> None:
    """
    Set the default log level for new loggers.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL or int)
    """
    global _default_level
    if isinstance(level, str):
        _default_level = getattr(logging, level.upper(), logging.INFO)
    else:
        _default_level = level

    # Update existing loggers
    for logger in _loggers.values():
        logger.setLevel(_default_level)


def get_logger(name: str, log_dir: Path | None = None) -> PipelineLogger:
    """
    Get or create a PipelineLogger for the given name.

    Args:
        name: Logger name (typically __name__)
        log_dir: Optional directory for file logging

    Returns:
        Configured PipelineLogger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Hello")
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.__class__ = PipelineLogger
    logger.setLevel(_default_level)

    # Only add handlers if none exist
    if not logger.handlers:
        # Console handler
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(PipelineFormatter(use_colors=True))
        logger.addHandler(console)

        # File handler (optional)
        if log_dir:
            log_dir = Path(log_dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"{name.replace('.', '_')}.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(PipelineFormatter(use_colors=False))
            logger.addHandler(file_handler)

    # Prevent propagation to root logger (avoid duplicate output)
    logger.propagate = False

    _loggers[name] = logger
    return logger


# =============================================================================
# Backward-Compatible log() Function
# =============================================================================

# Default logger for log() function
_default_logger: PipelineLogger | None = None


def log(*values: object, sep: str = " ", end: str = "\n", file=None, flush: bool = False) -> None:
    """
    Backward-compatible log function matching print() signature.

    This is a drop-in replacement for the existing log() function in script_runner.py.
    It maintains the same signature for backward compatibility while using the new
    logging infrastructure.

    Args:
        values: Values to log (same as print())
        sep: String inserted between values (default: " ")
        end: String appended after the last value (default: "\n")
        file: Ignored (for print() compatibility)
        flush: Ignored (for print() compatibility)

    Example:
        log("Processing", year, "data")
        log("Complete!")
    """
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("pipeline")

    try:
        msg = sep.join(str(v) for v in values)
    except Exception:
        msg = " ".join(map(str, values))

    _default_logger.info(msg)


# =============================================================================
# Convenience Functions
# =============================================================================


def log_debug(*values: object, sep: str = " ") -> None:
    """Log at DEBUG level."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("pipeline")
    msg = sep.join(str(v) for v in values)
    _default_logger.debug(msg)


def log_warning(*values: object, sep: str = " ") -> None:
    """Log at WARNING level."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("pipeline")
    msg = sep.join(str(v) for v in values)
    _default_logger.warning(msg)


def log_error(*values: object, sep: str = " ", exc_info: bool = False) -> None:
    """Log at ERROR level."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("pipeline")
    msg = sep.join(str(v) for v in values)
    _default_logger.error(msg, exc_info=exc_info)


# =============================================================================
# Decorator for Script Timing
# =============================================================================


def timed_script(name: str | None = None) -> Callable:
    """
    Decorator to automatically log script start/end with timing.

    Args:
        name: Script name (defaults to function name)

    Example:
        @timed_script("data_fetcher")
        def run_fetch(ctx):
            # ... work ...
            pass
    """

    def decorator(func: Callable) -> Callable:
        script_name = name or func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__ or "pipeline")
            start = logger.script_start(script_name)
            try:
                result = func(*args, **kwargs)
                logger.script_end(script_name, success=True, start_time=start)
                return result
            except Exception as e:
                logger.script_end(script_name, success=False, start_time=start)
                logger.error(f"Script failed: {e}", exc_info=True)
                raise

        return wrapper

    return decorator


# =============================================================================
# Initialize on Import
# =============================================================================

# Create default logger on import
_default_logger = get_logger("pipeline")
