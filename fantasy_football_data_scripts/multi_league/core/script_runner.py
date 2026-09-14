#!/usr/bin/env python3
"""
Script Runner Utilities

Provides utilities for orchestrating multiple child scripts with retry logic,
rate limiting, OAuth environment setup, and comprehensive logging.

Extracted from initial_import_v2.py for reusability across multiple orchestration scripts.
"""

from __future__ import annotations

import sys
import subprocess
import os
import json
import time
from pathlib import Path

# Import centralized logging
from multi_league.core.logging_config import log

_verbose = "--verbose" in sys.argv

# Try to import LeagueContext; gracefully handle if not available
try:
    from multi_league.core.league_context import LeagueContext
except ImportError:
    LeagueContext = None


def script_supports_flag(script: Path, flag: str, timeout: int = 10) -> bool:
    """
    Return True if running `script --help` mentions `flag` (defensive).

    This is used to avoid passing unsupported flags to legacy scripts.

    Args:
        script: Path to the script
        flag: Flag to check for (e.g., "--year")
        timeout: Timeout in seconds for the help command

    Returns:
        True if the flag is mentioned in the help text, False otherwise
    """
    try:
        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        help_text = (result.stdout or "") + "\n" + (result.stderr or "")
        return flag in help_text
    except Exception:
        return False


def setup_oauth_environment(context_path: str) -> dict:
    """
    Extract OAuth credentials from league context and set up environment variables.

    This centralizes OAuth setup logic that was previously duplicated in run_script().

    Args:
        context_path: Path to league_context.json

    Returns:
        Dictionary of environment variables to use for child processes
    """
    env = dict(os.environ)

    # CRITICAL: Set PYTHONPATH so child scripts can find multi_league package
    # The scripts directory is the parent of multi_league/core/
    scripts_dir = Path(__file__).parent.parent.parent
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{scripts_dir}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = str(scripts_dir)

    if LeagueContext is None:
        log("[ENV] LeagueContext not available; skipping OAuth setup")
        return env

    try:
        ctx = LeagueContext.load(context_path)
        oauth_file = getattr(ctx, "oauth_file_path", None)

        if oauth_file:
            oauth_path = Path(oauth_file)
            if oauth_path.exists():
                env["OAUTH_PATH"] = str(oauth_path)
                if _verbose:
                    log(f"[ENV] OAUTH_PATH={oauth_path}")

                try:
                    token_json = json.loads(oauth_path.read_text(encoding="utf-8"))
                    if token_json.get("access_token"):
                        env["YAHOO_ACCESS_TOKEN"] = token_json["access_token"]
                    if token_json.get("refresh_token"):
                        env["YAHOO_REFRESH_TOKEN"] = token_json["refresh_token"]
                    if token_json.get("consumer_key"):
                        env["YAHOO_CONSUMER_KEY"] = token_json["consumer_key"]
                    if token_json.get("consumer_secret"):
                        env["YAHOO_CONSUMER_SECRET"] = token_json["consumer_secret"]
                    if token_json.get("token_type"):
                        env["YAHOO_TOKEN_TYPE"] = token_json["token_type"]
                    env.setdefault("YAHOO_OAUTH_ACCESS_TOKEN", env.get("YAHOO_ACCESS_TOKEN", ""))
                    env.setdefault("OAUTH_TOKEN", env.get("YAHOO_ACCESS_TOKEN", ""))
                except Exception as e:
                    log(f"[ENV] OAuth JSON load failed: {e}")
    except Exception as e:
        log(f"[ENV] Context load failed (continuing without OAuth env): {e}")

    return env


def _make_base_env() -> dict:
    """Build a base environment with PYTHONPATH set (no OAuth setup)."""
    env = dict(os.environ)
    scripts_dir = Path(__file__).parent.parent.parent
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{scripts_dir}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = str(scripts_dir)
    return env


def run_script(
    script_path: str,
    label: str,
    context_path: str,
    additional_args: list[str] | None = None,
    timeout: int | None = 900,
    oauth_env: dict | None = None,
    *,
    db_name: str | None = None,
    data_dir: str | None = None,
    quick: bool = False,
) -> tuple[bool, str | None]:
    """
    Execute a child script with retry logic for rate limits.

    Features:
    - Automatic retry with exponential backoff for rate limit errors
    - OAuth environment setup (if oauth_env provided)
    - Defensive flag filtering for legacy scripts
    - Comprehensive logging
    - Optional --db/--data-dir mode (parquet-free pipeline)

    Args:
        script_path: Relative path to script from SCRIPT_DIR
        label: Human-readable label for logging
        context_path: Path to league_context.json (used when db_name/data_dir not set)
        additional_args: Extra CLI arguments to pass
        timeout: Timeout in seconds (default: 900), or None for no timeout
        oauth_env: Pre-configured OAuth environment (if None, will be set up)
        db_name: League database name. When provided with data_dir,
                 passes --db/--data-dir instead of --context.
        data_dir: Local data directory path. Used with db_name.
        quick: If True and using --db/--data-dir mode, also passes --quick.

    Returns:
        Tuple of (success: bool, stderr: Optional[str])

    Example:
        >>> ok, err = run_script(
        ...     "multi_league/data_fetchers/yahoo_fantasy_data.py",
        ...     "Yahoo player data",
        ...     "/path/to/context.json",
        ...     additional_args=["--year", "2024"]
        ... )
        >>> # Parquet-free mode:
        >>> ok, err = run_script(
        ...     "multi_league/transformations/matchup/modules/playoff_flags.py",
        ...     "Playoff flags",
        ...     "",
        ...     db_name="the_league", data_dir="/tmp/data",
        ... )
    """
    # Get script directory (assume this module is in multi_league/core/)
    SCRIPT_DIR = Path(__file__).parent.parent.parent

    def _run_once() -> tuple[bool, str | None]:
        script = SCRIPT_DIR / script_path
        if not script.exists():
            log(f"[SKIP] Script not found: {script}")
            return False, None

        # For our owned scripts, do NOT filter flags. They all support --year/--week/etc.
        KNOWN_SAFE = {
            "yahoo_fantasy_data_v2.py",
            "yahoo_fantasy_data.py",
            "nfl_offense_stats_v2.py",
            "nfl_offense_stats.py",
            "defense_stats_v2.py",
            "defense_stats.py",
            "yahoo_nfl_merge_v3.py",
            "yahoo_nfl_merge_v2.py",
            "yahoo_nfl_merge.py",
            "weekly_matchup_data_v2.py",
            "draft_data_v2.py",
            "transactions_v2.py",
            "combine_dst_to_nfl.py",
            # Yahoo fetchers (multi_league/data_fetchers/yahoo/)
            "yahoo_matchups.py",
            "yahoo_rosters.py",
            "yahoo_draft.py",
            "yahoo_transactions.py",
            "yahoo_schedules.py",
            # Sleeper fetchers (multi_league/data_fetchers/sleeper/)
            "sleeper_matchups.py",
            "sleeper_rosters.py",
            "sleeper_draft.py",
            "sleeper_transactions.py",
            "sleeper_schedules.py",
            "sleeper_traded_picks.py",
            # NFL data loaders
            "load_nfl_from_super_table.py",
        }

        filtered_args: list[str] = []
        if additional_args:
            if script.name in KNOWN_SAFE:
                filtered_args = list(additional_args)
            else:
                # Defensive filtering: if a flag is skipped, also skip its following value.
                idx = 0
                while idx < len(additional_args):
                    token = additional_args[idx]
                    if token.startswith("--"):
                        if script_supports_flag(script, token.split("=")[0]):
                            filtered_args.append(token)
                        else:
                            log(f"      [INFO] Skipping unsupported flag {token} for {script.name}")
                            # also skip next token if it is a value (not another flag)
                            nxt = additional_args[idx + 1] if idx + 1 < len(additional_args) else None
                            if nxt and not str(nxt).startswith("--"):
                                idx += 1
                    else:
                        filtered_args.append(token)
                    idx += 1

        # Build command: --db/--data-dir mode or legacy --context mode
        if db_name and data_dir:
            cmd = [sys.executable, str(script), "--db", db_name, "--data-dir", str(data_dir)]
            if quick:
                cmd.append("--quick")
        else:
            cmd = [sys.executable, str(script), "--context", context_path]
        if filtered_args:
            cmd.extend(filtered_args)

        # Use provided OAuth env or set up new one.
        # In --db/--data-dir mode, transforms don't need OAuth so just set PYTHONPATH.
        if db_name and data_dir:
            env = oauth_env if oauth_env is not None else _make_base_env()
        else:
            env = oauth_env if oauth_env is not None else setup_oauth_environment(context_path)

        log(f"[RUN] {label}")
        if _verbose:
            log(f"      Command: {' '.join(cmd)}")

        try:
            # Use Popen to stream output in real-time (helps debug CI timeouts)
            # Output is streamed as produced so we can see where scripts hang
            process = subprocess.Popen(
                cmd,
                cwd=str(script.parent),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
            )

            stdout_lines = []
            stderr_lines = []

            # Use threading for non-blocking I/O
            import threading
            from queue import Queue, Empty

            def enqueue_output(pipe, queue, label_type):
                """Read lines from pipe and put them in queue."""
                try:
                    for line in iter(pipe.readline, ""):
                        queue.put((label_type, line.rstrip()))
                    pipe.close()
                except Exception:  # noqa: broad-except
                    pass

            stdout_queue = Queue()
            stderr_queue = Queue()

            stdout_thread = threading.Thread(target=enqueue_output, args=(process.stdout, stdout_queue, "stdout"))
            stderr_thread = threading.Thread(target=enqueue_output, args=(process.stderr, stderr_queue, "stderr"))
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()

            # Stream output in real-time with timeout
            start_time = time.time()
            while True:
                # Check for timeout
                if timeout and (time.time() - start_time) > timeout:
                    process.kill()
                    log(f"[TIMEOUT] {label} timed out after {timeout}s")
                    return False, None

                # Check if process has completed
                retcode = process.poll()

                # Drain the queues
                while True:
                    try:
                        label_type, line = stdout_queue.get_nowait()
                        stdout_lines.append(line)
                        log(f"      {line}")
                    except Empty:
                        break

                while True:
                    try:
                        label_type, line = stderr_queue.get_nowait()
                        stderr_lines.append(line)
                    except Empty:
                        break

                if retcode is not None:
                    # Process finished, wait a moment for threads to finish
                    stdout_thread.join(timeout=1.0)
                    stderr_thread.join(timeout=1.0)

                    # Drain any remaining output
                    while True:
                        try:
                            label_type, line = stdout_queue.get_nowait()
                            stdout_lines.append(line)
                            log(f"      {line}")
                        except Empty:
                            break

                    while True:
                        try:
                            label_type, line = stderr_queue.get_nowait()
                            stderr_lines.append(line)
                        except Empty:
                            break

                    break

                # Small sleep to avoid busy-waiting
                time.sleep(0.05)

            if retcode != 0:
                if stderr_lines:
                    log(f"[ERROR] {label} failed with exit code {retcode}")
                    for line in stderr_lines[-25:]:
                        log(f"      {line}")
                return False, "\n".join(stderr_lines)

            log(f"[OK] Completed: {label}")
            return True, None

        except subprocess.TimeoutExpired:
            timeout_msg = f"{timeout}s" if timeout is not None else "unlimited"
            log(f"[TIMEOUT] {label} timed out after {timeout_msg}")
            return False, None
        except Exception as e:
            log(f"[FAIL] Error running {label}: {e}")
            return False, None

    # Retry logic for transient Yahoo throttling / temporary access denial
    MAX_RETRIES = 3
    RATE_LIMIT_COOLDOWN = 600  # 10 minutes in seconds

    last_stderr = None
    for attempt in range(MAX_RETRIES):
        ok, stderr = _run_once()
        last_stderr = stderr

        if ok:
            return True, None

        # Check if it's a retryable Yahoo throttling or temporary access-denial error
        if stderr and (
            "Request denied" in stderr
            or "Forbidden access" in stderr
            or "APITimeoutError" in stderr
            or "429" in stderr
            or "rate limit" in stderr.lower()
        ):
            if attempt < MAX_RETRIES - 1:  # Don't sleep on last attempt
                log(
                    f"[RETRYABLE API] Yahoo throttling or temporary access denial detected. Waiting {RATE_LIMIT_COOLDOWN // 60} minutes before retry {attempt + 2}/{MAX_RETRIES}..."
                )
                time.sleep(RATE_LIMIT_COOLDOWN)
                log(f"[RETRY] Retrying {label} (attempt {attempt + 2}/{MAX_RETRIES})")
                continue
            else:
                log(f"[FAIL] Max retries reached for {label} after retryable Yahoo API failures")
                return False, stderr
        else:
            # Not a rate limit error, don't retry
            return False, stderr

    return False, last_stderr


def run_scripts_parallel(
    scripts: list[tuple[str, str]],
    context_path: str,
    additional_args: list[str] | None = None,
    timeout: int | None = 900,
) -> dict:
    """
    Run multiple scripts in parallel (future enhancement).

    Currently runs sequentially; can be enhanced with ThreadPoolExecutor.

    Args:
        scripts: List of (script_path, label) tuples
        context_path: Path to league_context.json
        additional_args: Extra CLI arguments for all scripts
        timeout: Timeout per script (or None for no timeout)

    Returns:
        Dictionary mapping labels to success/failure status
    """
    # Set up OAuth environment once for all scripts
    oauth_env = setup_oauth_environment(context_path)

    results = {}
    for script_path, label, *_rest in scripts:
        ok, _ = run_script(script_path, label, context_path, additional_args, timeout, oauth_env)
        results[label] = ok

    return results
