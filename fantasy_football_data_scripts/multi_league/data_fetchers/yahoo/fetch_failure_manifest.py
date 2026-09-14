"""
Failure Manifest Utility for Yahoo Fetchers

Each fetcher writes a manifest after internal retries exhaust, recording what failed.
The orchestrator (initial_import_v2.py) reads all manifests after Phase 1 and retries.

Manifest location: {data_dir}/.fetch_failures/{fetcher}_{year}.json
"""

import json
from datetime import datetime
from pathlib import Path

MANIFEST_DIR_NAME = ".fetch_failures"


def write_manifest(
    data_dir,
    fetcher: str,
    year: int,
    *,
    failed_weeks: list[int] | None = None,
    failed_details: list[str] | None = None,
    failed_at_offset: int | None = None,
    failed_year: bool = False,
    error: str | None = None,
):
    """
    Write a failure manifest for a fetcher.

    Args:
        data_dir: League data directory (parent of fetcher-specific dirs)
        fetcher: Fetcher name ('matchups', 'rosters', 'transactions', 'draft', 'schedules')
        year: Season year
        failed_weeks: List of weeks that failed (for week-based fetchers)
        failed_details: Exact failed gap details (e.g. wk7_Daniel, txid::123)
        failed_at_offset: Offset where pagination broke (for transactions)
        failed_year: True if entire year failed (for draft)
        error: Error message string
    """
    manifest_dir = Path(data_dir) / MANIFEST_DIR_NAME
    manifest_dir.mkdir(parents=True, exist_ok=True)

    path = manifest_dir / f"{fetcher}_{year}.json"

    # Merge with existing manifest to avoid clobbering previous failure info
    # (e.g., transactions can fail at offset AND have Unknown managers — both matter)
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except Exception:
            pass

    manifest = {
        "fetcher": fetcher,
        "year": year,
    }

    # Merge failed_weeks: union of existing + new
    existing_weeks = set(existing.get("failed_weeks", []))
    new_weeks = set(failed_weeks or [])
    merged_weeks = sorted(existing_weeks | new_weeks)
    if merged_weeks:
        manifest["failed_weeks"] = merged_weeks

    existing_details = set(existing.get("failed_details", []))
    new_details = set(failed_details or [])
    merged_details = sorted(existing_details | new_details)
    if merged_details:
        manifest["failed_details"] = merged_details

    # Keep existing failed_at_offset if not provided in this call
    if failed_at_offset is not None:
        manifest["failed_at_offset"] = failed_at_offset
    elif "failed_at_offset" in existing:
        manifest["failed_at_offset"] = existing["failed_at_offset"]

    if failed_year:
        manifest["failed_year"] = True
    elif existing.get("failed_year"):
        manifest["failed_year"] = True

    # Append error messages (keep both if different)
    if error:
        existing_error = existing.get("error", "")
        new_error = str(error)[:500]
        if existing_error and existing_error != new_error:
            manifest["error"] = f"{existing_error} | {new_error}"[:500]
        else:
            manifest["error"] = new_error
    elif existing.get("error"):
        manifest["error"] = existing["error"]

    comparable_existing = {k: v for k, v in existing.items() if k != "timestamp"}
    if path.exists() and comparable_existing == manifest:
        return False

    manifest["timestamp"] = datetime.now().isoformat()
    path.write_text(json.dumps(manifest, indent=2))
    print(f"  [MANIFEST] Wrote failure manifest: {path.name}")
    return True


def read_all_manifests(data_dir) -> list[dict]:
    """Read all failure manifests from the data directory."""
    manifest_dir = Path(data_dir) / MANIFEST_DIR_NAME
    if not manifest_dir.exists():
        return []
    manifests = []
    for path in sorted(manifest_dir.glob("*.json")):
        try:
            manifests.append(json.loads(path.read_text()))
        except Exception:  # noqa: BLE001
            continue
    return manifests


def clear_manifest(data_dir, fetcher: str, year: int):
    """Remove a specific manifest after successful retry."""
    path = Path(data_dir) / MANIFEST_DIR_NAME / f"{fetcher}_{year}.json"
    if path.exists():
        path.unlink()


def clear_all_manifests(data_dir):
    """Remove all manifests (e.g., at start of fresh import)."""
    manifest_dir = Path(data_dir) / MANIFEST_DIR_NAME
    if manifest_dir.exists():
        for path in manifest_dir.glob("*.json"):
            path.unlink()
        try:
            manifest_dir.rmdir()
        except OSError:
            pass


def run_fetcher_per_year_with_retry(
    script_path: str,
    label: str,
    context_path: str,
    years: list[int],
    extra_args: list[str] = None,
    year_args_fn=None,
    timeout: int = 900,
    max_retries: int = 3,
) -> tuple[list[int], bool]:
    """
    Run a fetcher per-year with escalating backoff retry on failure.

    Replaces the duplicated retry-with-jitter pattern used by rosters and matchups
    in initial_import_v2.py. Can be used by any fetcher that runs year-by-year.

    Args:
        script_path: Path to the fetcher script
        label: Human-readable label for logging (e.g., "Yahoo player data")
        context_path: Path to league_context.json
        years: List of years to fetch
        extra_args: Base extra args passed to all invocations
        year_args_fn: Function(year) -> List[str] for year-specific args.
                      Default: ["--year", str(year)]
        timeout: Per-year timeout in seconds
        max_retries: Max retry attempts (with escalating backoff)

    Returns:
        (failed_years, all_ok) tuple
    """
    import time
    import random
    from multi_league.core.script_runner import run_script, log

    if extra_args is None:
        extra_args = []
    if year_args_fn is None:

        def year_args_fn(y):
            return ["--year", str(y)]

    # First pass: fetch each year
    failed_years = []
    for year in years:
        result = run_script(
            script_path,
            f"{label} ({year})",
            context_path,
            additional_args=extra_args + year_args_fn(year),
            timeout=timeout,
        )
        ok = result[0] if isinstance(result, tuple) else result
        if not ok:
            log(f"[WARN] {label} failed for {year} - will retry later")
            failed_years.append(year)

    # Retry pass: escalating backoff with jitter
    if failed_years:
        for attempt in range(max_retries):
            if not failed_years:
                break
            wait = 60 * (attempt + 1) + random.uniform(0, 10)
            log(f"[RETRY] Waiting {wait:.1f}s before retrying {len(failed_years)} failed year(s): {failed_years}")
            time.sleep(wait)

            still_failed = []
            for year in failed_years:
                result = run_script(
                    script_path,
                    f"{label} ({year}) [retry {attempt + 1}]",
                    context_path,
                    additional_args=extra_args + year_args_fn(year),
                    timeout=timeout,
                )
                ok = result[0] if isinstance(result, tuple) else result
                if ok:
                    log(f"[RETRY] Successfully fetched {year} on retry {attempt + 1}")
                else:
                    still_failed.append(year)
            failed_years = still_failed

    if failed_years:
        log(f"[ERROR] {label} failed for years after {max_retries} retries: {failed_years}")

    return failed_years, len(failed_years) == 0
