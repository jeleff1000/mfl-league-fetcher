"""
Phase 1.9: Post-Fetch Validation Gate

Cross-validates fetched data in LocalLeagueDB to detect silent failures that
individual API fetchers missed (partial responses, rate limit bypasses, team
mapping failures). Returns a list of FetchGap objects describing what's missing,
then retries the affected fetchers using the orchestrator's run_script() pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

from multi_league.core.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FetchGap:
    """A detected gap in fetched data that needs retry."""

    fetcher: str  # "rosters", "transactions", "matchups", "draft", "schedules"
    year: int
    severity: str  # "critical" (blocks enrichments) or "warning" (log but continue)
    description: str
    # Fetcher-specific retry info
    week: int | None = None
    manager: str | None = None
    team_key: str | None = None
    offset: int | None = None

    def __str__(self):
        parts = [f"[{self.fetcher}] {self.year}"]
        if self.week:
            parts.append(f"w{self.week}")
        if self.manager:
            parts.append(self.manager)
        parts.append(f"— {self.description}")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Check 1: Failure manifests
# ---------------------------------------------------------------------------


def check_failure_manifests(data_dir: Path) -> list[FetchGap]:
    """Convert any remaining failure manifests into FetchGap objects."""
    gaps: list[FetchGap] = []

    try:
        from multi_league.data_fetchers.yahoo.fetch_failure_manifest import (
            read_all_manifests,
        )

        manifests = read_all_manifests(data_dir)
    except Exception:
        return gaps

    for m in manifests:
        fetcher = m.get("fetcher", "unknown")
        year = m.get("year", 0)

        if m.get("failed_weeks"):
            for week in m["failed_weeks"]:
                gaps.append(
                    FetchGap(
                        fetcher=fetcher,
                        year=year,
                        week=week,
                        severity="critical",
                        description=f"Failure manifest: {fetcher} week {week}",
                    )
                )
        elif m.get("failed_at_offset") is not None:
            gaps.append(
                FetchGap(
                    fetcher=fetcher,
                    year=year,
                    offset=m["failed_at_offset"],
                    severity="critical",
                    description=(
                        f"Failure manifest: {fetcher} pagination failed " f"at offset {m['failed_at_offset']}"
                    ),
                )
            )
        elif m.get("failed_year"):
            gaps.append(
                FetchGap(
                    fetcher=fetcher,
                    year=year,
                    severity="critical",
                    description=f"Failure manifest: entire {fetcher} year failed",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] {len(gaps)} gaps from failure manifests")
    else:
        logger.info("[Phase 1.9] No failure manifests found")

    return gaps


# ---------------------------------------------------------------------------
# Helper: check if a table exists and has rows
# ---------------------------------------------------------------------------


def _table_exists(db, table_name: str) -> bool:
    """Check if a table exists in the local DuckDB."""
    try:
        result = db.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ? "
            "AND table_catalog = current_database()",
            [table_name],
        ).fetchone()
        return result[0] > 0
    except Exception:
        return False


def _collect_years_from_table(db, table_name: str) -> set:
    """Query a table in the local DuckDB and return the set of years found."""
    try:
        if not _table_exists(db, table_name):
            return set()
        rows = db.conn.execute(f"SELECT DISTINCT year FROM public.{table_name} WHERE year IS NOT NULL").fetchall()
        return {int(r[0]) for r in rows}
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Check 2: Roster coverage — every matchup week/manager needs player data
# ---------------------------------------------------------------------------


def check_roster_coverage(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that every matchup week/manager has started players in player_fantasy."""
    gaps: list[FetchGap] = []

    if not _table_exists(db, "matchup") or not _table_exists(db, "player_fantasy"):
        logger.warning("[Phase 1.9] Cannot check roster coverage — missing matchup or player_fantasy table")
        return gaps

    # Find matchup (year, week, manager) combos with no started players
    try:
        missing_rows = db.conn.execute("""
            SELECT m.year, m.week, m.manager
            FROM (
                SELECT DISTINCT year, week, manager
                FROM public.matchup
                WHERE opponent IS NOT NULL AND TRIM(opponent) != ''
            ) m
            LEFT JOIN (
                SELECT DISTINCT year, week, manager
                FROM public.player_fantasy
                WHERE COALESCE(is_started, 0) = 1
                   OR fantasy_position IS NOT NULL
            ) p ON m.year = p.year AND m.week = p.week AND m.manager = p.manager
            WHERE p.year IS NULL
            ORDER BY m.year, m.week, m.manager
        """).fetchall()
    except Exception as e:
        logger.warning(f"[Phase 1.9] Error checking roster coverage: {e}")
        return gaps

    for row in missing_rows:
        year, week, manager = int(row[0]), int(row[1]), row[2]
        if year in years:
            gaps.append(
                FetchGap(
                    fetcher="rosters",
                    year=year,
                    week=week,
                    manager=manager,
                    severity="critical",
                    description=f"Matchup exists but no started players for {manager}",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] Found {len(gaps)} matchup weeks with no player data")
    else:
        logger.info("[Phase 1.9] Roster coverage: OK — all matchup weeks have player data")

    return gaps


# ---------------------------------------------------------------------------
# Check 3: Transaction managers — no Unknown/null managers
# ---------------------------------------------------------------------------


def check_transaction_managers(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that no transactions have 'Unknown' manager or null manager."""
    gaps: list[FetchGap] = []

    if not _table_exists(db, "transactions"):
        return gaps

    try:
        rows = db.conn.execute("""
            SELECT year, COUNT(*) as cnt
            FROM public.transactions
            WHERE (manager IS NULL OR manager = 'Unknown')
            GROUP BY year
        """).fetchall()
    except Exception:
        return gaps

    for row in rows:
        year = int(row[0])
        count = int(row[1])
        if year not in years:
            continue
        gaps.append(
            FetchGap(
                fetcher="transactions",
                year=year,
                severity="warning",
                description=f"{count} transactions with Unknown/null manager",
            )
        )

    if gaps:
        logger.warning(f"[Phase 1.9] Found {len(gaps)} years with Unknown transaction managers")
    else:
        logger.info("[Phase 1.9] Transaction managers: OK — all resolved")

    return gaps


# ---------------------------------------------------------------------------
# Check 4: Transaction year coverage
# ---------------------------------------------------------------------------


def check_transaction_years(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that every year has transaction data (no missing years)."""
    gaps: list[FetchGap] = []

    fetched_years = _collect_years_from_table(db, "transactions")

    for year in years:
        if year not in fetched_years:
            gaps.append(
                FetchGap(
                    fetcher="transactions",
                    year=year,
                    severity="critical",
                    description=f"No transaction data for {year}",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] Missing transaction years: {[g.year for g in gaps]}")
    else:
        logger.info("[Phase 1.9] Transaction years: OK — all years present")

    return gaps


# ---------------------------------------------------------------------------
# Check 5: Matchup year coverage
# ---------------------------------------------------------------------------


def check_matchup_years(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that every year has matchup data."""
    gaps: list[FetchGap] = []

    fetched_years = _collect_years_from_table(db, "matchup")

    for year in years:
        if year not in fetched_years:
            gaps.append(
                FetchGap(
                    fetcher="matchups",
                    year=year,
                    severity="critical",
                    description=f"No matchup data for {year}",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] Missing matchup years: {[g.year for g in gaps]}")
    else:
        logger.info("[Phase 1.9] Matchup years: OK")

    return gaps


# ---------------------------------------------------------------------------
# Check 6: Draft year coverage
# ---------------------------------------------------------------------------


def check_draft_years(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that every year has draft data. Warning severity (some leagues lack drafts)."""
    gaps: list[FetchGap] = []

    fetched_years = _collect_years_from_table(db, "draft")

    for year in years:
        if year not in fetched_years:
            gaps.append(
                FetchGap(
                    fetcher="draft",
                    year=year,
                    severity="warning",
                    description=f"No draft data for {year}",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] Missing draft years: {[g.year for g in gaps]}")
    else:
        logger.info("[Phase 1.9] Draft years: OK")

    return gaps


# ---------------------------------------------------------------------------
# Check 7: Schedule year coverage
# ---------------------------------------------------------------------------


def check_schedule_years(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Check that every year has schedule data. Warning severity."""
    gaps: list[FetchGap] = []

    fetched_years = _collect_years_from_table(db, "schedule")

    for year in years:
        if year not in fetched_years:
            gaps.append(
                FetchGap(
                    fetcher="schedules",
                    year=year,
                    severity="warning",
                    description=f"No schedule data for {year}",
                )
            )

    if gaps:
        logger.warning(f"[Phase 1.9] Missing schedule years: {[g.year for g in gaps]}")
    else:
        logger.info("[Phase 1.9] Schedule years: OK")

    return gaps


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_all_checks(db, years: list, data_dir: Path | None = None) -> list[FetchGap]:
    """Run all cross-validation checks and return combined, deduplicated gaps.

    Args:
        db: LocalLeagueDB instance (or any object with a `conn` DuckDB connection).
        years: List of seasons to validate.
        data_dir: Optional data directory (used only for failure manifest check).
    """
    logger.info("=" * 80)
    logger.info("PHASE 1.9: POST-FETCH VALIDATION GATE")
    logger.info("=" * 80)

    all_gaps: list[FetchGap] = []
    if data_dir:
        all_gaps.extend(check_failure_manifests(Path(data_dir)))
    all_gaps.extend(check_roster_coverage(db, years))
    all_gaps.extend(check_transaction_managers(db, years))
    all_gaps.extend(check_transaction_years(db, years))
    all_gaps.extend(check_matchup_years(db, years))
    all_gaps.extend(check_draft_years(db, years))
    all_gaps.extend(check_schedule_years(db, years))

    # Deduplicate (same fetcher+year+week+manager)
    seen: set = set()
    deduped: list[FetchGap] = []
    for gap in all_gaps:
        key = (gap.fetcher, gap.year, gap.week, gap.manager)
        if key not in seen:
            seen.add(key)
            deduped.append(gap)

    critical = [g for g in deduped if g.severity == "critical"]
    warnings = [g for g in deduped if g.severity == "warning"]

    logger.info(f"[Phase 1.9] Detected {len(critical)} critical gaps, {len(warnings)} warnings")
    for gap in deduped:
        logger.info(f"  {'!!' if gap.severity == 'critical' else '  '} {gap}")

    return deduped


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

# Default fetcher script paths + timeouts (same as Phase 1.5 in initial_import_v2)
DEFAULT_FETCHER_SCRIPTS: dict[str, tuple[str, int]] = {
    "matchups": ("multi_league/data_fetchers/yahoo/yahoo_matchups.py", 1200),
    "rosters": ("multi_league/data_fetchers/yahoo/yahoo_rosters.py", 900),
    "transactions": ("multi_league/data_fetchers/yahoo/yahoo_transactions.py", 1800),
    "draft": ("multi_league/data_fetchers/yahoo/yahoo_draft.py", 900),
    "schedules": ("multi_league/data_fetchers/yahoo/yahoo_schedules.py", 600),
}


def _retry_gaps(
    gaps: list[FetchGap],
    run_script_fn: Callable,
    context_path: Path,
    fetcher_scripts: dict[str, tuple[str, int]],
    extra_args: list[str] | None = None,
) -> list[FetchGap]:
    """Retry fetchers for detected gaps and return gaps that remain unresolved.

    Uses the orchestrator's ``run_script()`` callable so there is zero coupling
    to fetcher class internals — each retry is a subprocess invocation identical
    to how Phase 1 originally ran the fetcher.
    """
    if extra_args is None:
        extra_args = []

    # Collapse gaps to unique (fetcher, year) pairs — re-running a fetcher for
    # a year fixes all week-level / manager-level gaps in that year.
    retry_targets: dict[tuple[str, int], list[FetchGap]] = {}
    for gap in gaps:
        key = (gap.fetcher, gap.year)
        retry_targets.setdefault(key, []).append(gap)

    still_failed: list[FetchGap] = []

    for (fetcher, year), year_gaps in sorted(retry_targets.items()):
        if fetcher not in fetcher_scripts:
            logger.warning(f"[Phase 1.9 retry] No script configured for fetcher '{fetcher}' — skipping")
            still_failed.extend(year_gaps)
            continue

        script_path, timeout = fetcher_scripts[fetcher]
        retry_args = list(extra_args) + ["--year", str(year)]

        # Rosters need --week 0 to re-fetch all weeks for the year
        if fetcher == "rosters":
            retry_args += ["--week", "0"]

        logger.info(f"[Phase 1.9 retry] {fetcher} {year} ...")
        result = run_script_fn(
            script_path,
            f"Phase1.9 {fetcher} retry ({year})",
            str(context_path),
            additional_args=retry_args,
            timeout=timeout,
        )
        ok = result[0] if isinstance(result, tuple) else result
        if ok:
            logger.info(f"  [OK] {fetcher} {year} retry succeeded")
        else:
            logger.warning(f"  [FAIL] {fetcher} {year} retry failed")
            still_failed.extend(year_gaps)

    return still_failed


def validate_and_retry(
    db,
    years: list,
    run_script_fn: Callable,
    context_path: Path,
    data_dir: Path | None = None,
    fetcher_scripts: dict[str, tuple[str, int]] | None = None,
    extra_args: list[str] | None = None,
    max_rounds: int = 10,
) -> bool:
    """Run all validation checks and tenaciously retry critical gaps.

    Returns ``True`` if all critical gaps are resolved (warnings are logged but
    do not block the pipeline).

    Strategy: escalating cooldowns (30s -> 60s -> 120s -> 300s -> 300s ...).
    Only gives up on a gap when the SAME gap fails in TWO consecutive rounds,
    meaning the API definitively cannot serve the data (not just rate-limited).
    Keeps retrying everything else until resolved or max_rounds hit.

    Parameters
    ----------
    db:
        LocalLeagueDB instance (or any object with a `conn` DuckDB connection).
    years:
        List of seasons to validate.
    run_script_fn:
        The orchestrator's ``run_script()`` function (subprocess-based).
    context_path:
        Path to the league_context.json used by fetcher scripts.
    data_dir:
        Optional data directory (used for failure manifest check).
    fetcher_scripts:
        Mapping of fetcher name -> (script_path, timeout).  Falls back to
        ``DEFAULT_FETCHER_SCRIPTS`` if not supplied.
    extra_args:
        Additional CLI args forwarded to every fetcher invocation (e.g.
        ``["--dry-run"]``).
    max_rounds:
        Maximum retry rounds before giving up (default 10 -- be tenacious).
    """
    import time as _time

    if fetcher_scripts is None:
        fetcher_scripts = DEFAULT_FETCHER_SCRIPTS

    cooldowns = [30, 60, 120, 300]  # Escalating, then 300s for all subsequent

    # Track gaps that failed in the PREVIOUS round — only give up on a gap
    # when it fails in two consecutive rounds (API truly can't serve it)
    prev_round_gap_keys: set[tuple] = set()
    permanently_failed: list[FetchGap] = []

    for round_num in range(1, max_rounds + 1):
        all_gaps = run_all_checks(db, years, data_dir=data_dir)
        critical = [g for g in all_gaps if g.severity == "critical"]

        # Exclude permanently failed gaps (API confirmed unavailable)
        perm_keys = {(g.fetcher, g.year, g.week, g.manager) for g in permanently_failed}
        retryable = [g for g in critical if (g.fetcher, g.year, g.week, g.manager) not in perm_keys]

        if not retryable:
            if round_num == 1 and not permanently_failed:
                logger.info("[Phase 1.9] All validation checks passed — no retries needed")
            elif not permanently_failed:
                logger.info(f"[Phase 1.9] All critical gaps resolved after {round_num - 1} retry round(s)")
            else:
                logger.info(
                    f"[Phase 1.9] Retryable gaps resolved. "
                    f"{len(permanently_failed)} gap(s) confirmed unavailable from API."
                )
            break

        cooldown = cooldowns[min(round_num - 1, len(cooldowns) - 1)]
        logger.info(
            f"[Phase 1.9] Round {round_num}/{max_rounds}: " f"{len(retryable)} gap(s) to retry, cooldown {cooldown}s..."
        )
        _time.sleep(cooldown)

        remaining = _retry_gaps(retryable, run_script_fn, context_path, fetcher_scripts, extra_args)

        # Check which gaps failed in BOTH this round and last round
        this_round_gap_keys = {(g.fetcher, g.year, g.week, g.manager) for g in remaining}
        consecutive_failures = this_round_gap_keys & prev_round_gap_keys

        if consecutive_failures:
            # These gaps failed twice in a row — API can't serve this data
            newly_permanent = [g for g in remaining if (g.fetcher, g.year, g.week, g.manager) in consecutive_failures]
            for g in newly_permanent:
                logger.warning(f"  [PERMANENT] {g} — failed 2 consecutive rounds, API cannot serve this data")
            permanently_failed.extend(newly_permanent)

        prev_round_gap_keys = this_round_gap_keys

        if not remaining:
            # Re-validate to confirm the retries actually fixed the data
            continue

    # Final report
    if permanently_failed:
        logger.warning(f"[Phase 1.9] {len(permanently_failed)} gap(s) confirmed unavailable from Yahoo API:")
        for gap in permanently_failed:
            logger.warning(f"  !! {gap}")
        logger.warning("[Phase 1.9] Proceeding with available data — these gaps cannot be recovered")
        return False

    return True
