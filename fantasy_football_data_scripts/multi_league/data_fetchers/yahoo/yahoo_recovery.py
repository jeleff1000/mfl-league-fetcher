"""Yahoo Recovery Utility — patch exact Yahoo gaps back into local DuckDB.

This module runs after Yahoo fetchers finish and before SQL enrichments/publish.
It detects missing data from:
  1. fetch failure manifests (.fetch_failures/*.json)
  2. the current local DuckDB state

Then it retries only the missing units of work and merges the recovered rows
back into local DuckDB using natural-key dedup.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from multi_league.data_fetchers.yahoo.yahoo_recovery_api import (
    RecoveryDeferredError,
    fetch_transactions_for_year_recovery,
    is_recovery_deferred_error,
)
from multi_league.data_fetchers.yahoo.fetch_failure_manifest import (
    MANIFEST_DIR_NAME,
    clear_all_manifests,
    clear_manifest,
    write_manifest,
)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

_ROSTER_MANAGER_WEEK_PREFIX = "mw::"
_ROSTER_GAP_RE = re.compile(r"wk(\d+)(?:_(.+))?$")
_NON_RETRYABLE_ERROR_MARKERS = (
    "missing schema key columns",
    "requires a local duckdb target",
    "no matchup data found",
)

# ── Fetcher name normalization ──────────────────────────────────────────────
_FETCHER_NORMALIZE = {
    "rosters": "roster",
    "matchups": "matchup",
    "transactions": "transaction",
    "schedules": "schedule",
    # Already singular — pass through
    "roster": "roster",
    "matchup": "matchup",
    "transaction": "transaction",
    "schedule": "schedule",
    "draft": "draft",
}


def _normalize_fetcher(name: str) -> str:
    """Normalize plural fetcher names to singular canonical form."""
    return _FETCHER_NORMALIZE.get(name, name)


def _is_non_retryable_recovery_error(exc: BaseException | str | None) -> bool:
    """Return True when a gap failure should be marked permanently missing."""
    if exc is None:
        return False
    text = str(exc).strip().lower()
    return any(marker in text for marker in _NON_RETRYABLE_ERROR_MARKERS)


# =============================================================================
# Task 1: Gap Data Model
# =============================================================================


@dataclass
class Gap:
    """Represents a single piece of missing data."""

    fetcher: str  # "roster", "transaction", "matchup", "draft", "schedule"
    year: int
    detail: str  # week/team_key combo, transaction offset, or "full_year"
    source: str  # "manifest" or "audit"
    attempts: int = 0
    hard_missing: bool = False
    last_error: str | None = None

    @property
    def key(self) -> tuple:
        """Dedup key — same (fetcher, year, detail) = same gap regardless of source."""
        return (self.fetcher, self.year, self.detail)


@dataclass
class RecoveryResult:
    """Summary of a recovery run."""

    success: bool
    resolved: list[Gap] = field(default_factory=list)
    unresolved: list[Gap] = field(default_factory=list)
    hard_missing: list[Gap] = field(default_factory=list)
    rounds: int = 0
    elapsed_seconds: float = 0.0


# =============================================================================
# Task 2: Manifest Reader
# =============================================================================


def _detect_gaps_from_manifests(data_dir: Path) -> list[Gap]:
    """
    Read all .fetch_failures/*.json manifests and convert to Gap objects.

    Manifest schema (from fetch_failure_manifest.py):
      - fetcher: str (plural form, e.g. "rosters")
      - year: int
      - failed_weeks: list[int]        (week-based fetchers)
      - failed_at_offset: int           (transactions pagination)
      - failed_year: bool               (entire year failed)
    """
    data_dir = Path(data_dir)
    manifest_dir = data_dir / ".fetch_failures"
    if not manifest_dir.exists():
        return []

    gaps: list[Gap] = []
    for path in sorted(manifest_dir.glob("*.json")):
        try:
            manifest = json.loads(path.read_text())
        except Exception:
            logger.warning("Skipping malformed manifest: %s", path.name)
            continue

        fetcher = _normalize_fetcher(manifest.get("fetcher", ""))
        year = manifest.get("year")
        if not fetcher or year is None:
            logger.warning("Manifest missing fetcher/year: %s", path.name)
            continue

        # Convert failed_weeks → one gap per week
        for week in manifest.get("failed_weeks", []):
            gaps.append(Gap(fetcher=fetcher, year=year, detail=f"wk{week}", source="manifest"))

        # Convert failed_at_offset → one gap
        for detail in manifest.get("failed_details", []):
            if detail:
                gaps.append(Gap(fetcher=fetcher, year=year, detail=str(detail), source="manifest"))

        offset = manifest.get("failed_at_offset")
        if offset is not None:
            gaps.append(
                Gap(
                    fetcher=fetcher,
                    year=year,
                    detail=f"offset_{offset}",
                    source="manifest",
                )
            )

        # Convert failed_year → one gap
        if manifest.get("failed_year"):
            gaps.append(Gap(fetcher=fetcher, year=year, detail="full_year", source="manifest"))

    return gaps


# =============================================================================
# Task 3: Parquet Audit
# =============================================================================


def _detect_gaps_from_audit(data_dir: Path, league_settings: dict) -> list[Gap]:
    """
    Cross-reference parquet files against league_settings to find missing data.

    Only checks years present in league_settings (handles year gaps like
    l_5_towns_football skipping 2011).

    Checks:
      - Matchups: every year has matchup parquet with rows
      - Transactions: every year has >0 rows when sibling years have data
      - Rosters: every (year, week, manager) in matchups has player rows
      - Schedules: every year has schedule data
      - Draft: warning-only (logs but does NOT add gaps)
    """
    data_dir = Path(data_dir)
    settings_years = sorted(int(y) for y in league_settings.keys())
    gaps: list[Gap] = []

    # ── Matchup audit ───────────────────────────────────────────────────────
    matchup_dir = data_dir / "matchup_data"
    matchup_data_by_year: dict[int, pd.DataFrame] = {}
    for year in settings_years:
        parquet_path = matchup_dir / f"matchup_data_week_all_year_{year}.parquet"
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                if len(df) > 0:
                    matchup_data_by_year[year] = df
                    continue
            except Exception:
                pass
        # Also try globbing for any matchup file with this year
        found = False
        if matchup_dir.exists():
            for p in matchup_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(p)
                    yr_df = df[df["year"] == year] if "year" in df.columns else pd.DataFrame()
                    if len(yr_df) > 0:
                        matchup_data_by_year[year] = yr_df
                        found = True
                        break
                except Exception:
                    continue
        if not found:
            gaps.append(Gap(fetcher="matchup", year=year, detail="full_year", source="audit"))
            logger.info("Audit: missing matchup data for year %d", year)

    # ── Transaction audit ───────────────────────────────────────────────────
    tx_dir = data_dir / "transaction_data"
    tx_counts: dict[int, int] = {}
    for year in settings_years:
        parquet_path = tx_dir / f"transactions_year_{year}.parquet"
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                tx_counts[year] = len(df)
            except Exception:
                tx_counts[year] = 0
        else:
            tx_counts[year] = 0

    # Only flag years with 0 transactions if at least one sibling year has data
    has_any_tx = any(c > 0 for c in tx_counts.values())
    if has_any_tx:
        for year in settings_years:
            if tx_counts.get(year, 0) == 0:
                gaps.append(Gap(fetcher="transaction", year=year, detail="full_year", source="audit"))
                logger.info("Audit: missing/empty transactions for year %d", year)

    # ── Roster/player audit ─────────────────────────────────────────────────
    player_dir = data_dir / "player_data"
    for year in settings_years:
        matchup_df = matchup_data_by_year.get(year)
        if matchup_df is None:
            continue  # Already flagged as missing matchup

        # Load player data for this year
        player_path = player_dir / f"yahoo_player_stats_{year}_all_weeks.parquet"
        if not player_path.exists():
            # All weeks missing for this year — one gap per matchup week
            matchup_weeks = sorted(matchup_df["week"].unique()) if "week" in matchup_df.columns else []
            for wk in matchup_weeks:
                gaps.append(Gap(fetcher="roster", year=year, detail=f"wk{wk}", source="audit"))
            if matchup_weeks:
                logger.info("Audit: missing all roster data for year %d", year)
            continue

        try:
            player_df = pd.read_parquet(player_path)
        except Exception:
            continue

        # Check each (week, manager) combo from matchups has player data
        # Only flag roster gaps confirmed by manifests OR when entire weeks are missing
        # (not individual manager mismatches which can be name normalization differences)
        if "week" in matchup_df.columns and "week" in player_df.columns:
            matchup_weeks = set(int(w) for w in matchup_df["week"].unique())
            player_weeks = set(int(w) for w in player_df["week"].unique())

            # Flag weeks completely missing from player data
            missing_weeks = matchup_weeks - player_weeks
            for wk in sorted(missing_weeks):
                gaps.append(Gap(fetcher="roster", year=year, detail=f"wk{wk}", source="audit"))
                logger.info("Audit: missing ALL roster data for year %d week %d", year, wk)

            # For weeks that DO have player data, check if any manager is completely absent
            # Use normalized names to avoid false positives from case/spacing differences
            if "manager" in matchup_df.columns and "manager" in player_df.columns:
                for wk in sorted(matchup_weeks & player_weeks):
                    matchup_mgrs = {
                        str(m).strip().lower()
                        for m in matchup_df.loc[matchup_df["week"] == wk, "manager"]
                        if pd.notna(m) and str(m).strip()
                    }
                    player_mgrs = {
                        str(m).strip().lower()
                        for m in player_df.loc[player_df["week"] == wk, "manager"]
                        if pd.notna(m) and str(m).strip()
                    }
                    missing_mgrs = matchup_mgrs - player_mgrs
                    if missing_mgrs and len(missing_mgrs) < len(matchup_mgrs):
                        # Only flag if SOME managers have data (full week missing already caught above)
                        gaps.append(Gap(fetcher="roster", year=year, detail=f"wk{wk}", source="audit"))
                        logger.info(
                            "Audit: missing roster data for year %d week %d (managers: %s)",
                            year,
                            wk,
                            list(missing_mgrs),
                        )

    # ── Schedule audit ──────────────────────────────────────────────────────
    sched_dir = data_dir / "schedule_data"
    schedule_years: set[int] = set()

    # Check known schedule file locations + glob for year-specific files
    candidate_paths = [sched_dir / "schedule.parquet", data_dir / "schedule.parquet"]
    if sched_dir.exists():
        candidate_paths.extend(sched_dir.glob("schedule_data_year_*.parquet"))
        candidate_paths.extend(sched_dir.glob("schedule_*.parquet"))
    for sched_path in candidate_paths:
        if isinstance(sched_path, Path) and sched_path.exists():
            try:
                sdf = pd.read_parquet(sched_path)
                if "year" in sdf.columns:
                    schedule_years.update(int(y) for y in sdf["year"].unique())
            except Exception:
                pass

    for year in settings_years:
        if year not in schedule_years:
            gaps.append(Gap(fetcher="schedule", year=year, detail="full_year", source="audit"))
            logger.info("Audit: missing schedule data for year %d", year)

    # ── Draft audit (warning only) ──────────────────────────────────────────
    draft_dir = data_dir / "draft_data"
    skip_suffixes = ("_all_years", "_combined", "_final", "_merged")
    for year in settings_years:
        found_draft = False
        if draft_dir.exists():
            for p in list(draft_dir.glob(f"draft_data_{year}*.parquet")) + list(
                draft_dir.glob(f"draft_year_{year}*.parquet")
            ):
                # Skip aggregate files
                stem = p.stem.replace(f"draft_data_{year}", "")
                if any(stem.endswith(s) or stem == s for s in skip_suffixes):
                    continue
                try:
                    df = pd.read_parquet(p)
                    if len(df) > 0:
                        found_draft = True
                        break
                except Exception:
                    continue
        if not found_draft:
            logger.warning("Audit: missing draft data for year %d (warning only, not adding gap)", year)
            # Draft is warning-only — do NOT append to gaps

    return gaps


def _detect_gaps_from_audit_lightweight(data_dir: Path, league_settings: dict) -> list[Gap]:
    """Lightweight audit — only checks for 0-row data types (no roster cross-referencing).

    Used when fetchers reported 0 failures and no manifests exist. This catches the
    silent failure case (e.g., transactions returning 0 rows) without the false-positive
    risk of cross-referencing manager names between matchup and player parquets.
    """
    data_dir = Path(data_dir)
    settings_years = sorted(int(y) for y in league_settings.keys())
    gaps: list[Gap] = []

    # Transaction audit (the main silent failure case)
    tx_dir = data_dir / "transaction_data"
    tx_counts: dict[int, int] = {}
    for year in settings_years:
        parquet_path = tx_dir / f"transactions_year_{year}.parquet"
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                tx_counts[year] = len(df)
            except Exception:
                tx_counts[year] = 0
        else:
            tx_counts[year] = 0

    has_any_tx = any(c > 0 for c in tx_counts.values())
    if has_any_tx:
        for year in settings_years:
            if tx_counts.get(year, 0) == 0:
                gaps.append(Gap(fetcher="transaction", year=year, detail="full_year", source="audit"))
                logger.info("Lightweight audit: missing/empty transactions for year %d", year)

    # Matchup audit (check for 0-row years)
    matchup_dir = data_dir / "matchup_data"
    for year in settings_years:
        found = False
        if matchup_dir.exists():
            for p in matchup_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(p, columns=["year"])
                    if year in df["year"].values:
                        found = True
                        break
                except Exception:
                    continue
        if not found:
            gaps.append(Gap(fetcher="matchup", year=year, detail="full_year", source="audit"))
            logger.info("Lightweight audit: missing matchup data for year %d", year)

    logger.info("[RECOVERY] Lightweight audit found %d gaps", len(gaps))
    return gaps


def _detect_gaps_from_local_db(local_db, league_settings: dict) -> list[Gap]:
    """Audit local DuckDB and emit the original Yahoo fetcher replay units.

    Recovery intentionally mirrors the initial fetchers:
    - matchup/schedule/transaction/draft replay by season
    - roster replays by week

    The audit therefore discovers missing work in those same units instead of
    synthesizing manager-level or transaction-level patch jobs.
    """
    settings_years = sorted(int(y) for y in league_settings.keys())
    conn = local_db.connect()

    def _table_exists(name: str) -> bool:
        return (
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
                [name],
            ).fetchone()[0]
            > 0
        )

    def _has_rows(name: str, year: int) -> bool:
        if not _table_exists(name):
            return False
        return conn.execute(f"SELECT COUNT(*) FROM public.{name} WHERE year = ?", [year]).fetchone()[0] > 0

    def _table_columns(name: str) -> set[str]:
        if not _table_exists(name):
            return set()
        return {
            str(column_name)
            for (column_name,) in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ?
                """,
                [name],
            ).fetchall()
        }

    def _mark_year(bucket: dict[int, set[str]], year: int, reason: str) -> None:
        bucket.setdefault(int(year), set()).add(reason)

    def _summarize_reasons(reasons: set[str]) -> str:
        ordered = sorted(reason for reason in reasons if reason)
        if not ordered:
            return "reason unknown"
        if len(ordered) <= 2:
            return "; ".join(ordered)
        return "; ".join(ordered[:2]) + f"; +{len(ordered) - 2} more"

    def _manager_preview(managers: set[str]) -> str:
        ordered = sorted(manager for manager in managers if manager)
        if not ordered:
            return ""
        preview = ", ".join(ordered[:3])
        if len(ordered) > 3:
            preview += f", +{len(ordered) - 3} more"
        return preview

    def _text_identity_sql(alias: str, column_name: str) -> str:
        return f"NULLIF(REPLACE(LOWER(TRIM(COALESCE({alias}.{column_name}, ''))), ' ', ''), '')"

    def _stable_identity_sql(alias: str, columns: set[str]) -> str:
        candidates: list[str] = []

        if "manager_week" in columns:
            candidates.append(f"NULLIF(TRIM(COALESCE({alias}.manager_week, '')), '')")
        if "franchise_id" in columns:
            candidates.append(
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({alias}.franchise_id, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({alias}.franchise_id, '')) "
                f"END"
            )
        if "manager_guid" in columns:
            candidates.append(
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({alias}.manager_guid, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({alias}.manager_guid, '')) "
                f"END"
            )
        if "team_key" in columns:
            candidates.append(
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({alias}.team_key, ''))) IN ('', 'nan') THEN NULL "
                f"ELSE TRIM(COALESCE({alias}.team_key, '')) "
                f"END"
            )
        if "team_name" in columns:
            candidates.append(_text_identity_sql(alias, "team_name"))
        if "manager" in columns:
            candidates.append(
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({alias}.manager, ''))) IN ('', 'bye', 'bye week', 'unrostered') THEN NULL "
                f"ELSE {_text_identity_sql(alias, 'manager')} "
                f"END"
            )

        return f"COALESCE({', '.join(candidates)})" if candidates else "NULL"

    def _stable_identity_match_sql(
        left_alias: str,
        left_columns: set[str],
        right_alias: str,
        right_columns: set[str],
    ) -> str:
        conditions: list[str] = []

        if "manager_week" in left_columns and "manager_week" in right_columns:
            left_expr = f"NULLIF(TRIM(COALESCE({left_alias}.manager_week, '')), '')"
            right_expr = f"NULLIF(TRIM(COALESCE({right_alias}.manager_week, '')), '')"
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        if "franchise_id" in left_columns and "franchise_id" in right_columns:
            left_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({left_alias}.franchise_id, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({left_alias}.franchise_id, '')) "
                f"END"
            )
            right_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({right_alias}.franchise_id, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({right_alias}.franchise_id, '')) "
                f"END"
            )
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        if "manager_guid" in left_columns and "manager_guid" in right_columns:
            left_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({left_alias}.manager_guid, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({left_alias}.manager_guid, '')) "
                f"END"
            )
            right_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({right_alias}.manager_guid, ''))) IN ('', 'nan', 'none', '<na>', '--', '--hidden--') THEN NULL "
                f"ELSE TRIM(COALESCE({right_alias}.manager_guid, '')) "
                f"END"
            )
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        if "team_key" in left_columns and "team_key" in right_columns:
            left_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({left_alias}.team_key, ''))) IN ('', 'nan') THEN NULL "
                f"ELSE TRIM(COALESCE({left_alias}.team_key, '')) "
                f"END"
            )
            right_expr = (
                f"CASE "
                f"WHEN LOWER(TRIM(COALESCE({right_alias}.team_key, ''))) IN ('', 'nan') THEN NULL "
                f"ELSE TRIM(COALESCE({right_alias}.team_key, '')) "
                f"END"
            )
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        if "team_name" in left_columns and "team_name" in right_columns:
            left_expr = _text_identity_sql(left_alias, "team_name")
            right_expr = _text_identity_sql(right_alias, "team_name")
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        if "manager" in left_columns and "manager" in right_columns:
            left_expr = _text_identity_sql(left_alias, "manager")
            right_expr = _text_identity_sql(right_alias, "manager")
            conditions.append(f"({left_expr} IS NOT NULL AND {right_expr} = {left_expr})")

        return " OR ".join(conditions) if conditions else "FALSE"

    matchup_exists = _table_exists("matchup")
    schedule_exists = _table_exists("schedule")
    player_exists = _table_exists("player_fantasy")
    txn_exists = _table_exists("transactions")
    draft_exists = _table_exists("draft")
    settings_exists = _table_exists("league_settings")
    matchup_columns = _table_columns("matchup")
    player_columns = _table_columns("player_fantasy")
    schedule_columns = _table_columns("schedule")

    matchup_year_reasons: dict[int, set[str]] = {}
    transaction_year_reasons: dict[int, set[str]] = {}
    schedule_year_reasons: dict[int, set[str]] = {}
    roster_week_reasons: dict[tuple[int, int], set[str]] = {}
    roster_week_managers: dict[tuple[int, int], set[str]] = {}

    for year in settings_years:
        if not _has_rows("matchup", year):
            _mark_year(matchup_year_reasons, year, "missing matchup data")

    if matchup_exists and settings_exists:
        incomplete_regular_weeks = conn.execute(
            f"""
            WITH settings AS (
                SELECT
                    year,
                    COALESCE(num_teams, 0) AS num_teams,
                    COALESCE(start_week, 1) AS start_week,
                    COALESCE(playoff_start_week, end_week + 1, CASE WHEN year >= 2021 THEN 15 ELSE 14 END) AS playoff_start
                FROM public.league_settings
                WHERE year IS NOT NULL
            ),
            observed AS (
                SELECT
                    year,
                    week,
                    -- Use franchise_id when available (handles duplicate manager names),
                    -- fall back to manager_guid, then manager name as last resort.
                    -- IMPORTANT: guard against empty strings — COALESCE('', ...) = '' collapses all.
                    COUNT(DISTINCT {_stable_identity_sql('m', matchup_columns)}) AS manager_count
                FROM public.matchup AS m
                WHERE {_stable_identity_sql('m', matchup_columns)} IS NOT NULL
                GROUP BY year, week
            ),
            raw_row_counts AS (
                SELECT year, week, COUNT(*) AS row_count
                FROM public.matchup
                GROUP BY year, week
            ),
            expected AS (
                SELECT
                    s.year,
                    gs.week,
                    s.num_teams
                FROM settings s,
                     generate_series(s.start_week, s.playoff_start - 1) AS gs(week)
                WHERE s.num_teams > 0
                  AND s.playoff_start > s.start_week
            )
            SELECT
                e.year,
                e.week,
                COALESCE(o.manager_count, 0) AS manager_count,
                e.num_teams
            FROM expected e
            LEFT JOIN observed o
              ON o.year = e.year
             AND o.week = e.week
            LEFT JOIN raw_row_counts r
              ON r.year = e.year
             AND r.week = e.week
            WHERE COALESCE(o.manager_count, 0) < e.num_teams
              -- If raw row count matches expected teams, data is complete —
              -- the identity gap is just an unresolvable column (e.g. franchise_id
              -- not yet populated before Phase 3.5 enrichments). Don't flag it.
              AND COALESCE(r.row_count, 0) < e.num_teams
            ORDER BY e.year, e.week
            """
        ).fetchall()
        for year, week, manager_count, expected_count in incomplete_regular_weeks:
            year = int(year)
            if not _has_rows("matchup", year):
                continue
            _mark_year(
                matchup_year_reasons,
                year,
                f"incomplete regular-season coverage ({manager_count}/{expected_count} managers in week {int(week)})",
            )

    if matchup_exists and schedule_exists:
        missing_matchups = conn.execute(
            f"""
            SELECT DISTINCT s.year, s.week
            FROM public.schedule s
            LEFT JOIN public.matchup m
              ON m.year = s.year
             AND m.week = s.week
             AND ({_stable_identity_match_sql('s', schedule_columns, 'm', matchup_columns)})
            WHERE m.year IS NULL
            ORDER BY s.year, s.week
            """
        ).fetchall()
        for year, week in missing_matchups:
            _mark_year(
                matchup_year_reasons, int(year), f"schedule rows missing matching matchup rows (week {int(week)})"
            )

    if not txn_exists:
        for year in settings_years:
            _mark_year(transaction_year_reasons, year, "transactions table missing")
    else:
        tx_counts = {
            int(year): int(count)
            for year, count in conn.execute("SELECT year, COUNT(*) FROM public.transactions GROUP BY year").fetchall()
            if year is not None
        }
        has_any_tx = any(count > 0 for count in tx_counts.values())
        if has_any_tx:
            for year in settings_years:
                if tx_counts.get(year, 0) == 0:
                    _mark_year(transaction_year_reasons, year, "missing transactions")

        tx_unknown_rows = conn.execute(
            """
            SELECT year, COUNT(DISTINCT transaction_id) AS tx_count
            FROM public.transactions
            WHERE (manager IS NULL OR TRIM(COALESCE(manager, '')) = '' OR LOWER(TRIM(manager)) = 'unknown')
              AND LOWER(COALESCE(transaction_type, '')) <> 'drop'
              AND transaction_id IS NOT NULL
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
        for year, tx_count in tx_unknown_rows:
            _mark_year(transaction_year_reasons, int(year), f"{int(tx_count)} transaction(s) have unresolved managers")

        missing_trade_sides = conn.execute(
            """
            SELECT year, COUNT(DISTINCT transaction_id) AS tx_count
            FROM (
              SELECT
                year,
                week,
                transaction_id,
                COUNT(DISTINCT NULLIF(TRIM(COALESCE(franchise_id, '')), '')) FILTER (
                  WHERE TRIM(COALESCE(franchise_id, '')) <> ''
                ) AS manager_count,
                MAX(CASE WHEN TRIM(COALESCE(source_manager, '')) <> '' THEN 1 ELSE 0 END) AS has_source,
                MAX(CASE WHEN TRIM(COALESCE(destination_manager, '')) <> '' THEN 1 ELSE 0 END) AS has_destination
              FROM public.transactions
              WHERE LOWER(COALESCE(transaction_type, '')) = 'trade'
              GROUP BY year, week, transaction_id
            ) t
            WHERE has_source = 1 AND has_destination = 1 AND manager_count < 2
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
        for year, tx_count in missing_trade_sides:
            _mark_year(transaction_year_reasons, int(year), f"{int(tx_count)} trade(s) are missing a side")

        if matchup_exists and player_exists:
            years_with_full_year_tx_gap = set(transaction_year_reasons)
            continuity_gaps = _ownership_change_transaction_pair_gaps(conn)
            for year, prev_week, next_week, missing_player_count in continuity_gaps:
                year = int(year)
                if year in years_with_full_year_tx_gap:
                    continue
                logger.info(
                    "Local audit: ownership changes without transactions for year %d boundary W%d->W%d (%d players)",
                    year,
                    prev_week,
                    next_week,
                    missing_player_count,
                )

    if not player_exists:
        if matchup_exists:
            matchup_weeks = conn.execute(
                """
                SELECT DISTINCT year, week
                FROM public.matchup
                WHERE opponent IS NOT NULL
                ORDER BY year, week
                """
            ).fetchall()
            for year, week in matchup_weeks:
                roster_week_reasons.setdefault((int(year), int(week)), set()).add("player_fantasy table missing")
    else:
        missing_rosters = conn.execute(
            f"""
            SELECT DISTINCT
                m.year,
                m.week,
                m.manager
            FROM public.matchup m
            WHERE m.opponent IS NOT NULL
              AND {_stable_identity_sql('m', matchup_columns)} IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM public.player_fantasy pf
                WHERE pf.year = m.year
                  AND pf.week = m.week
                  AND {_stable_identity_sql('pf', player_columns)} IS NOT NULL
                  AND (
                    (
                      NULLIF(TRIM(COALESCE(m.manager_week, '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(pf.manager_week, '')), '') = NULLIF(TRIM(COALESCE(m.manager_week, '')), '')
                    )
                    OR (
                      NULLIF(TRIM(COALESCE(m.manager_guid, '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(pf.manager_guid, '')), '') = NULLIF(TRIM(COALESCE(m.manager_guid, '')), '')
                    )
                    OR (
                      NULLIF(TRIM(COALESCE(m.franchise_id, '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(pf.franchise_id, '')), '') = NULLIF(TRIM(COALESCE(m.franchise_id, '')), '')
                    )
                  )
              )
            ORDER BY m.year, m.week, m.manager
            """
        ).fetchall()
        for year, week, manager in missing_rosters:
            key = (int(year), int(week))
            roster_week_reasons.setdefault(key, set()).add("missing roster rows")
            if manager and str(manager).strip():
                roster_week_managers.setdefault(key, set()).add(str(manager).strip())

    if matchup_exists:
        schedule_years_with_rows: set[int] = set()
        if schedule_exists:
            schedule_years_with_rows = {
                int(year) for (year,) in conn.execute("SELECT DISTINCT year FROM public.schedule").fetchall()
            }

        missing_schedule_weeks = (
            conn.execute(
                f"""
                SELECT DISTINCT m.year, m.week
                FROM public.matchup m
                WHERE {_stable_identity_sql('m', matchup_columns)} IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM public.schedule s
                    WHERE s.year = m.year
                      AND s.week = m.week
                      AND ({_stable_identity_match_sql('m', matchup_columns, 's', schedule_columns)})
                  )
                ORDER BY m.year, m.week
                """
            ).fetchall()
            if schedule_exists
            else []
        )
        for year, week in missing_schedule_weeks:
            year = int(year)
            if year not in schedule_years_with_rows:
                continue
            _mark_year(schedule_year_reasons, year, f"missing schedule rows for week {int(week)}")

    if not schedule_exists:
        for year in settings_years:
            if matchup_exists and _has_rows("matchup", year):
                _mark_year(schedule_year_reasons, year, "schedule table missing")
            else:
                _mark_year(schedule_year_reasons, year, "missing schedule data")
    else:
        for year in settings_years:
            if not _has_rows("schedule", year):
                if matchup_exists and _has_rows("matchup", year):
                    _mark_year(schedule_year_reasons, year, "missing schedule data for observed matchup weeks")
                else:
                    _mark_year(schedule_year_reasons, year, "missing schedule data")

    for year in settings_years:
        if not draft_exists:
            logger.warning("Local audit: missing draft data for year %d (warning only, not adding gap)", year)
        elif not _has_rows("draft", year):
            logger.warning("Local audit: missing draft data for year %d (warning only, not adding gap)", year)

    gaps: list[Gap] = []

    for year in sorted(matchup_year_reasons):
        logger.info(
            "Local audit: replay matchup year %d via season fetch (%s)",
            year,
            _summarize_reasons(matchup_year_reasons[year]),
        )
        gaps.append(Gap(fetcher="matchup", year=year, detail="full_year", source="audit"))

    for year in sorted(transaction_year_reasons):
        logger.info(
            "Local audit: replay transactions year %d via season fetch (%s)",
            year,
            _summarize_reasons(transaction_year_reasons[year]),
        )
        gaps.append(Gap(fetcher="transaction", year=year, detail="full_year", source="audit"))

    for year in sorted(schedule_year_reasons):
        logger.info(
            "Local audit: replay schedule year %d via season fetch (%s)",
            year,
            _summarize_reasons(schedule_year_reasons[year]),
        )
        gaps.append(Gap(fetcher="schedule", year=year, detail="full_year", source="audit"))

    for year, week in sorted(roster_week_reasons):
        reason_parts = set(roster_week_reasons[(year, week)])
        managers = roster_week_managers.get((year, week), set())
        if managers:
            reason_parts.add(f"{len(managers)} manager(s) missing: {_manager_preview(managers)}")
        logger.info(
            "Local audit: replay roster year %d week %d via weekly fetch (%s)",
            year,
            week,
            _summarize_reasons(reason_parts),
        )
        gaps.append(Gap(fetcher="roster", year=year, detail=f"wk{week}", source="audit"))

    deduped: dict[tuple, Gap] = {}
    for gap in gaps:
        deduped[gap.key] = gap
    return list(deduped.values())


# =============================================================================
# Task 4: Gap Merging
# =============================================================================


def _merge_gaps(manifest_gaps: list[Gap], audit_gaps: list[Gap]) -> list[Gap]:
    """
    Merge gaps from manifests and audit, deduplicating on gap.key.

    Manifest gaps take priority when the same gap appears in both sources.
    """
    seen: dict[tuple, Gap] = {}

    # Manifest first — these take priority
    for gap in manifest_gaps:
        seen[gap.key] = gap

    # Audit gaps — only add if not already seen
    for gap in audit_gaps:
        if gap.key not in seen:
            seen[gap.key] = gap

    return list(seen.values())


def _prune_broad_gaps(gaps: list[Gap]) -> list[Gap]:
    """Prefer exact gap details over coarse full-year or offset retries."""
    grouped: dict[tuple[str, int], list[Gap]] = {}
    for gap in gaps:
        grouped.setdefault((gap.fetcher, gap.year), []).append(gap)

    pruned: list[Gap] = []
    for scoped_gaps in grouped.values():
        has_specific = any(g.detail != "full_year" and not g.detail.startswith("offset_") for g in scoped_gaps)
        for gap in scoped_gaps:
            if has_specific and (gap.detail == "full_year" or gap.detail.startswith("offset_")):
                continue
            pruned.append(gap)

    return pruned


def _rewrite_active_manifests(data_dir: Path, gaps: list[Gap]) -> None:
    """Rewrite manifests to the exact current retryable gap set.

    Overlapping broad retries (for example ``full_year`` + an exact detail for the
    same fetcher/year) are normalized down to the exact scope before writing.
    """
    data_dir = Path(data_dir)
    manifest_dir = data_dir / MANIFEST_DIR_NAME
    if not gaps:
        clear_all_manifests(data_dir)
        return

    gaps = _prune_broad_gaps(gaps)

    existing_manifests: dict[tuple[str, int], dict] = {}
    if manifest_dir.exists():
        for path in manifest_dir.glob("*.json"):
            try:
                manifest = json.loads(path.read_text())
            except Exception:
                continue
            fetcher = _normalize_fetcher(str(manifest.get("fetcher", "")))
            year = manifest.get("year")
            if fetcher and year is not None:
                existing_manifests[(fetcher, int(year))] = manifest

    grouped: dict[tuple[str, int], list[Gap]] = {}
    for gap in gaps:
        grouped.setdefault((gap.fetcher, gap.year), []).append(gap)

    desired_names: set[str] = set()
    for (fetcher, year), scoped_gaps in grouped.items():
        failed_weeks: list[int] = []
        failed_details: list[str] = []
        failed_year = False
        failed_at_offset: int | None = None
        error_messages: list[str] = []

        for gap in scoped_gaps:
            detail = gap.detail
            if detail == "full_year":
                failed_year = True
            elif detail.startswith("offset_"):
                try:
                    failed_at_offset = int(detail.split("_", 1)[1])
                except Exception:
                    failed_details.append(detail)
            elif re.fullmatch(r"wk\d+", detail):
                failed_weeks.append(int(detail[2:]))
            else:
                failed_details.append(detail)

            if gap.last_error:
                error_messages.append(str(gap.last_error).strip())

        existing_error = str(existing_manifests.get((fetcher, year), {}).get("error", "")).strip()
        if existing_error and existing_error != "active Yahoo recovery gaps":
            error_messages.insert(0, existing_error)

        deduped_errors: list[str] = []
        seen_errors: set[str] = set()
        for message in error_messages:
            cleaned = message.strip()
            if cleaned and cleaned not in seen_errors:
                seen_errors.add(cleaned)
                deduped_errors.append(cleaned)

        clear_manifest(data_dir, fetcher, year)

        desired_names.add(f"{fetcher}_{year}.json")
        write_manifest(
            data_dir,
            fetcher,
            year,
            failed_weeks=sorted(set(failed_weeks)) or None,
            failed_details=sorted(set(failed_details)) or None,
            failed_at_offset=failed_at_offset,
            failed_year=failed_year,
            error=" | ".join(deduped_errors)[:500] if deduped_errors else None,
        )

    if manifest_dir.exists():
        for path in manifest_dir.glob("*.json"):
            if path.name not in desired_names:
                path.unlink()
        try:
            manifest_dir.rmdir()
        except OSError:
            pass


def _collect_retryable_gaps(
    data_dir: Path,
    local_db,
    league_settings: dict,
    hard_missing_keys: set[tuple] | None = None,
    exclude_years: set[int] | None = None,
) -> tuple[list[Gap], list[Gap], list[Gap]]:
    """Return manifest gaps, audit gaps, and the canonical fetch-unit queue.

    `exclude_years`: years to skip entirely (e.g., years populated from external
    staging data — their "gaps" aren't fetchable from Yahoo because the
    league_key is too old or was never API-accessible)."""
    manifest_gaps = _canonicalize_gap_units(_detect_gaps_from_manifests(data_dir), local_db=local_db)
    audit_gaps = _canonicalize_gap_units(_detect_gaps_from_local_db(local_db, league_settings), local_db=local_db)
    current_gaps = _canonicalize_gap_units(_merge_gaps(manifest_gaps, audit_gaps), local_db=local_db)
    if hard_missing_keys:
        current_gaps = [gap for gap in current_gaps if gap.key not in hard_missing_keys]
    if exclude_years:
        before = len(current_gaps)
        current_gaps = [gap for gap in current_gaps if gap.year not in exclude_years]
        manifest_gaps = [gap for gap in manifest_gaps if gap.year not in exclude_years]
        audit_gaps = [gap for gap in audit_gaps if gap.year not in exclude_years]
        dropped = before - len(current_gaps)
        if dropped:
            logger.info(
                "[RECOVERY] Excluded %d gap(s) from skipped year(s) %s",
                dropped,
                sorted(exclude_years),
            )
    return manifest_gaps, audit_gaps, current_gaps


def _merge_gap_queue(active_queue: dict[tuple, Gap], discovered_gaps: list[Gap]) -> dict[tuple, Gap]:
    """Merge newly discovered gaps into the active queue without losing retry state."""
    merged = dict(active_queue)

    for gap in discovered_gaps:
        existing = merged.get(gap.key)
        if existing is None:
            merged[gap.key] = gap
            continue

        if existing.source != "manifest" and gap.source == "manifest":
            existing.source = "manifest"
        if gap.last_error and not existing.last_error:
            existing.last_error = gap.last_error

    return merged


def _canonicalize_gap_units(
    gaps: list[Gap],
    local_db=None,
) -> list[Gap]:
    """Collapse manifests/audit into the original fetcher retry units.

    Recovery should replay the same unit that the fetcher originally ran:
    - matchup/schedule/transaction/draft: season-level
    - roster: week-level

    Older manifests may still contain narrower patch details from the retired
    gap engine; those are coalesced back to the canonical fetch unit here.
    """
    canonical: dict[tuple, Gap] = {}

    def _remember(new_gap: Gap) -> None:
        existing = canonical.get(new_gap.key)
        if existing is None:
            canonical[new_gap.key] = new_gap
            return
        if existing.source != "manifest" and new_gap.source == "manifest":
            existing.source = "manifest"
        if new_gap.last_error and not existing.last_error:
            existing.last_error = new_gap.last_error

    for gap in gaps:
        fetcher = _normalize_fetcher(gap.fetcher)

        if fetcher == "roster":
            week, _manager_token, _manager_week = _parse_roster_gap_detail(gap.detail)
            if week is not None:
                _remember(
                    Gap(
                        fetcher="roster",
                        year=gap.year,
                        detail=f"wk{week}",
                        source=gap.source,
                        attempts=gap.attempts,
                        hard_missing=gap.hard_missing,
                        last_error=gap.last_error,
                    )
                )
                continue

            if gap.detail == "full_year":
                for week_num in _get_recovery_weeks(local_db, gap.year, prefer_table="matchup"):
                    _remember(
                        Gap(
                            fetcher="roster",
                            year=gap.year,
                            detail=f"wk{week_num}",
                            source=gap.source,
                            attempts=gap.attempts,
                            hard_missing=gap.hard_missing,
                            last_error=gap.last_error,
                        )
                    )
                continue

            _remember(
                Gap(
                    fetcher="roster",
                    year=gap.year,
                    detail=gap.detail,
                    source=gap.source,
                    attempts=gap.attempts,
                    hard_missing=gap.hard_missing,
                    last_error=gap.last_error,
                )
            )
            continue

        _remember(
            Gap(
                fetcher=fetcher,
                year=gap.year,
                detail="full_year",
                source=gap.source,
                attempts=gap.attempts,
                hard_missing=gap.hard_missing,
                last_error=gap.last_error,
            )
        )

    return list(canonical.values())


# =============================================================================
# Task 7: Recovery Loop
# =============================================================================

# Sequential recovery with adaptive cooldown.
# Cooldown oscillates: escalate up then de-escalate back down.
# After 30+60+120=210s cumulative, the rate window has mostly reset — no need for 300s.
COOLDOWN_SCHEDULE = [30, 60, 120, 60, 30, 60, 120, 60, 30]  # cycles, resets on success
MAX_COOLDOWN_EVENTS = 10  # stop recovery after this many throttled/transient cooldowns
MAX_GAP_ATTEMPTS = 8  # attempts per gap across all passes before hard_missing
HARD_MISSING_THRESHOLD = 5  # consecutive empty API responses → hard_missing
STAGNANT_THRESHOLD = 3  # gap resolved then re-appears this many times → hard_missing
MAX_PASSES = 15  # hard cap on recovery passes
DEAD_MAN_MINUTES = 120  # total recovery time limit


def _get_current_nfl_year() -> int:
    """Return current NFL season year for completed-season checks."""
    from datetime import datetime

    now = datetime.now()
    return now.year if now.month >= 3 else now.year - 1


def estimate_gap_cost(gap) -> int:
    """Estimate API calls needed to resolve a gap.

    Used to sort gaps cheapest-first so we resolve the most items
    before hitting rate limits.
    """
    f, detail = gap.fetcher, gap.detail

    # FREE: schedule derivation for completed seasons
    if f == "schedule" and gap.year < _get_current_nfl_year():
        return 0

    # Matchup: the canonical replay is the original season matchup fetcher
    if f == "matchup":
        return 1

    # Schedule: 1 call (current season only — completed handled above)
    if f == "schedule":
        return 1

    # Roster: canonical replay is one weekly roster fetch
    if f == "roster":
        if detail.startswith("wk"):
            return 2  # all teams for 1 week
        if detail == "full_year":
            return 17
        return 5

    # Transaction: canonical replay is a full-season fetch
    if f == "transaction":
        if detail == "full_year":
            return 8
        return 8

    # Draft: always heavy
    if f == "draft":
        return 19

    return 5


def gap_sort_key(gap) -> tuple:
    """Sort key: dependency order first, then cheapest, then even/odd year alternation."""
    cost = estimate_gap_cost(gap)
    priority = _PRIORITY_ORDER.get(gap.fetcher, 99)
    parity = gap.year % 2
    return (priority, cost, parity, gap.year)


LOCAL_DB_DEDUP_KEYS = {
    "matchup": ["year", "week", "manager", "opponent"],
    "roster": ["manager_week", "yahoo_player_id", "year", "week"],
    "transaction": ["transaction_id", "yahoo_player_id", "player", "manager", "transaction_type"],
    "draft": ["year", "pick", "yahoo_player_id", "player"],
    "schedule": ["year", "week", "manager"],
}

# Dependency ordering: matchups first, then schedules that depend on them, then the rest.
_PRIORITY_ORDER = {"matchup": 0, "schedule": 1, "roster": 2, "transaction": 2, "draft": 3}


def _rate_limited_scope_key(gap: Gap) -> tuple[str, int]:
    """Bucket deferred gaps by fetcher/year so sibling work pauses after throttling."""
    return (gap.fetcher, gap.year)


def _cooldown_or_stop(cooldown_level: int, deadline: float, reason: str, gap: Gap | None = None) -> tuple[int, bool]:
    """Run one bounded cooldown. Returns (next_level, should_stop_recovery)."""
    if cooldown_level >= MAX_COOLDOWN_EVENTS:
        target = f" on {gap.key}" if gap is not None else ""
        logger.warning(
            "[RECOVERY] Cooldown cap (%d) reached%s - stopping recovery and continuing with available data",
            MAX_COOLDOWN_EVENTS,
            target,
        )
        return cooldown_level, True

    cooldown_secs = COOLDOWN_SCHEDULE[min(cooldown_level, len(COOLDOWN_SCHEDULE) - 1)]
    next_level = min(cooldown_level + 1, MAX_COOLDOWN_EVENTS)
    remaining = deadline - time.time()
    sleep_time = min(cooldown_secs, max(remaining, 0))

    if sleep_time > 0:
        if gap is None:
            logger.info(
                "[RECOVERY] Zero progress - cooling down %ds before next pass (level %d/%d)",
                int(sleep_time),
                next_level,
                MAX_COOLDOWN_EVENTS,
            )
        elif reason == "deferred":
            logger.info(
                "[RECOVERY] Yahoo throttled on %s - cooling down %ds (level %d/%d)",
                gap.key,
                int(sleep_time),
                next_level,
                MAX_COOLDOWN_EVENTS,
            )
        else:
            logger.info(
                "[RECOVERY] Rate limited on %s - cooling down %ds (level %d/%d)",
                gap.key,
                int(sleep_time),
                next_level,
                MAX_COOLDOWN_EVENTS,
            )
        time.sleep(sleep_time)

    return next_level, False


def _build_roster_gap_detail(week: int, manager_week: str | None = None, manager_name: str | None = None) -> str:
    """Build the narrowest roster gap detail we can recover surgically."""
    if manager_week and str(manager_week).strip():
        return f"wk{int(week)}_{_ROSTER_MANAGER_WEEK_PREFIX}{str(manager_week).strip()}"

    safe_manager = re.sub(r"\s+", " ", str(manager_name or "").strip())
    return f"wk{int(week)}_{safe_manager}" if safe_manager else f"wk{int(week)}"


def _parse_roster_gap_detail(detail: str) -> tuple[int | None, str | None, str | None]:
    """Parse roster gap detail into (week, manager_token, manager_week)."""
    match = _ROSTER_GAP_RE.fullmatch(detail or "")
    if not match:
        return (None, None, None)

    week = int(match.group(1))
    token = match.group(2)
    manager_week = None
    if token and token.startswith(_ROSTER_MANAGER_WEEK_PREFIX):
        manager_week = token[len(_ROSTER_MANAGER_WEEK_PREFIX) :].strip() or None
        token = None

    return (week, token, manager_week)


def _missing_roster_gap_details_for_week(local_db, year: int, week: int) -> list[str]:
    """Return exact missing roster manager-week details for one year/week."""
    if local_db is None:
        return []

    conn = local_db.connect()
    rows = conn.execute(
        """
        SELECT DISTINCT
            m.week,
            NULLIF(TRIM(COALESCE(m.manager_week, '')), '') AS manager_week,
            NULLIF(TRIM(COALESCE(m.manager, '')), '') AS manager
        FROM public.matchup m
        WHERE m.year = ?
          AND m.week = ?
          AND m.opponent IS NOT NULL
          AND LOWER(TRIM(COALESCE(m.manager, ''))) NOT IN ('unrostered','fa','free agent','waivers','')
          AND NOT EXISTS (
            SELECT 1
            FROM public.player_fantasy pf
            WHERE pf.year = m.year
              AND pf.week = m.week
              AND LOWER(TRIM(COALESCE(pf.manager, ''))) NOT IN ('unrostered','fa','free agent','waivers','')
              AND (
                (
                  NULLIF(TRIM(COALESCE(m.manager_week, '')), '') IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(pf.manager_week, '')), '') = NULLIF(TRIM(COALESCE(m.manager_week, '')), '')
                )
                OR (
                  NULLIF(TRIM(COALESCE(m.manager_guid, '')), '') IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(pf.manager_guid, '')), '') = NULLIF(TRIM(COALESCE(m.manager_guid, '')), '')
                )
                OR (
                  NULLIF(TRIM(COALESCE(m.franchise_id, '')), '') IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(pf.franchise_id, '')), '') = NULLIF(TRIM(COALESCE(m.franchise_id, '')), '')
                )
              )
          )
        ORDER BY manager
        """,
        [year, week],
    ).fetchall()

    details: list[str] = []
    for wk, manager_week, manager_name in rows:
        details.append(_build_roster_gap_detail(int(wk), manager_week=manager_week, manager_name=manager_name))

    return details


def _coalesce_roster_manifest_weeks(gaps: list[Gap]) -> list[Gap]:
    """Keep manifest week-level roster gaps as canonical over narrower same-week gaps."""
    if not gaps:
        return gaps

    manifest_week_scopes: set[tuple[int, int]] = set()
    for gap in gaps:
        if gap.fetcher != "roster" or gap.source != "manifest":
            continue
        week, manager_token, manager_week = _parse_roster_gap_detail(gap.detail)
        if week is not None and manager_token is None and manager_week is None:
            manifest_week_scopes.add((gap.year, week))

    if not manifest_week_scopes:
        return gaps

    coalesced: dict[tuple, Gap] = {}
    for gap in gaps:
        if gap.fetcher == "roster":
            week, manager_token, manager_week = _parse_roster_gap_detail(gap.detail)
            if (
                week is not None
                and (gap.year, week) in manifest_week_scopes
                and not (gap.source == "manifest" and manager_token is None and manager_week is None)
            ):
                continue
        coalesced[gap.key] = gap

    return list(coalesced.values())


def _narrow_roster_gaps(gaps: list[Gap], local_db) -> list[Gap]:
    """Prefer exact audit gaps, but preserve manifest roster weeks as fetch units."""
    if not gaps:
        return gaps

    gaps = _coalesce_roster_manifest_weeks(gaps)
    narrowed: list[Gap] = []
    specific_scopes: set[tuple[int, int]] = set()

    for gap in gaps:
        if gap.fetcher != "roster":
            narrowed.append(gap)
            continue

        week, manager_token, manager_week = _parse_roster_gap_detail(gap.detail)
        if week is None:
            narrowed.append(gap)
            continue

        if manager_token is not None or manager_week is not None:
            specific_scopes.add((gap.year, week))
            narrowed.append(gap)
            continue

        if gap.source == "manifest":
            narrowed.append(gap)
            continue

        exact_details = _missing_roster_gap_details_for_week(local_db, gap.year, week)
        if exact_details:
            specific_scopes.add((gap.year, week))
            for detail in exact_details:
                narrowed.append(
                    Gap(
                        fetcher=gap.fetcher,
                        year=gap.year,
                        detail=detail,
                        source=gap.source,
                        attempts=gap.attempts,
                        hard_missing=gap.hard_missing,
                    )
                )
        else:
            # No specific managers missing → data exists for all managers this week.
            # DROP the broad gap. Re-adding it here caused an infinite loop where
            # resolved gaps kept re-appearing in manifests every round.
            pass

    deduped: dict[tuple, Gap] = {}
    for gap in narrowed:
        if gap.fetcher == "roster":
            week, manager_token, manager_week = _parse_roster_gap_detail(gap.detail)
            if (
                week is not None
                and manager_token is None
                and manager_week is None
                and (gap.year, week) in specific_scopes
            ):
                continue
        deduped[gap.key] = gap

    return list(deduped.values())


def _lookup_roster_target_by_manager_week(local_db, year: int, week: int, manager_week: str) -> dict[str, str] | None:
    """Resolve an exact manager_week back to Yahoo team metadata from matchup rows."""
    if local_db is None or not manager_week:
        return None

    conn = local_db.connect()
    row = conn.execute(
        """
        SELECT
            NULLIF(TRIM(COALESCE(team_key, '')), '') AS team_key,
            NULLIF(TRIM(COALESCE(manager, '')), '') AS manager,
            NULLIF(TRIM(COALESCE(manager_guid, '')), '') AS manager_guid,
            NULLIF(TRIM(COALESCE(franchise_id, '')), '') AS franchise_id
        FROM public.matchup
        WHERE year = ?
          AND week = ?
          AND NULLIF(TRIM(COALESCE(manager_week, '')), '') = ?
        LIMIT 1
        """,
        [year, week, manager_week],
    ).fetchone()

    if not row:
        return None

    return {
        "team_key": row[0],
        "manager": row[1],
        "manager_guid": row[2],
        "franchise_id": row[3],
    }


def _is_yahoo_team_key(value: str | None) -> bool:
    """Return True when a value looks like a Yahoo team key (e.g. 406.l.38187.t.8)."""
    if not value:
        return False
    cleaned = str(value).strip()
    return ".l." in cleaned and ".t." in cleaned


def _unique_roster_targets(year_maps: dict[str, str]) -> list[tuple[str, str, str | None]]:
    """Collapse a mixed manager/guid/franchise map into unique team fetch targets."""
    grouped: dict[str, list[str]] = {}
    for identifier, team_key in year_maps.items():
        if not team_key:
            continue
        grouped.setdefault(team_key, []).append(identifier)

    targets: list[tuple[str, str, str | None]] = []
    for team_key, identifiers in grouped.items():
        preferred = next(
            (ident for ident in identifiers if ident and not ident.isdigit() and "." not in ident and len(ident) > 4),
            identifiers[0],
        )
        targets.append((team_key, preferred, None))

    return targets


def _ownership_change_transaction_pair_gaps(conn) -> list[tuple[int, int, int, int]]:
    """Return adjacent complete roster week-pairs with ownership changes lacking transactions."""
    return conn.execute(
        """
        WITH matchup_week_coverage AS (
            SELECT
                year,
                week,
                COUNT(
                    DISTINCT COALESCE(
                        NULLIF(TRIM(COALESCE(manager_week, '')), ''),
                        NULLIF(TRIM(COALESCE(manager_guid, '')), ''),
                        NULLIF(TRIM(COALESCE(franchise_id, '')), ''),
                        LOWER(TRIM(COALESCE(manager, '')))
                    )
                ) AS expected_count
            FROM public.matchup
            WHERE opponent IS NOT NULL
              AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
            GROUP BY year, week
        ),
        roster_week_coverage AS (
            SELECT
                year,
                week,
                COUNT(
                    DISTINCT COALESCE(
                        NULLIF(TRIM(COALESCE(manager_week, '')), ''),
                        NULLIF(TRIM(COALESCE(manager_guid, '')), ''),
                        NULLIF(TRIM(COALESCE(franchise_id, '')), ''),
                        LOWER(TRIM(COALESCE(manager, '')))
                    )
                ) AS actual_count
            FROM public.player_fantasy
            WHERE LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
            GROUP BY year, week
        ),
        complete_weeks AS (
            SELECT
                m.year,
                m.week
            FROM matchup_week_coverage m
            LEFT JOIN roster_week_coverage r
              ON r.year = m.year
             AND r.week = m.week
            WHERE m.expected_count > 0
              AND COALESCE(r.actual_count, 0) >= m.expected_count
        ),
        manager_keys AS (
            SELECT DISTINCT
                year,
                week,
                COALESCE(
                    NULLIF(TRIM(COALESCE(manager_guid, '')), ''),
                    NULLIF(TRIM(COALESCE(franchise_id, '')), ''),
                    LOWER(TRIM(COALESCE(manager, '')))
                ) AS manager_key
            FROM public.matchup
            WHERE opponent IS NOT NULL
              AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
        ),
        boundaries AS (
            SELECT
                c1.year,
                c1.week AS prev_week,
                c2.week AS next_week
            FROM complete_weeks c1
            JOIN complete_weeks c2
              ON c2.year = c1.year
             AND c2.week = c1.week + 1
        ),
        stable_boundaries AS (
            SELECT
                b.year,
                b.prev_week,
                b.next_week
            FROM boundaries b
            WHERE NOT EXISTS (
                SELECT manager_key
                FROM manager_keys
                WHERE year = b.year AND week = b.prev_week
                EXCEPT
                SELECT manager_key
                FROM manager_keys
                WHERE year = b.year AND week = b.next_week
            )
              AND NOT EXISTS (
                SELECT manager_key
                FROM manager_keys
                WHERE year = b.year AND week = b.next_week
                EXCEPT
                SELECT manager_key
                FROM manager_keys
                WHERE year = b.year AND week = b.prev_week
            )
        ),
        owners AS (
            SELECT
                year,
                week,
                CAST(yahoo_player_id AS VARCHAR) AS yahoo_player_id,
                MIN(LOWER(TRIM(COALESCE(manager, '')))) AS owner
            FROM public.player_fantasy
            WHERE yahoo_player_id IS NOT NULL
              AND TRIM(CAST(yahoo_player_id AS VARCHAR)) <> ''
              AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
            GROUP BY year, week, CAST(yahoo_player_id AS VARCHAR)
        ),
        boundary_players AS (
            SELECT
                b.year,
                b.prev_week,
                b.next_week,
                o.yahoo_player_id
            FROM stable_boundaries b
            JOIN owners o
              ON o.year = b.year
             AND o.week IN (b.prev_week, b.next_week)
            GROUP BY b.year, b.prev_week, b.next_week, o.yahoo_player_id
        ),
        ownership_changes AS (
            SELECT
                bp.year,
                bp.prev_week,
                bp.next_week,
                bp.yahoo_player_id,
                MAX(CASE WHEN o.week = bp.prev_week THEN o.owner END) AS prev_owner,
                MAX(CASE WHEN o.week = bp.next_week THEN o.owner END) AS next_owner
            FROM boundary_players bp
            LEFT JOIN owners o
              ON o.year = bp.year
             AND o.yahoo_player_id = bp.yahoo_player_id
             AND o.week IN (bp.prev_week, bp.next_week)
            GROUP BY bp.year, bp.prev_week, bp.next_week, bp.yahoo_player_id
            HAVING COALESCE(MAX(CASE WHEN o.week = bp.prev_week THEN o.owner END), '')
                <> COALESCE(MAX(CASE WHEN o.week = bp.next_week THEN o.owner END), '')
        ),
        missing_boundaries AS (
            SELECT
                oc.year,
                oc.prev_week,
                oc.next_week,
                COUNT(*) AS missing_player_count
            FROM ownership_changes oc
            WHERE NOT EXISTS (
                SELECT 1
                FROM public.transactions t
                WHERE t.year = oc.year
                  AND CAST(t.yahoo_player_id AS VARCHAR) = oc.yahoo_player_id
                  AND TRIM(CAST(t.yahoo_player_id AS VARCHAR)) <> ''
                  AND TRY_CAST(t.week AS INTEGER) IN (oc.prev_week, oc.next_week)
            )
            GROUP BY oc.year, oc.prev_week, oc.next_week
        )
        SELECT year, prev_week, next_week, missing_player_count
        FROM missing_boundaries
        ORDER BY year, prev_week, next_week
        """
    ).fetchall()


def _get_recovery_weeks(local_db, year: int, prefer_table: str = "schedule") -> list[int]:
    """Return the most specific known week set for a recovery year."""
    if local_db is None:
        return list(range(1, 19))

    conn = local_db.connect()
    require_observed_matchups = prefer_table == "matchup"
    sources = ["matchup"] if require_observed_matchups else [prefer_table, "matchup", "schedule"]
    seen_sources: list[str] = []
    for table_name in sources:
        if table_name in seen_sources or not local_db.table_exists(table_name):
            continue
        seen_sources.append(table_name)
        try:
            weeks = [
                int(week)
                for (week,) in conn.execute(
                    f"SELECT DISTINCT week FROM public.{table_name} WHERE year = ? AND week IS NOT NULL ORDER BY week",
                    [year],
                ).fetchall()
            ]
            if weeks:
                return weeks
        except Exception:
            continue

    if require_observed_matchups:
        return []

    try:
        row = conn.execute(
            """
            SELECT
                COALESCE(start_week, 1) AS start_week,
                COALESCE(end_week, CASE WHEN year >= 2021 THEN 17 ELSE 16 END) AS end_week
            FROM public.league_settings
            WHERE year = ?
            """,
            [year],
        ).fetchone()
        if row:
            start_week, end_week = int(row[0]), int(row[1])
            if end_week >= start_week:
                return list(range(start_week, end_week + 1))
    except Exception:
        pass

    return list(range(1, 19))


def _write_recovery_rows(
    local_db,
    fetcher: str,
    df: pd.DataFrame,
    platform: str = "yahoo",
    league_id: str | None = None,
) -> None:
    """Write recovered rows to the current import target."""
    if df is None or df.empty:
        return

    if local_db is None:
        raise ValueError("Yahoo recovery requires a local DuckDB target; parquet patching is no longer supported")

    table_name = {
        "roster": "player_fantasy",
        "matchup": "matchup",
        "transaction": "transactions",
        "draft": "draft",
        "schedule": "schedule",
    }[fetcher]
    local_db.merge_table(
        table_name,
        df,
        LOCAL_DB_DEDUP_KEYS[fetcher],
        platform=platform,
        league_id=league_id,
    )


def _sort_gaps_by_dependency(gaps: list[Gap]) -> list[Gap]:
    """Sort gaps so matchups/schedules are processed before rosters/transactions/draft."""
    return sorted(gaps, key=lambda g: _PRIORITY_ORDER.get(g.fetcher, 2))


# =============================================================================
# Task 8: Per-fetcher resolve functions + main entry point
# =============================================================================


def _get_league_key(ctx, year: int) -> str | None:
    """Extract league key for a given year from context."""
    if hasattr(ctx, "get_league_id_for_year"):
        key = ctx.get_league_id_for_year(year)
        if key:
            return key
    if hasattr(ctx, "league_ids"):
        ids = ctx.league_ids
        if isinstance(ids, dict):
            return ids.get(str(year)) or ids.get(year)
    logger.warning("[RECOVERY] No league key found for year %d", year)
    return None


def _get_oauth(ctx):
    """Extract OAuth object from context."""
    if hasattr(ctx, "oauth"):
        return ctx.oauth
    if hasattr(ctx, "sc"):
        return ctx.sc
    logger.warning("[RECOVERY] No OAuth object found in context")
    return None


def _get_recovery_roster_fetcher(ctx, year: int, roster_fetchers: dict | None = None, oauth=None):
    """Reuse one YahooRosterFetcher per league during recovery."""
    league_key = _get_league_key(ctx, year)
    if not league_key:
        return None

    if roster_fetchers is not None and league_key in roster_fetchers:
        fetcher = roster_fetchers[league_key]
        if oauth is not None and hasattr(fetcher, "oauth"):
            fetcher.oauth = oauth
        return fetcher

    try:
        from multi_league.data_fetchers.yahoo.yahoo_rosters import YahooRosterFetcher

        oauth_file = ctx.oauth_file_path if hasattr(ctx, "oauth_file_path") else getattr(ctx, "oauth_file", None)
        if oauth_file is None and getattr(ctx, "oauth_credentials", None) and getattr(ctx, "data_directory", None):
            # Contexts without an oauth file (e.g., corpus one-year imports) carry
            # credentials inline; materialize them once inside the private task
            # directory so recovery can replay fetches like the main roster path.
            materialized = Path(ctx.data_directory) / "oauth_recovery.json"
            if not materialized.exists():
                materialized.write_text(json.dumps(ctx.oauth_credentials), encoding="utf-8")
            oauth_file = str(materialized)
        if oauth_file is None:
            logger.warning("[RECOVERY] No oauth_file in context for roster recovery")
            return None
        fetcher = YahooRosterFetcher(
            oauth_file=Path(oauth_file),
            league_id=league_key,
        )
        if oauth is not None and hasattr(fetcher, "oauth"):
            fetcher.oauth = oauth
        if roster_fetchers is not None:
            roster_fetchers[league_key] = fetcher
        return fetcher
    except Exception as e:
        logger.warning("[RECOVERY] Failed to create roster fetcher for %s: %s", league_key, e)
        return None


def _normalize_recovery_roster_df(df: pd.DataFrame, year: int, week: int) -> pd.DataFrame:
    """Normalize roster fetch output to the local player_fantasy schema used by recovery."""
    if df is None or df.empty:
        return pd.DataFrame()

    def _clean_text(series: pd.Series) -> pd.Series:
        return (
            series.astype("string")
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
        )

    normalized = df.copy()
    rename_map = {
        "player_name": "player",
        "manager_name": "manager",
        "player_id": "yahoo_player_id",
        "primary_position": "nfl_position",
    }
    normalized.rename(
        columns={k: v for k, v in rename_map.items() if k in normalized.columns},
        inplace=True,
    )

    if "manager_guid" in normalized.columns:
        normalized["manager_guid"] = _clean_text(normalized["manager_guid"])
    if "team_key" in normalized.columns:
        normalized["team_key"] = _clean_text(normalized["team_key"])

    if "franchise_id" not in normalized.columns:
        normalized["franchise_id"] = pd.Series(pd.NA, index=normalized.index, dtype="string")
    else:
        normalized["franchise_id"] = _clean_text(normalized["franchise_id"])

    if "manager_guid" in normalized.columns:
        missing_franchise = normalized["franchise_id"].isna()
        normalized.loc[missing_franchise, "franchise_id"] = normalized.loc[
            missing_franchise, "manager_guid"
        ].str.strip()

    if "manager_week" not in normalized.columns:
        normalized["manager_week"] = pd.Series(pd.NA, index=normalized.index, dtype="string")
    else:
        normalized["manager_week"] = _clean_text(normalized["manager_week"])

    manager_week_suffix = f"_{year}_{week}"
    missing_manager_week = normalized["manager_week"].isna()
    if missing_manager_week.any():
        franchise_ids = _clean_text(normalized["franchise_id"])
        fill_with_franchise = missing_manager_week & franchise_ids.notna()
        normalized.loc[fill_with_franchise, "manager_week"] = franchise_ids[fill_with_franchise] + manager_week_suffix

    missing_manager_week = normalized["manager_week"].isna()
    if missing_manager_week.any() and "team_key" in normalized.columns:
        fill_with_team_key = missing_manager_week & normalized["team_key"].notna()
        normalized.loc[fill_with_team_key, "manager_week"] = (
            normalized.loc[fill_with_team_key, "team_key"] + manager_week_suffix
        )

    return normalized


def _backfill_recovery_roster_identity_from_matchup(
    df: pd.DataFrame,
    local_db,
    year: int,
    week: int,
) -> pd.DataFrame:
    """Copy canonical roster identity from local matchup rows using team_key."""
    if df is None or df.empty or local_db is None or "team_key" not in df.columns:
        return df

    conn = local_db.connect()

    try:
        matchup_cols = {row[0] for row in conn.execute("DESCRIBE public.matchup").fetchall()}
    except Exception:
        return df

    if "team_key" not in matchup_cols or "franchise_id" not in matchup_cols:
        return df

    def _clean_text(series: pd.Series) -> pd.Series:
        return (
            series.astype("string")
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
        )

    def _match_expr(col: str) -> str:
        if col in matchup_cols:
            return f"NULLIF(TRIM(COALESCE({col}, '')), '') AS {col}"
        return f"NULL AS {col}"

    lookup = conn.execute(
        f"""
        SELECT DISTINCT
            NULLIF(TRIM(COALESCE(team_key, '')), '') AS team_key,
            {_match_expr('franchise_id')},
            {_match_expr('manager_guid')},
            {_match_expr('manager_week')},
            {_match_expr('manager')},
            {_match_expr('team_name')}
        FROM public.matchup
        WHERE year = ?
          AND week = ?
          AND NULLIF(TRIM(COALESCE(team_key, '')), '') IS NOT NULL
        """,
        [year, week],
    ).df()

    if lookup.empty:
        return df

    lookup["team_key"] = _clean_text(lookup["team_key"])
    lookup = lookup.dropna(subset=["team_key"]).drop_duplicates(subset=["team_key"], keep="first")
    if lookup.empty:
        return df

    enriched = df.copy()
    enriched["_team_key_lookup"] = _clean_text(enriched["team_key"])
    match_by_team = lookup.set_index("team_key")

    for col in ("franchise_id", "manager_guid", "team_name"):
        if col not in enriched.columns:
            enriched[col] = pd.Series(pd.NA, index=enriched.index, dtype="string")
        else:
            enriched[col] = _clean_text(enriched[col])
        match_values = enriched["_team_key_lookup"].map(match_by_team[col])
        fill_mask = enriched[col].isna() & match_values.notna()
        enriched.loc[fill_mask, col] = match_values[fill_mask]

    if "manager" not in enriched.columns:
        enriched["manager"] = pd.Series(pd.NA, index=enriched.index, dtype="string")
    else:
        enriched["manager"] = _clean_text(enriched["manager"])
    match_manager = enriched["_team_key_lookup"].map(match_by_team["manager"])
    manager_missing = enriched["manager"].isna() | enriched["manager"].str.lower().eq("unknown")
    fill_manager = manager_missing & match_manager.notna()
    enriched.loc[fill_manager, "manager"] = match_manager[fill_manager]

    if "manager_week" not in enriched.columns:
        enriched["manager_week"] = pd.Series(pd.NA, index=enriched.index, dtype="string")
    else:
        enriched["manager_week"] = _clean_text(enriched["manager_week"])
    match_manager_week = enriched["_team_key_lookup"].map(match_by_team["manager_week"])
    overwrite_manager_week = match_manager_week.notna()
    enriched.loc[overwrite_manager_week, "manager_week"] = match_manager_week[overwrite_manager_week]

    manager_week_suffix = f"_{year}_{week}"
    missing_manager_week = enriched["manager_week"].isna()
    fill_from_franchise = missing_manager_week & enriched["franchise_id"].notna()
    enriched.loc[fill_from_franchise, "manager_week"] = (
        enriched.loc[fill_from_franchise, "franchise_id"] + manager_week_suffix
    )

    return enriched.drop(columns=["_team_key_lookup"])


def _resolve_roster_gap(
    gap: Gap,
    ctx,
    data_dir: Path,
    oauth=None,
    roster_fetchers: dict | None = None,
    roster_teams: dict | None = None,
    local_db=None,
) -> bool | str:
    """Resolve a roster gap by replaying the original weekly roster fetch."""
    week, manager_token, manager_week = _parse_roster_gap_detail(gap.detail)
    if week is None:
        logger.warning("[RECOVERY] Cannot parse roster gap detail: %s", gap.detail)
        return False

    if manager_token is not None or manager_week is not None:
        logger.info("[RECOVERY] Canonicalizing narrow roster gap %s to weekly replay wk%d", gap.key, week)

    league_key = _get_league_key(ctx, gap.year)
    if not league_key:
        return False

    roster_fetcher = _get_recovery_roster_fetcher(
        ctx,
        gap.year,
        roster_fetchers=roster_fetchers,
        oauth=oauth,
    )
    if roster_fetcher is None:
        return False

    # Match the original season fetcher behavior: access-denied is a per-attempt
    # signal, not sticky state across later recovery passes for the same league.
    if hasattr(roster_fetcher, "_access_denied_logged"):
        roster_fetcher._access_denied_logged = False

    cached_teams = (roster_teams or {}).get(gap.year, {}) if roster_teams is not None else {}
    if not cached_teams:
        cached_teams = roster_fetcher.fetch_teams()
        if not cached_teams and getattr(roster_fetcher, "_access_denied_logged", False):
            raise RecoveryDeferredError(f"Yahoo API access denied while fetching teams for {league_key}")
        if roster_teams is not None and cached_teams:
            roster_teams[gap.year] = cached_teams

    if not cached_teams:
        raise ValueError(f"No teams found for roster recovery year {gap.year}")

    week_df, week_failures = roster_fetcher.fetch_all_rosters_for_week(gap.year, week, cached_teams)
    if week_df is not None and not week_df.empty:
        if "team_key" in week_df.columns:
            week_df = week_df.copy()
            team_guid_map = {
                team_key: info.get("manager_guid")
                for team_key, info in cached_teams.items()
                if info.get("manager_guid")
            }
            team_name_map = {
                team_key: info.get("team_name") for team_key, info in cached_teams.items() if info.get("team_name")
            }
            if team_guid_map:
                week_df = week_df.copy()
                if "manager_guid" in week_df.columns:
                    missing_guid = week_df["manager_guid"].isna() | (
                        week_df["manager_guid"].astype(str).str.strip() == ""
                    )
                    week_df.loc[missing_guid, "manager_guid"] = week_df.loc[missing_guid, "team_key"].map(team_guid_map)
                else:
                    week_df["manager_guid"] = week_df["team_key"].map(team_guid_map)
            if team_name_map:
                if "team_name" in week_df.columns:
                    missing_team_name = week_df["team_name"].isna() | (
                        week_df["team_name"].astype(str).str.strip() == ""
                    )
                    week_df.loc[missing_team_name, "team_name"] = week_df.loc[missing_team_name, "team_key"].map(
                        team_name_map
                    )
                else:
                    week_df["team_name"] = week_df["team_key"].map(team_name_map)
        combined = _normalize_recovery_roster_df(week_df, gap.year, week)
        combined = _backfill_recovery_roster_identity_from_matchup(combined, local_db, gap.year, week)
        _write_recovery_rows(local_db, "roster", combined, league_id=league_key)

    if week_failures:
        if getattr(roster_fetcher, "_access_denied_logged", False):
            raise RecoveryDeferredError(f"Yahoo API access denied while fetching roster week {week} for {league_key}")
        gap.last_error = f"Partial roster recovery for year {gap.year} week {week}: {len(week_failures)} team(s) failed"
        return False

    if week_df is None or week_df.empty:
        return "empty"

    return True


def _resolve_matchup_gap(gap: Gap, ctx, data_dir: Path, oauth=None, local_db=None) -> bool | str:
    """Resolve a matchup gap by replaying the original matchup fetcher."""
    league_key = _get_league_key(ctx, gap.year)
    if not league_key:
        return False

    from multi_league.data_fetchers.yahoo.yahoo_matchups import weekly_matchup_data

    week = None
    if gap.detail != "full_year":
        match = re.match(r"wk(\d+)", gap.detail)
        if not match:
            logger.warning("[RECOVERY] Cannot parse matchup gap detail: %s", gap.detail)
            return False
        week = int(match.group(1))

    result = weekly_matchup_data(ctx=ctx, year=gap.year, week=week)
    df, failed_weeks = result if isinstance(result, tuple) else (result, [])

    if df is None or df.empty:
        return "empty"

    _write_recovery_rows(local_db, "matchup", df, league_id=league_key)
    if failed_weeks:
        gap.last_error = f"Partial matchup recovery for year {gap.year}: failed weeks {sorted(set(failed_weeks))}"
        return False

    return True


def _resolve_transaction_gap(gap: Gap, ctx, data_dir: Path, oauth=None, local_db=None) -> bool | str:
    """Resolve a transaction gap by replaying the original season transaction fetch."""
    league_key = _get_league_key(ctx, gap.year)
    if not league_key:
        return False

    if oauth is None:
        oauth = _get_oauth(ctx)
    if oauth is None:
        return False

    df = fetch_transactions_for_year_recovery(oauth, league_key, gap.year, local_db=local_db)
    if df.empty:
        return "empty"

    _write_recovery_rows(local_db, "transaction", df, league_id=league_key)
    return True


def _resolve_draft_gap(gap: Gap, ctx, data_dir: Path, oauth=None, local_db=None) -> bool | str:
    """Resolve a draft gap by replaying the original season draft fetcher."""
    league_key = _get_league_key(ctx, gap.year)
    if not league_key:
        return False

    from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_data

    df = fetch_draft_data(ctx=ctx, year=gap.year)
    if df.empty:
        return "empty"

    _write_recovery_rows(local_db, "draft", df, league_id=league_key)
    return True


def _resolve_schedule_gap(gap: Gap, ctx, data_dir: Path, oauth=None, local_db=None) -> bool | str:
    """Resolve a schedule gap by replaying the original season schedule fetcher."""
    league_key = _get_league_key(ctx, gap.year)
    if not league_key:
        return False

    from multi_league.data_fetchers.yahoo.yahoo_schedules import fetch_schedule_for_year

    df = fetch_schedule_for_year(ctx=ctx, year=gap.year, local_db=local_db)
    if df is None or df.empty:
        return "empty"

    _write_recovery_rows(local_db, "schedule", df, league_id=league_key)
    return True


def _try_resolve_gap(
    gap: Gap,
    ctx,
    data_dir: Path,
    oauth=None,
    roster_fetchers: dict | None = None,
    roster_teams: dict | None = None,
    local_db=None,
) -> bool | str:
    """Dispatch gap resolution to the appropriate per-fetcher function.

    Returns:
        True (resolved), "empty" (valid but no data), "rate_limited" (retry later),
        or False (error).
    """
    try:
        if gap.fetcher == "roster":
            return _resolve_roster_gap(
                gap,
                ctx,
                data_dir,
                oauth=oauth,
                roster_fetchers=roster_fetchers,
                roster_teams=roster_teams,
                local_db=local_db,
            )
        elif gap.fetcher == "matchup":
            return _resolve_matchup_gap(gap, ctx, data_dir, oauth=oauth, local_db=local_db)
        elif gap.fetcher == "transaction":
            return _resolve_transaction_gap(gap, ctx, data_dir, oauth=oauth, local_db=local_db)
        elif gap.fetcher == "draft":
            return _resolve_draft_gap(gap, ctx, data_dir, oauth=oauth, local_db=local_db)
        elif gap.fetcher == "schedule":
            return _resolve_schedule_gap(gap, ctx, data_dir, oauth=oauth, local_db=local_db)
        else:
            logger.warning("[RECOVERY] Unknown fetcher type: %s", gap.fetcher)
            gap.last_error = f"Unknown fetcher type: {gap.fetcher}"
            return False
    except RecoveryDeferredError as e:
        gap.last_error = str(e)
        logger.info("[RECOVERY] Deferred %s due to Yahoo throttling/auth: %s", gap.key, e)
        return "rate_limited"
    except ValueError as e:
        gap.last_error = str(e)
        if _is_non_retryable_recovery_error(e):
            logger.error("[RECOVERY] Non-retryable error resolving gap %s: %s", gap.key, e)
            gap.hard_missing = True
            return "empty"
        if is_recovery_deferred_error(e):
            logger.info("[RECOVERY] Deferred %s due to Yahoo throttling/auth: %s", gap.key, e)
            return "rate_limited"
        logger.warning("[RECOVERY] Retryable gap error resolving %s: %s", gap.key, e)
        return False
    except Exception as e:
        gap.last_error = str(e)
        if _is_non_retryable_recovery_error(e):
            logger.error("[RECOVERY] Non-retryable error resolving gap %s: %s", gap.key, e)
            gap.hard_missing = True
            return "empty"
        if is_recovery_deferred_error(e):
            logger.info("[RECOVERY] Deferred %s due to Yahoo throttling/auth: %s", gap.key, e)
            return "rate_limited"
        logger.warning("[RECOVERY] Exception resolving gap %s: %s", gap.key, e)
        return False


def _build_team_map_for_year(
    ctx, year: int, local_db=None, oauth=None, roster_fetchers: dict | None = None
) -> dict[str, str]:
    """Build a manager/team map for a single year on demand."""
    if local_db is not None:
        for table_name in ("player_fantasy", "matchup"):
            try:
                df = local_db.read_table(table_name, year=year)
            except Exception:
                continue

            required_cols = {"manager", "team_key"}
            if df.empty or not required_cols.issubset(df.columns):
                continue

            local_map: dict[str, str] = {}
            keep_cols = [c for c in ["manager", "team_key", "manager_guid", "franchise_id"] if c in df.columns]
            local_df = df[keep_cols].dropna(subset=["team_key"]).copy()
            if local_df.empty:
                continue

            for _, row in local_df.iterrows():
                team_key = str(row.get("team_key", "")).strip()
                if not _is_yahoo_team_key(team_key):
                    continue
                for key_col in ("manager", "manager_guid", "franchise_id"):
                    value = row.get(key_col)
                    if pd.notna(value):
                        cleaned = str(value).strip()
                        if cleaned:
                            local_map.setdefault(cleaned, team_key)

            if local_map:
                logger.info(
                    "[RECOVERY] Built team map for year %d from local %s: %d entries",
                    year,
                    table_name,
                    len(local_map),
                )
                return local_map

    league_key = _get_league_key(ctx, year)
    if not league_key:
        logger.warning("[RECOVERY] Cannot build team map for year %d: no league key", year)
        return {}

    try:
        fetcher = _get_recovery_roster_fetcher(
            ctx,
            year,
            roster_fetchers=roster_fetchers,
            oauth=oauth,
        )
        if fetcher is None:
            return {}
        teams = fetcher.fetch_teams()
        if not teams and getattr(fetcher, "_access_denied_logged", False):
            raise RecoveryDeferredError(f"Yahoo API access denied while fetching teams for {league_key}")

        year_map: dict[str, str] = {}
        for team_key, info in teams.items():
            mgr_name = info.get("manager_name")
            if mgr_name:
                year_map[mgr_name] = team_key
            mgr_guid = info.get("manager_guid")
            if mgr_guid:
                year_map[mgr_guid] = team_key

        logger.info("[RECOVERY] Built team map for year %d: %d entries", year, len(year_map))
        return year_map
    except RecoveryDeferredError:
        raise
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.warning("[RECOVERY] Failed to build team map for year %d: %s", year, e)
        return {}


def run_yahoo_recovery(
    ctx,
    league_settings: dict,
    dead_man_minutes: int = 120,
    local_db=None,
    exclude_years: set[int] | None = None,
) -> RecoveryResult:
    """Main entry point for Yahoo data recovery. Replaces Phase 1.5 + 1.9.

    1. Detects gaps from manifests and local import audit.
    2. If no gaps, returns success immediately.
    3. Builds team maps for roster recovery.
    4. Runs recovery loop with escalating cooldowns.
    5. Returns RecoveryResult.

    `exclude_years`: years whose gaps should not trigger Yahoo API retries.
    Use this for years whose data came entirely from external staging —
    Yahoo's league_key for those years is typically unreachable and retries
    just loop forever.
    """
    data_dir = (
        Path(ctx.data_directory)
        if hasattr(ctx, "data_directory")
        else Path(ctx.data_dir if hasattr(ctx, "data_dir") else ".")
    )
    if local_db is None:
        raise ValueError("Yahoo recovery now requires a LocalLeagueDB target; parquet recovery paths are retired")

    # Step 1: Create OAuth session for API calls
    oauth = None
    if hasattr(ctx, "get_oauth_session"):
        try:
            oauth = ctx.get_oauth_session()
            logger.info("[RECOVERY] Created OAuth session via ctx.get_oauth_session()")
        except Exception as e:
            logger.warning("[RECOVERY] Failed to create OAuth session: %s", e)
    elif hasattr(ctx, "oauth"):
        oauth = ctx.oauth

    # Step 2: Build roster fetchers and team snapshots lazily so unrelated
    # schedule/matchup recovery does not block on roster prerequisites.
    roster_fetchers: dict[str, object] = {}
    roster_teams: dict[int, dict[str, dict[str, str]]] = {}

    # Step 3: Round-based recovery. Each round snapshots the exact current gap
    # set, attempts every gap once, then redetects from manifests + local DuckDB.
    def resolve_fn(gap: Gap) -> bool | str:
        return _try_resolve_gap(
            gap,
            ctx,
            data_dir,
            oauth=oauth,
            roster_fetchers=roster_fetchers,
            roster_teams=roster_teams,
            local_db=local_db,
        )

    def refresh_oauth():
        nonlocal oauth
        try:
            if hasattr(ctx, "get_oauth_session"):
                oauth = ctx.get_oauth_session()
            elif oauth is not None and hasattr(oauth, "refresh_access_token"):
                oauth.refresh_access_token()
            if oauth is not None:
                for fetcher in roster_fetchers.values():
                    if hasattr(fetcher, "oauth"):
                        fetcher.oauth = oauth
        except Exception as e:
            logger.warning("[RECOVERY] OAuth refresh failed: %s", e)

    start_time = time.time()
    deadline = start_time + dead_man_minutes * 60
    pass_num = 0
    cooldown_level = 0  # index into COOLDOWN_SCHEDULE
    resolved_total: list[Gap] = []
    hard_missing_total: list[Gap] = []
    hard_missing_keys: set[tuple] = set()
    empty_counts: dict[tuple, int] = {}
    attempt_counts: dict[tuple, int] = {}  # tracks attempts across passes (gap keys persist)
    stagnant_counts: dict[tuple, int] = {}  # tracks resolved-then-reappeared cycles
    prev_resolved_keys: set[tuple] = set()  # gap keys resolved in previous pass
    manifest_gaps, audit_gaps, initial_gaps = _collect_retryable_gaps(
        data_dir,
        local_db,
        league_settings,
        hard_missing_keys,
        exclude_years=exclude_years,
    )
    active_queue: dict[tuple, Gap] = {gap.key: gap for gap in initial_gaps}
    _rewrite_active_manifests(data_dir, list(active_queue.values()))

    while True:
        if time.time() >= deadline:
            logger.warning("[RECOVERY] Dead man triggered after %.1f minutes", dead_man_minutes)
            break

        if pass_num >= MAX_PASSES:
            logger.warning("[RECOVERY] Max passes (%d) reached — stopping", MAX_PASSES)
            break

        if pass_num > 0:
            # After pass 1, re-audit/re-read manifests before each new pass.
            manifest_gaps, audit_gaps, discovered_gaps = _collect_retryable_gaps(
                data_dir,
                local_db,
                league_settings,
                hard_missing_keys,
                exclude_years=exclude_years,
            )
            active_queue = _merge_gap_queue(active_queue, discovered_gaps)
            active_queue = {key: gap for key, gap in active_queue.items() if key not in hard_missing_keys}
            _rewrite_active_manifests(data_dir, list(active_queue.values()))

        if not active_queue:
            clear_all_manifests(data_dir)
            elapsed = time.time() - start_time
            return RecoveryResult(
                success=True,
                resolved=resolved_total,
                unresolved=[],
                hard_missing=hard_missing_total,
                rounds=pass_num,
                elapsed_seconds=elapsed,
            )

        # Sort the current queue with dependency ordering before cost.
        current_gaps = list(active_queue.values())
        current_gaps.sort(key=gap_sort_key)
        pass_num += 1
        refresh_oauth()

        logger.info(
            "[RECOVERY] Pass %d: %d gaps (%d manifest, %d audit), sorted by queue priority",
            pass_num,
            len(current_gaps),
            len(manifest_gaps),
            len(audit_gaps),
        )

        pass_progress = 0  # gaps resolved in this pass
        pass_deferred = False
        pass_resolved_keys: set[tuple] = set()
        blocked_scopes: dict[tuple[str, int], str | None] = {}
        stop_recovery = False

        # --- Detect stagnant gaps (resolved last pass but re-appeared) ---
        for gap in current_gaps:
            if gap.key in prev_resolved_keys:
                sc = stagnant_counts.get(gap.key, 0) + 1
                stagnant_counts[gap.key] = sc
                if sc >= STAGNANT_THRESHOLD:
                    hard_missing_keys.add(gap.key)
                    hard_missing_total.append(gap)
                    logger.info("[RECOVERY] Stagnant after %d resolve cycles: %s", sc, gap.key)

        # Re-filter after stagnant detection
        current_gaps = [g for g in current_gaps if g.key not in hard_missing_keys and g.key in active_queue]
        if not current_gaps:
            if not active_queue:
                clear_all_manifests(data_dir)
            break

        # --- Sequential gap cycling ---
        for gap in current_gaps:
            if time.time() >= deadline:
                logger.warning("[RECOVERY] Dead man triggered mid-pass %d", pass_num)
                break
            if gap.key not in active_queue:
                continue
            scope_key = _rate_limited_scope_key(gap)
            if scope_key in blocked_scopes:
                continue

            # Track attempts across passes (gap objects are recreated each pass).
            # Deferred Yahoo throttling should not consume an attempt.
            prior_attempts = attempt_counts.get(gap.key, 0)
            ac = prior_attempts + 1
            gap.attempts = ac
            cost = estimate_gap_cost(gap)
            logger.info(
                "[RECOVERY] Attempting %s (cost=%d, attempt=%d/%d)",
                gap.key,
                cost,
                ac,
                MAX_GAP_ATTEMPTS,
            )

            try:
                result = resolve_fn(gap)
            except Exception as e:
                logger.warning("[RECOVERY] Error resolving %s: %s", gap.key, e)
                result = False

            if result == "rate_limited":
                pass_deferred = True
                blocked_scopes[scope_key] = gap.last_error
                if prior_attempts > 0:
                    attempt_counts[gap.key] = prior_attempts
                else:
                    attempt_counts.pop(gap.key, None)
                if gap.key in active_queue:
                    active_queue[gap.key].last_error = gap.last_error

                _rewrite_active_manifests(data_dir, list(active_queue.values()))
                cooldown_level, stop_recovery = _cooldown_or_stop(cooldown_level, deadline, "deferred", gap)
                if stop_recovery:
                    break
                logger.info(
                    "[RECOVERY] Deferring remaining %s %d gaps until next pass",
                    gap.fetcher,
                    gap.year,
                )
                refresh_oauth()
                continue

            attempt_counts[gap.key] = ac

            if result is True:
                # --- Resolved ---
                resolved_total.append(gap)
                pass_progress += 1
                pass_resolved_keys.add(gap.key)
                cooldown_level = 0  # reset escalation on success
                empty_counts.pop(gap.key, None)
                active_queue.pop(gap.key, None)
                _rewrite_active_manifests(data_dir, list(active_queue.values()))
                logger.info("[RECOVERY] Resolved: %s", gap.key)

            elif result == "empty":
                # API returned valid but empty — data may not exist
                count = empty_counts.get(gap.key, 0) + 1
                empty_counts[gap.key] = count
                if gap.hard_missing or count >= HARD_MISSING_THRESHOLD:
                    gap.hard_missing = True
                    hard_missing_keys.add(gap.key)
                    hard_missing_total.append(gap)
                    active_queue.pop(gap.key, None)
                    logger.info("[RECOVERY] Hard missing after %d empties: %s", count, gap.key)
                else:
                    if gap.key in active_queue:
                        active_queue[gap.key].last_error = gap.last_error or f"Empty recovery result for {gap.key}"
                _rewrite_active_manifests(data_dir, list(active_queue.values()))

            else:
                # False — unknown/retryable failure that was not classified as Yahoo throttling
                if ac >= MAX_GAP_ATTEMPTS:
                    gap.hard_missing = True
                    hard_missing_keys.add(gap.key)
                    hard_missing_total.append(gap)
                    active_queue.pop(gap.key, None)
                    _rewrite_active_manifests(data_dir, list(active_queue.values()))
                    logger.info("[RECOVERY] Hard missing after %d attempts: %s", ac, gap.key)
                    continue
                if gap.key in active_queue:
                    active_queue[gap.key].last_error = gap.last_error

                # Conservative cooldown for unknown transient failures.
                _rewrite_active_manifests(data_dir, list(active_queue.values()))
                cooldown_level, stop_recovery = _cooldown_or_stop(cooldown_level, deadline, "transient", gap)
                if stop_recovery:
                    break
                # After cooldown, continue to next gap (don't retry same gap immediately)

        if stop_recovery:
            break

        # --- End of pass: track resolved keys for stagnant detection ---
        prev_resolved_keys = pass_resolved_keys

        # --- End of pass: reconcile queue with the current DB state ---
        _, _, discovered_gaps = _collect_retryable_gaps(
            data_dir,
            local_db,
            league_settings,
            hard_missing_keys,
            exclude_years=exclude_years,
        )
        active_queue = _merge_gap_queue(active_queue, discovered_gaps)
        active_queue = {key: gap for key, gap in active_queue.items() if key not in hard_missing_keys}
        remaining_gaps = list(active_queue.values())
        _rewrite_active_manifests(data_dir, remaining_gaps)

        logger.info(
            "[RECOVERY] Pass %d complete: %d resolved, %d remaining, %d hard_missing",
            pass_num,
            pass_progress,
            len(remaining_gaps),
            len(hard_missing_total),
        )

        if not remaining_gaps:
            clear_all_manifests(data_dir)
            break

        if pass_progress == 0 and not pass_deferred:
            # Full pass with zero progress — escalate cooldown before next pass
            cooldown_level, stop_recovery = _cooldown_or_stop(cooldown_level, deadline, "zero_progress")
            if stop_recovery:
                break

    elapsed = time.time() - start_time
    final_manifest_gaps = _detect_gaps_from_manifests(data_dir)
    final_audit_gaps = _detect_gaps_from_local_db(local_db, league_settings)
    unresolved_total = _canonicalize_gap_units(
        _merge_gaps(final_manifest_gaps, final_audit_gaps),
        local_db=local_db,
    )
    unresolved_total = [gap for gap in unresolved_total if gap.key not in hard_missing_keys]
    if exclude_years:
        unresolved_total = [gap for gap in unresolved_total if gap.year not in exclude_years]

    logger.info(
        "[RECOVERY] Complete: success=%s resolved=%d unresolved=%d hard_missing=%d rounds=%d elapsed=%.1fs",
        len(unresolved_total) == 0,
        len(resolved_total),
        len(unresolved_total),
        len(hard_missing_total),
        pass_num,
        elapsed,
    )

    return RecoveryResult(
        success=len(unresolved_total) == 0,
        resolved=resolved_total,
        unresolved=unresolved_total,
        hard_missing=hard_missing_total,
        rounds=pass_num,
        elapsed_seconds=elapsed,
    )
