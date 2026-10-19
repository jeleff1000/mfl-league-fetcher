"""Thin wrappers around existing Yahoo fetcher methods for surgical single-item recovery."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from multi_league.core.transaction_periods import TransactionWeekError, local_yahoo_transaction_windows

logger = logging.getLogger(__name__)


class RecoveryDeferredError(RuntimeError):
    """Transient Yahoo failure that should defer recovery instead of marking gaps missing."""


_DEFERRED_ERROR_MARKERS = (
    "request denied",
    "access denied",
    "rate limit",
    "rate limited",
    "too many requests",
    "limit exceeded",
    "forbidden",
    "http 403",
    "403 client error",
    "http 429",
    "429 client error",
    "authentication failed",
    "unauthorized",
    "token expired",
    "invalid cookie",
    "please log in again",
)


def is_recovery_deferred_error(exc: BaseException | str | None) -> bool:
    """Return True when an error looks like Yahoo throttling/auth denial."""
    if exc is None:
        return False
    text = str(exc).strip().lower()
    return any(marker in text for marker in _DEFERRED_ERROR_MARKERS)


def _clean_text(series: pd.Series) -> pd.Series:
    return (
        series.astype("string")
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
    )


def fetch_single_team_roster(
    roster_fetcher,
    year: int,
    week: int,
    team_key: str,
    manager_name: str,
    manager_guid: str = None,
) -> pd.DataFrame:
    """Fetch roster for a single team/week using existing YahooRosterFetcher.

    Returns DataFrame with columns matching fetcher output, or empty DataFrame on
    non-throttling failure. Raises RecoveryDeferredError when Yahoo is temporarily
    denying requests.
    """
    try:
        rows = roster_fetcher.fetch_roster_for_week(
            year=year,
            week=week,
            team_key=team_key,
            manager_name=manager_name,
            manager_guid=manager_guid,
        )
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        rename_map = {
            "player_name": "player",
            "manager_name": "manager",
            "player_id": "yahoo_player_id",
            "primary_position": "nfl_position",
        }
        df.rename(
            columns={k: v for k, v in rename_map.items() if k in df.columns},
            inplace=True,
        )

        if "team_key" not in df.columns:
            df["team_key"] = team_key
        else:
            df["team_key"] = _clean_text(df["team_key"]).fillna(team_key)

        if "franchise_id" not in df.columns:
            df["franchise_id"] = pd.Series(pd.NA, index=df.index, dtype="string")
        else:
            df["franchise_id"] = _clean_text(df["franchise_id"])

        if manager_guid:
            df["franchise_id"] = df["franchise_id"].fillna(str(manager_guid).strip())
        elif "manager_guid" in df.columns:
            df["manager_guid"] = _clean_text(df["manager_guid"])
            df["franchise_id"] = df["franchise_id"].fillna(df["manager_guid"].str.strip())

        if "manager_week" not in df.columns:
            df["manager_week"] = pd.Series(pd.NA, index=df.index, dtype="string")
        else:
            df["manager_week"] = _clean_text(df["manager_week"])

        manager_week_key = df["franchise_id"].where(df["franchise_id"].notna(), df["team_key"])
        missing_manager_week = df["manager_week"].isna()
        fill_mask = missing_manager_week & manager_week_key.notna()
        df.loc[fill_mask, "manager_week"] = manager_week_key[fill_mask] + f"_{year}_{week}"

        return df
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.debug(f"[RECOVERY API] Roster fetch failed {team_key} wk{week}: {e}")
        return pd.DataFrame()


def fetch_single_week_matchups(
    oauth,
    league_key: str,
    year: int,
    week: int,
    manager_overrides: dict = None,
) -> pd.DataFrame:
    """Fetch matchups for a single week using existing parse_matchups_for_week.

    Returns DataFrame or empty DataFrame on non-throttling failure. Raises
    RecoveryDeferredError when Yahoo is temporarily denying requests.
    """
    try:
        from multi_league.data_fetchers.yahoo.yahoo_matchups import parse_matchups_for_week

        rows = parse_matchups_for_week(oauth, league_key, year, week, manager_overrides or {})
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.debug(f"[RECOVERY API] Matchup fetch failed {league_key} wk{week}: {e}")
        return pd.DataFrame()


def fetch_transactions_for_year_recovery(
    ctx,
    year: int,
    local_db,
) -> pd.DataFrame:
    """Replay the normal transaction path, including its calendar and aliases."""
    try:
        from multi_league.data_fetchers.yahoo.yahoo_transactions import (
            fetch_transactions,
            load_matchup_windows,
        )

        # Missing matchups need not mean missing dates: reuse the normal
        # retained/calendar cache path before making one bulk Yahoo request.
        windows = load_matchup_windows(ctx, year, local_db=local_db)
        if windows.empty:
            raise RecoveryDeferredError(f"Yahoo dated calendar unavailable for {year}; defer transaction recovery")
        return fetch_transactions(ctx, year, matchup_windows=windows, local_db=local_db)

    except (TransactionWeekError, RecoveryDeferredError):
        raise
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.warning("[RECOVERY API] Transaction recovery failed year %d: %s", year, e)
        return pd.DataFrame()


def _build_matchup_windows(local_db, year: int) -> pd.DataFrame:
    """Build matchup_windows DataFrame from the current local DuckDB state."""
    needed_cols = ["year", "week", "week_start", "week_end", "cumulative_week"]

    if local_db is None:
        return pd.DataFrame(columns=needed_cols)

    return local_yahoo_transaction_windows(local_db, year)


def fetch_draft_for_year(oauth, league_key: str, year: int) -> pd.DataFrame:
    """Fetch draft results for a year."""
    try:
        from multi_league.data_fetchers.yahoo.yahoo_client import YahooClient

        client = YahooClient(oauth)
        drafts = client.get_drafts(league_key)
        if not drafts:
            return pd.DataFrame()
        df = pd.DataFrame(drafts)
        df["year"] = year
        return df
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.debug(f"[RECOVERY API] Draft fetch failed {league_key}: {e}")
        return pd.DataFrame()


def fetch_schedule_for_year(ctx_or_oauth, league_key: str, year: int, data_dir=None) -> pd.DataFrame:
    """Fetch schedule for a year. Returns DataFrame or empty on failure.

    Schedules for completed seasons are derived from matchup data (no API call needed).
    For the current season, uses the Yahoo scoreboard API. Raises
    RecoveryDeferredError when Yahoo is temporarily denying requests.
    """
    try:
        from multi_league.data_fetchers.yahoo.yahoo_schedules import fetch_schedule_for_year as _fetch

        # The real fetch_schedule_for_year takes ctx as first arg.
        if hasattr(ctx_or_oauth, "data_directory"):
            result = _fetch(ctx=ctx_or_oauth, year=year)
        else:
            # No full ctx available - try to derive from matchup data on disk.
            if data_dir:
                matchup_dir = Path(data_dir) / "matchup_data"
                for p in matchup_dir.glob("*.parquet"):
                    try:
                        df = pd.read_parquet(p)
                        yr_df = df[df["year"] == year] if "year" in df.columns else pd.DataFrame()
                        if len(yr_df) > 0:
                            sched_cols = [
                                "year",
                                "week",
                                "manager",
                                "opponent",
                                "team_points",
                                "opponent_points",
                                "win",
                                "loss",
                                "tie",
                            ]
                            available = [c for c in sched_cols if c in yr_df.columns]
                            return yr_df[available].copy()
                    except Exception:
                        continue
            return pd.DataFrame()

        if result is None or (isinstance(result, pd.DataFrame) and result.empty):
            return pd.DataFrame()
        return result if isinstance(result, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.debug(f"[RECOVERY API] Schedule fetch failed {league_key}: {e}")
        return pd.DataFrame()


def fetch_schedule_for_week(
    ctx_or_oauth,
    league_key: str,
    year: int,
    week: int,
    manager_overrides: dict | None = None,
) -> pd.DataFrame:
    """Fetch a single schedule week directly from Yahoo's scoreboard API.

    Returns DataFrame or empty on non-throttling failure. Raises
    RecoveryDeferredError when Yahoo is temporarily denying requests.
    """
    try:
        from multi_league.data_fetchers.yahoo import yahoo_schedules as sched_mod

        oauth = ctx_or_oauth.get_oauth_session() if hasattr(ctx_or_oauth, "get_oauth_session") else ctx_or_oauth
        if oauth is None:
            return pd.DataFrame()

        sched_mod.oauth = oauth
        resolved_overrides = manager_overrides
        if resolved_overrides is None and hasattr(ctx_or_oauth, "manager_name_overrides"):
            resolved_overrides = ctx_or_oauth.manager_name_overrides

        rows = sched_mod.parse_week_schedule(
            league_key,
            year,
            week,
            resolved_overrides or {},
        )
        if not rows:
            return pd.DataFrame()
        return sched_mod.coerce_dtypes(pd.DataFrame(rows))
    except Exception as e:
        if is_recovery_deferred_error(e):
            raise RecoveryDeferredError(str(e)) from e
        logger.debug(f"[RECOVERY API] Schedule fetch failed {league_key} wk{week}: {e}")
        return pd.DataFrame()
