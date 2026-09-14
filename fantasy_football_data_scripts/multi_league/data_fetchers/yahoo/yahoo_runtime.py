"""Minimal runtime helpers for Yahoo fetchers.

These helpers let Yahoo scripts depend on a thin execution contract instead of
reaching into the full LeagueContext blob for every concern.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any
from collections.abc import Mapping

from multi_league.core.date_utils import get_current_nfl_season_year


def _ctx_value(ctx, attr: str, default=None):
    return getattr(ctx, attr, default) if ctx is not None else default


def get_league_ids(ctx=None, league_ids: Mapping[str, str] | Mapping[int, str] | None = None) -> dict[str, str]:
    """Return a normalized year -> league_key mapping."""
    source = league_ids if league_ids is not None else _ctx_value(ctx, "league_ids", {}) or {}
    normalized: dict[str, str] = {}
    for year, league_key in dict(source).items():
        if league_key:
            normalized[str(year)] = str(league_key)
    return normalized


def get_league_key_for_year(
    year: int,
    *,
    ctx=None,
    league_ids: Mapping[str, str] | Mapping[int, str] | None = None,
    explicit_league_key: str | None = None,
) -> str | None:
    """Resolve the league key for a season without needing the full context."""
    if explicit_league_key:
        return explicit_league_key

    if ctx is not None and hasattr(ctx, "get_league_id_for_year"):
        try:
            key = ctx.get_league_id_for_year(year)
            if key:
                return key
        except Exception:
            pass

    return get_league_ids(ctx=ctx, league_ids=league_ids).get(str(year))


def get_manager_name_overrides(ctx=None, manager_name_overrides: Mapping[str, str] | None = None) -> dict[str, str]:
    source = (
        manager_name_overrides if manager_name_overrides is not None else _ctx_value(ctx, "manager_name_overrides", {})
    )
    return dict(source or {})


def get_yahoo_data_dir(ctx=None, data_dir: str | Path | None = None) -> Path:
    if data_dir is not None:
        return Path(data_dir)
    ctx_dir = _ctx_value(ctx, "data_directory", None) or _ctx_value(ctx, "data_dir", None)
    if ctx_dir is not None:
        return Path(ctx_dir)
    return Path.cwd()


def get_output_dir(subdir: str, *, ctx=None, data_dir: str | Path | None = None) -> Path:
    path = get_yahoo_data_dir(ctx=ctx, data_dir=data_dir) / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_configured_years(
    *,
    ctx=None,
    league_ids: Mapping[str, str] | Mapping[int, str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    years: list[int] | None = None,
) -> list[int]:
    """Resolve the year set from explicit args first, then league_ids, then legacy range."""
    if years is not None:
        return sorted({int(year) for year in years})

    normalized_ids = get_league_ids(ctx=ctx, league_ids=league_ids)
    if normalized_ids:
        return sorted(int(year) for year in normalized_ids.keys())

    if start_year is None:
        start_year = _ctx_value(ctx, "start_year", None)
    if end_year is None:
        end_year = _ctx_value(ctx, "end_year", None)

    if start_year is None and end_year is None:
        return []

    current_year = get_current_nfl_season_year()
    if end_year is None:
        end_year = current_year
    if start_year is None:
        start_year = end_year

    return list(range(int(start_year), int(end_year) + 1))


def get_game_code(ctx=None, game_code: str | None = None) -> str:
    return game_code or _ctx_value(ctx, "game_code", "nfl") or "nfl"


def get_oauth_session(
    *,
    ctx=None,
    oauth_file: str | Path | None = None,
    oauth_credentials: Mapping[str, Any] | None = None,
):
    """Build an OAuth session from the minimal Yahoo runtime inputs."""
    if ctx is not None and hasattr(ctx, "get_oauth_session"):
        return ctx.get_oauth_session()

    creds = oauth_credentials if oauth_credentials is not None else _ctx_value(ctx, "oauth_credentials", None)
    oauth_path = oauth_file if oauth_file is not None else _ctx_value(ctx, "oauth_file_path", None)

    try:
        from yahoo_oauth import OAuth2
    except ImportError as exc:
        raise ImportError("yahoo_oauth is required. Install with: pip install yahoo_oauth") from exc

    if oauth_path:
        path = Path(oauth_path)
        if not path.exists():
            raise FileNotFoundError(f"OAuth file not found: {path}")
        oauth = OAuth2(None, None, from_file=str(path))
        if not oauth.token_is_valid():
            oauth.refresh_access_token()
        return oauth

    if creds:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(dict(creds), tmp)
            tmp_path = tmp.name
        try:
            oauth = OAuth2(None, None, from_file=tmp_path)
            if not oauth.token_is_valid():
                oauth.refresh_access_token()
            return oauth
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    raise ValueError("Yahoo runtime requires oauth_file or oauth_credentials")


def materialize_oauth_file(
    *,
    ctx=None,
    oauth_file: str | Path | None = None,
    oauth_credentials: Mapping[str, Any] | None = None,
    oauth_session=None,
) -> tuple[Path, bool]:
    """Return a file-backed OAuth payload for legacy code paths that still need one."""
    if oauth_file is not None:
        path = Path(oauth_file)
        if not path.exists():
            raise FileNotFoundError(f"OAuth file not found: {path}")
        return path, False

    ctx_oauth_file = _ctx_value(ctx, "oauth_file_path", None)
    if ctx_oauth_file:
        path = Path(ctx_oauth_file)
        if path.exists():
            return path, False

    creds = oauth_credentials if oauth_credentials is not None else _ctx_value(ctx, "oauth_credentials", None)
    if creds is not None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(dict(creds), tmp, indent=2)
            return Path(tmp.name), True

    if oauth_session is not None:
        payload = {
            "access_token": "expired",
            "consumer_key": os.environ.get("YAHOO_CLIENT_ID", ""),
            "consumer_secret": os.environ.get("YAHOO_CLIENT_SECRET", ""),
            "refresh_token": getattr(oauth_session, "refresh_token", ""),
            "token_time": getattr(oauth_session, "token_time", 0.0),
            "token_type": "bearer",
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            json.dump(payload, tmp, indent=2)
            return Path(tmp.name), True

    raise ValueError("Yahoo runtime requires oauth_file, oauth_credentials, or oauth_session")
