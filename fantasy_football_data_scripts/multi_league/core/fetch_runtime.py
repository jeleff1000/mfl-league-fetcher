"""Shared thin runtime wrapper for import and fetch orchestration.

This module normalizes the minimum execution contract needed across Yahoo,
Sleeper, and ESPN flows:

- platform
- league identity
- league database name
- local data directory
- year -> league_id mapping
- auth payload
- manager overrides

It is intentionally smaller than the legacy context classes. Fetchers and
orchestrators can depend on this runtime instead of reaching into the full
serialized context blob.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _sanitize_database_name(name: str) -> str:
    """Local copy of DB-name sanitization to avoid import cycles."""
    if not name:
        return "league_db"

    db = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not db:
        db = "league_db"
    if db[0].isdigit():
        db = "l_" + db
    return db[:63]


def _as_path(value: Any) -> Path | None:
    if value is None:
        return None
    return Path(value)


def _normalize_league_ids(league_ids: Any) -> dict[str, str]:
    if not league_ids:
        return {}
    return {str(year): str(league_id) for year, league_id in dict(league_ids).items() if league_id}


def detect_platform(source: Any) -> str:
    """Detect platform from a context/session-like source."""
    if isinstance(source, (str, Path)):
        return load_runtime(source).platform

    platform = getattr(source, "platform", None)
    if platform:
        return str(platform).lower()

    cls_name = type(source).__name__.lower()
    if "sleeper" in cls_name:
        return "sleeper"
    if "espn" in cls_name:
        return "espn"
    if "session" in cls_name and getattr(source, "platform", None):
        return str(source.platform).lower()
    return "yahoo"


def _resolve_db_name(source: Any, league_name: str) -> str:
    db_name = getattr(source, "database_name", None) or getattr(source, "motherduck_db_name", None)
    if db_name:
        return str(db_name)
    return _sanitize_database_name(league_name)


def _resolve_auth(source: Any, platform: str) -> dict[str, Any]:
    session_auth = getattr(source, "auth", None) or {}

    if platform == "yahoo":
        oauth_file = session_auth.get("oauth_file") or getattr(source, "oauth_file_path", None)
        if oauth_file is None and hasattr(source, "oauth_file"):
            oauth_file = source.oauth_file()
        oauth_credentials = session_auth.get("oauth_credentials") or getattr(source, "oauth_credentials", None)
        auth: dict[str, Any] = {}
        if oauth_file:
            auth["oauth_file"] = Path(oauth_file)
        if oauth_credentials:
            auth["oauth_credentials"] = dict(oauth_credentials)
        refresh_token = session_auth.get("refresh_token")
        if refresh_token:
            auth["refresh_token"] = refresh_token
        return auth

    if platform == "espn":
        espn_s2 = session_auth.get("espn_s2") or getattr(source, "espn_s2", None)
        swid = session_auth.get("swid") or getattr(source, "swid", None)
        auth = {}
        if espn_s2:
            auth["espn_s2"] = espn_s2
        if swid:
            auth["swid"] = swid
        return auth

    return {}


@dataclass(frozen=True)
class LeagueRuntime:
    """Minimal execution contract shared across platforms."""

    platform: str
    league_id: str
    league_name: str
    db_name: str
    data_dir: Path
    league_ids: dict[str, str] = field(default_factory=dict)
    auth: dict[str, Any] = field(default_factory=dict)
    manager_name_overrides: dict[str, str] = field(default_factory=dict)
    start_year: int | None = None
    end_year: int | None = None
    import_mode: str | None = None
    context_path: Path | None = None

    @property
    def years(self) -> list[int]:
        if self.league_ids:
            return sorted(int(year) for year in self.league_ids)

        years: list[int] = []
        if self.start_year is not None:
            years.append(int(self.start_year))
        if self.end_year is not None:
            years.append(int(self.end_year))
        return sorted(set(years))

    @property
    def is_single_year_import(self) -> bool:
        if self.import_mode == "quick":
            return True
        if self.start_year is None or self.end_year is None:
            return len(self.years) <= 1
        return int(self.start_year) == int(self.end_year)

    @property
    def context_filename(self) -> str:
        if self.platform == "sleeper":
            return "sleeper_context.json"
        if self.platform == "espn":
            return "espn_context.json"
        if self.platform == "fleaflicker":
            return "fleaflicker_context.json"
        return "league_context.json"

    def league_id_for_year(self, year: int) -> str | None:
        year_key = str(year)
        if self.league_ids:
            return self.league_ids.get(year_key)

        if self.end_year is not None and int(year) != int(self.end_year):
            return None

        return self.league_id


def runtime_from_source(source: Any, *, context_path: str | Path | None = None) -> LeagueRuntime:
    """Build a thin runtime wrapper from a context object, session, or path."""
    if isinstance(source, (str, Path)):
        return load_runtime(source)

    platform = detect_platform(source)
    league_name = str(getattr(source, "league_name", "") or "")
    league_id = str(getattr(source, "league_id", "") or "")
    data_dir = _as_path(getattr(source, "data_dir", None) or getattr(source, "data_directory", None))
    if data_dir is None:
        data_dir = Path.cwd()

    league_ids = _normalize_league_ids(getattr(source, "league_ids", None))
    auth = _resolve_auth(source, platform)
    manager_name_overrides = dict(getattr(source, "manager_name_overrides", None) or {})

    start_year = getattr(source, "start_year", None)
    end_year = getattr(source, "end_year", None)
    import_mode = getattr(source, "import_mode", None)

    if start_year is None and league_ids:
        start_year = min(int(year) for year in league_ids)
    if end_year is None and league_ids:
        end_year = max(int(year) for year in league_ids)

    resolved_context_path = resolve_context_path(source, explicit_path=context_path)

    return LeagueRuntime(
        platform=platform,
        league_id=league_id,
        league_name=league_name,
        db_name=_resolve_db_name(source, league_name),
        data_dir=data_dir,
        league_ids=league_ids,
        auth=auth,
        manager_name_overrides=manager_name_overrides,
        start_year=int(start_year) if start_year is not None else None,
        end_year=int(end_year) if end_year is not None else None,
        import_mode=str(import_mode) if import_mode is not None else None,
        context_path=resolved_context_path,
    )


def resolve_context_path(source: Any, explicit_path: str | Path | None = None) -> Path | None:
    """Resolve the serialized context path for a context/session-like source."""
    if explicit_path is not None:
        return Path(explicit_path)

    context_path = getattr(source, "context_path", None)
    if context_path:
        return Path(context_path)

    if isinstance(source, (str, Path)):
        return Path(source)

    platform = detect_platform(source)
    data_dir = _as_path(getattr(source, "data_dir", None) or getattr(source, "data_directory", None))
    if data_dir is None:
        return None

    if platform == "sleeper":
        filename = "sleeper_context.json"
    elif platform == "espn":
        filename = "espn_context.json"
    elif platform == "fleaflicker":
        filename = "fleaflicker_context.json"
    else:
        filename = "league_context.json"

    candidates = [data_dir / filename, data_dir.parent / filename]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def load_context_source(context_path: str | Path) -> Any:
    """Load a legacy context object from disk with platform detection."""
    path = Path(context_path)
    if not path.exists():
        raise FileNotFoundError(f"Context file not found: {path}")

    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)

    platform = str(data.get("platform", "yahoo")).lower()
    if platform == "sleeper":
        from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext

        return SleeperContext.from_dict(data)
    if platform == "espn":
        from multi_league.data_fetchers.espn.espn_context import ESPNContext

        return ESPNContext.from_dict(data)
    if platform == "fleaflicker":
        from multi_league.data_fetchers.fleaflicker.fleaflicker_context import FleaflickerContext

        return FleaflickerContext.from_dict(data)

    from multi_league.core.league_context import LeagueContext

    return LeagueContext.from_dict(data)


def load_runtime(context_path: str | Path) -> LeagueRuntime:
    """Load runtime directly from a serialized context file."""
    path = Path(context_path)
    source = load_context_source(path)
    return runtime_from_source(source, context_path=path)
