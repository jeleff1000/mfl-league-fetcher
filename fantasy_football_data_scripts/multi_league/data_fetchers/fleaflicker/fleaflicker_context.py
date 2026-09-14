"""Fleaflicker league context."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from multi_league.core.date_utils import get_current_nfl_season_year

YearFilter = int | str | Iterable[int | str] | None


def resolve_years_to_fetch(ctx: FleaflickerContext, year_filter: YearFilter = None) -> list[int]:
    if year_filter is None:
        return ctx.get_processing_years()
    raw_years: Iterable[int | str] = [year_filter] if isinstance(year_filter, int | str) else year_filter
    years: list[int] = []
    seen: set[int] = set()
    for raw in raw_years:
        try:
            year = int(raw)
        except (TypeError, ValueError):
            continue
        if year not in seen:
            seen.add(year)
            years.append(year)
    return years


@dataclass
class FleaflickerContext:
    league_id: str
    league_name: str
    start_year: int
    end_year: int | None = None
    data_directory: Path | None = None
    league_ids: dict[str, str] = field(default_factory=dict)
    manager_name_overrides: dict[str, str] = field(default_factory=dict)
    franchise_merges: list[dict[str, Any]] = field(default_factory=list)
    external_column_maps: list[dict] = field(default_factory=list)
    external_identity_maps: dict[str, dict] = field(default_factory=dict)
    has_external_data: bool = False
    merge_source: dict[str, Any] | None = None
    merge_sources: list[dict[str, Any]] = field(default_factory=list)
    import_mode: str = "full"
    require_oauth: bool = False
    keeper_rules: dict[str, Any] | None = None
    league_rules: dict[str, Any] | None = None
    standings_weights: dict[str, Any] | None = None
    database_name: str | None = None
    motherduck_db_name: str | None = None
    max_workers: int = 12
    rate_limit_per_min: int = 0
    created_at: str | None = None
    updated_at: str | None = None
    platform: str = "fleaflicker"

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()
        if self.data_directory is None:
            safe_name = self._sanitize_name(self.league_name)
            self.data_directory = Path.home() / "fantasy_football_data" / f"fleaflicker_{safe_name}"
        else:
            self.data_directory = Path(self.data_directory).resolve()
        self._create_directories()
        self._validate()

    @staticmethod
    def _sanitize_name(name: str) -> str:
        safe = re.sub(r"[^\w\-]", "_", name.lower())
        safe = re.sub(r"_+", "_", safe)
        return safe.strip("_") or "league"

    def _create_directories(self):
        for directory in (
            self.data_directory,
            self.data_directory / "league_settings",
            self.player_data_directory,
            self.matchup_data_directory,
            self.transaction_data_directory,
            self.draft_data_directory,
            self.schedule_data_directory,
            self.logs_directory,
            self.cache_directory,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def _validate(self):
        if not self.league_id:
            raise ValueError("league_id is required")
        if not self.league_name:
            raise ValueError("league_name is required")
        current = get_current_nfl_season_year()
        if self.start_year < 2005 or self.start_year > current + 1:
            raise ValueError(f"Invalid start_year: {self.start_year}")
        if self.end_year is not None and self.end_year < self.start_year:
            raise ValueError(f"end_year ({self.end_year}) cannot be before start_year ({self.start_year})")

    @property
    def player_data_directory(self) -> Path:
        return self.data_directory / "player_data"

    @property
    def matchup_data_directory(self) -> Path:
        return self.data_directory / "matchup_data"

    @property
    def transaction_data_directory(self) -> Path:
        return self.data_directory / "transaction_data"

    @property
    def draft_data_directory(self) -> Path:
        return self.data_directory / "draft_data"

    @property
    def schedule_data_directory(self) -> Path:
        return self.data_directory / "schedule_data"

    @property
    def logs_directory(self) -> Path:
        return self.data_directory / "logs"

    @property
    def cache_directory(self) -> Path:
        return self.data_directory / "cache"

    @property
    def canonical_player_file(self) -> Path:
        return self.data_directory / "player_fantasy.parquet"

    @property
    def canonical_matchup_file(self) -> Path:
        return self.data_directory / "matchup.parquet"

    @property
    def canonical_transaction_file(self) -> Path:
        return self.data_directory / "transactions.parquet"

    @property
    def canonical_draft_file(self) -> Path:
        return self.data_directory / "draft.parquet"

    @property
    def is_single_year_import(self) -> bool:
        if self.import_mode == "quick":
            return True
        effective_end = self.end_year or get_current_nfl_season_year()
        return self.start_year == effective_end

    @property
    def keepers_enabled(self) -> bool:
        return bool((self.keeper_rules or {}).get("enabled", False))

    @property
    def max_keepers(self) -> int:
        return int((self.keeper_rules or {}).get("max_keepers", 0))

    @property
    def keeper_budget(self) -> int:
        return int((self.keeper_rules or {}).get("budget", 200))

    @property
    def sacko_mode(self) -> str:
        return (self.league_rules or {}).get("sacko_mode", "consolation_bracket")

    @property
    def use_regular_season_sacko(self) -> bool:
        return self.sacko_mode == "regular_season_last"

    def get_league_id_for_year(self, year: int) -> str | None:
        return self.league_ids.get(str(year)) or self.league_id

    def get_year_range(self) -> range:
        end = self.end_year if self.end_year else get_current_nfl_season_year()
        return range(self.start_year, end + 1)

    def get_processing_years(self, season_cap: int | None = None) -> list[int]:
        cap = season_cap if season_cap is not None else get_current_nfl_season_year()
        discovered = []
        for year_key in self.league_ids:
            try:
                year = int(year_key)
            except (TypeError, ValueError):
                continue
            if self.start_year <= year <= cap:
                discovered.append(year)
        return sorted(set(discovered)) or list(self.get_year_range())

    def get_cache_path(self, cache_type: str) -> Path:
        path = self.cache_directory / cache_type
        path.mkdir(parents=True, exist_ok=True)
        return path

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["data_directory"] = str(self.data_directory)
        return payload

    def save(self, path: str | Path | None = None) -> Path:
        target = Path(path) if path else self.data_directory / "fleaflicker_context.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.to_dict(), indent=2, default=str), encoding="utf-8")
        return target

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FleaflickerContext:
        data = dict(payload)
        if data.get("data_directory"):
            data["data_directory"] = Path(data["data_directory"])
        return cls(**data)

    @classmethod
    def load(cls, path: str | Path) -> FleaflickerContext:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(payload)
