"""
Unified league settings loading for Yahoo, Sleeper, and ESPN.

This module provides a consistent interface for loading league settings
(scoring rules and roster settings) regardless of platform. It prefers the
canonical flat ``league_settings`` table from DuckDB/Fly and only falls
back to legacy JSON files when the canonical table is unavailable.

Usage:
    from multi_league.core.settings_loader import LeagueSettingsLoader

    loader = LeagueSettingsLoader(ctx.data_directory / "league_settings", ctx.league_id, db_name=ctx.database_name)
    scoring_rules, roster_settings = loader.load_all()

    roster = loader.load_roster_settings()
    scoring = loader.load_scoring_rules()
    params = loader.get_scoring_params()
"""

from pathlib import Path
from typing import Any
import json
import logging
import os

from multi_league.core.roster_settings import load_roster_settings_from_json

logger = logging.getLogger(__name__)

_SCORING_PREFIX = "scoring_"
_ROSTER_PREFIX = "roster_"
_IGNORED_ROSTER_POSITIONS = {"BN", "IR"}


class LeagueSettingsLoader:
    """Unified league settings loader backed by the flat league_settings table."""

    def __init__(self, settings_dir: Path, league_id: str | None = None, db_name: str | None = None):
        self.settings_dir = self._resolve_settings_dir(Path(settings_dir))
        self.league_id = league_id
        self.db_name = db_name
        self._scoring_cache: dict[int, dict] | None = None
        self._roster_cache: dict[int, dict[str, int]] | None = None
        self._scoring_params_cache: dict[str, Any] | None = None
        self._settings_rows_cache: list[dict[str, Any]] | None = None

    @staticmethod
    def _resolve_settings_dir(settings_dir: Path) -> Path:
        """Resolve the actual settings directory.

        Callers typically pass ctx.data_directory / "league_settings", but Sleeper
        stores league_settings_YEAR.json directly in the root data directory.
        If the provided path has no settings files, check the parent directory.
        """
        if settings_dir.exists() and settings_dir.is_dir():
            has_files = (
                any(settings_dir.glob("league_settings_*.json"))
                or any(settings_dir.glob("yahoo_roster_*.json"))
                or any(settings_dir.glob("settings_*.json"))
                or (settings_dir / "league_settings.parquet").exists()
            )
            if has_files:
                return settings_dir

        parent = settings_dir.parent
        if parent.exists() and parent.is_dir() and any(parent.glob("league_settings_*.json")):
            return parent

        return settings_dir

    def load_all(self) -> tuple[dict[int, dict], dict[int, dict[str, int]]]:
        return self.load_scoring_rules(), self.load_roster_settings()

    def load_scoring_rules(self) -> dict[int, dict]:
        """Load scoring rules from canonical league_settings rows first."""
        if self._scoring_cache is not None:
            return self._scoring_cache

        settings_rows = self._load_settings_rows()
        scoring_from_table = self._scoring_rules_from_settings_rows(settings_rows)
        if scoring_from_table:
            self._scoring_cache = scoring_from_table
            return self._scoring_cache

        try:
            from multi_league.transformations.player.modules.scoring_calculator import load_scoring_rules

            self._scoring_cache = load_scoring_rules(self.settings_dir) or {}
        except ImportError:
            try:
                from transformations.player.modules.scoring_calculator import load_scoring_rules

                self._scoring_cache = load_scoring_rules(self.settings_dir) or {}
            except ImportError:
                logger.warning("Could not import load_scoring_rules - settings may be unavailable")
                self._scoring_cache = {}
        except Exception as e:
            logger.warning(f"Error loading scoring rules: {e}")
            self._scoring_cache = {}

        return self._scoring_cache

    def load_roster_settings(self) -> dict[int, dict[str, int]]:
        """Load roster settings from canonical league_settings rows first."""
        if self._roster_cache is not None:
            return self._roster_cache

        settings_rows = self._load_settings_rows()
        roster_from_table = self._roster_settings_from_settings_rows(settings_rows)
        if roster_from_table:
            self._roster_cache = roster_from_table
            return self._roster_cache

        try:
            self._roster_cache = load_roster_settings_from_json(self.settings_dir, league_id=self.league_id) or {}
        except Exception as e:
            logger.warning(f"Error loading roster settings: {e}")
            self._roster_cache = {}

        return self._roster_cache

    def _load_settings_rows(self) -> list[dict[str, Any]]:
        """Load canonical league_settings rows once and reuse them."""
        if self._settings_rows_cache is not None:
            return self._settings_rows_cache

        settings_rows = self._load_settings_rows_from_local_duckdb()
        if not settings_rows:
            settings_rows = self._load_settings_rows_from_fly()

        self._settings_rows_cache = settings_rows
        return self._settings_rows_cache

    def _load_settings_rows_from_local_duckdb(self) -> list[dict[str, Any]]:
        """Read flat settings rows from the local league DuckDB when available."""
        try:
            import duckdb
        except ImportError:
            logger.warning("duckdb not available - cannot load settings from local DuckDB")
            return []

        for db_path in self._candidate_local_db_paths():
            if not db_path.exists():
                continue

            try:
                conn = duckdb.connect(str(db_path), read_only=True)
                rows = self._read_settings_rows_from_connection(conn)
                conn.close()
                if rows:
                    logger.info("Loaded canonical settings for %s years from local DuckDB: %s", len(rows), db_path)
                    return rows
            except Exception as exc:
                logger.debug("Could not read league_settings from %s: %s", db_path, exc)

        return []

    def _load_settings_rows_from_fly(self) -> list[dict[str, Any]]:
        """Read flat settings rows from Fly as the remote fallback."""
        if not self.db_name:
            return []

        try:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
            rows = reader.query(
                "SELECT * FROM public.league_settings ORDER BY year",
                database=self.db_name,
            )
            if rows:
                logger.info("Loaded canonical settings for %s years from Fly: %s", len(rows), self.db_name)
            return rows
        except Exception as e:
            logger.warning(f"Error loading settings from Fly: {e}")
            return []

    def _candidate_local_db_paths(self) -> list[Path]:
        """Build likely local DuckDB candidates from the settings directory."""
        candidates: list[Path] = []
        search_roots = [self.settings_dir, self.settings_dir.parent]

        if self.db_name:
            for root in search_roots:
                candidates.append(root / f"{self.db_name}.duckdb")

        for root in search_roots:
            if not root.exists():
                continue
            candidates.extend(sorted(root.glob("*.duckdb")))

        deduped: list[Path] = []
        seen: set[Path] = set()
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except OSError:
                resolved = candidate
            if resolved in seen:
                continue
            seen.add(resolved)
            deduped.append(resolved)
        return deduped

    def _read_settings_rows_from_connection(self, conn) -> list[dict[str, Any]]:
        """Read raw league_settings rows from a DuckDB connection."""
        table_check = conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = 'league_settings'
            """
        ).fetchone()[0]
        if not table_check:
            return []

        result = conn.execute("SELECT * FROM public.league_settings ORDER BY year")
        columns = [desc[0] for desc in (result.description or [])]
        return [dict(zip(columns, values)) for values in result.fetchall()]

    @staticmethod
    def _value_present(value: Any) -> bool:
        if value is None:
            return False
        try:
            return value == value
        except Exception:
            return True

    @classmethod
    def _extract_flat_scoring_settings(cls, row: dict[str, Any]) -> dict[str, float]:
        from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row

        return extract_scoring_settings_from_flat_row(row)

    @classmethod
    def _extract_flat_roster_settings(cls, row: dict[str, Any]) -> dict[str, int]:
        roster_settings: dict[str, int] = {}
        for key, value in row.items():
            if not key.startswith(_ROSTER_PREFIX) or not cls._value_present(value):
                continue
            position = key.removeprefix(_ROSTER_PREFIX)
            if position in _IGNORED_ROSTER_POSITIONS:
                continue
            count = int(value)
            if count > 0:
                roster_settings[position] = count
        return roster_settings

    @classmethod
    def _scoring_rules_from_settings_rows(cls, rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        rules_by_year: dict[int, dict[str, Any]] = {}
        for row in rows:
            year = row.get("year")
            if not cls._value_present(year):
                continue

            scoring_settings = cls._extract_flat_scoring_settings(row)
            if scoring_settings:
                rules_by_year[int(year)] = {"scoring_settings": scoring_settings}

        return rules_by_year

    @classmethod
    def _roster_settings_from_settings_rows(cls, rows: list[dict[str, Any]]) -> dict[int, dict[str, int]]:
        roster_by_year: dict[int, dict[str, int]] = {}
        for row in rows:
            year = row.get("year")
            if not cls._value_present(year):
                continue

            position_counts = cls._extract_flat_roster_settings(row)
            if position_counts:
                roster_by_year[int(year)] = position_counts

        return roster_by_year

    def get_scoring_variant(self) -> dict[str, str]:
        """Get the scoring column variant for this league."""
        try:
            from multi_league.transformations.player.modules.scoring_calculator import detect_scoring_variant

            return detect_scoring_variant(self.load_scoring_rules()) or {}
        except ImportError:
            try:
                from transformations.player.modules.scoring_calculator import detect_scoring_variant

                return detect_scoring_variant(self.load_scoring_rules()) or {}
            except ImportError:
                logger.warning("Could not import detect_scoring_variant")
                return {}
        except Exception as e:
            logger.warning(f"Error detecting scoring variant: {e}")
            return {}

    def get_scoring_params(self) -> dict[str, Any]:
        """Get numeric scoring parameters."""
        if self._scoring_params_cache is not None:
            return self._scoring_params_cache

        defaults = {"ppr": 0.0, "pass_td_pts": 4, "idp_scoring": "std"}

        try:
            from multi_league.transformations.player.modules.scoring_calculator import extract_scoring_params

            variant = self.get_scoring_variant()
            if variant:
                self._scoring_params_cache = extract_scoring_params(variant) or defaults
            else:
                self._scoring_params_cache = defaults
        except ImportError:
            try:
                from transformations.player.modules.scoring_calculator import extract_scoring_params

                variant = self.get_scoring_variant()
                if variant:
                    self._scoring_params_cache = extract_scoring_params(variant) or defaults
                else:
                    self._scoring_params_cache = defaults
            except ImportError:
                logger.warning("Could not import extract_scoring_params - using defaults")
                self._scoring_params_cache = defaults
        except Exception as e:
            logger.warning(f"Error extracting scoring params: {e}")
            self._scoring_params_cache = defaults

        return self._scoring_params_cache

    def clear_cache(self) -> None:
        self._scoring_cache = None
        self._roster_cache = None
        self._scoring_params_cache = None
        self._settings_rows_cache = None

    def __repr__(self) -> str:
        return (
            f"LeagueSettingsLoader(settings_dir={self.settings_dir}, "
            f"league_id={self.league_id}, db_name={self.db_name})"
        )


def load_settings_json_files(settings_dir: Path) -> dict[int, dict]:
    """Load all league_settings_*.json files from a directory."""
    result: dict[int, dict] = {}
    settings_dir = Path(settings_dir)
    if not settings_dir.exists():
        return result

    for file_path in sorted(settings_dir.glob("league_settings_*.json")):
        try:
            stem = file_path.stem.replace("league_settings_", "")
            year = int(stem.split("_")[0])
            with open(file_path, encoding="utf-8") as f:
                result[year] = json.load(f)
        except (ValueError, json.JSONDecodeError, OSError):
            continue

    return result
