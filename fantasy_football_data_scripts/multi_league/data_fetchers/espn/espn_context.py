"""
ESPN Context - Configuration for ESPN league processing.

Parallel to SleeperContext but for ESPN-specific fields.

Key differences from SleeperContext:
- Auth via cookies (espn_s2, swid) for private leagues
- No username-based discovery (ESPN doesn't support this)
- league_id is numeric int (e.g., 71580)
- Uses team_id for team identification (1-12, stable across years)
- Manager GUID from ESPN owner 'id' field (stable across years)
- start_year default is 2012 (ESPN data goes back further than Sleeper)
"""

import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any
from datetime import datetime

from multi_league.core.date_utils import get_current_nfl_season_year


@dataclass
class ESPNContext:
    """
    Configuration context for processing an ESPN fantasy league.

    Designed to be parallel to SleeperContext/LeagueContext for downstream compatibility.
    All directory structures and file patterns match Yahoo/Sleeper conventions.

    Usage:
        # Create new context
        ctx = ESPNContext(
            league_id=71580,
            league_name="TFL of extraordinary gentleman",
        )

        # Save to file
        ctx.save()

        # Load from file
        ctx = ESPNContext.load("leagues/my_league/espn_context.json")

        # Access directories (same structure as SleeperContext)
        player_data_path = ctx.player_data_directory
        matchup_data_path = ctx.matchup_data_directory
    """

    # === Required Fields ===
    league_id: int  # ESPN league ID (numeric)
    league_name: str  # Human-readable name

    # === Auth Fields (for private leagues) ===
    espn_s2: str | None = None  # ESPN auth cookie
    swid: str | None = None  # ESPN SWID cookie

    # === Optional League Metadata ===
    sport: str = "nfl"  # Sport type
    start_year: int = 2012  # First year of data to fetch
    end_year: int | None = None  # Last year (None = current year)
    num_teams: int | None = None  # Number of teams in league
    playoff_teams: int | None = None  # Number of playoff teams
    bye_teams: int | None = None  # Number of teams with first-round byes
    playoff_start_week: int | None = None  # Week playoffs begin
    regular_season_weeks: int | None = None  # Weeks in regular season

    # === Scoring Format ===
    uses_median_scoring: bool = False  # H2H + Median scoring

    # === App Visibility ===
    is_private: bool = False  # Hide from public league discovery; direct links still work

    # === Data Storage ===
    data_directory: Path | None = None  # Root directory for this league's data

    # === Processing Configuration ===
    max_workers: int = 3  # Parallelism for data fetching
    enable_caching: bool = True  # Enable performance caching

    # === Manager Mappings (built from ESPN team/owner data) ===
    # Maps team_id (int) -> manager display name (latest year, used as fallback)
    team_to_manager: dict[int, str] = field(default_factory=dict)

    # Maps team_id (int) -> owner GUID (ESPN owner 'id') (latest year, used as fallback)
    team_to_guid: dict[int, str] = field(default_factory=dict)

    # Maps year (str) -> {team_id (int) -> team_name (str)}
    # Team names change yearly so we need per-year mapping
    team_to_team_name: dict[str, dict[int, str]] = field(default_factory=dict)

    # Per-year manager/GUID mappings for historical accuracy
    # Maps year (str) -> {team_id (int) -> manager display name}
    team_to_manager_by_year: dict[str, dict[int, str]] = field(default_factory=dict)
    # Maps year (str) -> {team_id (int) -> owner GUID}
    team_to_guid_by_year: dict[str, dict[int, str]] = field(default_factory=dict)

    # === League History (year -> league_id mapping for combined leagues) ===
    # Maps year (as string) to ESPN league_id for that season
    # e.g., {"2014": 71580, "2015": 71580, "2024": 483401, "2025": 483401}
    # When empty, all years use self.league_id
    league_ids: dict[str, int] = field(default_factory=dict)

    # === Manager Name Overrides ===
    manager_name_overrides: dict[str, str] = field(default_factory=dict)

    franchise_merges: list[dict[str, Any]] = field(default_factory=list)
    # Durable owner/GUID merge payloads consumed by FranchiseRegistry.

    # === External Data Ingestion (v1) ===
    external_column_maps: list[dict] = field(default_factory=list)
    # Per-source-file column-name maps. Loaded from external_source_config.json during scan.

    external_identity_maps: dict[str, dict] = field(default_factory=dict)
    # External-manager-string → identity decision dict. Loaded from franchise_config.json.

    has_external_data: bool = False
    # True when staging contains intentional cross-platform or external source data.

    merge_source: dict[str, Any] | None = None
    # Legacy single in-system source league copied after upload for merge imports.

    merge_sources: list[dict[str, Any]] = field(default_factory=list)
    # Multiple in-system source leagues copied after upload for multi-platform imports.

    # === Metadata ===
    created_at: str | None = None  # ISO timestamp of context creation
    updated_at: str | None = None  # ISO timestamp of last update

    # === Platform Identifier ===
    platform: str = "espn"  # Always "espn" for this context

    # === LeagueContext Compatibility Fields ===
    import_mode: str = "full"  # "quick" or "full"
    quick_import_years: list[int] = field(default_factory=list)
    explicit_import_years: list[int] = field(default_factory=list)
    require_oauth: bool = False  # ESPN uses cookies, not OAuth
    keeper_rules: dict[str, Any] | None = None
    league_rules: dict[str, Any] | None = None
    standings_weights: dict[str, Any] | None = None

    # === League Database Name ===
    database_name: str | None = None
    motherduck_db_name: str | None = None  # Deprecated alias kept for old saved contexts

    def __post_init__(self):
        """Initialize derived fields and validate configuration."""
        # Set timestamps
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()

        # Resolve data directory
        if self.data_directory is None:
            base_dir = Path.home() / "fantasy_football_data"
            safe_name = self._sanitize_name(self.league_name)
            self.data_directory = base_dir / f"espn_{safe_name}"
        else:
            self.data_directory = Path(self.data_directory).resolve()

        # Create directory structure
        self._create_directories()

        # Validate
        self._validate()

    def _sanitize_name(self, name: str) -> str:
        """Convert name to filesystem-safe format."""
        safe = re.sub(r"[^\w\-]", "_", name.lower())
        safe = re.sub(r"_+", "_", safe)
        return safe.strip("_")

    def _create_directories(self):
        """Create all necessary subdirectories for league data."""
        directories = [
            self.data_directory,
            self.data_directory / "league_settings",
            self.player_data_directory,
            self.matchup_data_directory,
            self.transaction_data_directory,
            self.draft_data_directory,
            self.schedule_data_directory,
            self.logs_directory,
            self.cache_directory,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def _validate(self):
        """Validate required fields and configuration."""
        if not self.league_id:
            raise ValueError("league_id is required")

        if not self.league_name:
            raise ValueError("league_name is required")

        # ESPN data can go back to ~2004, but our pipeline supports 2004+
        if self.start_year < 2004 or self.start_year > get_current_nfl_season_year() + 1:
            raise ValueError(f"Invalid start_year: {self.start_year} (ESPN supported range: 2004-present)")

        if self.end_year is not None:
            if self.end_year < self.start_year:
                raise ValueError(f"end_year ({self.end_year}) cannot be before start_year ({self.start_year})")

    # === Directory Properties (match SleeperContext/LeagueContext) ===

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

    # === Canonical File Paths (match SleeperContext) ===

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

    # === LeagueContext Compatibility Properties ===

    @property
    def is_single_year_import(self) -> bool:
        if self.import_mode == "quick":
            return True
        effective_end = self.end_year or get_current_nfl_season_year()
        return self.start_year == effective_end

    @property
    def keepers_enabled(self) -> bool:
        if self.keeper_rules is None:
            return False
        return self.keeper_rules.get("enabled", False)

    @property
    def max_keepers(self) -> int:
        if self.keeper_rules is None:
            return 0
        return self.keeper_rules.get("max_keepers", 0)

    @property
    def keeper_budget(self) -> int:
        if self.keeper_rules is None:
            return 200
        return self.keeper_rules.get("budget", 200)

    @property
    def sacko_mode(self) -> str:
        if self.league_rules is None:
            return "consolation_bracket"
        return self.league_rules.get("sacko_mode", "consolation_bracket")

    @property
    def use_regular_season_sacko(self) -> bool:
        return self.sacko_mode == "regular_season_last"

    # === Manager Mapping Helpers ===

    def get_manager_name(self, team_id: int, team_name: str = "", year: int = 0) -> str:
        """Get manager display name for a team_id.

        Uses per-year mapping when available (handles ownership changes),
        falls back to latest-year flat mapping for backward compatibility.
        Checks overrides by both manager name and team name.
        """
        # Try per-year lookup first (handles historical ownership changes)
        if year and str(year) in self.team_to_manager_by_year:
            name = self.team_to_manager_by_year[str(year)].get(team_id)
            if name is not None:
                # Check overrides
                if name in self.manager_name_overrides:
                    return self.manager_name_overrides[name]
                if team_name and team_name in self.manager_name_overrides:
                    return self.manager_name_overrides[team_name]
                if not team_name and year:
                    looked_up = self.get_team_name(team_id, year)
                    if looked_up in self.manager_name_overrides:
                        return self.manager_name_overrides[looked_up]
                return name

        # Fallback to flat mapping (latest year / legacy contexts)
        name = self.team_to_manager.get(team_id, "Unknown")
        # Check override by manager name
        if name in self.manager_name_overrides:
            return self.manager_name_overrides[name]
        # Check override by team name (unknown manager UI keys by team_name)
        if team_name and team_name in self.manager_name_overrides:
            return self.manager_name_overrides[team_name]
        # Try looking up team name from context if not provided
        if not team_name and year:
            looked_up = self.get_team_name(team_id, year)
            if looked_up in self.manager_name_overrides:
                return self.manager_name_overrides[looked_up]
        return name

    def get_manager_guid(self, team_id: int, year: int = 0) -> str:
        """Get manager GUID (ESPN owner id) for a team_id.

        Uses per-year mapping when available, falls back to flat mapping.
        """
        if year and str(year) in self.team_to_guid_by_year:
            guid = self.team_to_guid_by_year[str(year)].get(team_id)
            if guid is not None:
                return guid
        return self.team_to_guid.get(team_id, "")

    def get_team_name(self, team_id: int, year: int) -> str:
        """Get team name for a team_id in a specific year."""
        year_str = str(year)
        if year_str in self.team_to_team_name:
            return self.team_to_team_name[year_str].get(team_id, f"Team {team_id}")
        return f"Team {team_id}"

    def get_franchise_id(self, team_id: int, year: int = 0) -> str:
        """
        Get franchise_id for a team_id.

        Uses {owner_guid}_{team_index} pattern matching Yahoo/Sleeper.
        Falls back to espn_{team_id} if no GUID available.
        Uses per-year GUID mapping when available for historical accuracy.
        """
        # Get the GUID map for this year (or fall back to flat)
        if year and str(year) in self.team_to_guid_by_year:
            guid_map = self.team_to_guid_by_year[str(year)]
        else:
            guid_map = self.team_to_guid

        guid = guid_map.get(team_id, "")
        if guid:
            # Count how many teams this owner has (for multi-team owners)
            owner_teams = [tid for tid, g in guid_map.items() if g == guid]
            team_index = sorted(owner_teams).index(team_id)
            return f"{guid}_{team_index}"
        return f"espn_{team_id}"

    # === Utility Methods ===

    def get_year_range(self) -> range | list[int]:
        """Get range of years to process.

        Caps end year to the latest NFL season that could have real data,
        based on calendar date (Sept-Dec = current year, Jan-Aug = previous year).
        This prevents Sleeper API returning a future season year (e.g. 2026
        during the 2025 offseason) from causing ESPN to fetch non-existent data.
        """
        if self.import_mode == "quick" and self.quick_import_years:
            return sorted({int(year) for year in self.quick_import_years})

        now = datetime.now()
        latest_real_season = now.year if now.month >= 9 else now.year - 1
        if self.explicit_import_years:
            # Explicit years are an import request, not a discovery result.
            # Preserve the current preseason shell so draft/settings data can
            # be imported before scoring begins, just like quick imports.
            return sorted({int(year) for year in self.explicit_import_years})

        end = self.end_year if self.end_year else get_current_nfl_season_year()
        # An explicitly bounded import may target the current preseason shell
        # for draft/settings data. Only auto-discovered ranges use the calendar
        # cap that prevents accidental future-season fetches.
        if self.end_year is None:
            end = min(end, latest_real_season)
        return range(self.start_year, end + 1)

    def get_cache_path(self, cache_type: str) -> Path:
        cache_path = self.cache_directory / cache_type
        cache_path.mkdir(parents=True, exist_ok=True)
        return cache_path

    def get_league_id_for_year(self, year: int) -> int:
        """
        Get the ESPN league_id for a specific year.

        For combined leagues, different years may map to different league IDs.
        Falls back to self.league_id if no mapping exists.
        """
        if self.league_ids:
            return self.league_ids.get(str(year), self.league_id)
        return self.league_id

    # === Serialization ===

    def to_dict(self) -> dict[str, Any]:
        """Convert context to dictionary for serialization."""
        data = asdict(self)

        # Convert Path to string
        if self.data_directory:
            data["data_directory"] = str(self.data_directory)

        # Convert int keys to string for JSON
        data["team_to_manager"] = {str(k): v for k, v in self.team_to_manager.items()}
        data["team_to_guid"] = {str(k): v for k, v in self.team_to_guid.items()}

        # league_ids: ensure string keys
        if self.league_ids:
            data["league_ids"] = {str(k): v for k, v in self.league_ids.items()}

        # Nested dict: year -> {team_id -> team_name}
        serialized_names = {}
        for year_str, team_map in self.team_to_team_name.items():
            serialized_names[str(year_str)] = {str(k): v for k, v in team_map.items()}
        data["team_to_team_name"] = serialized_names

        # Per-year manager/GUID mappings
        serialized_mgr_by_year = {}
        for year_str, team_map in self.team_to_manager_by_year.items():
            serialized_mgr_by_year[str(year_str)] = {str(k): v for k, v in team_map.items()}
        data["team_to_manager_by_year"] = serialized_mgr_by_year

        serialized_guid_by_year = {}
        for year_str, team_map in self.team_to_guid_by_year.items():
            serialized_guid_by_year[str(year_str)] = {str(k): v for k, v in team_map.items()}
        data["team_to_guid_by_year"] = serialized_guid_by_year

        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ESPNContext":
        """Create ESPNContext from dictionary."""
        # Convert data_directory string to Path
        if "data_directory" in data and data["data_directory"]:
            data["data_directory"] = Path(data["data_directory"])

        # Convert string keys back to int for team mappings
        if "team_to_manager" in data:
            data["team_to_manager"] = {int(k): v for k, v in data["team_to_manager"].items()}
        if "team_to_guid" in data:
            data["team_to_guid"] = {int(k): v for k, v in data["team_to_guid"].items()}

        # Nested dict: year -> {team_id -> team_name}
        if "team_to_team_name" in data:
            restored = {}
            for year_str, team_map in data["team_to_team_name"].items():
                restored[str(year_str)] = {int(k): v for k, v in team_map.items()}
            data["team_to_team_name"] = restored

        # Per-year manager/GUID mappings
        if "team_to_manager_by_year" in data:
            restored = {}
            for year_str, team_map in data["team_to_manager_by_year"].items():
                restored[str(year_str)] = {int(k): v for k, v in team_map.items()}
            data["team_to_manager_by_year"] = restored

        if "team_to_guid_by_year" in data:
            restored = {}
            for year_str, team_map in data["team_to_guid_by_year"].items():
                restored[str(year_str)] = {int(k): v for k, v in team_map.items()}
            data["team_to_guid_by_year"] = restored

        # league_ids: ensure string keys, int values
        if "league_ids" in data and data["league_ids"]:
            data["league_ids"] = {str(k): int(v) for k, v in data["league_ids"].items()}
        if "explicit_import_years" in data and data["explicit_import_years"]:
            data["explicit_import_years"] = sorted({int(y) for y in data["explicit_import_years"]})

        # Filter to valid fields
        import dataclasses

        valid_fields = {f.name for f in dataclasses.fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}

        return cls(**filtered_data)

    def save(self, path: Path | None = None):
        """Save context to JSON file."""
        if path is None:
            path = self.data_directory / "espn_context.json"
        else:
            path = Path(path)

        path.parent.mkdir(parents=True, exist_ok=True)
        self.updated_at = datetime.now().isoformat()

        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, path: Path) -> "ESPNContext":
        """Load context from JSON file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"ESPN context file not found: {path}")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def __repr__(self) -> str:
        return f"ESPNContext(" f"league_id={self.league_id}, " f"league_name='{self.league_name}')"

    def summary(self) -> str:
        """Get human-readable summary of context."""
        lines = [
            f"League: {self.league_name} ({self.league_id})",
            "Platform: ESPN",
            f"Data Directory: {self.data_directory}",
            f"Year Range: {self.start_year}-{self.end_year or 'current'}",
            f"Teams: {self.num_teams or 'unknown'}",
            f"Auth: {'configured' if self.espn_s2 else 'public'}",
        ]

        if self.team_to_manager:
            lines.append(f"Manager Mappings: {len(self.team_to_manager)} teams")

        if self.manager_name_overrides:
            lines.append(f"Manager Overrides: {len(self.manager_name_overrides)}")

        lines.append(f"Created: {self.created_at}")
        lines.append(f"Updated: {self.updated_at}")

        return "\n".join(lines)


# === Helper Functions ===


def build_manager_names(teams: list) -> dict[int, str]:
    """
    Build manager display names from ESPN team owner data.

    Implements the first-name/last-initial/full-last dedup logic:
    1. Start with first name only
    2. If duplicate, add last initial
    3. If still duplicate, use full last name

    Args:
        teams: List of ESPN team objects with owners attribute.
               Each owner has: firstName, lastName, id

    Returns:
        Dict mapping team_id -> display name
    """
    # Collect raw names: team_id -> (first_name, last_name)
    raw_names = {}
    for team in teams:
        team_id = team.team_id if hasattr(team, "team_id") else team.get("team_id", 0)
        owners = team.owners if hasattr(team, "owners") else team.get("owners", [])

        if not owners:
            raw_names[team_id] = ("Unknown", "")
            continue

        # Use primary owner (first in list)
        owner = owners[0] if isinstance(owners, list) else owners
        if isinstance(owner, dict):
            first = (owner.get("firstName", "Unknown") or "Unknown").strip()
            last = (owner.get("lastName", "") or "").strip()
        else:
            first = (getattr(owner, "firstName", "Unknown") or "Unknown").strip()
            last = (getattr(owner, "lastName", "") or "").strip()

        raw_names[team_id] = (first, last)

    # Phase 1: Start with first names
    display_names = {tid: first for tid, (first, _) in raw_names.items()}

    # Phase 2: Find duplicates and add last initial
    name_counts = {}
    for tid, name in display_names.items():
        name_lower = name.lower()
        name_counts.setdefault(name_lower, []).append(tid)

    for name_lower, team_ids in name_counts.items():
        if len(team_ids) > 1:
            # Duplicate first names - add last initial
            for tid in team_ids:
                first, last = raw_names[tid]
                if last:
                    display_names[tid] = f"{first} {last[0]}."

    # Phase 3: Check for remaining duplicates, use full last name
    name_counts2 = {}
    for tid, name in display_names.items():
        name_lower = name.lower()
        name_counts2.setdefault(name_lower, []).append(tid)

    for name_lower, team_ids in name_counts2.items():
        if len(team_ids) > 1:
            for tid in team_ids:
                first, last = raw_names[tid]
                display_names[tid] = f"{first} {last}" if last else first

    return display_names


def create_espn_context(
    league_id: int, league_name: str, start_year: int = 2012, data_directory: Path | None = None, **kwargs
) -> ESPNContext:
    """
    Factory function to create and save a new ESPNContext.

    Args:
        league_id: ESPN league_id (numeric)
        league_name: Human-readable league name
        start_year: First year to process
        data_directory: Custom data directory (optional)
        **kwargs: Additional context parameters

    Returns:
        ESPNContext instance (also saved to disk)
    """
    ctx = ESPNContext(
        league_id=league_id, league_name=league_name, start_year=start_year, data_directory=data_directory, **kwargs
    )

    ctx.save()
    return ctx


def load_espn_context(path: Path) -> ESPNContext:
    """Load ESPN context from JSON file."""
    return ESPNContext.load(path)
