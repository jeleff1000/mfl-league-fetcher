"""
Sleeper Context - Configuration for Sleeper league processing.

Parallel to LeagueContext but for Sleeper-specific fields.

Key differences from Yahoo LeagueContext:
- No OAuth (Sleeper is public API)
- league_id is numeric string (e.g., "917358392482123776")
- Uses username for discovery instead of OAuth
- Uses roster_id for team identification (not team_key)
"""

import json
import re
from dataclasses import dataclass, field, asdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from datetime import datetime

from multi_league.core.date_utils import get_current_nfl_season_year


YearFilter = int | str | Iterable[int | str] | None


def resolve_years_to_fetch(ctx: "SleeperContext", year_filter: YearFilter = None) -> list[int]:
    """Normalize a single-year, multi-year, or absent year filter."""
    if year_filter is None:
        return ctx.get_processing_years()

    raw_years: Iterable[int | str]
    if isinstance(year_filter, int | str):
        raw_years = [year_filter]
    else:
        raw_years = year_filter

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
class SleeperContext:
    """
    Configuration context for processing a Sleeper fantasy league.

    Designed to be parallel to LeagueContext for downstream compatibility.
    All directory structures and file patterns match Yahoo conventions.

    Usage:
        # Create new context
        ctx = SleeperContext(
            league_id="917358392482123776",
            league_name="My League",
            username="my_sleeper_username"
        )

        # Save to file
        ctx.save()

        # Load from file
        ctx = SleeperContext.load("leagues/my_league/sleeper_context.json")

        # Access directories (same structure as LeagueContext)
        player_data_path = ctx.player_data_directory
        matchup_data_path = ctx.matchup_data_directory
    """

    # === Required Fields ===
    league_id: str  # Sleeper league ID (numeric string)
    league_name: str  # Human-readable name
    username: str  # Sleeper username for discovery

    # === Optional League Metadata ===
    sport: str = "nfl"  # Sport type (nfl, nba, etc.)
    start_year: int = 2020  # First year of data to fetch
    end_year: int | None = None  # Last year (None = current year)
    num_teams: int | None = None  # Number of teams in league
    playoff_teams: int | None = None  # Number of playoff teams
    bye_teams: int | None = None  # Number of teams with first-round byes
    playoff_start_week: int | None = None  # Week playoffs begin
    regular_season_weeks: int | None = None  # Weeks in regular season

    # === Scoring Format ===
    uses_median_scoring: bool = False  # H2H + Median scoring (league_average_match)

    # === App Visibility ===
    is_private: bool = False  # Hide from public league discovery; direct links still work

    # === Data Storage ===
    data_directory: Path | None = None  # Root directory for this league's data

    # === Processing Configuration ===
    max_workers: int = 3  # Parallelism for data fetching
    enable_caching: bool = True  # Enable performance caching
    rate_limit_per_min: int = 1000  # API rate limit (requests/min)

    # === Manager Mappings (built from roster/user data) ===
    # Maps roster_id -> manager display name
    roster_to_manager: dict[int, str] = field(default_factory=dict)

    # Maps roster_id -> owner user_id (GUID equivalent)
    roster_to_guid: dict[int, str] = field(default_factory=dict)

    # === Manager Name Overrides ===
    manager_name_overrides: dict[str, str] = field(default_factory=dict)
    # Maps old manager names to new names

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

    # === League History (year -> league_id mapping) ===
    league_ids: dict[str, str] = field(default_factory=dict)
    # Maps year (as string) to Sleeper league_id for that season
    # Sleeper leagues can have different IDs each season

    # === User ID (cached from API) ===
    user_id: str | None = None  # Sleeper user_id for the username

    # === Metadata ===
    created_at: str | None = None  # ISO timestamp of context creation
    updated_at: str | None = None  # ISO timestamp of last update

    # === Platform Identifier ===
    platform: str = "sleeper"  # Always "sleeper" for this context

    # === LeagueContext Compatibility Fields ===
    # These fields ensure duck-typing compatibility with LeagueContext
    import_mode: str = "full"  # "quick" or "full" (matches LeagueContext)
    require_oauth: bool = False  # Always False (Sleeper uses public API)
    keeper_rules: dict[str, Any] | None = None  # Keeper configuration (if any)
    league_rules: dict[str, Any] | None = None  # League rules (sacko mode, etc.)
    standings_weights: dict[str, Any] | None = None  # Custom standings formula

    # === League Database Name ===
    database_name: str | None = None
    motherduck_db_name: str | None = None  # Deprecated alias kept for old saved contexts

    def __post_init__(self):
        """
        Initialize derived fields and validate configuration.

        - Resolves data_directory to absolute path
        - Creates necessary subdirectories
        - Sets created_at timestamp if new
        """
        # Set timestamps
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()

        # Resolve data directory
        if self.data_directory is None:
            # Default: ~/fantasy_football_data/sleeper_{league_name}/
            base_dir = Path.home() / "fantasy_football_data"
            safe_name = self._sanitize_name(self.league_name)
            self.data_directory = base_dir / f"sleeper_{safe_name}"
        else:
            self.data_directory = Path(self.data_directory).resolve()

        # Create directory structure
        self._create_directories()

        # Validate
        self._validate()

    def _sanitize_name(self, name: str) -> str:
        """
        Convert name to filesystem-safe format.

        Replaces spaces and special characters with underscores.
        """
        # Note: use global re import at top of file (Python 3.12+ scoping issue)
        safe = re.sub(r"[^\w\-]", "_", name.lower())
        # Remove consecutive underscores
        safe = re.sub(r"_+", "_", safe)
        return safe.strip("_")

    def _create_directories(self):
        """
        Create all necessary subdirectories for league data.

        Directory structure matches LeagueContext:
            {data_directory}/
                player_data/
                matchup_data/
                transaction_data/
                draft_data/
                schedule_data/
                logs/
                cache/
        """
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
        """
        Validate required fields and configuration.

        Raises:
            ValueError: If required fields are missing or invalid
        """
        if not self.league_id:
            raise ValueError("league_id is required")

        if not self.league_name:
            raise ValueError("league_name is required")

        # Note: username is optional when league_id is provided directly.
        # It's only needed for discovery flows (finding leagues by username).
        # Import flows work directly with league_id.

        # Validate year range (allow current NFL season + 1 for planning ahead)
        if self.start_year < 2017 or self.start_year > get_current_nfl_season_year() + 1:
            raise ValueError(f"Invalid start_year: {self.start_year} (Sleeper started in 2017)")

        if self.end_year is not None:
            if self.end_year < self.start_year:
                raise ValueError(f"end_year ({self.end_year}) cannot be before start_year ({self.start_year})")

    # === Directory Properties (match LeagueContext) ===

    @property
    def player_data_directory(self) -> Path:
        """Directory for player stats data."""
        return self.data_directory / "player_data"

    @property
    def matchup_data_directory(self) -> Path:
        """Directory for matchup data."""
        return self.data_directory / "matchup_data"

    @property
    def transaction_data_directory(self) -> Path:
        """Directory for transaction data."""
        return self.data_directory / "transaction_data"

    @property
    def draft_data_directory(self) -> Path:
        """Directory for draft data."""
        return self.data_directory / "draft_data"

    @property
    def schedule_data_directory(self) -> Path:
        """Directory for schedule data."""
        return self.data_directory / "schedule_data"

    @property
    def logs_directory(self) -> Path:
        """Directory for run logs."""
        return self.data_directory / "logs"

    @property
    def cache_directory(self) -> Path:
        """Directory for performance caching (including player database)."""
        return self.data_directory / "cache"

    # === Canonical File Paths (match LeagueContext) ===

    @property
    def canonical_player_file(self) -> Path:
        """Path to canonical player_fantasy.parquet file."""
        return self.data_directory / "player_fantasy.parquet"

    @property
    def canonical_matchup_file(self) -> Path:
        """Path to canonical matchup.parquet file."""
        return self.data_directory / "matchup.parquet"

    @property
    def canonical_transaction_file(self) -> Path:
        """Path to canonical transactions.parquet file."""
        return self.data_directory / "transactions.parquet"

    @property
    def canonical_draft_file(self) -> Path:
        """Path to canonical draft.parquet file."""
        return self.data_directory / "draft.parquet"

    @property
    def canonical_traded_picks_file(self) -> Path:
        """Path to canonical traded_picks.parquet file."""
        return self.data_directory / "traded_picks.parquet"

    # === LeagueContext Compatibility Properties ===

    @property
    def is_single_year_import(self) -> bool:
        """
        True if this context is configured for a single-year (quick) import.

        Matches LeagueContext behavior for transformation compatibility.
        """
        if self.import_mode == "quick":
            return True
        effective_end = self.end_year or get_current_nfl_season_year()
        return self.start_year == effective_end

    @property
    def keepers_enabled(self) -> bool:
        """Check if keeper rules are enabled for this league."""
        if self.keeper_rules is None:
            return False
        return self.keeper_rules.get("enabled", False)

    @property
    def max_keepers(self) -> int:
        """Get maximum number of keepers allowed (default 0 if not configured)."""
        if self.keeper_rules is None:
            return 0
        return self.keeper_rules.get("max_keepers", 0)

    @property
    def keeper_budget(self) -> int:
        """Get auction budget for keeper calculations (default 200)."""
        if self.keeper_rules is None:
            return 200
        return self.keeper_rules.get("budget", 200)

    @property
    def sacko_mode(self) -> str:
        """
        Get sacko determination mode.

        Returns:
            'consolation_bracket' (default) - Sacko is loser of sacko bowl game
            'regular_season_last' - Sacko is always worst regular season record
        """
        if self.league_rules is None:
            return "consolation_bracket"
        return self.league_rules.get("sacko_mode", "consolation_bracket")

    @property
    def use_regular_season_sacko(self) -> bool:
        """
        Check if sacko should be determined by regular season standings.

        Returns:
            True if sacko is last place regular season, False for consolation bracket
        """
        return self.sacko_mode == "regular_season_last"

    # === League ID Helpers ===

    def get_league_id_for_year(self, year: int) -> str | None:
        """
        Get the specific league_id for a given year.

        Args:
            year: The season year (e.g., 2023)

        Returns:
            The Sleeper league_id for that year, or None if not found
        """
        year_str = str(year)
        if year_str in self.league_ids:
            return self.league_ids[year_str]

        # Fallback to current league_id only if year matches current
        current_year = get_current_nfl_season_year()
        end = self.end_year if self.end_year else current_year
        if year == end:
            return self.league_id

        return None

    def has_league_ids_mapping(self) -> bool:
        """Check if league_ids mapping is populated."""
        return bool(self.league_ids)

    # === Manager Mapping Helpers ===

    def get_manager_name(self, roster_id: int) -> str:
        """
        Get manager display name for a roster_id.

        Args:
            roster_id: Sleeper roster_id

        Returns:
            Manager name, or "Unknown" if not found
        """
        name = self.roster_to_manager.get(roster_id, "Unknown")

        # Apply overrides
        if name in self.manager_name_overrides:
            return self.manager_name_overrides[name]

        return name

    def get_manager_guid(self, roster_id: int) -> str:
        """
        Get manager GUID (user_id) for a roster_id.

        Args:
            roster_id: Sleeper roster_id

        Returns:
            User ID, or empty string if not found
        """
        return self.roster_to_guid.get(roster_id, "")

    def build_roster_mappings(self, rosters: list[dict], users: list[dict]):
        """
        Build roster_id -> manager mappings from API responses.

        Args:
            rosters: Response from client.get_league_rosters()
            users: Response from client.get_league_users()
        """
        # Build user_id -> display_name mapping
        user_names = {}
        for user in users:
            user_id = user.get("user_id")
            display_name = user.get("display_name") or user.get("username", "Unknown")
            if user_id:
                user_names[user_id] = display_name

        # Build roster mappings
        self.roster_to_manager = {}
        self.roster_to_guid = {}

        for roster in rosters:
            roster_id = roster.get("roster_id")
            owner_id = roster.get("owner_id")

            if roster_id is not None and owner_id:
                self.roster_to_guid[roster_id] = owner_id
                self.roster_to_manager[roster_id] = user_names.get(owner_id, "Unknown")

    # === Utility Methods ===

    def get_year_range(self) -> range:
        """
        Get range of years to process.

        Returns:
            range object from start_year to end_year (inclusive)
        """
        end = self.end_year if self.end_year else get_current_nfl_season_year()
        return range(self.start_year, end + 1)

    def get_processing_years(self, season_cap: int | None = None) -> list[int]:
        """
        Get concrete season years the pipeline should process.

        Prefer discovered ``league_ids`` when available so worker imports are driven
        by the actual Sleeper history chain rather than a stale payload
        ``start_year``/``end_year`` window.

        Args:
            season_cap: Optional upper bound for included seasons. Defaults to the
                current NFL season year.

        Returns:
            Sorted list of years to process.
        """
        cap = season_cap if season_cap is not None else get_current_nfl_season_year()

        discovered_years: set[int] = set()
        for year_key in self.league_ids or {}:
            try:
                year = int(year_key)
            except (TypeError, ValueError):
                continue
            if 2017 <= year <= cap:
                discovered_years.add(year)

        if discovered_years:
            return sorted(discovered_years)

        return list(self.get_year_range())

    def get_cache_path(self, cache_type: str) -> Path:
        """
        Get cache directory for specific cache type.

        Args:
            cache_type: Type of cache (e.g., 'player_cache', 'api_cache')

        Returns:
            Path to cache directory
        """
        cache_path = self.cache_directory / cache_type
        cache_path.mkdir(parents=True, exist_ok=True)
        return cache_path

    # === Serialization ===

    def to_dict(self) -> dict[str, Any]:
        """
        Convert context to dictionary for serialization.

        Converts Path objects to strings for JSON compatibility.
        """
        data = asdict(self)

        # Convert Path to string
        if self.data_directory:
            data["data_directory"] = str(self.data_directory)

        # Convert int keys to string for JSON (roster mappings)
        data["roster_to_manager"] = {str(k): v for k, v in self.roster_to_manager.items()}
        data["roster_to_guid"] = {str(k): v for k, v in self.roster_to_guid.items()}

        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SleeperContext":
        """
        Create SleeperContext from dictionary.

        Args:
            data: Dictionary with context fields

        Returns:
            SleeperContext instance
        """
        # Convert data_directory string to Path if present
        if "data_directory" in data and data["data_directory"]:
            data["data_directory"] = Path(data["data_directory"])

        # Convert string keys back to int for roster mappings
        if "roster_to_manager" in data:
            data["roster_to_manager"] = {int(k): v for k, v in data["roster_to_manager"].items()}
        if "roster_to_guid" in data:
            data["roster_to_guid"] = {int(k): v for k, v in data["roster_to_guid"].items()}

        # Get valid field names from the dataclass
        import dataclasses

        valid_fields = {f.name for f in dataclasses.fields(cls)}

        # Filter out unknown keys
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}

        return cls(**filtered_data)

    def save(self, path: Path | None = None):
        """
        Save context to JSON file.

        Args:
            path: Path to save file. If None, saves to {data_directory}/sleeper_context.json
        """
        if path is None:
            path = self.data_directory / "sleeper_context.json"
        else:
            path = Path(path)

        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)

        # Update timestamp
        self.updated_at = datetime.now().isoformat()

        # Write JSON
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, path: Path) -> "SleeperContext":
        """
        Load context from JSON file.

        Args:
            path: Path to sleeper_context.json file

        Returns:
            SleeperContext instance

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Sleeper context file not found: {path}")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"SleeperContext("
            f"league_id='{self.league_id}', "
            f"league_name='{self.league_name}', "
            f"username='{self.username}')"
        )

    def summary(self) -> str:
        """
        Get human-readable summary of context.

        Returns:
            Multi-line string with key configuration details
        """
        lines = [
            f"League: {self.league_name} ({self.league_id})",
            "Platform: Sleeper",
            f"Username: {self.username}",
            f"Data Directory: {self.data_directory}",
            f"Year Range: {self.start_year}-{self.end_year or 'current'}",
            f"Teams: {self.num_teams or 'unknown'}",
            f"Rate Limit: {self.rate_limit_per_min} req/min",
            f"Max Workers: {self.max_workers}",
            f"Caching: {'enabled' if self.enable_caching else 'disabled'}",
        ]

        if self.roster_to_manager:
            lines.append(f"Roster Mappings: {len(self.roster_to_manager)} teams")

        if self.manager_name_overrides:
            lines.append(f"Manager Overrides: {len(self.manager_name_overrides)}")

        lines.append(f"Created: {self.created_at}")
        lines.append(f"Updated: {self.updated_at}")

        return "\n".join(lines)


# === Helper Functions ===


def season_has_matchup_data(client: "SleeperAPIClient", league_id: str) -> bool:
    """
    Check if a season has any matchup data (at least 1 week with matchups).

    This is used to detect "empty" future seasons that Sleeper creates
    but haven't started yet (e.g., 2026 league created in Jan 2025).

    Uses a lenient check: if the API returns ANY matchup entries for week 1,
    the season is considered active. The previous strict check (requiring
    points > 0 or starters set) incorrectly skipped completed historical
    seasons where Sleeper returns matchup entries with points=0 or points=null.

    Args:
        client: SleeperAPIClient instance
        league_id: League ID for the specific season

    Returns:
        True if the season has at least 1 week of matchup data
    """
    try:
        # Check week 1 - if it has any matchup entries, the season has data
        matchups = client.get_league_matchups(league_id, week=1)
        if matchups and len(matchups) > 0:
            # Any matchup entry (even with points=0) means the season existed
            # and had real matchups. Empty future seasons return [] or None.
            return True
        return False
    except Exception as e:
        # API error — assume season exists to avoid silently dropping history.
        # Better to fetch empty data than to lose an entire year.
        print(f"  [HISTORY] Error checking season {league_id}, assuming it has data: {e}")
        return True


def discover_league_history(
    client: "SleeperAPIClient", league_id: str, skip_empty_seasons: bool = True
) -> dict[str, str]:
    """
    Discover full league history by following previous_league_id chain.

    Args:
        client: SleeperAPIClient instance
        league_id: Current league ID
        skip_empty_seasons: If True, exclude seasons with no matchup data
                           (e.g., 2026 league shell created but season not started)

    Returns:
        Dict mapping year (str) to league_id for all seasons

    Example:
        client = SleeperAPIClient()
        league_ids = discover_league_history(client, '1257088277819691008')
        # Returns: {'2024': '1131974495503253504', '2025': '1257088277819691008'}
    """
    league_ids = {}
    current_id = str(league_id)
    seen_ids: set[str] = set()
    newer_season: int | None = None

    while current_id:
        if current_id in seen_ids:
            raise ValueError("Sleeper renewal chain contains a cycle")
        seen_ids.add(current_id)
        league = client.get_league(current_id)
        if not isinstance(league, dict) or str(league.get("league_id") or "") != current_id:
            raise ValueError(f"Sleeper renewal chain identity is missing or mismatched: {current_id}")

        season = league.get("season")
        if not str(season or "").isdigit():
            raise ValueError(f"Sleeper renewal chain omitted a valid season: {current_id}")
        if newer_season is not None and int(season) >= newer_season:
            raise ValueError("Sleeper renewal chain seasons are not strictly decreasing")
        newer_season = int(season)
        if season:
            # Check if season has actual data (skip empty future seasons)
            if skip_empty_seasons:
                if season_has_matchup_data(client, current_id):
                    league_ids[str(season)] = current_id
                else:
                    # Print to stdout so it shows in import logs
                    print(f"  [HISTORY] Skipping empty season {season} (league_id={current_id}) - no matchup data yet")
            else:
                league_ids[str(season)] = current_id

        # Follow the chain backwards
        previous_id = league.get("previous_league_id")
        current_id = str(previous_id) if previous_id else ""

    return league_ids


def create_sleeper_context(
    league_id: str,
    league_name: str,
    username: str,
    start_year: int = 2020,
    data_directory: Path | None = None,
    **kwargs,
) -> SleeperContext:
    """
    Factory function to create and save a new SleeperContext.

    Args:
        league_id: Sleeper league_id
        league_name: Human-readable league name
        username: Sleeper username for API discovery
        start_year: First year to process
        data_directory: Custom data directory (optional)
        **kwargs: Additional context parameters

    Returns:
        SleeperContext instance (also saved to disk)
    """
    ctx = SleeperContext(
        league_id=league_id,
        league_name=league_name,
        username=username,
        start_year=start_year,
        data_directory=data_directory,
        **kwargs,
    )

    # Save to default location
    ctx.save()

    return ctx


def load_sleeper_context(path: Path) -> SleeperContext:
    """
    Load Sleeper context from JSON file.

    Args:
        path: Path to sleeper_context.json

    Returns:
        SleeperContext instance
    """
    return SleeperContext.load(path)
