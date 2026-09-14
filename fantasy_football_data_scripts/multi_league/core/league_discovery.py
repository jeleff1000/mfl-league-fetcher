"""
League Discovery Tool - Auto-discover Yahoo Fantasy Leagues

This module provides tools to:
- Discover all leagues accessible via OAuth credentials
- Fetch league metadata (name, team count, scoring type, etc.)
- Create LeagueContext objects for discovered leagues
- Register multiple leagues for batch processing

Key features:
- OAuth-based league discovery
- Automatic metadata fetching
- Interactive league selection
- Batch context creation
- League registry management

Usage:
    # Discover all leagues
    discovery = LeagueDiscovery(oauth_file=Path("Oauth.json"))
    leagues = discovery.discover_leagues(year=2024)

    # Create context for specific league
    ctx = discovery.create_league_context(
        league_id="nfl.l.123456",
        oauth_file=Path("Oauth.json"),
        start_year=2020
    )

    # Interactive registration
    discovery.interactive_register_leagues()
"""

import json
from pathlib import Path
from typing import Any
from datetime import datetime

from multi_league.core.date_utils import get_current_nfl_season_year

try:
    from yahoo_oauth import OAuth2
    from yahoo_fantasy_api import Game, League

    YAHOO_API_AVAILABLE = True
except ImportError:
    YAHOO_API_AVAILABLE = False
    print("Warning: yahoo_oauth or yahoo_fantasy_api not available. Install with:")
    print("  pip install yahoo_oauth yahoo_fantasy_api")

from core.league_context import LeagueContext, create_league_context


class LeagueDiscovery:
    """
    Discover and register Yahoo Fantasy Football leagues.

    This class provides tools to:
    1. Discover all leagues accessible via OAuth
    2. Fetch detailed league metadata
    3. Create LeagueContext objects
    4. Manage league registry
    """

    def __init__(self, oauth_file: Path | None = None, game_code: str = "nfl"):
        """
        Initialize league discovery.

        Args:
            oauth_file: Path to Yahoo OAuth credentials (optional)
            game_code: Game type (nfl, mlb, nba, nhl)
        """
        self.oauth_file = Path(oauth_file) if oauth_file else None
        self.game_code = game_code
        self.oauth = None
        self.game = None

        if oauth_file and YAHOO_API_AVAILABLE:
            self._initialize_oauth()

    def _initialize_oauth(self):
        """Initialize OAuth connection."""
        if not self.oauth_file or not self.oauth_file.exists():
            raise FileNotFoundError(f"OAuth file not found: {self.oauth_file}")

        try:
            self.oauth = OAuth2(None, None, from_file=str(self.oauth_file))
            self.game = Game(self.oauth, self.game_code)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Yahoo OAuth: {e}")

    def discover_leagues(self, year: int | None = None) -> list[dict[str, Any]]:
        """
        Discover all leagues for authenticated user.

        Args:
            year: Specific year to search (None = current year)

        Returns:
            List of league metadata dictionaries

        Example:
            leagues = discovery.discover_leagues(year=2024)
            for league in leagues:
                print(f"{league['league_name']}: {league['league_id']}")
        """
        if not YAHOO_API_AVAILABLE:
            raise RuntimeError("Yahoo API libraries not available")

        if not self.game:
            raise RuntimeError("OAuth not initialized. Provide oauth_file.")

        if year is None:
            year = get_current_nfl_season_year()

        print(f"Discovering {self.game_code.upper()} leagues for {year}...")

        try:
            # Get all league IDs for the year
            league_ids = self.game.league_ids(year=year)

            if not league_ids:
                print(f"No leagues found for {year}")
                return []

            print(f"Found {len(league_ids)} league(s)")

            # Fetch metadata for each league
            leagues = []
            for i, league_key in enumerate(league_ids, 1):
                print(f"  [{i}/{len(league_ids)}] Fetching {league_key}...", end=" ")
                try:
                    metadata = self._fetch_league_metadata(league_key)
                    leagues.append(metadata)
                    print(f"[OK] {metadata['league_name']}")
                except Exception as e:
                    print(f"[FAIL] Error: {e}")

            return leagues

        except Exception as e:
            raise RuntimeError(f"Failed to discover leagues: {e}")

    def _fetch_league_metadata(self, league_key: str) -> dict[str, Any]:
        """
        Fetch detailed metadata for a specific league.

        Args:
            league_key: Yahoo league_key (e.g., "nfl.l.123456")

        Returns:
            Dictionary with league metadata
        """
        league = League(self.oauth, league_key)

        # Fetch settings
        settings = league.settings()

        # Extract metadata
        metadata = {
            "league_id": league_key,
            "league_name": settings.get("name", "Unknown League"),
            "game_code": settings.get("game_code", self.game_code),
            "season": settings.get("season"),
            "num_teams": settings.get("num_teams"),
            "scoring_type": settings.get("scoring_type", "standard"),
            "playoff_start_week": settings.get("playoff_start_week"),
            "num_playoff_teams": settings.get("num_playoff_teams"),
            "num_playoff_consolation_teams": settings.get("num_playoff_consolation_teams", 0),
            "start_week": settings.get("start_week", 1),
            "end_week": settings.get("end_week", 18),
            "current_week": settings.get("current_week"),
            "is_finished": settings.get("is_finished", False),
            "url": settings.get("url"),
            "discovered_at": datetime.now().isoformat(),
        }

        # Get roster positions
        try:
            roster_positions = league.positions()
            metadata["roster_positions"] = roster_positions
        except Exception:
            metadata["roster_positions"] = None

        return metadata

    def create_league_context(
        self,
        league_id: str,
        oauth_file: Path,
        start_year: int | None = None,
        league_name: str | None = None,
        data_directory: Path | None = None,
        fetch_metadata: bool = True,
        **kwargs,
    ) -> LeagueContext:
        """
        Create LeagueContext for a specific league.

        Args:
            league_id: Yahoo league_key
            oauth_file: Path to OAuth credentials
            start_year: First year to process (auto-detect if None)
            league_name: Override league name (fetch if None)
            data_directory: Custom data directory
            fetch_metadata: Fetch metadata from Yahoo API
            **kwargs: Additional LeagueContext parameters

        Returns:
            LeagueContext instance (saved to disk)

        Example:
            ctx = discovery.create_league_context(
                league_id="nfl.l.123456",
                oauth_file=Path("Oauth.json"),
                start_year=2020
            )
        """
        # Fetch metadata if requested
        if fetch_metadata and YAHOO_API_AVAILABLE:
            if not self.oauth:
                self._initialize_oauth()

            print(f"Fetching metadata for {league_id}...")
            metadata = self._fetch_league_metadata(league_id)

            # Use fetched data
            if not league_name:
                league_name = metadata["league_name"]

            if not start_year:
                start_year = metadata["season"]

            # Add metadata to kwargs
            kwargs.setdefault("num_teams", metadata.get("num_teams"))
            kwargs.setdefault("playoff_teams", metadata.get("num_playoff_teams"))
            kwargs.setdefault("regular_season_weeks", metadata.get("playoff_start_week", 14) - 1)

        # Validate required fields
        if not league_name:
            raise ValueError("league_name is required (provide or enable fetch_metadata)")

        if not start_year:
            raise ValueError("start_year is required (provide or enable fetch_metadata)")

        # Create context
        ctx = create_league_context(
            league_id=league_id,
            league_name=league_name,
            oauth_file_path=str(oauth_file),
            start_year=start_year,
            data_directory=data_directory,
            **kwargs,
        )

        print(f"Created context: {ctx.data_directory / 'league_context.json'}")
        return ctx

    def interactive_register_leagues(
        self, year: int | None = None, oauth_file: Path | None = None
    ) -> list[LeagueContext]:
        """
        Interactive CLI for discovering and registering leagues.

        Discovers all available leagues and prompts user to select which
        ones to register for data processing.

        Args:
            year: Year to search (None = current year)
            oauth_file: OAuth file (uses self.oauth_file if None)

        Returns:
            List of created LeagueContext objects

        Example:
            discovery = LeagueDiscovery(oauth_file=Path("Oauth.json"))
            contexts = discovery.interactive_register_leagues()
        """
        if oauth_file is None:
            oauth_file = self.oauth_file

        if not oauth_file:
            raise ValueError("oauth_file is required")

        # Discover leagues
        leagues = self.discover_leagues(year=year)

        if not leagues:
            print("No leagues found.")
            return []

        # Display discovered leagues
        print("\n" + "=" * 70)
        print("DISCOVERED LEAGUES")
        print("=" * 70)

        for i, league in enumerate(leagues, 1):
            print(f"\n[{i}] {league['league_name']}")
            print(f"    League ID: {league['league_id']}")
            print(f"    Season: {league['season']}")
            print(f"    Teams: {league['num_teams']}")
            print(f"    Scoring: {league['scoring_type']}")
            print(f"    Status: {'Finished' if league['is_finished'] else 'Active'}")

        # Prompt for selection
        print("\n" + "=" * 70)
        print("Which leagues do you want to register?")
        print("Enter numbers separated by commas (e.g., '1,3' or 'all'):")
        selection = input("> ").strip().lower()

        # Parse selection
        if selection == "all":
            selected_indices = list(range(len(leagues)))
        else:
            try:
                selected_indices = [int(x.strip()) - 1 for x in selection.split(",")]
                selected_indices = [i for i in selected_indices if 0 <= i < len(leagues)]
            except ValueError:
                print("Invalid selection. Aborting.")
                return []

        if not selected_indices:
            print("No leagues selected.")
            return []

        # Create contexts for selected leagues
        contexts = []
        for idx in selected_indices:
            league = leagues[idx]
            print(f"\nRegistering: {league['league_name']}...")

            # Prompt for start year
            default_start = league["season"]
            start_year_input = input(f"  Start year (default: {default_start}): ").strip()
            start_year = int(start_year_input) if start_year_input else default_start

            # Create context
            try:
                ctx = self.create_league_context(
                    league_id=league["league_id"],
                    oauth_file=oauth_file,
                    start_year=start_year,
                    league_name=league["league_name"],
                    fetch_metadata=False,  # Already have metadata
                )
                contexts.append(ctx)
                print(f"  [OK] Registered: {ctx.data_directory}")
            except Exception as e:
                print(f"  ✗ Error: {e}")

        print(f"\n{'=' * 70}")
        print(f"Registered {len(contexts)} league(s)")
        print(f"{'=' * 70}\n")

        return contexts


class LeagueRegistry:
    """
    Manage registry of all configured leagues.

    The registry is a JSON file that tracks all leagues that have been
    set up for processing. This makes it easy to:
    - List all configured leagues
    - Batch process multiple leagues
    - Track which leagues are active/inactive
    """

    def __init__(self, registry_file: Path | None = None):
        """
        Initialize registry.

        Args:
            registry_file: Path to registry JSON (default: ~/fantasy_football_data/leagues.json)
        """
        if registry_file is None:
            registry_file = Path.home() / "fantasy_football_data" / "leagues.json"
        else:
            registry_file = Path(registry_file)

        self.registry_file = registry_file
        self.leagues = self._load_registry()

    def _load_registry(self) -> dict[str, dict[str, Any]]:
        """Load registry from disk."""
        if not self.registry_file.exists():
            return {}

        try:
            with open(self.registry_file, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load registry: {e}")
            return {}

    def _save_registry(self):
        """Save registry to disk."""
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)

        with open(self.registry_file, "w", encoding="utf-8") as f:
            json.dump(self.leagues, f, indent=2, ensure_ascii=False)

    def register_league(self, ctx: LeagueContext):
        """
        Add league to registry.

        Args:
            ctx: LeagueContext to register
        """
        self.leagues[ctx.league_id] = {
            "league_id": ctx.league_id,
            "league_name": ctx.league_name,
            "context_file": str(ctx.data_directory / "league_context.json"),
            "data_directory": str(ctx.data_directory),
            "start_year": ctx.start_year,
            "end_year": ctx.end_year,
            "num_teams": ctx.num_teams,
            "registered_at": datetime.now().isoformat(),
            "status": "active",
        }

        self._save_registry()

    def unregister_league(self, league_id: str):
        """
        Remove league from registry.

        Args:
            league_id: League to remove
        """
        if league_id in self.leagues:
            del self.leagues[league_id]
            self._save_registry()

    def get_league(self, league_id: str) -> dict[str, Any] | None:
        """Get league entry from registry."""
        return self.leagues.get(league_id)

    def list_leagues(self, status: str | None = None) -> list[dict[str, Any]]:
        """
        List all registered leagues.

        Args:
            status: Filter by status ('active', 'inactive', None=all)

        Returns:
            List of league entries
        """
        leagues = list(self.leagues.values())

        if status:
            leagues = [l for l in leagues if l.get("status") == status]

        return leagues

    def load_contexts(self, status: str | None = None) -> list[LeagueContext]:
        """
        Load LeagueContext objects for all registered leagues.

        Args:
            status: Filter by status ('active', 'inactive', None=all)

        Returns:
            List of LeagueContext objects
        """
        contexts = []

        for league in self.list_leagues(status=status):
            context_file = Path(league["context_file"])

            try:
                ctx = LeagueContext.load(context_file)
                contexts.append(ctx)
            except Exception as e:
                print(f"Warning: Could not load {league['league_id']}: {e}")

        return contexts

    def summary(self) -> str:
        """Get summary of registry."""
        active = len([l for l in self.leagues.values() if l.get("status") == "active"])
        total = len(self.leagues)

        lines = [
            f"League Registry: {self.registry_file}",
            f"Total Leagues: {total}",
            f"Active: {active}",
            f"Inactive: {total - active}",
            "",
        ]

        if self.leagues:
            lines.append("Registered Leagues:")
            for league in sorted(self.leagues.values(), key=lambda x: x["league_name"]):
                status_icon = "[OK]" if league.get("status") == "active" else "✗"
                lines.append(f"  [{status_icon}] {league['league_name']} ({league['league_id']})")
                lines.append(f"      Years: {league['start_year']}-{league['end_year'] or 'current'}")

        return "\n".join(lines)


# === CLI Support ===

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="League Discovery and Registration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Discover leagues for 2024
  python league_discovery.py discover --oauth Oauth.json --year 2024

  # Interactive registration
  python league_discovery.py register --oauth Oauth.json

  # Create context for specific league
  python league_discovery.py create --league-id nfl.l.123456 --oauth Oauth.json --start-year 2020

  # List registered leagues
  python league_discovery.py list
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Discover command
    discover_parser = subparsers.add_parser("discover", help="Discover available leagues")
    discover_parser.add_argument("--oauth", required=True, help="Path to OAuth file")
    discover_parser.add_argument("--year", type=int, help="Year to search (default: current)")
    discover_parser.add_argument("--game", default="nfl", help="Game code (nfl, mlb, nba, nhl)")

    # Register command (interactive)
    register_parser = subparsers.add_parser("register", help="Interactively register leagues")
    register_parser.add_argument("--oauth", required=True, help="Path to OAuth file")
    register_parser.add_argument("--year", type=int, help="Year to search (default: current)")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create context for specific league")
    create_parser.add_argument("--league-id", required=True, help="Yahoo league_key")
    create_parser.add_argument("--oauth", required=True, help="Path to OAuth file")
    create_parser.add_argument("--start-year", type=int, help="Start year")
    create_parser.add_argument("--league-name", help="League name (auto-fetch if omitted)")
    create_parser.add_argument("--data-dir", help="Custom data directory")

    # List command
    list_parser = subparsers.add_parser("list", help="List registered leagues")
    list_parser.add_argument("--status", choices=["active", "inactive"], help="Filter by status")
    list_parser.add_argument("--registry", help="Path to registry file")

    args = parser.parse_args()

    if args.command == "discover":
        discovery = LeagueDiscovery(oauth_file=Path(args.oauth), game_code=args.game)
        leagues = discovery.discover_leagues(year=args.year)

        print(f"\nFound {len(leagues)} league(s):\n")
        for league in leagues:
            print(f"  {league['league_name']} ({league['league_id']})")
            print(f"    Season: {league['season']}, Teams: {league['num_teams']}")
            print()

    elif args.command == "register":
        discovery = LeagueDiscovery(oauth_file=Path(args.oauth))
        contexts = discovery.interactive_register_leagues(year=args.year)

        if contexts:
            # Add to registry
            registry = LeagueRegistry()
            for ctx in contexts:
                registry.register_league(ctx)

            print("\nLeagues registered successfully!")
            print(registry.summary())

    elif args.command == "create":
        discovery = LeagueDiscovery(oauth_file=Path(args.oauth))
        ctx = discovery.create_league_context(
            league_id=args.league_id,
            oauth_file=Path(args.oauth),
            start_year=args.start_year,
            league_name=args.league_name,
            data_directory=Path(args.data_dir) if args.data_dir else None,
        )

        print(f"\nCreated context: {ctx.data_directory / 'league_context.json'}")
        print("\n" + ctx.summary())

        # Ask if user wants to register
        register_input = input("\nAdd to league registry? [y/N]: ").strip().lower()
        if register_input == "y":
            registry = LeagueRegistry()
            registry.register_league(ctx)
            print("Registered!")

    elif args.command == "list":
        registry_file = Path(args.registry) if args.registry else None
        registry = LeagueRegistry(registry_file=registry_file)

        print(registry.summary())

    else:
        parser.print_help()
