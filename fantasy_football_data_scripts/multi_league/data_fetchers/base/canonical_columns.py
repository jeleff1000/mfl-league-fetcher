"""
Canonical Column Definitions for Fantasy Football Data

This module defines the standard column names that ALL platform-specific
data fetchers must produce after normalization. This ensures:
1. Yahoo and Sleeper data have identical schemas
2. Downstream transformations work platform-agnostically
3. Schema validation can verify output correctness

Column Naming Conventions:
- Use snake_case for all column names
- Platform-specific IDs keep their prefix (yahoo_player_id, sleeper_player_id)
- NFL_player_id is the universal join key to the super table
- Composite keys (player_week, cumulative_week) are computed post-normalization
"""


class CanonicalPlayerColumns:
    """
    Canonical column names for player/roster data (player_fantasy table).

    These columns are produced by:
    - Yahoo: yahoo_fantasy_data.py -> yahoo_normalizer.py
    - Sleeper: sleeper_rosters.py -> sleeper_normalizer.py
    """

    # Time dimensions
    YEAR = "year"
    WEEK = "week"

    # Manager/Team identifiers
    MANAGER = "manager"  # Display name (canonical)
    MANAGER_GUID = "manager_guid"  # Platform user ID (stable)
    TEAM_KEY = "team_key"  # Platform team ID (per-year)
    TEAM_NAME = "team_name"  # Team display name
    FRANCHISE_ID = "franchise_id"  # Cross-year franchise identifier

    # Player identifiers
    PLAYER = "player"  # Display name (canonical)
    NFL_PLAYER_ID = "NFL_player_id"  # Universal join key to super table
    YAHOO_PLAYER_ID = "yahoo_player_id"  # Yahoo-specific (NULL for Sleeper)
    SLEEPER_PLAYER_ID = "sleeper_player_id"  # Sleeper-specific (NULL for Yahoo)

    # Position columns
    POSITION = "position"  # NFL position (QB, RB, WR, TE, K, DEF)
    FANTASY_POSITION = "fantasy_position"  # Roster slot (QB, RB1, FLEX, BN)
    ELIGIBLE_POSITIONS = "eligible_positions"  # List of eligible slots

    # Stats
    FANTASY_POINTS = "fantasy_points"  # Points scored
    PROJECTED_POINTS = "projected_points"  # Projected points (Yahoo only, NULL for Sleeper)

    # NFL team
    NFL_TEAM = "nfl_team"  # Team abbreviation (KC, SF, etc.)

    # Computed flags
    IS_STARTED = "is_started"  # True if not on bench
    IS_ROSTERED = "is_rostered"  # True if on a roster

    # Composite keys (computed post-normalization)
    PLAYER_WEEK = "player_week"  # {NFL_player_id}_{year}_{week}
    PLAYER_YEAR = "player_year"  # {NFL_player_id}_{year}
    CUMULATIVE_WEEK = "cumulative_week"  # year * 100 + week

    # Platform/League identifiers
    PLATFORM = "platform"  # 'yahoo' or 'sleeper'
    LEAGUE_ID = "league_id"  # Platform-specific league ID

    # Backward compatibility aliases
    # These are added by normalizers for UI compatibility
    PLAYER_NAME = "player_name"  # Alias for player
    YAHOO_POSITION = "yahoo_position"  # Alias for position
    ROSTER_POSITION = "roster_position"  # Alias for fantasy_position


class CanonicalMatchupColumns:
    """
    Canonical column names for matchup data (matchup table).

    These columns are produced by:
    - Yahoo: weekly_matchup_data_v2.py -> yahoo_normalizer.py
    - Sleeper: sleeper_matchups.py -> sleeper_normalizer.py
    """

    # Time dimensions
    YEAR = "year"
    WEEK = "week"

    # Manager/Team identifiers
    MANAGER = "manager"
    MANAGER_GUID = "manager_guid"
    TEAM_KEY = "team_key"
    TEAM_NAME = "team_name"
    FRANCHISE_ID = "franchise_id"

    # Opponent
    OPPONENT = "opponent"
    OPPONENT_GUID = "opponent_guid"
    OPPONENT_TEAM_NAME = "opponent_team_name"

    # Scores
    POINTS = "points"  # Canonical name (also aliased as team_points)
    TEAM_POINTS = "team_points"  # Alias for backward compatibility
    OPPONENT_POINTS = "opponent_points"
    MARGIN = "margin"  # points - opponent_points

    # Projections (Yahoo only, NULL for Sleeper)
    TEAM_PROJECTED_POINTS = "team_projected_points"
    OPPONENT_PROJECTED_POINTS = "opponent_projected_points"

    # Win/Loss
    WIN = "win"
    LOSS = "loss"
    TIE = "tie"

    # Matchup metadata
    MATCHUP_ID = "matchup_id"  # Unique matchup identifier
    IS_PLAYOFFS = "is_playoffs"
    IS_CONSOLATION = "is_consolation"
    IS_BYE = "is_bye_week"

    # Grades (Yahoo only, NULL for Sleeper)
    GRADE = "grade"  # A+, A, B+, etc.
    GPA = "gpa"  # Numeric grade (4.0, 3.7, etc.)

    # Recap (Yahoo only, NULL for Sleeper)
    MATCHUP_RECAP_URL = "matchup_recap_url"
    MATCHUP_RECAP_TITLE = "matchup_recap_title"

    # League standings context
    TEAMS_BEAT_THIS_WEEK = "teams_beat_this_week"  # For median scoring
    LEAGUE_WEEKLY_MEAN = "league_weekly_mean"

    # Platform identifier
    PLATFORM = "platform"
    LEAGUE_ID = "league_id"


class CanonicalDraftColumns:
    """
    Canonical column names for draft data (draft table).

    These columns are produced by:
    - Yahoo: draft_data_v2.py -> yahoo_normalizer.py
    - Sleeper: sleeper_draft.py -> sleeper_normalizer.py
    """

    # Time dimension
    YEAR = "year"

    # Pick info
    PICK = "pick"  # Overall pick number (1-indexed)
    ROUND = "round"  # Round number (1-indexed)
    PICK_IN_ROUND = "pick_in_round"  # Pick within round

    # Manager/Team identifiers
    MANAGER = "manager"
    MANAGER_GUID = "manager_guid"
    TEAM_KEY = "team_key"
    FRANCHISE_ID = "franchise_id"

    # Player identifiers
    PLAYER = "player"
    NFL_PLAYER_ID = "NFL_player_id"
    YAHOO_PLAYER_ID = "yahoo_player_id"
    SLEEPER_PLAYER_ID = "sleeper_player_id"

    # Position
    POSITION = "position"

    # NFL team
    NFL_TEAM = "nfl_team"

    # Auction/Keeper
    COST = "cost"  # Auction cost (NULL for snake drafts)
    IS_KEEPER = "is_keeper"  # Boolean keeper flag
    DRAFT_TYPE = "draft_type"  # 'auction', 'snake', 'offline'

    # ADP data (Yahoo only, NULL for Sleeper)
    AVG_PICK = "avg_pick"
    AVG_ROUND = "avg_round"
    AVG_COST = "avg_cost"
    PERCENT_DRAFTED = "percent_drafted"
    PRESEASON_AVG_PICK = "preseason_avg_pick"
    PRESEASON_AVG_ROUND = "preseason_avg_round"
    PRESEASON_AVG_COST = "preseason_avg_cost"
    PRESEASON_PERCENT_DRAFTED = "preseason_percent_drafted"

    # Pick quality metrics (computed by transformations)
    PICK_QUALITY_SCORE = "pick_quality_score"  # Yahoo: percentile
    PICK_QUALITY_ZSCORE = "pick_quality_zscore"  # Sleeper: z-score

    # Platform identifier
    PLATFORM = "platform"
    LEAGUE_ID = "league_id"

    # Backward compatibility
    PLAYER_NAME = "player_name"
    PICK_NUMBER = "pick_number"  # Alias for pick
    KEEPER_COST = "keeper_cost"  # Alias for cost


class CanonicalTransactionColumns:
    """
    Canonical column names for transaction data (transactions table).

    These columns are produced by:
    - Yahoo: transactions_v2.py -> yahoo_normalizer.py
    - Sleeper: sleeper_transactions.py -> sleeper_normalizer.py
    """

    # Transaction identifier
    TRANSACTION_ID = "transaction_id"

    # Time dimensions
    YEAR = "year"
    WEEK = "week"
    TIMESTAMP = "timestamp"  # Unix timestamp or datetime

    # Manager/Team identifiers
    MANAGER = "manager"
    MANAGER_GUID = "manager_guid"
    TEAM_KEY = "team_key"
    TEAM_NAME = "team_name"
    FRANCHISE_ID = "franchise_id"

    # Player identifiers
    PLAYER = "player"
    NFL_PLAYER_ID = "NFL_player_id"
    YAHOO_PLAYER_ID = "yahoo_player_id"
    SLEEPER_PLAYER_ID = "sleeper_player_id"

    # Transaction type
    TRANSACTION_TYPE = "transaction_type"  # 'add', 'drop', 'trade'

    # FAAB
    FAAB_SPENT = "faab_spent"  # Amount spent (NULL if not FAAB league)

    # Source/Destination
    SOURCE_TEAM = "source_team"  # Where player came from
    DESTINATION_TEAM = "destination_team"  # Where player went

    # Position
    POSITION = "position"

    # Platform identifier
    PLATFORM = "platform"
    LEAGUE_ID = "league_id"

    # Backward compatibility
    PLAYER_NAME = "player_name"


# Required columns for schema validation
# These MUST be present after normalization

PLAYER_REQUIRED_COLUMNS: list[str] = [
    "year",
    "week",
    "manager",
    "manager_guid",
    "player",
    "position",
    "fantasy_position",
    "fantasy_points",
    "NFL_player_id",
    "player_week",
    "platform",
]

MATCHUP_REQUIRED_COLUMNS: list[str] = [
    "year",
    "week",
    "manager",
    "manager_guid",
    "points",
    "opponent",
    "opponent_points",
    "platform",
]

DRAFT_REQUIRED_COLUMNS: list[str] = [
    "year",
    "pick",
    "round",
    "manager",
    "manager_guid",
    "player",
    "position",
    "NFL_player_id",
    "platform",
]

TRANSACTION_REQUIRED_COLUMNS: list[str] = [
    "transaction_id",
    "year",
    "week",
    "manager",
    "player",
    "transaction_type",
    "platform",
]


# Column type specifications for validation
PLAYER_COLUMN_TYPES: dict[str, str] = {
    "year": "Int64",
    "week": "Int64",
    "manager": "string",
    "manager_guid": "string",
    "player": "string",
    "position": "string",
    "fantasy_position": "string",
    "fantasy_points": "float64",
    "NFL_player_id": "string",
    "player_week": "string",
    "platform": "string",
    "is_started": "bool",
    "is_rostered": "bool",
}

MATCHUP_COLUMN_TYPES: dict[str, str] = {
    "year": "Int64",
    "week": "Int64",
    "manager": "string",
    "manager_guid": "string",
    "points": "float64",
    "opponent": "string",
    "opponent_points": "float64",
    "margin": "float64",
    "win": "Int64",
    "loss": "Int64",
    "tie": "Int64",
    "platform": "string",
}

DRAFT_COLUMN_TYPES: dict[str, str] = {
    "year": "Int64",
    "pick": "Int64",
    "round": "Int64",
    "manager": "string",
    "manager_guid": "string",
    "player": "string",
    "position": "string",
    "cost": "float64",
    "is_keeper": "bool",
    "NFL_player_id": "string",
    "platform": "string",
}

TRANSACTION_COLUMN_TYPES: dict[str, str] = {
    "transaction_id": "string",
    "year": "Int64",
    "week": "Int64",
    "manager": "string",
    "player": "string",
    "transaction_type": "string",
    "faab_spent": "float64",
    "platform": "string",
}


# Yahoo-only columns (Sleeper sets these to NULL)
YAHOO_ONLY_COLUMNS: set[str] = {
    # Draft ADP data
    "avg_pick",
    "avg_round",
    "avg_cost",
    "percent_drafted",
    "preseason_avg_pick",
    "preseason_avg_round",
    "preseason_avg_cost",
    "preseason_percent_drafted",
    # Matchup grades/recaps
    "grade",
    "gpa",
    "matchup_recap_url",
    "matchup_recap_title",
    # Projections
    "projected_points",
    "team_projected_points",
    "opponent_projected_points",
    "has_draft_grade",
}


# Sleeper-specific columns (Yahoo sets these to NULL)
SLEEPER_ONLY_COLUMNS: set[str] = {
    # Currently none - Sleeper mimics Yahoo schema
}


# Column aliases for backward compatibility
# Maps canonical name -> list of acceptable aliases
COLUMN_ALIASES: dict[str, list[str]] = {
    "manager": ["manager_name"],
    "player": ["player_name"],
    "position": ["yahoo_position", "primary_position", "nfl_position"],
    "fantasy_position": ["roster_position"],
    "points": ["team_points"],
    "pick": ["pick_number"],
    "cost": ["keeper_cost"],
    "is_keeper": ["is_keeper_status", "is_keeper_cost"],
}
