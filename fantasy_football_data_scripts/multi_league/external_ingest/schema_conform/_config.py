"""Static configuration: slot whitelists, thresholds, aliases.

All tunables live here. Phase B will move some to YAML; Phase A keeps them hardcoded.
"""

REQUIRED_IDENTITY_SLOTS = {
    "matchup": ["year", "week", "manager_guid"],
    "player_fantasy": ["year", "week", "manager_guid", "yahoo_player_id"],
    # Draft uploads often arrive as human-entered auction sheets with player
    # names but no platform player IDs. The staging merger can backfill player
    # identity from the API draft rows by year+player, so do not require a
    # Yahoo-specific ID during schema conform.
    "draft": ["year", "round", "pick", "manager_guid"],
    "transactions": ["year", "week", "transaction_id", "manager_guid", "yahoo_player_id"],
}

REQUIRED_DATA_SLOTS = {
    "matchup": ["team_name", "team_points", "opponent", "opponent_points"],
    "player_fantasy": ["player", "fantasy_position", "fantasy_points"],
    "draft": ["player", "cost"],
    "transactions": ["transaction_type"],
}

OPTIONAL_SLOTS = {
    # win/loss/tie are derivable from team_points > opponent_points and the
    # main pipeline does that, but external imports (e.g. KMFFL 2013 historical)
    # often carry win/loss WITHOUT scores. The bracket tracer falls back to
    # these flags to identify champions when SUM(points) is NULL, so we must
    # preserve them through schema_conform instead of silently dropping them.
    "matchup": ["team_key", "division_id", "win", "loss", "tie"],
    "player_fantasy": ["team_key", "position"],
    "draft": ["team_key", "position", "is_keeper", "draft_type"],
    "transactions": ["source_type", "destination", "faab_bid", "timestamp", "player"],
}

COVERAGE_THRESHOLDS = {
    "matchup": 0.98,
    "player_fantasy": 0.95,
    "draft": 0.95,
    "transactions": 0.85,  # sparse / partial exports common
}

MIN_REF_SIZE_BY_SLOT = {
    "manager_guid": 8,
    "opponent_guid": 8,
    "team_key": 8,
    "yahoo_player_id": 100,
    "transaction_id": 100,
}
DEFAULT_MIN_REF_SIZE = 50

ALIASES = {
    "mgr": "manager",
    "owner": "manager",
    "gm": "manager",
    "key": "id",
    "pts": "points",
    "fp": "fantasy_points",
    "txn": "transaction",
    "tx": "transaction",
    "yr": "year",
    "wk": "week",
    "rd": "round",
    "pos": "position",
    "tm": "team",
    "opp": "opponent",
}

# Token set that triggers context-sensitive 'id' → 'guid' folding in _alias_norm.
IDENTITY_TOKENS = {"manager", "player", "team", "owner", "mgr", "gm"}

# Score thresholds
CONFIDENCE_FLOOR = 0.85
COLLISION_GAP = 0.05
FUZZY_CUTOFF = 90
FUZZY_GAP = 5
REQUIRED_DATA_NULL_FLOOR = 0.5
COOCCURRENCE_MI_FLOOR = 0.5
COOCCURRENCE_MIN_ROWS = 50
CROSS_TABLE_OVERLAP_FLOOR = 0.8

# Adaptive weight regimes
WEIGHTS_THIN_REF = {"name": 0.45, "value": 0.15, "dtype": 0.15, "shape": 0.25}
WEIGHTS_THICK_REF = {"name": 0.30, "value": 0.45, "dtype": 0.10, "shape": 0.15}
