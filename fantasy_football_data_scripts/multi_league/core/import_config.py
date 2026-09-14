"""
Import Configuration Constants

Central configuration for the import pipeline. Contains:
- DATA_FETCHERS: List of data fetcher scripts to run
- TRANSFORMATIONS_PASS_*: Transformation scripts organized by dependency pass
- Terminal and recoverable error patterns

This is the SINGLE SOURCE OF TRUTH for transformation pass definitions.
All three orchestrators (Yahoo, Sleeper, ESPN) import from here.
"""

# Transformation entry: (script_path, description, timeout) or
#                        (script_path, description, timeout, [additional_args])
TransformEntry = tuple[str, str, int] | tuple[str, str, int, list]

# =============================================================================
# Script Paths
# =============================================================================

# Legacy script paths — still imported by initial_import_v2, initial_import_v3, orchestrator
YAHOO_NFL_MERGE = "multi_league/data_fetchers/yahoo/yahoo_nfl_merge.py"
NFL_SUPER_TABLE_LOADER = "multi_league/data_fetchers/load_nfl_from_super_table.py"
YAHOO_FETCHER = "multi_league/data_fetchers/yahoo/yahoo_rosters.py"

# =============================================================================
# Data Fetchers
# =============================================================================

DATA_FETCHERS: list[tuple[str, str, int]] = [
    ("multi_league/data_fetchers/yahoo/yahoo_matchups.py", "Matchup data", 1800),
    (
        "multi_league/data_fetchers/yahoo/yahoo_rosters.py",
        "Yahoo player data",
        3600,
    ),  # Heaviest: many years x weeks x teams + rate limit cooldowns
    ("multi_league/data_fetchers/load_nfl_from_super_table.py", "NFL stats (super table)", 900),
    ("multi_league/data_fetchers/yahoo/yahoo_draft.py", "Draft data (all years)", 900),
    ("multi_league/data_fetchers/yahoo/yahoo_transactions.py", "Transactions (all years)", 1800),
    ("multi_league/data_fetchers/yahoo/yahoo_schedules.py", "Schedule data (league week windows)", 600),
]

SLEEPER_DATA_FETCHERS: list[tuple[str, str, int]] = [
    ("multi_league/data_fetchers/sleeper/sleeper_matchups.py", "Matchup data", 1800),
    ("multi_league/data_fetchers/sleeper/sleeper_rosters.py", "Roster data", 1800),
    ("multi_league/data_fetchers/load_nfl_from_super_table.py", "NFL stats (super table)", 900),
    ("multi_league/data_fetchers/sleeper/sleeper_draft.py", "Draft data", 900),
    ("multi_league/data_fetchers/sleeper/sleeper_transactions.py", "Transactions", 1800),
    ("multi_league/data_fetchers/sleeper/sleeper_traded_picks.py", "Traded draft picks", 600),
    ("multi_league/data_fetchers/sleeper/sleeper_schedules.py", "Schedule data", 600),
]

ESPN_DATA_FETCHERS: list[tuple[str, str, int]] = [
    ("multi_league/data_fetchers/espn/espn_matchups.py", "Matchup data", 1800),
    ("multi_league/data_fetchers/espn/espn_rosters.py", "Roster data", 1800),
    ("multi_league/data_fetchers/load_nfl_from_super_table.py", "NFL stats (super table)", 900),
    ("multi_league/data_fetchers/espn/espn_draft.py", "Draft data", 900),
    ("multi_league/data_fetchers/espn/espn_transactions.py", "Transactions", 1800),
    ("multi_league/data_fetchers/espn/espn_schedules.py", "Schedule data", 600),
]

FLEAFLICKER_DATA_FETCHERS: list[tuple[str, str, int]] = [
    ("multi_league/data_fetchers/fleaflicker/fleaflicker_matchups.py", "Matchup data", 1800),
    ("multi_league/data_fetchers/fleaflicker/fleaflicker_rosters.py", "Roster data", 3600),
    ("multi_league/data_fetchers/load_nfl_from_super_table.py", "NFL stats (super table)", 900),
    ("multi_league/data_fetchers/fleaflicker/fleaflicker_draft.py", "Draft data", 900),
    ("multi_league/data_fetchers/fleaflicker/fleaflicker_transactions.py", "Transactions", 1800),
    ("multi_league/data_fetchers/fleaflicker/fleaflicker_schedules.py", "Schedule data", 600),
]

# =============================================================================
# Transformations - Pass Definitions
# =============================================================================

# Pass 1: EMPTY — resolve_hidden_managers + discover_franchises replaced by
# sql_matchup_enrichments.resolve_hidden_managers() which runs in the enrichment pipeline.
TRANSFORMATIONS_PASS_1: list[TransformEntry] = []

# ESPN-specific extras for Pass 1 (empty — expected_record_v2.py moved to playoff runner)
ESPN_PASS_1_EXTRAS: list[TransformEntry] = []

# Pass 2A: Create player table from matchups (rostered players only)
# MUST run before pre-upload so we have fresh rostered data
# Manager optimal is calculated via SQL enrichment (sql_player_enrichments.manager_optimal_lineup)
# DEPRECATED: player_stats_v2.py --manager-optimal-only -> replaced by sql_player_enrichments.manager_optimal_lineup()
# DEPRECATED: backfill_yahoo_points.py -> replaced by sql_player_enrichments.backfill_fantasy_points_from_super_table()
TRANSFORMATIONS_PASS_2A: list[TransformEntry] = []

# Pass 2B: Expand to all NFL and calculate stats
# Runs AFTER pre-upload so rostered data is available.
# DEPRECATED: player_stats_v2.py --skip-manager-optimal -> replaced by SQL enrichments:
#   manager_optimal_lineup, calculate_ppg_metrics, calculate_manager_rankings,
#   populate_position_rank, league_wide_optimal_for_all
# PASS 2B: EMPTY — expand_to_all_nfl + scoring + LAMAR now in sql_enrichments.run_all()
TRANSFORMATIONS_PASS_2B: list[TransformEntry] = []

# Pass 3: Draft/transaction LAMAR + finalizations
# These use CORRECT replacement levels (calculated with full NFL pool in Pass 2B)
TRANSFORMATIONS_PASS_3: list[TransformEntry] = [
    # DEPRECATED: player_to_matchup_v2.py - replaced by sql_enrichments.player_to_matchup()
    # Runs in pure SQL after upload (Phase 4), no OOM risk from loading player data
    # ("multi_league/transformations/matchup/player_to_matchup_v2.py", "Player -> Matchup", 600),
    # DEPRECATED: player_to_draft_v2.py - replaced by sql_draft_enrichments.player_to_draft()
    # ("multi_league/transformations/draft/player_to_draft_v2.py", "Player -> Draft", 600),
    # Draft metrics (z-scores, grades, cost buckets, bench insurance, etc.) are now
    # calculated by SQL enrichments in sql_draft_enrichments.py after upload.
    # DEPRECATED: draft_to_player_v2.py -> replaced by sql_draft_enrichments.draft_to_player()
    # DEPRECATED: fix_unknown_managers.py -> replaced by sql_transaction_enrichments.fix_unknown_managers()
    # DEPRECATED: normalize_def_players.py, player_to_transactions_v2.py,
    # enrich_draft_pick_conveyances.py — all replaced by sql_transaction_enrichments.py
    # DEPRECATED: transactions_to_player_v2.py -> replaced by sql_transaction_enrichments.transactions_to_player()
    # DEPRECATED: skinny_player_table.py -> replaced by DDL-first schema (canonical_player.py)
    # MOVED: keeper_economics is now a SQL enrichment in Wave 3 of SQLEnrichments.run_all()
    #        (PlayerEnrichmentsMixin.populate_keeper_economics, after transactions_to_player)
    # NOTE: expected_record_v2.py and playoff_odds_import.py run in separate playoff_odds_worker
    # NOTE: homepage_summary.py runs after playoff odds complete (needs P_Champ data)
]

ALL_TRANSFORMATIONS = (
    TRANSFORMATIONS_PASS_1 + list(TRANSFORMATIONS_PASS_2A) + list(TRANSFORMATIONS_PASS_2B) + TRANSFORMATIONS_PASS_3
)

# =============================================================================
# Error Patterns
# =============================================================================

TERMINAL_ERROR_PATTERNS = [
    r"no (league|data) found",
    r"no data returned",
    r"404",
    r"league.*not found",
]

RECOVERABLE_ERROR_PATTERNS = [
    r"RecoverableAPIError",
    r"Request denied",
    r"temporarily unavailable",
]

# =============================================================================
# Table Configuration
# =============================================================================

PROTECTED_TABLES = {"credentials", "oauth_tokens", "user_credentials", "tokens"}

EXPECTED_COLUMNS = {
    "matchup": ["wins_to_date", "losses_to_date", "champion", "sacko", "final_playoff_seed"],
    "transactions": [
        "fa_lamar_ros",
        "transaction_score",
        "transaction_grade",
        "score_percentile",
        "trade_asset_lamar",
        "trade_net_lamar",
        "trade_grade",
        "trade_percentile",
        "timing_category",
        "pickup_type",
    ],
    "draft": ["manager_lamar", "draft_grade", "pick_score"],
    "player": ["player_lamar", "manager_lamar", "player_week"],
}

# =============================================================================
# Timeout Defaults
# =============================================================================

DEFAULT_FETCHER_TIMEOUT = 600
DEFAULT_TRANSFORMATION_TIMEOUT = 600
DEFAULT_MERGE_TIMEOUT = 1800
DEFAULT_UPLOAD_TIMEOUT = 600
