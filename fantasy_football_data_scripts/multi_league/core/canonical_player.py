"""Canonical player_fantasy schema — one universal DDL for all 500+ leagues.

Every column that player_fantasy can have across all platforms and league types.
Leagues that don't use a feature (IDP, superflex, keepers) get NULL for those columns.
No conditional columns. No per-league schema differences. DuckLake-ready.

Column sources:
  "api"      — raw from platform fetcher (roster data)
  "join_key" — derived by fetcher or early enrichment
  "sql"      - computed by SQL enrichments

Usage:
    from multi_league.core.canonical_player import (
        PLAYER_FANTASY_SCHEMA, COLUMN_TYPES, PLAYER_FANTASY_COLUMNS,
    )
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Canonical schema: (column_name, duckdb_type, source)
# ---------------------------------------------------------------------------

PLAYER_FANTASY_SCHEMA: list[tuple[str, str, str]] = [
    ("db_name", "VARCHAR", "system"),
    # === Join keys ===
    ("player_week", "VARCHAR", "join_key"),  # Primary join to super_table
    ("NFL_player_id", "VARCHAR", "join_key"),  # Canonical player identity
    ("year", "INTEGER", "api"),
    ("week", "INTEGER", "api"),
    ("cumulative_week", "BIGINT", "join_key"),  # year * 100 + week
    ("manager_week", "VARCHAR", "join_key"),  # franchise_id + cumulative_week
    ("manager_year", "VARCHAR", "join_key"),  # franchise_id + year
    # === Player identity ===
    ("player", "VARCHAR", "api"),  # Display name (fallback when super_table join fails)
    ("position", "VARCHAR", "api"),  # NFL position (QB, RB, WR, TE, K, DEF, LB, DL, DB)
    # headshot_url: DEPRECATED on player_fantasy — lives on super_table, JOINed at query time
    # === Platform IDs (NULL on platforms that don't use them) ===
    ("yahoo_player_id", "VARCHAR", "api"),
    ("sleeper_player_id", "VARCHAR", "api"),
    ("espn_player_id", "VARCHAR", "api"),
    ("fleaflicker_player_id", "VARCHAR", "api"),
    ("mfl_player_id", "VARCHAR", "api"),
    # === League context ===
    ("manager", "VARCHAR", "api"),
    ("manager_guid", "VARCHAR", "api"),
    ("franchise_id", "VARCHAR", "join_key"),
    ("franchise_name", "VARCHAR", "join_key"),
    ("team_key", "VARCHAR", "api"),
    ("team_name", "VARCHAR", "api"),
    ("league_id", "VARCHAR", "api"),
    ("platform", "VARCHAR", "api"),
    # === Roster context ===
    ("fantasy_position", "VARCHAR", "api"),  # Lineup slot (QB, RB, FLEX, BN, IR, TAXI)
    ("lineup_position", "VARCHAR", "sql"),  # Canonical lineup slot
    ("is_started", "INTEGER", "api"),  # 1 if in starting lineup
    ("is_rostered", "INTEGER", "api"),  # 1 if on a real manager's roster
    # === Points (league-specific scoring) ===
    ("fantasy_points", "DOUBLE", "api"),  # From platform API or calculated
    ("bonus_points", "DOUBLE", "sql"),  # Bonus scoring applied on top (idempotent tracker)
    ("te_premium_points", "DOUBLE", "sql"),  # TE premium applied on top (idempotent tracker)
    ("projected_points", "DOUBLE", "api"),  # Platform projections (NULL if unavailable)
    # === Opponent / matchup context (from matchup_to_player) ===
    ("opponent", "VARCHAR", "sql"),
    ("opponent_franchise_id", "VARCHAR", "sql"),
    ("opponent_year", "VARCHAR", "sql"),
    ("team_points", "DOUBLE", "sql"),
    ("opponent_points", "DOUBLE", "sql"),
    ("margin", "DOUBLE", "sql"),
    ("win", "INTEGER", "sql"),
    ("loss", "INTEGER", "sql"),
    ("tie", "INTEGER", "sql"),
    ("matchup_name", "VARCHAR", "sql"),  # LEAST(mgr,opp) __vs__ GREATEST(mgr,opp)
    ("team_1", "VARCHAR", "sql"),
    ("team_2", "VARCHAR", "sql"),
    # === League comparison (from matchup_to_player) ===
    ("above_league_median", "INTEGER", "sql"),
    ("below_league_median", "INTEGER", "sql"),
    ("teams_beat_this_week", "INTEGER", "sql"),
    ("opponent_teams_beat_this_week", "INTEGER", "sql"),
    ("league_weekly_mean", "DOUBLE", "sql"),
    ("league_weekly_median", "DOUBLE", "sql"),
    ("close_margin", "INTEGER", "sql"),
    ("total_matchup_score", "DOUBLE", "sql"),
    # === Projection metrics (from matchup_to_player, NULL if no projections) ===
    ("expected_spread", "DOUBLE", "sql"),
    ("expected_odds", "DOUBLE", "sql"),
    ("proj_score_error", "DOUBLE", "sql"),
    ("abs_proj_score_error", "DOUBLE", "sql"),
    ("above_proj_score", "INTEGER", "sql"),
    ("below_proj_score", "INTEGER", "sql"),
    ("win_vs_spread", "INTEGER", "sql"),
    ("lose_vs_spread", "INTEGER", "sql"),
    ("underdog_wins", "INTEGER", "sql"),
    ("favorite_losses", "INTEGER", "sql"),
    ("proj_wins", "INTEGER", "sql"),
    ("proj_losses", "INTEGER", "sql"),
    ("gpa", "DOUBLE", "sql"),  # Yahoo grade → GPA (NULL for non-Yahoo)
    # === Playoff context (from matchup_to_player + bracket tracer) ===
    ("is_playoffs", "BOOLEAN", "sql"),
    ("is_consolation", "BOOLEAN", "sql"),
    ("is_bye_week", "INTEGER", "sql"),  # Phantom row for bye/eliminated teams
    ("championship", "INTEGER", "sql"),
    ("champion", "INTEGER", "sql"),
    ("sacko", "INTEGER", "sql"),
    ("playoff_round", "VARCHAR", "sql"),
    ("consolation_round", "VARCHAR", "sql"),
    ("placement_rank", "INTEGER", "sql"),  # 1=Champion through N=Sacko
    ("placement_game", "VARCHAR", "sql"),  # "Champion", "3rd Place", "Sacko", etc.
    ("final_playoff_seed", "INTEGER", "sql"),
    ("playoff_seed", "INTEGER", "sql"),
    ("team_made_playoffs", "INTEGER", "sql"),
    ("is_championship", "INTEGER", "sql"),
    # === Playoff sim columns (from playoff_scenarios_standalone) ===
    ("team_mu", "DOUBLE", "sql"),
    ("team_sigma", "DOUBLE", "sql"),
    ("win_probability", "DOUBLE", "sql"),
    ("win_probability_vs_avg", "DOUBLE", "sql"),
    ("clinched_playoffs", "INTEGER", "sql"),
    ("clinched_bye", "INTEGER", "sql"),
    ("clinched_first_seed", "INTEGER", "sql"),
    ("eliminated_from_playoffs", "INTEGER", "sql"),
    ("eliminated_from_bye", "INTEGER", "sql"),
    ("playoff_magic_number", "INTEGER", "sql"),
    ("bye_magic_number", "INTEGER", "sql"),
    ("elimination_number", "INTEGER", "sql"),
    ("first_seed_magic_number", "INTEGER", "sql"),
    ("drama_score", "DOUBLE", "sql"),
    ("is_dramatic_win", "INTEGER", "sql"),
    ("is_dramatic_loss", "INTEGER", "sql"),
    ("players_rostered", "INTEGER", "sql"),
    ("players_started", "INTEGER", "sql"),
    ("total_player_points", "DOUBLE", "sql"),
    # === Streak columns (from matchup_to_player) ===
    ("win_streak", "INTEGER", "sql"),
    ("loss_streak", "INTEGER", "sql"),
    # === Manager optimal lineup (best lineup from manager's roster) ===
    ("optimal_player", "INTEGER", "sql"),  # 1 if in manager's optimal lineup
    ("optimal_position", "VARCHAR", "sql"),  # QB1, WR2, FLEX label within manager's optimal
    ("optimal_points", "DOUBLE", "sql"),  # Sum of manager's optimal lineup points
    ("lineup_efficiency", "DOUBLE", "sql"),  # actual / manager_optimal
    ("bench_points", "DOUBLE", "sql"),
    # === League-wide optimal (best possible lineup from entire NFL pool) ===
    ("league_wide_optimal_player", "INTEGER", "sql"),
    ("league_wide_optimal_position", "VARCHAR", "sql"),
    # === Position rank (from super_table, maps to league's scoring variant) ===
    ("position_rank", "INTEGER", "sql"),  # Weekly position rank (generic, used by optimal lineup calc)
    # === Position rank variants (for flex/superflex/IDP, NULL if league doesn't use) ===
    ("flex_week_rank", "INTEGER", "sql"),
    ("flex_season_rank", "INTEGER", "sql"),
    ("flex_alltime_rank", "INTEGER", "sql"),
    ("rec_flex_week_rank", "INTEGER", "sql"),
    ("rec_flex_season_rank", "INTEGER", "sql"),
    ("rec_flex_alltime_rank", "INTEGER", "sql"),
    ("sflex_week_rank", "INTEGER", "sql"),
    ("sflex_season_rank", "INTEGER", "sql"),
    ("sflex_alltime_rank", "INTEGER", "sql"),
    ("db_lb_week_rank", "INTEGER", "sql"),
    ("db_lb_season_rank", "INTEGER", "sql"),
    ("db_lb_alltime_rank", "INTEGER", "sql"),
    ("dl_lb_week_rank", "INTEGER", "sql"),
    ("dl_lb_season_rank", "INTEGER", "sql"),
    ("dl_lb_alltime_rank", "INTEGER", "sql"),
    # === Track 1: NFL-wide rank using league's rules (from super_table, pre-computed) ===
    # "Purdy was the #5 QB in the NFL this week under KMFFL's scoring"
    ("position_week_rank", "INTEGER", "sql"),  # NFL-wide weekly position rank (from super_table)
    ("position_season_rank", "INTEGER", "sql"),  # NFL-wide season position rank (from super_table)
    ("position_alltime_rank", "INTEGER", "sql"),  # NFL-wide all-time position rank (from super_table)
    ("position_week_pct", "DOUBLE", "sql"),
    ("position_season_pct", "DOUBLE", "sql"),
    ("position_alltime_pct", "DOUBLE", "sql"),
    # All-players variant (rank across ALL positions, not just your position)
    ("all_players_week_rank", "INTEGER", "sql"),
    ("all_players_week_pct", "DOUBLE", "sql"),
    ("all_players_alltime_rank", "INTEGER", "sql"),
    ("all_players_alltime_pct", "DOUBLE", "sql"),
    # === Track 2: Manager-specific rank (computed, window functions on player_fantasy) ===
    # "This was Eleff's 3rd best QB game this season"
    (
        "manager_position_week_rank",
        "INTEGER",
        "sql",
    ),  # rank among manager's rostered players at this position this week
    ("manager_position_season_rank", "INTEGER", "sql"),  # manager's Nth best game at this position this season
    ("manager_position_alltime_rank", "INTEGER", "sql"),  # manager's Nth best game at this position all-time
    # Manager-roster rank (across ALL positions on manager's roster)
    ("manager_player_week_rank", "INTEGER", "sql"),  # rank among ALL players on manager's roster this week
    ("manager_player_week_pct", "DOUBLE", "sql"),
    ("manager_player_season_rank", "INTEGER", "sql"),  # rank by cumulative points in manager's season
    ("manager_player_season_pct", "DOUBLE", "sql"),
    ("manager_player_alltime_rank", "INTEGER", "sql"),
    ("manager_player_alltime_pct", "DOUBLE", "sql"),
    ("optimal_lineup_position", "VARCHAR", "sql"),  # QB1, WR1, WR2 label
    # === PPG metrics (league-specific scoring) ===
    ("season_ppg", "DOUBLE", "sql"),
    ("season_games", "INTEGER", "sql"),
    ("alltime_ppg", "DOUBLE", "sql"),
    ("alltime_games", "INTEGER", "sql"),
    ("weighted_ppg", "DOUBLE", "sql"),
    ("ppg_trend", "DOUBLE", "sql"),
    ("consistency_score", "DOUBLE", "sql"),
    ("rolling_point_total", "DOUBLE", "sql"),
    # === LAMAR (base — always populated for rostered players) ===
    ("player_lamar", "DOUBLE", "sql"),
    ("manager_lamar", "DOUBLE", "sql"),
    ("bench_lamar", "DOUBLE", "sql"),
    ("replacement_ppg", "DOUBLE", "sql"),
    ("player_lamar_ytd", "DOUBLE", "sql"),
    ("manager_lamar_ytd", "DOUBLE", "sql"),
    ("bench_lamar_ytd", "DOUBLE", "sql"),
    # === Clutch equity (computed by clutch_to_player after playoff odds) ===
    ("clutch_equity", "DOUBLE", "sim"),
    ("starter_baseline_lamar", "DOUBLE", "sim"),
    ("above_baseline", "DOUBLE", "sim"),
    ("below_baseline", "DOUBLE", "sim"),
    ("odds_delta", "DOUBLE", "sim"),
    ("team_above_baseline", "DOUBLE", "sim"),
    ("team_below_baseline", "DOUBLE", "sim"),
    # === LAMAR flex variants (NULL if league has no FLEX/FLX slot) ===
    ("player_lamar_flx", "DOUBLE", "sql"),
    ("manager_lamar_flx", "DOUBLE", "sql"),
    ("bench_lamar_flx", "DOUBLE", "sql"),
    ("replacement_ppg_flx", "DOUBLE", "sql"),
    ("player_lamar_flx_ytd", "DOUBLE", "sql"),
    ("manager_lamar_flx_ytd", "DOUBLE", "sql"),
    ("bench_lamar_flx_ytd", "DOUBLE", "sql"),
    # === LAMAR rec_flex variants (NULL if league has no REC_FLEX/W/T slot) ===
    ("player_lamar_rec_flex", "DOUBLE", "sql"),
    ("manager_lamar_rec_flex", "DOUBLE", "sql"),
    ("bench_lamar_rec_flex", "DOUBLE", "sql"),
    ("replacement_ppg_rec_flex", "DOUBLE", "sql"),
    ("player_lamar_rec_flex_ytd", "DOUBLE", "sql"),
    ("manager_lamar_rec_flex_ytd", "DOUBLE", "sql"),
    ("bench_lamar_rec_flex_ytd", "DOUBLE", "sql"),
    # === LAMAR superflex variants (NULL if league has no SUPER_FLEX/Q/W/R/T slot) ===
    ("player_lamar_super_flex", "DOUBLE", "sql"),
    ("manager_lamar_super_flex", "DOUBLE", "sql"),
    ("bench_lamar_super_flex", "DOUBLE", "sql"),
    ("replacement_ppg_super_flex", "DOUBLE", "sql"),
    ("player_lamar_super_flex_ytd", "DOUBLE", "sql"),
    ("manager_lamar_super_flex_ytd", "DOUBLE", "sql"),
    ("bench_lamar_super_flex_ytd", "DOUBLE", "sql"),
    # === LAMAR IDP variants (NULL if league has no LB/DL/DB slots) ===
    ("player_lamar_idp", "DOUBLE", "sql"),
    ("manager_lamar_idp", "DOUBLE", "sql"),
    ("bench_lamar_idp", "DOUBLE", "sql"),
    ("replacement_ppg_idp", "DOUBLE", "sql"),
    ("player_lamar_idp_ytd", "DOUBLE", "sql"),
    ("manager_lamar_idp_ytd", "DOUBLE", "sql"),
    ("bench_lamar_idp_ytd", "DOUBLE", "sql"),
    # === LAMAR DB_LB flex variants (NULL if league has no DB_LB slot) ===
    ("player_lamar_db_lb", "DOUBLE", "sql"),
    ("manager_lamar_db_lb", "DOUBLE", "sql"),
    ("bench_lamar_db_lb", "DOUBLE", "sql"),
    ("replacement_ppg_db_lb", "DOUBLE", "sql"),
    ("player_lamar_db_lb_ytd", "DOUBLE", "sql"),
    ("manager_lamar_db_lb_ytd", "DOUBLE", "sql"),
    ("bench_lamar_db_lb_ytd", "DOUBLE", "sql"),
    # === LAMAR DL_LB flex variants (NULL if league has no DL_LB slot) ===
    ("player_lamar_dl_lb", "DOUBLE", "sql"),
    ("manager_lamar_dl_lb", "DOUBLE", "sql"),
    ("bench_lamar_dl_lb", "DOUBLE", "sql"),
    ("replacement_ppg_dl_lb", "DOUBLE", "sql"),
    ("player_lamar_dl_lb_ytd", "DOUBLE", "sql"),
    ("manager_lamar_dl_lb_ytd", "DOUBLE", "sql"),
    ("bench_lamar_dl_lb_ytd", "DOUBLE", "sql"),
    # === Keeper/dynasty context (NULL for redraft leagues) ===
    ("round", "INTEGER", "api"),
    ("pick", "INTEGER", "api"),
    ("cost", "DOUBLE", "api"),
    ("cost_bucket", "VARCHAR", "sql"),
    ("is_keeper", "INTEGER", "api"),
    ("keeper_price", "DOUBLE", "sql"),
    ("keeper_year", "INTEGER", "sql"),
    ("base_keeper_cost", "DOUBLE", "sql"),
    ("kept_next_year", "INTEGER", "sql"),
    ("draft_roi", "DOUBLE", "sql"),
    ("max_faab_bid_to_date", "DOUBLE", "sql"),
]

# Yahoo's individual roster endpoint can return the platform-native weekly
# player_stats block. Keep it in the canonical table so the scorer can rebuild
# Yahoo points from Yahoo stat IDs instead of losing that substrate at upload.
PLAYER_FANTASY_SCHEMA.extend(
    [
        ("yahoo_official_points", "DOUBLE", "api"),
        ("yahoo_stats_available", "BOOLEAN", "api"),
        *[(f"yahoo_stat_{stat_id}", "DOUBLE", "api") for stat_id in range(1, 85)],
    ]
)


# ---------------------------------------------------------------------------
# Convenience lookups
# ---------------------------------------------------------------------------

PLAYER_FANTASY_COLUMNS = {name for name, _, _ in PLAYER_FANTASY_SCHEMA}
COLUMN_TYPES = {name: dtype for name, dtype, _ in PLAYER_FANTASY_SCHEMA}
API_COLUMNS = [name for name, _, src in PLAYER_FANTASY_SCHEMA if src == "api"]
SQL_COLUMNS = [name for name, _, src in PLAYER_FANTASY_SCHEMA if src == "sql"]
ALL_COLUMNS = [name for name, _, _ in PLAYER_FANTASY_SCHEMA]


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------


def create_player_fantasy_table_sql(database_name: str) -> str:
    """Generate CREATE TABLE DDL for canonical player_fantasy table.

    All canonical columns are defined with correct types from the start.
    No ALTER TABLE needed during enrichments — columns exist as NULL.
    Safe to call on existing tables (IF NOT EXISTS).
    """
    cols = []
    for name, dtype, _ in PLAYER_FANTASY_SCHEMA:
        cols.append(f'    "{name}" {dtype} NOT NULL' if name == "db_name" else f'    "{name}" {dtype}')
    col_defs = ",\n".join(cols)
    return f"""
CREATE TABLE IF NOT EXISTS "{database_name}".public.player_fantasy (
{col_defs}
)
"""


def upload_player_fantasy(database_name: str, df, conn=None) -> bool:
    """Upload player_fantasy DataFrame through the active DuckDB connection.

    Creates table with canonical DDL if needed. Upserts by deleting
    existing year/week combos then inserting.

    Args:
        database_name: League database name
        df: Player fantasy DataFrame
        conn: Optional DuckDB connection

    Returns:
        True if upload succeeded
    """
    if df is None or len(df) == 0:
        return False
    df = df.copy()
    if "db_name" not in df.columns:
        df["db_name"] = database_name

    close_conn = False
    if conn is None:
        raise RuntimeError("conn is required - all pipeline work uses local DuckDB files.")

    try:
        # Create table with canonical DDL (all columns, correct types)
        conn.execute(create_player_fantasy_table_sql(database_name))

        # Register DataFrame and insert (only columns that exist in both)
        conn.register("_upload_pf", df)
        df_cols = set(df.columns)
        schema_cols = {name for name, _, _ in PLAYER_FANTASY_SCHEMA}
        insert_cols = ["db_name"] + sorted((df_cols & schema_cols) - {"db_name"})

        if not insert_cols:
            print("[WARN] No matching columns between DataFrame and schema")
            return False

        # Delete existing data for the years being uploaded
        if "year" in df.columns:
            years = df["year"].dropna().unique().tolist()
            if years:
                year_list = ", ".join(str(int(y)) for y in years)
                conn.execute(f'DELETE FROM "{database_name}".public.player_fantasy WHERE year IN ({year_list})')

        col_list = ", ".join(f'"{c}"' for c in insert_cols)
        conn.execute(
            f'INSERT INTO "{database_name}".public.player_fantasy ({col_list}) SELECT {col_list} FROM _upload_pf'
        )
        conn.unregister("_upload_pf")

        row_count = conn.execute(f'SELECT COUNT(*) FROM "{database_name}".public.player_fantasy').fetchone()[0]
        print(f"[UPLOAD] player_fantasy: {row_count:,} total rows")
        return True

    except Exception as e:
        print(f"[ERROR] player_fantasy upload failed: {e}")
        return False
    finally:
        if close_conn:
            conn.close()
