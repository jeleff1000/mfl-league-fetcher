from pathlib import Path

from direct_fly_matchup_rollup import (
    build_matchup_base_sql,
    build_narrow_weekly_activity_sql,
    build_narrow_season_table_sql,
    build_matchup_rollup_sql,
    required_matchup_bundle_tables,
)


def test_weekly_activity_projection_is_narrow():
    sql = build_narrow_weekly_activity_sql(2020, 2025).lower()
    assert "select *" not in sql
    assert "nfl_player_id" in sql
    assert "year" in sql and "week" in sql
    assert "season_type" in sql


def test_season_projection_contains_position_scoring_and_games():
    sql = build_narrow_season_table_sql(2020, 2025).lower()
    assert "select *" not in sql
    assert "nfl_player_id" in sql
    assert "games_played" in sql
    assert "position" in sql
    assert "fpts_4pt_half" in sql


def test_matchup_base_uses_cached_outcome_fields_without_lamar_recalculation():
    sql = build_matchup_base_sql(2020, 2025).lower()
    assert "manager_lamar" not in sql
    assert "player_fantasy" in sql
    assert "player_nfl_season" not in sql


def test_rollup_is_population_aggregated_not_league_sharded():
    sql = build_matchup_rollup_sql(2025, 2025).lower()
    assert "count(distinct db_name)" in sql
    assert "n_leagues" in sql
    assert "group by db_name" not in sql
    assert "manager_lamar" not in sql
    assert "is_playoffs" in sql
    assert "team_made_playoffs" in sql
    assert "final_playoff_seed" in sql
    assert "is_championship" in sql
    assert "group by" in sql


def test_bundle_contract_contains_all_matchup_grains():
    names = required_matchup_bundle_tables()
    assert {"research_matchup", "research_matchup_weekly", "research_matchup_career"} <= set(names)
    assert {
        "research_matchup_adaptive",
        "research_matchup_adaptive_weekly",
        "research_matchup_adaptive_career",
    } <= set(names)


def test_release_workflow_will_have_no_shard_inputs_after_cutover():
    workflow = Path(__file__).parents[2] / ".github" / "workflows" / "research_matchup_release.yml"
    text = workflow.read_text(encoding="utf-8")
    assert "build_bounded_matchup_cache.py" in text
    assert "old_shard_run" not in text
    assert "modern_shard_run" not in text
    assert "rescue_run" not in text
    assert "actions/download-artifact" not in text


def test_direct_builder_is_cache_to_narrow_adaptive_surface():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "ATTACH" in source and "AS lake (READ_ONLY)" in source
    assert "AS ops (READ_ONLY)" in source
    assert "CREATE TABLE research_matchup_adaptive_weekly" in source
    assert "q_league_type" in source and "q_lineup_mode" in source
    assert "actions/download-artifact" not in source


def test_direct_builder_fans_out_position_from_ops_player_week_cache():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "ops.nfl_historical.nfl_player_stats_all" in lower
    assert "position_fanout" in lower
    assert "f.player_id_key=cast(p.nfl_player_id as varchar)" in lower
    assert "f.year=p.year" in lower
    assert "f.week=p.week" in lower
    assert "f.position" in lower
    assert "left join position_fanout f" in lower
    assert "f.position as position" in lower


def test_bounded_builder_materializes_one_narrow_ops_snapshot_per_year():
    source = Path(__file__).with_name("build_bounded_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "materialize_ops_year_snapshot" in lower
    assert "ops_snapshot" in lower
    assert '"--ops-cache", str(ops_snapshot)' in source


def test_position_fanout_keeps_postseason_player_weeks():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    fanout = source.lower().split("position_fanout as", 1)[1].split("population_inventory as", 1)[0]
    assert "season_type, 'reg'" not in fanout
    assert "week is not null" in source.lower().split("create temp table position_fanout_base", 1)[1].split("create temp table player_fantasy_base", 1)[0]


def test_year_snapshot_does_not_copy_the_wide_player_table():
    source = Path(__file__).with_name("build_bounded_matchup_cache.py").read_text(encoding="utf-8").lower()
    assert "create table public.player_fantasy as\n          select *" not in source


def test_direct_builder_supports_narrow_player_and_week_pilots():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "player_id_filter" in source
    assert "week_filter" in source
    assert "--player-id" in source
    assert "--week" in source
    bounded = Path(__file__).with_name("build_bounded_matchup_cache.py").read_text(encoding="utf-8")
    assert 'parser.add_argument("--player-id")' in bounded
    assert 'parser.add_argument("--week", type=int)' in bounded


def test_position_pilots_filter_base_rows_before_metric_rollup():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8").lower()
    assert "create temp table position_fanout_base" in source
    assert "from position_fanout_base" in source
    assert "{position_filter.replace('p.position', 'f.position')}" in source
    assert 'position_group == "RB"' in source
    assert 'position_group in ("IDP", "WR")' in source


def test_direct_builder_preserves_null_roster_rows_and_all_idp_slot_lanes():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER)=1" in source
    assert "cohort_position_eligible" in source
    assert "position_eligible = 1" in source


def test_direct_builder_normalizes_nfl_player_ids_before_rollups():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "CAST(NFL_player_id AS VARCHAR) AS player_id_key" in source
    assert "CAST(p.NFL_player_id AS VARCHAR) AS player_id_key" in source


def test_weekly_win_rate_uses_started_games_denominator():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "100.0*wins_started/NULLIF(valid_outcome_started_leagues,0) AS win_rate_pct" in source
    assert "100.0*wins_started/NULLIF(n_leagues,0) AS win_rate_pct" not in source


def test_unpaired_starts_stay_in_start_rate_but_not_win_denominator():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1) AS started_leagues" in source
    assert "COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND (p.outcome_win=1 OR p.outcome_loss=1 OR p.outcome_tie=1)) AS valid_outcome_started_leagues" in source
    assert "100.0*started_leagues/NULLIF(n_leagues,0) AS start_rate_pct" in source


def test_weekly_expected_wins_is_start_rate_times_win_rate():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "wins_started/NULLIF(n_leagues,0) * started_leagues/NULLIF(valid_outcome_started_leagues,0) AS expected_wins" in source
    assert "CAST(wins_started AS DOUBLE) AS expected_wins" not in source


def test_expected_losses_are_the_complement_of_started_win_rate():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "(started_leagues/NULLIF(n_leagues,0)) * (1.0-wins_started/NULLIF(valid_outcome_started_leagues,0)) AS expected_losses" in source
    assert "expected_starts*(1.0-wins_total/NULLIF(valid_outcome_started_total,0)) AS expected_losses" in source
    assert "losses_started/NULLIF(n_leagues,0) AS expected_losses" not in source


def test_season_start_rates_separate_all_weeks_from_active_weeks():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "SUM(w.n_leagues) FILTER (WHERE w.bye_week=0) AS eligible_league_weeks" in source
    assert "SUM(w.n_leagues) FILTER (WHERE w.active_week=1 AND w.bye_week=0) AS healthy_eligible_league_weeks" in source
    assert "100.0*started_total/NULLIF(eligible_league_weeks,0) AS start_rate_pct" in source
    assert "100.0*healthy_started_rows/NULLIF(healthy_eligible_league_weeks,0) AS healthy_start_rate_pct" in source
    assert "(started_total/NULLIF(eligible_league_weeks,0))*active_weeks AS expected_starts" in source


def test_career_playoff_and_championship_rates_average_season_rates():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "AVG(playoff_as_starter_pct) AS playoff_as_starter_pct" in source
    assert "AVG(champ_as_starter_pct) AS champ_as_starter_pct" in source
    assert "champ_as_starter_pct,playoff_as_starter_pct" in source
    assert "SUM(champ_started)*100.0/NULLIF(SUM(champ_eligible_leagues),0) AS champ_as_starter_pct" not in source
    assert "SUM(playoff_started)*100.0/NULLIF(SUM(playoff_eligible_leagues),0) AS playoff_as_starter_pct" not in source


def test_season_rollup_excludes_player_byes_from_week_populations():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "team_game_week" in source
    assert "active_id" in source
    assert "LEFT JOIN LATERAL" not in source
    assert "bye_week" in source
    assert "SUM(w.n_leagues) FILTER (WHERE w.bye_week=0) AS eligible_league_weeks" in source
    assert "SUM(w.n_leagues) FILTER (WHERE w.active_week=1 AND w.bye_week=0) AS healthy_eligible_league_weeks" in source
    assert "COUNT(DISTINCT p.week) FILTER (WHERE p.active_id IS NULL AND p.bye_week=0) AS inactive_weeks" in source


def test_season_expected_outcomes_sum_weekly_expected_outcomes():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "SUM(w.wins_started/NULLIF(w.n_leagues,0)) FILTER (WHERE w.bye_week=0) AS expected_wins_total" in source
    assert "SUM(w.losses_started/NULLIF(w.n_leagues,0)) FILTER (WHERE w.bye_week=0) AS expected_losses_total" in source
    assert "expected_starts*wins_total/NULLIF(valid_outcome_started_total,0) AS expected_wins" in source
    assert "expected_starts*(1.0-wins_total/NULLIF(valid_outcome_started_total,0)) AS expected_losses" in source


def test_season_clutch_is_the_weekly_clutch_sum_and_playoff_rates_use_all_eligible_leagues():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "SUM(w.clutch_sum/NULLIF(w.started_leagues,0)) FILTER (WHERE w.bye_week=0) AS clutch_sum" in source
    assert "SUM(clutch_sum) AS avg_clutch_started" in source
    assert "SUM(w.clutch_sum) FILTER (WHERE w.bye_week=0) AS clutch_sum" not in source
    assert "100.0*champ_started_leagues/NULLIF(n_leagues,0) AS champ_as_starter_pct" in source
    assert "100.0*playoff_started_leagues/NULLIF(n_leagues,0) AS playoff_as_starter_pct" in source


def test_direct_builder_inventory_comes_from_full_player_population():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "population_inventory as" in lower
    assert "from lake.public.player_fantasy" in lower
    assert "select distinct" in lower and "cast(p.year as integer) as year" in lower
    inventory = lower.split("population_inventory as", 1)[1].split("eligible as", 1)[0]
    assert "from settings" not in inventory
    assert "cohort_teams" in inventory
    for column in ("week", "teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode"):
        assert column in inventory


def test_direct_builder_has_position_inventory_for_denominators():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "position_inventory as" in lower
    assert "position_eligible" in lower
    assert "count(distinct db_name)" in lower
    assert "where position_eligible = 1" in lower


def test_direct_builder_applies_season_and_weekly_pool_thresholds():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "season_pooling as" in lower
    assert "weekly_pooling as" in lower
    assert "season_pool_threshold" in lower or "50" in lower
    assert "weekly_pool_threshold" in lower or "150" in lower
    assert "effective_teams" in lower
    assert "effective_bracket" in lower


def test_season_metrics_use_season_grain_population_not_weekly_pool_keys():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    assert "season_weekly_exact AS" in source
    assert "FROM season_weekly_exact w" in source


def test_bracket_does_not_partition_normal_opportunity_denominators():
    source = Path(__file__).with_name("build_direct_matchup_cache.py").read_text(encoding="utf-8")
    lower = source.lower()
    assert "weekly_ordinary as" in lower
    assert "season_ordinary as" in lower
    assert "bracket-specific playoff fields" in lower
