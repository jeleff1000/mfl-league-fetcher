from __future__ import annotations

from top10_extraction_sql import (
    draft_population_sql,
    light_draft_sql,
    light_matchup_sql,
    light_transaction_sql,
    matchup_active_sql,
    matchup_week_population_sql,
    transaction_population_sql,
)


def test_light_draft_extractor_keeps_sample_recomputable_facts_without_roster_scan() -> None:
    sql = " ".join(light_draft_sql(2025).split())

    assert "public.player_fantasy" not in sql
    assert "db_name" in sql
    assert "sum_adp_pick" in sql and "n_adp_pick" in sql
    assert "sum_auction_cost_pct" in sql and "n_auction_cost_pct" in sql
    assert "GROUP BY db_name, teams, roster, ppr, td" in sql


def test_light_transaction_extractor_retains_week_for_both_weekly_and_season_boards() -> None:
    sql = " ".join(light_transaction_sql(2025).split())

    assert "public.player_fantasy" not in sql
    assert "t.week" in sql
    assert "n_add_events" in sql and "n_drop_events" in sql
    assert "sum_faab_pct" in sql and "n_faab_pct" in sql
    assert "sum_transaction_score" in sql and "n_transaction_score" in sql
    assert "GROUP BY db_name, teams, roster, ppr, td" in sql


def test_population_queries_preserve_zero_event_leagues_and_draft_completeness() -> None:
    draft_sql = " ".join(draft_population_sql(2025).split())
    transaction_sql = " ".join(transaction_population_sql(2025).split())

    assert "z.picks >= 10*NULLIF(ls.num_teams,0)" in draft_sql
    assert "is_auction" in draft_sql
    assert "public.transactions" not in transaction_sql
    assert "k_eligible" in transaction_sql and "def_eligible" in transaction_sql


def test_matchup_population_is_week_resolved_and_player_activity_is_league_independent() -> None:
    population_sql = " ".join(matchup_week_population_sql(2025).split())
    active_sql = " ".join(matchup_active_sql(2025).split())

    assert "wk.week <= e.mw" in population_sql
    assert "is_champ_week" in population_sql
    assert "public.player_active_week" in active_sql
    assert "public.player_fantasy" not in active_sql


def test_light_matchup_extractor_deduplicates_without_a_window_sort() -> None:
    sql = " ".join(light_matchup_sql(2025).split())

    assert "public.player_fantasy" in sql
    assert "QUALIFY" not in sql
    assert "GROUP BY p.db_name,p.year,p.week,p.NFL_player_id" in sql
    assert "rostered_leagues" in sql and "started_leagues" in sql
    assert "wins_started" in sql and "losses_started" in sql
    assert "points_started" in sql and "clutch_sum" in sql
    assert "champ_started" in sql
