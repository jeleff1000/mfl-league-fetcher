import pandas as pd

from build_cohort_top10_pooled import metric_source

from build_cohort_decile_bootstrap import (
    bootstrap_required_leagues,
    player_required_leagues,
    metric_cell_columns,
)


def test_metric_cell_columns_describes_wide_aggregated_contract():
    columns = metric_cell_columns("start_pct")
    assert columns == {
        "numerator": "start_num",
        "observations": "start_obs",
        "sumsq": "start_sumsq",
    }


def test_top10_metric_source_extracts_wide_cell_without_long_metric_rows():
    cells = pd.DataFrame({
        "cohort_base": ["10t|flx|ppr|4pt", "10t|flx|ppr|4pt"],
        "db_name": ["league_a", "league_b"],
        "player": ["player_a", "player_a"],
        "year": [2025, 2025],
        "week": [1, 1],
        "roster_num": [1.0, 0.0],
        "roster_obs": [1.0, 1.0],
        "start_num": [0.0, 0.0],
        "start_obs": [1.0, 0.0],
    })
    source = metric_source(cells, "start_pct")
    assert list(source["value"]) == [0.0]
    assert list(source["db_name"]) == ["league_a"]
    assert "metric" not in source.columns


def test_bootstrap_required_leagues_is_cluster_based_and_deterministic():
    values = pd.Series([0.20, 0.40, 0.60, 0.80])
    first = bootstrap_required_leagues(values, 0.05, 0.85, reps=100, seed=7)
    second = bootstrap_required_leagues(values, 0.05, 0.85, reps=100, seed=7)
    assert first == second
    assert first[0] is not None and first[0] > 0
    assert first[1] is not None and first[1] > 0


def test_player_requirement_uses_each_players_cross_league_variance():
    values = pd.DataFrame({
        "player": ["baker", "baker", "baker", "ricard", "ricard"],
        "db_name": ["a", "b", "c", "a", "b"],
        "season_value": [0.20, 0.50, 0.80, 0.0, 0.0],
    })
    result = player_required_leagues(values, "season_value", 0.05, 0.85)
    assert set(result.player) == {"baker", "ricard"}
    assert result.loc[result.player == "baker", "required_leagues"].iloc[0] > result.loc[result.player == "ricard", "required_leagues"].iloc[0]
