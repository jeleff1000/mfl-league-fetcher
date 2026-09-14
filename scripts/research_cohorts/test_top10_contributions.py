from __future__ import annotations

import pandas as pd
import pytest

import duckdb
from types import SimpleNamespace

from top10_contributions import (
    aggregate_sample,
    contribution_queries,
    contributions_from_wide,
    materialize_contributions,
    materialize_queries,
)
from top10_rank import BoardSpec


def _spec(
    *,
    dataset: str = "matchup",
    grain: str = "season",
    metric: str = "start_rate",
    support_metric: str = "eligible_leagues",
    common_pool: str = "all",
) -> BoardSpec:
    return BoardSpec(
        dataset=dataset,
        grain=grain,
        metric=metric,
        direction="desc",
        support_metric=support_metric,
        position_scope=("ALL", "QB"),
        common_pool=common_pool,
        source_metric=metric,
        ui_sortable=True,
    )


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    defaults = {
        "year": 2025,
        "week": None,
        "position": "QB",
        "aggregation": "ratio",
        "denominator_mode": "observed",
        "support": 0.0,
        "pool_support": 0.0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_ratio_aggregation_reproduces_start_rate_from_additive_facts() -> None:
    contributions = _frame(
        [
            {"dataset": "matchup", "grain": "season", "metric": "start_rate", "db_name": "l1", "NFL_player_id": "p1", "numerator": 1.0, "denominator": 4.0, "support": 4.0},
            {"dataset": "matchup", "grain": "season", "metric": "start_rate", "db_name": "l2", "NFL_player_id": "p1", "numerator": 2.0, "denominator": 4.0, "support": 4.0},
        ]
    )

    result = aggregate_sample(contributions, ("l1", "l2"), _spec(), year=2025)

    assert result.loc[0, "start_rate"] == pytest.approx(0.375)
    assert result.loc[0, "eligible_leagues"] == pytest.approx(8.0)


def test_metric_mean_uses_its_own_non_null_denominator() -> None:
    contributions = _frame(
        [
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l1", "NFL_player_id": "p1", "numerator": 10.0, "denominator": 1.0, "support": 1.0},
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l2", "NFL_player_id": "p1", "numerator": 30.0, "denominator": 1.0, "support": 1.0},
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l3", "NFL_player_id": "p1", "numerator": 0.0, "denominator": 0.0, "support": 0.0},
        ]
    )
    spec = _spec(dataset="draft", metric="adp", support_metric="n_drafted")

    result = aggregate_sample(contributions, ("l1", "l2", "l3"), spec, year=2025)

    assert result.loc[0, "adp"] == pytest.approx(20.0)
    assert result.loc[0, "n_drafted"] == pytest.approx(2.0)


def test_three_percent_common_pool_excludes_one_league_artifact() -> None:
    contributions = _frame(
        [
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l1", "NFL_player_id": "rare", "numerator": 1.0, "denominator": 1.0, "support": 1.0, "pool_support": 1.0, "denominator_mode": "sampled_leagues"},
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l1", "NFL_player_id": "common", "numerator": 10.0, "denominator": 1.0, "support": 1.0, "pool_support": 1.0, "denominator_mode": "sampled_leagues"},
            {"dataset": "draft", "grain": "season", "metric": "adp", "db_name": "l2", "NFL_player_id": "common", "numerator": 12.0, "denominator": 1.0, "support": 1.0, "pool_support": 1.0, "denominator_mode": "sampled_leagues"},
        ]
    )
    spec = _spec(dataset="draft", metric="adp", support_metric="n_drafted", common_pool="drafted_3pct")
    league_ids = tuple(f"l{i}" for i in range(1, 41))

    result = aggregate_sample(
        contributions,
        league_ids,
        spec,
        year=2025,
        eligible_by_position={"QB": 40},
    )

    assert result["NFL_player_id"].tolist() == ["common"]


def test_sampled_rate_divides_player_events_by_position_eligible_leagues() -> None:
    contributions = _frame(
        [
            {"dataset": "draft", "grain": "season", "metric": "draft_rate", "db_name": "l1", "NFL_player_id": "p1", "numerator": 1.0, "denominator": 0.0, "support": 1.0, "pool_support": 1.0, "aggregation": "sampled_rate"},
            {"dataset": "draft", "grain": "season", "metric": "draft_rate", "db_name": "l2", "NFL_player_id": "p1", "numerator": 1.0, "denominator": 0.0, "support": 1.0, "pool_support": 1.0, "aggregation": "sampled_rate"},
        ]
    )
    spec = _spec(
        dataset="draft",
        metric="draft_rate",
        support_metric="n_drafted",
        common_pool="drafted_3pct",
    )

    result = aggregate_sample(
        contributions,
        tuple(f"l{i}" for i in range(1, 41)),
        spec,
        year=2025,
        eligible_by_position={"QB": 40},
    )

    assert result.loc[0, "draft_rate"] == pytest.approx(0.05)


def test_sample_cannot_cross_year_boundary() -> None:
    contributions = _frame(
        [
            {"dataset": "matchup", "grain": "season", "metric": "start_rate", "db_name": "l1", "NFL_player_id": "p1", "numerator": 1.0, "denominator": 1.0},
            {"dataset": "matchup", "grain": "season", "metric": "start_rate", "db_name": "l2", "NFL_player_id": "p1", "numerator": 1.0, "denominator": 1.0, "year": 2024},
        ]
    )

    with pytest.raises(ValueError, match="single year"):
        aggregate_sample(contributions, ("l1", "l2"), _spec())


def test_sampled_league_units_count_unobserved_eligible_leagues() -> None:
    contributions = _frame(
        [
            {
                "dataset": "matchup", "grain": "season", "metric": "start_rate",
                "db_name": "l1", "NFL_player_id": "p1", "numerator": 2.0,
                "denominator": 0.0, "support": 0.0,
                "denominator_mode": "sampled_league_units",
            }
        ]
    )

    result = aggregate_sample(
        contributions,
        ("l1", "l2", "l3", "l4"),
        _spec(),
        year=2025,
        eligible_by_position={"QB": 4},
        eligibility_units_by_player={"p1": 5},
    )

    assert result.loc[0, "start_rate"] == pytest.approx(2 / 20)
    assert result.loc[0, "eligible_leagues"] == pytest.approx(4)


def test_sampled_league_denominator_counts_unobserved_eligible_leagues() -> None:
    contributions = _frame(
        [
            {
                "dataset": "transactions", "grain": "season", "metric": "add_rate",
                "db_name": "l1", "NFL_player_id": "p1", "numerator": 1.0,
                "denominator": 0.0, "support": 1.0,
                "denominator_mode": "sampled_leagues",
            }
        ]
    )
    spec = _spec(
        dataset="transactions", metric="add_rate", support_metric="n_add"
    )

    result = aggregate_sample(
        contributions,
        ("l1", "l2", "l3", "l4"),
        spec,
        year=2025,
        eligible_by_position={"QB": 4},
    )

    assert result.loc[0, "add_rate"] == pytest.approx(0.25)


def test_materialized_query_manifest_records_rows_and_content_hash(tmp_path) -> None:
    con = duckdb.connect()

    manifest = materialize_queries(
        con,
        {"fixture": "SELECT * FROM (VALUES ('l1', 2025, 3.0)) t(db_name, year, value)"},
        tmp_path,
    )

    artifact = tmp_path / "fixture.parquet"
    assert artifact.is_file()
    assert manifest["fixture"].rows == 1
    assert len(manifest["fixture"].sha256) == 64
    assert duckdb.connect().execute(
        "SELECT db_name, year, value FROM read_parquet(?)", [str(artifact)]
    ).fetchall() == [("l1", 2025, 3.0)]


def test_materialize_contributions_keeps_year_artifacts_separate(tmp_path) -> None:
    reader = SimpleNamespace(con=duckdb.connect())

    def queries(year: int, *, datasets: tuple[str, ...]) -> dict[str, str]:
        assert datasets == ("matchup",)
        return {
            f"matchup_season_{year}": (
                f"SELECT 'l{year}' db_name, {year}::INTEGER AS \"year\", 'p1' NFL_player_id"
            )
        }

    manifest = materialize_contributions(
        reader,
        years=(2024, 2025),
        out_path=tmp_path,
        datasets=("matchup",),
        query_builder=queries,
    )

    assert manifest.years == (2024, 2025)
    assert set(manifest.artifacts) == {"matchup_season_2024", "matchup_season_2025"}
    assert (tmp_path / "contribution_manifest.json").is_file()


@pytest.mark.parametrize(
    ("spec", "wide", "expected"),
    [
        (
            _spec(metric="start_rate", support_metric="eligible_leagues"),
            {
                "db_name": "m1", "year": 2025, "NFL_player_id": "p1", "position": "WR",
                "started_active_weeks": 3.0,
            },
            (3.0, 0.0, "sampled_league_units"),
        ),
        (
            _spec(dataset="draft", metric="adp", support_metric="n_drafted"),
            {
                "db_name": "d1", "year": 2025, "NFL_player_id": "p1", "position": "WR",
                "sum_adp_pick": 42.0, "n_adp_pick": 2.0, "n_drafted": 1.0,
            },
            (42.0, 2.0, "observed"),
        ),
        (
            _spec(dataset="transactions", metric="add_lamar", support_metric="n_add"),
            {
                "db_name": "t1", "year": 2025, "NFL_player_id": "p1", "position": "WR",
                "sum_add_lamar": 18.0, "n_add_lamar": 2.0, "n_add_leagues": 1.0,
            },
            (18.0, 2.0, "observed"),
        ),
    ],
)
def test_wide_production_facts_map_to_metric_contributions(spec, wide, expected) -> None:
    result = contributions_from_wide(pd.DataFrame([wide]), spec)

    assert result.loc[0, "numerator"] == pytest.approx(expected[0])
    assert result.loc[0, "denominator"] == pytest.approx(expected[1])
    assert result.loc[0, "denominator_mode"] == expected[2]
    assert result.loc[0, "metric"] == spec.metric


def test_draft_and_transaction_cache_queries_use_light_event_lanes() -> None:
    queries = contribution_queries(2025, datasets=("draft", "transactions"))

    assert set(queries) == {"draft_season_2025", "transactions_weekly_2025"}
    assert all("public.player_fantasy" not in sql for sql in queries.values())
