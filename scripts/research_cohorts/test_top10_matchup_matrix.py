from __future__ import annotations

import pandas as pd
import pytest

from top10_matchup_matrix import MatchupSparseBoard


def _frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    players = [f"p{index:02d}" for index in range(12)]
    weekly_rows: list[dict[str, object]] = []
    for league in ("l1", "l2"):
        for week in (1, 2):
            if league == "l2" and week == 2:
                continue
            for index, player in enumerate(players):
                started = int(index == 0 or (index == 1 and league == "l1" and week == 1))
                weekly_rows.append(
                    {
                        "db_name": league,
                        "week": week,
                        "NFL_player_id": player,
                        "position": "QB",
                        "rostered_leagues": 1,
                        "started_leagues": started,
                        "wins_started": started if league == "l1" else 0,
                        "losses_started": started if league == "l2" else 0,
                        "points_started": float(started * (20 - index)),
                        "clutch_sum": float(started * (12 - index)),
                        "champ_started": int(started and week == 2),
                    }
                )
    eligibility = pd.DataFrame(
        [
            {"db_name": "l1", "week": 1, "pos_grp": "SKILL", "eligible": 1},
            {"db_name": "l2", "week": 1, "pos_grp": "SKILL", "eligible": 1},
            {"db_name": "l1", "week": 2, "pos_grp": "SKILL", "eligible": 1},
        ]
    )
    active = pd.DataFrame(
        [
            {"NFL_player_id": player, "week": week}
            for player in players
            for week in (1, 2)
        ]
    )
    return pd.DataFrame(weekly_rows), eligibility, active


def test_weekly_start_rate_orders_metric_then_support_then_player_id() -> None:
    board = MatchupSparseBoard.from_frames(*_frames())

    result = board.ordered_top10(
        ("l1", "l2"), grain="weekly", metric="start_rate", position="QB", week=1
    )

    assert result == ("p00", "p01", "p02", "p03", "p04", "p05", "p06", "p07", "p08", "p09")


def test_season_start_rate_averages_weekly_shares_over_active_weeks() -> None:
    board = MatchupSparseBoard.from_frames(*_frames())

    values = board.metric_rows(
        ("l1", "l2"), grain="season", metric="start_rate", position="QB"
    ).set_index("NFL_player_id")

    assert values.loc["p00", "metric"] == pytest.approx(1.0)
    assert values.loc["p01", "metric"] == pytest.approx(0.25)
    assert values.loc["p00", "support"] == pytest.approx(3.0)


def test_won_rate_is_start_share_times_decided_win_rate() -> None:
    board = MatchupSparseBoard.from_frames(*_frames())

    values = board.metric_rows(
        ("l1", "l2"), grain="weekly", metric="win_rate", position="QB", week=1
    ).set_index("NFL_player_id")

    assert values.loc["p00", "metric"] == pytest.approx(0.5)
    assert values.loc["p01", "metric"] == pytest.approx(0.5)


def test_sample_rejects_unknown_or_duplicate_leagues() -> None:
    board = MatchupSparseBoard.from_frames(*_frames())

    with pytest.raises(ValueError, match="unknown leagues"):
        board.ordered_top10(("missing",), grain="season", metric="start_rate", position="QB")
    with pytest.raises(ValueError, match="duplicates"):
        board.ordered_top10(("l1", "l1"), grain="season", metric="start_rate", position="QB")


def test_subset_leagues_preserves_board_values_and_rejects_unknowns() -> None:
    board = MatchupSparseBoard.from_frames(*_frames())

    subset = board.subset_leagues(("l2",))

    assert subset.leagues == ("l2",)
    assert subset.ordered_top10(
        ("l2",), grain="season", metric="start_rate", position="QB"
    ) == board.ordered_top10(
        ("l2",), grain="season", metric="start_rate", position="QB"
    )
    with pytest.raises(ValueError, match="unknown leagues"):
        board.subset_leagues(("missing",))


@pytest.mark.parametrize(
    ("grain", "metric", "week"),
    [
        ("weekly", "start_rate", 1),
        ("weekly", "win_rate", 1),
        ("season", "start_rate", None),
        ("season", "clutch", None),
        ("season", "ppg", None),
        ("season", "points", None),
        ("season", "champ_started", None),
        ("season", "started_weeks", None),
        ("season", "active_weeks", None),
    ],
)
def test_batched_top10_matches_scalar_boards(
    grain: str, metric: str, week: int | None
) -> None:
    board = MatchupSparseBoard.from_frames(*_frames())
    samples = (("l1",), ("l2",), ("l1", "l2"))

    expected = tuple(
        board.ordered_top10(
            sample, grain=grain, metric=metric, position="QB", week=week
        )
        for sample in samples
    )

    assert board.batched_top10(
        samples, grain=grain, metric=metric, position="QB", week=week
    ) == expected


def test_parquet_shard_loader_reproduces_in_memory_board(tmp_path) -> None:
    weekly, eligibility, active = _frames()
    weekly_paths = []
    population_paths = []
    for index, league in enumerate(("l1", "l2")):
        weekly_path = tmp_path / f"weekly_{index}.parquet"
        weekly.loc[weekly.db_name.eq(league)].to_parquet(weekly_path, index=False)
        weekly_paths.append(weekly_path)

        group = eligibility.loc[eligibility.db_name.eq(league)]
        population = (
            group.pivot(index=["db_name", "week"], columns="pos_grp", values="eligible")
            .reset_index()
            .rename(columns={"SKILL": "skill_eligible", "K": "k_eligible", "DEF": "def_eligible"})
        )
        for column in ("skill_eligible", "k_eligible", "def_eligible"):
            if column not in population:
                population[column] = 0
        population_path = tmp_path / f"population_{index}.parquet"
        population.to_parquet(population_path, index=False)
        population_paths.append(population_path)

    active_path = tmp_path / "active.parquet"
    active.to_parquet(active_path, index=False)

    expected = MatchupSparseBoard.from_frames(weekly, eligibility, active)
    actual = MatchupSparseBoard.from_parquet_shards(
        weekly_paths, population_paths, (active_path, active_path)
    )

    assert actual.leagues == expected.leagues
    assert actual.players == expected.players
    assert actual.weeks == expected.weeks
    for metric in ("start_rate", "win_rate", "clutch", "started_weeks"):
        assert actual.ordered_top10(
            ("l1", "l2"), grain="season", metric=metric, position="QB"
        ) == expected.ordered_top10(
            ("l1", "l2"), grain="season", metric=metric, position="QB"
        )


def test_canonical_matchup_values_keep_sample_recomputed_player_membership() -> None:
    weekly, eligibility, active = _frames()
    players = sorted(weekly.NFL_player_id.unique())
    direct = pd.DataFrame(
        {
            "NFL_player_id": players,
            "canonical_points": list(range(len(players))),
            "canonical_ppg": list(range(len(players))),
        }
    )
    board = MatchupSparseBoard.from_frames(
        weekly, eligibility, active
    ).with_direct_values(direct)
    samples = (("l1",), ("l2",), ("l1", "l2"))

    for metric in ("canonical_points", "canonical_ppg"):
        expected = tuple(
            board.ordered_top10(
                sample, grain="season", metric=metric, position="QB"
            )
            for sample in samples
        )
        assert board.batched_top10(
            samples, grain="season", metric=metric, position="QB"
        ) == expected
