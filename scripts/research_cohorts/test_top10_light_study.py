from __future__ import annotations

import pandas as pd

from top10_light_study import LIGHT_METRICS, load_light_board, supported_candidates


def test_supported_candidates_include_exact_half_population_ceiling() -> None:
    assert supported_candidates(525)[-2:] == (200, 262)
    assert supported_candidates(10) == (5,)


def test_light_metric_contract_covers_recomputable_behavior_columns() -> None:
    keys = {(metric.dataset, metric.grain, metric.metric) for metric in LIGHT_METRICS}

    assert ("draft", "season", "adp") in keys
    assert ("draft", "season", "draft_rate") in keys
    assert ("draft", "season", "cost") in keys
    assert ("transactions", "season", "add_rate") in keys
    assert ("transactions", "season", "faab") in keys


def test_light_board_accepts_production_canonical_direct_values(tmp_path) -> None:
    players = [f"p{index:02d}" for index in range(12)]
    pd.DataFrame(
        [
            {
                "db_name": "l1",
                "skill_eligible": 1,
                "k_eligible": 1,
                "def_eligible": 1,
            }
        ]
    ).to_parquet(tmp_path / "draft_population_2025.parquet", index=False)
    pd.DataFrame(
        [
            {
                "db_name": "l1",
                "NFL_player_id": player,
                "position": "QB",
                "sum_adp_pick": index + 1,
                "n_adp_pick": 1,
                "n_drafted": 1,
                "sum_auction_cost_pct": 0,
                "n_auction_cost_pct": 0,
            }
            for index, player in enumerate(players)
        ]
    ).to_parquet(tmp_path / "draft_season_2025.parquet", index=False)
    direct = pd.DataFrame(
        {
            "NFL_player_id": players,
            "canonical_points": list(range(12)),
        }
    )

    board = load_light_board(
        tmp_path, dataset="draft", year=2025, direct_values=direct
    )

    assert board.ordered_top10(
        ("l1",),
        numerator="canonical_points",
        denominator="direct",
        support="n_drafted",
        pool_support="n_drafted",
        minimum_pool_rate=0.03,
        position="ALL",
    ) == tuple(reversed(players[2:]))
