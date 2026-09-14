from __future__ import annotations

import json

import pytest

from top10_rank import BoardSpec, load_board_specs, ordered_top10, top10_rank_rho


DESC_SPEC = BoardSpec(
    dataset="matchup",
    grain="season",
    metric="start_rate",
    direction="desc",
    support_metric="eligible_leagues",
    position_scope=("ALL",),
    common_pool="all",
    source_metric="start_rate_pct",
    ui_sortable=True,
)


def test_exact_order_has_perfect_rank_correlation() -> None:
    board = tuple(f"p{i}" for i in range(1, 11))

    assert top10_rank_rho(board, board) == pytest.approx(1.0)


def test_membership_change_reduces_rank_correlation() -> None:
    left = tuple(f"p{i}" for i in range(1, 11))
    right = (*left[:9], "p11")

    assert top10_rank_rho(left, right) < 1.0


def test_reversing_top_ten_has_negative_rank_correlation() -> None:
    left = tuple(f"p{i}" for i in range(1, 11))

    assert top10_rank_rho(left, tuple(reversed(left))) == pytest.approx(-1.0)


def test_absent_rank_magnitude_does_not_change_tie_aware_spearman() -> None:
    left = tuple(f"p{i}" for i in range(1, 11))
    right = (*left[:8], "p11", "p12")

    assert top10_rank_rho(left, right, absent_rank=11) == pytest.approx(
        top10_rank_rho(left, right, absent_rank=20)
    )


def test_ordered_top_ten_breaks_metric_ties_by_support_then_player_id() -> None:
    rows = [
        {"NFL_player_id": "b", "start_rate": 10.0, "eligible_leagues": 4},
        {"NFL_player_id": "a", "start_rate": 10.0, "eligible_leagues": 4},
        {"NFL_player_id": "c", "start_rate": 10.0, "eligible_leagues": 5},
        {"NFL_player_id": "ignored-null", "start_rate": None, "eligible_leagues": 99},
    ]

    assert ordered_top10(rows, DESC_SPEC) == ("c", "a", "b")


def test_ordered_top_ten_honors_ascending_metric_direction() -> None:
    spec = BoardSpec(**{**DESC_SPEC.__dict__, "direction": "asc"})
    rows = [
        {"NFL_player_id": "late", "start_rate": 20.0, "eligible_leagues": 10},
        {"NFL_player_id": "early", "start_rate": 10.0, "eligible_leagues": 10},
    ]

    assert ordered_top10(rows, spec) == ("early", "late")


def test_duplicate_player_ids_are_rejected() -> None:
    board = tuple(["p1", "p1", *[f"p{i}" for i in range(2, 10)]])

    with pytest.raises(ValueError, match="duplicate player IDs"):
        top10_rank_rho(board, tuple(dict.fromkeys(board)))


def test_contract_loader_rejects_duplicate_board_identity(tmp_path) -> None:
    payload = {
        "version": 1,
        "boards": [DESC_SPEC.to_dict(), DESC_SPEC.to_dict()],
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate board spec"):
        load_board_specs(path)


def test_contract_loader_expands_group_defaults(tmp_path) -> None:
    payload = {
        "version": 1,
        "groups": [
            {
                "dataset": "matchup",
                "grain": "season",
                "direction": "desc",
                "position_scope": ["ALL", "QB"],
                "common_pool": "all",
                "metrics": [
                    {
                        "metric": "start_rate",
                        "support_metric": "eligible_leagues",
                        "source_metric": "start_rate_pct",
                        "ui_sortable": True,
                    },
                    {
                        "metric": "clutch",
                        "support_metric": "started_weeks",
                        "source_metric": "avg_clutch_started",
                        "ui_sortable": True,
                    },
                ],
            }
        ],
    }
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    specs = load_board_specs(path)

    assert [spec.metric for spec in specs] == ["start_rate", "clutch"]
    assert specs[1].position_scope == ("ALL", "QB")
