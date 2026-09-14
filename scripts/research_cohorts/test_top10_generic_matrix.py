from __future__ import annotations

import pandas as pd
import pytest

from top10_generic_matrix import SparseResearchBoard


def _frame() -> pd.DataFrame:
    rows = []
    for league in ("l1", "l2", "l3", "l4"):
        for index in range(12):
            rows.append(
                {
                    "db_name": league,
                    "NFL_player_id": f"p{index:02d}",
                    "position": "QB" if index < 11 else "K",
                    "sum_value": float(12 - index) * (2 if league == "l1" else 1),
                    "n_value": 1.0,
                    "n_events": 1.0 if index < 11 else (1.0 if league == "l1" else 0.0),
                }
            )
    return pd.DataFrame(rows)


def test_ratio_board_sums_sample_facts_before_dividing() -> None:
    board = SparseResearchBoard.from_frame(
        _frame(), fact_columns=("sum_value", "n_value", "n_events")
    )

    rows = board.metric_rows(
        ("l1", "l2"), numerator="sum_value", denominator="n_value",
        support="n_events", position="QB",
    ).set_index("NFL_player_id")

    assert rows.loc["p00", "metric"] == pytest.approx(18.0)
    assert rows.loc["p00", "support"] == 2.0


def test_common_pool_is_recomputed_from_the_selected_leagues() -> None:
    board = SparseResearchBoard.from_frame(
        _frame(), fact_columns=("sum_value", "n_value", "n_events")
    )

    rows = board.metric_rows(
        ("l1", "l2", "l3", "l4"), numerator="sum_value", denominator="n_value",
        support="n_events", pool_support="n_events", position="ALL",
        minimum_pool_rate=0.30,
    )

    assert "p11" not in set(rows["NFL_player_id"])


def test_position_eligibility_controls_rate_denominator() -> None:
    eligibility = pd.DataFrame(
        [
            {"db_name": "l1", "position_group": "SKILL", "eligible": 1},
            {"db_name": "l2", "position_group": "SKILL", "eligible": 1},
            {"db_name": "l1", "position_group": "K", "eligible": 1},
            {"db_name": "l2", "position_group": "K", "eligible": 0},
        ]
    )
    board = SparseResearchBoard.from_frame(
        _frame().query("db_name in ['l1', 'l2']"),
        fact_columns=("sum_value", "n_value", "n_events"),
        eligibility=eligibility,
    )

    rows = board.metric_rows(
        ("l1", "l2"), numerator="n_events", denominator="eligible_leagues",
        support="n_events", position="K",
    )

    assert rows.iloc[0]["metric"] == pytest.approx(1.0)


def test_direct_player_values_still_use_sample_recomputed_pool_membership() -> None:
    board = SparseResearchBoard.from_frame(
        _frame(), fact_columns=("n_events",),
        direct_values=pd.DataFrame(
            [{"NFL_player_id": f"p{index:02d}", "canon": float(index)} for index in range(12)]
        ),
    )

    top = board.ordered_top10(
        ("l1", "l2", "l3", "l4"), numerator="canon", denominator="direct",
        support="n_events", pool_support="n_events", position="QB",
        minimum_pool_rate=0.03, direction="desc",
    )

    assert top[:3] == ("p10", "p09", "p08")


def test_unknown_and_duplicate_sample_leagues_fail() -> None:
    board = SparseResearchBoard.from_frame(_frame(), fact_columns=("n_events",))
    with pytest.raises(ValueError, match="unknown"):
        board.metric_rows(("missing",), numerator="n_events", denominator="eligible_leagues",
                          support="n_events", position="ALL")
    with pytest.raises(ValueError, match="duplicates"):
        board.metric_rows(("l1", "l1"), numerator="n_events", denominator="eligible_leagues",
                          support="n_events", position="ALL")


def test_subset_leagues_preserves_values_and_direct_metrics() -> None:
    board = SparseResearchBoard.from_frame(
        _frame(), fact_columns=("sum_value", "n_value", "n_events"),
        direct_values=pd.DataFrame(
            [{"NFL_player_id": f"p{index:02d}", "canon": float(index)} for index in range(12)]
        ),
    )

    subset = board.subset_leagues(("l3", "l1"))

    assert subset.leagues == ("l3", "l1")
    kwargs = dict(
        numerator="canon", denominator="direct", support="n_events",
        position="QB", direction="desc",
    )
    assert subset.ordered_top10(subset.leagues, **kwargs) == board.ordered_top10(
        subset.leagues, **kwargs
    )


def test_batched_top10_matches_scalar_boards() -> None:
    board = SparseResearchBoard.from_frame(
        _frame(), fact_columns=("sum_value", "n_value", "n_events")
    )
    samples = (("l1", "l2"), ("l3", "l4"))
    kwargs = dict(
        numerator="sum_value", denominator="n_value", support="n_events",
        pool_support="n_events", minimum_pool_rate=0.03, position="QB", direction="desc",
    )

    batched = board.batched_top10(samples, **kwargs)

    assert batched == tuple(board.ordered_top10(sample, **kwargs) for sample in samples)
