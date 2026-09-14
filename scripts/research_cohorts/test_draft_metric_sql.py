from __future__ import annotations

import duckdb

from draft_metric_sql import auction_bid_pct_sql, auction_cost_select_sql


def evaluate(avg_cost: float | None, drafted: int, eligible: int) -> tuple:
    con = duckdb.connect()
    try:
        sql = auction_cost_select_sql("avg_cost", "drafted", "eligible")
        return con.execute(
            f"SELECT {sql} FROM (SELECT ?::DOUBLE avg_cost, ?::INTEGER drafted, ?::INTEGER eligible)",
            [avg_cost, drafted, eligible],
        ).fetchone()
    finally:
        con.close()


def test_auction_cost_is_conditional_cost_times_auction_draft_rate() -> None:
    rate, cost = evaluate(40.0, drafted=2, eligible=4)

    assert rate == 50.0
    assert cost == 20.0


def test_auction_cost_is_null_when_no_auction_leagues_are_eligible() -> None:
    rate, cost = evaluate(40.0, drafted=0, eligible=0)

    assert rate is None
    assert cost is None


def test_undrafted_player_has_zero_rate_adjusted_cost() -> None:
    rate, cost = evaluate(None, drafted=0, eligible=5)

    assert rate == 0.0
    assert cost == 0.0


def test_auction_rate_and_cost_cannot_exceed_conditional_cost() -> None:
    rate, cost = evaluate(40.0, drafted=4, eligible=3)

    assert rate == 100.0
    assert cost == 40.0


def test_auction_bid_share_is_bounded_to_the_total_budget() -> None:
    con = duckdb.connect()
    try:
        value = con.execute(
            f"SELECT {auction_bid_pct_sql('bid_value', 'budget_value')} "
            "FROM (SELECT 140::DOUBLE bid_value, 130::DOUBLE budget_value)"
        ).fetchone()[0]
    finally:
        con.close()

    assert value == 100.0
