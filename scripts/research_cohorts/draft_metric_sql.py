"""Reusable SQL fragments for draft cohort metrics."""
from __future__ import annotations


def auction_bid_pct_sql(cost_column: str, budget_column: str) -> str:
    """Return a bid's share of its league budget, bounded to a valid percentage."""
    return f"LEAST(100.0, 100.0 * {cost_column} / NULLIF({budget_column}, 0))"


def auction_cost_select_sql(
    average_cost_column: str,
    drafted_auction_leagues_column: str,
    eligible_auction_leagues_column: str,
) -> str:
    """Return auction draft rate and unconditional rate-adjusted cost expressions."""
    rate = (
        f"100.0 * LEAST({drafted_auction_leagues_column}, {eligible_auction_leagues_column}) / "
        f"NULLIF({eligible_auction_leagues_column}, 0)"
    )
    return (
        f"ROUND({rate}, 2) AS auction_draft_rate_pct, "
        f"ROUND(COALESCE({average_cost_column}, 0) * ({rate}) / 100.0, 2) AS cost_pct"
    )
