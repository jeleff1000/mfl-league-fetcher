from __future__ import annotations

import pytest

from market_adp import (
    MarketAdpRow,
    expand_market_only_players,
    normalize_yahoo_draft_rate,
    select_market_adp,
)


def row(
    *,
    year: int = 2002,
    source: str,
    market_format: str,
    teams: int | None,
    adp: float,
    times_drafted: int | None,
    player_id: str = "p1",
) -> MarketAdpRow:
    return MarketAdpRow(
        year=year,
        source=source,
        market_format=market_format,
        teams=teams,
        NFL_player_id=player_id,
        adp=adp,
        stdev=None,
        times_drafted=times_drafted,
        pct_drafted=None,
        source_player_id=None,
        match_basis="player_bridge",
    )


def test_market_row_preserves_pre_2005_yahoo_player_season() -> None:
    observation = row(
        source="historical",
        market_format="blind",
        teams=None,
        adp=17.5,
        times_drafted=400,
    )

    assert observation.year == 2002
    assert observation.NFL_player_id == "p1"


def test_yahoo_fractional_percent_drafted_becomes_display_percent() -> None:
    assert normalize_yahoo_draft_rate(1.0) == 100.0
    assert normalize_yahoo_draft_rate(0.86) == 86.0
    assert normalize_yahoo_draft_rate(None) is None


def test_exact_format_and_team_consensus_precedes_broader_sources() -> None:
    selected = select_market_adp(
        [
            row(source="ffc", market_format="ppr", teams=12, adp=20.0, times_drafted=100),
            row(source="mfl", market_format="ppr", teams=12, adp=30.0, times_drafted=300),
            row(source="yahoo", market_format="blind", teams=None, adp=5.0, times_drafted=10_000),
        ],
        teams=12,
        market_format="ppr",
    )

    assert selected is not None
    assert selected.adp == pytest.approx(27.5)
    assert selected.basis == "external_exact_consensus"
    assert selected.effective_n == 400
    assert selected.sources == ("ffc", "mfl")


def test_format_lane_precedes_blind_fallback() -> None:
    selected = select_market_adp(
        [
            row(source="ffc", market_format="ppr", teams=10, adp=24.0, times_drafted=50),
            row(source="yahoo", market_format="blind", teams=None, adp=11.0, times_drafted=500),
        ],
        teams=12,
        market_format="ppr",
    )

    assert selected is not None
    assert selected.adp == 24.0
    assert selected.basis == "external_format_consensus"


def test_null_sample_sources_use_equal_weight_without_becoming_zero_evidence() -> None:
    selected = select_market_adp(
        [
            row(source="a", market_format="blind", teams=None, adp=10.0, times_drafted=None),
            row(source="b", market_format="blind", teams=None, adp=20.0, times_drafted=None),
        ],
        teams=12,
        market_format="ppr",
    )

    assert selected is not None
    assert selected.adp == 15.0
    assert selected.effective_n == 2
    assert selected.basis == "external_blind_consensus"


def test_external_only_players_are_added_to_every_compatible_cohort() -> None:
    existing = [{
        "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt", "year": 2002,
        "NFL_player_id": "native", "pos_grp": "SKILL", "n_drafted": 4,
    }]
    denoms = [{
        "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt", "year": 2002,
        "pos_grp": "SKILL", "n_leagues": 10, "n_auction_eligible_leagues": 2,
    }]
    market = [
        {"year": 2002, "market_format": "ppr", "teams": 12, "NFL_player_id": "native", "position": "RB", "adp": 8.0},
        {"year": 2002, "market_format": "ppr", "teams": 12, "NFL_player_id": "external", "position": "WR", "adp": 25.0},
    ]

    expanded = expand_market_only_players(existing, denoms, market)

    assert [row["NFL_player_id"] for row in expanded] == ["external"]
    assert expanded[0]["n_drafted"] == 0
    assert expanded[0]["n_auction_leagues"] == 0


def test_external_position_disagreement_never_duplicates_native_output_grain() -> None:
    existing = [{
        "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt", "year": 2020,
        "NFL_player_id": "native", "pos_grp": "SKILL", "n_drafted": 4,
    }]
    denoms = [{
        "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt", "year": 2020,
        "pos_grp": "K", "n_leagues": 10, "n_auction_eligible_leagues": 2,
    }]
    # External feeds can disagree with the canonical position taxonomy. pos_grp is only a
    # denominator routing key and is not part of the published table grain.
    market = [{
        "year": 2020, "market_format": "ppr", "teams": 12,
        "NFL_player_id": "native", "position": "K", "adp": 131.0,
    }]

    assert expand_market_only_players(existing, denoms, market) == []
