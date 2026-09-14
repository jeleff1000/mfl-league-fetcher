from __future__ import annotations

import pandas as pd
import pytest

from top10_matchup_study import (
    eligible_leagues_from_population,
    league_members_from_population,
)


def test_population_loader_builds_unique_four_dimension_members(tmp_path) -> None:
    path = tmp_path / "population.parquet"
    pd.DataFrame(
        [
            {"db_name": "a", "week": 1, "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt"},
            {"db_name": "a", "week": 2, "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt"},
            {"db_name": "b", "week": 1, "teams": "10t", "roster": "sflx", "ppr": "half", "td": "6pt"},
        ]
    ).to_parquet(path, index=False)

    assert league_members_from_population((path,)) == {
        "a": "12t/flx/ppr/4pt",
        "b": "10t/sflx/half/6pt",
    }


def test_population_loader_rejects_league_cohort_drift(tmp_path) -> None:
    path = tmp_path / "population.parquet"
    pd.DataFrame(
        [
            {"db_name": "a", "week": 1, "teams": "12t", "roster": "flx", "ppr": "ppr", "td": "4pt"},
            {"db_name": "a", "week": 2, "teams": "10t", "roster": "flx", "ppr": "ppr", "td": "4pt"},
        ]
    ).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="multiple cohort members"):
        league_members_from_population((path,))


def test_position_population_excludes_leagues_without_required_slot(tmp_path) -> None:
    path = tmp_path / "population.parquet"
    pd.DataFrame(
        [
            {"db_name": "flex", "week": 1, "roster": "flx", "skill_eligible": 1, "k_eligible": 0, "def_eligible": 1},
            {"db_name": "idp", "week": 1, "roster": "idp", "skill_eligible": 1, "k_eligible": 1, "def_eligible": 0},
        ]
    ).to_parquet(path, index=False)

    assert eligible_leagues_from_population((path,), "QB") == {"flex", "idp"}
    assert eligible_leagues_from_population((path,), "K") == {"idp"}
    assert eligible_leagues_from_population((path,), "DEF") == {"flex"}
    assert eligible_leagues_from_population((path,), "LB") == {"idp"}
