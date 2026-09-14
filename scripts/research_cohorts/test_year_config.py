import pytest

from scripts.research_cohorts.year_config import configured_years, year_predicate


def test_configured_years_are_inclusive_and_shardable(monkeypatch):
    monkeypatch.setenv("RESEARCH_YEAR_START", "2012")
    monkeypatch.setenv("RESEARCH_YEAR_END", "2016")

    assert list(configured_years()) == [2012, 2013, 2014, 2015, 2016]
    assert year_predicate("ls.year") == "ls.year BETWEEN 2012 AND 2016"


def test_configured_years_fail_closed_outside_lake(monkeypatch):
    monkeypatch.setenv("RESEARCH_YEAR_START", "1996")
    monkeypatch.setenv("RESEARCH_YEAR_END", "2025")

    with pytest.raises(ValueError, match="supported lake range"):
        configured_years()


def test_research_policy_excludes_1999_through_2002_without_deleting_prior_years(monkeypatch):
    monkeypatch.setenv("RESEARCH_YEAR_START", "1997")
    monkeypatch.setenv("RESEARCH_YEAR_END", "2006")

    assert list(configured_years()) == [1997, 1998, 2003, 2004, 2005, 2006]
    assert year_predicate("ls.year") == "ls.year IN (1997,1998,2003,2004,2005,2006)"
