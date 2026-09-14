"""The publisher must target exactly the tables the frontend reads.

A publish that misses a grain leaves the site serving a mixed build (new season numbers
against old career ones), which is worse than not publishing at all -- and it is invisible
until someone compares two pages. So the table list is asserted against the frontend's own
routing map rather than kept in sync by hand.
"""
from __future__ import annotations

import re
from pathlib import Path

import publish_research_matchup_to_fly as P

ROOT = Path(__file__).resolve().parents[2]
QUERY_TS = ROOT / "frontend/src/lib/research-cohort-query.ts"


def test_publisher_covers_every_matchup_grain_the_frontend_reads():
    text = QUERY_TS.read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if ln.strip().startswith("matchup: { season:"))
    served = set(re.findall(r"___ops\.nfl_historical\.(\w+)", line))
    assert served, "could not read the matchup table map out of research-cohort-query.ts"
    # The route map names the default 6-team table; the API appends _4po/_8po
    # dynamically from the bracket query parameter.  The publisher must cover both
    # the three route-visible defaults and those six dynamic bracket tables.
    base = {"research_matchup", "research_matchup_weekly", "research_matchup_career"}
    expected = base | {
        f"research_matchup{grain}_{bracket}"
        for bracket in ("4po", "8po")
        for grain in ("", "_weekly", "_career")
    }
    assert served == base, f"unexpected frontend matchup map: {sorted(served)}"
    expected |= {"research_matchup_adaptive", "research_matchup_adaptive_weekly",
                 "research_matchup_adaptive_career"}
    assert set(P.TABLES) == expected, (
        f"publisher targets {sorted(P.TABLES)} but expected {sorted(expected)}")


def test_publisher_defaults_to_a_dry_run():
    """--apply alone must not be enough; the prod write needs the explicit second flag."""
    source = Path(P.__file__).read_text(encoding="utf-8")
    assert "--i-understand-this-writes-prod" in source
    assert "write_prod = args.apply and args.i_understand_this_writes_prod" in source


def test_publisher_gates_the_served_rate_lanes():
    """The lanes T6/T7 redefined are the ones a bad denominator would blow past 100%."""
    assert {"win_rate", "champ_total", "champ_started",
            "playoff_total", "playoff_started"} <= set(P.RATE_LANES)
