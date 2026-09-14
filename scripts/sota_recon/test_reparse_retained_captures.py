"""Tests for the retained-bytes re-parse lane (O.8f Phase 0).

The failure guarded: a capture parser that drops columns makes those fields invisible
to Law C forever, because Law C enumerates only what SURVIVED the parser.
"""

from __future__ import annotations

from . import reparse_retained_captures as R


def test_parser_is_header_driven_not_positional():
    """StatsCrew serves 10-column rosters on most pages and 7-column rosters (no
    jersey/GP/GS) on others. A positional parser drops the narrow pages silently --
    the exact class of loss this lane exists to undo."""
    wide = ('<table><tr><th>#</th><th>Player</th><th>Pos.</th><th>Birth Date</th>'
            '<th>Height</th><th>Weight</th><th>College</th><th>Hometown</th>'
            '<th>GP</th><th>GS</th></tr>'
            '<tr><td>66</td>'
            '<td><a href="https://www.statscrew.com/football/stats/p-andrigeo001">George Andrie</a></td>'
            '<td>LDE</td><td sorttable_customkey="1940-04-20">April 20, 1940</td>'
            '<td sorttable_customkey="78">6\'6"</td><td>250</td><td>Marquette</td>'
            '<td>Grand Rapids, MI</td><td>14</td><td>14</td></tr></table>')
    rows, diag = R.parse_statscrew_roster(wide)
    assert len(rows) == 1 and not diag["unmapped_headers"]
    r = rows[0]
    assert r["birth_date"] == "1940-04-20"      # ISO, gifted by sorttable_customkey
    assert r["source_player_id"] == "andrigeo001"
    assert r["height_inches"] == 78 and r["weight_lbs"] == 250
    assert r["games_played"] == 14 and r["games_started"] == 14
    assert r["jersey_number"] == 66 and r["college"] == "Marquette"

    narrow = ('<table><tr><th>Player</th><th>Pos.</th><th>Birth Date</th><th>Height</th>'
              '<th>Weight</th><th>College</th><th>Hometown</th></tr>'
              '<tr><td><a href="https://www.statscrew.com/football/stats/p-smitjoe001">Joe Smith</a></td>'
              '<td>QB</td><td sorttable_customkey="1921-03-02">March 2, 1921</td>'
              '<td>6\'0"</td><td>190</td><td>Army</td><td>Boston, MA</td></tr></table>')
    rows2, diag2 = R.parse_statscrew_roster(narrow)
    assert len(rows2) == 1, "narrow (7-column) pages must NOT be dropped"
    assert rows2[0]["birth_date"] == "1921-03-02"
    assert rows2[0]["jersey_number"] is None   # absent on this page shape, not invented


def test_unknown_headers_surface_as_counts_never_vanish():
    html = ('<table><tr><th>Player</th><th>Pos.</th><th>Some New Column</th></tr>'
            '<tr><td>A B</td><td>QB</td><td>x</td></tr></table>')
    rows, diag = R.parse_statscrew_roster(html)
    assert "some new column" in diag["unmapped_headers"], (
        "a header we have never seen must be COUNTED, not silently ignored")
    assert rows and rows[0]["player"] == "A B"


def test_every_source_page_column_is_mapped_or_deliberately_unmapped():
    for label in R.STATSCREW_ROSTER_COLUMNS:
        assert label.lower() in R.HEADER_MAP, f"{label}: source column has no mapping"


def test_the_dropped_set_is_recorded():
    """The point of the lane: what the original parser threw away is named, so the
    recovery is auditable rather than asserted."""
    dropped = [c for c in R.STATSCREW_ROSTER_COLUMNS if c not in R.ORIGINAL_KEPT]
    assert "Birth Date" in dropped and "GP" in dropped and "GS" in dropped
    assert len(dropped) == 8
