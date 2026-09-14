from __future__ import annotations

from .nflcom_targeted_cache_reparse import normalize_tables


def test_normalize_tables_preserves_the_exact_layout_for_polysemous_columns():
    tables = [
        {
            "caption": "Regular Season",
            "headers": [
                "WK", "Game Date", "OPP", "RESULT", "COMP", "ATT", "YDS", "AVG",
                "TD", "INT", "SCK", "SCKY", "RATE", "ATT", "YDS", "AVG", "TD",
                "FUM", "LOST",
            ],
            "keys": [
                "wk", "game_date", "opp", "result", "comp", "att", "yds", "avg",
                "td", "int", "sck", "scky", "rate", "att_2", "yds_2", "avg_2",
                "td_2", "fum", "lost",
            ],
            "rows": [{"wk": "1", "yds": "250", "yds_2": "12"}],
        },
        {
            "caption": "Regular Season",
            "headers": [
                "WK", "Game Date", "OPP", "RESULT", "Total", "Solo", "AST", "SCK",
                "SFTY", "PDEF", "INT", "YDS", "AVG", "LNG", "TDS", "FF", "FR",
            ],
            "keys": [
                "wk", "game_date", "opp", "result", "total", "solo", "ast", "sck",
                "sfty", "pdef", "int", "yds", "avg", "lng", "tds", "ff", "fr",
            ],
            "rows": [{"wk": "1", "yds": "31"}],
        },
    ]

    rows, failures = normalize_tables(
        tables,
        slug="example-player",
        season="1970",
        artifacts=["fumbles_pre1978", "season_type_probe"],
        cache_sha1="abc123",
    )

    assert failures == []
    assert {row["_layout"] for row in rows} == {
        "id_gamelog:QB",
        "id_gamelog:DEF_log",
    }
    qb = next(row for row in rows if row["_layout"] == "id_gamelog:QB")
    defense = next(row for row in rows if row["_layout"] == "id_gamelog:DEF_log")
    assert qb["yds"] == "250"
    assert defense["yds"] == "31"
    assert qb["_source_artifacts"] == "fumbles_pre1978;season_type_probe"


def test_normalize_tables_refuses_an_unknown_header_instead_of_guessing():
    tables = [{
        "caption": "Regular Season",
        "headers": ["WK", "WIDGETS"],
        "keys": ["wk", "widgets"],
        "rows": [{"wk": "1", "widgets": "9"}],
    }]
    rows, failures = normalize_tables(
        tables, slug="x", season="1970", artifacts=["probe"], cache_sha1="deadbeef"
    )
    assert rows == []
    assert failures[0]["status"] == "UNRESOLVED"
