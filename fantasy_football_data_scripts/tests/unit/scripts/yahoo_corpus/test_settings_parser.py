from __future__ import annotations

import pytest

from scripts.yahoo_corpus.settings_parser import (
    classify_settings,
    extract_renewal_keys,
    parse_settings_xml,
)


def _settings_xml(
    *,
    num_teams: int = 12,
    reception_points: float = 1.0,
    pass_td_points: float = 4.0,
    extra_slots: tuple[tuple[str, int], ...] = (("W/R/T", 1),),
) -> str:
    slots = (("QB", 1), ("RB", 2), ("WR", 2), ("TE", 1), *extra_slots, ("BN", 6))
    roster_xml = "".join(
        f"<roster_position><position>{position}</position><count>{count}</count></roster_position>"
        for position, count in slots
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<fantasy_content xmlns="http://fantasysports.yahooapis.com/fantasy/v2/base.rng">
  <league>
    <league_key>461.l.12345</league_key>
    <league_id>12345</league_id>
    <name>Private Fixture League</name>
    <season>2025</season>
    <num_teams>{num_teams}</num_teams>
    <settings>
      <draft_type>live_standard</draft_type>
      <roster_positions>{roster_xml}</roster_positions>
      <stat_categories><stats>
        <stat><stat_id>4</stat_id><enabled>1</enabled><name>Passing Touchdowns</name><display_name>Pass TD</display_name></stat>
        <stat><stat_id>10</stat_id><enabled>1</enabled><name>Receptions</name><display_name>Rec</display_name></stat>
      </stats></stat_categories>
      <stat_modifiers><stats>
        <stat><stat_id>4</stat_id><value>{pass_td_points}</value></stat>
        <stat><stat_id>10</stat_id><value>{reception_points}</value></stat>
      </stats></stat_modifiers>
    </settings>
  </league>
</fantasy_content>"""


def test_parse_settings_xml_reuses_canonical_yahoo_shape() -> None:
    settings = parse_settings_xml(_settings_xml(), "461.l.12345")

    assert settings["league_key"] == "461.l.12345"
    assert settings["metadata"]["num_teams"] == 12
    assert settings["roster_position_counts"]["QB"] == 1
    assert settings["scoring_settings"]["rec"] == 1.0
    assert settings["scoring_settings"]["pass_td"] == 4.0


def test_classify_settings_maps_default_flex_ppr_four_point_cohort() -> None:
    row = classify_settings(parse_settings_xml(_settings_xml(), "461.l.12345"))

    assert row["teams"] == "12t"
    assert row["roster"] == "flx"
    assert row["ppr"] == "ppr"
    assert row["td"] == "4pt"
    assert row["cohort_slug"] == "12t_flx_ppr_4pt"
    assert row["roster_shape"] == "QB1_RB2_WR2_TE1_FLX1_SF0"
    assert row["classification_status"] == "classified"


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {
                "num_teams": 10,
                "reception_points": 0.5,
                "pass_td_points": 6.0,
                "extra_slots": (("Q/W/R/T", 1),),
            },
            "10t_sflx_half_6pt",
        ),
        (
            {
                "reception_points": 0.0,
                "extra_slots": (("W/R/T", 1),),
            },
            "12t_flx_std_4pt",
        ),
        (
            {
                "extra_slots": (("Q/W/R/T", 1), ("LB", 2)),
            },
            "12t_idp_ppr_4pt",
        ),
    ],
)
def test_classify_settings_matches_research_cohort_buckets(kwargs: dict, expected: str) -> None:
    row = classify_settings(parse_settings_xml(_settings_xml(**kwargs), "461.l.12345"))
    assert row["cohort_slug"] == expected


def test_classify_settings_marks_missing_team_count_incomplete() -> None:
    settings = parse_settings_xml(_settings_xml(), "461.l.12345")
    settings["metadata"].pop("num_teams", None)

    row = classify_settings(settings)

    assert row["classification_status"] == "incomplete"
    assert "num_teams" in row["classification_reason"]


def test_extract_renewal_keys_normalizes_yahoo_link_format() -> None:
    xml = _settings_xml().replace(
        "<settings>",
        "<renew>449_777</renew><renewed>461_12345</renewed><settings>",
    )

    assert extract_renewal_keys(xml) == ["449.l.777", "461.l.12345"]
