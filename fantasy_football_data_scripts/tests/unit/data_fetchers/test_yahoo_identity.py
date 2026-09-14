"""Tests for Yahoo-only manager identity resolution.

Scenarios are drawn from the real ``u_can_t_handle_this`` league, where Yahoo
redacts every guid to ``--hidden--`` across all 23 seasons:

- Two distinct "Danish" managers every year: one with a stable profile photo
  (chains by image), one always on the default placeholder (chains by name).
- The 2003 "two Imrans" share the *same* profile image -> one person, two teams.
- Several photo-less managers share the default image -> never merged by image.
"""

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.data_fetchers.yahoo.yahoo_identity import (
    image_identity_hash,
    resolve_yahoo_manager_guids,
)

DANISH_A_IMG = "https://s.yimg.com/ag/images/4532/24494747418_36e9c1_64sq.jpg"
IMRAN_IMG = "https://s.yimg.com/ag/images/4532/38982953646_c92742_64sq.jpg"
DEFAULT_IMG = "https://s.yimg.com/ag/images/default_user_profile_pic_64sq.jpg"


def _row(year, team_key, nick, image):
    return {
        "year": year,
        "team_key": team_key,
        "manager_nickname_raw": nick,
        "manager_image_url": "" if "default_user_profile" in (image or "") else image,
        "manager_guid": "--hidden--",
        "manager": nick,
    }


def test_image_hash_ignores_default_and_extracts_token():
    assert image_identity_hash(DEFAULT_IMG) is None
    assert image_identity_hash("") is None
    assert image_identity_hash(None) is None
    assert image_identity_hash(DANISH_A_IMG) == image_identity_hash(DANISH_A_IMG)
    assert image_identity_hash(DANISH_A_IMG) != image_identity_hash(IMRAN_IMG)


def test_two_danishes_split_image_vs_default_and_chain_across_years():
    df = pd.DataFrame(
        [
            _row(2010, "242.l.1161.t.3", "Danish", DANISH_A_IMG),  # Danish A
            _row(2010, "242.l.1161.t.4", "Danish", DEFAULT_IMG),  # Danish B
            _row(2013, "314.l.237.t.8", "Danish", DANISH_A_IMG),  # Danish A
            _row(2013, "314.l.237.t.9", "Danish", DEFAULT_IMG),  # Danish B
            _row(2024, "449.l.5702.t.11", "Danish", DEFAULT_IMG),  # only Danish (B)
        ]
    )
    g = resolve_yahoo_manager_guids(df)
    # Danish A is one stable identity across years via image.
    assert g.iloc[0] == g.iloc[2]
    assert g.iloc[0].startswith("yh-img-")
    # Danish B chains across years via nickname (default image = no signal).
    assert g.iloc[1] == g.iloc[3] == g.iloc[4]
    assert g.iloc[1].startswith("yh-nick-")
    # A and B are never conflated.
    assert g.iloc[0] != g.iloc[1]


def test_same_image_same_year_two_teams_is_one_person():
    # 2003 had two "Imran" teams; both carried Imran's identical profile image.
    df = pd.DataFrame(
        [
            _row(2003, "79.l.182382.t.6", "Imran", IMRAN_IMG),
            _row(2003, "79.l.182382.t.9", "Imran", IMRAN_IMG),
            _row(2010, "242.l.1161.t.9", "Imran", IMRAN_IMG),
        ]
    )
    g = resolve_yahoo_manager_guids(df)
    assert g.nunique() == 1  # one person; downstream {guid}_{team_index} splits the 2 teams


def test_default_image_does_not_merge_distinct_names():
    # Multiple photo-less managers share the default image; they must stay distinct.
    df = pd.DataFrame(
        [
            _row(2007, "x.t.1", "Scott Yanofski", DEFAULT_IMG),
            _row(2007, "x.t.2", "Jon Troy", DEFAULT_IMG),
            _row(2007, "x.t.3", "RobbieG", DEFAULT_IMG),
        ]
    )
    g = resolve_yahoo_manager_guids(df)
    assert g.nunique() == 3


def test_redacted_nickname_and_no_image_becomes_team_singleton():
    df = pd.DataFrame([_row(2003, "79.l.182382.t.11", "--hidden--", DEFAULT_IMG)])
    g = resolve_yahoo_manager_guids(df)
    assert g.iloc[0] == "yh-team-79.l.182382.t.11"


def test_mixed_pic_same_person_reconciles_when_never_co_occurring():
    # One person, photo-less early then added a photo later: never two in a season.
    df = pd.DataFrame(
        [
            _row(2010, "a.t.1", "Bob", DEFAULT_IMG),
            _row(2011, "b.t.1", "Bob", DEFAULT_IMG),
            _row(2015, "c.t.1", "Bob", "https://s.yimg.com/ag/images/9/111_aaa_64sq.jpg"),
            _row(2016, "d.t.1", "Bob", "https://s.yimg.com/ag/images/9/111_aaa_64sq.jpg"),
        ]
    )
    g = resolve_yahoo_manager_guids(df)
    assert g.nunique() == 1
    assert g.iloc[2].startswith("yh-img-")


def _scoreboard_xml(team_blocks: str) -> str:
    return f"""<?xml version="1.0"?>
<fantasy_content xmlns="http://x.com/f">
 <league><scoreboard><matchups><matchup>
   <week>14</week>
   <winner_team_key>314.l.237.t.8</winner_team_key>
   <teams>{team_blocks}</teams>
 </matchup></matchups></scoreboard></league>
</fantasy_content>"""


def _team_block(team_key, nick, image):
    return f"""<team>
   <team_key>{team_key}</team_key><team_id>{team_key.split(".t.")[-1]}</team_id>
   <name>{nick} Team</name>
   <managers><manager><manager_id>1</manager_id><nickname>{nick}</nickname>
     <guid>--hidden--</guid><image_url>{image}</image_url></manager></managers>
   <team_points><total>100.0</total></team_points>
 </team>"""


def test_end_to_end_parse_assembles_identity_inputs_and_resolves():
    """Integration: real parse path (XML -> parse_matchups_from_xml -> all_df ->
    resolver) must carry manager_nickname_raw/manager_image_url AND produce stable
    synthetic guids — never leave manager_guid="--hidden--" (which collapses to
    "--hidden--_<slot>" downstream). Guards the build_row/wiring seam that a
    resolver-only unit test misses.
    """
    import xml.etree.ElementTree as ET

    from multi_league.data_fetchers.yahoo.yahoo_matchups import parse_matchups_from_xml

    # Two distinct "Danish" teams: one with a real photo, one on the default pic.
    xml = _scoreboard_xml(
        _team_block("314.l.237.t.8", "Danish", DANISH_A_IMG) + _team_block("314.l.237.t.9", "Danish", DEFAULT_IMG)
    )
    root = ET.fromstring(__import__("re").sub(r' xmlns="[^"]+"', "", xml, count=1))

    rows = parse_matchups_from_xml(root, 2013, league_key="314.l.237")
    df = pd.DataFrame(rows)

    # The identity inputs must survive assembly (the bug that no-op'd the resolver).
    assert "manager_nickname_raw" in df.columns
    assert "manager_image_url" in df.columns
    # extract_team strips the default placeholder image to "" (no identity signal).
    danish_a = df[df["team_key"] == "314.l.237.t.8"].iloc[0]
    danish_b = df[df["team_key"] == "314.l.237.t.9"].iloc[0]
    assert danish_a["manager_image_url"] and "default_user_profile" not in danish_a["manager_image_url"]
    assert danish_b["manager_image_url"] == ""

    resolved = resolve_yahoo_manager_guids(df)
    df["fid"] = resolved
    assert (df["fid"] == "--hidden--").sum() == 0
    assert not df["fid"].astype(str).str.startswith("--hidden--").any()
    # The two Danishes split: image-anchored vs nickname-anchored.
    a_fid = df.loc[df["team_key"] == "314.l.237.t.8", "fid"].iloc[0]
    b_fid = df.loc[df["team_key"] == "314.l.237.t.9", "fid"].iloc[0]
    assert a_fid.startswith("yh-img-")
    assert b_fid.startswith("yh-nick-")
    assert a_fid != b_fid


def test_real_guid_is_left_untouched():
    df = pd.DataFrame(
        [
            {
                "year": 2024,
                "team_key": "449.l.1.t.1",
                "manager_nickname_raw": "Real",
                "manager_image_url": DANISH_A_IMG,
                "manager_guid": "ABCDEF1234567890",
                "manager": "Real",
            }
        ]
    )
    g = resolve_yahoo_manager_guids(df)
    assert g.iloc[0] == "ABCDEF1234567890"
