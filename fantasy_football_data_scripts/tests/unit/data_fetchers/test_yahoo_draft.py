from __future__ import annotations

from xml.etree import ElementTree as ET

import pandas as pd

from multi_league.data_fetchers.yahoo import yahoo_draft
from multi_league.data_fetchers.yahoo.yahoo_draft import DraftPick


def test_fetch_draft_picks_uses_combined_draftresults_players_payload(monkeypatch):
    xml = """
    <fantasy_content>
      <league>
        <draft_results>
          <draft_result>
            <pick>141</pick>
            <round>15</round>
            <cost>35</cost>
            <team_key>461.l.90939.t.3</team_key>
            <player_key>461.p.33477</player_key>
          </draft_result>
        </draft_results>
        <players>
          <player>
            <player_key>461.p.33477</player_key>
            <player_id>33477</player_id>
            <name>
              <full>Nico Collins</full>
            </name>
            <display_position>WR</display_position>
            <primary_position>WR</primary_position>
            <is_keeper>
              <status>1</status>
              <cost>35</cost>
              <kept>1</kept>
            </is_keeper>
          </player>
        </players>
      </league>
    </fantasy_content>
    """

    monkeypatch.setattr(yahoo_draft, "fetch_url", lambda url, oauth: ET.fromstring(xml))

    picks = yahoo_draft.fetch_draft_picks(oauth=object(), league_id="461.l.90939", year=2025)

    assert len(picks) == 1
    pick = picks[0]
    assert pick.pick == 141
    assert pick.round == 15
    assert pick.team_key == "461.l.90939.t.3"
    assert pick.yahoo_player_id == "33477"
    assert pick.player == "Nico Collins"
    assert pick.yahoo_position == "WR"
    assert pick.is_keeper_status == "1"
    assert pick.is_keeper_cost == "35"


def test_merge_draft_data_preserves_keeper_fields_from_pick_payload():
    picks = [
        DraftPick(
            year=2025,
            pick=141,
            round=15,
            team_key="461.l.90939.t.3",
            yahoo_player_id="33477",
            cost=35.0,
            player="Nico Collins",
            yahoo_position="WR",
            is_keeper_status="1",
            is_keeper_cost="35",
        )
    ]

    merged = yahoo_draft.merge_draft_data(
        picks=picks,
        analysis_df=None,
        team_key_to_manager={"461.l.90939.t.3": "Gavi"},
        team_key_to_guid={"461.l.90939.t.3": "ABC12345FULL"},
        player_id_to_team={"33477": "HOU"},
        player_id_to_name={"33477": "Nico Collins"},
        manager_name_overrides=None,
    )

    row = merged.iloc[0]
    assert row["is_keeper_status"] == "1"
    assert row["is_keeper_cost"] == "35"
    assert row["manager"] == "Gavi"
    assert row["manager_guid"] == "ABC12345FULL"
    assert row["nfl_team"] == "HOU"


def test_merge_draft_data_returns_empty_schema_for_empty_pick_payload():
    merged = yahoo_draft.merge_draft_data(
        picks=[],
        analysis_df=None,
        team_key_to_manager={},
        team_key_to_guid={},
        player_id_to_team={},
        player_id_to_name={},
        manager_name_overrides=None,
    )

    assert merged.empty
    assert list(merged.columns) == yahoo_draft.DRAFT_FINAL_COLUMNS
    assert str(merged["yahoo_player_id"].dtype) == "string"


def test_normalize_yahoo_draft_type_promotes_any_priced_draft_to_auction():
    draft_df = pd.DataFrame({"cost": [35.0, 1.0, None]})

    assert yahoo_draft._normalize_yahoo_draft_type("snake", draft_df) == "auction"


def test_normalize_yahoo_draft_type_defaults_unknown_unpriced_draft_to_snake():
    draft_df = pd.DataFrame({"cost": [None, None]})

    assert yahoo_draft._normalize_yahoo_draft_type("unknown", draft_df) == "snake"
