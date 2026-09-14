import pandas as pd

from multi_league.core.canonical_draft import (
    clean_external_draft_player_fields,
    external_draft_player_match_key,
    normalize_draft_df,
    split_external_draft_player,
)


def test_normalize_draft_df_unifies_keeper_and_normalizes_def_names():
    df = pd.DataFrame(
        {
            "year": [2025, 2024],
            "player": ["Bills", "Ravens"],
            "position": [None, ""],
            "yahoo_position": ["DST", None],
            "primary_position": [None, "D/ST"],
            "is_keeper_status": ["1", ""],
            "is_keeper_cost": ["", "7"],
            "manager_guid": ["ABCDEF123456", "ZYXWVUT98765"],
            "league_id": ["", None],
            "player_id": ["123.0", "456"],
        }
    )

    normalized = normalize_draft_df(df, platform="yahoo", league_id="461.l.90939")

    assert normalized["player"].tolist() == ["Bills DST", "Ravens DST"]
    assert normalized["position"].tolist() == ["DEF", "DEF"]
    assert normalized["is_keeper"].astype(int).tolist() == [1, 1]
    assert normalized["league_id"].tolist() == ["461.l.90939", "461.l.90939"]
    assert normalized["draft_id"].tolist() == [
        "yahoo_461.l.90939_2025_default",
        "yahoo_461.l.90939_2024_default",
    ]
    # franchise_id falls back to the full manager_guid when not pre-populated
    # (canonical_draft.py:206-214). The original test asserted an 8-char
    # truncation that the code no longer does.
    assert normalized["franchise_id"].tolist() == ["ABCDEF123456", "ZYXWVUT98765"]
    assert normalized["yahoo_player_id"].tolist() == ["123", "456"]


def test_normalize_draft_df_clears_yahoo_all_keeper_full_draft_artifact():
    rows = []
    for pick in range(1, 21):
        rows.append(
            {
                "year": 2025,
                "round": ((pick - 1) // 2) + 1,
                "pick": pick,
                "player": f"Player {pick}",
                "position": "RB",
                "is_keeper_status": "1",
                "is_keeper_cost": "",
                "manager_guid": f"GUID{pick % 2}",
                "player_id": str(pick),
            }
        )
    df = pd.DataFrame(rows)

    normalized = normalize_draft_df(df, platform="yahoo", league_id="461.l.90939")

    assert normalized["is_keeper"].astype(int).sum() == 0


def test_normalize_draft_df_preserves_real_all_keeper_draft_with_cost_signal():
    rows = []
    for pick in range(1, 21):
        rows.append(
            {
                "year": 2025,
                "round": ((pick - 1) // 2) + 1,
                "pick": pick,
                "player": f"Player {pick}",
                "position": "RB",
                "is_keeper_status": "1",
                "is_keeper_cost": "5" if pick == 1 else "",
                "manager_guid": f"GUID{pick % 2}",
                "player_id": str(pick),
            }
        )

    normalized = normalize_draft_df(
        pd.DataFrame(rows), platform="yahoo", league_id="461.l.90939"
    )

    assert normalized["is_keeper"].astype(int).sum() == 20


def test_normalize_draft_df_treats_hidden_yahoo_guid_as_missing_franchise_id():
    df = pd.DataFrame(
        {
            "year": [2003, 2003],
            "round": [1, 1],
            "pick": [1, 2],
            "player": ["Player A", "Player B"],
            "manager": ["Hidden Team", "Known Manager"],
            "manager_guid": ["--hidden--", "known-guid"],
            "franchise_id": ["--hidden--", None],
            "player_id": ["123", "456"],
        }
    )

    normalized = normalize_draft_df(df, platform="yahoo", league_id="461.l.90939")

    assert pd.isna(normalized.loc[0, "franchise_id"])
    assert normalized.loc[1, "franchise_id"] == "known-guid"


def test_normalize_draft_df_preserves_sleeper_draft_id_for_duplicate_pick_slots():
    df = pd.DataFrame(
        {
            "year": [2025, 2025],
            "round": [1, 1],
            "pick": [1, 1],
            "draft_id": ["startup-1", "rookie-1"],
            "player": ["Player A", "Player B"],
            "position": ["RB", "WR"],
            "player_id": ["111", "222"],
        }
    )

    normalized = normalize_draft_df(df, platform="sleeper", league_id="league123")

    assert normalized["draft_id"].tolist() == ["startup-1", "rookie-1"]
    assert normalized[["year", "draft_id", "round", "pick"]].duplicated().sum() == 0


def test_split_external_draft_player_preserves_team_and_position_hints():
    assert split_external_draft_player("Derrick Henry, Ten RB") == ("Derrick Henry", "TEN", "RB")
    assert split_external_draft_player("Ryan Tannehill TEN") == ("Ryan Tannehill", "TEN", None)
    assert split_external_draft_player("Kareem Hunt, FA") == ("Kareem Hunt", None, None)
    assert split_external_draft_player("Isaiah Davis, SDSU") == ("Isaiah Davis", None, None)
    assert split_external_draft_player("Jaguars, JAC DST") == ("Jaguars", "JAX", "DEF")


def test_clean_external_draft_player_fields_cleans_live_draft_beer_aliases():
    df = pd.DataFrame(
        {
            "player": [
                "Jacobi Myers, LV WR",
                "Derick Carr NO",
                "Chigoziem Okonkwo, TEN TE",
                "Hollywood Brown, KC WR",
            ],
            "position": [None, "", None, "WR"],
            "nfl_team_api": [None, None, "", ""],
        }
    )

    cleaned = clean_external_draft_player_fields(df)

    assert cleaned["player"].tolist() == ["Jakobi Meyers", "Derek Carr", "Chig Okonkwo", "Marquise Brown"]
    assert cleaned["position"].tolist() == ["WR", "", "TE", "WR"]
    assert cleaned["nfl_team_api"].tolist() == ["LV", "NO", "TEN", "KC"]


def test_external_draft_player_match_key_handles_known_aliases_and_suffixes():
    assert external_draft_player_match_key("Patrick Mahomes II, KC QB") == "patrick mahomes"
    assert external_draft_player_match_key("Ken Walker III, SEA RB") == "kenneth walker"
    assert external_draft_player_match_key("Robby Anderson, CAR WR") == "robbie chosen"
    assert external_draft_player_match_key("jesper horset, CHI TE") == "jesper horsted"


def test_normalize_draft_df_cleans_external_player_names_before_canonical_save():
    df = pd.DataFrame(
        {
            "year": [2025, 2025],
            "round": [1, None],
            "pick": [1, None],
            "player": ["Joshua Palmer, LAC WR", "Patrick Mahomes II, KC QB"],
            "manager": ["Joshua", "Iossi"],
            "cost": [4, 51],
            "draft_type": ["Auction", "Auction"],
        }
    )

    normalized = normalize_draft_df(df, platform="yahoo", league_id="461.l.90939")

    assert normalized["player"].tolist() == ["Josh Palmer", "Patrick Mahomes II"]
    assert normalized["position"].tolist() == ["WR", "QB"]
    assert normalized["nfl_team_api"].tolist() == ["LAC", "KC"]
