import pandas as pd
from multi_league.external_ingest.schema_conform._normalize import (
    _norm,
    _norm_col_name,
    _alias_norm,
    normalize_columns,
)


def test_norm_col_name_lowercase_snake():
    assert _norm_col_name("Manager GUID") == "manager_guid"
    assert _norm_col_name("YahooPlayerID") == "yahooplayerid"  # camelCase not split — that's fine
    assert _norm_col_name("  spaced col  ") == "spaced_col"
    assert _norm_col_name("with-dashes") == "with_dashes"


def test_norm_strips_unicode_and_suffixes():
    assert _norm("Adin 🏆") == "adin"
    assert _norm("John Smith Jr.") == "john smith"
    assert _norm("José") == "jose"
    assert _norm("Daniel III") == "daniel"


def test_alias_norm_simple():
    # mgr → manager
    assert _alias_norm("mgr_name") == "manager_name"
    # pts → points
    assert _alias_norm("total_pts") == "total_points"
    # txn_type → transaction_type
    assert _alias_norm("txn_type") == "transaction_type"


def test_alias_norm_id_to_guid_only_with_identity_token():
    # 'id' folds to 'guid' when paired with identity token
    assert _alias_norm("manager_id") == "manager_guid"
    assert _alias_norm("player_id") == "player_guid"
    # 'id' DOES NOT fold without identity token — important guard
    assert _alias_norm("league_id") == "league_id"
    assert _alias_norm("matchup_id") == "matchup_id"
    assert _alias_norm("transaction_id") == "transaction_id"


def test_alias_norm_mgr_id_aliases_to_manager_guid():
    # The motivating case: mgr_id should normalize to manager_guid
    assert _alias_norm("mgr_id") == "manager_guid"


def test_normalize_columns_strips_whitespace_and_renames():
    df = pd.DataFrame({"Manager GUID": ["a", "b "], "Team Name": [" team1", "team2"]})
    out = normalize_columns(df)
    assert list(out.columns) == ["manager_guid", "team_name"]
    assert out.iloc[0]["team_name"] == "team1"
