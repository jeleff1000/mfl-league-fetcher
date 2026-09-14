from types import SimpleNamespace
import pandas as pd
from multi_league.external_ingest.schema_conform._derivation import (
    _fuzzy_lookup,
    apply_derivation,
)


def test_fuzzy_exact_match():
    mapping = {"adin": "GUID_ADIN", "marc": "GUID_MARC"}
    result = _fuzzy_lookup("adin", mapping)
    assert result.value == "GUID_ADIN"
    assert result.candidates[0] == ("adin", 100)


def test_fuzzy_high_confidence_with_clear_winner():
    mapping = {"daniel": "GUID_DAN", "marc": "GUID_MARC"}
    result = _fuzzy_lookup("danel", mapping)  # typo for daniel
    assert result.value == "GUID_DAN"


def test_fuzzy_ambiguous_returns_none_with_candidates():
    """'Dan' matches 'Daniel' and 'Dave' too closely — must abstain."""
    mapping = {"daniel": "GUID_DAN", "dave": "GUID_DAV"}
    result = _fuzzy_lookup("dan", mapping)
    assert result.value is None
    assert len(result.candidates) >= 2
    # Top candidates with their scores must be present for UI disambiguation
    names = [c[0] for c in result.candidates]
    assert "daniel" in names or "dave" in names


def test_fuzzy_below_cutoff_returns_none():
    mapping = {"adin": "GUID_ADIN"}
    result = _fuzzy_lookup("xyz", mapping)
    assert result.value is None


def test_fuzzy_empty_input():
    result = _fuzzy_lookup("", {"adin": "G"})
    assert result.value is None
    assert result.candidates == []


def test_apply_derivation_fills_manager_guid_via_name():
    """Source has 'manager' but not 'manager_guid'; derivation fills it."""
    df = pd.DataFrame({"manager": ["Adin", "Marc", "Tani"]})
    ctx = SimpleNamespace(
        name_to_guid={"adin": "GUID_ADIN", "marc": "GUID_MARC", "tani": "GUID_TANI"},
        team_key_to_guid={},
    )
    out = apply_derivation(df, slot="manager_guid", ctx=ctx)
    assert out.tolist() == ["GUID_ADIN", "GUID_MARC", "GUID_TANI"]


def test_apply_derivation_returns_none_for_unknown():
    df = pd.DataFrame({"manager": ["Adin", "Stranger"]})
    ctx = SimpleNamespace(
        name_to_guid={"adin": "GUID_ADIN"},
        team_key_to_guid={},
    )
    out = apply_derivation(df, slot="manager_guid", ctx=ctx)
    assert out.iloc[0] == "GUID_ADIN"
    assert pd.isna(out.iloc[1])
