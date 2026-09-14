import pytest
import pandas as pd
from multi_league.external_ingest.schema_conform._scoring import (
    token_jaccard,
    value_jaccard,
    dtype_match,
    shape_cosine,
    weights_for,
    compute_score,
    _shannon_entropy_per_char,
)


def test_token_jaccard_identical():
    assert token_jaccard("manager_guid", "manager_guid") == 1.0


def test_token_jaccard_partial():
    # "manager_guid" vs "manager_age": [manager, guid] vs [manager, age] = 1/3
    assert token_jaccard("manager_guid", "manager_age") == pytest.approx(1 / 3, rel=1e-3)


def test_token_jaccard_disjoint():
    assert token_jaccard("foo_bar", "baz_qux") == 0.0


def test_value_jaccard_identical_sets():
    s1 = {"a", "b", "c"}
    s2 = {"a", "b", "c"}
    assert value_jaccard(s1, s2) == 1.0


def test_value_jaccard_partial():
    s1 = {"a", "b"}
    s2 = {"b", "c"}
    assert value_jaccard(s1, s2) == 1 / 3


def test_value_jaccard_empty_sets_returns_zero():
    assert value_jaccard(set(), set()) == 0.0


def test_dtype_match_same_dtype():
    s1 = pd.Series([1, 2, 3], dtype="int64")
    s2 = pd.Series([4, 5], dtype="int64")
    assert dtype_match(s1, s2) == 1.0


def test_dtype_match_numeric_compat():
    s1 = pd.Series([1, 2], dtype="int64")
    s2 = pd.Series([1.0, 2.0], dtype="float64")
    assert dtype_match(s1, s2) == 0.5


def test_dtype_match_string_vs_numeric():
    s1 = pd.Series(["a", "b"])
    s2 = pd.Series([1, 2])
    assert dtype_match(s1, s2) == 0.0


def test_shape_cosine_identical_distributions():
    s1 = pd.Series(["abc", "def", "ghi"])
    s2 = pd.Series(["xyz", "uvw", "rst"])  # same length distribution (all 3)
    assert shape_cosine(s1, s2) == pytest.approx(1.0, rel=1e-3)


def test_weights_thick_when_ref_above_threshold():
    w = weights_for("manager_guid", ref_distinct_count=20)  # threshold is 8
    assert w["value"] == 0.45
    assert w["name"] == 0.30


def test_weights_thin_when_ref_below_threshold():
    w = weights_for("manager_guid", ref_distinct_count=5)  # below 8
    assert w["value"] == 0.15
    assert w["name"] == 0.45


def test_weights_unknown_slot_uses_default():
    w = weights_for("unknown_slot", ref_distinct_count=30)  # default threshold 50
    assert w == {"name": 0.45, "value": 0.15, "dtype": 0.15, "shape": 0.25}  # thin


def test_shannon_entropy_uniform_high():
    high = pd.Series(["abc123", "def456", "ghi789", "jkl012", "mno345"])
    e = _shannon_entropy_per_char(high)
    assert e > 2.5


def test_shannon_entropy_repeated_low():
    low = pd.Series(["aaa", "aaa", "aaa", "aaa"])
    e = _shannon_entropy_per_char(low)
    assert e < 1.0


def test_compute_score_full_match():
    src = pd.Series(["AAA", "BBB", "CCC"])
    ref = pd.Series(["AAA", "BBB", "CCC"])
    score = compute_score(
        src_col_name="manager_guid",
        src_values=src,
        slot="manager_guid",
        ref_values=ref,
        ref_distinct_count=3,
    )
    assert score > 0.9  # high score for identical name + values
