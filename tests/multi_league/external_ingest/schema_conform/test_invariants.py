from types import SimpleNamespace
import pandas as pd
from multi_league.external_ingest.schema_conform._invariants import (
    validate_invariants,
)


def _ctx(manager_series):
    return SimpleNamespace(df=pd.DataFrame({"manager": manager_series}))


def test_manager_guid_yahoo_shape_passes():
    src = pd.Series(["SAQHTT6JGGOWN3ROP6QMGS6KTU"] * 9 + ["A" * 26])
    ctx = _ctx(["m1"] * 9 + ["m2"])
    failures = validate_invariants("manager_guid", src, ctx)
    assert failures == []


def test_manager_guid_low_cardinality_fails():
    # Only 1 distinct guid for 10 managers — cardinality invariant fails
    src = pd.Series(["SAQHTT6JGGOWN3ROP6QMGS6KTU"] * 10)
    ctx = _ctx([f"m{i}" for i in range(10)])
    failures = validate_invariants("manager_guid", src, ctx)
    assert any("cardinality" in f.lower() or "match" in f.lower() for f in failures)


def test_manager_guid_garbage_shape_fails():
    src = pd.Series(["abc", "xy", "12"] * 4)  # not Yahoo guid / Sleeper id / ESPN swid shape
    ctx = _ctx([f"m{i}" for i in range(12)])
    failures = validate_invariants("manager_guid", src, ctx)
    assert len(failures) > 0


def test_year_in_range_passes():
    src = pd.Series([2013, 2014, 2015])
    failures = validate_invariants("year", src, ctx=SimpleNamespace(df=None))
    assert failures == []


def test_year_out_of_range_fails():
    src = pd.Series([1850, 2014, 2015])
    failures = validate_invariants("year", src, ctx=SimpleNamespace(df=None))
    assert len(failures) > 0


def test_week_in_range_passes():
    src = pd.Series([1, 5, 17, 22])
    failures = validate_invariants("week", src, ctx=SimpleNamespace(df=None))
    assert failures == []


def test_yahoo_player_id_digit_shape_required():
    src = pd.Series(["abc123", "def456", "5479", "9274"] * 5)  # mixed shapes — should fail
    failures = validate_invariants("yahoo_player_id", src, ctx=SimpleNamespace(df=None))
    assert len(failures) > 0


def test_yahoo_player_id_pure_digits_passes():
    src = pd.Series([str(i) for i in range(1000, 2000)])  # 1000 distinct ints
    failures = validate_invariants("yahoo_player_id", src, ctx=SimpleNamespace(df=None))
    assert failures == []


def test_transaction_id_entropy_skipped_on_small_sample():
    # < 50 rows — entropy check should be skipped
    src = pd.Series(["aaaaaa"] * 10)
    failures = validate_invariants("transaction_id", src, ctx=SimpleNamespace(df=None))
    # presence + median length still apply; entropy skipped
    assert all("entropy" not in f.lower() for f in failures)


def test_transaction_id_low_entropy_fails_on_large_sample():
    src = pd.Series(["aaaaaa"] * 100)  # zero variation, entropy ~0
    failures = validate_invariants("transaction_id", src, ctx=SimpleNamespace(df=None))
    assert any("entropy" in f.lower() for f in failures)
