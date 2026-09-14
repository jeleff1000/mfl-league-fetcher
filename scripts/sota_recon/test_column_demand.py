"""Guards for the demand-join lane and the equation lane. Synthetic fixtures only."""
from __future__ import annotations

from .audit_capacity_runner import parse
from .column_demand import MIN_AGREEMENT, MIN_PUBLISHERS, evaluate


def _rec(**kw):
    base = {"proposed_canonical": "x", "unit": "yards", "aggregation_class": "MAX",
            "publishers": [{"source": "a", "lineage_root": "nflcom"},
                           {"source": "b", "lineage_root": "pfr"}],
            "agreement": [{"agree_pct": 0.99, "n": 1000}],
            "crossed_control": "the alternative reading scores 52%",
            "constraint": "bound holds on both publishers",
            "lower_layer_denominator": {"counter": "volume", "status": "PASS"},
            "supply": "pfr.some_col, 89,818 rows"}
    base.update(kw)
    return base


def test_a_complete_case_is_evidenced():
    assert evaluate(_rec())[0] == "EVIDENCED"


def test_status_is_derived_and_a_typed_status_cannot_force_it():
    """Six nflcom families sat PENDING_CROSSWALK for days after their receipt PASSED because
    a generator short-circuited on a hand-typed string. evaluate() must ignore one."""
    status, _ = evaluate(_rec(status="EVIDENCED", publishers=[{"source": "a",
                                                              "lineage_root": "nflcom"}]))
    assert status == "INSUFFICIENT"


def test_one_publisher_is_a_request_not_a_statistic():
    status, why = evaluate(_rec(publishers=[{"source": "a", "lineage_root": "nflcom"}]))
    assert status == "INSUFFICIENT"
    assert any(str(MIN_PUBLISHERS) in w for w in why)


def test_two_publishers_sharing_one_root_do_not_corroborate():
    """`ko` is a header nflcom publishes and never fills; a candidate must not be able to
    clear the bar on two views of the same lineage."""
    status, why = evaluate(_rec(publishers=[{"source": "a", "lineage_root": "nflcom"},
                                            {"source": "b", "lineage_root": "nflcom"}]))
    assert status == "INSUFFICIENT"
    assert any("lineage root" in w for w in why)


def test_agreeing_on_a_name_is_not_agreeing_on_a_number():
    status, why = evaluate(_rec(agreement=[{"agree_pct": 0.40, "n": 1000}]))
    assert status == "INSUFFICIENT"
    assert any(str(MIN_AGREEMENT) in w for w in why)


def test_a_missing_field_is_declared_not_evidenced():
    for field in ("crossed_control", "constraint", "lower_layer_denominator", "supply", "publishers"):
        rec = _rec(); rec[field] = None
        assert evaluate(rec)[0] == "DECLARED", field


def test_a_clean_candidate_with_failed_lower_layer_stays_insufficient():
    status, why = evaluate(_rec(lower_layer_denominator={
        "counter": "volume", "status": "HOLD", "reason": "lower counter is unstable"
    }))
    assert status == "INSUFFICIENT"
    assert any("lower-layer denominator" in item for item in why)


def test_equation_parser_takes_only_a_two_operand_ratio():
    assert parse("avg == yds / att") == ("avg", "yds", "att", 1.0)
    assert parse("pct == made / att * 100") == ("pct", "made", "att", 100.0)
    assert parse("x == a + b") is None, "a general evaluator would let a claim mean anything"
    assert parse("passer_rating == magic") is None


def test_scale_is_declared_per_claim_in_both_directions():
    """fg% needs x100 and xp% needed /100 in the same sweep, so no blanket rule works."""
    assert parse("a == b / c * 100")[3] == 100.0
    assert parse("a == b / c * 0.01")[3] == 0.01
    assert parse("a == b / c")[3] == 1.0
