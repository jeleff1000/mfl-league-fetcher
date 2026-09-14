import pytest

from scripts.sota_recon.reconciliation_grid import compare_observation, reconcile_source_values


def test_compare_observation_passes_exact_identity():
    result = compare_observation(
        rule_id="passing.completions_le_attempts",
        scope_key="p1|2020|1",
        expected_value=10,
        observed_value=10,
        source_values={"pfr": 10, "pbp": 10},
        recon_rule_class="EXACT_IDENTITY",
    )
    assert result["recon_status"] == "PASS"
    assert result["recon_residual"] == 0
    assert result["recon_source_count"] == 2


def test_compare_observation_uses_absolute_and_percent_tolerance():
    result = compare_observation(
        rule_id="rate.rounding",
        scope_key="p1|2020",
        expected_value=10.0,
        observed_value=10.04,
        tolerance_abs=0.05,
        recon_rule_class="CONDITIONAL_EXACT",
    )
    assert result["recon_status"] == "PASS"
    assert result["recon_abs_residual"] == pytest.approx(0.04)


def test_compare_observation_keeps_null_distinct_from_zero():
    result = compare_observation(
        rule_id="stat.witness",
        scope_key="p1|1932",
        expected_value=0,
        observed_value=None,
        recon_rule_class="CROSS_SOURCE_EQUALITY",
    )
    assert result["recon_status"] == "UNRESOLVED"
    assert result["recon_exception_bucket"] == "MISSING_WITNESS"


def test_reconcile_source_values_detects_disagreement_without_majority_override():
    result = reconcile_source_values(
        rule_id="passing.yards",
        scope_key="p1|2020",
        source_values={"pfr": 300, "pbp": 301, "nflverse": 302},
        recon_rule_class="CROSS_SOURCE_EQUALITY",
    )
    assert result["recon_status"] == "SOURCE_DISAGREEMENT"
    assert result["recon_source_spread"] == 2
    assert result["recon_repair_candidate"] is False
