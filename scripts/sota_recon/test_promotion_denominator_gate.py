"""Promotion candidates cannot clear on a clean counter without denominator evidence."""

import json
from pathlib import Path

from .build_pfr_final_adjudication_summary import denominator_status


def test_clean_counter_without_denominator_stays_held():
    status, note = denominator_status({"comparable": 330, "matches": 330, "mismatches": 0})
    assert status == "CLEAN_WITHOUT_DENOMINATOR"
    assert "no numerator/denominator" in note


def test_clean_rate_requires_explicit_operands():
    status, _ = denominator_status({
        "comparable": 684,
        "matches": 684,
        "mismatches": 0,
        "equation": "pass_target_yds / pass_att * 1",
    })
    assert status == "DENOMINATOR_RECEIPTED"


def test_failed_rate_denominator_cannot_clear():
    status, _ = denominator_status({
        "comparable": 30,
        "matches": 28,
        "mismatches": 2,
        "numerator": "com",
        "denominator": "att",
    })
    assert status == "DENOMINATOR_FAILED"


def test_current_pfr_summary_exposes_held_clean_counters():
    path = Path("docs/audits/pfr-final-adjudication-summary-2025.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["promotion_ready_count"] == 1
    assert payload["promotion_held_without_denominator_count"] == 112
    assert payload["promotion_denominator_counts"]["CLEAN_WITHOUT_DENOMINATOR"] == 2
