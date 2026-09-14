import json
from pathlib import Path

from sota_recon.audit_position_residuals import classify_case, parse_tokens


ROOT = Path(__file__).resolve().parents[2]
SUMMARY = ROOT / "docs/audits/sota-recon/position/position-residual-summary.json"
CASES = ROOT / "docs/audits/sota-recon/position/position-residual-cases.csv"


def test_historical_ls_is_scoped_alignment():
    result = classify_case("LS", 1958, ["DB"], ["DB"], ["DB"])
    assert result["role_kind"] == "ALIGNMENT"
    assert result["adjudication_class"] == "SAME_BROAD_ALIGNMENT"


def test_p_is_not_globally_demoted():
    assert classify_case("P", 1958, ["P"], ["P"], ["P"])["adjudication_class"] == "EXACT_ALIAS"
    result = classify_case("P", 1951, ["QB"], ["QB"], ["QB"])
    assert result["role_kind"] == "USAGE_ROLE"
    assert result["adjudication_class"] == "USAGE_ROLE_NOT_PRIMARY"


def test_emitted_receipt_has_no_unclassified_historical_cases():
    assert SUMMARY.exists()
    assert CASES.exists()
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    assert summary["unclassified_1950_1969"] == 0


def test_parser_preserves_source_tokens():
    assert parse_tokens("LDH-LS-LOH") == ["LDH", "LS", "LOH"]
