import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs/audits/sota-recon/position/season-positions-consumer-ledger.json"


def test_legacy_season_positions_has_no_unclassified_authority_consumers():
    payload = json.loads(LEDGER.read_text(encoding="utf-8"))
    assert payload["search"] == "git grep -n -I season_positions -- ."
    assert payload["position_authority_violations"] == []


def test_legacy_builder_is_explicitly_non_authoritative():
    source = (ROOT / "scripts/sota_recon/build_position_eligibility_v26.py").read_text(encoding="utf-8")
    assert "DEPRECATED_NON_AUTHORITATIVE" in source
