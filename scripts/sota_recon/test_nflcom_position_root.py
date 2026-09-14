import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RECEIPT = ROOT / "docs/audits/sota-recon/position/nflcom-root-receipt.json"


def test_nflcom_position_root_is_collapsed_and_manifest_pinned():
    payload = json.loads(RECEIPT.read_text(encoding="utf-8"))
    assert payload["source_root"] == "nflcom"
    assert payload["source_artifact_sha256"]
    assert payload["within_root"]["one_root_vote_required"] is True
    assert payload["within_root"]["conflicting_slug_seasons"] == 0
    assert payload["expected_coverage"]["mapped_slugs"] <= payload["expected_coverage"]["parsed_slugs"]
