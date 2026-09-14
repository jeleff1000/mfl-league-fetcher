import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RECEIPT = REPO_ROOT / "docs/audits/sota-recon/position/closeout-preflight.json"


def test_preflight_receipt_has_identity_and_scope():
    assert RECEIPT.exists(), f"missing receipt: {RECEIPT}"
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    assert receipt["branch"] == "main"
    assert receipt["head"]
    assert receipt["scope"] == "position-closeout"
    assert "required_paths" in receipt
    assert "focused_tests" in receipt


def test_preflight_receipt_records_dirty_paths_without_claiming_clean():
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    assert isinstance(receipt["dirty_paths"], list)
    assert receipt["worktree_clean"] is False
