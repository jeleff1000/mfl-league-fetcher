"""Record a bounded lock-regeneration attempt without modifying the lock contract."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/audits/sota-recon/position/lock-regeneration-attempt.json"
LOCK = ROOT / "scripts/sota_recon/witness_gate/contracts/witness_locks.v1.json"


def main() -> None:
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    payload = {
        "schema_version": "lock-regeneration-attempt.v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "command": "python scripts/sota_recon/vouch_2024.py",
        "status": "TIMEOUT_120S_BEFORE_WRITE",
        "head_observed": head,
        "lock_contract_modified": False,
        "reason": "The full machine-generated vouch scan did not reach its write phase within 120 seconds while scanning the 1,082-column weekly plane.",
        "required_follow_up": "Run the existing builder to completion in an isolated maintenance window, review the lock delta, then commit only the generated lock and builder receipts.",
        "position_lanes_ready": ["nflcom_player_page_positions:nfl_position", "nflcom_player_page_positions:position"],
        "derived_position_status": "fantasy_position requires a structural law certificate; no external witness lock was created.",
        "lock_path": str(LOCK),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "status": payload["status"]}, indent=2))


if __name__ == "__main__":
    main()
