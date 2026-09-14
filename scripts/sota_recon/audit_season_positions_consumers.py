"""Classify every repository occurrence of the legacy season_positions field."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/audits/sota-recon/position/season-positions-consumer-ledger.json"


def classify(path: str, line: str) -> tuple[str, str]:
    stripped = line.lstrip()
    if path.endswith("apply_weekly_overlays.py") or path.endswith("build_position_declaration.py"):
        return "COMMENT_ONLY", "Narrative documentation explicitly rejects the retired v26 field as authority."
    if path.endswith("build_nfl_position_from_pfr_v26.py"):
        return "TEST_FIXTURE", "Legacy anchor/receipt label; not used to derive current eligibility."
    if "witness_audit_v2" in path or "build_stat_contracts.py" in path or "contracts/" in path:
        return "LEGACY_DISPLAY", "Contract/inventory compatibility metadata; not a position derivation authority."
    if stripped.startswith(("#", "//", "/*", "*")):
        return "COMMENT_ONLY", "Narrative reference; no authority behavior."
    if "build_position_eligibility_v26.py" in path:
        return "LEGACY_DISPLAY", "Deprecated v26 stat-line-presence builder; not an authority path."
    if "build_research_team_aggregates.py" in path:
        return "STAT_LINE_MEMBERSHIP", "Derived membership feature; never eligible for position or fantasy-position authority."
    if "golden_points.py" in path or "test" in path:
        return "TEST_FIXTURE", "Regression fixture; must use the PFR declaration for eligibility assertions."
    if path.startswith("docs/") or "generated" in path:
        return "LEGACY_DISPLAY", "Inventory/compatibility metadata; not a derivation authority."
    return "REVIEW_REQUIRED", "Occurrence requires explicit semantic review before use in a position derivation."


def build_ledger() -> dict:
    proc = subprocess.run(["git", "grep", "-n", "-I", "season_positions", "--", "."], cwd=ROOT, text=True, capture_output=True)
    rows = []
    for raw in proc.stdout.splitlines():
        path, line_no, line = raw.split(":", 2)
        disposition, reason = classify(path, line)
        rows.append({"path": path, "line": int(line_no), "text": line, "disposition": disposition, "reason": reason})
    return {
        "schema_version": "season-positions-consumer-ledger.v1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "search": "git grep -n -I season_positions -- .",
        "occurrence_count": len(rows),
        "position_authority_violations": [r for r in rows if r["disposition"] == "REVIEW_REQUIRED"],
        "rows": rows,
    }


def main() -> None:
    ledger = build_ledger(); OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "occurrences": ledger["occurrence_count"], "violations": len(ledger["position_authority_violations"])}, indent=2))


if __name__ == "__main__":
    main()
