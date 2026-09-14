"""Materialize the final PFR disposition summary from immutable receipts."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

AUDITS = Path("docs/audits")
OUT = AUDITS / "pfr-final-adjudication-summary-2025.json"


def denominator_status(checks: object) -> tuple[str, str]:
    """Classify the denominator evidence beneath a promotion candidate.

    A clean counter comparison is not a denominator receipt.  Rates must expose an
    equation (or explicit numerator/denominator); bare counter equality stays held.
    """
    if not isinstance(checks, dict):
        return "NO_LOWER_LAYER_DENOMINATOR", "no lower-layer check was published"

    comparable = checks.get("comparable")
    matches = checks.get("matches")
    mismatches = checks.get("mismatches")
    has_equation = bool(checks.get("equation"))
    has_operands = bool(checks.get("numerator")) and bool(checks.get("denominator"))

    if (has_equation or has_operands) and mismatches in (None, 0):
        return "DENOMINATOR_RECEIPTED", "explicit numerator/denominator equation passed"
    if mismatches not in (None, 0):
        return "DENOMINATOR_FAILED", "published lower-layer equation has mismatches"
    if comparable not in (None, 0) and matches == comparable:
        return "CLEAN_WITHOUT_DENOMINATOR", (
            "counter comparison is clean, but no numerator/denominator receipt was published"
        )
    return "NO_LOWER_LAYER_DENOMINATOR", "no comparable lower-layer denominator evidence"


def walk(value: object, counts: Counter, wrong: list[dict], candidates: list[dict], blocked: list[dict], artifact: str) -> None:
    if isinstance(value, dict):
        disposition = value.get("disposition")
        if isinstance(disposition, str):
            counts[disposition] += 1
            row = {"artifact": artifact, "source": value.get("source"), "table_key": value.get("table_key"), "column": value.get("column"), "canonical": value.get("canonical"), "reason": value.get("reason"), "checks": value.get("checks")}
            if disposition == "MAPPED_BUT_WRONG":
                wrong.append(row)
            elif disposition == "PROMOTION_CANDIDATE":
                row["denominator_status"], row["denominator_note"] = denominator_status(row["checks"])
                candidates.append(row)
            elif disposition == "OPEN_BLOCKED":
                blocked.append(row)
        for nested in value.values():
            walk(nested, counts, wrong, candidates, blocked, artifact)
    elif isinstance(value, list):
        for nested in value:
            walk(nested, counts, wrong, candidates, blocked, artifact)


def main() -> None:
    counts: Counter = Counter()
    wrong: list[dict] = []
    candidates: list[dict] = []
    blocked: list[dict] = []
    for path in sorted(AUDITS.glob("pfr-*.json")):
        if path.name == OUT.name:
            continue
        walk(json.loads(path.read_text(encoding="utf-8")), counts, wrong, candidates, blocked, path.name)
    coverage = json.loads((AUDITS / "pfr-audit-coverage-2025.json").read_text(encoding="utf-8"))
    columns = json.loads((AUDITS / "pfr-column-coverage-2025.json").read_text(encoding="utf-8"))
    output = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "coverage": {"groups": coverage["coverage_counts"]["groups"], "receipted": coverage["coverage_counts"]["receipted"], "unmapped": coverage["coverage_counts"]["unmapped"], "invalid_artifacts": coverage["coverage_counts"]["invalid_artifacts"], "blocked_artifacts": coverage["coverage_counts"]["blocked_artifacts"]},
        "column_coverage": {"groups": columns["groups"], "complete_groups": columns["complete_groups"], "incomplete_groups": columns["incomplete_groups"]},
        "disposition_counts": dict(counts),
        "mapped_but_wrong": wrong,
        "promotion_candidates": candidates,
        "open_blocked": blocked,
        "promotion_denominator_counts": dict(Counter(row["denominator_status"] for row in candidates)),
        "promotion_ready_count": sum(
            row["denominator_status"] == "DENOMINATOR_RECEIPTED" for row in candidates
        ),
        "promotion_held_without_denominator_count": sum(
            row["denominator_status"] != "DENOMINATOR_RECEIPTED" for row in candidates
        ),
        "backfill_policy": "No backfills performed; promotion candidates and mapped-but-wrong definitions remain explicitly deferred.",
    }
    OUT.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print({"written": str(OUT), "mapped_but_wrong": len(wrong), "promotion_candidates": len(candidates), "open_blocked": len(blocked)})


if __name__ == "__main__":
    main()
