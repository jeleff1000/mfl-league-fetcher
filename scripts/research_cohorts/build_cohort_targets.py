"""build_cohort_targets.py -- turn the coverage report + ladder into a per-cohort GAP file.

Emits corpus_seed/cohort_targets.json: for every cohort, how many more imported
league-years it needs to clear the first reliability rung (r75) for the two
sample-size-driving metrics (start_pct, adp). The Sleeper crawl reads this to
pull thin cohorts first and skip cohorts already past r75, instead of a flat
per-cohort target that treats a saturated PPR cohort like a starved IDP one.

    py -3 scripts/research_cohorts/build_cohort_targets.py
"""
from __future__ import annotations

import json
from pathlib import Path

COHORTS = Path("D:/league-history-data/fantasy_leagues/cohort_aggregates")
COVERAGE = COHORTS / "corpus_coverage.json"
LADDER = COHORTS / "ladder_thresholds.json"
OUT = Path(__file__).resolve().parents[2] / "corpus_seed" / "cohort_targets.json"

# The metrics whose r75 sample size gates a cohort's usefulness. start_pct is the
# cheapest rung (n_r75=273); adp is the draft-ladder rung (n_r75=402). A cohort is
# "satisfied" once it clears BOTH, so the target is the larger of the two.
GATING_METRICS = ("start_pct", "adp")
# A little headroom above the bare r75 count so a cohort lands comfortably past the
# threshold rather than exactly on it (imports also fail/drop a few).
HEADROOM = 1.15


def main() -> int:
    coverage = json.loads(COVERAGE.read_text(encoding="utf-8"))
    ladder = json.loads(LADDER.read_text(encoding="utf-8"))

    r75 = 0
    for metric in GATING_METRICS:
        need = (ladder.get("metrics", {}).get(metric) or {}).get("n_r75")
        if need:
            r75 = max(r75, int(need))
    target = int(round(r75 * HEADROOM))

    current = {row["cohort_slug"]: row["imported"] for row in coverage["cohorts"]}
    targets = {}
    for cohort, have in current.items():
        gap = max(target - have, 0)
        targets[cohort] = {"current": have, "target": target, "gap": gap}
    # Cohorts we have never imported still exist as valid formats -- seed them at full gap
    # so the crawl will chase them if the drain has any.
    payload = {
        "generated_from": {"coverage": str(COVERAGE.name), "ladder": ladder.get("version")},
        "r75_target": target,
        "gating_metrics": list(GATING_METRICS),
        "cohorts": dict(sorted(targets.items(), key=lambda kv: -kv[1]["gap"])),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    needing = [(c, v["gap"]) for c, v in targets.items() if v["gap"] > 0]
    print(f"[targets] r75 target={target} (with {HEADROOM}x headroom)")
    print(f"[targets] {len(needing)} cohorts still short of r75:")
    for cohort, gap in sorted(needing, key=lambda kv: -kv[1])[:15]:
        print(f"    {cohort:<20} gap={gap}")
    print(f"[targets] wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
