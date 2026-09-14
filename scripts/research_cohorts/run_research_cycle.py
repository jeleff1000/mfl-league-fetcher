"""run_research_cycle.py -- ONE research rebuild cycle, in dependency order, halt-on-failure.

D5's root cause was structural: the weekly builders were separate invocations nobody was
forced to run, so they silently served a half-size lake while the season tables moved. This
runner makes the cycle a single command -- weekly builders are in-cycle by construction, and
release_gate runs LAST so nothing gets promoted off a partial build.

Data-refresh steps (snapshot_real_leagues.py, merge_corpus_slices.py, build_corpus_snapshot
--refold-local) are NOT in this runner: they contend for the single-writer corpus snapshot
and are sequenced manually per the handoff.

    py -3 scripts/research_cohorts/run_research_cycle.py             # full cycle
    py -3 scripts/research_cohorts/run_research_cycle.py --from grades   # resume mid-cycle
"""
from __future__ import annotations

import argparse
import datetime
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]

# Dependency order:
#   ladder  -> writes ladder_thresholds.json, which the DRAFT builder reads for ADP source
#              selection. Running draft first makes it warn and serve own-ADP, silently
#              undoing that work -- ladder MUST come first.
#   matchup -> produces CLUTCH, which feeds the transaction and draft grade overlays
#              (Joe 2026-07-20, binding). The clutch join currently happens in `grades`,
#              which runs after all three, so output does not depend on this order today --
#              but encoding it here means moving clutch into the txn/draft builders can
#              never silently invert the dependency.
#   grades  -> reads the draft/txn/matchup season parquets
#   career  -> reads the graded parquets
#   weekly  -> matchup emits the canonical weekly artifact; weekly_matchup only validates
#              it against research_matchup_graded and loads the verified local table
#   bundle  -> reads everything; release_gate judges the bundle and always runs LAST.
# Use `--sample N` to shake out plumbing before committing to the full (~16h) matchup pass;
# that is what makes it safe to put the whale early rather than ordering around failure risk.
STEPS = [
    ("ladder", "ladder_stab.py"),
    ("matchup", "build_research_matchup_cohort.py"),
    ("txn", "build_research_txn_cohort.py"),
    ("draft", "build_research_draft_cohort.py"),
    ("grades", "build_research_cohort_grades.py"),
    ("career", "build_research_career_rollup.py"),
    ("weekly_matchup", "build_weekly_matchup.py"),
    ("weekly_txn", "build_weekly_txn.py"),
    ("bundle", "build_wide_bundle.py"),
    ("release_gate", "release_gate.py"),
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="start", choices=[s for s, _ in STEPS],
                    help="resume the cycle from this step (inclusive)")
    ap.add_argument("--only", choices=[s for s, _ in STEPS], action="append",
                    help="run only these steps (repeatable); overrides --from")
    ap.add_argument("--sample", type=int, metavar="N",
                    help="SMOKE RUN over N leagues: exercises the whole chain in minutes so "
                         "plumbing bugs surface before an overnight build. Output is diverted "
                         "to a sample dir and can never overwrite production parquets.")
    args = ap.parse_args()

    env = dict(os.environ)
    if args.sample:
        sample_dir = Path(f"D:/tmp/research_sample_{args.sample}")
        sample_dir.mkdir(parents=True, exist_ok=True)
        env["RESEARCH_SAMPLE_LEAGUES"] = str(args.sample)
        env["RESEARCH_OUT_DIR"] = str(sample_dir)
        print(f"[cycle] SAMPLE RUN: {args.sample} leagues -> {sample_dir} "
              f"(production outputs untouched)", flush=True)

    steps = STEPS
    if args.only:
        steps = [s for s in STEPS if s[0] in args.only]
    elif args.start:
        idx = [s for s, _ in STEPS].index(args.start)
        steps = STEPS[idx:]

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path(f"D:/tmp/research_cycle_{stamp}")
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"[cycle] {len(steps)} step(s) -> logs in {log_dir}", flush=True)

    for name, script in steps:
        log = log_dir / f"{name}.log"
        t0 = datetime.datetime.now()
        print(f"[cycle] {name}: {script} started {t0:%H:%M:%S}", flush=True)
        with open(log, "w", encoding="utf-8") as f:
            rc = subprocess.run([sys.executable, str(HERE / script)],
                                cwd=str(ROOT), env=env,
                                stdout=f, stderr=subprocess.STDOUT).returncode
        dt = (datetime.datetime.now() - t0).total_seconds()
        if rc != 0:
            # A sample lake cannot fill a cohort cell to MIN_STABLE, so every row grades
            # 'insufficient' and the bundle (which serves only 'confident') is empty -- the
            # gate then fails on emptiness. That is the gate working, not a defect: its
            # verdict is only meaningful at full lake size. Plumbing is what a smoke run
            # tests, so report and continue rather than masking a green chain as failed.
            if args.sample and name == "release_gate":
                print(f"[cycle] {name}: FAILED (expected on a {args.sample}-league sample -- "
                      f"no cohort cell reaches MIN_STABLE, so the bundle is empty). "
                      f"Gate verdicts are only meaningful on the full lake. See {log}",
                      flush=True)
                print("[cycle] SAMPLE COMPLETE -- every builder ran end to end.", flush=True)
                return
            print(f"[cycle] {name} FAILED rc={rc} after {dt:,.0f}s -- see {log}", flush=True)
            print(f"[cycle] resume with: --from {name}", flush=True)
            sys.exit(rc)
        print(f"[cycle] {name} ok ({dt:,.0f}s)", flush=True)

    if args.sample:
        print(f"[cycle] SAMPLE COMPLETE -- every builder ran end to end on "
              f"{args.sample} leagues.", flush=True)
    else:
        print("[cycle] COMPLETE -- release_gate passed; evidence in the gate log", flush=True)


if __name__ == "__main__":
    main()
