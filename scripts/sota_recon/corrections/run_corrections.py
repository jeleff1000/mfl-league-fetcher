"""
sota_recon/corrections/run_corrections.py

Replay the full, ordered correction registry against the current v26 release.
Idempotent: re-running (or running after a rebuild) re-applies cleanly.

    python -m scripts.sota_recon.corrections.run_corrections --dry-run   # preview counts
    python -m scripts.sota_recon.corrections.run_corrections             # apply + write
"""

from __future__ import annotations

import argparse
import json

from .framework import CorrectionRun
from . import (wave1_hard, wave2_backfill, wave3_dedup_def,
               wave4_allowed, wave4b_def_returns, wave4c_dst_pa, wave5_structural,
               wave6_passer_rating, wave7_efficiency_ratios)

# ordered registry — order matters when corrections compose.
# dedup runs FIRST (structural: removes duplicate rows, merges their atoms) so the
# downstream fills operate on the canonical row set.
REGISTRY = [
    *wave3_dedup_def.CORRECTIONS,
    *wave1_hard.CORRECTIONS,
    *wave2_backfill.CORRECTIONS,
    *wave4_allowed.CORRECTIONS,
    *wave4b_def_returns.CORRECTIONS,
    *wave4c_dst_pa.CORRECTIONS,   # must run AFTER 4b (uses the def-return components)
    *wave5_structural.CORRECTIONS,
    *wave6_passer_rating.CORRECTIONS,
    *wave7_efficiency_ratios.CORRECTIONS,
]


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply the v26 correction layer")
    ap.add_argument("--dry-run", action="store_true", help="count detections, write nothing")
    args = ap.parse_args()

    run = CorrectionRun()
    for c in REGISTRY:
        run.add(c)

    m = run.execute(dry_run=args.dry_run)
    print(json.dumps(
        {"dry_run": m["dry_run"], "row_count": m["row_count"],
         "total_rows_changed": m["total_rows_changed"],
         "corrections": m["corrections"]},
        indent=2,
    ))
    if not args.dry_run:
        print("\nmanifest:", m.get("manifest_path"))


if __name__ == "__main__":
    main()
