"""
sota_recon/build_ruled_consolidation_drop_v26.py -- the PROVEN half of Joe's 2026-08-01
consolidation ruling, as a weekly-plane column drop in the build_legacy_vocab_drop pattern.

RULED (Joe, 2026-08-01): retire the canonical/extra twins. PROVEN zero-loss tonight:

    fg_made_60_plus_canonical  0 value disagreements across all both-nonnull cells and a
                               STRICT SUBSET of fg_made_60plus (keeper even holds 13
                               nonzero events canonical lacks). Keeper carries the
                               scoring-log licence (100%).
    touches                    0 value disagreements across 863,893 both-nonnull weekly
                               cells vs total_touches; the FRONTEND serves total_touches
                               (player-column-plan), so the keeper is the product's name.
                               FOLLOW-UP OWED: the generated spec touches->touches dies
                               with the column; the dossier decision re-points to
                               total_touches at the next generator regen (one-path law
                               blocks a hand spec until then). Season grain NOT in scope:
                               471 disagreeing player-seasons await witness arbitration.

RULED BUT REFUSED TONIGHT -- the zero-loss proofs FAILED and deletion discipline holds:

    total_tds_accounted_for        5,836 cells violate total_tds_accounted_for = total_tds_scored + passing_tds. Not purely
                     derivable; the violating cells go to scoring-log arbitration first.
    fg_yds_over_30   6,090 of 15,682 cells violate legacy = canonical - 30*(30+ makes).
                     The buckets, canonical, and legacy do not reconcile on 39% of the
                     shared cells; the scoring log arbitrates which column is wrong
                     there before any of the three is dropped.

    python -m scripts.sota_recon.build_ruled_consolidation_drop_v26            # dry-run
    python -m scripts.sota_recon.build_ruled_consolidation_drop_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import duckdb

from . import sources as S

DROP_COLS = ["fg_made_60_plus_canonical", "touches"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    src = Path(S.latest_v26())
    con = duckdb.connect()
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{src.as_posix()}')").fetchall()]
    present = [c for c in DROP_COLS if c in cols]
    missing = [c for c in DROP_COLS if c not in cols]

    # re-verify the zero-loss claims against the live file before any apply --
    # a proof measured yesterday is not a proof of today's file
    checks = {}
    if "fg_made_60_plus_canonical" in present:
        n = con.execute(f"""SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')
            WHERE fg_made_60_plus_canonical IS NOT NULL AND fg_made_60plus IS NOT NULL
              AND TRY_CAST(fg_made_60_plus_canonical AS DOUBLE)
                  <> TRY_CAST(fg_made_60plus AS DOUBLE)""").fetchone()[0]
        checks["fg_made_60_plus_canonical_disagreements"] = n
    if "touches" in present:
        n = con.execute(f"""SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')
            WHERE touches IS NOT NULL AND total_touches IS NOT NULL
              AND TRY_CAST(touches AS DOUBLE) <> TRY_CAST(total_touches AS DOUBLE)"""
            ).fetchone()[0]
        checks["touches_disagreements"] = n
    report = {"drop": present, "already_absent": missing, "zero_loss_checks": checks,
              "apply": args.apply}
    if any(v != 0 for v in checks.values()):
        report["REFUSED"] = "zero-loss check failed against the LIVE file; nothing dropped"
        print(json.dumps(report, indent=1))
        return 1
    if not args.apply:
        print(json.dumps({**report, "note": "dry-run; --apply writes the reduced file "
                          "beside the source with a .bak of the original"}, indent=1))
        return 0

    keep = [c for c in cols if c not in present]
    bak = src.with_suffix(src.suffix + ".bak_ruled_drop")
    shutil.copy2(src, bak)
    tmp = src.with_suffix(".tmp_ruled_drop.parquet")
    quoted = ", ".join(f'"{c}"' for c in keep)
    con.execute(f"COPY (SELECT {quoted} FROM read_parquet('{src.as_posix()}')) "
                f"TO '{tmp.as_posix()}' (FORMAT parquet)")
    tmp.replace(src)
    report["backup"] = str(bak)
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
