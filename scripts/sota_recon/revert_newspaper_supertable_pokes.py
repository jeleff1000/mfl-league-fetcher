"""
sota_recon/revert_newspaper_supertable_pokes.py -- revert wave58 + wave60 super-table cell
pokes back to the clean pre-newspaper state.

Why: wave58 (NULL fills) and wave60 (PFR-arbitrated overrides) wrote LEAF stat cells
directly into nfl_player_stats_all without recomputing the derived columns that depend on
them (total_tds_scored, scrimmage_tds, pts_*, fantasy points, LAMAR, ranks). That left rows
internally inconsistent (e.g. receiving_tds=1 but total_tds_scored=0; def_tds=1 with zero granular
backing). Per CLAUDE.md "Precompute Everything / Fix Pipeline, Not Data", scoring
corrections must flow through the derivation pipeline, not cell pokes. This restores the
clean franchise_backfill v26 state; the bio backfill (wave59, a different table with no
derived dependents) is intentionally NOT reverted.

Guards: verifies the restore target is a real pre-wave58 backup with the expected clean
sha, snapshots the current (post-poke) file first, restores, re-verifies sha.

    python -m scripts.sota_recon.revert_newspaper_supertable_pokes            # DRY RUN
    python -m scripts.sota_recon.revert_newspaper_supertable_pokes --apply
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

from .sources import latest_v26

# clean pre-wave58 backups (all three identical; earliest is the truest pre-newspaper state)
CLEAN_BACKUP = ("D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_"
                "20260617T122657Z_v26/tables/nfl_player_stats_all_prew58_20260717T180043Z.parquet")
CLEAN_SHA = "165ef7b92f1723b969817b05644aba90a3f4f20a663f9943036bc2897850aa76"


def _sha(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def run(apply: bool) -> dict:
    subject = latest_v26()
    clean = Path(CLEAN_BACKUP)
    if not clean.is_file():
        raise SystemExit(f"GUARD: clean backup missing: {clean}")
    clean_sha = _sha(str(clean))
    if clean_sha != CLEAN_SHA:
        raise SystemExit(f"GUARD: clean backup sha {clean_sha[:12]} != expected {CLEAN_SHA[:12]}")
    cur_sha = _sha(subject)
    res = {"subject": subject, "current_sha": cur_sha, "clean_backup": str(clean),
           "clean_sha": clean_sha, "mode": "APPLY" if apply else "DRY-RUN",
           "already_clean": cur_sha == clean_sha}
    if not apply or res["already_clean"]:
        return res
    snap = subject.replace(".parquet", "_postpoke_prerevert.parquet")
    shutil.copy2(subject, snap)          # preserve the post-poke state for audit
    shutil.copy2(str(clean), subject)    # restore clean
    res.update(prerevert_snapshot=snap, restored=True, new_sha=_sha(subject),
               ok=_sha(subject) == CLEAN_SHA)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    print(json.dumps(run(apply=ap.parse_args().apply), indent=2))
