"""
sota_recon/build_bracket_drop_v26.py  --  drop the redundant DST points-/yards-allowed BRACKET
columns from physical storage. They are one-hot encodings of a scalar and are re-derived in the
___ops view (cutover_ops_views._select_items), so this is a storage de-duplication, not a
removal: every reader still sees the columns, derived, value-identical.

WHY SAFE: each dropped bracket equals its derived one-hot on 100% of DEF rows (measured; see
dst_brackets + the promote-time verify). The four LOW pts_allow tiers (0/1_6/7_13/14_20) are
shared DST/IDP mirror columns and are KEPT physically -- only the 12 non-mirror brackets drop:

    pts_allow_21_27, pts_allow_28_34, pts_allow_35_plus,
    yds_allow_0_99, yds_allow_100_199, yds_allow_200_299, yds_allow_300_349, yds_allow_350_399,
    yds_allow_400_449, yds_allow_450_499, yds_allow_500_549, yds_allow_550_plus

ORDERING: run LAST, after any bracket-reading build wave (e.g. build_dst_scoring_v26 reads
pts_allow_* to compute pts_def_std). Those waves see the brackets while they are still stored;
this drop happens just before promote, and the promoted view re-derives them.

The scalars dst_points_allowed / total_yds_allowed are NOT dropped -- they are the source of
truth the view derives from.

    python -m scripts.sota_recon.build_bracket_drop_v26            # dry-run
    python -m scripts.sota_recon.build_bracket_drop_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "fantasy_football_data_scripts"))
from multi_league.transformations.common.dst_brackets import (  # noqa: E402
    DROPPABLE_BRACKETS,
    MIRROR_BRACKETS,
    PTS_ALLOW_SCALAR,
    YDS_ALLOW_SCALAR,
    bracket_onehot_sql,
)

PROV = "wave66.bracket_drop"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA disable_progress_bar")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    present = [c for c in DROPPABLE_BRACKETS if c in cols]
    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]

    # the scalars we derive from MUST survive, or the view derivation has nothing to read
    for scalar in (PTS_ALLOW_SCALAR, YDS_ALLOW_SCALAR):
        if scalar not in cols:
            con.close()
            raise SystemExit(f"ABORT: scalar {scalar} absent -- cannot drop brackets that derive from it")
    # the mirror tiers must NOT be in the drop set
    bad = [c for c in present if c in MIRROR_BRACKETS]
    if bad:
        con.close()
        raise SystemExit(f"ABORT: refusing to drop mirror brackets {bad}")

    if not present:
        con.close()
        return {"note": "no droppable brackets present -- already dropped", "rows": before_rows}

    # prove each dropped bracket is reproduced by its derivation on 100% of DEF rows before removing it
    mismatches = {}
    for c in present:
        onehot = bracket_onehot_sql(c)
        n_bad = con.execute(
            f"SELECT COUNT(*) FROM '{vq}' WHERE position='DEF' AND COALESCE({_q(c)},0) <> ({onehot})"
        ).fetchone()[0]
        if n_bad:
            mismatches[c] = n_bad
    if mismatches:
        con.close()
        raise SystemExit(f"ABORT: derivation does not reproduce stored bracket on some DEF rows: {mismatches}")

    if not apply:
        con.close()
        return {
            "rows": before_rows,
            "columns_before": len(cols),
            "would_drop": present,
            "kept_mirrors": sorted(MIRROR_BRACKETS),
            "derivation_verified_all_def_rows": True,
        }

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    excl = ", ".join(_q(c) for c in present)
    out_sql = f"SELECT * EXCLUDE ({excl}) FROM '{vq}'"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_bracketdrop.parquet")
    reader = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for batch in reader:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()

    problems = []
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    if after_rows != before_rows:
        problems.append(f"row count moved {before_rows} -> {after_rows}")
    new_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{tq}'").fetchall()]
    expected = [c for c in cols if c not in present]
    if new_cols != expected:
        problems.append("remaining column set/order changed beyond the intended drop")
    for scalar in (PTS_ALLOW_SCALAR, YDS_ALLOW_SCALAR):
        if scalar not in new_cols:
            problems.append(f"scalar {scalar} was lost")

    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew66_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "dropped": present,
        "kept_mirrors": sorted(MIRROR_BRACKETS),
        "columns_before": len(cols),
        "columns_after": len(new_cols),
        "backup": str(backup),
        "provenance": PROV,
        "next": "promote with --allow-drop=" + ",".join(present) + " ; the ___ops view re-derives them",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the dropped-bracket release")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
