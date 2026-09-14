"""
sota_recon/nflcom_semantic_validation.py -- O.9.0 phase 3: turn DECLARED labels into RECEIPTED ones.

Phase 2 proved SEGMENTATION (exactly one layout regenerates each header signature) but not the LABELS
inside a block: nothing yet forced `avg` in the QB block to actually be passing yards per attempt
rather than, say, yards per completion. Labels were read off the NFL.com legend and marked
NOT_PROVEN. Validating them normally waits on the slug->pfr_id crosswalk (O.9.2).

IT DOES NOT HAVE TO. NFL.com publishes DERIVED columns next to their inputs, and a derived column is
an arithmetic identity over the very columns whose names we are trying to prove:

    QB block     avg      == pass_yds / pass_att        (yards per ATTEMPT, not per completion)
    RBFB block   avg      == rush_yds / rush_att
    WRTE block   avg      == rec_yds  / rec             (per RECEPTION -- a different denominator)
    P  block     avg      == punt_yds / punts
    K  block     pct      == fg_made  / fg_att

These identities are SELF-CONTAINED (one row, no join, no crosswalk) and, critically, they are
DISCRIMINATING: the RBFB and WRTE layouts differ only in which of rushing/receiving comes first, and
they divide by different denominators, so a swapped layout FAILS its identity instead of passing
quietly. Same for pass-yards-per-attempt vs per-completion.

A label is receipted when its identity holds on a large sample within tolerance; it stays declared
when there is no identity that touches it (counts like `td` have no derived neighbour). This never
promotes anything to voting -- it establishes what NFL.com's column IS, which is O.9.0's whole job.

    python -m scripts.sota_recon.nflcom_semantic_validation --validate
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from . import sources as S

OUT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-semantic-validation.json"
# NFL.com DISPLAYS these quotients to one decimal, so the identity can only ever hold to the
# display precision. 0.06 is the rounding boundary (0.05 + float slop), NOT a fitted threshold:
# a tolerance sweep on the QB block saturates at exactly 100.00% there and stays flat out to
# 0.25 (57.93% @ 0.02, 96.69% @ 0.05, 100.00% @ 0.06+). A cliff to 100% -- rather than an
# asymptote to 99.x% -- is the signature of an EXACT identity seen through rounding.
TOL = 0.06          # absolute, in the quotient's own DISPLAYED units (never scaled)
MIN_AGREE = 0.99    # below this the LABEL is suspect, not the data
MIN_ROWS = 200      # identities measured on fewer rows than this are reported, never graded


class Identity:
    """One derived-column identity: quotient == numerator / denominator, inside a caption."""

    def __init__(self, family: str, caption: str, quotient: str, num: str, den: str, proves: str):
        self.family, self.caption = family, caption
        self.quotient, self.num, self.den = quotient, num, den
        self.proves = proves   # which label assignments this identity discriminates


# Stored (deduped) column names per caption -- these are the PHYSICAL parquet columns.
IDENTITIES = [
    # player_career: caption IS the position block, so the layout is unambiguous per row
    Identity("player_career", "QB Career", "avg", "yds", "att",
             "QB block: avg=pass_yds/pass_att (per ATTEMPT, not per completion)"),
    Identity("player_career", "RBFB Career", "avg", "yds", "att",
             "RBFB block: leading block is RUSHING (avg=rush_yds/rush_att)"),
    Identity("player_career", "RBFB Career", "avg_2", "yds_2", "rec",
             "RBFB block: trailing block is RECEIVING (avg_2=rec_yds/rec)"),
    Identity("player_career", "WRTE Career", "avg", "yds", "rec",
             "WRTE block: leading block is RECEIVING (avg=rec_yds/rec) -- the RBFB/WRTE "
             "discriminator; a swapped layout divides by the wrong denominator"),
    Identity("player_career", "WRTE Career", "avg_2", "yds_2", "att",
             "WRTE block: trailing block is RUSHING (avg_2=rush_yds/rush_att)"),
    Identity("player_career", "P Career", "avg", "yds", "punts",
             "P block: avg=punt_yds/punts"),
    Identity("player_career", "P Career", "net_avg", "net_yds", "punts",
             "P block: net_avg=punt_net_yds/punts"),
    Identity("player_career", "Defense Career", "avg", "yds", "int",
             "DEF block: the yds/avg pair belongs to INT RETURNS, not scrimmage"),
    Identity("player_career", "K Career", "pct", "fgm", "fg_att",
             "K block: pct=fg_made/fg_att (x100)"),
]


def _fam_glob(key: str) -> str:
    return os.path.join(S.registry()[key].path, "*.parquet").replace("\\", "/")


def _measure(con, ident: Identity) -> dict:
    src = _fam_glob(f"nflcom_{ident.family}")
    scale = 100.0 if ident.quotient == "pct" else 1.0  # scales the IMPLIED value, never the tolerance
    q = f"""
    WITH r AS (
      SELECT TRY_CAST("{ident.quotient}" AS DOUBLE) AS obs,
             TRY_CAST("{ident.num}" AS DOUBLE) AS num,
             TRY_CAST("{ident.den}" AS DOUBLE) AS den
      FROM read_parquet('{src}', union_by_name=true)
      WHERE _table = '{ident.caption}'
    )
    SELECT COUNT(*) AS n,
           SUM(CASE WHEN abs(obs - ({scale} * num / den)) <= {TOL} THEN 1 ELSE 0 END) AS ok
    FROM r WHERE obs IS NOT NULL AND num IS NOT NULL AND den IS NOT NULL AND den > 0"""
    n, ok = con.execute(q).fetchone()
    n, ok = int(n or 0), int(ok or 0)
    rate = (ok / n) if n else 0.0
    if n < MIN_ROWS:
        verdict = "INSUFFICIENT_ROWS"
    elif rate >= MIN_AGREE:
        verdict = "RECEIPTED"
    else:
        verdict = "LABEL_SUSPECT"
    return {"family": ident.family, "caption": ident.caption,
            "identity": f"{ident.quotient} == {ident.num}/{ident.den}"
                        + (" x100" if scale == 100.0 else ""),
            "rows": n, "agree": ok, "agreement": round(rate, 5),
            "verdict": verdict, "proves": ident.proves}


def _counter_check(con) -> dict:
    """THE DISCRIMINATION PROOF. If the RBFB/WRTE layouts were swapped, the identities would still
    have to hold. Measure the WRONG denominator explicitly and show it FAILS -- otherwise a passing
    identity proves nothing (both readings would agree and the test would be vacuous)."""
    src = _fam_glob("nflcom_player_career")
    out = {}
    for caption, right, wrong in (("WRTE Career", "rec", "att"), ("RBFB Career", "att", "rec")):
        q = f"""
        WITH r AS (SELECT TRY_CAST(avg AS DOUBLE) o, TRY_CAST(yds AS DOUBLE) y,
                          TRY_CAST("{right}" AS DOUBLE) d_right, TRY_CAST("{wrong}" AS DOUBLE) d_wrong
                   FROM read_parquet('{src}', union_by_name=true) WHERE _table='{caption}')
        SELECT COUNT(*),
               SUM(CASE WHEN abs(o - y/d_right) <= {TOL} THEN 1 ELSE 0 END),
               SUM(CASE WHEN d_wrong > 0 AND abs(o - y/d_wrong) <= {TOL} THEN 1 ELSE 0 END)
        FROM r WHERE o IS NOT NULL AND y IS NOT NULL AND d_right > 0"""
        n, ok_r, ok_w = (int(x or 0) for x in con.execute(q).fetchone())
        out[caption] = {"rows": n,
                        "declared_denominator": right, "declared_agreement": round(ok_r / n, 5) if n else 0,
                        "swapped_denominator": wrong, "swapped_agreement": round(ok_w / n, 5) if n else 0,
                        "discriminating": bool(n and (ok_r / n) - (ok_w / n) > 0.5)}
    return out


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    rows = [_measure(con, i) for i in IDENTITIES]
    counter = _counter_check(con)
    n_rec = sum(1 for r in rows if r["verdict"] == "RECEIPTED")
    n_sus = sum(1 for r in rows if r["verdict"] == "LABEL_SUSPECT")
    print(f"{'caption':<22} {'identity':<28} {'rows':>9} {'agree':>8}  verdict")
    for r in rows:
        print(f"{r['caption']:<22} {r['identity']:<28} {r['rows']:>9,} "
              f"{r['agreement']*100:>7.2f}%  {r['verdict']}")
    print("\nDISCRIMINATION (a passing identity is worthless if the swapped reading also passes):")
    for cap, c in counter.items():
        print(f"  {cap:<16} declared /{c['declared_denominator']:<5} {c['declared_agreement']*100:6.2f}%   "
              f"swapped /{c['swapped_denominator']:<5} {c['swapped_agreement']*100:6.2f}%   "
              f"discriminating={c['discriminating']}")
    payload = {"generated": "O.9.0 phase 3: label validation by derived-column identity",
               "method": "self-contained arithmetic identities; no join, no crosswalk",
               "tolerance": TOL, "min_agreement": MIN_AGREE,
               "receipted": n_rec, "label_suspect": n_sus,
               "identities": rows, "discrimination": counter}
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nRECEIPTED {n_rec}/{len(rows)}   LABEL_SUSPECT {n_sus}")
    print(f"-> {OUT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true")
    ap.parse_args()
    main()
