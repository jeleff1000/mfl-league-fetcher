"""O.9.3: an INDEPENDENT, arithmetic receipt for the splits/situational layout identities.

The block census (`nflcom_splits_block_census`) reads the site's own <h3> section name.
That is the primary evidence and it is the source's own published name -- but it is read
from a ~12% cache sample, and one method reading one artifact is one method. So this
module settles the same question a second way, from the STORED VALUES ONLY, touching no
HTML at all: a rate column identifies its own operands.

The test is DISCRIMINATING BY CONSTRUCTION, which is the whole point. For every layout it
takes each rate-shaped column and scores it against EVERY ordered pair of other columns in
that layout as numerator/denominator. `player_splits_L3.avg` is not merely *consistent
with* yds/att -- it agrees on 99.x% of eligible rows while the next best pair in the same
layout agrees on a small fraction. A one-sided "it matches" proves nothing; the MARGIN is
the receipt, so the runner-up is reported beside the winner and a layout whose margin is
thin is reported as such rather than quietly passing.

WHAT IT PROVES AND WHAT IT DOES NOT.
  PROVES     the arithmetic RELATIONSHIP between columns inside a block: which column is
             the count, which the yardage, which the average of the two. That is exactly
             what a MAPPED_TO_CANONICAL decision needs to not swap two columns.
  DOES NOT   name the stat FAMILY. yds/att is the same equation for a rush and a pass.
             The family comes from the <h3>, and the two together are what license the
             correspondence table.

    python -m scripts.sota_recon.nflcom_splits_layout_identity --build
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-layout-identity.json"
UNSHIFT_RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-unshift-receipt.json"
TABLES = Path("D:/league-history-data/nfl/raw/nflcom/tables")

# columns shaped like a rate rather than a count. Restricting the TARGET keeps the sweep
# tractable; the numerator/denominator side is swept exhaustively and is where a wrong
# reading would actually hide.
RATE_TARGETS = ("avg", "pct", "1st_2", "rate", "net_avg")
# `pct`/`1st_2` are published as percentages, so the ratio is scaled before comparison.
PERCENT_TARGETS = {"pct", "1st_2"}

MIN_ELIGIBLE = 500      # below this a "100%" is noise, and is reported as INSUFFICIENT
TOLERANCE = 0.06        # published to 1 d.p.; half a tick plus float slack

# A ZERO RATE IS NOT EVIDENCE, and the first version of this sweep did not know that.
# These are split rows, so most are empty: avg = 0 over yds = 0 satisfies 0/x for EVERY
# denominator x. Counting those rows made the defense block's `avg` score 99.95% for
# yds/int and 96.47% for yds/sfty -- a 3-point margin that reported WEAK and looked like a
# genuine rival reading, when it was mass agreement on nothing. Eligibility therefore
# requires the target AND the numerator to be NON-ZERO: a row only votes when the equation
# it is asked about actually has something to say. This TIGHTENS the test -- it throws away
# the rows that inflate every candidate equally -- rather than relaxing the margin rule
# that caught the problem.
NONDEGENERATE = True


def _num(column: str) -> str:
    """The stored cells are VARCHAR and carry '--', '' and thousands separators."""
    return (f"TRY_CAST(NULLIF(REPLACE(REPLACE(\"{column}\", ',', ''), '--', ''), '') "
            f"AS DOUBLE)")


def layouts() -> dict[str, list[str]]:
    unshift = json.loads(UNSHIFT_RECEIPT.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for family, body in unshift["families"].items():
        for entry in body["layouts"]:
            # the lost column is NULL on every row by construction -- never an operand
            out[entry["layout"]] = [c for c in entry["columns"]
                                    if c != entry["lost_column"]]
    return out


def score_layout(connection, family: str, layout: str, columns: list[str]) -> dict:
    source = (TABLES / f"{family}_unshifted" / "*.parquet").as_posix()
    targets = [c for c in columns if c in RATE_TARGETS]
    results: dict[str, dict] = {}
    for target in targets:
        operands = [c for c in columns if c != target]
        scale = 100.0 if target in PERCENT_TARGETS else 1.0
        selects, pairs = [], []
        for numerator in operands:
            for denominator in operands:
                if numerator == denominator:
                    continue
                index = len(pairs)
                pairs.append((numerator, denominator))
                eligible = (f"{_num(target)} <> 0 AND {_num(numerator)} <> 0 "
                            f"AND {_num(denominator)} > 0") if NONDEGENERATE else (
                    f"{_num(target)} IS NOT NULL AND {_num(numerator)} IS NOT NULL "
                    f"AND {_num(denominator)} > 0")
                agree = (f"ABS({_num(target)} - {scale} * {_num(numerator)} / "
                         f"{_num(denominator)}) <= {TOLERANCE}")
                selects.append(f"SUM(CASE WHEN {eligible} THEN 1 ELSE 0 END) AS e{index}")
                selects.append(
                    f"SUM(CASE WHEN {eligible} AND {agree} THEN 1 ELSE 0 END) AS a{index}")
        if not selects:
            continue
        row = connection.execute(
            f"SELECT {', '.join(selects)} FROM read_parquet('{source}') "
            f"WHERE _layout = '{layout}'").fetchone()
        scored = []
        for index, (numerator, denominator) in enumerate(pairs):
            eligible, agreed = row[index * 2] or 0, row[index * 2 + 1] or 0
            if eligible < MIN_ELIGIBLE:
                continue
            scored.append({"numerator": numerator, "denominator": denominator,
                           "eligible": eligible, "agreed": agreed,
                           "pct": round(100.0 * agreed / eligible, 3)})
        scored.sort(key=lambda entry: -entry["pct"])
        if not scored:
            results[target] = {"status": "INSUFFICIENT_ROWS"}
            continue
        best = scored[0]
        runner = scored[1] if len(scored) > 1 else None
        results[target] = {
            "best": best,
            "runner_up": runner,
            "margin_points": round(best["pct"] - runner["pct"], 3) if runner else None,
            "status": ("RESOLVED" if best["pct"] >= 95.0 and (
                runner is None or best["pct"] - runner["pct"] >= 20.0) else "WEAK"),
            "candidates_scored": len(scored),
        }
    return results


def build() -> dict:
    connection = duckdb.connect()
    connection.execute("SET enable_progress_bar=false")
    connection.execute("SET memory_limit='6GB'")
    out: dict[str, dict] = {}
    try:
        for layout, columns in sorted(layouts().items()):
            family = layout.rsplit("_L", 1)[0]
            print(f"  {layout} ...", flush=True)
            out[layout] = {"family": family, "columns": columns,
                           "identities": score_layout(connection, family, layout, columns)}
    finally:
        connection.close()
    receipt = {
        "generated": "O.9.3 arithmetic layout identity -- rate columns identify their own "
                     "operands, scored against EVERY ordered pair in the same layout so "
                     "the discrimination margin is the receipt",
        "tolerance": TOLERANCE, "min_eligible_rows": MIN_ELIGIBLE,
        "layouts": out,
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    if not args.build:
        parser.error("nothing to do -- pass --build")
    receipt = build()
    print()
    for layout, body in sorted(receipt["layouts"].items()):
        if not body["identities"]:
            print(f"  {layout:26s} (no rate column -- no arithmetic identity available)")
        for target, result in sorted(body["identities"].items()):
            if result.get("status") == "INSUFFICIENT_ROWS":
                print(f"  {layout:26s} {target:8s} INSUFFICIENT_ROWS")
                continue
            best, runner = result["best"], result["runner_up"]
            runner_text = (f"runner-up {runner['numerator']}/{runner['denominator']} "
                           f"{runner['pct']}%") if runner else "no runner-up"
            print(f"  {layout:26s} {target:8s} {result['status']:8s} "
                  f"{best['numerator']}/{best['denominator']} {best['pct']}% "
                  f"(n={best['eligible']:,})  |  {runner_text}")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
