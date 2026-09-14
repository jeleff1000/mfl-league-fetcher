"""THE DEMAND-JOIN LANE: what it takes to earn a new supertable column.

WHY THIS EXISTS. `NEW_SUPERTABLE_COLUMN_CANDIDATE` has always carried the reason text
"routes through the census + demand join (24.4), never ad-hoc". As of 2026-07-31 that
phrase appeared in exactly two places -- the disposition's own reason string and the
scoreboard's description of it -- and nowhere else in the package. There was no demand-join
module, no candidate ledger and no counter, so every candidate ever raised was parked in
prose and none could be burned down. `def_interception_long` reached full demand evidence
(two independent sources agreeing at 99.23%) with nowhere to file it.

A candidate is NOT a wish. Adding a column to the supertable is a promote-class change, so
the bar is evidence a reviewer can check without re-deriving it:

  PUBLISHERS   at least two INDEPENDENT sources that publish the statistic. One source
               proposing a column is a request; two sources agreeing on its values is a
               statistic. (Two sources that merely both have a column of that NAME is not
               enough -- see AGREEMENT.)
  AGREEMENT    measured pairwise agreement between publishers on a common key base, with
               its n. This is what distinguishes "both sites have a column called LNG"
               from "both sites report the same number".
  CONTROL      a crossed control that rules out the obvious alternative reading, exactly as
               a mapping adjudication requires. def_interception_long: `lng` vs the PFR
               season YARDAGE total reads 52.77% where PFR's own long reads 99.23%.
  CONSTRAINT   a bound or internal identity the values satisfy, checked on each publisher
               independently. A long must not exceed its season total; a subset must not
               exceed its superset.
  SUPPLY       which source would populate it and over how many rows -- because a column we
               can define and cannot fill is a different decision from one we can fill.

STATUS is DERIVED from those fields, never hand-typed. This programme has already paid for
a hand-typed status: six nflcom families sat PENDING_CROSSWALK for days after their receipt
PASSED because `kc_planes.generate()` short-circuited on a typed string before the receipt
gate ran. `evaluate()` below is the only thing that decides EVIDENCED.

WHAT THIS LANE DOES NOT DO. It does not add columns. It produces the evidence pack and the
queue; the promote is Joe's, and `remedy_applied` stays false until it happens.

    python -m scripts.sota_recon.column_demand
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

from .column_dossier import load_decisions, row_key

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "witness_gate" / "contracts" / "column_demand.v1.json"
DOSSIER = HERE.parents[1] / "docs" / "column-dossier.json"

#: A demand record must carry all of these to be EVIDENCED. Absence is not an error -- it
#: is the difference between a candidate with a case and a candidate without one.
REQUIRED = ("proposed_canonical", "unit", "aggregation_class", "publishers",
            "agreement", "crossed_control", "constraint", "supply",
            "lower_layer_denominator")

#: Two publishers is the floor. It is not arbitrary: a single source proposing a column
#: cannot be distinguished from that source's own parsing artefact -- `ko` on K Career is
#: a header nflcom publishes and never fills, and it looked exactly like a candidate.
MIN_PUBLISHERS = 2

#: Below this, "two sources agree" is not a finding. Set at the same 99.5% the table-audit
#: uses for a clean mapping: a statistic two sources define identically should agree at
#: essentially the mapping bar, not merely correlate.
MIN_AGREEMENT = 0.95


def _load() -> dict:
    if not CONTRACT.exists():
        return {"contract_version": "1", "law": __doc__.strip().splitlines()[0], "demands": []}
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def evaluate(record: dict) -> tuple[str, list[str]]:
    """STATUS IS DERIVED. Returns (status, reasons_it_is_not_evidenced)."""
    missing = [f for f in REQUIRED if not record.get(f)]
    if missing:
        return "DECLARED", [f"missing: {', '.join(missing)}"]
    why = []
    denominator = record["lower_layer_denominator"]
    if denominator.get("status") != "PASS":
        why.append(
            "lower-layer denominator/constraint did not pass: "
            + str(denominator.get("reason") or "status is not PASS")
        )
    pubs = record.get("publishers") or []
    if len(pubs) < MIN_PUBLISHERS:
        why.append(f"only {len(pubs)} publisher(s); {MIN_PUBLISHERS} required")
    roots = {p.get("lineage_root") for p in pubs if p.get("lineage_root")}
    if len(roots) < MIN_PUBLISHERS:
        why.append(f"publishers span {len(roots)} lineage root(s); independence unproven")
    best = max((a.get("agree_pct") or 0) for a in record["agreement"]) if record["agreement"] else 0
    if best < MIN_AGREEMENT:
        why.append(f"best pairwise agreement {best} < {MIN_AGREEMENT}")
    return ("EVIDENCED" if not why else "INSUFFICIENT"), why


def candidates(source: str | None = None) -> list[dict]:
    """Every NEW_SUPERTABLE_COLUMN_CANDIDATE row in the dossier -- the lane's denominator.

    THE DENOMINATOR IS THE DOSSIER, NOT THE CONTRACT. Counting only rows someone wrote a
    demand record for would read zero on an empty lane, which is the failure this whole
    programme keeps re-learning.
    """
    dossier = json.loads(DOSSIER.read_text(encoding="utf-8"))
    dec = load_decisions()
    out = []
    for row in dossier["rows"]:
        if row["disposition"] != "NEW_SUPERTABLE_COLUMN_CANDIDATE":
            continue
        if source and row["source"] != source:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        out.append({"key": key, "source": row["source"],
                    "table_key": row["table_key"], "column": row["column"],
                    "reason": (dec.get(key) or {}).get("reason")})
    return out


def build(source: str | None = None) -> dict:
    doc = _load()
    by_key = {d["key"]: d for d in doc["demands"]}
    cands = candidates(source)
    statuses, rows = collections.Counter(), []
    for c in cands:
        rec = by_key.get(c["key"])
        if rec is None:
            statuses["NO_RECORD"] += 1
            rows.append({**c, "status": "NO_RECORD", "why": ["no demand record written"]})
            continue
        status, why = evaluate(rec)
        statuses[status] += 1
        rows.append({**c, "status": status, "why": why,
                     "proposed_canonical": rec.get("proposed_canonical")})
    orphans = sorted(set(by_key) - {c["key"] for c in cands})
    return {"counters": {
                "column_demand_candidates": len(cands),
                # the queue: a candidate nobody has built a case for
                "column_demand_candidates_without_evidence":
                    len(cands) - statuses["EVIDENCED"],
                "column_demand_evidenced_awaiting_promote": statuses["EVIDENCED"],
                # a record whose dossier row no longer exists -- never silently dropped,
                # same law as column_dispositions_orphaned
                "column_demand_records_orphaned": len(orphans)},
            "statuses": dict(statuses), "orphans": orphans, "rows": rows}


def main() -> int:
    report = build()
    for k, v in report["counters"].items():
        print(f"  {k:52s} {v:5,}")
    print()
    for status, n in collections.Counter(r["status"] for r in report["rows"]).most_common():
        print(f"  {status:14s} {n:4d}")
    ev = [r for r in report["rows"] if r["status"] == "EVIDENCED"]
    if ev:
        print("\nEVIDENCED, awaiting promote:")
        for r in ev:
            print(f"   {r['source']}|{r['table_key']}|{r['column']}"
                  f"  -> {r.get('proposed_canonical')}")
    ins = [r for r in report["rows"] if r["status"] == "INSUFFICIENT"]
    if ins:
        print("\nINSUFFICIENT -- a case was made and does not clear the bar:")
        for r in ins:
            print(f"   {r['column']:10s} {'; '.join(r['why'])}")
    if report["orphans"]:
        print(f"\nORPHANED records: {report['orphans']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
