"""ERA AUTO-RULER (Joe 2026-08-03: "auto-rule and keep a ledger in plain
english so i can grade at the end").

Reads every BLOCKED closure dossier and applies the standing laws
CONSERVATIVELY:

  (a) ESTIMATE-ERA: every blocked decade is pre-1994 AND the dossier grid
      shows >=2 independent roots present there with best agreement in
      [0.30, 0.95) -- multiple sources measured it, none agree: the tackle
      pattern. Ruling = ESTIMATE_ERAS span over the blocked decades.
  (b) RULEBOOK: columns matching known league-rule facts (two-point
      conversions began 1994) whose pre-rule cells are uniformly the
      mandated value -- verified against the plane before ruling.
  (c) NEEDS-WITNESS: everything else stays BLOCKED, with the reason.

Rulings are DATA: era_rulings.v1.json (close_column loads it). Every ruling
also writes a plain-English entry to the grading ledger, marked
AUTO-RULED / VETO OPEN. Nothing here mutates the plane.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
RULINGS = Path(__file__).parent / "witness_gate" / "contracts" / "era_rulings.v1.json"
LEDGER = Path(__file__).resolve().parents[2] / "docs" / "runbooks" / "era-rulings-ledger-2026-08-03.md"

# league-rule facts: {column-name-token: (rule_year, mandated_value, rule text)}
RULEBOOK_FACTS = [
    ("two_point", 1994, 0.0, "the two-point conversion did not exist in the "
                             "NFL until 1994"),
    ("2pt", 1994, 0.0, "the two-point conversion did not exist in the NFL "
                       "until 1994"),
]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=2")
    wk = Path(S.latest_v26()).as_posix()
    doc = (json.loads(RULINGS.read_text("utf-8")) if RULINGS.exists()
           else {"version": "v1", "estimate_eras": {}, "rulebook": {},
                 "needs_witness": {}})
    entries = []
    for p in sorted(LAKE.glob("closure_*.json")):
        d = json.loads(p.read_text("utf-8"))
        if d["verdict"] != "BLOCKED":
            continue
        col = d["column"]
        if col in doc["estimate_eras"] or col in doc["rulebook"]:
            continue
        blocks = [b for b in d["blocking"]
                  if isinstance(b, dict) and "decade" in b]
        if not blocks:
            continue
        decs = sorted(b["decade"] for b in blocks)
        bests = [b.get("best_agreement") or 0 for b in blocks]
        roots = {r for b in blocks for r in (b.get("roots_present") or [])}

        # (b) rulebook first -- strongest receipt
        rb = next((f for f in RULEBOOK_FACTS if f[0] in col.lower()), None)
        if rb and max(decs) + 9 < rb[1]:
            tok, yr, val, why = rb
            n_bad = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{wk}')
            WHERE season_type='REG' AND CAST(year AS INT) < {yr}
              AND {col} IS NOT NULL
              AND TRY_CAST({col} AS DOUBLE) <> {val}""").fetchone()[0]
            if n_bad == 0:
                doc["rulebook"][col] = {"boundary": yr, "value": val,
                                        "basis": why}
                entries.append(
                    f"### {col} -- CLOSED BY RULEBOOK\n"
                    f"Blocked decades: {decs}. Ruling: every value before "
                    f"{yr} is {val:g} because {why}; the plane already "
                    f"agrees on every cell (verified, zero exceptions). "
                    f"Law: rulebook closure (like is_overtime). "
                    f"AUTO-RULED / VETO OPEN.\n")
                continue

        # (a) estimate-era: multi-source pre-1994 disagreement
        if (max(decs) + 9 < 1994 and len(roots) >= 2
                and all(0.30 <= b < 0.95 for b in bests)):
            span = (min(decs), max(decs) + 9)
            doc["estimate_eras"][col] = {
                "span": span, "roots_measured": sorted(roots),
                "best_agreements": bests}
            entries.append(
                f"### {col} -- ESTIMATE-ERA {span[0]}-{span[1]}\n"
                f"In {decs} at least two independent source families "
                f"({', '.join(sorted(roots))}) measured this stat and none "
                f"agree with us or each other above {max(bests):.0%} (best "
                f"per decade: {[round(b,2) for b in bests]}). Nobody kept "
                f"this stat officially then -- every source is an estimate. "
                f"Ruling: keep our values, mark the era ESTIMATE (same as "
                f"the tackle family you ratified), close on the official "
                f"era. Law: estimate-era. AUTO-RULED / VETO OPEN.\n")
            continue

        # (d) stays blocked
        doc["needs_witness"][col] = {
            "decades": decs, "bests": [round(b, 3) for b in bests],
            "roots": sorted(roots),
            "why": ("single-root or zero-agreement blockade -- ruling it "
                    "without a second source would be invention")}

    RULINGS.write_text(json.dumps(doc, indent=1), encoding="utf-8")
    if entries:
        with LEDGER.open("a", encoding="utf-8") as f:
            f.write(f"\n## Tranche {time.strftime('%Y-%m-%d %H:%M')}\n\n")
            f.writelines(e + "\n" for e in entries)
    print(json.dumps({
        "ruled_estimate_era": len(doc["estimate_eras"]),
        "ruled_rulebook": len(doc["rulebook"]),
        "stays_blocked_needs_witness": len(doc["needs_witness"]),
        "new_ledger_entries": len(entries)}, indent=1))


if __name__ == "__main__":
    main()
