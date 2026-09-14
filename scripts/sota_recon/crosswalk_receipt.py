"""
sota_recon/crosswalk_receipt.py  --  O.5/§19.2: the bio pfr_id<->NFL_player_id ALIAS RECEIPT

Proof-or-pending: no lane may join across the NFL_player_id / pfr_id key spaces without a
passing receipt. This builder MEASURES the bio crosswalk and writes the receipt contract
  scripts/sota_recon/witness_gate/contracts/crosswalk_receipts.v1.json

Gates (all must PASS for the receipt to license the crosswalk):
  bijection      0 NFL_player_ids mapping to >1 pfr_id AND 0 pfr_ids mapping to >1
                 NFL_player_id over rows carrying both ids (the twins hazard is exactly
                 what this counter watches -- any violation is a finding, never a
                 silent pick)
  coverage       per dependent source: share of its distinct NFL_player_ids that resolve
                 to a pfr_id. Coverage is RECORDED per source; unresolved ids become the
                 K-plane's null_key_rows (typed, counted), not an excuse to block the
                 join for the resolvable 97-100%.

The receipt licenses the 4 formerly-PENDING_CROSSWALK kc contracts (ngs x2,
pbp_player_week_rollup, legacy supertable). kc_planes cites it; test_kc_planes enforces
receipt-before-ACTIVE; recon_kc_planes executes the crosswalked joins.

Re-measure + rewrite:  python -m scripts.sota_recon.crosswalk_receipt
(The committed receipt pins the measured numbers; re-running against a changed bio is
the §19 re-verification path -- gates flipping to FAIL is a red build, not a mystery.)
"""

from __future__ import annotations

import json
import os

import duckdb

from .sources import registry

RECEIPT_PATH = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts",
                            "crosswalk_receipts.v1.json")
RECEIPT_ID = "bio_pfr_nflid"
DEPENDENT_SOURCES = ("ngs_season_published", "ngs_weekly_raw",
                     "pbp_player_week_rollup", "legacy_motherduck_supertable")


def _posix(p: str) -> str:
    return p.replace("\\", "/")


def measure() -> dict:
    reg = registry()
    bio = _posix(reg["player_bio"].path)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    n_rows, both_ids, n_nflid, n_pfrid = con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE NFL_player_id IS NOT NULL AND pfr_id IS NOT NULL),
               COUNT(DISTINCT NFL_player_id) FILTER (WHERE pfr_id IS NOT NULL),
               COUNT(DISTINCT pfr_id)
        FROM '{bio}'""").fetchone()
    nflid_multi = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT NFL_player_id FROM '{bio}'
        WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT pfr_id) > 1)""").fetchone()[0]
    pfr_multi = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT pfr_id FROM '{bio}'
        WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id) > 1)""").fetchone()[0]
    coverage = {}
    for sid in DEPENDENT_SOURCES:
        p = _posix(reg[sid].path)
        n, cov = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE x.pfr_id IS NOT NULL)
            FROM (SELECT DISTINCT NFL_player_id FROM '{p}'
                  WHERE NFL_player_id IS NOT NULL) s
            LEFT JOIN (SELECT DISTINCT NFL_player_id, pfr_id FROM '{bio}'
                       WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) x
              USING (NFL_player_id)""").fetchone()
        coverage[sid] = {"distinct_ids": n, "crosswalked": cov,
                         "coverage": round(cov / n, 6) if n else None}
    con.close()
    bijection_pass = (nflid_multi == 0 and pfr_multi == 0)
    return {
        "version": "v1",
        "receipts": [{
            "receipt_id": RECEIPT_ID,
            "claim": "bio (NFL_player_id, pfr_id) pairs form a bijection usable as the "
                     "NFL_player_id->pfr_id join for K/C-plane and voting-lane joins",
            "crosswalk": {"via": "player_bio", "on": "NFL_player_id", "adds": "pfr_id"},
            "measured": {
                "bio_rows": n_rows,
                "rows_with_both_ids": both_ids,
                "distinct_nfl_player_id_mapped": n_nflid,
                "distinct_pfr_id": n_pfrid,
                "nflid_to_multiple_pfr": nflid_multi,
                "pfr_to_multiple_nflid": pfr_multi,
                "dependent_source_coverage": coverage,
            },
            "gates": {
                "bijection": "PASS" if bijection_pass else "FAIL",
            },
            "status": "PASS" if bijection_pass else "FAIL",
            "licenses": list(DEPENDENT_SOURCES),
            "unresolved_id_policy": "unresolved NFL_player_ids surface as K-plane "
                                    "null_key_rows (typed, counted) -- never dropped "
                                    "silently, never COALESCE'd",
        }],
    }


def load() -> dict:
    with open(RECEIPT_PATH, encoding="utf-8") as f:
        return json.load(f)


def get(receipt_id: str = RECEIPT_ID) -> dict:
    for r in load()["receipts"]:
        if r["receipt_id"] == receipt_id:
            return r
    raise KeyError(receipt_id)


def main() -> int:
    doc = measure()
    from .recon_common import utc_stamp
    doc["measured_utc"] = utc_stamp()
    with open(RECEIPT_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    r = doc["receipts"][0]
    m = r["measured"]
    print(f"receipt {r['receipt_id']}: {r['status']}")
    print(f"  pairs={m['rows_with_both_ids']:,}  1:N violations "
          f"nflid={m['nflid_to_multiple_pfr']} pfr={m['pfr_to_multiple_nflid']}")
    for sid, c in m["dependent_source_coverage"].items():
        print(f"  {sid:32s} {c['crosswalked']:>7,}/{c['distinct_ids']:>7,} ({c['coverage']:.2%})")
    print(f"-> {RECEIPT_PATH}")
    return 0 if r["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
