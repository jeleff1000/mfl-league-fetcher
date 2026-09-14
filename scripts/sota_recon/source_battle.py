"""
sota_recon/source_battle.py  --  O.5: the SOURCE TOURNAMENT v0 (master plan §25.5, addendum E)

Pairwise battles between independent lineage ROOTS per comparable:
  (root_A x root_B x stat x era x grain x season_type)

A comparable exists where two DIFFERENT roots hold a licensed mapping for the same v26
stat with overlapping year windows. Copies inside one root never battle each other --
intra-root disagreement is the copy-consistency lane's business (witness_votes). v0 scope:
MODERN core families (passing / rushing / receiving / scoring) at player-season grain,
REG, eras 1978-98 and 1999-2025. In 1978-98 the pbp stream folds into the pfr root
(lineage_roots.v1.json), so most core stats have ONE root there: that era emits
root-poor rows, not fake battles.

Per battle, the typed geometry of disagreement (§20.4/20.5):
  shared / only_A / only_B         key overlap + missing-row asymmetry (only_* counts
                                   inherit each source's DECLARED coverage model --
                                   SPARSE_POSITIVE_ONLY vs PARTICIPANT_ONLY etc.; read
                                   them through kc_planes.v1.json's C plane, e.g. a
                                   dense-zero rollup vs a positive-only pages table)
  null asymmetry                   value-null rates on shared keys
  exact / tolerance match          agreement rates (tolerance = unit-family rule)
  signed delta distribution        mean, median, p95(|d|), share positive
  rounding signature               share of mismatch deltas divisible by 5 / 10
  digit-transposition signature    share of mismatch |deltas| in {9,18,...,81}
  untyped conflicts                mismatches matching NO known signature -> queue

Adjudication: v0 records conflicts with their geometry; `adjudicated_wins` stays
UNADJUDICATED until a receipted ruling exists (proof-or-pending -- no reliability claim
without a holdout, §25.5 zero-counter).

LOROO (leave-one-root-out), SPECIFIED here and unit-tested in test_source_battle.py:
when judging root A on a comparable, the reference consensus is built ONLY from roots
other than A and A's descendants (loroo_consensus). With exactly 2 comparable roots the
reference degenerates to one root and LOROO is typed DEGENERATE_LT3_ROOTS -- the harness
runs, reports honestly, and becomes decisive as root diversity grows (newspaper/pfa_loc
eras; future nflcom).

Outputs:
  docs/source-battle-v0.json                       committed battle table + zero-counters
  {receipts}/source_battle/battles.csv             regenerable receipt copy

Run:  python -m scripts.sota_recon.source_battle
"""

from __future__ import annotations

import argparse
import json
import os

import duckdb

from . import sources as S
from . import witness_map as WM
from .lineage_roots import root_of
from .witness_votes import _ROLE_RANK, _family_of, tolerance_of

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "source-battle-v0.json")
LANE = "source_battle"

GRAIN = "player_season"
SEASON_TYPE = "REG"
ERAS = [("1978_98", 1978, 1998), ("1999_2025", 1999, 2025)]
CORE_FAMILIES = {"passing", "rushing", "receiving"}
# the scoring-family authority: licensed stats sourced from it join the core scope
SCORING_SOURCE = "pfr_player_scoring"

# digit-transposition |delta| signature: |ab - ba| = 9*|a-b| for 2-digit transposes
_TRANSPOSE_DELTAS = tuple(9 * k for k in range(1, 10))


def _tol(stat: str) -> float:
    """DECLARED tolerance from stat_contracts (§17.1) -- EXACT (float-eps) unless a
    receipted per-stat policy says otherwise. Never an ad-hoc constant here."""
    return tolerance_of(stat)


def core_stats() -> dict[str, list[WM.MapSpec]]:
    """Licensed REG season-sum specs for the modern core families, grouped by stat."""
    lic = WM.licensed()
    out: dict[str, list[WM.MapSpec]] = {}
    for m in WM.WITNESS_MAP:
        if m.agg != "sum" or m.v26_expr or m.season_type != "REG" \
                or m.validation_grain != "season":
            continue
        if (m.source_key, m.v26_col) not in lic:
            continue
        fam = _family_of(m.v26_col)
        if fam in CORE_FAMILIES or m.source_key == SCORING_SOURCE:
            out.setdefault(m.v26_col, []).append(m)
    return out


def _root_specs(specs: list[WM.MapSpec], lo: int, hi: int) -> dict[str, WM.MapSpec]:
    """Preferred spec per ROOT over an era window (role rank inside a root; the root's
    curated authority beats its box copies -- copies never get their own column)."""
    reg = S.registry()
    mid = (lo + hi) // 2
    by_root: dict[str, WM.MapSpec] = {}
    for m in specs:
        src = reg[m.source_key]
        if src.year_max < lo or src.year_min > hi:
            continue
        root = root_of(m.source_key, mid, _family_of(m.v26_col))
        cur = by_root.get(root)
        if cur is None or _ROLE_RANK.get(src.role, 9) < _ROLE_RANK.get(reg[cur.source_key].role, 9):
            by_root[root] = m
    return by_root


def loroo_consensus(votes_by_root: dict[str, float], judged_root: str, tol: float,
                    descendants: dict[str, set[str]] | None = None):
    """Leave-one-root-out reference: consensus over roots EXCLUDING the judged root and
    its descendants. Returns (consensus_value | None, n_reference_roots, status).
    status: OK (>=2 reference roots agree) / DEGENERATE_LT3_ROOTS (reference is a single
    root -- comparison possible, independent confirmation not) / NO_REFERENCE."""
    desc = (descendants or {}).get(judged_root, set())
    ref = {r: v for r, v in votes_by_root.items() if r != judged_root and r not in desc}
    if not ref:
        return None, 0, "NO_REFERENCE"
    if len(ref) == 1:
        return next(iter(ref.values())), 1, "DEGENERATE_LT3_ROOTS"
    vals = sorted(ref.values())
    med = vals[len(vals) // 2]
    agree = sum(1 for v in vals if abs(v - med) <= tol)
    if agree >= 2:
        return med, len(ref), "OK"
    return None, len(ref), "NO_REFERENCE"  # reference roots themselves split


def _battle(con: duckdb.DuckDBPyConnection, stat: str, era: str, lo: int, hi: int,
            root_a: str, spec_a: WM.MapSpec, root_b: str, spec_b: WM.MapSpec) -> dict:
    tol = _tol(stat)
    row = con.execute(f"""
        WITH a AS ({WM.build_witness_sql(spec_a)}),
             b AS ({WM.build_witness_sql(spec_b)}),
             j AS (SELECT COALESCE(a.pfr_id, b.pfr_id) pfr_id,
                          COALESCE(a.yr, b.yr) yr, a.val av, b.val bv
                   FROM a FULL OUTER JOIN b ON a.pfr_id = b.pfr_id AND a.yr = b.yr
                   WHERE COALESCE(a.yr, b.yr) BETWEEN {lo} AND {hi}),
             d AS (SELECT av - bv AS delta, ABS(av - bv) AS ad FROM j
                   WHERE av IS NOT NULL AND bv IS NOT NULL)
        SELECT
          (SELECT COUNT(*) FROM j WHERE av IS NOT NULL AND bv IS NOT NULL) shared,
          (SELECT COUNT(*) FROM j WHERE av IS NOT NULL AND bv IS NULL)     only_a,
          (SELECT COUNT(*) FROM j WHERE av IS NULL AND bv IS NOT NULL)     only_b,
          (SELECT COUNT(*) FROM d WHERE ad = 0)                            exact,
          (SELECT COUNT(*) FROM d WHERE ad <= {tol})                       within_tol,
          (SELECT AVG(delta) FROM d)                                       delta_mean,
          (SELECT MEDIAN(delta) FROM d)                                    delta_median,
          (SELECT QUANTILE_CONT(ad, 0.95) FROM d)                          abs_delta_p95,
          (SELECT COUNT(*) FROM d WHERE delta > {tol})                     a_higher,
          (SELECT COUNT(*) FROM d WHERE delta < -{tol})                    b_higher,
          (SELECT COUNT(*) FROM d WHERE ad > {tol} AND (delta % 5) = 0)    mism_mod5,
          (SELECT COUNT(*) FROM d WHERE ad > {tol} AND (delta % 10) = 0)   mism_mod10,
          (SELECT COUNT(*) FROM d WHERE ad > {tol}
             AND CAST(ad AS INT) IN {_TRANSPOSE_DELTAS})                   mism_transpose
        """).fetchone()
    (shared, only_a, only_b, exact, within_tol, dmean, dmed, dp95,
     a_hi, b_hi, mod5, mod10, transpose) = row
    mismatch = shared - within_tol
    typed = min(mismatch, mod5 + transpose)  # signatures may overlap; conservative floor
    return {
        "stat": stat, "family": _family_of(stat), "era": era,
        "grain": GRAIN, "season_type": SEASON_TYPE,
        "root_a": root_a, "source_a": spec_a.source_key,
        "root_b": root_b, "source_b": spec_b.source_key,
        "tolerance": tol,
        "shared_keys": shared, "only_a": only_a, "only_b": only_b,
        "exact_match": exact, "within_tolerance": within_tol,
        "exact_rate": round(exact / shared, 6) if shared else None,
        "tolerance_rate": round(within_tol / shared, 6) if shared else None,
        "mismatch": mismatch,
        "delta_mean": None if dmean is None else round(dmean, 4),
        "delta_median": dmed,
        "abs_delta_p95": dp95,
        "a_higher": a_hi, "b_higher": b_hi,
        "mismatch_mod5": mod5, "mismatch_mod10": mod10,
        "mismatch_digit_transpose": transpose,
        "untyped_conflicts": max(0, mismatch - typed),
        "adjudicated_wins": "UNADJUDICATED",
        "loroo_status": "DEGENERATE_LT3_ROOTS",  # exactly 2 roots per modern comparable
    }


def run(stats: list[str] | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    groups = core_stats()
    if stats:
        groups = {k: v for k, v in groups.items() if k in set(stats)}
    battles: list[dict] = []
    root_poor: list[dict] = []
    comparable = battled = 0
    for stat in sorted(groups):
        for era, lo, hi in ERAS:
            by_root = _root_specs(groups[stat], lo, hi)
            roots = sorted(by_root)
            if len(roots) < 2:
                root_poor.append({"stat": stat, "era": era, "roots": roots,
                                  "reason": "single-root comparable -- no adversary "
                                            "(root-diversity closure queue)"})
                continue
            for i in range(len(roots)):
                for j in range(i + 1, len(roots)):
                    comparable += 1
                    battles.append(_battle(con, stat, era, lo, hi,
                                           roots[i], by_root[roots[i]],
                                           roots[j], by_root[roots[j]]))
                    battled += 1
    con.close()
    return {
        "version": "v0",
        "scope": {"grain": GRAIN, "season_type": SEASON_TYPE,
                  "eras": [e[0] for e in ERAS],
                  "families": sorted(CORE_FAMILIES) + ["scoring (via pfr_player_scoring)"],
                  "stats": sorted(groups)},
        "zero_counters": {
            "comparable_pairs_never_battled": comparable - battled,
            "battles_missing_typed_geometry": 0,  # every battle row carries the geometry block
            "reliability_claims_without_adjudicated_holdouts": 0,  # v0 makes NO claims
        },
        "untyped_conflict_total": sum(b["untyped_conflicts"] for b in battles),
        "n_battles": len(battles),
        "battles": battles,
        "root_poor_comparables": root_poor,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stat", action="append", default=None)
    ap.add_argument("--no-write", action="store_true")
    a = ap.parse_args()
    doc = run(stats=a.stat)
    print(f"battles={doc['n_battles']}  root-poor comparables={len(doc['root_poor_comparables'])}  "
          f"never-battled={doc['zero_counters']['comparable_pairs_never_battled']}")
    for b in doc["battles"]:
        tr = f"{b['tolerance_rate']:.2%}" if b["tolerance_rate"] is not None else "n/a"
        print(f"  {b['stat']:24s} {b['era']:9s} {b['root_a']}({b['source_a']}) vs "
              f"{b['root_b']}({b['source_b']}): shared={b['shared_keys']:,} "
              f"tol={tr} onlyA={b['only_a']:,} onlyB={b['only_b']:,} "
              f"untyped={b['untyped_conflicts']:,}")
    if a.no_write:
        return 0
    from .recon_common import lane_dir, new_run_dir, utc_stamp
    doc["generated_utc"] = utc_stamp()
    if a.stat:
        doc["partial_scope"] = sorted(a.stat)
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    out = lane_dir(new_run_dir(), LANE)
    if doc["battles"]:
        import csv
        with open(os.path.join(out, "battles.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(doc["battles"][0]))
            w.writeheader()
            w.writerows(doc["battles"])
    print(f"summary -> {os.path.abspath(SUMMARY_PATH)}")
    print(f"receipts -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
