"""
sota_recon/precedence.py  --  WS4b PHASE-2 GATE: the witness precedence registry.

"Verified against witness" is ambiguous the moment two witnesses disagree. For every
stat x era band with 2+ independent LINEAGE streams in the LICENSED witness map, this
lane derives a ruling from MEASUREMENT (cross-lineage agreement on that band's actual
atoms), never from trust, and records it:

  winner    one lineage is authoritative for the band; disagreeing cells are fix-queue
            input (they do not re-open the ruling)
  disputed  streams disagree beyond tolerance on a material share; BOTH values recorded;
            never a silent pick (cells enumerated by witness_votes' dispute CSV)

Policy (deterministic, measured):
  * lineage votes reconcile exactly as in witness_votes (role authority preferred
    within a lineage; intra-lineage-inconsistent atoms abstain and are excluded here --
    they are their own defect class, not a precedence question)
  * cross-lineage agreement >= 0.98 on the band  -> ruling=winner. Winner = the pfr
    lineage when present: PFR carries the official gamebook numbers, and the measured
    degradation pattern (pbp targets 94.4%, pbp solo tackles 66.3%) shows pbp
    re-derivation is the stream that bends where charting is subtle.
  * 0.90 <= agreement < 0.98                     -> ruling=disputed (both recorded)
  * agreement < 0.90                             -> ruling=winner for the authority
    stream; the divergent stream is demoted to plausibility-only IN THE RULING REASON
    (it stays licensed for corroboration, never adjudication)
  * single-lineage bands get NO ruling: they are the depth-1 acquisition wishlist
    (witness_coverage), not a precedence question.

Every ruling is emitted as a source_precedence_decision fact (facts.py) under
--emit-facts; emission is idempotent (an identical effective ruling is not re-emitted).

    python -m scripts.sota_recon.precedence                # measure + write registry
    python -m scripts.sota_recon.precedence --emit-facts   # also record rulings as facts
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import facts as F
from . import sources as S
from . import witness_votes as WV
from . import witness_map as WM

REGISTRY_OUT = os.path.join(S.DATA_LAKE, "derived", "validation", "sota_recon_master",
                            "PRECEDENCE_REGISTRY.json")

MIN_BAND_ATOMS = 30      # a year needs this many multi-lineage atoms to enter a band
WINNER_MIN_AGREE = 0.98  # >= : clean winner
DISPUTED_MIN_AGREE = 0.90  # in [0.90, 0.98): disputed; below: winner + demotion

WAVE_ID = "phase2_precedence_gate_v1"


def _authority_source(stat: str, lineage: str, year: int) -> str:
    """The role-rank-best LICENSED source of a lineage for a stat (witness_votes rank)."""
    reg = S.registry()
    cands = [m.source_key for m in WV._specs_for(stat)
             if WV._lineage_of(m.source_key, year) == lineage]
    if not cands:
        return lineage
    return min(cands, key=lambda k: WV._ROLE_RANK.get(reg[k].role, 9))


def measure(only_stat: str | None = None) -> list[dict]:
    """Per stat: reconcile lineage votes per atom, then band years by lineage-set and
    measure cross-lineage agreement per band. Returns registry rows (multi-lineage only,
    plus depth-1 bands recorded informationally with ruling=None)."""
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    out: list[dict] = []
    for stat, tol in WV.stats_from_licensed().items():
        if only_stat and stat != only_stat:
            continue
        rows = con.execute(f"""
            SELECT pfr_id, yr, array_agg(witness), array_agg(val)
            FROM ({WV._long_sql(stat)}) t(witness, pfr_id, yr, val)
            WHERE pfr_id IS NOT NULL AND yr IS NOT NULL AND val IS NOT NULL
            GROUP BY pfr_id, yr""").fetchall()
        # per-year tallies over LINEAGE votes (v26 never participates)
        WV._lineages.tol = tol
        per_year: dict[int, dict] = defaultdict(lambda: dict(
            lineages=set(), n_multi=0, n_agree=0, n_single=0))
        for pfr_id, yr, wits, vals in rows:
            year = int(yr)
            votes, _inconsistent = WV._lineages(dict(zip(wits, vals)), year)
            y = per_year[year]
            y["lineages"] |= set(votes)
            if len(votes) >= 2:
                y["n_multi"] += 1
                vs = list(votes.values())
                if max(vs) - min(vs) <= tol:
                    y["n_agree"] += 1
            elif len(votes) == 1:
                y["n_single"] += 1
        # band years by (lineage-set, is-multi) contiguity
        years = sorted(per_year)
        bands: list[dict] = []
        for year in years:
            y = per_year[year]
            multi = y["n_multi"] >= MIN_BAND_ATOMS
            key = (frozenset(y["lineages"]), multi)
            if bands and bands[-1]["key"] == key and bands[-1]["y1"] == year - 1:
                b = bands[-1]
                b["y1"] = year
                b["n_multi"] += y["n_multi"]
                b["n_agree"] += y["n_agree"]
                b["n_single"] += y["n_single"]
            else:
                bands.append(dict(key=key, y0=year, y1=year, n_multi=y["n_multi"],
                                  n_agree=y["n_agree"], n_single=y["n_single"]))
        for b in bands:
            lineages, multi = b["key"]
            era = f"{b['y0']}-{b['y1']}"
            if not multi or len(lineages) < 2:
                out.append(dict(stat=stat, era=era, grain="player_season",
                                lineages=sorted(lineages), n_atoms=b["n_multi"] + b["n_single"],
                                agreement=None, ruling=None, winner=None,
                                reason="depth-1 band (single lineage) -- acquisition "
                                       "wishlist, not a precedence question"))
                continue
            agree = b["n_agree"] / b["n_multi"]
            win_lineage = "pfr" if "pfr" in lineages else sorted(lineages)[0]
            winner = f"{win_lineage}:{_authority_source(stat, win_lineage, b['y1'])}"
            losers = sorted(lineages - {win_lineage})
            if agree >= WINNER_MIN_AGREE:
                ruling, reason = "winner", (
                    f"measured cross-lineage agreement {agree:.1%} on {b['n_multi']:,} "
                    f"atoms; {win_lineage} carries official gamebook numbers; "
                    f"disagreeing cells -> fix queue")
            elif agree >= DISPUTED_MIN_AGREE:
                ruling, winner, reason = "disputed", None, (
                    f"cross-lineage agreement {agree:.1%} on {b['n_multi']:,} atoms is "
                    f"below the winner bar; both values recorded, cells enumerated in "
                    f"the witness_votes dispute CSV")
            else:
                ruling, reason = "winner", (
                    f"cross-lineage agreement only {agree:.1%} on {b['n_multi']:,} "
                    f"atoms: {'+'.join(losers)} demoted to plausibility-only for this "
                    f"stat x era (stays licensed for corroboration, never adjudication)")
            out.append(dict(stat=stat, era=era, grain="player_season",
                            lineages=sorted(lineages), n_atoms=b["n_multi"],
                            agreement=round(agree, 4), ruling=ruling, winner=winner,
                            reason=reason))
    con.close()
    return out


def write_registry(rows: list[dict]) -> str:
    os.makedirs(os.path.dirname(REGISTRY_OUT), exist_ok=True)
    with open(REGISTRY_OUT, "w", encoding="utf-8") as f:
        json.dump({"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                   "wave_id": WAVE_ID,
                   "policy": {"winner_min_agree": WINNER_MIN_AGREE,
                              "disputed_min_agree": DISPUTED_MIN_AGREE,
                              "min_band_atoms": MIN_BAND_ATOMS},
                   "rows": rows}, f, indent=1)
    return REGISTRY_OUT


def emit_facts(rows: list[dict]) -> int:
    """Record each RULING as a source_precedence_decision fact. Idempotent: skips
    (stat, era, grain) whose newest fact already carries the same ruling+winner."""
    snapshot = (f"{Path(WM.LICENSES).name}@{datetime.now(timezone.utc):%Y%m%d} + "
                f"{Path(S.latest_v26()).parent.parent.name}")
    con = F.connect()
    try:
        existing = {}
        for stat, era, grain, ruling, winner in con.execute(
                """SELECT stat, era, grain, ruling, winner FROM (
                     SELECT *, ROW_NUMBER() OVER (PARTITION BY stat, era, grain
                                                  ORDER BY created_at DESC) rn
                     FROM source_precedence_decision) WHERE rn = 1""").fetchall():
            existing[(stat, era, grain)] = (ruling, winner)
        n = 0
        for r in rows:
            if r["ruling"] is None:
                continue  # depth-1 bands are wishlist entries, not rulings
            key = (r["stat"], r["era"], r["grain"])
            if existing.get(key) == (r["ruling"], r["winner"]):
                continue
            F.emit_fact(
                "source_precedence_decision", con=con,
                wave_id=WAVE_ID, reason=r["reason"],
                witness=f"lineages={'+'.join(r['lineages'])} "
                        f"agreement={r['agreement']} n={r['n_atoms']}",
                source_snapshot_id=snapshot,
                stat=r["stat"], era=r["era"], grain=r["grain"],
                ruling=r["ruling"], winner=r["winner"])
            n += 1
        return n
    finally:
        con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-facts", action="store_true")
    ap.add_argument("--stat", default=None, help="measure a single stat (smoke test; "
                                                 "registry is NOT written)")
    a = ap.parse_args()
    rows = measure(a.stat)
    path = write_registry(rows) if not a.stat else "(partial run -- registry not written)"
    rulings = [r for r in rows if r["ruling"]]
    depth1 = [r for r in rows if not r["ruling"]]
    print(f"{'stat':26s} {'era':11s} {'lineages':22s} {'n':>8s} {'agree':>7s}  ruling")
    for r in rulings:
        ag = f"{r['agreement']:.1%}" if r["agreement"] is not None else "-"
        win = f" -> {r['winner']}" if r["winner"] else ""
        print(f"{r['stat']:26s} {r['era']:11s} {'+'.join(r['lineages']):22s} "
              f"{r['n_atoms']:>8,} {ag:>7s}  {r['ruling'].upper()}{win}")
    print(f"\nrulings: {len(rulings)}  depth-1 bands (wishlist): {len(depth1)}")
    print(f"registry: {path}")
    if a.emit_facts:
        if a.stat:
            raise SystemExit("--emit-facts requires a FULL measure run (drop --stat)")
        n = emit_facts(rows)
        print(f"facts emitted: {n} (identical effective rulings skipped)")
