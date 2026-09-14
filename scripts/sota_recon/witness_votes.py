"""
sota_recon/witness_votes.py  --  LANE: per-atom witness VOTE enumeration (WS4c consensus arm).

For every player-season atom, enumerate how many independent witnesses agree and how many
disagree -- per cell, not in aggregate. stat_consensus does this at TEAM-GAME grain; this
lane does it at PLAYER grain using the Phase-2 witness expansion:

  pages : PFR player-page season tables (season authority)
  box   : sum of PFR per-game boxscore lines (REG games via nfl_team_games_all)
  pbp   : sum of the PBP-derived weekly rollup (1978+)

VOTES ARE PER-LINEAGE, NOT PER-TABLE. pages and box are both PFR-scraped -- correlated
copies of one bloodline -- so they reconcile into ONE PFR vote (pages preferred as PFR's
own curated season total); if they disagree beyond tolerance the PFR lineage ABSTAINS and
the cell is flagged INTRA-LINEAGE-INCONSISTENT (its own defect class: PFR disagreeing with
itself). The pbp stream is era-split: nflverse-rooted 1999+ (independent lineage),
PFR-pbp-rooted 1978-98 (folds INTO the pfr lineage -- those years have ONE independent
bloodline, and the verdict says so honestly). Legacy supertables and our own derived
tables carry witness_class subject_history/derived and can never vote (sources.py).

Verdict among independent LINEAGES (v26 never votes): UNANIMOUS / MAJORITY / SPLIT /
SINGLE. Then v26 is judged against the consensus: agrees / DISAGREES / missing. Disputed
cells (SPLIT, intra-lineage, or v26 vs consensus) are enumerated to CSV -- the queue feeds
precedence rulings (WS4b) and Phase-3 waves.

    python -m scripts.sota_recon.witness_votes [--csv out.csv]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

import duckdb

from . import sources as S
from . import witness_map as WM
from .sources import PLAYER_BIO, latest_v26

# EXACT comparison epsilon: float-representation guard ONLY (witness values are DOUBLE
# sums of integer-valued data -- exact up to 2^53). This is NOT a tolerance; any larger
# window must be a DECLARED per-stat policy in stat_contracts (§17.1).
EXACT_EPS = 1e-6


def tolerance_of(stat: str) -> float:
    """Comparison tolerance for a stat, read from its DECLARED tolerance_policy in
    stat_contracts.v1.json (§17.1: no ad-hoc TOLs in lane code). EXACT -> EXACT_EPS;
    a DECLARED policy supplies its receipted absolute tolerance. The pre-2026-07-26
    ad-hoc rule (±1.5 yards / ±0.5 counts) is dead -- a ±1 disagreement between roots
    is a conflict, not noise."""
    import json
    import os
    if not hasattr(tolerance_of, "_map"):
        p = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts",
                         "stat_contracts.v1.json")
        with open(p, encoding="utf-8") as f:
            doc = json.load(f)
        tolerance_of._map = {r["stat_id"]: r.get("tolerance_policy") or {"policy": "EXACT"}
                             for r in doc["stats"]}
    pol = tolerance_of._map.get(stat, {"policy": "EXACT"})
    if pol["policy"] == "EXACT":
        return EXACT_EPS
    return float(pol["tolerance"])


def stats_from_licensed() -> dict[str, float]:
    """Every stat with a licensed sum-grain mapping -> its DECLARED tolerance.
    The pilot hand-list retired 2026-07-10: the LICENSED map IS the stat list.
    max/value-grain specs (longs, NGS rates) are excluded -- the v26 arm of the long
    SQL sums, which is only correct for additive stats."""
    lic = WM.licensed()
    out: dict[str, float] = {}
    for m in WM.WITNESS_MAP:
        if m.agg != "sum" or m.v26_expr or m.season_type != "REG" \
                or m.validation_grain != "season":
            continue  # votes are a REG season-sum instrument; POST and week-grain
            # (NGS rate) specs vouch via their own arms
        if (m.source_key, m.v26_col) not in lic:
            continue
        out.setdefault(m.v26_col, tolerance_of(m.v26_col))
    return out

# within-lineage preference: authority (curated season totals) > appearance > oracle > team
_ROLE_RANK = {"authority": 0, "appearance": 1, "oracle": 2, "team": 3}


def _family_of(stat: str) -> str:
    """stat -> stat-family (stat_contracts.v1.json); root identity is family-scoped."""
    import json
    import os
    if not hasattr(_family_of, "_map"):
        p = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts",
                         "stat_contracts.v1.json")
        with open(p, encoding="utf-8") as f:
            doc = json.load(f)
        _family_of._map = {r["stat_id"]: r.get("family") or "*" for r in doc["stats"]}
    return _family_of._map.get(stat, "*")


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _specs_for(stat: str) -> list[WM.MapSpec]:
    """Licensed REG mappings only -- an unvalidated mapping cannot vouch, and POST
    specs never mix into the REG vote stream (witness_map.licensed)."""
    lic = WM.licensed()
    return [m for m in WM.WITNESS_MAP
            if m.v26_col == stat and m.season_type == "REG"
            and m.validation_grain == "season"
            and (m.source_key, m.v26_col) in lic]


def _long_sql(stat: str) -> str:
    parts = [f"SELECT '{m.source_key}' AS witness, w.* FROM ({WM.build_witness_sql(m)}) w"
             for m in _specs_for(stat)]
    parts.append(f"""
        SELECT 'v26', bio.pfr_id, v.year, SUM(v.{stat})
        FROM '{_q(latest_v26())}' v
        JOIN '{_q(PLAYER_BIO)}' bio USING (NFL_player_id)
        WHERE v.season_type = 'REG' AND bio.pfr_id IS NOT NULL AND v.{stat} IS NOT NULL
        GROUP BY 1, 2, 3""")
    return " UNION ALL ".join(parts)


def _lineage_of(source_key: str, year: int, family: str = "*") -> str:
    """Root resolution via the committed lineage_roots.v1.json contract (O.5).

    Era splits (pbp nflverse-1999+/PFR-1978-98) are CONTRACT DATA, never lane code --
    lineage_roots.root_of is the single resolver every voting lane must use."""
    from .lineage_roots import root_of
    return root_of(source_key, year, family)


def _lineages(vals: dict[str, float], year: int,
              family: str = "*") -> tuple[dict[str, float], list[str]]:
    """Reconcile source streams into per-ROOT votes. Returns (root->value, inconsistent).
    independent_witness_count = len(votes) = COUNT(DISTINCT root) -- copies never add."""
    reg = S.registry()
    groups: dict[str, dict[str, float]] = {}
    for k, v in vals.items():
        if k == "v26" or v is None:
            continue
        if reg[k].witness_class != "primary":
            continue  # derived / subject_history / identity NEVER vote
        groups.setdefault(_lineage_of(k, year, family), {})[k] = v
    votes, inconsistent = {}, []
    for lin, members in groups.items():
        vs = list(members.values())
        if max(vs) - min(vs) <= _lineages.tol:
            best = min(members, key=lambda k: _ROLE_RANK.get(reg[k].role, 9))
            votes[lin] = members[best]
        else:
            inconsistent.append(lin)  # lineage disagrees with itself -> abstains
    return votes, inconsistent


def _cluster(votes: dict[str, float], tol: float):
    """(verdict, consensus, n_agree, n_disagree) among a set of votes -- shared by the
    root-collapsed lane (here) and the naive arm of the flip report (lineage_flip_report)."""
    if len(votes) == 0:
        return "NO-WITNESS", None, 0, 0
    if len(votes) == 1:
        return "SINGLE", next(iter(votes.values())), 1, 0
    clusters: list[list[float]] = []
    for v in votes.values():
        for c in clusters:
            if abs(c[0] - v) <= tol:
                c.append(v)
                break
        else:
            clusters.append([v])
    clusters.sort(key=len, reverse=True)
    top = clusters[0]
    agree, disagree = len(top), len(votes) - len(top)
    if disagree == 0:
        return "UNANIMOUS", top[0], agree, 0
    if agree > len(votes) / 2:
        return "MAJORITY", top[0], agree, disagree
    return "SPLIT", None, agree, disagree


def _vote(vals: dict[str, float], tol: float, year: int, family: str = "*"):
    """(verdict, consensus, n_agree, n_disagree, intra_inconsistent) among ROOTS."""
    _lineages.tol = tol
    votes, inconsistent = _lineages(vals, year, family)
    if len(votes) == 0:
        return ("INTRA-LINEAGE-INCONSISTENT" if inconsistent else "NO-WITNESS",
                None, 0, 0, inconsistent)
    verdict, consensus, agree, disagree = _cluster(votes, tol)
    return verdict, consensus, agree, disagree, inconsistent


def run(csv: str | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    summary, disputes = {}, []
    for stat, tol in stats_from_licensed().items():
        rows = con.execute(f"""
            SELECT pfr_id, yr, array_agg(witness), array_agg(val)
            FROM ({_long_sql(stat)}) t(witness, pfr_id, yr, val)
            WHERE pfr_id IS NOT NULL AND yr IS NOT NULL AND val IS NOT NULL
            GROUP BY pfr_id, yr""").fetchall()
        cnt = defaultdict(int)
        v26_dis = intra = 0
        fam = _family_of(stat)
        for pfr_id, yr, wits, vals in rows:
            d = dict(zip(wits, vals))
            verdict, consensus, agree, disagree, inconsistent = _vote(d, tol, int(yr), fam)
            cnt[verdict] += 1
            intra += bool(inconsistent)
            v26 = d.get("v26")
            if consensus is not None and v26 is not None and abs(v26 - consensus) > tol:
                v26_dis += 1
                if len(disputes) < 5000:
                    disputes.append(dict(stat=stat, pfr_id=pfr_id, year=yr, verdict=verdict,
                                         consensus=consensus, v26=v26,
                                         witnesses={k: v for k, v in d.items() if k != "v26"}))
            elif (verdict in ("SPLIT", "INTRA-LINEAGE-INCONSISTENT") or inconsistent) \
                    and len(disputes) < 5000:
                disputes.append(dict(stat=stat, pfr_id=pfr_id, year=yr, verdict=verdict,
                                     consensus=consensus, v26=v26,
                                     witnesses={k: v for k, v in d.items() if k != "v26"}))
        summary[stat] = {"cells": len(rows), **dict(cnt),
                         "intra_lineage_inconsistent": intra,
                         "v26_disagrees_with_consensus": v26_dis}
    con.close()
    if csv and disputes:
        import csv as _csv
        with open(csv, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=["stat", "pfr_id", "year", "verdict",
                                               "consensus", "v26", "witnesses"])
            w.writeheader()
            for x in disputes:
                w.writerow({**x, "witnesses": str(x["witnesses"])})
    return {"summary": summary, "disputes": len(disputes)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    r = run(a.csv)
    for stat, s in r["summary"].items():
        total = s["cells"] or 1
        una = s.get("UNANIMOUS", 0)
        print(f"{stat:20s} cells={s['cells']:>7,}  unanimous={una:>7,} ({una/total:.1%})  "
              f"majority={s.get('MAJORITY',0):>6,}  split={s.get('SPLIT',0):>5,}  "
              f"single={s.get('SINGLE',0):>6,}  intra-lineage={s['intra_lineage_inconsistent']:>5,}  "
              f"v26-vs-consensus={s['v26_disagrees_with_consensus']:,}")
    print(f"disputes enumerated: {r['disputes']}")
