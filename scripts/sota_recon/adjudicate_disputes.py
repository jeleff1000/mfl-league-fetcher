"""
sota_recon/adjudicate_disputes.py  --  drill the v26-vs-consensus queue to game grain.

The witness-vote queue says WHO disagrees at season grain; this lane says WHERE and WHO IS
RIGHT, by aligning three week-grain records for every disputed player-season:

    ours : v26 weekly rows
    box  : PFR per-game boxscore lines (week via nfl_team_games_all)
    pbp  : the PBP-derived weekly rollup (1978+)

The season delta then localizes to specific weeks and classifies:

  MISSING_WEEK      witnesses have a game we lack ............... we are wrong (fill)
  EXTRA_WEEK        we have a game no witness has ............... we are wrong (phantom)
  WEEK_VALUE_WRONG  same week, box==pbp != ours ................. we are wrong (cell fix,
                                                                  correct value + citation)
  WITNESS_CONFLICT  box != pbp at week grain .................... precedence ruling needed
  PAGES_QUIRK       weekly all reconcile; only the season page
                    total differs ............................... v26 likely right; witness quirk
  UNRESOLVED        delta does not localize cleanly ............. manual queue

READ-ONLY: emits the classified report + proposed corrections as CSV. Nothing is applied;
applying is Phase 3 (facts + Joe sign-off per wave).

    python -m scripts.sota_recon.adjudicate_disputes [--csv out.csv]
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

from .sources import (PLAYER_BIO, PLAYER_DEFENSE_BOX, PLAYER_OFFENSE_BOX, PBP_ROLLUP,
                      TEAM_GAMES, latest_v26)
from . import witness_votes as WV

# stat -> (v26 col, box source, box col, pbp col, tol)
DRILL = {
    "passing_yards": (PLAYER_OFFENSE_BOX, "pass_yds", "passing_yards", 1.5),
    "rushing_yards": (PLAYER_OFFENSE_BOX, "rush_yds", "rushing_yards", 1.5),
    "receiving_yards": (PLAYER_OFFENSE_BOX, "rec_yds", "receiving_yards", 1.5),
    "def_interceptions": (PLAYER_DEFENSE_BOX, "def_int", "def_interceptions", 0.5),
    "fg_made": (None, None, "fg_made", 0.5),   # box arm needs blank-zero handling in
                                               # _weekly_frames before wiring (fgm licensed
                                               # for votes since 2026-07-10)
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _disputed(con) -> dict[str, list[tuple[str, int]]]:
    """Recompute the v26-vs-consensus queue (season grain) per DRILLABLE stat."""
    out = {}
    for stat, (_, _, _, tol) in DRILL.items():
        rows = con.execute(f"""
            SELECT pfr_id, yr, array_agg(witness), array_agg(val)
            FROM ({WV._long_sql(stat)}) t(witness, pfr_id, yr, val)
            WHERE pfr_id IS NOT NULL AND yr IS NOT NULL AND val IS NOT NULL
            GROUP BY pfr_id, yr""").fetchall()
        bad = []
        for pfr_id, yr, wits, vals in rows:
            d = dict(zip(wits, vals))
            _, consensus, _, _, _ = WV._vote(d, tol, int(yr))
            v26 = d.get("v26")
            if consensus is not None and v26 is not None and abs(v26 - consensus) > tol:
                bad.append((pfr_id, int(yr)))
        out[stat] = bad
    return out


def _weekly_frames(con, stat: str, keys: list[tuple[str, int]]):
    """(pfr_id, yr, week) -> {ours, box, pbp} for all disputed keys of one stat."""
    box_src, box_col, pbp_col, _ = DRILL[stat]
    con.execute("CREATE OR REPLACE TEMP TABLE dk (pfr_id VARCHAR, yr INT)")
    con.executemany("INSERT INTO dk VALUES (?, ?)", keys)
    v26 = _q(latest_v26()); bio = _q(PLAYER_BIO); tg = _q(TEAM_GAMES)

    frames: dict[tuple, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))
    for k, yr, wk, val in con.execute(f"""
        SELECT bio.pfr_id, v.year, CAST(v.week AS INT), SUM(v.{stat})
        FROM '{v26}' v JOIN '{bio}' bio USING (NFL_player_id)
        JOIN dk ON dk.pfr_id = bio.pfr_id AND dk.yr = v.year
        WHERE v.season_type = 'REG' AND v.{stat} IS NOT NULL
        GROUP BY 1, 2, 3""").fetchall():
        frames[(k, yr)]["ours"][wk] = val
    if box_src is not None:
        for k, yr, wk, val in con.execute(f"""
            SELECT b.pid, g.year, CAST(g.week AS INT), SUM(b.x) FROM (
              SELECT regexp_extract(player_link_ids, '^([^,]+)', 1) AS pid, boxscore_id,
                     TRY_CAST({box_col} AS DOUBLE) AS x
              FROM '{_q(box_src)}' WHERE player_link_ids IS NOT NULL) b
            JOIN (SELECT DISTINCT boxscore_id, year, week FROM '{tg}'
                  WHERE season_type = 'REG') g USING (boxscore_id)
            JOIN dk ON dk.pfr_id = b.pid AND dk.yr = g.year
            WHERE b.x IS NOT NULL GROUP BY 1, 2, 3""").fetchall():
            frames[(k, yr)]["box"][wk] = val
    for k, yr, wk, val in con.execute(f"""
        SELECT bio.pfr_id, r.year, CAST(r.week AS INT), SUM(r.{pbp_col})
        FROM '{_q(PBP_ROLLUP)}' r JOIN '{bio}' bio USING (NFL_player_id)
        JOIN dk ON dk.pfr_id = bio.pfr_id AND dk.yr = r.year
        WHERE r.season_type = 'REG' AND r.{pbp_col} IS NOT NULL
        GROUP BY 1, 2, 3""").fetchall():
        frames[(k, yr)]["pbp"][wk] = val
    return frames


def _classify(f: dict[str, dict[int, float]], tol: float) -> tuple[str, list[dict]]:
    ours, box, pbp = f.get("ours", {}), f.get("box", {}), f.get("pbp", {})
    witnesses_weekly = bool(box) or bool(pbp)
    findings = []
    weeks = sorted(set(ours) | set(box) | set(pbp))
    for wk in weeks:
        o, b, p = ours.get(wk), box.get(wk), pbp.get(wk)
        wit = [x for x in (b, p) if x is not None]
        if o is None and wit and max(wit) != 0:
            findings.append(dict(week=wk, kind="MISSING_WEEK", ours=None, box=b, pbp=p))
        elif o is not None and not wit and o != 0 and witnesses_weekly:
            findings.append(dict(week=wk, kind="EXTRA_WEEK", ours=o, box=b, pbp=p))
        elif o is not None and wit:
            if all(abs(o - x) <= tol for x in wit):
                continue
            if len(wit) == 2 and abs(b - p) <= tol:
                findings.append(dict(week=wk, kind="WEEK_VALUE_WRONG", ours=o, box=b, pbp=p))
            elif len(wit) == 2:
                findings.append(dict(week=wk, kind="WITNESS_CONFLICT", ours=o, box=b, pbp=p))
            elif abs(o - wit[0]) > tol:
                findings.append(dict(week=wk, kind="WEEK_VALUE_DIFF_1WIT", ours=o, box=b, pbp=p))
    if not findings:
        return ("PAGES_QUIRK" if witnesses_weekly else "NO_WEEKLY_WITNESS"), []
    kinds = {x["kind"] for x in findings}
    if kinds <= {"MISSING_WEEK"}:
        return "MISSING_WEEK", findings
    if kinds <= {"EXTRA_WEEK"}:
        return "EXTRA_WEEK", findings
    if kinds <= {"WEEK_VALUE_WRONG", "MISSING_WEEK", "EXTRA_WEEK"}:
        return "WEEK_VALUE_WRONG", findings
    if "WITNESS_CONFLICT" in kinds:
        return "WITNESS_CONFLICT", findings
    return "UNRESOLVED", findings


def run(csv: str | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    queue = _disputed(con)
    verdicts, rows_out = Counter(), []
    for stat, keys in queue.items():
        if not keys:
            continue
        tol = DRILL[stat][3]
        frames = _weekly_frames(con, stat, keys)
        for (pfr_id, yr) in keys:
            verdict, findings = _classify(frames.get((pfr_id, yr), {}), tol)
            verdicts[f"{stat}:{verdict}"] += 1
            verdicts[f"__total:{verdict}"] += 1
            for x in (findings or [dict(week=None, kind=verdict, ours=None, box=None, pbp=None)]):
                rows_out.append(dict(stat=stat, pfr_id=pfr_id, year=yr, season_verdict=verdict,
                                     **x))
    con.close()
    if csv and rows_out:
        import csv as _csv
        with open(csv, "w", newline="", encoding="utf-8") as fh:
            w = _csv.DictWriter(fh, fieldnames=list(rows_out[0]))
            w.writeheader(); w.writerows(rows_out)
    return {"verdicts": dict(verdicts), "rows": len(rows_out)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    r = run(a.csv)
    print("SEASON-DISPUTE ADJUDICATION (who is right, per atom):")
    for k in sorted(r["verdicts"]):
        if k.startswith("__total:"):
            print(f"  TOTAL {k.split(':', 1)[1]:24s} {r['verdicts'][k]:>5,}")
    for k in sorted(r["verdicts"]):
        if not k.startswith("__total:"):
            print(f"    {k:38s} {r['verdicts'][k]:>5,}")
    print(f"finding rows: {r['rows']:,}")
