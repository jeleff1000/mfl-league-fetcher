"""Per-metric stabilization: split-half reliability of each ladder metric vs cohort size.

Reliability = split-half Pearson correlation of per-player metric values across two random
halves of n league-years, restricted to high-appearance ("elite") players (apples-to-apples;
marginal one-league players are pure noise and dominate any unrestricted pool).

Reports, per metric, the interpolated n where reliability crosses r = 0.70, 0.85 and 0.95
(handoff 2026-07-18 §0.1b) and writes the VERSIONED threshold table
(cohort_aggregates/ladder_thresholds.json) that the wide-bundle ladder and the ADP
source-selection gate against.

Adopted into the repo 2026-07-19 (previously lived only in session scratchpads / inline in
the 07-17 runbook). Two derivation-quality fixes vs the scratchpad original:
  * reads through LocalReader, so the real-vs-sampled double-count guard, the
    denominator-ghost gate AND the points-sanity gate all apply (the original unioned
    corp+real blindly -- troll-scored leagues and never-played shells polluted the curves);
  * league-years are drawn only from gated league_settings, matching what cohorts serve.

Heavy: pulls per-(league-year, player) aggregates into Python. Run when the box is quiet
(NOT alongside a cohort rebuild on the 12GB box).
"""
from __future__ import annotations
import os

import argparse
import json
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sleeper_corpus"))
from local_reader import LocalReader  # noqa: E402

OUT_DEFAULT = Path(os.environ.get("RESEARCH_OUT_DIR", os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))) / "ladder_thresholds.json"
TARGETS = (0.75, 0.85, 0.95)   # Joe 2026-07-20 (was 0.70); only n_r85 is consumed downstream
NGRID = [50, 100, 200, 400, 800, 1600, 3200, 6400, 12800]

# stab metric -> the wide-bundle MIN_STABLE class it calibrates
CLASS_OF = {"start_pct": "srate", "waiver_add": "rate", "adp": "adp", "waiver_score": "wscore",
            "draft_score": "dscore", "clutch": "clutch", "champ": "champ", "win_pct": "win",
            # three playoff-rate shapes (Joe 2026-07-19 §2.4) kept as SEPARATE classes so the
            # r-crossings say which basis carries signal -- the evidence picks what ships
            "po_wkwt": "po_wkwt", "po_final": "po_final", "po_started": "po_started"}

# Every source rides through the gated league_settings view: ghosts, double-counted real
# leagues and points-poisoned leagues never enter a half.
_GATE = "JOIN public.league_settings ls ON ls.db_name = t.db_name AND ls.year = t.year"


# GRAIN (Joe 2026-07-20). Reliability is not one number per metric -- it depends on the grain
# the page serves. A weekly cell holds one week of evidence, a season cell holds seventeen, and
# a career cell pools every year, so the leagues needed to reach r=.75/.85/.95 differ by an
# order of magnitude between them. The SPLIT is always over leagues (that is the axis the
# question asks about); what the grain changes is the unit whose values get correlated:
#   week   -> one value per (player, week)
#   season -> one value per (player, year)      [what the ladder used to measure, only]
#   career -> one value per player, leagues pooled across all years
GRAINS = ("week", "season", "career")


def _keys(grain: str, alias: str = "t") -> tuple[str, str]:
    """(sample-unit key, value-unit key) for a grain.

    The SAMPLE unit is always the LEAGUE-SEASON (Joe 2026-07-20: "we never cared about
    leagues, we cared about seasons -- it doesn't matter if a league has a history before or
    after the season we're capturing"). A ten-year league is ten observations, not one, so
    the split is over league-seasons at every grain. Pooling a league's years into one unit
    would both understate the sample and leak the same league across both halves.

    Only the VALUE unit moves with the grain:
      week   -> (player, year, week)
      season -> (player, year)
      career -> (player), pooling that half's league-seasons
    """
    ly = f"{alias}.db_name||'|'||CAST({alias}.year AS VARCHAR)"
    yr = f"'|'||CAST({alias}.year AS VARCHAR)"
    if grain == "week":
        pid = f"{alias}.NFL_player_id||{yr}||'|w'||CAST({alias}.week AS VARCHAR)"
    elif grain == "season":
        pid = f"{alias}.NFL_player_id||{yr}"
    else:
        pid = f"{alias}.NFL_player_id"
    return ly, pid


def _pf(expr_select: str, where: str, grain: str = "season") -> str:
    ly, pid = _keys(grain)
    return f"""
      SELECT {ly} AS ly, {pid} AS pid, {expr_select} AS v
      FROM public.player_fantasy t {_GATE}
      WHERE t.NFL_player_id IS NOT NULL AND {where} GROUP BY 1, 2"""


def _draft(expr: str, grain: str = "season") -> str:
    ly, pid = _keys(grain)
    return f"""
      SELECT {ly} AS ly, {pid} AS pid, {expr} AS v
      FROM public.draft t {_GATE}
      WHERE t.NFL_player_id IS NOT NULL GROUP BY 1, 2"""


def _txn(expr: str, where: str, grain: str = "season") -> str:
    ly, pid = _keys(grain)
    return f"""
      SELECT {ly} AS ly, {pid} AS pid, {expr} AS v
      FROM public.transactions t {_GATE}
      WHERE t.NFL_player_id IS NOT NULL AND {where} GROUP BY 1, 2"""


def _po(expr: str, grain: str = "season") -> str:
    """Per (league-year, player) playoff-rate credit over rostered weeks. made_po is GROUND
    TRUTH (final_playoff_seed within playoff_teams -- the seed is a full final standing, so
    presence-in-bracket is exact); leagues without matchup rows simply contribute no rows
    here, which is the honest stab sample."""
    return f"""
      WITH mp AS (
        SELECT m.db_name, m.year, m.manager,
               MAX(CASE WHEN m.final_playoff_seed <= s.playoff_teams THEN 1 ELSE 0 END) AS made_po
        FROM public.matchup m JOIN public.league_settings s
          ON s.db_name = m.db_name AND s.year = m.year
        WHERE m.manager IS NOT NULL AND m.final_playoff_seed IS NOT NULL
        GROUP BY 1, 2, 3),
      pog AS (SELECT DISTINCT db_name, year, manager, week FROM public.matchup
              WHERE CAST(is_playoffs AS INT) = 1)
      SELECT {_keys(grain)[0]} AS ly, {_keys(grain)[1]} AS pid, {expr} AS v
      FROM public.player_fantasy t {_GATE}
      JOIN mp ON mp.db_name = t.db_name AND mp.year = t.year AND mp.manager = t.manager
      LEFT JOIN pog pg ON pg.db_name = t.db_name AND pg.year = t.year
             AND pg.manager = t.manager AND pg.week = t.week
      WHERE t.NFL_player_id IS NOT NULL AND CAST(t.is_rostered AS INT) = 1
        AND t.manager IS NOT NULL GROUP BY 1, 2"""


def interp_crossings(curve: dict[int, float], targets=TARGETS) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    xs = sorted(curve)
    for target in targets:
        key = f"n_r{int(target * 100)}"
        val = None
        if xs and curve[xs[0]] >= target:
            val = xs[0]
        else:
            for i in range(1, len(xs)):
                a, b = xs[i - 1], xs[i]
                ra, rb = curve[a], curve[b]
                if ra < target <= rb:
                    val = int(round(a + (target - ra) / (rb - ra) * (b - a)))
                    break
        out[key] = val  # None = never crossed in range (serve from a coarser rung / pooled)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=None,
                    help="default: ladder_thresholds.json for season, "
                         "ladder_thresholds.<grain>.json otherwise")
    ap.add_argument("--reps", type=int, default=15)
    ap.add_argument("--boards", action="store_true", help="also run the clutch top/bottom board overlap diagnostic")
    ap.add_argument("--grain", choices=GRAINS, default="season",
                    help="grain whose reliability is measured: week | season | career. "
                         "Thresholds differ by an order of magnitude between them, so the "
                         "weekly and career pages must be calibrated on their OWN grain.")
    args = ap.parse_args()

    if args.out is None:
        args.out = (OUT_DEFAULT if args.grain == "season"
                    else OUT_DEFAULT.with_name(f"ladder_thresholds.{args.grain}.json"))
    random.seed(11)
    reader = LocalReader()
    con = reader.con

    types = [r[0] for r in con.execute(
        "SELECT DISTINCT transaction_type FROM public.transactions WHERE transaction_type IS NOT NULL").fetchall()]
    add_types = [t for t in types if any(k in str(t).lower() for k in ("add", "waiver", "free", "claim", "fa"))]
    add_in = "(" + ",".join(f"'{t}'" for t in add_types) + ")" if add_types else "('add')"
    print(f"txn types: {types}\n add-types used: {add_types}", flush=True)

    def build_metrics(grain: str) -> dict:
        """Metric SQL at a given grain. Definitions track what the PAGES serve -- a ladder
        measured on a superseded definition calibrates nothing (2026-07-20: clutch moved to a
        per-eligible-league title-odds total, and win/lost became start-rate weighted, so the
        pre-existing thresholds were stale on arrival)."""
        return {
            # usage: started share of eligible weeks
            "start_pct": ("avg", _pf(
                "SUM(is_started)*1.0/NULLIF(SUM(CASE WHEN is_rostered=1 THEN 1 END),0)", "1=1", grain)),
            # start-rate weighted record: wins per rostered-eligible week, not per start
            "win_pct": ("avg", _pf(
                "SUM(CASE WHEN is_started=1 AND win=1 THEN 1 ELSE 0 END)*1.0"
                "/NULLIF(SUM(CASE WHEN is_rostered=1 THEN 1 END),0)", "1=1", grain)),
            # clutch: TOTAL title-odds impact, summed (per-league normalisation happens in the
            # builder); an AVG over started weeks is the old, superseded shape
            "clutch": ("avg", _pf(
                "SUM(clutch_equity)", "is_started=1 AND clutch_equity IS NOT NULL", grain)),
            "champ": ("rate_app", _pf("MAX(COALESCE(champion,0))", "is_started=1", grain)),
            "adp": ("avg", _draft("AVG(pick)", grain)),
            "draft_score": ("avg", _draft("AVG(pick_quality_zscore)", grain)),
            "waiver_add": ("rate_all", _txn("1", f"transaction_type IN {add_in}", grain)),
            "waiver_score": ("avg", _txn(
                "AVG(transaction_score)",
                f"transaction_type IN {add_in} AND transaction_score IS NOT NULL", grain)),
            # playoff-rate shapes (Joe 2026-07-19 §2.4)
            "po_wkwt": ("avg", _po("SUM(mp.made_po)*1.0/COUNT(*)", grain)),
            "po_final": ("avg", _po(
                "MAX(CASE WHEN t.week = ls.playoff_start_week - 1 THEN mp.made_po ELSE 0 END)", grain)),
            "po_started": ("avg", _po(
                "MAX(CASE WHEN CAST(t.is_started AS INT)=1 AND pg.week IS NOT NULL THEN 1 ELSE 0 END)", grain)),
        }

    # metrics that only exist at one grain -- a draft pick has no week, a career has no season
    SKIP = {"week": {"adp", "draft_score", "champ", "po_wkwt", "po_final", "po_started"},
            "career": set(), "season": set()}

    metrics = build_metrics(args.grain)
    for _m in SKIP.get(args.grain, set()):
        metrics.pop(_m, None)

    results: dict[str, dict] = {}
    for name, (kind, sql) in metrics.items():
        rows = con.execute(sql).fetchall()
        by_ly: dict[str, dict[str, float]] = defaultdict(dict)
        appear: dict[str, int] = defaultdict(int)
        for ly, pid, v in rows:
            if v is None:
                continue
            by_ly[ly][pid] = float(v)
            appear[pid] += 1
        all_ly = list(by_ly.keys())
        if not all_ly:
            print(f"\n[{name}] no data", flush=True)
            continue
        # "Elite" = high-appearance units, so halves are compared on players with real
        # evidence rather than one-league noise. The threshold must be scoped to what a unit
        # COULD appear in: a (player, 2024) season-unit can only show up in 2024 league-
        # seasons, so the old global `len(all_ly)//20` silently emptied the pool once units
        # became year-scoped. Career units span every year and keep the global basis.
        _ly_per_year: dict[str, int] = defaultdict(int)
        for _ly in all_ly:
            _ly_per_year[_ly.split("|", 1)[1] if "|" in _ly else ""] += 1

        def _available(pid_: str) -> int:
            parts = pid_.split("|")
            return _ly_per_year.get(parts[1], len(all_ly)) if len(parts) > 1 else len(all_ly)

        elite = {pid for pid, a in appear.items()
                 if a >= max(_available(pid) // 20, min(50, _available(pid)))}

        def half_rate(group):
            num: dict[str, float] = defaultdict(float)
            den: dict[str, int] = defaultdict(int)
            for ly in group:
                for pid, v in by_ly[ly].items():
                    num[pid] += v
                    den[pid] += 1
            if kind == "rate_all":  # denominator = all leagues in the half
                return {pid: num[pid] / len(group) for pid in num}
            return {pid: num[pid] / den[pid] for pid in den}

        def reliab(n):
            cs = []
            for _ in range(args.reps):
                if n > len(all_ly):
                    break
                s = random.sample(all_ly, n)
                h = n // 2
                ra, rb = half_rate(s[:h]), half_rate(s[h:])
                common = [p for p in ra if p in rb and p in elite]
                if len(common) < 20:
                    continue
                try:
                    cs.append(statistics.correlation([ra[p] for p in common], [rb[p] for p in common]))
                except statistics.StatisticsError:
                    pass
            return statistics.mean(cs) if cs else float("nan")

        curve = {}
        for n in NGRID:
            r = reliab(n)
            if r == r:
                curve[n] = r
        crossings = interp_crossings(curve) if curve else {f"n_r{int(t*100)}": None for t in TARGETS}
        results[name] = {"class": CLASS_OF.get(name), "curve": {str(k): round(v, 4) for k, v in curve.items()},
                         "elite_players": len(elite), "league_years": len(all_ly), **crossings}
        line = "  ".join(f"{n}:{curve[n]:+.2f}" for n in NGRID if n in curve)
        print(f"\n[{name}] elite={len(elite)} of {len(all_ly)} ly | "
              + " | ".join(f"r={t} at n~{crossings[f'n_r{int(t*100)}']}" for t in TARGETS), flush=True)
        print(f"   {line}", flush=True)

    if args.boards:
        print("\n" + "=" * 78, flush=True)
        print("BOARD REPRODUCIBILITY -- do the TOP 20 / BOTTOM 20 survive an independent half?",
              flush=True)
        print("Full-ranking reliability and BOARD stability are different questions: a metric\n"
              "can have a noisy middle and still name the same extremes every time. Pages show\n"
              "boards, so this is the number that decides whether a board is worth serving.",
              flush=True)
        print("=" * 78, flush=True)
        for _n, (_k, _sql) in metrics.items():
            _board_overlap(con, _sql, _n)

    n_settings = con.execute("SELECT COUNT(*) FROM public.league_settings").fetchone()[0]
    payload = {
        "version": datetime.now(timezone.utc).strftime("thresholds-%Y%m%d"),
        "derived_utc": datetime.now(timezone.utc).isoformat(),
        "grain": args.grain,
        "targets": list(TARGETS),
        "gated_league_year_rows": n_settings,
        "ngrid": NGRID,
        "reps": args.reps,
        "metrics": results,
    }
    args.out.write_text(json.dumps(payload, indent=2))
    print(f"\nSUMMARY (elite split-half; gated lake, {n_settings:,} league-year settings rows):", flush=True)
    for name, res in results.items():
        print("   " + f"{name:<13}" + " | ".join(
            f"r{int(t*100)}: n~{res[f'n_r{int(t*100)}'] or '>grid'}" for t in TARGETS), flush=True)
    print(f"wrote {args.out}", flush=True)
    reader.close()


def _board_overlap(con, sql: str, label: str = "clutch", topn: int = 20) -> None:
    """Top-N/bottom-N board reproducibility across independent halves of league-seasons."""
    rows = con.execute(sql).fetchall()
    by_ly: dict[str, dict[str, float]] = defaultdict(dict)
    appear: dict[str, int] = defaultdict(int)
    for ly, pid, v in rows:
        if v is None:
            continue
        by_ly[ly][pid] = float(v)
        appear[pid] += 1
    all_ly = list(by_ly.keys())
    _lpy: dict[str, int] = defaultdict(int)
    for _l in all_ly:
        _lpy[_l.split("|", 1)[1] if "|" in _l else ""] += 1

    def _avail(pid_: str) -> int:
        pr = pid_.split("|")
        return _lpy.get(pr[1], len(all_ly)) if len(pr) > 1 else len(all_ly)

    elite = {pid for pid, a in appear.items()
             if a >= max(_avail(pid) // 20, min(50, _avail(pid)))}

    def prate(group):
        num: dict[str, float] = defaultdict(float)
        den: dict[str, int] = defaultdict(int)
        for ly in group:
            for pid, v in by_ly[ly].items():
                if pid in elite:
                    num[pid] += v
                    den[pid] += 1
        return {p: num[p] / den[p] for p in den if den[p] >= 3}

    # TIE STRUCTURE first -- without it the overlap numbers are unreadable. A saturated metric
    # (start_rate has 418 player-seasons tied at exactly 1.000) makes "top 20" a random draw
    # from the tie cluster, so overlap collapses toward 20/cluster_size however sound the
    # metric is. That is a BOARD-DESIGN problem needing a tiebreaker, not a signal problem.
    _allv = sorted((v for d in by_ly.values() for p_, v in d.items() if p_ in elite), reverse=True)
    _tie_note = ""
    if len(_allv) > topn:
        _cut = _allv[topn - 1]
        _tied = sum(1 for v in _allv if v == _cut)
        if _tied > 1:
            _tie_note = f" | TIE at the top-{topn} cut: {_tied:,} values equal {_cut:.4g}"
    print(f"\n [{label}]{_tie_note}", flush=True)
    for n in [200, 400, 800, 1600, 3200, 6400]:
        tops, bots = [], []
        for _ in range(15):
            if n > len(all_ly):
                break
            s = random.sample(all_ly, n)
            h = n // 2
            ra, rb = prate(s[:h]), prate(s[h:])
            ca = [p for p in ra if p in rb]
            if len(ca) < 25:
                continue
            # RANDOM tie-breaking, not dict order. Python's sort is stable, so equal values
            # resolve by insertion order -- which is IDENTICAL in both halves and manufactures
            # agreement out of nothing. waiver_add scored a perfect 100%/100% at every n that
            # way, while 4,768 player-seasons sit tied at exactly one add. A tie is genuine
            # uncertainty about who belongs on the board and must be sampled, not frozen.
            jit = {p: random.random() for p in ca}
            jit2 = {p: random.random() for p in ca}   # independent draw per half
            ta = set(sorted(ca, key=lambda p: (-ra[p], jit[p]))[:topn])
            tb = set(sorted(ca, key=lambda p: (-rb[p], jit2[p]))[:topn])
            ba = set(sorted(ca, key=lambda p: (ra[p], jit[p]))[:topn])
            bb = set(sorted(ca, key=lambda p: (rb[p], jit2[p]))[:topn])
            tops.append(len(ta & tb))
            bots.append(len(ba & bb))
        if tops:
            print(f"   n={n:>5} lg-seasons | top-{topn} {statistics.mean(tops):>4.1f}/{topn}"
                  f"  ({100*statistics.mean(tops)/topn:>3.0f}%) | "
                  f"bottom-{topn} {statistics.mean(bots):>4.1f}/{topn}"
                  f"  ({100*statistics.mean(bots)/topn:>3.0f}%)", flush=True)


if __name__ == "__main__":
    main()
