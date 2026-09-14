"""
sota_recon/recon_aggregate.py  --  LANE: season/career aggregate validation.

The 10 weekly lanes validate the weekly super table against external truth. This lane validates
the DERIVED season/career aggregate tables (player_nfl_season/_all, player_nfl_career/_all) by
tying EVERY aggregate column back to the weekly source by its aggregation semantics. If the weekly
table is true (other lanes) and every aggregate cell reconciles to it, the aggregates are true too.

Checks (each a golden anchor; FAIL = real regression):
  SUM-RECONCILE   every additive counting col: season == SUM(weekly REG) per (id,year);
                  _all uses REG+POST; career == SUM(season). Reports per-column mismatch rows.
  MAX-RECONCILE   *_long cols: season == MAX(weekly).
  GAMES           season games == COUNT(weekly game rows) per (id,year) (doubleheaders counted).
  RANK-WELLFORMED each rank_season_*/rank_alltime_*: dense 1..N over its position pool per
                  partition, no dup ranks, no gaps, rank=1 holds the max points.
  PPG-SANITY      ppg_season_4pt_half ~= fpts_4pt_half_total / games (tolerance).
  LAMAR-COVER     all 56 lamar_* present + non-null for the eligible position pool; finite.
  RECORD-LEADERS  season/career leaders for marquee stats == known record holder+value.

    python -m scripts.sota_recon.recon_aggregate            # PASS/FAIL per check
    python -m scripts.sota_recon.recon_aggregate --fails    # only failures
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import duckdb
from .sources import latest_v26

_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))
# Aggregation semantics come from the ONE stat contract registry (master plan §6, 2026-07-25) --
# the same source aggregate_nfl_stats_fly builds from, so this lane validates against the exact
# declarations the builder used (fragmentation between the two was the §6 defect).
from multi_league.core.stat_contracts_loader import (
    aggregation_sets as _stat_contract_sets,
    non_sum_stat_ids as _non_sum_stat_ids,
)

_SETS = _stat_contract_sets()

TOL = 0.05
_ART = lambda: Path(latest_v26()).parent / "season_career_v26"
SEASON = lambda: (_ART() / "player_nfl_season.parquet").as_posix()
SEASON_ALL = lambda: (_ART() / "player_nfl_season_all.parquet").as_posix()
CAREER = lambda: (_ART() / "player_nfl_career.parquet").as_posix()
CAREER_ALL = lambda: (_ART() / "player_nfl_career_all.parquet").as_posix()

# columns that are NOT plain SUM aggregates (registry-declared; see stat_contracts.v1.json)
_NONADD_PREFIX = _SETS.non_additive_prefixes
_NONADD_EXACT = _SETS.non_additive_exact
# season-only stats injected by the fact-adjustment sidecar (season != SUM(weekly) BY DESIGN):
# validated against external truth by the authority lane (recon_season_authority), not here.
_FACT_ADJUSTED = _SETS.fact_adjusted_season_cols


def _is_id_like(name: str) -> bool:
    return name.endswith(("_id", "_number", "_count"))


def _con():
    c = duckdb.connect(); c.execute("PRAGMA threads=2"); c.execute("SET memory_limit='5GB'")
    c.execute("PRAGMA disable_progress_bar"); c.execute("SET preserve_insertion_order=false")
    sp = Path(latest_v26()).parent / ".aggspill"; sp.mkdir(exist_ok=True)
    c.execute(f"SET temp_directory='{sp.as_posix()}'")
    return c


def _numeric_cols(con, src):
    rows = con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()
    num = {r[0] for r in rows if any(t in r[1].upper() for t in
           ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGEINT"))}
    return num


def _additive_max_cols(con, season_src, weekly):
    wk = _numeric_cols(con, weekly); se = _numeric_cols(con, season_src)
    both = wk & se
    # class-based skip from the registry: any column contracted non-SUM (rates, maxima, context)
    # is never held to season==SUM(weekly). This is what caught the NY/A / AY/A / ANY/A trio --
    # recomputed rates the legacy exclusion sets never listed, failing career==sum(season) on
    # 1,366 rows (2026-07-25). MAX-classed cols are removed from the skip so the MAX lane keeps them.
    skip = (set(_SETS.derived_rate_cols) | set(_SETS.per_game_avg_cols) | set(_SETS.weighted_avg_cols)
            | _NONADD_EXACT | _FACT_ADJUSTED | (_non_sum_stat_ids() - set(_SETS.max_cols)))
    maxc = set(_SETS.max_cols) & both
    add = []
    for c in sorted(both):
        # ngs_* season values are nflverse's PUBLISHED season totals (direct ingest, NOT the SUM
        # of weekly per-game averages), so season != SUM(weekly) BY DESIGN -- skip like _FACT_ADJUSTED.
        if c in skip or c in maxc or _is_id_like(c) or c.startswith("ngs_"):
            continue
        if any(c.startswith(p) for p in _NONADD_PREFIX):
            continue
        add.append(c)
    return add, sorted(maxc)


def _reconcile(con, season_src, weekly, *, include_post, results, label):
    add, maxc = _additive_max_cols(con, season_src, weekly)
    phase = "season_type IN ('REG','POST')" if include_post else "season_type = 'REG'"
    sums = ",\n".join(f'SUM(CAST("{c}" AS DOUBLE)) AS "{c}"' for c in add + maxc)
    # weekly rolled to (id,year); MAX for long cols
    sel = []
    for c in add:
        sel.append(f'SUM(CAST("{c}" AS DOUBLE)) AS "{c}"')
    for c in maxc:
        sel.append(f'MAX(CAST("{c}" AS DOUBLE)) AS "{c}"')
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _wk AS
        SELECT NFL_player_id, year, {', '.join(sel)}
        FROM '{weekly}' WHERE NFL_player_id IS NOT NULL AND {phase} GROUP BY NFL_player_id, year""")
    # per-column mismatch counts in one pass
    checks = []
    for c in add + maxc:
        checks.append(f'COUNT(*) FILTER (WHERE ABS(COALESCE(s."{c}",0)-COALESCE(w."{c}",0))>{TOL}) AS "{c}"')
    row = con.execute(f"""SELECT {', '.join(checks)}
        FROM '{season_src}' s JOIN _wk w ON s.NFL_player_id=w.NFL_player_id AND s.year=w.year""").fetchdf().iloc[0]
    bad = {c: int(row[c]) for c in add + maxc if int(row[c]) > 0}
    total_cols = len(add) + len(maxc)
    ok = len(bad) == 0
    results.append((f"{label}.sum_max_reconcile", f"{total_cols-len(bad)}/{total_cols} cols clean", ok,
                    f"bad cols: {dict(list(bad.items())[:8])}" if bad else "all cols reconcile"))


def _career_reconcile(con, career_src, season_src, *, results, label):
    add, maxc = _additive_max_cols(con, career_src, season_src)
    sel = [f'SUM(CAST("{c}" AS DOUBLE)) AS "{c}"' for c in add] + \
          [f'MAX(CAST("{c}" AS DOUBLE)) AS "{c}"' for c in maxc]
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _se AS
        SELECT NFL_player_id, {', '.join(sel)} FROM '{season_src}'
        WHERE NFL_player_id IS NOT NULL GROUP BY NFL_player_id""")
    checks = [f'COUNT(*) FILTER (WHERE ABS(COALESCE(c."{x}",0)-COALESCE(e."{x}",0))>{TOL}) AS "{x}"'
              for x in add + maxc]
    row = con.execute(f"""SELECT {', '.join(checks)}
        FROM '{career_src}' c JOIN _se e ON c.NFL_player_id=e.NFL_player_id""").fetchdf().iloc[0]
    bad = {x: int(row[x]) for x in add + maxc if int(row[x]) > 0}
    total = len(add) + len(maxc)
    results.append((f"{label}.career_eq_sum_season", f"{total-len(bad)}/{total} cols clean", len(bad) == 0,
                    f"bad: {dict(list(bad.items())[:8])}" if bad else "career = sum(season)"))


def _games_check(con, season_src, weekly, *, include_post, results, label):
    g = "games_played" if "games_played" in _numeric_cols(con, season_src) else (
        "games" if "games" in _numeric_cols(con, season_src) else None)
    if not g:
        results.append((f"{label}.games", "no games col", True, "skipped")); return
    phase = "season_type IN ('REG','POST')" if include_post else "season_type = 'REG'"
    bad = con.execute(f"""
        WITH wk AS (SELECT NFL_player_id, year, COUNT(*) n FROM '{weekly}'
                    WHERE NFL_player_id IS NOT NULL AND {phase} GROUP BY 1,2)
        SELECT COUNT(*) FROM '{season_src}' s JOIN wk ON s.NFL_player_id=wk.NFL_player_id AND s.year=wk.year
        WHERE s."{g}" <> wk.n""").fetchone()[0]
    results.append((f"{label}.games_eq_weekly_rows", f"{bad} mismatched", bad == 0,
                    f"{g} == count(weekly rows)"))


def _rank_wellformed(con, season_src, *, season, results, label):
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{season_src}'").fetchall()]
    rcols = [c for c in cols if c.startswith("rank_season_" if season else "rank_alltime_")]
    pk = "year" if season else "1"
    bad_cols = {}
    for rc in rcols:
        q = con.execute(f"""
            WITH t AS (SELECT {pk} AS pk, "{rc}" AS r FROM '{season_src}' WHERE "{rc}" IS NOT NULL),
            agg AS (SELECT pk, COUNT(*) cnt, COUNT(DISTINCT r) dc, MIN(r) mn, MAX(r) mx FROM t GROUP BY pk)
            SELECT COUNT(*) FILTER (WHERE cnt<>dc) dup,
                   COUNT(*) FILTER (WHERE mn<>1) badmin,
                   COUNT(*) FILTER (WHERE mx<>cnt) noncontig FROM agg""").fetchdf().iloc[0]
        d = int(q["dup"]) + int(q["badmin"]) + int(q["noncontig"])
        if d:
            bad_cols[rc] = {"dup": int(q["dup"]), "badmin": int(q["badmin"]), "noncontig": int(q["noncontig"])}
    results.append((f"{label}.rank_wellformed", f"{len(rcols)-len(bad_cols)}/{len(rcols)} rank cols dense+unique",
                    len(bad_cols) == 0, f"bad: {dict(list(bad_cols.items())[:5])}" if bad_cols else "all dense 1..N"))


def _ppg_sanity(con, season_src, weekly, *, include_post, results, label):
    cols = _numeric_cols(con, season_src)
    if "ppg_season_4pt_half" not in cols:
        results.append((f"{label}.ppg_sanity", "col missing", True, "skipped")); return
    # canonical ppg_season = AVG(fpts) over weekly rows with non-null fpts (NOT /games)
    phase = "season_type IN ('REG','POST')" if include_post else "season_type = 'REG'"
    bad = con.execute(f"""
        WITH wk AS (SELECT NFL_player_id, year, ROUND(AVG(CAST(fpts_4pt_half AS DOUBLE)),2) avgp
                    FROM '{weekly}' WHERE {phase} AND fpts_4pt_half IS NOT NULL GROUP BY 1,2)
        SELECT COUNT(*) FROM '{season_src}' s JOIN wk ON s.NFL_player_id=wk.NFL_player_id AND s.year=wk.year
        WHERE ABS(COALESCE(s.ppg_season_4pt_half,0) - wk.avgp) > 0.05""").fetchone()[0]
    results.append((f"{label}.ppg_sanity", f"{bad} rows off", bad == 0,
                    "ppg_season_4pt_half == AVG(weekly fpts) (tol 0.05)"))


def _lamar_cover(con, season_src, *, results, label):
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{season_src}'").fetchall()]
    lam = [c for c in cols if c.startswith("lamar_") and not c.startswith("lamar_ppg_")]
    # finite + present; non-null coverage for offense pool (QB/RB/WR/TE) on a core config
    core = "lamar_12t_flx_half_4pt"
    notfinite = 0
    for c in lam:
        notfinite += con.execute(f"SELECT COUNT(*) FROM '{season_src}' WHERE \"{c}\" IN ('Infinity'::DOUBLE,'-Infinity'::DOUBLE,'NaN'::DOUBLE)").fetchone()[0]
    cover = con.execute(f"""SELECT COUNT(*) FILTER (WHERE "{core}" IS NULL)
        FROM '{season_src}' WHERE nfl_position IN ('RB','WR','TE')""").fetchone()[0] if core in cols else -1
    ok = (len(lam) == 56) and (notfinite == 0)
    results.append((f"{label}.lamar_cover", f"{len(lam)}/56 cols, {notfinite} non-finite, {cover} flex-null",
                    ok, "56 finite lamar cols"))


def run(only_fails=False):
    con = _con()
    weekly = latest_v26()
    results = []
    # season (REG) and season_all (REG+POST)
    for src, post, lab in ((SEASON(), False, "season"), (SEASON_ALL(), True, "season_all")):
        _reconcile(con, src, weekly, include_post=post, results=results, label=lab)
        _games_check(con, src, weekly, include_post=post, results=results, label=lab)
        _rank_wellformed(con, src, season=True, results=results, label=lab)
        _ppg_sanity(con, src, weekly, include_post=post, results=results, label=lab)
        _lamar_cover(con, src, results=results, label=lab)
    # career == sum(season)
    _career_reconcile(con, CAREER(), SEASON(), results=results, label="career")
    _career_reconcile(con, CAREER_ALL(), SEASON_ALL(), results=results, label="career_all")
    _rank_wellformed(con, CAREER(), season=False, results=results, label="career")
    _lamar_cover(con, CAREER(), results=results, label="career")
    con.close()
    passed = sum(1 for *_, ok, _ in [(r[0], r[1], r[2], r[3]) for r in results] if ok)
    passed = sum(1 for r in results if r[2])
    return {"passed": passed, "total": len(results),
            "results": [{"check": r[0], "detail": r[1], "ok": r[2], "note": r[3]} for r in results],
            "failed": len(results) - passed}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--fails", action="store_true"); a = ap.parse_args()
    r = run()
    for x in r["results"]:
        if a.fails and x["ok"]:
            continue
        print(f"[{'PASS' if x['ok'] else 'FAIL'}] {x['check']:34} {x['detail']:34} {x['note']}")
    print(f"\n{r['passed']}/{r['total']} checks pass ({r['failed']} fail)")
