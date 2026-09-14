"""
sota_recon/recon_bounce.py  --  double-entry "allowed vs gained" validation of the DST team-context cols.

A team's defensive ALLOWED-X must equal its opponent's offensive GAINED-X in the same game (pure internal
double-entry, no external source). This covers the DST-context columns that no other lane validates:
rushing_yds_allowed / passing_yds_allowed / rushing_tds_allowed / passing_tds_allowed (vs opponent
offense), and total_yds_allowed (== rush_allowed + pass_allowed). Modern (1978+) where player boxscores
are complete; pre-1978 excluded (source-incomplete, documented).

PASS when each bounce reconciles >=99% of modern team-games.

    python -m scripts.sota_recon.recon_bounce
"""
from __future__ import annotations
from pathlib import Path
import duckdb
from .sources import latest_v26

D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def run(run_dir: str | None = None) -> dict:
    v26 = Path(latest_v26()).as_posix()
    con = duckdb.connect(); con.execute("PRAGMA threads=3"); con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA disable_progress_bar")
    con.execute(f"""CREATE TEMP TABLE off AS SELECT nfl_franchise_number f, year, week,
        SUM({D('rushing_yards')}) ru, SUM({D('passing_yards')}) pa,
        SUM({D('rushing_tds')}) rutd, SUM({D('passing_tds')}) ptd
        FROM '{v26}' WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3""")
    con.execute(f"""CREATE TEMP TABLE b AS
        SELECT {D('d.rushing_yds_allowed')} ruA, o.ru ruB, {D('d.passing_yds_allowed')} paA, o.pa paB,
               {D('d.rushing_tds_allowed')} rutdA, o.rutd rutdB, {D('d.passing_tds_allowed')} ptdA, o.ptd ptdB,
               {D('d.total_yds_allowed')} totA, ({D('d.rushing_yds_allowed')}+{D('d.passing_yds_allowed')}) totParts
        FROM '{v26}' d JOIN off o ON d.opponent_nfl_franchise_number=o.f AND d.year=o.year AND d.week=o.week
        WHERE d.position='DEF' AND d.year>=1978""")
    n = con.execute("SELECT COUNT(*) FROM b").fetchone()[0]
    def pct(expr):
        return con.execute(f"SELECT ROUND(100.0*SUM(CASE WHEN {expr} THEN 1 ELSE 0 END)/COUNT(*),2) FROM b").fetchone()[0]
    checks = {
        "rushing_yds_allowed==opp_rush": pct("ABS(ruA-ruB)<=2"),
        "passing_yds_allowed==opp_pass": pct("ABS(paA-paB)<=2"),
        "rushing_tds_allowed==opp_rushtd": pct("ABS(rutdA-rutdB)<=0"),
        "passing_tds_allowed==opp_passtd": pct("ABS(ptdA-ptdB)<=0"),
        "total_yds_allowed==rush+pass_allowed": pct("ABS(totA-totParts)<=3"),
    }
    con.close()
    worst = min(checks.values()) if checks else 0
    status = "pass" if worst >= 99.0 else "review"
    return {"status": status, "counts": {"games": n, "min_match_pct": worst, **{k: v for k, v in checks.items()}}}


if __name__ == "__main__":
    r = run()
    print(f"[{r['status']}] double-entry bounce ({r['counts']['games']:,} modern team-games)")
    for k, v in r["counts"].items():
        if k not in ("games", "min_match_pct"):
            print(f"  {v:>6}%  {k}")
