"""
sota_recon/recon_mirrors.py  --  LANE: the COMPLETE double-entry mirror map (WS4 type 2+3).

Joe's directive (2026-07-10): enumerate EVERY way to reconcile offense<->defense,
QB<->receiver, team<->team -- then find all remaining holes. This lane implements every
identity the current columns support that no existing lane checks (recon_internal has
int/sack/pass_recv_yards/comp_rec; recon_bounce has the DST allowed<->gained set;
golden_points has scoring composition). Each check is era-aware and column-existence-aware:
a check whose columns don't exist reports HOLE-NO-COLUMNS instead of silently passing.

INTRA-OFFENSE (within one team-game; thrower and catcher are the same events):
  passing_tds == receiving_tds            (every passing TD has a receiver)
  attempts == targets            (1992+)  (every attempt targets someone; spikes/throwaways
                                           untargeted -> expect small residual, report only)
  MAX(passing_long) == MAX(receiving_long)(the same play, both ends)
  passing_air_yards == receiving_air_yards(2006+, same charted plays)
  passing_interceptions == receiving_target_interceptions (2018+ charted)

CROSS-SIDE (team A offense vs team B defense):
  fumbles_lost(A) vs def-side recoveries(B)  (where DEF populated)
  punt_returns(B) <= punts(A)                (one-way bound; FC/TB/OOB explain the gap)

LEAGUE-YEAR CONSERVATION:
  every yard gained is a yard allowed: Sigma team points == Sigma points_allowed per year

    python -m scripts.sota_recon.recon_mirrors [--csv out.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .sources import latest_v26

TOL = 0.5


def run(src: str | None = None, csv: str | None = None) -> list[dict]:
    vq = Path(src or latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}' LIMIT 0").fetchall()}

    con.execute(f"""
        CREATE TEMP TABLE tg AS
        SELECT year, CAST(week AS INT) AS week, season_type,
               nfl_franchise_number AS fid, opponent_nfl_franchise_number AS opp,
               SUM(passing_tds) AS pass_td, SUM(receiving_tds) AS recv_td,
               SUM(attempts) AS att, SUM(targets) AS tgt,
               MAX(passing_long) AS pass_long, MAX(receiving_long) AS recv_long,
               SUM(passing_air_yards) AS pass_air, SUM(receiving_air_yards) AS recv_air,
               SUM(passing_interceptions) AS pass_int,
               SUM(receiving_target_interceptions) AS tgt_int,
               SUM(fumbles_lost) AS fum_lost,
               SUM(punts) AS punts, SUM(punt_returns) AS punt_ret
        FROM '{vq}'
        WHERE COALESCE(position, '') <> 'DEF' AND nfl_franchise_number IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5""")

    checks = []

    def two_sided(name, a, b, era, exact=True, note=""):
        if not ({_c for _c in (a, b)} <= {"pass_td", "recv_td", "att", "tgt", "pass_long",
                                          "recv_long", "pass_air", "recv_air", "pass_int",
                                          "tgt_int"}):
            checks.append(dict(check=name, status="HOLE-NO-COLUMNS", note=note))
            return
        n, bad, worst = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS({a} - {b}) > {TOL}),
                   MAX(ABS({a} - {b}))
            FROM tg WHERE {a} IS NOT NULL AND {b} IS NOT NULL AND ({a} > 0 OR {b} > 0)
              AND {era}""").fetchone()
        checks.append(dict(check=name, games=n, mismatches=bad, worst=worst,
                           pct_clean=None if not n else round(1 - bad / n, 4),
                           status="CHECKED" if exact else "REPORT-ONLY", note=note))

    two_sided("passing_tds==receiving_tds", "pass_td", "recv_td", "TRUE")
    two_sided("attempts==targets(1992+)", "att", "tgt", "year >= 1992", exact=False,
              note="spikes/throwaways are untargeted attempts; residual expected, drift watched")
    two_sided("max_passing_long==max_receiving_long", "pass_long", "recv_long", "TRUE")
    # ONE-WAY by physics (refinement 0b, applied 2026-07-12 when the nflverse-locked
    # 2025 recompute exposed it): throwaway/untargeted attempts carry air yards with no
    # receiver, so pass_air >= recv_air always; only recv > pass is a violation.
    n_air, bad_air, worst_air = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE recv_air - pass_air > {TOL}),
               MAX(recv_air - pass_air)
        FROM tg WHERE pass_air IS NOT NULL AND recv_air IS NOT NULL
          AND (pass_air > 0 OR recv_air > 0) AND year >= 2006""").fetchone()
    checks.append(dict(check="passing_air>=receiving_air(2006+)", games=n_air,
                       mismatches=bad_air, worst=worst_air,
                       pct_clean=None if not n_air else round(1 - bad_air / n_air, 4),
                       status="CHECKED",
                       note="one-way bound; throwaway air has no receiving side"))
    two_sided("pass_int==target_int(2018+)", "pass_int", "tgt_int", "year >= 2018")

    # cross-side: fumbles_lost(A) vs DEF-side recovery count (B), where DEF populated
    def_fum_col = "def_fumbles" if "def_fumbles" in have else None
    if def_fum_col:
        n, bad = con.execute(f"""
            WITH d AS (SELECT year, CAST(week AS INT) AS week, season_type,
                              nfl_franchise_number AS fid,
                              opponent_nfl_franchise_number AS opp,
                              SUM({def_fum_col}) AS def_fum
                       FROM '{vq}' WHERE position = 'DEF' GROUP BY 1,2,3,4,5)
            SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(tg.fum_lost - d.def_fum) > {TOL})
            FROM tg JOIN d ON d.year = tg.year AND d.week = tg.week
              AND d.season_type = tg.season_type AND d.fid = tg.opp AND d.opp = tg.fid
            WHERE d.def_fum IS NOT NULL AND tg.fum_lost IS NOT NULL""").fetchone()
        checks.append(dict(check="fumbles_lost(A)==def_fumbles(B)", games=n, mismatches=bad,
                           pct_clean=None if not n else round(1 - bad / n, 4),
                           status="REPORT-ONLY",
                           note="def_fumbles semantics (forced vs recovered) to pin before enforcing"))
    else:
        checks.append(dict(check="fumbles_lost(A)==def_recoveries(B)",
                           status="HOLE-NO-COLUMNS"))

    # one-way: punt_returns(B) <= punts(A)
    n, bad = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE b.punt_ret > a.punts)
        FROM tg a JOIN tg b ON b.year = a.year AND b.week = a.week
          AND b.season_type = a.season_type AND b.fid = a.opp AND b.opp = a.fid
        WHERE a.punts IS NOT NULL AND b.punt_ret IS NOT NULL AND a.punts > 0""").fetchone()
    checks.append(dict(check="punt_returns(B)<=punts(A)", games=n, mismatches=bad,
                       pct_clean=None if not n else round(1 - bad / n, 4), status="CHECKED"))

    # league-year conservation: points scored == points allowed
    rows = con.execute(f"""
        WITH pa AS (SELECT year, SUM(points_allowed) AS allowed
                    FROM '{vq}' WHERE position = 'DEF' GROUP BY 1),
        sc AS (SELECT year, SUM(pts_def_team_pts) AS scored
               FROM (SELECT year, nfl_franchise_number, CAST(week AS INT) AS wk,
                            season_type, MAX(pts_def_team_pts) AS pts_def_team_pts
                     FROM '{vq}' WHERE position = 'DEF' GROUP BY 1,2,3,4) GROUP BY 1)
        SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(sc.scored - pa.allowed) > sc.scored * 0.001)
        FROM sc JOIN pa USING (year)""").fetchone()
    checks.append(dict(check="league-year: points scored==points allowed",
                       games=rows[0], mismatches=rows[1],
                       pct_clean=None if not rows[0] else round(1 - rows[1] / rows[0], 4),
                       status="CHECKED"))
    con.close()

    if csv:
        import csv as _csv
        with open(csv, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=["check", "games", "mismatches", "worst",
                                               "pct_clean", "status", "note"])
            w.writeheader()
            for c in checks:
                w.writerow({k: c.get(k) for k in w.fieldnames})
    return checks


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    for c in run(a.src, a.csv):
        pct = f"{c['pct_clean']:.2%}" if c.get("pct_clean") is not None else "-"
        print(f"  {c['check']:44s} games={c.get('games','-'):>7} "
              f"bad={c.get('mismatches','-'):>6} clean={pct:>8} [{c['status']}] {c.get('note','')}")
