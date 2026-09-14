# -*- coding: utf-8 -*-
"""missing_mirrors.py -- measure every double-entry identity NOT yet in the recon lattice.

Each check compares two INDEPENDENTLY-attributed views of the same events at team-game grain
(year, week, season_type, franchise). agree% = games where |A-B| < 0.5.
"""
import glob
from pathlib import Path

import duckdb

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("PRAGMA threads=6")
sup = sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
             key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]
V = f"read_parquet('{Path(sup).as_posix()}')"
D = lambda c: f"COALESCE(TRY_CAST(\"{c}\" AS DOUBLE),0)"

# offense team-game sums (non-DEF rows)
con.execute(f"""CREATE TEMP TABLE off AS SELECT CAST(year AS INT) y, CAST(week AS INT) w,
  season_type st, nfl_franchise_number f, opponent_nfl_franchise_number opp,
  SUM({D('passing_first_downs')}) p_fd, SUM({D('receiving_first_downs')}) r_fd,
  SUM({D('passing_2pt_conversions')}) p2, SUM({D('receiving_2pt_conversions')}) r2,
  SUM({D('pass_explosive_20')}) pex, SUM({D('rec_explosive_20')}) rex,
  SUM({D('rz_pass_td')}) rzp, SUM({D('rz_rec_td')}) rzr,
  SUM({D('completions_40plus')}) c40, SUM({D('receptions_40plus')}) r40,
  SUM({D('passing_tds_40plus')}) ptd40, SUM({D('receiving_tds_40plus')}) rtd40,
  SUM({D('passing_tds_50plus')}) ptd50, SUM({D('receiving_tds_50plus')}) rtd50,
  SUM({D('pick6')}) pick6, SUM({D('punts_blocked')}) pblk, SUM({D('fg_blocked')}) fgblk,
  SUM({D('pat_blocked')}) patblk,
  SUM({D('passing_pressured')}) qb_prs, SUM({D('passing_hurried')}) qb_hur,
  SUM({D('passing_hits')}) qb_hit, SUM({D('passing_drops')}) qb_drp,
  SUM({D('receiving_drops')}) rec_drp,
  SUM({D('passing_yards_after_catch')}) p_yac, SUM({D('receiving_yards_after_catch')}) r_yac,
  SUM({D('passing_completed_air_yards')}) p_cay, SUM({D('receiving_completed_air_yards')}) r_cay,
  SUM({D('attempts')}) att, SUM({D('targets')}) tgt, SUM({D('carries')}) car,
  SUM({D('sacks_suffered')}) sk, SUM({D('rushing_yards')}) ry, SUM({D('receiving_yards')}) recy,
  SUM({D('pass_success_plays')}) psp, SUM({D('rec_success_plays')}) rsp
  FROM {V} WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4,5""")

# defender individual sums (non-DEF IDP rows)
con.execute(f"""CREATE TEMP TABLE idp AS SELECT CAST(year AS INT) y, CAST(week AS INT) w,
  season_type st, nfl_franchise_number f,
  SUM({D('def_pressures')}) d_prs, SUM({D('def_hurries')}) d_hur,
  SUM({D('def_knockdowns')}) d_kd, SUM({D('def_blk_kick')}) d_blk,
  SUM({D('def_int_ret_td')}) d_p6
  FROM {V} WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")

# DST-row cols
con.execute(f"""CREATE TEMP TABLE dst AS SELECT CAST(year AS INT) y, CAST(week AS INT) w,
  season_type st, nfl_franchise_number f,
  {D('def_plays')} d_plays, {D('def_yards_allowed')} d_yds, {D('def_int_ret_td')} dst_p6,
  {D('def_blk_kick')} dst_blk
  FROM {V} WHERE position='DEF' AND nfl_franchise_number IS NOT NULL""")

CHECKS = [
    # (name, era_lo, A expr table alias o=off same-team, B expr, join: same|opp, B table)
    ("passing_first_downs == receiving_first_downs", 1978, "o.p_fd", "o.r_fd", "same", None),
    ("passing_2pt == receiving_2pt", 1999, "o.p2", "o.r2", "same", None),
    ("pass_explosive_20 == rec_explosive_20", 1978, "o.pex", "o.rex", "same", None),
    ("rz_pass_td == rz_rec_td", 1978, "o.rzp", "o.rzr", "same", None),
    ("completions_40plus == receptions_40plus", 1978, "o.c40", "o.r40", "same", None),
    ("passing_tds_40plus == receiving_tds_40plus", 1978, "o.ptd40", "o.rtd40", "same", None),
    ("passing_tds_50plus == receiving_tds_50plus", 1978, "o.ptd50", "o.rtd50", "same", None),
    ("pass_success_plays == rec_success_plays", 1978, "o.psp", "o.rsp", "same", None),
    ("passing_yac == receiving_yac (team)", 2006, "o.p_yac", "o.r_yac", "same", None),
    ("passing_completed_air == receiving_completed_air", 2006, "o.p_cay", "o.r_cay", "same", None),
    ("passing_drops(QB) == receiving_drops(recv)", 2018, "o.qb_drp", "o.rec_drp", "same", None),
    ("targets <= attempts (1978-91 extension)", 1978, "o.tgt", "o.att", "same_le", None),
    ("pick6(QBs A) == def_int_ret_td(IDP sum B)", 1978, "o.pick6", "b.d_p6", "opp", "idp"),
    ("pick6(QBs A) == def_int_ret_td(DST row B)", 1978, "o.pick6", "b.dst_p6", "opp", "dst"),
    ("blocked kicks: opp def_blk_kick(IDP) == pblk+fgblk+patblk", 1999, "o.pblk+o.fgblk+o.patblk", "b.d_blk", "opp", "idp"),
    ("QB pressured(A) == def_pressures(IDP sum B)", 2018, "o.qb_prs", "b.d_prs", "opp", "idp"),
    ("QB hurried(A) == def_hurries(IDP sum B)", 2018, "o.qb_hur", "b.d_hur", "opp", "idp"),
    ("QB hits(A) == def_knockdowns(IDP sum B)", 2018, "o.qb_hit", "b.d_kd", "opp", "idp"),
    ("def_plays(DST B) == att+carries+sacks(A)", 1978, "o.att+o.car+o.sk", "b.d_plays", "opp", "dst"),
    ("def_yards_allowed(DST B) == scrimmage yds(A)", 1978, "o.ry+o.recy", "b.d_yds", "opp", "dst"),
]

print(f"{'identity':56}{'era':>6}{'games':>9}{'agree%':>8}{'med|diff|':>10}{'worst-era agree%':>18}")
print("-" * 110)
for name, lo, ea, eb, mode, btab in CHECKS:
    if mode.startswith("same"):
        join = f"FROM off o WHERE o.y>={lo}"
    else:
        join = (f"FROM off o JOIN {btab} b ON b.y=o.y AND b.w=o.w AND b.st=o.st AND b.f=o.opp "
                f"WHERE o.y>={lo}")
    cmp_expr = f"({ea})-({eb})"
    ok = f"ABS({cmp_expr})<0.5" if mode != "same_le" else f"({ea})<=({eb})+0.5"
    try:
        rows = con.execute(f"""
          SELECT (o.y//10)*10 AS dcd, COUNT(*) n, COUNT(*) FILTER (WHERE {ok}) good,
                 MEDIAN(ABS({cmp_expr})) md
          {join} GROUP BY 1 ORDER BY 1""").fetchall()
    except Exception as e:  # noqa: BLE001
        print(f"{name:56}{lo:>6}  ERR {e}")
        continue
    n = sum(r[1] for r in rows); good = sum(r[2] for r in rows)
    if not n:
        print(f"{name:56}{lo:>6}{'0':>9}")
        continue
    md = max(rows, key=lambda r: r[1])[3]
    worst = min(rows, key=lambda r: r[2] / r[1])
    print(f"{name:56}{lo:>6}{n:>9,}{100*good/n:>8.1f}{md:>10.1f}"
          f"   {100*worst[2]/worst[1]:>6.1f} ({int(worst[0])}s)")
