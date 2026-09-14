"""
sota_recon/recon_scoring_vs_fly.py  --  LANE: scoring-calc verification vs the platform (Fly).

Independent check that OUR scoring engine is right: recompute each rostered player-week's points
from the super-table ATOMS x the league's OWN per-year scoring config, and compare to the
platform's STORED fantasy_points (___leagues.player_fantasy). Cross-database, server-side on Fly
(read-only): ___leagues.player_fantasy + ___leagues.league_settings + ___ops super table.

This is the strongest scoring test because it works for ANY ruleset (not just clean variants) and
isolates scoring-formula / atom disagreements per position. Offense is straightforward; K/DST/IDP
are tier-heavy and platform-specific -> reported best-effort with explicit coverage.

Buckets: QB/RB/WR/TE (offense), K (kicker), DST (team defense), IDP (LB/DL/DB).
Per bucket: rows compared, MAD (mean abs diff), match% within 0.5, and worst leagues + a weird-combo
breakdown of mismatches (return TD / 2pt / safety present) so messy cases are visible.

    python -m scripts.sota_recon.recon_scoring_vs_fly                 # default multi-platform sample
    python -m scripts.sota_recon.recon_scoring_vs_fly --all           # every league (slow)
    python -m scripts.sota_recon.recon_scoring_vs_fly --db the_league # one league
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys

_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))
from multi_league.data_fetchers.aggregate_nfl_stats_fly import load_env  # reuses .env loader
from multi_league.core.readers.fly_reader import FlyReader

PF = "___leagues.public.player_fantasy"
LS = "___leagues.public.league_settings"
S = "___ops.nfl_historical.nfl_player_stats_all"
TOL = 0.5

# offense expected points from atoms x league config (incl return TDs + offensive 2pt)
OFF_EXPECTED = f"""
  s.passing_yards*ls.scoring_pass_yd + s.passing_tds*ls.scoring_pass_td
  + s.passing_interceptions*COALESCE(ls.scoring_pass_int,0)
  + s.rushing_yards*ls.scoring_rush_yd + s.rushing_tds*ls.scoring_rush_td
  + s.receiving_yards*ls.scoring_rec_yd + s.receiving_tds*ls.scoring_rec_td
  + s.receptions*COALESCE(ls.scoring_rec,0)
  + COALESCE(s.fumbles_lost,0)*COALESCE(ls.scoring_fum_lost,0)
  + (COALESCE(s.passing_2pt_conversions,0)+COALESCE(s.rushing_2pt_conversions,0)
     +COALESCE(s.receiving_2pt_conversions,0))*2
  + (COALESCE(s.kickoff_return_tds,0)+COALESCE(s.punt_return_tds,0)
     +COALESCE(s.fum_ret_td,0))*6
  + COALESCE(s.passing_first_downs,0)*COALESCE(ls.scoring_pass_fd,0)
  + COALESCE(s.rushing_first_downs,0)*COALESCE(ls.scoring_rush_fd,0)
  + COALESCE(s.receiving_first_downs,0)*COALESCE(ls.scoring_rec_fd,0)
  + COALESCE(s.kickoff_return_yards,0)*COALESCE(ls.scoring_kr_yd,0)
  + COALESCE(s.punt_return_yards,0)*COALESCE(ls.scoring_pr_yd,0)
"""
STORED = "pf.fantasy_points - COALESCE(pf.bonus_points,0) - COALESCE(pf.te_premium_points,0)"


def _reader():
    load_env()
    return FlyReader()


def offense(r, where):
    sql = f"""
    WITH cmp AS (
      SELECT pf.db_name, pf.position, {STORED} AS stored, ({OFF_EXPECTED}) AS expected,
             (COALESCE(s.kickoff_return_tds,0)+COALESCE(s.punt_return_tds,0)+COALESCE(s.fum_ret_td,0))>0 AS has_rettd,
             (COALESCE(s.passing_2pt_conversions,0)+COALESCE(s.rushing_2pt_conversions,0)+COALESCE(s.receiving_2pt_conversions,0))>0 AS has_2pt
      FROM {PF} pf JOIN {LS} ls ON ls.db_name=pf.db_name AND ls.year=pf.year
      JOIN {S} s ON s.player_week=pf.player_week
      WHERE pf.position IN ('QB','RB','WR','TE') AND pf.is_started=1 AND s.season_type='REG'
        AND pf.fantasy_points IS NOT NULL AND ls.scoring_pass_yd IS NOT NULL {where})
    SELECT position, COUNT(*) n, ROUND(AVG(ABS(stored-expected)),3) mad,
           ROUND(100.0*COUNT(*) FILTER (WHERE ABS(stored-expected)<={TOL})/COUNT(*),2) pct,
           COUNT(*) FILTER (WHERE ABS(stored-expected)>{TOL} AND has_rettd) miss_rettd,
           COUNT(*) FILTER (WHERE ABS(stored-expected)>{TOL} AND has_2pt) miss_2pt,
           COUNT(*) FILTER (WHERE ABS(stored-expected)>{TOL}) miss
    FROM cmp GROUP BY position ORDER BY position"""
    return r.query(sql, database="___leagues")


def kicker(r, where):
    # flat-FG leagues only (scoring_fgm present and per-distance tiers absent/zero) -> expected = fg_made*fgm + pat
    sql = f"""
    WITH cmp AS (
      SELECT pf.db_name, {STORED} AS stored,
        COALESCE(s.fg_made,0)*COALESCE(ls.scoring_fgm,0) + COALESCE(s.pat_made,0)*COALESCE(ls.scoring_xpm,0) AS expected
      FROM {PF} pf JOIN {LS} ls ON ls.db_name=pf.db_name AND ls.year=pf.year
      JOIN {S} s ON s.player_week=pf.player_week
      WHERE pf.position='K' AND pf.is_started=1 AND s.season_type='REG' AND pf.fantasy_points IS NOT NULL
        AND COALESCE(ls.scoring_fgm,0)<>0 {where})
    SELECT COUNT(*) n, ROUND(AVG(ABS(stored-expected)),3) mad,
           ROUND(100.0*COUNT(*) FILTER (WHERE ABS(stored-expected)<={TOL})/NULLIF(COUNT(*),0),2) pct
    FROM cmp"""
    return r.query(sql, database="___leagues")


def dst(r, where):
    # best-effort flat DST (sacks/int/fr/td/safety/blk); points-allowed tiers vary -> report MAD + match,
    # NOT a strict pass (tiered PA not modeled here). Uses pts_def_std as our engine's standard output.
    sql = f"""
    WITH cmp AS (
      SELECT pf.db_name, {STORED} AS stored, s.pts_def_std AS our_std
      FROM {PF} pf JOIN {S} s ON s.player_week=pf.player_week
      WHERE pf.position='DEF' AND pf.is_started=1 AND s.season_type='REG'
        AND pf.fantasy_points IS NOT NULL AND s.pts_def_std IS NOT NULL {where})
    SELECT COUNT(*) n, ROUND(AVG(ABS(stored-our_std)),3) mad,
           ROUND(100.0*COUNT(*) FILTER (WHERE ABS(stored-our_std)<=1.0)/NULLIF(COUNT(*),0),2) pct_within1
    FROM cmp"""
    return r.query(sql, database="___leagues")


def idp(r, where):
    # IDP: tackles*?, sacks*?, int*?, ff*? -> compare vs pts_idp_std (our standard IDP) best-effort
    sql = f"""
    WITH cmp AS (
      SELECT pf.db_name, {STORED} AS stored, s.pts_idp_std AS our_std
      FROM {PF} pf JOIN {S} s ON s.player_week=pf.player_week
      WHERE pf.position IN ('LB','DL','DB','DE','DT','CB','S','ILB','OLB','MLB','NT','SS','FS','EDGE')
        AND pf.is_started=1 AND s.season_type='REG' AND pf.fantasy_points IS NOT NULL AND s.pts_idp_std IS NOT NULL {where})
    SELECT COUNT(*) n, ROUND(AVG(ABS(stored-our_std)),3) mad,
           ROUND(100.0*COUNT(*) FILTER (WHERE ABS(stored-our_std)<=1.0)/NULLIF(COUNT(*),0),2) pct_within1
    FROM cmp"""
    return r.query(sql, database="___leagues")


SAMPLE = ("the_league", "you_are_a_pirate", "nyu_ffl", "the_real_ff_league",
          "tfl_of_extraordinary_gentleman", "the_pigskin_platoon", "zootown_dynasty",
          "keeper_league", "degenerate_gamblers_football_league")


def run(db=None, all_leagues=False):
    r = _reader()
    if db:
        where = f"AND pf.db_name='{db}'"
    elif all_leagues:
        where = ""
    else:
        inlist = ",".join(f"'{d}'" for d in SAMPLE)
        where = f"AND pf.db_name IN ({inlist})"
    return {"offense": offense(r, where), "kicker": kicker(r, where),
            "dst": dst(r, where), "idp": idp(r, where), "scope": db or ("ALL" if all_leagues else "sample")}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db"); ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    res = run(db=a.db, all_leagues=a.all)
    print(f"scope: {res['scope']}\n")
    print("OFFENSE (atoms x league config vs platform stored, REG, started):")
    for row in res["offense"]:
        print(f"  {row['position']:3} n={row['n']:>7,} match={row['pct']}%  MAD={row['mad']}  "
              f"miss={row['miss']} (rettd {row['miss_rettd']}, 2pt {row['miss_2pt']})")
    k = res["kicker"][0] if res["kicker"] else {}
    print(f"\nKICKER (flat-FG leagues): n={k.get('n',0):,} match={k.get('pct')}%  MAD={k.get('mad')}")
    d = res["dst"][0] if res["dst"] else {}
    print(f"DST (vs pts_def_std, tiered-PA not modeled): n={d.get('n',0):,} within1={d.get('pct_within1')}%  MAD={d.get('mad')}")
    i = res["idp"][0] if res["idp"] else {}
    print(f"IDP (vs pts_idp_std, best-effort): n={i.get('n',0):,} within1={i.get('pct_within1')}%  MAD={i.get('mad')}")
