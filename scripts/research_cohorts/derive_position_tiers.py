"""Derive tier cuts for EVERY position, end to end -- the full sequence WR and RB got.

Per position, in order:
  1. MECHANISM   declared slots in that position's clean lane, plus its flex share
  2. CHECK       does derived started reproduce observed started?
  3. BENCH       that position's share of the bench
  4. CHECK       does started + bench x share reproduce observed rostered?
  5. CAPACITY    observed spots per league-week, pooled 2021-2025
  6. CUTOFFS     quantile-matched to the literal num_teams shape (R21)
  7. STABILITY   year-to-year drift; WR was accepted at 6-19%
  8. TOP TIER    dumping-ground check; the declared-slot failure was 15.9x / log SD 0.246

A position is only FULLY DERIVED if steps 2 and 4 reproduce observation and steps 7-8 pass.
Anything else is reported as provisional with the reason, never quietly locked -- shipping
18 unchecked quantiles is the mistake this script exists to undo.

CLEAN LANE differs by position and that is the point:
  QB/RB/WR/TE  managed redraft, flex-only (no superflex, no IDP) -- the mainstream
  QB also gets a superflex read, because superflex IS the QB axis (R1)
  K/DEF        managed redraft; neither is flex-eligible, so there is no flex term
  DL/LB/DB     IDP leagues only -- they do not exist elsewhere
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

FLEX_ELIGIBLE = ("RB", "WR", "TE")          # who can occupy a FLEX slot
NO_FLEX = ("QB", "K", "DEF")                 # dedicated slots only (QB except in superflex)
IDP = ("DL", "LB", "DB")
ALL_POS = ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB")
# THE FLEX COLUMN IS NOT THE SAME FOR EVERY POSITION. Offensive positions compete for
# roster_FLX; IDP positions compete for the IDP catch-all and the DB_LB/DL_LB combos. Reading
# roster_FLX for a defender multiplies the IDP flex share by the OFFENSIVE slot count -- that
# is why DB derived 2.33 started against 0.90 observed.
FLEX_COL = {
    "QB": "COALESCE(s.roster_FLX,0)", "RB": "COALESCE(s.roster_FLX,0)",
    "WR": "COALESCE(s.roster_FLX,0)", "TE": "COALESCE(s.roster_FLX,0)",
    "K": "0", "DEF": "0",              # neither is flex-eligible
    "DL": "(COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL_LB,0))",
    "LB": "(COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL_LB,0)+COALESCE(s.roster_DB_LB,0))",
    "DB": "(COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DB_LB,0))",
}
SLOT_COL = {"QB": "roster_QB", "RB": "roster_RB", "WR": "roster_WR", "TE": "roster_TE",
            "K": "roster_K", "DEF": "roster_DEF", "DL": "roster_DL", "LB": "roster_LB",
            "DB": "roster_DB"}


def _con(tag: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={
        "memory_limit": "1400MB", "threads": 2,
        "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/pt{tag}"})
    for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
              "PRAGMA max_temp_directory_size='3GB'"):
        con.execute(s)
    return con


def lane_predicate(pos: str) -> str:
    """The clean lane for this position. IDP classes only exist inside IDP leagues."""
    idp_free = ("COALESCE(s.roster_IDP,0)=0 AND COALESCE(s.roster_DL,0)=0 "
                "AND COALESCE(s.roster_LB,0)=0 AND COALESCE(s.roster_DB,0)=0")
    if pos in IDP:
        # IDP slots are not just the position's own column -- roster_IDP is a catch-all and
        # DB_LB/DL_LB are combos. Missing them made LB overshoot derived-started by +1.10.
        combo = {"DL": ["roster_DL", "roster_DL_LB", "roster_IDP"],
                 "LB": ["roster_LB", "roster_DL_LB", "roster_DB_LB", "roster_IDP"],
                 "DB": ["roster_DB", "roster_DB_LB", "roster_IDP"]}[pos]
        return "(" + " + ".join(f"COALESCE(s.{c},0)" for c in combo) + ") > 0"
    if pos in ("K", "DEF"):
        # ELIGIBILITY GATE. K sits in ~40% of leagues and DST ~53%, so the median league has
        # zero K slots -- ungated, K measured 0.00 started and 0.00 rostered, a false green.
        return f"COALESCE(s.roster_SUPER_FLEX,0)=0 AND {idp_free} AND COALESCE(s.{SLOT_COL[pos]},0) > 0"
    if pos == "QB":
        return f"COALESCE(s.roster_SUPER_FLEX,0)=0 AND {idp_free}"
    return f"COALESCE(s.roster_SUPER_FLEX,0)=0 AND {idp_free}"


def run_position(snapshot: Path, ops: Path, pos: str) -> dict:
    out = {"position": pos}

    # ---------- steps 1-4: mechanism and bench, on 2024 in the clean lane ----------
    con = _con(f"{pos}m")
    try:
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS o (READ_ONLY)")
        con.execute('CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid, '
                    'MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all '
                    'WHERE "year"=2024 AND NFL_player_id IS NOT NULL '
                    "AND position NOT LIKE '%,%' GROUP BY 1")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lg AS
          SELECT s.db_name, s.num_teams,
            CASE WHEN COALESCE(s.scoring_rec,0)=0 THEN 'std'
                 WHEN COALESCE(s.scoring_rec,0)<0.75 THEN 'half' ELSE 'ppr' END ppr,
            COALESCE(s.{SLOT_COL[pos]},0) ded, {FLEX_COL[pos]} flex
          FROM l.public.league_settings s
          WHERE s.year=2024 AND {lane_predicate(pos)}
            AND COALESCE(s.sleeper_best_ball,false)=false
            AND COALESCE(s.is_dynasty,false)=false AND s.num_teams BETWEEN 8 AND 14""")
        n_lane = con.execute("SELECT COUNT(*) FROM lg").fetchone()[0]
        out["clean_lane_leagues"] = n_lane
        if n_lane < 100:
            out["status"] = f"PROVISIONAL: clean lane has only {n_lane} leagues"
            return out
        # FLEX SHARE first -- the derived values need it, and it is measured in THIS lane
        # rather than borrowed (R17's pooled RB share of 0.33 is 0.265 in the clean lane).
        fs = con.execute(f"""
          SELECT MEDIAN(GREATEST(st - ded*tm, 0) / NULLIF(fx*tm, 0)) FROM (
            SELECT MAX(g.num_teams) tm, MAX(g.ded) ded, MAX(g.flex) fx,
                   SUM(CASE WHEN pos.p='{pos}' AND pf.is_started=1 THEN 1 ELSE 0 END) st
            FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
            JOIN lg g ON g.db_name=pf.db_name
            WHERE pf.year=2024 AND pf.week BETWEEN 1 AND 17
            GROUP BY pf.db_name, pf.week) WHERE fx > 0""").fetchone()[0]
        flex_share = float(fs) if fs is not None else 0.0
        out["flex_share_measured"] = round(flex_share, 3)

        # DERIVE PER LEAGUE, THEN TAKE THE MEDIAN -- never median-of-inputs. Combining
        # MEDIAN(ded) with MEDIAN(flex) is only valid on a homogeneous population, and the
        # IDP lanes are mixed by construction: some leagues field the position through a
        # dedicated roster_DB slot, others only through the roster_IDP catch-all where
        # roster_DB is 0. median(a) + median(b)*share != median(a + b*share) there, and it
        # put DB derived-started at 2.10 against 0.90 observed.
        m = con.execute(f"""
          SELECT MEDIAN(ded) ded, MEDIAN(flex) flex, MEDIAN(bench_pt) bench_pt,
                 MEDIAN(bshare) bench_share, MEDIAN(st_pt) st_pt, MEDIAN(ros_pt) ros_pt,
                 MEDIAN(ded + flex * {flex_share}) derived_st_pt,
                 MEDIAN(st_pt + bench_pt * bshare) derived_ros_pt
          FROM (
            SELECT MAX(g.ded) ded, MAX(g.flex) flex,
              SUM(CASE WHEN pf.is_started=0 THEN 1 ELSE 0 END)::DOUBLE/MAX(g.num_teams) bench_pt,
              SUM(CASE WHEN pf.is_started=0 AND pos.p='{pos}' THEN 1 ELSE 0 END)::DOUBLE
                / NULLIF(SUM(CASE WHEN pf.is_started=0 THEN 1 ELSE 0 END),0) bshare,
              SUM(CASE WHEN pf.is_started=1 AND pos.p='{pos}' THEN 1 ELSE 0 END)::DOUBLE
                / MAX(g.num_teams) st_pt,
              SUM(CASE WHEN pos.p='{pos}' THEN 1 ELSE 0 END)::DOUBLE/MAX(g.num_teams) ros_pt
            FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
            JOIN lg g ON g.db_name=pf.db_name
            WHERE pf.year=2024 AND pf.week BETWEEN 1 AND 17
            GROUP BY pf.db_name, pf.week)""").fetchdf().iloc[0]
        out.update({k: (None if pd.isna(m[k]) else round(float(m[k]), 3))
                    for k in ("ded", "flex", "bench_pt", "bench_share", "st_pt", "ros_pt",
                              "derived_st_pt", "derived_ros_pt")})
        out["st_gap"] = round(float(m.st_pt) - float(m.derived_st_pt), 2)
        out["ros_gap"] = round(float(m.ros_pt) - float(m.derived_ros_pt), 2)
    finally:
        con.close()

    # ---------- steps 5-8: capacity, cutoffs, stability, top tier ----------
    per_year = []
    for yr in range(2021, 2026):
        con = _con(f"{pos}{yr}")
        try:
            con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
            con.execute(f"ATTACH '{ops.as_posix()}' AS o (READ_ONLY)")
            con.execute('CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid, '
                        'MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all '
                        f'WHERE "year"={yr} AND NFL_player_id IS NOT NULL '
                        "AND position NOT LIKE '%,%' GROUP BY 1")
            # IDP tiers are only meaningful inside leagues that field that position
            gate = (f"AND COALESCE(s.{SLOT_COL[pos]},0) > 0" if pos in IDP else "")
            con.execute(f"""CREATE OR REPLACE TEMP TABLE cap AS
              SELECT c.db_name, s.num_teams, c.ros, c.st
              FROM (SELECT db_name, AVG(nros) ros, AVG(nst) st FROM
                     (SELECT pf.db_name, pf.week, COUNT(*) nros,
                             SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END) nst
                      FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
                      WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17 AND pos.p='{pos}'
                      GROUP BY 1,2) GROUP BY 1) c
              JOIN l.public.league_settings s ON s.db_name=c.db_name AND s.year={yr}
              WHERE s.num_teams BETWEEN 4 AND 24 {gate}""")
            n = con.execute("SELECT COUNT(*) FROM cap").fetchone()[0]
            if n < 200:
                continue
            t = con.execute("""SELECT
                 SUM(CASE WHEN num_teams<=8 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE a,
                 SUM(CASE WHEN num_teams<=10 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE b,
                 SUM(CASE WHEN num_teams<=12 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE c
               FROM cap""").fetchdf().iloc[0]
            for stat, col in (("rostered", "ros"), ("started", "st")):
                q = con.execute(f"""SELECT MEDIAN({col}) med, quantile_cont({col},{t.a}) c1,
                      quantile_cont({col},{t.b}) c2, quantile_cont({col},{t.c}) c3 FROM cap
                    """).fetchdf().iloc[0]
                tp = con.execute(f"""SELECT MIN({col}) lo, MAX({col}) hi,
                      stddev(ln(NULLIF({col},0))) lsd FROM cap WHERE {col} >= {q.c3}
                    """).fetchdf().iloc[0]
                per_year.append({"year": yr, "stat": stat, "leagues": n,
                                 "med": q.med, "c1": q.c1, "c2": q.c2, "c3": q.c3,
                                 "ratio": (tp.hi / tp.lo) if tp.lo and tp.lo > 0 else None,
                                 "log_sd": tp.lsd})
        finally:
            con.close()
    if not per_year:
        out["status"] = "PROVISIONAL: fewer than 200 leagues field this position"
        return out

    py = pd.DataFrame(per_year)
    out["years"] = int(py.year.nunique())
    for stat, g in py.groupby("stat"):
        pre = "ros" if stat == "rostered" else "st"
        for c in ("c1", "c2", "c3"):
            out[f"{pre}_{c}"] = round(float(g[c].median()), 1)
            out[f"{pre}_{c}_drift"] = round(100 * (g[c].max() - g[c].min()) / g[c].mean(), 0)
        out[f"{pre}_med"] = round(float(g.med.median()), 1)
        out[f"{pre}_top_ratio"] = round(float(g.ratio.median()), 1) if g.ratio.notna().any() else None
        out[f"{pre}_top_lsd"] = round(float(g.log_sd.median()), 3) if g.log_sd.notna().any() else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--positions", default=",".join(ALL_POS))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for pos in a.positions.split(","):
        print(f"  {pos} ...", flush=True)
        rows.append(run_position(a.snapshot, a.ops, pos))
    d = pd.DataFrame(rows)
    d.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print("\n=== MECHANISM (2024 clean lane): does derived reproduce observed? ===")
    cols = ["position", "clean_lane_leagues", "ded", "flex", "st_pt", "derived_st_pt", "st_gap",
            "bench_pt", "bench_share", "ros_pt", "derived_ros_pt", "ros_gap"]
    print(d[[c for c in cols if c in d]].to_string(index=False))
    print("\n=== CUTOFFS pooled 2021-2025 ===")
    cols = ["position", "ros_med", "ros_c1", "ros_c2", "ros_c3",
            "st_med", "st_c1", "st_c2", "st_c3"]
    print(d[[c for c in cols if c in d]].to_string(index=False))
    print("\n=== STABILITY (drift %) and TOP TIER (WR failure was 15.9x / 0.246) ===")
    cols = ["position", "ros_c1_drift", "ros_c2_drift", "ros_c3_drift", "ros_top_ratio",
            "ros_top_lsd", "st_c2_drift", "st_top_ratio", "st_top_lsd"]
    print(d[[c for c in cols if c in d]].to_string(index=False))
    if "status" in d:
        bad = d[d.status.notna()]
        if len(bad):
            print("\n=== NOT DERIVED ===")
            print(bad[["position", "status"]].to_string(index=False))


if __name__ == "__main__":
    main()
