"""v4: quantile-matched to the real population shape, with the mass-point boundary fixed.

WHAT EACH VERSION GOT RIGHT AND WRONG.
  v2  quantile-matched (correct SHAPE) but placed each cut exactly ON a mass point, so a
      10-team league starting exactly 10 QBs fell above a 10.0 cut and read 12tm.
  v3  midpoints 9r/11r/13r fixed the BOUNDARY but abandoned shape matching, so the tier
      proportions stopped matching the real league-size distribution and 14tm swelled to
      44-62% of the corpus.
  v4  quantile-match (v2's shape) AND nudge any cut that lands on a mass point up past it
      (v3's boundary correctness). Same buckets, right proportions, right edges.

R21 established why the shape must match: the tiers carry the same proportions as the real
population even though membership differs, and that beat equal-spacing by 22.7% on within-tier
heterogeneity.

THE NUDGE. A cut is moved to `v + 0.5` when more than 2% of the lane sits at exactly `v`.
K started IS num_teams, so 8/10/12 are dense mass points and `>= cut` would sweep a whole
league size up a tier. Continuous capacities (WR rostered) have no mass points and are left
untouched.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

IDP = ("DL", "LB", "DB")
GATED = {"K", "DEF", "DL", "LB", "DB"}
LANES = {"QB": ("flx", "sflx")}
SLOT_COL = {"QB": "roster_QB", "RB": "roster_RB", "WR": "roster_WR", "TE": "roster_TE",
            "K": "roster_K", "DEF": "roster_DEF", "DL": "roster_DL", "LB": "roster_LB",
            "DB": "roster_DB"}


def lane_sql(pos: str, lane: str | None) -> str:
    idp_free = ("COALESCE(s.roster_IDP,0)=0 AND COALESCE(s.roster_DL,0)=0 "
                "AND COALESCE(s.roster_LB,0)=0 AND COALESCE(s.roster_DB,0)=0")
    base = "COALESCE(s.sleeper_best_ball,false)=false AND COALESCE(s.is_dynasty,false)=false"
    if pos in IDP:
        combo = {"DL": ["roster_DL", "roster_DL_LB", "roster_IDP"],
                 "LB": ["roster_LB", "roster_DL_LB", "roster_DB_LB", "roster_IDP"],
                 "DB": ["roster_DB", "roster_DB_LB", "roster_IDP"]}[pos]
        joined = " + ".join(f"COALESCE(s.{c},0)" for c in combo)
        return f"{base} AND ({joined}) > 0"
    if pos in GATED:
        return f"{base} AND {idp_free} AND COALESCE(s.{SLOT_COL[pos]},0) > 0"
    op = ">" if lane == "sflx" else "="
    return f"{base} AND {idp_free} AND COALESCE(s.roster_SUPER_FLEX,0) {op} 0"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--positions", default="QB,RB,WR,TE,K,DEF,DL,LB,DB")
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for pos in a.positions.split(","):
        for lane in LANES.get(pos, (None,)):
            print(f"  {pos}{'/' + lane if lane else ''} ...", flush=True)
            for yr in range(2021, 2026):
                con = duckdb.connect(config={
                    "memory_limit": "1400MB", "threads": 2,
                    "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/v4{pos}{yr}"})
                try:
                    for s in ("SET enable_progress_bar=false",
                              "SET preserve_insertion_order=false",
                              "PRAGMA max_temp_directory_size='3GB'"):
                        con.execute(s)
                    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS l (READ_ONLY)")
                    con.execute(f"ATTACH '{a.ops.as_posix()}' AS o (READ_ONLY)")
                    con.execute(
                        'CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid, '
                        'MAX(UPPER(TRIM(position))) p '
                        'FROM o.nfl_historical.nfl_player_stats_all '
                        f'WHERE "year"={yr} AND NFL_player_id IS NOT NULL '
                        "AND position NOT LIKE '%,%' GROUP BY 1")
                    con.execute(f"""CREATE OR REPLACE TEMP TABLE cap AS
                      SELECT c.db_name, s.num_teams, c.ros, c.st
                      FROM (SELECT db_name, AVG(nros) ros, AVG(nst) st FROM
                             (SELECT pf.db_name, pf.week, COUNT(*) nros,
                                     SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END) nst
                              FROM l.public.player_fantasy pf
                              JOIN pos ON pos.pid=pf.NFL_player_id
                              WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17
                                AND pos.p='{pos}' GROUP BY 1,2) GROUP BY 1) c
                      JOIN l.public.league_settings s
                        ON s.db_name=c.db_name AND s.year={yr}
                      WHERE s.num_teams BETWEEN 4 AND 24 AND {lane_sql(pos, lane)}""")
                    n = con.execute("SELECT COUNT(*) FROM cap").fetchone()[0]
                    if n < 200:
                        continue
                    # TARGET SHAPE: the real league-size distribution inside this very lane
                    t = con.execute("""SELECT
                        SUM(CASE WHEN num_teams<=8 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE a,
                        SUM(CASE WHEN num_teams<=10 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE b,
                        SUM(CASE WHEN num_teams<=12 THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE c
                      FROM cap""").fetchdf().iloc[0]
                    for stat, col in (("rostered", "ros"), ("started", "st")):
                        q = con.execute(f"""SELECT quantile_cont({col},{t.a}) c1,
                              quantile_cont({col},{t.b}) c2, quantile_cont({col},{t.c}) c3
                            FROM cap""").fetchdf().iloc[0]
                        cuts = []
                        for v in (q.c1, q.c2, q.c3):
                            share = con.execute(
                                f"SELECT COUNT(*)/{n}::DOUBLE FROM cap "
                                f"WHERE abs({col} - {v}) < 0.001").fetchone()[0]
                            cuts.append(round(v + 0.5, 2) if share > 0.02 else round(v, 2))
                        got = con.execute(f"""SELECT
                            SUM(CASE WHEN {col}<{cuts[0]} THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE s8,
                            SUM(CASE WHEN {col}>={cuts[0]} AND {col}<{cuts[1]} THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE s10,
                            SUM(CASE WHEN {col}>={cuts[1]} AND {col}<{cuts[2]} THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE s12,
                            SUM(CASE WHEN {col}>={cuts[2]} THEN 1 ELSE 0 END)/COUNT(*)::DOUBLE s14
                          FROM cap""").fetchdf().iloc[0]
                        rows.append({
                            "position": pos, "lane": lane or "-", "stat": stat, "year": yr,
                            "leagues": n, "c1": cuts[0], "c2": cuts[1], "c3": cuts[2],
                            "t8": round(100 * t.a), "t10": round(100 * (t.b - t.a)),
                            "t12": round(100 * (t.c - t.b)), "t14": round(100 * (1 - t.c)),
                            "g8": round(100 * got.s8), "g10": round(100 * got.s10),
                            "g12": round(100 * got.s12), "g14": round(100 * got.s14)})
                finally:
                    con.close()

    d = pd.DataFrame(rows)
    d.to_parquet(a.out, index=False)
    pd.set_option("display.width", 230)
    g = (d.groupby(["position", "lane", "stat"])
           .agg(c1=("c1", "median"), c2=("c2", "median"), c3=("c3", "median"),
                t8=("t8", "median"), t10=("t10", "median"),
                t12=("t12", "median"), t14=("t14", "median"),
                g8=("g8", "median"), g10=("g10", "median"),
                g12=("g12", "median"), g14=("g14", "median"),
                drift=("c2", lambda s: round(100 * (s.max() - s.min()) / s.mean())))
           .round(1))
    g["shape_err"] = (abs(g.g8 - g.t8) + abs(g.g10 - g.t10)
                      + abs(g.g12 - g.t12) + abs(g.g14 - g.t14)).round(0)
    print("\nv4 QUANTILE-MATCHED CUTS -- target (t*) vs achieved (g*) tier shares\n")
    print(g.to_string())


if __name__ == "__main__":
    main()
