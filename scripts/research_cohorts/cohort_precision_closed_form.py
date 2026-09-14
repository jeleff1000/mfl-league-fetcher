"""How many leagues are needed to hit +/-1/3/5% on each rate, by position/cohort/decile.

Closed form, no bootstrap.  Sampling K leagues from L is finite-population sampling of a
league-level value, so  N = z^2*s2 / (e^2 + z^2*s2/L).

Denominator law: the eligible, live league set -- absence of a player_fantasy row IS a zero.
Absent leagues contribute 0 to both sum and sumsq, so full-population moments come from the
present rows plus the denominator count.  No zero rows are ever materialised.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import pandas as pd

MARGINS = (1.0, 3.0, 5.0)
CONFIDENCE = 0.95
POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")

# A league is eligible for a position only if it carries a slot that position can fill.
ELIGIBILITY = {
    "QB":  "COALESCE(roster_QB,0)+COALESCE(roster_SUPER_FLEX,0)",
    "RB":  "COALESCE(roster_RB,0)+COALESCE(roster_FLX,0)",
    "WR":  "COALESCE(roster_WR,0)+COALESCE(roster_FLX,0)",
    "TE":  "COALESCE(roster_TE,0)+COALESCE(roster_FLX,0)",
    "K":   "COALESCE(roster_K,0)",
    "DEF": "COALESCE(roster_DEF,0)",
}

COHORTS = """
UNNEST(list_filter([
  CASE WHEN num_teams=10 THEN 'teams_10' END,
  CASE WHEN num_teams=12 THEN 'teams_12' END,
  CASE WHEN scoring_rec>=0.99 THEN 'ppr_full' END,
  CASE WHEN scoring_rec IS NULL OR scoring_rec<0.01 THEN 'ppr_std' END,
  CASE WHEN scoring_rec BETWEEN 0.4 AND 0.6 THEN 'ppr_half' END,
  CASE WHEN scoring_pass_td=4 THEN 'passtd_4' END,
  CASE WHEN scoring_pass_td=6 THEN 'passtd_6' END,
  CASE WHEN roster_SUPER_FLEX>0 THEN 'superflex' END,
  CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
           +COALESCE(roster_DB,0)>0 THEN 'idp' END,
  'ALL'], x -> x IS NOT NULL)) AS t(cohort)
"""


def required_leagues(var: float, L: float, margin_pct: float, z: float) -> float | None:
    """Finite-population sample size for a margin expressed in percentage points."""
    if var is None or not (var == var) or var < 0 or L is None or L < 2:
        return None
    e = margin_pct / 100.0
    if var <= 0:
        return 2.0
    naive = z * z * var / (e * e)
    return naive / (1.0 + naive / L)


def build(args: argparse.Namespace) -> pd.DataFrame:
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "threads": args.threads,
                                 "temp_directory": str(args.tmp)})
    con.execute("SET enable_progress_bar=false")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")

    frames = []
    for year in args.years:
        print(f"[{year}] building...", flush=True)

        elig_cases = ",\n".join(
            f"  CASE WHEN ({expr})>0 THEN '{pos}' END" for pos, expr in ELIGIBILITY.items()
        )
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE league_cohort AS
        SELECT db_name, cohort, position FROM lake.public.league_settings,
          {COHORTS},
          UNNEST(list_filter([\n{elig_cases}\n], x -> x IS NOT NULL)) AS p(position)
        WHERE "year"={year} AND NOT COALESCE(sleeper_best_ball, FALSE)
        """)

        # NFL-side facts: natural position, and weekly activity for Healthy Start %.
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nfl AS
        SELECT NFL_player_id, CAST("week" AS INTEGER) AS week,
               MAX(CASE WHEN UPPER(TRIM(position)) IN ('DST','D/ST','DEF') THEN 'DEF'
                        ELSE UPPER(TRIM(position)) END) AS position,
               MAX(CASE WHEN COALESCE(offense_snaps,0)+COALESCE(defense_snaps,0)
                             +COALESCE(special_teams_snaps,0)>=20 THEN 1 ELSE 0 END) AS is_active
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE "year"={year} AND NFL_player_id IS NOT NULL AND position IS NOT NULL
        GROUP BY 1,2
        """)

        # A league is "live" in a week if it produced any roster row that week.
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE live AS
        SELECT DISTINCT db_name, week FROM lake.public.player_fantasy WHERE year={year}
        """)

        # Denominators.  Weekly: eligible AND live that week.  Season: eligible AND live at all.
        con.execute("""
        CREATE OR REPLACE TEMP TABLE den_week AS
        SELECT lc.cohort, lc.position, l.week, COUNT(*) AS n_leagues
        FROM league_cohort lc JOIN live l USING (db_name) GROUP BY 1,2,3
        """)
        con.execute("""
        CREATE OR REPLACE TEMP TABLE den_season AS
        SELECT lc.cohort, lc.position, COUNT(DISTINCT lc.db_name) AS n_leagues
        FROM league_cohort lc WHERE lc.db_name IN (SELECT db_name FROM live)
        GROUP BY 1,2
        """)

        # Season length is a per-league fact.  Using a global distinct-week count lets a
        # handful of 22-week leagues deflate every other league's season rate.
        con.execute("""
        CREATE OR REPLACE TEMP TABLE league_weeks AS
        SELECT db_name, COUNT(DISTINCT week) AS wks FROM live GROUP BY 1
        """)
        cohort_list = [r[0] for r in con.execute(
            "SELECT DISTINCT cohort FROM league_cohort ORDER BY 1").fetchall()]

        # One cohort at a time.  Fanning every league out to all its cohort memberships at
        # once multiplies player_fantasy ~4x (~190M rows for a single season) and blows the
        # memory limit; filtering to one cohort keeps each pass streaming.
        for cohort in cohort_list:
            base = f"""
              SELECT lc.position, pf.NFL_player_id AS player, pf.db_name, pf.week,
                     CAST(pf.is_started AS DOUBLE) AS start_x,
                     CASE WHEN n.is_active=1 THEN CAST(pf.is_started AS DOUBLE) END AS healthy_x,
                     CASE WHEN CAST(pf.is_started AS DOUBLE)=1
                          THEN TRY_CAST(pf.win AS DOUBLE) END AS win_x
              FROM lake.public.player_fantasy pf
              JOIN nfl n ON n.NFL_player_id=pf.NFL_player_id AND n.week=pf.week
              JOIN league_cohort lc ON lc.db_name=pf.db_name AND lc.position=n.position
                                   AND lc.cohort='{cohort}'
              WHERE pf.year={year}
            """

            weekly = con.execute(f"""
            WITH b AS ({base}), agg AS (
              SELECT position, player, week,
                     COUNT(*) rs, COUNT(*) rss,
                     SUM(start_x) ss, SUM(start_x*start_x) sss,
                     SUM(healthy_x) hs, SUM(healthy_x*healthy_x) hss, COUNT(healthy_x) hn,
                     SUM(win_x) ws, SUM(win_x*win_x) wss, COUNT(win_x) wn
              FROM b GROUP BY 1,2,3
            )
            SELECT a.position, a.player, a.week, d.n_leagues,
                   a.rs, a.rss, a.ss, a.sss, a.hs, a.hss, a.hn, a.ws, a.wss, a.wn
            FROM agg a JOIN den_week d
              ON d.cohort='{cohort}' AND d.position=a.position AND d.week=a.week
            WHERE d.n_leagues >= 2
            """).fetchdf()
            weekly["grain"] = "weekly"

            # Season: one value per league first (absence of a week IS a zero, so the
            # denominator is the season's week count, not the weeks the player appeared).
            season = con.execute(f"""
            WITH b AS ({base}), per_league AS (
              SELECT b.position, b.player, b.db_name,
                     COUNT(*)::DOUBLE  / MAX(w.wks)                    AS roster_v,
                     SUM(b.start_x)    / MAX(w.wks)                    AS start_v,
                     SUM(b.healthy_x)  / NULLIF(COUNT(b.healthy_x),0)  AS healthy_v,
                     SUM(b.win_x)      / NULLIF(COUNT(b.win_x),0)      AS win_v
              FROM b JOIN league_weeks w ON w.db_name=b.db_name
              GROUP BY 1,2,3
            ), agg AS (
              SELECT position, player,
                     SUM(roster_v) rs, SUM(roster_v*roster_v) rss,
                     SUM(start_v)  ss, SUM(start_v*start_v)  sss,
                     SUM(healthy_v) hs, SUM(healthy_v*healthy_v) hss, COUNT(healthy_v) hn,
                     SUM(win_v) ws, SUM(win_v*win_v) wss, COUNT(win_v) wn
              FROM per_league GROUP BY 1,2
            )
            SELECT a.position, a.player, NULL::INTEGER AS week, d.n_leagues,
                   a.rs, a.rss, a.ss, a.sss, a.hs, a.hss, a.hn, a.ws, a.wss, a.wn
            FROM agg a JOIN den_season d
              ON d.cohort='{cohort}' AND d.position=a.position
            WHERE d.n_leagues >= 2
            """).fetchdf()
            season["grain"] = "season"

            df = pd.concat([weekly, season], ignore_index=True)
            df["year"] = year
            df["cohort"] = cohort
            frames.append(df)
            print(f"[{year}] {cohort}: {len(df):,} cells", flush=True)

    return pd.concat(frames, ignore_index=True)


def summarise(cells: pd.DataFrame) -> pd.DataFrame:
    """Turn raw moments into per-decile required-league counts."""
    z = NormalDist().inv_cdf((1.0 + CONFIDENCE) / 2.0)
    metrics = {
        "roster_pct":        ("rs", "rss", None),
        "start_pct":         ("ss", "sss", None),
        "healthy_start_pct": ("hs", "hss", "hn"),
        "win_pct":           ("ws", "wss", "wn"),
    }
    out = []
    for metric, (s_col, ss_col, n_col) in metrics.items():
        d = cells.copy()
        # roster/start denominate on the full eligible-live set (absence = 0).
        # healthy/win are only defined where observed, so they denominate on their own count.
        d["L"] = d["n_leagues"] if n_col is None else d[n_col]
        d = d[d["L"] >= 2]
        d["p"] = d[s_col] / d["L"]
        d["var"] = (d[ss_col] - d[s_col] ** 2 / d["L"]) / (d["L"] - 1)
        d = d[d["p"].between(0, 1) & d["var"].notna()]
        if d.empty:
            continue
        d["decile"] = (d["p"] * 10).apply(lambda v: min(10, max(1, math.ceil(v) if v > 0 else 1))) * 10
        for margin in MARGINS:
            d[f"req_{margin:g}"] = [
                required_leagues(v, L, margin, z) for v, L in zip(d["var"], d["L"])
            ]
        g = d.groupby(["grain", "position", "cohort", "decile"], as_index=False).agg(
            cells=("p", "size"),
            available_leagues=("L", "median"),
            **{f"req_{m:g}": (f"req_{m:g}", lambda s: s.quantile(0.90)) for m in MARGINS},
        )
        g["metric"] = metric
        out.append(g)
    return pd.concat(out, ignore_index=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--years", type=int, nargs="+", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--memory-mb", type=int, default=6000)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--tmp", type=Path, default=Path("C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp"))
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    cells = build(args)
    cells.to_parquet(args.out.with_suffix(".cells.parquet"), index=False)
    summary = summarise(cells)
    summary.to_parquet(args.out, index=False)
    print(f"\nwrote {len(summary):,} summary rows -> {args.out}")


if __name__ == "__main__":
    main()
