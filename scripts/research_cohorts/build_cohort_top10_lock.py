"""Estimate sample sizes that recover the population top-10 order.

This is intentionally a bounded, year/position Actions job.  It keeps only a
cohort's league-by-player aggregate matrices in memory and samples leagues
from those matrices; it never builds the full cross-position research table.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


METRICS = ("roster_pct", "start_pct", "healthy_start_pct", "win_pct")
LOCK_LEVELS = (0.75, 0.85, 0.95)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def cols(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[0] for r in con.execute(
        "SELECT column_name FROM duckdb_columns() WHERE database_name='lake' AND schema_name='public' AND table_name=?",
        [table]).fetchall()}


def expr(names: set[str], candidates: tuple[str, ...], default: str = "ALL") -> str:
    for name in candidates:
        if name in names:
            return f"COALESCE(CAST({q(name)} AS VARCHAR), '{default}')"
    return f"'{default}'"


def slot_expr(names: set[str], position: str) -> str:
    def col(name: str) -> str:
        return f"COALESCE(CAST({q(name)} AS DOUBLE), 0)" if name in names else "0"
    if position == "QB":
        return f"({col('roster_QB')} + {col('roster_SUPER_FLEX')})"
    if position == "RB":
        return f"({col('roster_RB')} + {col('roster_FLX')})"
    if position == "WR":
        return f"({col('roster_WR')} + {col('roster_FLX')})"
    if position == "TE":
        return f"({col('roster_TE')} + {col('roster_FLX')})"
    if position == "K":
        return col("roster_K")
    if position == "DEF":
        return col("roster_DEF")
    for name in ("num_teams", "teams", "team_count"):
        if name in names:
            return f"COALESCE(CAST({q(name)} AS DOUBLE), 0)"
    return "0"


def sample_sizes(n: int) -> list[int]:
    if n < 2:
        return []
    vals = set(x for x in (5, 10, 15, 20, 30, 40, 50, 75, 100) if x <= n)
    vals.update(x for x in (125, 150, 200, 300, 500, 750, 1000) if x <= n)
    vals.add(n)
    return sorted(vals)


def lock_probability(matrix: np.ndarray, population_order: np.ndarray, n: int, rng: np.random.Generator) -> float:
    if n < 2 or matrix.shape[0] < n:
        return 0.0
    reps = min(60, max(25, 1000 // max(1, n // 10)))
    hits = 0
    for _ in range(reps):
        take = rng.choice(matrix.shape[0], size=n, replace=False)
        values = np.nanmean(matrix[take], axis=0)
        # Missing values cannot outrank a measured player; keep population
        # player order deterministic for ties.
        values = np.nan_to_num(values, nan=-np.inf)
        order = np.lexsort((np.arange(values.size), -values))[:10]
        if np.array_equal(order, population_order):
            hits += 1
    return hits / reps


def build(args: argparse.Namespace) -> None:
    args.out.parent.mkdir(parents=True, exist_ok=True)
    (args.out.parent / "duckdb_temp").mkdir(exist_ok=True)
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "temp_directory": str(args.out.parent / "duckdb_temp")})
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    ls_cols = cols(con, "league_settings")
    pf_cols = cols(con, "player_fantasy")
    public_tables = {r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='lake' AND schema_name='public' "
        "UNION ALL SELECT view_name FROM duckdb_views() WHERE database_name='lake' AND schema_name='public'"
    ).fetchall()}
    if args.ops:
        con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    if "player_position" in public_tables:
        con.execute("CREATE OR REPLACE TEMP VIEW player_position_map AS SELECT DISTINCT NFL_player_id, year, position FROM lake.public.player_position")
    else:
        con.execute("""CREATE OR REPLACE TEMP VIEW player_position_map AS
            SELECT DISTINCT NFL_player_id, CAST("year" AS INTEGER) AS "year", "position"
            FROM ops.nfl_historical.nfl_player_stats_all
            WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL""")
    pos_expr = "CASE WHEN UPPER(TRIM(pp.position)) IN ('DST','D/ST','DEF') THEN 'DEF' ELSE UPPER(TRIM(pp.position)) END"
    pos_filter = "TRUE" if args.position == "ALL" else f"{pos_expr}='{args.position}'"
    teams = slot_expr(ls_cols, args.position)
    roster = expr(ls_cols, ("roster", "roster_structure"))
    ppr = expr(ls_cols, ("ppr", "scoring_ppr"))
    td = expr(ls_cols, ("td", "scoring_td"))
    bracket = expr(ls_cols, ("bracket", "playoff_teams", "bracket_size"))
    cohort = f"concat_ws('|', {teams}, {roster}, {ppr}, {td}, {bracket})"
    tables = public_tables
    tgw = "TRUE" if "player_team_game_week" not in tables else "tgw.NFL_player_id IS NOT NULL"
    act = "TRUE" if "player_active_week" not in tables else "act.NFL_player_id IS NOT NULL"
    joins = ""
    if "player_team_game_week" in tables:
        joins += " LEFT JOIN lake.public.player_team_game_week tgw ON tgw.NFL_player_id=pf.NFL_player_id AND tgw.year=pf.year AND tgw.week=pf.week"
    if "player_active_week" in tables:
        joins += " LEFT JOIN lake.public.player_active_week act ON act.NFL_player_id=pf.NFL_player_id AND act.year=pf.year AND act.week=pf.week"
    has_win = "win" in pf_cols
    win = "TRY_CAST(pf.win AS DOUBLE)" if has_win else "NULL::DOUBLE"
    win_present = "pf.win IS NOT NULL" if has_win else "FALSE"
    base = f"""
      SELECT pf.db_name, pf.week, CAST(pf.NFL_player_id AS VARCHAR) player,
             {cohort} cohort_key,
             CAST(pf.is_rostered AS DOUBLE) roster_x,
             CASE WHEN {tgw} THEN CAST(pf.is_started AS DOUBLE) ELSE 0 END start_num,
             CASE WHEN {tgw} THEN 1 ELSE 0 END start_den,
             CASE WHEN {act} THEN CAST(pf.is_started AS DOUBLE) ELSE 0 END healthy_num,
             CASE WHEN {act} THEN 1 ELSE 0 END healthy_den,
             CASE WHEN {tgw} AND CAST(pf.is_started AS DOUBLE)=1 AND {win_present} THEN {win} ELSE 0 END win_num,
             CASE WHEN {tgw} AND CAST(pf.is_started AS DOUBLE)=1 AND {win_present} THEN 1 ELSE 0 END win_den,
             1 AS roster_den
      FROM lake.public.player_fantasy pf
      JOIN player_position_map pp ON CAST(pp.NFL_player_id AS VARCHAR)=CAST(pf.NFL_player_id AS VARCHAR) AND pp.year=pf.year
      JOIN lake.public.league_settings ls ON ls.db_name=pf.db_name AND ls.year=pf.year
      {joins}
      WHERE pf.year={args.year} AND {pos_filter} AND ({teams}) > 0
    """
    # Weekly cells and season cells use the same numerator/denominator fields.
    weekly = con.execute(f"""
      WITH b AS ({base})
      SELECT db_name, week, player, cohort_key,
        SUM(roster_x)/NULLIF(SUM(roster_den),0) roster_pct,
        SUM(start_num)/NULLIF(SUM(start_den),0) start_pct,
        SUM(healthy_num)/NULLIF(SUM(healthy_den),0) healthy_start_pct,
        SUM(win_num)/NULLIF(SUM(win_den),0) win_pct
      FROM b GROUP BY ALL
    """).fetchdf()
    season = con.execute(f"""
      WITH b AS ({base})
      SELECT db_name, player, cohort_key,
        SUM(roster_x)/NULLIF(SUM(roster_den),0) roster_pct,
        SUM(start_num)/NULLIF(SUM(start_den),0) start_pct,
        SUM(healthy_num)/NULLIF(SUM(healthy_den),0) healthy_start_pct,
        SUM(win_num)/NULLIF(SUM(win_den),0) win_pct
      FROM b GROUP BY ALL
    """).fetchdf()
    rng = np.random.default_rng(20260731 + args.year + sum(map(ord, args.position)))
    rows = []
    for grain, frame, group_cols in (("weekly", weekly, ["cohort_key", "week"]), ("season", season, ["cohort_key"])):
        for group_key, group in frame.groupby(group_cols, dropna=False):
            if not isinstance(group_key, tuple):
                group_key = (group_key,)
            cohort_key = group_key[0]
            leagues = sorted(group.db_name.unique())
            if len(leagues) < 2:
                continue
            league_index = {name: i for i, name in enumerate(leagues)}
            players = sorted(group.player.unique())
            player_index = {name: i for i, name in enumerate(players)}
            for metric in METRICS:
                mat = np.full((len(leagues), len(players)), np.nan)
                for row in group[["db_name", "player", metric]].itertuples(index=False):
                    mat[league_index[row.db_name], player_index[row.player]] = row[2]
                pop = np.nanmean(mat, axis=0)
                pop = np.nan_to_num(pop, nan=-np.inf)
                pop_order = np.lexsort((np.arange(len(players)), -pop))[:10]
                probs = [(n, lock_probability(mat, pop_order, n, rng)) for n in sample_sizes(len(leagues))]
                thresholds = {}
                for level in LOCK_LEVELS:
                    thresholds[level] = next((n for n, prob in probs if prob >= level), None)
                rows.append({
                    "year": args.year, "position": args.position, "grain": grain,
                    "cohort_key": cohort_key, "week": group_key[1] if grain == "weekly" else None,
                    "metric": metric, "available_leagues": len(leagues),
                    "lock_75_leagues": thresholds[0.75], "lock_85_leagues": thresholds[0.85],
                    "lock_95_leagues": thresholds[0.95], "population_top10": ",".join(players[i] for i in pop_order),
                })
    pd.DataFrame(rows).to_parquet(args.out, index=False)
    print(f"wrote {len(rows):,} top10 lock rows to {args.out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=False)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--position", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--memory-mb", type=int, default=3000)
    build(ap.parse_args())


if __name__ == "__main__":
    main()
