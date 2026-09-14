"""Build bounded cohort precision inputs from the public Actions lake.

The job owns one year and one position.  It deliberately emits planning rows,
not the league x player x week matrix: the normal/binomial weekly requirement
is analytic, and the season requirement is adjusted with an observed
league-season design effect.  This keeps the expensive work on Actions and
prevents the local process from materialising the calibration population.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import duckdb
import pandas as pd

from cohort_precision import decile_band, independent_observations, season_leagues


POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "ALL")
METRICS = ("roster_pct", "start_pct", "healthy_start_pct", "win_pct")
TARGETS = tuple(range(10, 101, 10))
MARGINS = (1, 3, 5)


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _tables(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name='lake' AND schema_name='public' "
            "UNION ALL SELECT view_name FROM duckdb_views() "
            "WHERE database_name='lake' AND schema_name='public'"
        ).fetchall()
    }


def _columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE database_name='lake' AND schema_name='public' AND table_name=?", [table]
        ).fetchall()
    }


def _cohort_expr(cols: set[str], candidates: tuple[str, ...], default: str = "ALL") -> str:
    for name in candidates:
        if name in cols:
            return f"COALESCE(CAST({_q(name)} AS VARCHAR), '{default}')"
    return f"'{default}'"


def _position_expr() -> str:
    return "CASE WHEN UPPER(TRIM(pp.position)) IN ('DST','D/ST','DEF') THEN 'DEF' ELSE UPPER(TRIM(pp.position)) END"


def _slot_expr(cols: set[str], position: str) -> str:
    def col(name: str) -> str:
        return f"COALESCE(CAST({_q(name)} AS DOUBLE), 0)" if name in cols else "0"
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
        if name in cols:
            return f"COALESCE(CAST({_q(name)} AS DOUBLE), 0)"
    return "0"


def _icc_frame(con: duckdb.DuckDBPyConnection, year: int, position: str, tables: set[str], ls_cols: set[str]) -> pd.DataFrame:
    """Estimate repeated-week ICCs from compact per-league/player aggregates."""
    if "player_fantasy" not in tables:
        return pd.DataFrame(columns=["cohort_key", "metric", "rho", "periods"])
    pf_cols = _columns(con, "player_fantasy")
    required = {"db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered"}
    if not required.issubset(pf_cols):
        return pd.DataFrame(columns=["cohort_key", "metric", "rho", "periods"])
    team_join = "TRUE"
    active_join = "TRUE"
    if "player_team_game_week" in tables:
        team_join = "tgw.NFL_player_id IS NOT NULL"
    if "player_active_week" in tables:
        active_join = "act.NFL_player_id IS NOT NULL"
    if "win" not in pf_cols:
        # Win is still emitted analytically, but cannot contribute an observed
        # cluster factor when the source does not expose a decision flag.
        win_expr = "NULL::DOUBLE"
        win_eligible_expr = "0"
    else:
        win_expr = "TRY_CAST(pf.win AS DOUBLE)"
        win_eligible_expr = f"CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND {team_join} AND pf.win IS NOT NULL THEN 1 ELSE 0 END"
    joins = ""
    if "player_team_game_week" in tables:
        joins += " LEFT JOIN lake.public.player_team_game_week tgw ON tgw.NFL_player_id=pf.NFL_player_id AND tgw.year=pf.year AND tgw.week=pf.week"
    if "player_active_week" in tables:
        joins += " LEFT JOIN lake.public.player_active_week act ON act.NFL_player_id=pf.NFL_player_id AND act.year=pf.year AND act.week=pf.week"
    teams = _slot_expr(ls_cols, position)
    roster = _cohort_expr(ls_cols, ("roster", "roster_structure"))
    ppr = _cohort_expr(ls_cols, ("ppr", "scoring_ppr"))
    td = _cohort_expr(ls_cols, ("td", "scoring_td"))
    bracket = _cohort_expr(ls_cols, ("bracket", "playoff_teams", "bracket_size"))
    cohort = f"concat_ws('|', {teams}, {roster}, {ppr}, {td}, {bracket})"
    pos_filter = "TRUE" if position == "ALL" else f"{_position_expr()}='{position}'"
    sql = f"""
    WITH base AS (
      SELECT pf.db_name, pf.week, CAST(pf.NFL_player_id AS VARCHAR) AS player,
             {cohort} AS cohort_key,
             CAST(pf.is_rostered AS DOUBLE) AS roster_x,
             CAST(pf.is_started AS DOUBLE) * CASE WHEN {team_join} THEN 1 ELSE 0 END AS start_x,
             CAST(pf.is_started AS DOUBLE) * CASE WHEN {active_join} THEN 1 ELSE 0 END AS healthy_x,
             {win_expr} AS win_x,
             CASE WHEN {team_join} THEN 1 ELSE 0 END AS team_eligible,
             CASE WHEN {active_join} THEN 1 ELSE 0 END AS active_eligible,
             {win_eligible_expr} AS win_eligible
      FROM lake.public.player_fantasy pf
      JOIN player_position_map pp ON CAST(pp.NFL_player_id AS VARCHAR)=CAST(pf.NFL_player_id AS VARCHAR) AND pp.year=pf.year
      JOIN lake.public.league_settings ls ON ls.db_name=pf.db_name AND ls.year=pf.year
      {joins}
      WHERE pf.year={year} AND {pos_filter}
    ), cells AS (
      SELECT cohort_key, player, db_name, 'roster_pct' metric, roster_x x, 1 eligible FROM base
      UNION ALL SELECT cohort_key, player, db_name, 'start_pct', start_x, team_eligible FROM base
      UNION ALL SELECT cohort_key, player, db_name, 'healthy_start_pct', healthy_x, active_eligible FROM base
      UNION ALL SELECT cohort_key, player, db_name, 'win_pct', COALESCE(win_x,0), win_eligible FROM base
    ), eligible AS (
      SELECT * FROM cells WHERE eligible=1
    ), lp AS (
      SELECT cohort_key, metric, player, db_name, SUM(x)::DOUBLE numerator,
             COUNT(*)::DOUBLE observations
      FROM eligible GROUP BY ALL
    )
    SELECT * FROM lp
    """
    try:
        lp = con.execute(sql).fetchdf()
        kdf = con.execute(f"""
          SELECT {cohort} AS cohort_key, COUNT(DISTINCT db_name)::DOUBLE AS k
          FROM lake.public.league_settings
          WHERE year={year} AND ({teams}) > 0
          GROUP BY 1
        """).fetchdf()
        if lp.empty:
            return pd.DataFrame(columns=["cohort_key", "metric", "rho", "periods"])
        lp = lp.merge(kdf, on="cohort_key", how="left")
        lp["base_periods"] = 16.0 if year <= 2020 else 17.0
        lp["value"] = lp["numerator"] / lp["base_periods"]
        win = lp.metric.eq("win_pct")
        lp.loc[win, "value"] = lp.loc[win, "numerator"] / lp.loc[win, "observations"].replace(0, pd.NA)
        lp.loc[win, "base_periods"] = lp.loc[win, "observations"]
        out = []
        for (cohort_key, metric), g in lp.groupby(["cohort_key", "metric"], dropna=False):
            for target_pct in TARGETS:
                lower, upper = decile_band(target_pct)
                decile_g = g[(g.value >= lower) & ((g.value < upper) | ((target_pct == 100) & (g.value <= upper)))]
                vals = decile_g.value.astype(float).tolist()
                periods = decile_g.base_periods.astype(float).tolist()
                s = pd.Series(vals, dtype="float64")
                between = float(s.var(ddof=1)) if len(s) > 1 else 0.0
                mean_periods = max(1.0, float(pd.Series(periods).mean())) if periods else 1.0
                within = float((s * (1.0 - s)).mean()) if len(s) else 0.0
                rho = 0.0 if between <= 0 else max(0.0, min(0.99, (between - within / mean_periods) / between))
                out.append({
                    "cohort_key": cohort_key,
                    "metric": metric,
                    "target_pct": target_pct,
                    "rho": rho,
                    "periods": mean_periods,
                    "decile_observations": len(decile_g),
                })
        return pd.DataFrame(out)
    except duckdb.Error as exc:
        print(f"ICC unavailable for {year}/{position}: {exc}", flush=True)
        return pd.DataFrame(columns=["cohort_key", "metric", "rho", "periods"])


def build(args: argparse.Namespace) -> None:
    if args.position not in POSITIONS:
        raise SystemExit(f"unsupported position: {args.position}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "temp_directory": str(args.out.parent / "duckdb_temp")})
    (args.out.parent / "duckdb_temp").mkdir(exist_ok=True)
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    if args.ops:
        con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    tables = _tables(con)
    ls_cols = _columns(con, "league_settings")
    if "player_position" in tables:
        con.execute("CREATE OR REPLACE TEMP VIEW player_position_map AS SELECT DISTINCT NFL_player_id, year, position FROM lake.public.player_position")
    else:
        con.execute("""CREATE OR REPLACE TEMP VIEW player_position_map AS
            SELECT DISTINCT NFL_player_id, CAST("year" AS INTEGER) AS "year", "position"
            FROM ops.nfl_historical.nfl_player_stats_all
            WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL""")
    teams = _slot_expr(ls_cols, args.position)
    roster = _cohort_expr(ls_cols, ("roster", "roster_structure"))
    ppr = _cohort_expr(ls_cols, ("ppr", "scoring_ppr"))
    td = _cohort_expr(ls_cols, ("td", "scoring_td"))
    bracket = _cohort_expr(ls_cols, ("bracket", "playoff_teams", "bracket_size"))
    cohort = f"concat_ws('|', {teams}, {roster}, {ppr}, {td}, {bracket})"
    available = con.execute(f"""
      SELECT {cohort} AS cohort_key, COUNT(DISTINCT db_name)::BIGINT AS available_leagues
      FROM lake.public.league_settings
      WHERE year={args.year} AND ({teams}) > 0
      GROUP BY 1
    """).fetchdf()
    icc = _icc_frame(con, args.year, args.position, tables, ls_cols)
    if icc.empty:
        icc = available.assign(metric=list(METRICS)[0], rho=0.0, periods=1.0).iloc[0:0]
    rows: list[dict] = []
    for avail in available.itertuples(index=False):
        cohort_key = avail.cohort_key
        for metric in METRICS:
            for target in TARGETS:
                for margin_pct in MARGINS:
                    match = icc[
                        (icc.cohort_key == cohort_key)
                        & (icc.metric == metric)
                        & (icc.target_pct == target)
                    ]
                    rho = float(match.rho.iloc[0]) if len(match) else 0.0
                    periods = float(match.periods.iloc[0]) if len(match) else (1.0 if metric == "win_pct" else (16.0 if args.year <= 2020 else 17.0))
                    weekly = independent_observations(target / 100.0, margin_pct / 100.0, args.confidence)
                    season = season_leagues(weekly, max(1, int(round(periods))), rho)
                    rows.append({
                        "year": args.year, "position": args.position, "cohort_key": cohort_key,
                        "metric": metric, "target_pct": target, "margin_pct": margin_pct,
                        "confidence": args.confidence, "weekly_required_leagues": weekly,
                        "season_required_leagues": season, "available_leagues": int(avail.available_leagues),
                        "rho": rho, "periods": periods,
                        "source": "cached_public_lake",
                        "rho_scope": "target_decile_band",
                        "decile_observations": int(match.decile_observations.iloc[0]) if len(match) else 0,
                    })
    out = pd.DataFrame(rows)
    out.to_parquet(args.out, index=False)
    print(f"wrote {len(out):,} rows to {args.out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=False)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--position", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--confidence", type=float, default=0.85)
    ap.add_argument("--memory-mb", type=int, default=3000)
    build(ap.parse_args())


if __name__ == "__main__":
    main()
