"""Estimate decile sample sizes with a league-clustered empirical bootstrap."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import NormalDist

import duckdb
import numpy as np
import pandas as pd

from build_cohort_precision_calibration import (
    MARGINS,
    METRICS,
    POSITIONS,
    TARGETS,
    _columns,
    _cohort_expr,
    _position_expr,
    _q,
    _slot_expr,
)
from cohort_precision import decile_band
from cohort_format_sql import cohort_league_settings_sql


_METRIC_PREFIXES = {
    "roster_pct": "roster",
    "start_pct": "start",
    "healthy_start_pct": "healthy",
    "win_pct": "win",
}


def metric_cell_columns(metric: str) -> dict[str, str]:
    """Return the wide SQL column names for one metric's aggregated cell."""
    try:
        prefix = _METRIC_PREFIXES[metric]
    except KeyError as exc:
        raise ValueError(f"unsupported metric: {metric}") from exc
    return {
        "numerator": f"{prefix}_num",
        "observations": f"{prefix}_obs",
        "sumsq": f"{prefix}_sumsq",
    }


def bootstrap_required_leagues(
    league_values: pd.Series,
    margin: float,
    confidence: float,
    reps: int = 400,
    seed: int = 20260731,
) -> tuple[int | None, float | None]:
    """Return required leagues and bootstrap SE for a league-level statistic."""
    values = pd.to_numeric(league_values, errors="coerce").dropna().to_numpy(dtype=float)
    n = len(values)
    if n < 2:
        return None, None
    rng = np.random.default_rng(seed)
    means = np.empty(reps, dtype=float)
    for i in range(reps):
        means[i] = values[rng.integers(0, n, size=n)].mean()
    se_at_n = float(means.std(ddof=1))
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    required = math.ceil((z * se_at_n * math.sqrt(n) / margin) ** 2)
    return max(2, required), se_at_n


def player_required_leagues(
    values: pd.DataFrame,
    value_col: str,
    margin: float,
    confidence: float,
) -> pd.DataFrame:
    """Estimate leagues needed per player from that player's cross-league variance."""
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    rows = []
    for player, group in values.groupby("player", dropna=False):
        league_values = group.groupby("db_name", as_index=False)[value_col].mean()
        if len(league_values) < 2:
            continue
        sd = float(league_values[value_col].std(ddof=1))
        rows.append({
            "player": player,
            "available_leagues": len(league_values),
            "required_leagues": max(2, math.ceil((z * sd / margin) ** 2)),
            "cross_league_sd": sd,
        })
    return pd.DataFrame(rows)


def _weekly_cells(con: duckdb.DuckDBPyConnection, years: list[int], position: str,
                  cohort_position: str | None = None) -> pd.DataFrame:
    if position == "ALL" and cohort_position is None:
        parts = [_weekly_cells(con, years, p, "ALL") for p in ("QB", "RB", "WR", "TE", "K", "DEF")]
        return pd.concat(parts, ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
    cohort_position = cohort_position or position
    if len(years) > 1:
        parts = [_weekly_cells(con, [year], position, cohort_position) for year in years]
        return pd.concat(parts, ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name='lake' AND schema_name='public' "
            "UNION ALL SELECT view_name FROM duckdb_views() WHERE database_name='lake' AND schema_name='public'"
        ).fetchall()
    }
    ls_cols = _columns(con, "league_settings")
    pf_cols = _columns(con, "player_fantasy")
    if "player_position" in tables:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW player_position_map AS "
            "SELECT DISTINCT NFL_player_id, year, position FROM lake.public.player_position"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW player_position_map AS "
            "SELECT DISTINCT NFL_player_id, CAST(year AS INTEGER) AS year, position "
            "FROM ops.nfl_historical.nfl_player_stats_all WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL"
        )
    team_join = "TRUE" if "player_team_game_week" not in tables else "tgw.NFL_player_id IS NOT NULL"
    active_join = "TRUE" if "player_active_week" not in tables else "act.NFL_player_id IS NOT NULL"
    joins = ""
    if "player_team_game_week" in tables:
        joins += " LEFT JOIN lake.public.player_team_game_week tgw ON tgw.NFL_player_id=pf.NFL_player_id AND tgw.year=pf.year AND tgw.week=pf.week"
    if "player_active_week" in tables:
        joins += " LEFT JOIN lake.public.player_active_week act ON act.NFL_player_id=pf.NFL_player_id AND act.year=pf.year AND act.week=pf.week"
    win_expr = "TRY_CAST(pf.win AS DOUBLE)" if "win" in pf_cols else "NULL::DOUBLE"
    win_eligible = (
        f"CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND {team_join} AND pf.win IS NOT NULL THEN 1 ELSE 0 END"
        if "win" in pf_cols else "0"
    )
    teams = _slot_expr(ls_cols, cohort_position)
    # The cohort contract's `teams_QB/RB/WR/TE` buckets are position-specific
    # starting-slot markets. K/DEF retain the league-level bucket because their
    # slot columns are too sparse to support a position-slot model.
    settings_sql = cohort_league_settings_sql(position_slots=True).replace(
        "public.", "lake.public."
    )
    team_bucket = (
        f"CASE '{cohort_position}' "
        "WHEN 'QB' THEN ls.teams_QB WHEN 'RB' THEN ls.teams_RB "
        "WHEN 'WR' THEN ls.teams_WR WHEN 'TE' THEN ls.teams_TE "
        "ELSE ls.teams END"
    )
    cohort_base = "concat_ws('|', %s, ls.roster, ls.ppr, ls.td)" % team_bucket
    pos_filter = "TRUE" if position == "ALL" else f"{_position_expr()}='{position}'"
    year_list = ",".join(str(int(y)) for y in years)
    sql = f"""
    WITH ls AS ({settings_sql}), base AS (
      SELECT pf.db_name, pf.week, CAST(pf.NFL_player_id AS VARCHAR) AS player,
             CAST(pf.year AS INTEGER) AS year,
             {cohort_base} AS cohort_base,
             ls.bracket AS bracket,
             CAST(pf.is_rostered AS DOUBLE) AS roster_x,
             CAST(pf.is_started AS DOUBLE) * CASE WHEN {team_join} THEN 1 ELSE 0 END AS start_x,
             CAST(pf.is_started AS DOUBLE) * CASE WHEN {active_join} THEN 1 ELSE 0 END AS healthy_x,
             {win_expr} AS win_x,
             CASE WHEN {team_join} THEN 1 ELSE 0 END AS team_eligible,
             CASE WHEN {active_join} THEN 1 ELSE 0 END AS active_eligible,
             {win_eligible} AS win_eligible
      FROM lake.public.player_fantasy pf
      JOIN player_position_map pp ON CAST(pp.NFL_player_id AS VARCHAR)=CAST(pf.NFL_player_id AS VARCHAR) AND pp.year=pf.year
      JOIN ls ON ls.db_name=pf.db_name AND ls.year=pf.year
      {joins}
      WHERE pf.year IN ({year_list}) AND {pos_filter}
        AND ls.lineup_mode <> 'best_ball'
    )
    SELECT cohort_base, bracket, db_name, player, year, week,
           SUM(roster_x)::DOUBLE AS roster_num,
           COUNT(*)::DOUBLE AS roster_obs,
           SUM(roster_x * roster_x)::DOUBLE AS roster_sumsq,
           SUM(CASE WHEN team_eligible=1 THEN start_x END)::DOUBLE AS start_num,
           SUM(team_eligible)::DOUBLE AS start_obs,
           SUM(CASE WHEN team_eligible=1 THEN start_x * start_x END)::DOUBLE AS start_sumsq,
           SUM(CASE WHEN active_eligible=1 THEN healthy_x END)::DOUBLE AS healthy_num,
           SUM(active_eligible)::DOUBLE AS healthy_obs,
           SUM(CASE WHEN active_eligible=1 THEN healthy_x * healthy_x END)::DOUBLE AS healthy_sumsq,
           SUM(CASE WHEN win_eligible=1 THEN COALESCE(win_x, 0) END)::DOUBLE AS win_num,
           SUM(win_eligible)::DOUBLE AS win_obs,
           SUM(CASE WHEN win_eligible=1 THEN COALESCE(win_x, 0) * COALESCE(win_x, 0) END)::DOUBLE AS win_sumsq
    FROM base
    GROUP BY cohort_base, bracket, db_name, player, year, week
    """
    return con.execute(sql).fetchdf()


def build(args: argparse.Namespace) -> None:
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "temp_directory": str(args.out.parent / "duckdb_temp")})
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    (args.out.parent / "duckdb_temp").mkdir(exist_ok=True)
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    if args.ops:
        con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    cells = _weekly_cells(con, args.years, args.position)
    if cells.empty:
        pd.DataFrame().to_parquet(args.out, index=False)
        return
    rows: list[dict] = []
    year_label = str(args.years[0]) if len(args.years) == 1 else f"{min(args.years)}-{max(args.years)}_pooled"
    for metric in METRICS:
        columns = metric_cell_columns(metric)
        metric_cells = cells[[
            "cohort_base", "bracket", "db_name", "player", "year", "week",
            columns["numerator"], columns["observations"], columns["sumsq"],
        ]].rename(columns={
            columns["numerator"]: "numerator",
            columns["observations"]: "observations",
            columns["sumsq"]: "sumsq",
        })
        metric_cells = metric_cells[metric_cells["observations"] > 0].copy()
        metric_cells["periods"] = metric_cells.year.map(lambda y: 16 if int(y) <= 2020 else 17)
        grouped = metric_cells.groupby(
            ["cohort_base", "bracket", "db_name", "player"], as_index=False
        ).agg(
            numerator=("numerator", "sum"), observations=("observations", "sum"),
            sumsq=("sumsq", "sum"), periods=("periods", "sum"),
        )
        grouped["season_value"] = grouped["numerator"] / grouped["periods"].replace(0, np.nan)
        grouped["weekly_value"] = grouped["numerator"] / grouped["observations"].replace(0, np.nan)
        grouped["within_week_sd"] = np.sqrt(
            (grouped["sumsq"] / grouped["observations"] - grouped["weekly_value"] ** 2).clip(lower=0)
            * grouped["observations"] / (grouped["observations"] - 1).replace(0, np.nan)
        )
        if metric == "win_pct":
            grouped["season_value"] = grouped["numerator"] / grouped["observations"].replace(0, np.nan)
            grouped["weekly_value"] = grouped["season_value"]
        grouped["cohort_key"] = grouped["cohort_base"]
        for cohort in sorted(grouped.cohort_key.dropna().unique()):
            season_group = grouped[grouped.cohort_key == cohort]
            week_group = season_group
            player_global = season_group.groupby("player", as_index=False).season_value.mean().rename(columns={"season_value": "global_value"})
            season_group = season_group.merge(player_global, on="player", how="left")
            for target in TARGETS:
                lower, upper = decile_band(target)
                selected_players = player_global[(player_global.global_value >= lower) & ((player_global.global_value < upper) | ((target == 100) & (player_global.global_value <= upper)))][["player"]]
                season_band = season_group.merge(selected_players, on="player", how="inner")
                week_band = week_group.merge(selected_players, on="player", how="inner")
                weekly_player_league = week_band[["player", "db_name", "weekly_value"]].rename(columns={"weekly_value": "x"})
                season_player_league = season_band[["player", "db_name", "season_value"]].drop_duplicates()
                weekly_requirements: dict[int, pd.DataFrame] = {}
                season_requirements: dict[int, pd.DataFrame] = {}
                for margin_pct in MARGINS:
                    weekly_requirements[margin_pct] = player_required_leagues(
                        weekly_player_league, "x", margin_pct / 100, args.confidence
                    )
                    season_requirements[margin_pct] = player_required_leagues(
                        season_player_league, "season_value", margin_pct / 100, args.confidence
                    )
                    weekly_req = weekly_requirements[margin_pct]
                    season_req = season_requirements[margin_pct]
                    within_week = week_band[["player", "db_name", "within_week_sd"]].drop_duplicates()
                    weekly_available = week_band.db_name.nunique()
                    season_available = season_band.db_name.nunique()
                    rows.append({
                        "year": year_label, "position": args.position, "cohort_key": cohort,
                        "metric": metric, "target_pct": target, "margin_pct": margin_pct,
                        "confidence": args.confidence,
                        "weekly_required_leagues": int(weekly_req.required_leagues.quantile(0.90)) if not weekly_req.empty else None,
                        "season_required_leagues": int(season_req.required_leagues.quantile(0.90)) if not season_req.empty else None,
                        "weekly_required_leagues_p50": int(weekly_req.required_leagues.quantile(0.50)) if not weekly_req.empty else None,
                        "season_required_leagues_p50": int(season_req.required_leagues.quantile(0.50)) if not season_req.empty else None,
                        "weekly_available_leagues": int(weekly_available),
                        "season_available_leagues": int(season_available),
                        "players_with_2plus_leagues": int(len(season_req)),
                        "weekly_cross_league_sd_p90": float(weekly_req.cross_league_sd.quantile(0.90)) if not weekly_req.empty else None,
                        "season_cross_league_sd_p90": float(season_req.cross_league_sd.quantile(0.90)) if not season_req.empty else None,
                        "within_league_week_sd_p90": float(within_week.within_week_sd.quantile(0.90)) if not within_week.empty else None,
                        "decile_player_leagues": int(len(season_band)),
                        "source": "cached_public_lake",
                    })
    pd.DataFrame(rows).to_parquet(args.out, index=False)
    print(f"wrote {len(rows):,} rows to {args.out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path)
    ap.add_argument("--year", type=int)
    ap.add_argument("--years")
    ap.add_argument("--position", choices=POSITIONS, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--confidence", type=float, default=0.85)
    ap.add_argument("--reps", type=int, default=400)
    ap.add_argument("--seed", type=int, default=20260731)
    ap.add_argument("--memory-mb", type=int, default=3000)
    args = ap.parse_args()
    if args.years:
        args.years = [int(x) for x in json.loads(args.years)]
    elif args.year is not None:
        args.years = [args.year]
    else:
        raise SystemExit("one of --year or --years is required")
    build(args)


if __name__ == "__main__":
    main()
