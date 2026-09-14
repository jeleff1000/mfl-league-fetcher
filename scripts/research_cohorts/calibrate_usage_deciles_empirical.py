"""Empirical minimum league-season sizes for weekly and season usage rates.

The sampling unit is a league-season.  Player rows are sparse: a missing row is a
zero for roster/start/win, while the denominator comes from eligible league rows.
NFL team-game and active-week schedules are derived from the cached ops reference,
so byes never enter Start % or Healthy Start % denominators.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")
TARGETS = tuple(i / 100 for i in range(10, 101, 10))
GRID = (25, 50, 100, 200, 400, 800, 1200, 1600, 2400, 3200, 4800, 6400)
METRICS = ("roster_rate", "start_rate", "healthy_start_rate", "win_rate")


def eligible_expr(pos: str) -> str:
    if pos == "QB":
        return "COALESCE(roster_QB,0)>0 OR COALESCE(roster_SUPER_FLEX,0)>0"
    if pos in {"RB", "WR", "TE"}:
        return f"COALESCE(roster_{pos},0)>0 OR COALESCE(roster_FLX,0)>0"
    return f"COALESCE(roster_{pos},0)>0"


def corr(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 4 or np.unique(a).size < 2 or np.unique(b).size < 2:
        return float("nan")
    return float(np.corrcoef(a, b)[0, 1])


def schedule(con: duckdb.DuckDBPyConnection, years: list[int]) -> pd.DataFrame:
    lo, hi = min(years), max(years)
    raw = con.execute(f"""
      SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS player_id,
             CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
             CAST(nfl_team AS VARCHAR) AS team
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE year BETWEEN {lo} AND {hi} AND season_type='REG'
        AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL
    """).fetch_df()
    if raw.empty:
        return pd.DataFrame(columns=["player_id", "year", "week", "team_game", "active"])
    teams = set(map(tuple, raw[["year", "week", "team"]].drop_duplicates().itertuples(index=False, name=None)))
    rows: list[tuple[str, int, int, int, int]] = []
    for (pid, year), g in raw.groupby(["player_id", "year"], sort=False):
        g = g.sort_values("week")
        first, last = int(g.week.min()), int(g.week.max())
        by_week = {int(r.week): r.team for r in g.itertuples()}
        current = None
        for week in range(first, last + 1):
            if week in by_week:
                current = by_week[week]
            if current is not None:
                rows.append((pid, int(year), week,
                             int((int(year), week, current) in teams),
                             int(week in by_week)))
    return pd.DataFrame(rows, columns=["player_id", "year", "week", "team_game", "active"])


def sample_requirements(
    cell_rows: dict[str, pd.DataFrame],
    population: pd.DataFrame,
    eligible_ids: list[str],
    metric: str,
    grain: str,
    rng: random.Random,
) -> dict[float, dict[str, int | None]]:
    out: dict[float, dict[str, int | None]] = {}
    for target in TARGETS:
        cand = population[(population[metric] - target).abs() <= 0.05].copy()
        if cand.empty:
            continue
        if cell_rows:
            cand = cand[cand.key.isin(cell_rows)]
            if cand.empty:
                continue
        out[target] = {"precision_n": None, "r75_n": None, "r85_n": None, "r95_n": None}
        # Keep the simulation bounded while retaining the whole rate band in the
        # population estimate.  Candidate cells are the value units being compared.
        if len(cand) > 80:
            cand = cand.sample(80, random_state=17)
        keys = cand.key.tolist()
        target_values = cand[metric].to_numpy(dtype=np.float64)
        meta = cand.set_index("key").to_dict("index")
        # Convert each candidate cell to dense league-aligned arrays once.  The
        # earlier implementation repeatedly filtered pandas frames inside every
        # simulation draw, which made a four-year run take hours and could leave
        # no artifact even though the process was healthy.
        league_index = {str(db): i for i, db in enumerate(eligible_ids)}
        roster_arrays: dict[str, np.ndarray] = {}
        start_arrays: dict[str, np.ndarray] = {}
        win_arrays: dict[str, np.ndarray] = {}
        decided_arrays: dict[str, np.ndarray] = {}
        for key in keys:
            d = cell_rows.get(key, pd.DataFrame())
            roster = np.zeros(len(eligible_ids), dtype=np.float64)
            started = np.zeros(len(eligible_ids), dtype=np.float64)
            wins = np.zeros(len(eligible_ids), dtype=np.float64)
            decided = np.zeros(len(eligible_ids), dtype=np.float64)
            if not d.empty:
                for row in d.itertuples(index=False):
                    idx = league_index.get(str(row.db_name))
                    if idx is not None:
                        roster[idx] = float(row.rostered)
                        started[idx] = float(row.started)
                        wins[idx] = float(row.win)
                        decided[idx] = float(row.decided)
            roster_arrays[key] = roster
            start_arrays[key] = started
            win_arrays[key] = wins
            decided_arrays[key] = decided
        # Stack candidates into matrices so each simulation draw is one NumPy
        # reduction rather than a Python loop over candidate cells.
        roster_matrix = np.stack([roster_arrays[k] for k in keys])
        start_matrix = np.stack([start_arrays[k] for k in keys])
        win_matrix = np.stack([win_arrays[k] for k in keys])
        decided_matrix = np.stack([decided_arrays[k] for k in keys])
        if metric != "win_rate" and grain == "season":
            period_field = {"roster_rate": "roster_periods",
                            "start_rate": "start_periods",
                            "healthy_start_rate": "healthy_periods"}[metric]
            periods = np.array([max(int(meta[k][period_field]), 1) for k in keys], dtype=np.float64)
        else:
            periods = None
        precision: dict[int, list[float]] = {n: [] for n in GRID}
        reliability: dict[int, list[float]] = {n: [] for n in GRID}
        for _ in range(20):
            for n in GRID:
                if n > len(eligible_ids):
                    continue
                picked_indices = rng.sample(range(len(eligible_ids)), n)
                half = n // 2
                left_indices = picked_indices[:half]
                right_indices = picked_indices[half:]

                def values(indices: list[int]) -> np.ndarray:
                    n_selected = len(indices)
                    if metric == "win_rate":
                        numerator = win_matrix[:, indices].sum(axis=1)
                        denominator = decided_matrix[:, indices].sum(axis=1)
                        return np.divide(numerator, denominator,
                                          out=np.full(len(keys), np.nan),
                                          where=denominator > 0)
                    source = roster_matrix if metric == "roster_rate" else start_matrix
                    denominator = n_selected if periods is None else n_selected * periods
                    return source[:, indices].sum(axis=1) / denominator

                full = values(picked_indices)
                finite = np.isfinite(full) & np.isfinite(target_values)
                errors = np.abs(full[finite] - target_values[finite])
                if errors.size:
                    precision[n].append(float(np.quantile(errors, 0.85)))
                a, b = values(left_indices), values(right_indices)
                common = np.isfinite(a) & np.isfinite(b)
                if common.sum() >= 4:
                    r = corr(a[common], b[common])
                    if np.isfinite(r):
                        reliability[n].append(r)
        if out[target]["precision_n"] is None:
            out[target]["precision_n"] = next((n for n in GRID if precision[n] and np.quantile(precision[n], .85) <= .05), None)
        for threshold, name in ((.75, "r75_n"), (.85, "r85_n"), (.95, "r95_n")):
            if out[target][name] is None:
                out[target][name] = next((n for n in GRID if reliability[n] and np.quantile(reliability[n], .15) >= threshold), None)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--observed-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--start-year", type=int, default=2020)
    ap.add_argument("--end-year", type=int, default=2025)
    args = ap.parse_args()
    years = list(range(args.start_year, args.end_year + 1))
    con = duckdb.connect(config={"memory_limit": "3GB", "temp_directory": str(args.out.parent / "duckdb_temp")})
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    sched = schedule(con, years)
    rng = random.Random(20260730)
    results = []
    for year in years:
        path = args.observed_dir / f"usage_observed_{year}.parquet"
        if not path.exists() or path.stat().st_size == 0:
            print(f"skip missing {year}", flush=True)
            continue
        # A missing active stat row is still a team-game opportunity; active rows are
        # the healthy denominator.  Roster-only bye rows have nfl_team NULL.
        year_sched = sched[sched.year == year]
        for pos in POSITIONS:
            ids = con.execute(f"""
              SELECT CAST(db_name AS VARCHAR) FROM lake.public.league_settings
              WHERE year={year} AND LOWER(COALESCE(platform,'')) IN ('sleeper','mfl','fleaflicker')
                AND ({eligible_expr(pos)})
            """).fetchall()
            eligible_ids = [r[0] for r in ids]
            if not eligible_ids:
                continue
            # Build weekly and season cells with grouped joins.  The previous
            # implementation repeatedly filtered schedule and observation frames
            # inside nested player/week loops; on a full year that turned a
            # checkpoint into a multi-hour operation.
            parquet = path.as_posix().replace("'", "''")
            weekly_obs = con.execute(f"""
                SELECT CAST(NFL_player_id AS VARCHAR) AS player_id,
                       CAST(week AS INTEGER) AS week,
                       SUM(rostered) AS rostered, SUM(started) AS started,
                       SUM(win) AS win, SUM(decided) AS decided
                FROM read_parquet('{parquet}')
                WHERE position = ?
                GROUP BY 1,2
            """, [pos]).fetch_df()
            if weekly_obs.empty:
                continue
            psched = year_sched[year_sched.player_id.isin(weekly_obs.player_id.unique())][
                ["player_id", "week", "team_game", "active"]
            ].copy()
            weekly_cells = weekly_obs.merge(psched, on=["player_id", "week"], how="left")
            weekly_cells[["team_game", "active"]] = weekly_cells[["team_game", "active"]].fillna(0)
            weekly_cells["team_game"] = weekly_cells["team_game"].astype(int)
            weekly_cells["active"] = weekly_cells["active"].astype(int)
            season_obs = con.execute(f"""
                SELECT CAST(NFL_player_id AS VARCHAR) AS player_id,
                       SUM(rostered) AS rostered, SUM(started) AS started,
                       SUM(win) AS win, SUM(decided) AS decided
                FROM read_parquet('{parquet}')
                WHERE position = ?
                GROUP BY 1
            """, [pos]).fetch_df()
            season_meta = (
                psched.groupby("player_id", as_index=False, sort=False)
                .agg(team_periods=("team_game", "sum"),
                     healthy_periods=("active", "sum"),
                     roster_periods=("week", "nunique"))
            )
            season_cells = season_obs.merge(season_meta, on="player_id", how="left")
            season_cells[["team_periods", "healthy_periods", "roster_periods"]] = (
                season_cells[["team_periods", "healthy_periods", "roster_periods"]].fillna(0)
            )
            for grain in ("weekly", "season"):
                if grain == "weekly":
                    cells = weekly_cells.copy()
                    cells["key"] = cells.apply(lambda r: f"{r.player_id}|{year}|{int(r.week)}", axis=1)
                    cells["roster_periods"] = 1
                    cells["start_periods"] = cells["team_game"].astype(int)
                    cells["healthy_periods"] = cells["active"].astype(int)
                    cells["roster_rate"] = cells.rostered / len(eligible_ids)
                    cells["start_rate"] = np.where(
                        cells.team_game.eq(1), cells.started / len(eligible_ids), np.nan
                    )
                    cells["healthy_start_rate"] = np.where(
                        cells.active.eq(1), cells.started / len(eligible_ids), np.nan
                    )
                    cells["win_rate"] = np.where(
                        cells.decided.gt(0), cells.win / cells.decided, np.nan
                    )
                else:
                    cells = season_cells.copy()
                    cells["key"] = cells.player_id.map(lambda pid: f"{pid}|{year}|0")
                    cells["start_periods"] = cells["team_periods"].astype(int)
                    cells["roster_rate"] = cells.rostered / (
                        len(eligible_ids) * cells.roster_periods.clip(lower=1)
                    )
                    cells["start_rate"] = np.where(
                        cells.team_periods.gt(0),
                        cells.started / (len(eligible_ids) * cells.team_periods),
                        np.nan,
                    )
                    cells["healthy_start_rate"] = np.where(
                        cells.healthy_periods.gt(0),
                        cells.started / (len(eligible_ids) * cells.healthy_periods),
                        np.nan,
                    )
                    cells["win_rate"] = np.where(
                        cells.decided.gt(0), cells.win / cells.decided, np.nan
                    )
                pop = pd.DataFrame(cells)
                if pop.empty:
                    continue
                # Do not group the entire sparse lake before candidate selection.  The
                # 2024 population has 22M+ player-week rows; pandas' groupby indexer for
                # all of them alone needs ~700 MiB.  Sampling only asks about cells within
                # one metric's target bands, so aggregate one metric's candidates at a
                # time and release that map before moving to the next metric.
                for metric in METRICS:
                    cell_rows: dict[str, pd.DataFrame] = {}
                    selected_keys: set[str] = set()
                    for target in TARGETS:
                        target_cells = pop[(pop[metric] - target).abs() <= .05]
                        if len(target_cells) > 80:
                            target_cells = target_cells.sample(80, random_state=17)
                        selected_keys.update(target_cells.key.tolist())
                    candidate_mask = pop.key.isin(selected_keys)
                    if grain == "weekly":
                        candidate_pairs = pop.loc[candidate_mask, ["player_id", "week"]].drop_duplicates()
                        con.register("_candidate_pairs", candidate_pairs)
                        candidate_obs = con.execute(f"""
                            SELECT CAST(o.NFL_player_id AS VARCHAR) AS player_id,
                                   CAST(o.week AS INTEGER) AS week, o.db_name,
                                   SUM(o.rostered) AS rostered, SUM(o.started) AS started,
                                   SUM(o.win) AS win, SUM(o.decided) AS decided
                            FROM read_parquet('{parquet}') o
                            JOIN _candidate_pairs c
                              ON CAST(o.NFL_player_id AS VARCHAR)=c.player_id
                             AND CAST(o.week AS INTEGER)=c.week
                            WHERE o.position = ?
                            GROUP BY 1,2,3
                        """, [pos]).fetch_df()
                        con.unregister("_candidate_pairs")
                        for (pid, week), gg in candidate_obs.groupby(["player_id", "week"], sort=False):
                            key = f"{pid}|{year}|{int(week)}"
                            cell_rows[key] = gg[["db_name", "rostered", "started", "win", "decided"]].groupby(
                                "db_name", as_index=False, sort=False
                            ).sum(numeric_only=True)
                    else:
                        candidate_players = pop.loc[candidate_mask, ["player_id"]].drop_duplicates()
                        con.register("_candidate_players", candidate_players)
                        candidate_obs = con.execute(f"""
                            SELECT CAST(o.NFL_player_id AS VARCHAR) AS player_id, o.db_name,
                                   SUM(o.rostered) AS rostered, SUM(o.started) AS started,
                                   SUM(o.win) AS win, SUM(o.decided) AS decided
                            FROM read_parquet('{parquet}') o
                            JOIN _candidate_players c
                              ON CAST(o.NFL_player_id AS VARCHAR)=c.player_id
                            WHERE o.position = ?
                            GROUP BY 1,2
                        """, [pos]).fetch_df()
                        con.unregister("_candidate_players")
                        for pid, gg in candidate_obs.groupby("player_id", sort=False):
                            key = f"{pid}|{year}|0"
                            cell_rows[key] = gg[["db_name", "rostered", "started", "win", "decided"]].groupby(
                                "db_name", as_index=False, sort=False
                            ).sum(numeric_only=True)
                    reqs = sample_requirements(cell_rows, pop.dropna(subset=[metric]), eligible_ids, metric, grain, rng)
                    for target in TARGETS:
                        band = pop[(pop[metric] - target).abs() <= .05]
                        req = reqs.get(target, {"precision_n": None, "r75_n": None, "r85_n": None, "r95_n": None})
                        results.append({"year": year, "position": pos, "grain": grain,
                                        "metric": metric, "target_rate": target,
                                        "eligible_league_seasons": len(eligible_ids),
                                        "candidate_cells": int(len(band)), **req})
        print(f"calibrated {year}", flush=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).to_csv(args.out, index=False)
    args.out.with_suffix(".json").write_text(json.dumps(results, indent=2))
    con.close()


if __name__ == "__main__":
    main()
