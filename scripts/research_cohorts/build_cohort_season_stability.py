"""Season-only decile and top/bottom-board stability for player outcomes.

The unit is a player-season within a league.  Weeks are used only to construct
the season statistic; they are never treated as independent observations.
Outputs are intentionally diagnostic: they include the observed overlap and
support counts needed to decide whether a cell can stand alone or must pool.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from build_cohort_top10_lock import (
    cols,
    expr,
    sample_sizes,
    slot_expr,
)
from build_cohort_decile_bootstrap import player_required_leagues
from cohort_format_sql import cohort_league_settings_sql

METRICS = (
    "healthy_start_pct", "expected_wins", "expected_losses", "expected_starts",
    "expected_wl", "clutch", "champ", "playoffs",
)
TARGETS = tuple(range(10, 101, 10))
CLUTCH_TARGETS = tuple(range(-50, 51, 10))
MARGINS = (1, 3, 5)
LOCK_LEVELS = (0.75, 0.85, 0.95)


def _position_map(con: duckdb.DuckDBPyConnection, tables: set[str]) -> None:
    if "player_position" in tables:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW player_position_map AS "
            "SELECT DISTINCT NFL_player_id, year, position FROM lake.public.player_position"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW player_position_map AS "
            "SELECT DISTINCT NFL_player_id, CAST(\"year\" AS INTEGER) AS year, position "
            "FROM ops.nfl_historical.nfl_player_stats_all "
            "WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL"
        )


def _season_frame(con: duckdb.DuckDBPyConnection, years: list[int], position: str,
                  cohort_position: str | None = None) -> pd.DataFrame:
    if position == "ALL" and cohort_position is None:
        parts = [_season_frame(con, years, p, "ALL") for p in ("QB", "RB", "WR", "TE", "K", "DEF")]
        return pd.concat(parts, ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
    cohort_position = cohort_position or position
    if len(years) > 1:
        parts = [_season_frame(con, [year], position, cohort_position) for year in years]
        return pd.concat(parts, ignore_index=True) if any(not p.empty for p in parts) else pd.DataFrame()
    tables = {
        r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name='lake' AND schema_name='public' "
            "UNION ALL SELECT view_name FROM duckdb_views() WHERE database_name='lake' AND schema_name='public'"
        ).fetchall()
    }
    ls_cols = cols(con, "league_settings")
    pf_cols = cols(con, "player_fantasy")
    _position_map(con, tables)

    pos_expr = "CASE WHEN UPPER(TRIM(pp.position)) IN ('DST','D/ST','DEF') THEN 'DEF' ELSE UPPER(TRIM(pp.position)) END"
    pos_filter = "TRUE" if position == "ALL" else f"{pos_expr}='{position}'"
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
    year_list = ",".join(str(int(y)) for y in years)

    tgw = "TRUE" if "player_team_game_week" not in tables else "tgw.NFL_player_id IS NOT NULL"
    act = "TRUE" if "player_active_week" not in tables else "act.NFL_player_id IS NOT NULL"
    team_join = tgw
    active_join = act
    joins = ""
    if "player_team_game_week" in tables:
        joins += " LEFT JOIN lake.public.player_team_game_week tgw ON tgw.NFL_player_id=pf.NFL_player_id AND tgw.year=pf.year AND tgw.week=pf.week"
    if "player_active_week" in tables:
        joins += " LEFT JOIN lake.public.player_active_week act ON act.NFL_player_id=pf.NFL_player_id AND act.year=pf.year AND act.week=pf.week"

    clutch = "100.0 * AVG(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND pf.clutch_equity IS NOT NULL THEN CAST(pf.clutch_equity AS DOUBLE) END)" if "clutch_equity" in pf_cols else "NULL::DOUBLE"
    champ_col = next((c for c in (
        "is_championship", "championship", "is_championship_game", "championship_game"
    ) if c in pf_cols), None)
    champ = (
        f"MAX(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 "
        f"AND COALESCE(CAST(pf.{champ_col} AS DOUBLE),0)=1 THEN 1 ELSE 0 END)"
        if champ_col else "NULL::DOUBLE"
    )

    if "matchup" in tables and {"manager", "final_playoff_seed"}.issubset(cols(con, "matchup")):
        mp = """
        LEFT JOIN (
          SELECT m.db_name, m.year, m.manager,
                 MAX(CASE WHEN m.final_playoff_seed <= ls.playoff_teams THEN 1 ELSE 0 END) AS made_po
          FROM lake.public.matchup m
          JOIN lake.public.league_settings ls ON ls.db_name=m.db_name AND ls.year=m.year
          WHERE m.manager IS NOT NULL AND m.final_playoff_seed IS NOT NULL
          GROUP BY 1,2,3
        ) mp ON mp.db_name=pf.db_name AND mp.year=pf.year AND mp.manager=pf.manager
        """
        playoffs = "MAX(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND COALESCE(mp.made_po,0)=1 THEN 1 ELSE 0 END)"
    else:
        mp = ""
        playoffs = "NULL::DOUBLE"

    if "manager" in pf_cols:
        manager_filter = "AND pf.manager IS NOT NULL"
    else:
        manager_filter = ""

    sql = f"""
    WITH ls AS ({settings_sql})
    SELECT pf.db_name, CAST(pf.NFL_player_id AS VARCHAR) AS player,
           CAST(pf.year AS INTEGER) AS year,
           {cohort_base} AS cohort_base, ls.bracket AS bracket,
           SUM(CASE WHEN CAST(pf.is_rostered AS DOUBLE)=1 THEN 1 ELSE 0 END)::DOUBLE AS roster_weeks,
           SUM(CASE WHEN CAST(pf.is_rostered AS DOUBLE)=1 AND CAST(pf.is_started AS DOUBLE)=1 THEN 1 ELSE 0 END)::DOUBLE AS start_weeks,
           SUM(CASE WHEN {team_join} THEN 1 ELSE 0 END)::DOUBLE AS team_eligible_weeks,
           SUM(CASE WHEN {active_join} THEN 1 ELSE 0 END)::DOUBLE AS healthy_eligible_weeks,
           SUM(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND {active_join} THEN 1 ELSE 0 END)::DOUBLE AS healthy_start_weeks,
           SUM(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND {team_join} AND pf.win=1 THEN 1 ELSE 0 END)::DOUBLE AS wins_started,
           SUM(CASE WHEN CAST(pf.is_started AS DOUBLE)=1 AND {team_join} AND pf.win=0 THEN 1 ELSE 0 END)::DOUBLE AS losses_started,
           {clutch} AS clutch,
           {champ} AS champ,
           {playoffs} AS playoffs
    FROM lake.public.player_fantasy pf
    JOIN player_position_map pp ON CAST(pp.NFL_player_id AS VARCHAR)=CAST(pf.NFL_player_id AS VARCHAR) AND pp.year=pf.year
    JOIN ls ON ls.db_name=pf.db_name AND ls.year=pf.year
    {joins}{mp}
    WHERE pf.year IN ({year_list}) AND {pos_filter} AND {team_bucket} IS NOT NULL
      AND ls.lineup_mode <> 'best_ball' {manager_filter}
    GROUP BY pf.db_name, player, pf.year, cohort_base, ls.bracket
    """
    frame = con.execute(sql).fetchdf()
    if frame.empty:
        return frame
    frame["healthy_start_pct"] = frame["healthy_start_weeks"] / frame["healthy_eligible_weeks"].replace(0, np.nan)
    frame["start_pct"] = frame["start_weeks"] / frame["team_eligible_weeks"].replace(0, np.nan)
    decided = (frame["wins_started"] + frame["losses_started"]).replace(0, np.nan)
    frame["expected_starts"] = frame["start_pct"]
    frame["expected_wins"] = frame["expected_starts"] * frame["wins_started"] / decided
    frame["expected_losses"] = frame["expected_starts"] * frame["losses_started"] / decided
    frame["expected_wl"] = frame["expected_wins"] - frame["expected_losses"]
    frame["cohort_key"] = frame["cohort_base"]
    return frame


def _lock_probability(matrix: np.ndarray, population_order: np.ndarray, n: int, rng: np.random.Generator, mode: str, bottom: bool = False) -> float:
    if n < 2 or matrix.shape[0] < n:
        return 0.0
    reps = min(100, max(40, 1200 // max(1, n // 10)))
    hits = 0
    population_set = set(population_order.tolist())
    for _ in range(reps):
        take = rng.choice(matrix.shape[0], size=n, replace=False)
        values = np.nanmean(matrix[take], axis=0)
        values = np.nan_to_num(values, nan=-np.inf)
        order = np.lexsort((np.arange(values.size), -values))
        sampled = order[-10:][::-1] if bottom else order[:10]
        if mode == "order":
            hit = np.array_equal(sampled, population_order)
        else:
            hit = set(sampled.tolist()) == population_set
        hits += int(hit)
    return hits / reps


def _decile_lock(matrix: np.ndarray, labels: np.ndarray, target: int, n: int, rng: np.random.Generator) -> float:
    if n < 2 or matrix.shape[0] < n:
        return 0.0
    idx = np.flatnonzero(labels == target)
    if len(idx) == 0:
        return 0.0
    reps = min(100, max(40, 1200 // max(1, n // 10)))
    hits = 0
    for _ in range(reps):
        take = rng.choice(matrix.shape[0], size=n, replace=False)
        values = np.nanmean(matrix[take], axis=0)
        order = np.lexsort((np.arange(values.size), -np.nan_to_num(values, nan=-np.inf)))
        sampled_labels = np.zeros(len(values), dtype=int)
        ranked = np.array_split(order, 10)
        for i, bucket in enumerate(ranked, start=1):
            sampled_labels[bucket] = i * 10
        hits += int(np.mean(sampled_labels[idx] == target) >= 0.85)
    return hits / reps


def _threshold(probs: list[tuple[int, float]], level: float) -> int | None:
    return next((n for n, p in probs if p >= level), None)


def build(args: argparse.Namespace) -> None:
    args.out.parent.mkdir(parents=True, exist_ok=True)
    (args.out.parent / "duckdb_temp").mkdir(exist_ok=True)
    con = duckdb.connect(config={"memory_limit": f"{args.memory_mb}MB", "temp_directory": str(args.out.parent / "duckdb_temp")})
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"ATTACH '{args.snapshot.as_posix()}' AS lake (READ_ONLY)")
    if args.ops:
        con.execute(f"ATTACH '{args.ops.as_posix()}' AS ops (READ_ONLY)")
    frame = _season_frame(con, args.years, args.position)
    rows: list[dict] = []
    if frame.empty:
        pd.DataFrame(rows).to_parquet(args.out, index=False)
        return

    year_label = str(args.years[0]) if len(args.years) == 1 else f"{min(args.years)}-{max(args.years)}_pooled"
    rng = np.random.default_rng(args.seed + sum(args.years) + sum(map(ord, args.position)))

    for metric in METRICS:
        metric_frame = frame.copy()
        if metric in {"clutch", "champ", "playoffs"}:
            metric_frame["cohort_key"] = metric_frame["cohort_base"] + "|" + metric_frame["bracket"].fillna("ALL")
        else:
            metric_frame["cohort_key"] = metric_frame["cohort_base"]
        for cohort, group in metric_frame.groupby("cohort_key", dropna=False):
            group = group[group[metric].notna()].copy()
            if group.empty:
                continue
            board_group = group[group.start_pct.between(args.start_low, args.start_high)].copy()
            players = sorted(group.player.unique())
            leagues = sorted(group.db_name.unique())
            if len(leagues) < 2:
                continue
            pidx = {p: i for i, p in enumerate(players)}
            lidx = {l: i for i, l in enumerate(leagues)}
            matrix_frame = group.groupby(["db_name", "player"], as_index=False)[metric].mean()
            matrix = np.full((len(leagues), len(players)), np.nan)
            for r in matrix_frame[["db_name", "player", metric]].itertuples(index=False):
                matrix[lidx[r.db_name], pidx[r.player]] = r[2]
            population = np.nanmean(matrix, axis=0)
            order = np.lexsort((np.arange(len(players)), -np.nan_to_num(population, nan=-np.inf)))
            labels = np.zeros(len(players), dtype=int)
            for i, bucket in enumerate(np.array_split(order, 10), start=1):
                labels[bucket] = i * 10
            counts = pd.Series(np.sum(~np.isnan(matrix), axis=0))
            support = {
                "available_leagues": len(leagues),
                "eligible_players": len(players),
                "player_league_rows": int(np.sum(~np.isnan(matrix))),
                "players_with_2plus_leagues": int((counts >= 2).sum()),
                "median_player_leagues": float(counts.median()),
                "p10_player_leagues": float(counts.quantile(0.10)),
                "p90_player_leagues": float(counts.quantile(0.90)),
                "overlap_share_2plus": float((counts >= 2).mean()),
            }
            support_band = (
                "not_estimable" if support["players_with_2plus_leagues"] < 5 or len(leagues) < 5
                else "thin_pool" if support["players_with_2plus_leagues"] < 30 or len(leagues) < 30
                else "supported"
            )
            sizes = sample_sizes(len(leagues))
            board_matrix_frame = board_group.groupby(["db_name", "player"], as_index=False)[metric].mean()
            board_players = sorted(board_group.player.unique())
            board_leagues = sorted(board_group.db_name.unique())
            board_pidx = {p: i for i, p in enumerate(board_players)}
            board_lidx = {l: i for i, l in enumerate(board_leagues)}
            board_matrix = np.full((len(board_leagues), len(board_players)), np.nan)
            for r in board_matrix_frame[["db_name", "player", metric]].itertuples(index=False):
                board_matrix[board_lidx[r.db_name], board_pidx[r.player]] = r[2]
            board_population = np.nanmean(board_matrix, axis=0) if board_players else np.array([])
            board_order = np.lexsort((np.arange(len(board_players)), -np.nan_to_num(board_population, nan=-np.inf))) if board_players else np.array([], dtype=int)
            top_order = board_order[:10]
            bottom_order = board_order[-10:][::-1]
            for board, board_order in (("top10", top_order), ("bottom10", bottom_order)):
                is_bottom = board == "bottom10"
                board_counts = pd.Series(np.sum(~np.isnan(board_matrix), axis=0)) if board_players else pd.Series(dtype=float)
                board_estimable = len(board_players) >= 10 and int((board_counts >= 2).sum()) >= 10
                board_sizes = sample_sizes(len(board_leagues))
                order_probs = [(n, _lock_probability(board_matrix, board_order, n, rng, "order", bottom=is_bottom)) for n in board_sizes] if board_estimable else []
                member_probs = [(n, _lock_probability(board_matrix, board_order, n, rng, "membership", bottom=is_bottom)) for n in board_sizes] if board_estimable else []
                rows.append({
                    "year": year_label, "position": args.position, "analysis": "board",
                    "cohort_key": cohort, "metric": metric, "target_pct": 50,
                    "board": board, "grain": "season", "start_rate_filter": f"{args.start_low:.2f}-{args.start_high:.2f}",
                    "target_definition": "start_rate_45_55_band",
                    "lock_75_leagues": _threshold(order_probs, .75), "lock_85_leagues": _threshold(order_probs, .85), "lock_95_leagues": _threshold(order_probs, .95),
                    "membership_lock_85_leagues": _threshold(member_probs, .85),
                    "cell_estimable": board_estimable,
                    **support, "board_available_leagues": len(board_leagues),
                    "board_eligible_players": len(board_players),
                    "board_players_with_2plus_leagues": int((board_counts >= 2).sum()) if board_players else 0,
                    "support_band": support_band,
                    "population_board": ",".join(board_players[i] for i in board_order),
                })
            player_league = matrix_frame.rename(columns={metric: "value"})
            player_global = player_league.groupby("player", as_index=False)["value"].mean()
            if metric == "clutch":
                targets = CLUTCH_TARGETS
                target_players = {
                    target: set(player_global.loc[
                        player_global["value"].between(target - 5, target + 5), "player"
                    ])
                    for target in targets
                }
            else:
                targets = TARGETS
                rank_order = player_global.sort_values(["value", "player"], ascending=[False, True])["player"].tolist()
                rank_labels = {
                    player: (i * 10 // max(1, len(rank_order)) + 1) * 10
                    for i, player in enumerate(rank_order)
                }
                target_players = {
                    target: {player for player, label in rank_labels.items() if label == target}
                    for target in targets
                }
            for target in targets:
                selected_players = target_players[target]
                target_values = player_league[player_league.player.isin(selected_players)].copy()
                target_count = len(selected_players)
                target_2plus = int((target_values.groupby("player")["db_name"].nunique() >= 2).sum())
                decile_estimable = metric != "clutch" and target_count >= 3 and target_2plus >= 3
                probs = [(n, _decile_lock(matrix, labels, target, n, rng)) for n in sizes] if decile_estimable else []
                # Rates are stored as fractions; outcomes and Clutch are in their native units.
                margin_unit = (lambda m: float(m)) if metric == "clutch" else (lambda m: m / 100.0)
                required = {}
                for margin in MARGINS:
                    if target_values.empty:
                        required[f"required_leagues_{margin}pct_p50"] = None
                        required[f"required_leagues_{margin}pct_p90"] = None
                    else:
                        req = player_required_leagues(
                            target_values.rename(columns={"value": "season_value"}),
                            "season_value", margin_unit(margin), args.confidence,
                        )
                        required[f"required_leagues_{margin}pct_p50"] = (
                            int(req.required_leagues.quantile(.50)) if not req.empty else None
                        )
                        required[f"required_leagues_{margin}pct_p90"] = (
                            int(req.required_leagues.quantile(.90)) if not req.empty else None
                        )
                rows.append({
                    "year": year_label, "position": args.position, "analysis": "decile" if metric != "clutch" else "target_band",
                    "cohort_key": cohort, "metric": metric, "target_pct": target,
                    "board": None, "grain": "season", "start_rate_filter": f"{args.start_low:.2f}-{args.start_high:.2f}",
                    "target_definition": "metric_decile" if metric != "clutch" else "clutch_plus_minus_5_band",
                    "decile_lock_75_leagues": _threshold(probs, .75), "decile_lock_85_leagues": _threshold(probs, .85), "decile_lock_95_leagues": _threshold(probs, .95),
                    "decile_players": target_count, "decile_players_2plus": target_2plus,
                    "cell_estimable": decile_estimable if metric != "clutch" else target_2plus >= 3,
                    "target_band_width": 10 if metric == "clutch" else None,
                    **required, **support, "support_band": support_band,
                })
    pd.DataFrame(rows).to_parquet(args.out, index=False)
    print(f"wrote {len(rows):,} season stability rows to {args.out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path)
    ap.add_argument("--year", type=int)
    ap.add_argument("--years")
    ap.add_argument("--position", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--start-low", type=float, default=.45)
    ap.add_argument("--start-high", type=float, default=.55)
    ap.add_argument("--confidence", type=float, default=.85)
    ap.add_argument("--memory-mb", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=20260731)
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
