"""Build season and weekly rank-slot pooling studies for non-RB positions.

The rank unit is the Nth-most-started (or Nth-most-expected-wins) player within a
position/cohort/period.  The player identity is intentionally discarded after ranking.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb
import pandas as pd

import position_slots_contract as PS
from weekly_rb_rank_slot_pooling import assign_rank_slots, rank_slot_spread


ROOT = Path(__file__).resolve().parents[2]
CORPUS = Path(os.environ.get(
    "RESEARCH_CORPUS_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb",
))
OPS = Path(os.environ.get(
    "RESEARCH_OPS_CACHE_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb",
))
POSITIONS = ("WR", "QB", "TE", "DEF", "K")
LANES = ("flx", "sflx", "idp")
DIMS = ("pos_slots", "flx_slots", "ppr", "td", "bracket")


def _lane_expr(alias: str = "s") -> str:
    return f"""CASE WHEN COALESCE({alias}.roster_IDP,0)+COALESCE({alias}.roster_DL,0)
        +COALESCE({alias}.roster_LB,0)+COALESCE({alias}.roster_DB,0)
        +COALESCE({alias}.roster_DB_LB,0)+COALESCE({alias}.roster_DL_LB,0)>0 THEN 'idp'
        WHEN COALESCE({alias}.roster_SUPER_FLEX,0)>0 THEN 'sflx' ELSE 'flx' END"""


def _scoring_expr(alias: str = "s") -> str:
    return f"""CASE WHEN COALESCE({alias}.scoring_rec,0)=0 THEN 'std'
        WHEN COALESCE({alias}.scoring_rec,0)<0.75 THEN 'half' ELSE 'ppr' END"""


def _position_bucket(position: str) -> str:
    if position not in PS.TIER_POSITIONS:
        return "CASE WHEN num_teams <= 11 THEN '10t' ELSE '12t' END"
    lane = _lane_expr("s")
    scoring = _scoring_expr("s")
    # OBSERVED CAPACITY, the only path now. team_equiv_sql read declared roster_* columns,
    # which understate real rosters by 3-5 spots and had no formula at all for K/DEF/IDP.
    equiv = f"cap_{position.lower()}.spots"
    return f"CASE WHEN ({equiv}) <= {PS.TEAM_EQUIV_CUT} THEN '10t' ELSE '12t' END"


def _league_sql(position: str) -> str:
    pos_bucket = _position_bucket(position)
    return f"""
        SELECT db_name, end_week,
          {_lane_expr('s')} AS lane,
          ({pos_bucket}) AS pos_slots,
          CASE WHEN COALESCE(roster_FLX,0)<=0 THEN 'flx0'
               WHEN COALESCE(roster_FLX,0)=1 THEN 'flx1'
               WHEN COALESCE(roster_FLX,0)=2 THEN 'flx2' ELSE 'flx3+' END AS flx_slots,
          CASE WHEN COALESCE(scoring_rec,0)=0 THEN 'std'
               WHEN COALESCE(scoring_rec,0)<0.75 THEN 'half' ELSE 'ppr' END AS ppr,
          CASE WHEN COALESCE(scoring_pass_td,4)>=5 THEN '6pt' ELSE '4pt' END AS td,
          CASE WHEN playoff_teams IS NULL THEN NULL
               WHEN playoff_teams<=5 THEN '4po'
               WHEN playoff_teams<=7 THEN '6po' ELSE '8po' END AS bracket
        FROM s.public.league_settings s
        WHERE year=2025 AND COALESCE(sleeper_best_ball,false)=false
          AND num_teams BETWEEN 8 AND 14 AND end_week IS NOT NULL
    """


def build_atoms(position: str) -> pd.DataFrame:
    if position not in POSITIONS:
        raise ValueError(f"unsupported position: {position}")
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='5000MB'")
    con.execute(f"ATTACH '{CORPUS.as_posix()}' AS s (READ_ONLY)")
    con.execute(f"ATTACH '{OPS.as_posix()}' AS ops (READ_ONLY)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE top50 AS
        SELECT NFL_player_id
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE year=2025 AND season_type='REG' AND position=?
          AND NFL_player_id IS NOT NULL
        GROUP BY NFL_player_id
        ORDER BY SUM(COALESCE(fantasy_points_ppr,0)) DESC
        LIMIT 50
    """, [position])
    con.execute("""
        CREATE OR REPLACE TEMP TABLE active_player_weeks AS
        SELECT DISTINCT NFL_player_id AS pid, CAST(week AS INTEGER) AS week
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE year=2025 AND season_type='REG' AND position=?
          AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL
    """, [position])
    con.execute(f"CREATE OR REPLACE TEMP TABLE lg AS {_league_sql(position)}")
    return con.execute("""
        SELECT f.NFL_player_id AS pid, f.week, g.lane, g.pos_slots,
               g.flx_slots, g.ppr, g.td, g.bracket,
               COUNT(*) AS eligible,
               SUM(CASE WHEN f.is_started=1 THEN 1 ELSE 0 END) AS started,
               SUM(CASE WHEN f.is_started=1 AND f.win=1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN f.is_started=1 AND f.win=0 THEN 1 ELSE 0 END) AS losses
        FROM s.public.player_fantasy f
        JOIN lg g ON g.db_name=f.db_name
        JOIN top50 t ON t.NFL_player_id=f.NFL_player_id
        JOIN active_player_weeks apw
          ON apw.pid=f.NFL_player_id AND apw.week=f.week
        WHERE f.year=2025 AND f.is_rostered=1 AND f.week<=g.end_week
        GROUP BY 1,2,3,4,5,6,7,8
    """).fetchdf()


def _period_atoms(atoms: pd.DataFrame, grain: str) -> tuple[pd.DataFrame, tuple[str, ...]]:
    dims = ["lane", *DIMS, "pid"]
    if grain == "weekly":
        return atoms.assign(year=2025), ("week",)
    if grain != "season":
        raise ValueError(f"unsupported grain: {grain}")
    return (
        atoms.groupby(dims, dropna=False, as_index=False)[
            ["eligible", "started", "wins", "losses"]
        ].sum().assign(year=2025),
        ("year",),
    )


def summarize_curves(curve: pd.DataFrame) -> pd.DataFrame:
    """Average rank-slot spreads while retaining the interpretable cohort axes."""
    keys = ["position", "grain", "rank_by", "lane", "dimension"]
    measures = ["start_pct_spread", "win_pct_spread", "expected_wl_spread"]
    missing = {*keys, *measures} - set(curve.columns)
    if missing:
        raise ValueError(f"missing curve summary columns: {sorted(missing)}")
    return curve.groupby(keys, as_index=False)[measures].mean()


def run(output_dir: Path, positions: tuple[str, ...] = POSITIONS) -> tuple[pd.DataFrame, pd.DataFrame]:
    details: list[pd.DataFrame] = []
    for position in positions:
        atoms = build_atoms(position)
        for grain in ("season", "weekly"):
            period_atoms, period_cols = _period_atoms(atoms, grain)
            for lane in LANES:
                lane_atoms = period_atoms[period_atoms.lane == lane]
                for dim in DIMS:
                    pooled = (
                        lane_atoms.groupby([dim, *period_cols, "pid"], dropna=False, as_index=False)[
                            ["eligible", "started", "wins", "losses"]
                        ].sum().rename(columns={dim: "cohort"})
                    )
                    for rank_by in ("start_pct", "expected_wins"):
                        ranked = assign_rank_slots(
                            pooled, rank_by=rank_by, max_rank=50, period_cols=period_cols
                        )
                        spread = rank_slot_spread(
                            ranked, cohort_col="cohort", period_cols=period_cols
                        )
                        if spread.empty:
                            continue
                        spread.insert(0, "rank_by", rank_by)
                        spread.insert(1, "position", position)
                        spread.insert(2, "grain", grain)
                        spread.insert(3, "lane", lane)
                        spread.insert(4, "dimension", dim)
                        details.append(spread)
    detail = pd.concat(details, ignore_index=True)
    curve_keys = ["rank_by", "position", "grain", "lane", "dimension", "slot"]
    curve = detail.groupby(curve_keys, as_index=False)[
        ["start_pct_spread", "win_pct_spread", "expected_wl_spread"]
    ].mean()
    summary = summarize_curves(curve)
    output_dir.mkdir(parents=True, exist_ok=True)
    detail.to_csv(output_dir / "rank_slot_spread.csv", index=False)
    curve.to_csv(output_dir / "spread_by_rank.csv", index=False)
    summary.to_csv(output_dir / "spread_summary.csv", index=False)
    return detail, curve


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--positions", nargs="+", choices=POSITIONS, default=list(POSITIONS))
    args = parser.parse_args()
    detail, curve = run(args.output_dir, tuple(args.positions))
    print(f"wrote {len(detail):,} period/slot spreads and {len(curve):,} rank-curve rows")


if __name__ == "__main__":
    main()
