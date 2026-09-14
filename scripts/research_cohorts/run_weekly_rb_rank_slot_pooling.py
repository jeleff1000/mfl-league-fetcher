"""Run the weekly 2025 top-50 RB rank-slot pooling study.

This is deliberately separate from the production cohort builder. It reads the immutable
research corpus, ranks independently inside each (cohort, week), and writes two auditable
CSV surfaces:

* ``rank_slot_spread.csv``: one row per (rank_by, lane, dimension, week, slot)
* ``spread_by_rank.csv``: the same spreads averaged over weeks, retaining the rank curve
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
DIMS = ("rb_slots", "flx_slots", "ppr", "td", "bracket")
LANES = ("flx", "sflx", "idp")


def _rb_slots_case() -> str:
    weights = {
        lane: {scoring: values["RB"] for scoring, values in scoring_map.items()}
        for lane, scoring_map in PS.FLEX_FILL.items()
    }

    def w(lane: str, scoring: str) -> float:
        return weights[lane][scoring]

    def lane_weight(lane: str) -> str:
        return (
            f"CASE WHEN COALESCE(scoring_rec,0)=0 THEN {w(lane, 'std')} "
            f"WHEN COALESCE(scoring_rec,0)<0.75 THEN {w(lane, 'half')} "
            f"ELSE {w(lane, 'ppr')} END"
        )

    contested = (
        "(COALESCE(roster_FLX,0) + CASE WHEN lane IN ('sflx','idp') "
        "THEN COALESCE(roster_SUPER_FLEX,0) ELSE 0 END)"
    )
    return (
        "CASE WHEN lane='idp' THEN num_teams*(COALESCE(roster_RB,0)+"
        f"({lane_weight('idp')})*{contested}) "
        "WHEN lane='sflx' THEN num_teams*(COALESCE(roster_RB,0)+"
        f"({lane_weight('sflx')})*{contested}) "
        "ELSE num_teams*(COALESCE(roster_RB,0)+"
        f"({lane_weight('flx')})*COALESCE(roster_FLX,0)) END"
    )


def build_atoms() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='5000MB'")
    con.execute(f"ATTACH '{CORPUS.as_posix()}' AS s (READ_ONLY)")
    con.execute(f"ATTACH '{OPS.as_posix()}' AS ops (READ_ONLY)")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE top50 AS
        SELECT NFL_player_id
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE year=2025 AND season_type='REG' AND position='RB'
          AND NFL_player_id IS NOT NULL
        GROUP BY NFL_player_id
        ORDER BY SUM(COALESCE(fantasy_points_ppr,0)) DESC
        LIMIT 50
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE pw AS
        SELECT a.NFL_player_id, a.week, ANY_VALUE(a.nfl_team) AS nfl_team
        FROM ops.nfl_historical.nfl_player_stats_all a
        JOIN top50 t USING (NFL_player_id)
        WHERE a.year=2025 AND a.season_type='REG'
        GROUP BY 1,2
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE pteam AS
        SELECT NFL_player_id, nfl_team
        FROM (
          SELECT NFL_player_id, nfl_team,
                 ROW_NUMBER() OVER (PARTITION BY NFL_player_id ORDER BY COUNT(*) DESC) AS rk
          FROM pw GROUP BY 1,2
        ) WHERE rk=1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tw AS
        SELECT nfl_team, week
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE year=2025 AND season_type='REG' AND nfl_team IS NOT NULL
        GROUP BY 1,2
    """)
    rb_slots = _rb_slots_case()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lg AS
        SELECT db_name, end_week,
          CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)
                    +COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)
                    +COALESCE(roster_DL_LB,0)>0 THEN 'idp'
               WHEN COALESCE(roster_SUPER_FLEX,0)>0 THEN 'sflx' ELSE 'flx' END AS lane,
          CASE WHEN ({rb_slots}) < 25 THEN 'rb<25'
               WHEN ({rb_slots}) < 30 THEN 'rb25-29'
               WHEN ({rb_slots}) < 35 THEN 'rb30-34' ELSE 'rb35+' END AS rb_slots,
          CASE WHEN COALESCE(roster_FLX,0)<=0 THEN 'flx0'
               WHEN COALESCE(roster_FLX,0)=1 THEN 'flx1'
               WHEN COALESCE(roster_FLX,0)=2 THEN 'flx2' ELSE 'flx3+' END AS flx_slots,
          CASE WHEN COALESCE(scoring_rec,0)=0 THEN 'std'
               WHEN COALESCE(scoring_rec,0)<0.75 THEN 'half' ELSE 'ppr' END AS ppr,
          CASE WHEN COALESCE(scoring_pass_td,4)>=5 THEN '6pt' ELSE '4pt' END AS td,
          CASE WHEN playoff_teams IS NULL THEN NULL
               WHEN playoff_teams<=5 THEN '4po'
               WHEN playoff_teams<=7 THEN '6po' ELSE '8po' END AS bracket
        FROM s.public.league_settings
        WHERE year=2025 AND COALESCE(sleeper_best_ball,false)=false
          AND num_teams BETWEEN 8 AND 14 AND end_week IS NOT NULL
    """)
    atoms = con.execute("""
        SELECT f.NFL_player_id AS pid, f.week, g.lane, g.rb_slots, g.flx_slots,
               g.ppr, g.td, g.bracket,
               SUM(CASE WHEN tw.week IS NOT NULL THEN 1 ELSE 0 END) AS eligible,
               SUM(CASE WHEN tw.week IS NOT NULL AND f.is_started=1 THEN 1 ELSE 0 END) AS started,
               SUM(CASE WHEN pw.week IS NOT NULL AND f.is_started=1 AND f.win=1 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN pw.week IS NOT NULL AND f.is_started=1 AND f.win=0 THEN 1 ELSE 0 END) AS losses
        FROM s.public.player_fantasy f
        JOIN lg g ON g.db_name=f.db_name
        JOIN top50 t ON t.NFL_player_id=f.NFL_player_id
        JOIN pteam pt ON pt.NFL_player_id=f.NFL_player_id
        LEFT JOIN tw ON tw.nfl_team=pt.nfl_team AND tw.week=f.week
        LEFT JOIN pw ON pw.NFL_player_id=f.NFL_player_id AND pw.week=f.week
        WHERE f.year=2025 AND f.is_rostered=1 AND f.week<=g.end_week
        GROUP BY 1,2,3,4,5,6,7,8
    """).fetchdf()
    return atoms


def run(output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    atoms = build_atoms()
    details: list[pd.DataFrame] = []
    for lane in LANES:
        lane_atoms = atoms[atoms.lane == lane]
        for dim in DIMS:
            pooled = (
                lane_atoms.groupby([dim, "week", "pid"], dropna=False, as_index=False)[
                    ["eligible", "started", "wins", "losses"]
                ]
                .sum()
                .rename(columns={dim: "cohort"})
            )
            for rank_by in ("start_pct", "expected_wins"):
                ranked = assign_rank_slots(
                    pooled,
                    rank_by=rank_by,
                    max_rank=50,
                )
                spread = rank_slot_spread(ranked, cohort_col="cohort")
                if spread.empty:
                    continue
                spread.insert(0, "rank_by", rank_by)
                spread.insert(1, "lane", lane)
                spread.insert(2, "dimension", dim)
                details.append(spread)
    detail = pd.concat(details, ignore_index=True)
    curve = (
        detail.groupby(["rank_by", "lane", "dimension", "slot"], as_index=False)
        [["start_pct_spread", "win_pct_spread", "expected_wl_spread"]]
        .mean()
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    detail.to_csv(output_dir / "rank_slot_spread.csv", index=False)
    curve.to_csv(output_dir / "spread_by_rank.csv", index=False)
    return detail, curve


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("D:/league-history-data/fantasy_leagues/derived_reports/weekly_rb_rank_slots_2025"),
    )
    args = parser.parse_args()
    detail, curve = run(args.output_dir)
    print(f"wrote {len(detail):,} week/slot spreads and {len(curve):,} rank-curve rows")


if __name__ == "__main__":
    main()
