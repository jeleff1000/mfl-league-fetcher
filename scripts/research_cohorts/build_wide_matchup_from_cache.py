"""Build the wide matchup product directly from the enriched GitHub cache.

The cache is the source of truth for league dimensions.  This program only uses
the ops player-week cache to identify NFL activity/position and emits the
normalized source tables consumed by ``build_wide_bundle``.  The wide bundle
then pivots every cohort lane into columns, so the serving grain is exactly:

    player/week, player/season, and player/career

There is no league-settings join and no shard input.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


SLUGS = [
    f"{teams}_{roster}_{ppr}_{td}"
    for teams in ("10t", "12t")
    for roster in ("flx", "sflx", "idp")
    for ppr in ("std", "half", "ppr")
    for td in ("4pt", "6pt")
]
BRACKETS = ("4po", "6po", "8po")


def _values(items: tuple[str, ...] | list[str]) -> str:
    return ", ".join("(" + ", ".join(repr(v) for v in item) + ")" for item in items)


def build_source(
    snapshot: Path,
    ops_cache: Path,
    source_db: Path,
    year_start: int,
    year_end: int,
    player_id: str | None = None,
    week: int | None = None,
) -> None:
    con = duckdb.connect(str(source_db))
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute("SET memory_limit='10000MB'")
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops_cache.as_posix()}' AS ops (READ_ONLY)")

        player_filter = ""
        if player_id:
            player_filter += f" AND CAST(p.NFL_player_id AS VARCHAR) = '{player_id}'"
        if week is not None:
            player_filter += f" AND CAST(p.week AS INTEGER) = {int(week)}"

        slug_values = ", ".join(
            f"('{s}', '{s.split('_')[0]}', '{s.split('_')[1]}', '{s.split('_')[2]}', '{s.split('_')[3]}')"
            for s in SLUGS
        )
        bracket_values = ", ".join(f"('{b}')" for b in BRACKETS)

        sql = f"""
        CREATE OR REPLACE TABLE matchup_weekly AS
        WITH
        active_raw AS (
          SELECT CAST(NFL_player_id AS VARCHAR) AS player_id,
                 CAST(year AS INTEGER) AS year,
                 CAST(week AS INTEGER) AS week,
                 COALESCE(position, nfl_position) AS position,
                 nfl_team,
                 player_week
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE year BETWEEN {year_start} AND {year_end}
            AND week IS NOT NULL
            AND COALESCE(season_type, 'REG') = 'REG'
        ),
        active AS (
          SELECT * FROM active_raw
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY player_id, year, week ORDER BY player_week
          ) = 1
        ),
        team_active AS (
          SELECT DISTINCT year, week, nfl_team
          FROM active
          WHERE nfl_team IS NOT NULL AND TRIM(CAST(nfl_team AS VARCHAR)) <> ''
        ),
        player_team AS (
          SELECT player_id, year, MODE(nfl_team) AS nfl_team
          FROM active
          WHERE nfl_team IS NOT NULL AND TRIM(CAST(nfl_team AS VARCHAR)) <> ''
          GROUP BY 1, 2
        ),
        matchup_one AS (
          SELECT db_name, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                 LOWER(TRIM(CAST(manager AS VARCHAR))) AS manager_key,
                 win, loss, tie, is_playoffs, final_playoff_seed, champion,
                 ROW_NUMBER() OVER (
                   PARTITION BY db_name, year, week, LOWER(TRIM(CAST(manager AS VARCHAR)))
                   ORDER BY CASE WHEN win IS NOT NULL OR loss IS NOT NULL OR tie IS NOT NULL THEN 0 ELSE 1 END
                 ) AS rn
          FROM lake.public.matchup
          WHERE year BETWEEN {year_start} AND {year_end}
        ),
        player_one AS (
          SELECT p.db_name, CAST(p.year AS INTEGER) AS year, CAST(p.week AS INTEGER) AS week,
                 CAST(p.NFL_player_id AS VARCHAR) AS player_id, p.manager,
                 COALESCE(a.position, p.position) AS position,
                 CASE WHEN COALESCE(a.position, p.position) IN ('QB','RB','WR','TE','K','DEF')
                      THEN COALESCE(a.position, p.position) ELSE 'IDP' END AS pos_grp,
                 p.cohort_teams AS source_teams, p.cohort_roster AS source_roster,
                 p.cohort_scoring AS source_ppr, p.cohort_pass_td AS source_td,
                 p.cohort_playoff_teams AS source_bracket,
                 p.cohort_dynasty AS source_league_type,
                 p.cohort_best_ball AS source_lineup_mode,
                 CAST(p.cohort_position_eligible AS INTEGER) AS position_eligible,
                 CAST(p.is_rostered AS INTEGER) AS is_rostered,
                 CAST(p.is_started AS INTEGER) AS is_started,
                 p.fantasy_points, p.clutch_equity,
                 CASE WHEN a.player_id IS NOT NULL THEN 1 ELSE 0 END AS active_week,
                 CASE WHEN a.player_id IS NULL AND pt.nfl_team IS NOT NULL AND ta.nfl_team IS NULL
                      THEN 1 ELSE 0 END AS bye_week,
                 COALESCE(m.win, TRY_CAST(p.win AS INTEGER)) AS outcome_win,
                 COALESCE(m.loss, CASE WHEN p.win IS NOT NULL THEN 1-TRY_CAST(p.win AS INTEGER) END) AS outcome_loss,
                 m.tie AS outcome_tie,
                 COALESCE(m.is_playoffs, p.is_playoffs) AS playoff_signal,
                 COALESCE(m.final_playoff_seed, p.final_playoff_seed) AS playoff_seed_signal,
                 COALESCE(m.champion, p.champion) AS champion_signal,
                 ROW_NUMBER() OVER (
                   PARTITION BY p.db_name, p.year, p.week, p.NFL_player_id
                   ORDER BY CAST(p.is_started AS INTEGER) DESC, p.fantasy_points DESC NULLS LAST
                 ) AS rn
          FROM lake.public.player_fantasy p
          LEFT JOIN active a ON a.player_id=CAST(p.NFL_player_id AS VARCHAR)
            AND a.year=p.year AND a.week=p.week
          LEFT JOIN player_team pt ON pt.player_id=CAST(p.NFL_player_id AS VARCHAR) AND pt.year=p.year
          LEFT JOIN team_active ta ON ta.year=p.year AND ta.week=p.week AND ta.nfl_team=pt.nfl_team
          LEFT JOIN matchup_one m ON m.db_name=p.db_name AND m.year=p.year AND m.week=p.week
            AND m.manager_key=LOWER(TRIM(CAST(p.manager AS VARCHAR))) AND m.rn=1
          WHERE p.year BETWEEN {year_start} AND {year_end}
            AND p.week IS NOT NULL AND p.NFL_player_id IS NOT NULL
            AND (p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER)=1)
            {player_filter}
        ),
        player AS (SELECT * FROM player_one WHERE rn=1),
        inventory AS (
          SELECT DISTINCT db_name, year, week, pos_grp,
                 source_teams, source_roster, source_ppr, source_td,
                 source_bracket, source_league_type, source_lineup_mode
          FROM player
          WHERE position_eligible=1
        ),
        weekly_counts AS (
          SELECT year, week, pos_grp,
            COUNT(DISTINCT db_name) FILTER (WHERE source_teams='10t') AS c_10t,
            COUNT(DISTINCT db_name) FILTER (WHERE source_teams='12t') AS c_12t,
            COUNT(DISTINCT db_name) FILTER (WHERE source_roster='flx') AS c_flx,
            COUNT(DISTINCT db_name) FILTER (WHERE source_roster='sflx') AS c_sflx,
            COUNT(DISTINCT db_name) FILTER (WHERE source_roster='idp') AS c_idp,
            COUNT(DISTINCT db_name) FILTER (WHERE source_ppr='std') AS c_std,
            COUNT(DISTINCT db_name) FILTER (WHERE source_ppr='half') AS c_half,
            COUNT(DISTINCT db_name) FILTER (WHERE source_ppr='ppr') AS c_ppr,
            COUNT(DISTINCT db_name) FILTER (WHERE source_td='4pt') AS c_4pt,
            COUNT(DISTINCT db_name) FILTER (WHERE source_td='6pt') AS c_6pt,
            COUNT(DISTINCT db_name) FILTER (WHERE source_league_type='redraft') AS c_redraft,
            COUNT(DISTINCT db_name) FILTER (WHERE source_league_type='dynasty') AS c_dynasty,
            COUNT(DISTINCT db_name) FILTER (WHERE source_lineup_mode='managed') AS c_managed,
            COUNT(DISTINCT db_name) FILTER (WHERE source_lineup_mode='best_ball') AS c_best_ball
          FROM inventory GROUP BY 1,2,3
        ),
        pooled AS (
          SELECT i.*,
            CASE WHEN (i.source_teams='10t' AND c.c_10t >= 150) OR (i.source_teams='12t' AND c.c_12t >= 150) THEN i.source_teams ELSE 'ALL' END AS teams,
            CASE WHEN (i.source_roster='flx' AND c.c_flx >= 150) OR (i.source_roster='sflx' AND c.c_sflx >= 150) OR (i.source_roster='idp' AND c.c_idp >= 150) THEN i.source_roster ELSE 'ALL' END AS roster,
            CASE WHEN (i.source_ppr='std' AND c.c_std >= 150) OR (i.source_ppr='half' AND c.c_half >= 150) OR (i.source_ppr='ppr' AND c.c_ppr >= 150) THEN i.source_ppr ELSE 'ALL' END AS ppr,
            CASE WHEN (i.source_td='4pt' AND c.c_4pt >= 150) OR (i.source_td='6pt' AND c.c_6pt >= 150) THEN i.source_td ELSE 'ALL' END AS td,
            CASE WHEN (i.source_league_type='redraft' AND c.c_redraft >= 150) OR (i.source_league_type='dynasty' AND c.c_dynasty >= 150) THEN i.source_league_type ELSE 'ALL' END AS league_type,
            CASE WHEN (i.source_lineup_mode='managed' AND c.c_managed >= 150) OR (i.source_lineup_mode='best_ball' AND c.c_best_ball >= 150) THEN i.source_lineup_mode ELSE 'ALL' END AS lineup_mode
          FROM inventory i JOIN weekly_counts c USING (year,week,pos_grp)
        ),
        requests(slug, teams, roster, ppr, td) AS (VALUES {slug_values}),
        brackets(bracket) AS (VALUES {bracket_values}),
        eligible AS (
          SELECT r.slug, r.teams, r.roster, r.ppr, r.td, p.year, p.week, p.pos_grp,
                 COUNT(DISTINCT p.db_name) AS n_leagues
          FROM pooled p JOIN requests r ON
             (p.teams='ALL' OR p.teams=r.teams) AND (p.roster='ALL' OR p.roster=r.roster)
             AND (p.ppr='ALL' OR p.ppr=r.ppr) AND (p.td='ALL' OR p.td=r.td)
          GROUP BY ALL
        ),
        cell AS (
          SELECT r.slug, r.teams, r.roster, r.ppr, r.td, b.bracket,
            p.year, p.week, p.player_id, p.pos_grp,
            MAX(p.active_week) AS active_week, MAX(p.bye_week) AS bye_week,
            COUNT(DISTINCT p.db_name) AS rostered_leagues,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1) AS started_leagues,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND p.active_week=1) AS healthy_started_leagues,
            SUM(CASE WHEN p.is_started=1 AND p.active_week=1 THEN COALESCE(p.fantasy_points,0) ELSE 0 END) AS points_started,
            SUM(CASE WHEN p.is_started=1 AND p.active_week=1 THEN COALESCE(p.clutch_equity,0) ELSE 0 END) AS clutch_sum,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND p.outcome_win=1) AS wins_started,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND p.outcome_loss=1) AS losses_started,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND (p.outcome_win=1 OR p.outcome_loss=1 OR p.outcome_tie=1)) AS valid_outcome_started,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND p.source_bracket=b.bracket AND p.playoff_signal=1) AS playoff_started,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.is_started=1 AND p.source_bracket=b.bracket AND p.champion_signal=1) AS champ_started,
            MAX(e.n_leagues) AS n_leagues
          FROM player p
          JOIN pooled po ON po.db_name=p.db_name AND po.year=p.year AND po.week=p.week AND po.pos_grp=p.pos_grp
            AND po.source_teams=p.source_teams AND po.source_roster=p.source_roster
            AND po.source_ppr=p.source_ppr AND po.source_td=p.source_td
            AND po.source_bracket=p.source_bracket AND po.source_league_type=p.source_league_type
            AND po.source_lineup_mode=p.source_lineup_mode
          JOIN requests r ON (po.teams='ALL' OR po.teams=r.teams) AND (po.roster='ALL' OR po.roster=r.roster)
            AND (po.ppr='ALL' OR po.ppr=r.ppr) AND (po.td='ALL' OR po.td=r.td)
          CROSS JOIN brackets b
          LEFT JOIN eligible e ON e.slug=r.slug AND e.year=p.year AND e.week=p.week AND e.pos_grp=p.pos_grp
          GROUP BY ALL
        )
        SELECT c.player_id AS NFL_player_id, c.year, c.week, c.slug,
          c.teams, c.roster, c.ppr, c.td, c.bracket,
          '4' AS cohort_level, CASE WHEN c.n_leagues >= 35 THEN 'confident' ELSE 'mushy' END AS confidence,
          c.n_leagues, c.rostered_leagues, c.started_leagues, c.healthy_started_leagues,
          c.points_started, c.clutch_sum, c.wins_started, c.losses_started,
          c.valid_outcome_started, c.playoff_started, c.champ_started,
          c.active_week, c.bye_week,
          100.0*c.rostered_leagues/NULLIF(c.n_leagues,0) AS roster_rate_pct,
          100.0*c.started_leagues/NULLIF(c.n_leagues,0) AS start_rate_pct,
          100.0*c.healthy_started_leagues/NULLIF(c.n_leagues,0) AS healthy_start_rate_pct,
          c.points_started/NULLIF(c.started_leagues,0) AS ppg_when_started,
          c.points_started AS total_points_observed,
          100.0*c.wins_started/NULLIF(c.valid_outcome_started,0) AS win_rate_pct,
          100.0*c.wins_started/NULLIF(c.n_leagues,0) AS won_pct,
          100.0*c.losses_started/NULLIF(c.n_leagues,0) AS lost_pct,
          c.started_leagues/NULLIF(c.n_leagues,0)*c.wins_started/NULLIF(c.valid_outcome_started,0) AS expected_wins,
          c.started_leagues/NULLIF(c.n_leagues,0)*(1.0-c.wins_started/NULLIF(c.valid_outcome_started,0)) AS expected_losses,
          CAST(c.started_leagues AS DOUBLE) AS expected_starts,
          c.clutch_sum/NULLIF(c.started_leagues,0) AS avg_clutch_started,
          c.n_leagues AS healthy_eligible_league_weeks, c.n_leagues AS eligible_leagues,
          c.n_leagues AS champ_eligible, c.n_leagues AS champ_eligible_leagues,
          c.active_week AS nfl_active_weeks, CASE WHEN c.bye_week=0 AND c.active_week=0 THEN 1 ELSE 0 END AS inactive_weeks,
          CASE WHEN c.started_leagues>0 THEN 1 ELSE 0 END AS started_weeks,
          c.playoff_started AS playoff_started_leagues, c.champ_started AS champ_started_leagues,
          c.playoff_started AS n_started_po, c.champ_started AS n_champ_start_leagues,
          c.n_leagues AS playoff_eligible_leagues, c.n_leagues AS n_champ_leagues,
          c.n_leagues AS n_final_po, c.n_leagues AS po_n_rostered_leagues,
          c.playoff_started AS po_wkwt_credit,
          c.champ_started AS started_champ_active,
          c.clutch_sum AS sum_clutch_started_active,
          c.started_leagues AS started_team_game_weeks,
          c.n_leagues AS team_game_eligible_league_weeks,
          c.healthy_started_leagues AS started_active_weeks,
          c.n_leagues AS rostered_league_weeks,
          c.n_leagues AS roster_eligible_league_weeks
        FROM cell c
        """
        con.execute(sql)
        con.execute("CREATE OR REPLACE TEMP TABLE weekly_stage AS SELECT * FROM matchup_weekly")
        # Season ledger: one row per player/year/slug/bracket.  The wide bundle's
        # career builder consumes these additive fields, so career remains a sum of
        # seasons rather than a second interpretation of weekly rows.
        con.execute("""
        CREATE OR REPLACE TABLE matchup AS
        WITH w AS (SELECT * FROM weekly_stage),
        s AS (
          SELECT NFL_player_id, year, slug,
            ANY_VALUE(teams) AS teams, ANY_VALUE(roster) AS roster,
            ANY_VALUE(ppr) AS ppr, ANY_VALUE(td) AS td, bracket,
            MAX(cohort_level) AS cohort_level, MAX(confidence) AS confidence,
            MAX(n_leagues) AS n_leagues,
            SUM(rostered_league_weeks) AS rostered_league_weeks,
            SUM(roster_eligible_league_weeks) AS roster_eligible_league_weeks,
            SUM(started_team_game_weeks) AS started_team_game_weeks,
            SUM(team_game_eligible_league_weeks) AS team_game_eligible_league_weeks,
            SUM(started_active_weeks) AS started_active_weeks,
            SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
            SUM(started_leagues) AS started_leagues,
            SUM(expected_wins) AS expected_wins, SUM(expected_losses) AS expected_losses,
            SUM(expected_starts) AS expected_starts,
            SUM(sum_clutch_started_active/NULLIF(started_leagues,0)) AS sum_clutch_started_active,
            SUM(points_started) AS total_points_observed,
            SUM(points_started)/NULLIF(SUM(started_leagues),0) AS ppg_when_started,
            SUM(wins_started) AS wins_started, SUM(losses_started) AS losses_started,
            COUNT(DISTINCT week) FILTER (WHERE active_week=1) AS active_weeks,
            COUNT(DISTINCT week) FILTER (WHERE active_week=0 AND bye_week=0) AS inactive_weeks,
            COUNT(DISTINCT week) FILTER (WHERE started_leagues>0) AS started_weeks,
            SUM(n_leagues) AS elig_league_weeks,
            SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks_sum,
            MAX(champ_eligible_leagues) AS champ_elig_leagues,
            SUM(started_champ_active) AS started_champ_active,
            MAX(n_champ_leagues) AS n_champ_leagues,
            SUM(n_champ_start_leagues) AS n_champ_start_leagues,
            MAX(playoff_eligible_leagues) AS playoff_eligible_leagues,
            MAX(n_final_po) AS n_final_po, SUM(n_started_po) AS n_started_po,
            SUM(po_wkwt_credit) AS po_wkwt_credit,
            SUM(expected_wins) AS expected_wins_sum, SUM(expected_losses) AS expected_losses_sum
          FROM w GROUP BY 1,2,3,8
        )
        SELECT *,
          100.0*rostered_league_weeks/NULLIF(roster_eligible_league_weeks,0) AS roster_rate_pct,
          100.0*started_team_game_weeks/NULLIF(team_game_eligible_league_weeks,0) AS start_rate_pct,
          100.0*started_active_weeks/NULLIF(healthy_eligible_league_weeks_sum,0) AS healthy_start_rate_pct,
          100.0*expected_wins/NULLIF(expected_starts,0) AS win_rate_pct,
          100.0*expected_wins/NULLIF(team_game_eligible_league_weeks,0) AS won_pct,
          100.0*expected_losses/NULLIF(team_game_eligible_league_weeks,0) AS lost_pct,
          expected_starts AS expected_starts,
          sum_clutch_started_active AS avg_clutch_started,
          100.0*started_champ_active/NULLIF(champ_elig_leagues,0) AS champ_week_rate_pct,
          100.0*po_wkwt_credit/NULLIF(playoff_eligible_leagues,0) AS playoff_rate_wkwt,
          100.0*n_final_po/NULLIF(playoff_eligible_leagues,0) AS playoff_total_pct,
          100.0*n_started_po/NULLIF(playoff_eligible_leagues,0) AS playoff_as_starter_pct,
          100.0*n_champ_leagues/NULLIF(champ_elig_leagues,0) AS champ_total_pct,
          100.0*n_champ_start_leagues/NULLIF(champ_elig_leagues,0) AS champ_as_starter_pct,
          active_weeks AS nfl_active_weeks,
          expected_wins AS expected_wins,
          expected_losses AS expected_losses,
          started_leagues AS started_team_game_weeks_sum,
          champ_elig_leagues AS champ_eligible_leagues,
          n_champ_start_leagues AS started_champ_active_sum
        FROM s
        """)
        # Keep both normalized grains.  build_wide_bundle pivots these into the
        # final one-row-per-player tables.
        con.execute("CREATE OR REPLACE TABLE matchup_weekly AS SELECT * FROM weekly_stage WHERE active_week=1")
        con.execute("CREATE OR REPLACE TABLE matchup_career AS SELECT * FROM matchup WHERE FALSE")
    finally:
        con.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--ops-cache", type=Path, required=True)
    p.add_argument("--source-db", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--year-start", type=int, required=True)
    p.add_argument("--year-end", type=int, required=True)
    p.add_argument("--player-id")
    p.add_argument("--week", type=int)
    a = p.parse_args()
    build_source(a.snapshot, a.ops_cache, a.source_db, a.year_start, a.year_end, a.player_id, a.week)
    os.environ["RESEARCH_OUT_DIR"] = str(a.out_dir)
    os.environ["RESEARCH_OPS_CACHE_PATH"] = str(a.ops_cache)
    os.environ["RESEARCH_WIDE_DATASETS"] = "matchup"
    from build_wide_bundle import build_bundle
    build_bundle(only_datasets={"matchup"})


if __name__ == "__main__":
    main()
