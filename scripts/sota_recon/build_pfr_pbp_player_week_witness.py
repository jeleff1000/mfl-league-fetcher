"""Build the independent PFR play-by-play player-week witness lane.

This is intentionally a witness artifact, not a replacement for the canonical
PBP rollup.  It uses only the local PFR box-score PBP capture, PFR team-game
calendar, PFR player-offense box, and the local identity anchor.  The lane is
kept separate so the quorum code can count it as the ``pfr`` root rather than
mistaking it for another nflverse/PBP rollup.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from . import sources as S


OUT = Path(S.DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "pfr_pbp_player_week_passing.parquet"


def build(output: Path = OUT) -> Path:
    pbp = Path(S.registry()["pfr_box_pbp"].path).as_posix()
    team_games = Path(S.registry()["pfr_team_games"].path).as_posix()
    offense = Path(S.registry()["pfr_player_offense_box"].path).as_posix()
    bio = Path(S.registry()["player_bio"].path).as_posix()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    # PFR's first linked player on a passing description is the passer.  The
    # offense box supplies the passer's team, which lets the PFR team-game
    # calendar supply the week and the location string be interpreted from the
    # offense's perspective.
    sql = f"""
    WITH games AS (
        SELECT boxscore_id, team_code, week, season_type
        FROM read_parquet('{team_games}')
        WHERE season_type = 'REG'
        GROUP BY 1, 2, 3, 4
    ), plays AS (
        SELECT p.*,
               regexp_extract(CAST(p.detail_link_ids AS VARCHAR), '^([^;,]+)', 1) AS pfr_id,
               regexp_extract(CAST(p.location AS VARCHAR), '^([^ ]+)', 1) AS location_team,
               TRY_CAST(regexp_extract(CAST(p.location AS VARCHAR), ' ([0-9]+)$', 1) AS INTEGER) AS location_yard
        FROM read_parquet('{pbp}') p
        WHERE p.detail_link_ids IS NOT NULL
    ), attributed AS (
        SELECT
            pl.pfr_id,
            bio.NFL_player_id,
            pl.season AS year,
            g.week,
            g.season_type,
            pl.detail,
            pl.location_team,
            pl.location_yard,
            o.team,
            TRY_CAST(pl.exp_pts_after AS DOUBLE) - TRY_CAST(pl.exp_pts_before AS DOUBLE) AS epa,
            CASE
                WHEN pl.detail NOT LIKE '%no play%'
                 AND (pl.detail LIKE '% pass complete %'
                   OR pl.detail LIKE '% pass incomplete %'
                   OR pl.detail LIKE '% pass intercepted %'
                   OR pl.detail LIKE '% is intercepted %')
                THEN 1 ELSE 0
            END AS pass_attempt,
            CASE
                WHEN pl.detail NOT LIKE '%no play%'
                 AND pl.detail LIKE '% sacked %'
                THEN 1 ELSE 0
            END AS sack,
            CASE
                WHEN pl.detail LIKE '% pass complete %'
                 AND pl.detail NOT LIKE '%no play%'
                THEN 1 ELSE 0
            END AS complete_pass,
            TRY_CAST(regexp_extract(pl.detail, ' for (-?[0-9]+) yards', 1) AS INTEGER) AS passing_yards
        FROM plays pl
        JOIN read_parquet('{offense}') o
          ON o.boxscore_id = pl.boxscore_id AND o.player_link_ids = pl.pfr_id
        JOIN games g
          ON g.boxscore_id = pl.boxscore_id AND g.team_code = o.team
        JOIN read_parquet('{bio}') bio ON bio.pfr_id = pl.pfr_id
        WHERE bio.NFL_player_id IS NOT NULL
    )
    , box_counts AS (
        SELECT bio.NFL_player_id,
               g.year,
               g.week,
               SUM(COALESCE(TRY_CAST(o.pass_att AS DOUBLE), 0)
                   + COALESCE(TRY_CAST(o.pass_sacked AS DOUBLE), 0)) AS pass_success_plays_box
        FROM read_parquet('{offense}') o
        JOIN read_parquet('{team_games}') g
          ON g.boxscore_id = o.boxscore_id AND g.team_code = o.team
        JOIN read_parquet('{bio}') bio ON bio.pfr_id = o.player_link_ids
        WHERE g.season_type = 'REG'
        GROUP BY 1, 2, 3
    )
    SELECT
        a.pfr_id,
        a.NFL_player_id,
        a.year,
        a.week,
        a.season_type,
        SUM(CASE WHEN a.complete_pass = 1 AND a.passing_yards >= 20 THEN 1 ELSE 0 END)::DOUBLE AS pass_explosive_20,
        SUM(CASE WHEN (a.pass_attempt = 1 OR a.sack = 1) AND a.epa > 0 THEN 1 ELSE 0 END)::DOUBLE AS pass_success,
        SUM(CASE WHEN (a.pass_attempt = 1 OR a.sack = 1) AND a.epa IS NOT NULL THEN 1 ELSE 0 END)::DOUBLE AS pass_success_plays,
        ANY_VALUE(b.pass_success_plays_box)::DOUBLE AS pass_success_plays_box,
        SUM(CASE WHEN a.pass_attempt = 1
                  AND ((a.location_team = a.team AND a.location_yard >= 80)
                    OR (a.location_team <> a.team AND a.location_yard <= 20))
                 THEN 1 ELSE 0 END)::DOUBLE AS rz_pass_att,
        SUM(CASE WHEN (a.pass_attempt = 1 OR a.sack = 1) THEN a.epa ELSE 0 END)::DOUBLE AS passing_epa
    FROM attributed a
    LEFT JOIN box_counts b USING (NFL_player_id, year, week)
    GROUP BY 1, 2, 3, 4, 5
    """
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY ({sql}) TO '{output.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()
    print(build(args.output))
