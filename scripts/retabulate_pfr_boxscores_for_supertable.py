"""Retabulate scraped PFR boxscore tables into audit-ready player-game facts.

This script does not modify Fly. It turns the compact PFR boxscore scrape into
small local parquet layers that can be compared against the live supertable:

* pfr_game_team_dim.parquet - one row per game/team side.
* pfr_player_game_fact.parquet - one row per player/team/game with boxscore stats.
* pfr_player_game_position_hint.parquet - starter/snap/position hints.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


DEFAULT_BASE_DIR = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\pfr_boxscores")
DEFAULT_OUTPUT_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\_catalog")


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def run_scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    value = con.execute(sql).fetchone()[0]
    return int(value or 0)


def build_game_team_dim(con: duckdb.DuckDBPyConnection, base_dir: Path, out_dir: Path) -> Path:
    team_games = sql_path(base_dir / "team_games_raw.parquet")
    out_path = out_dir / "pfr_game_team_dim.parquet"
    con.execute(
        f"""
        COPY (
            WITH raw AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(boxscore_url AS VARCHAR) AS boxscore_url,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    TRY_CAST(week_num AS INTEGER) AS pfr_week_num,
                    CAST(team_name_abbr AS VARCHAR) AS team,
                    NULLIF(CAST(opp_name_abbr AS VARCHAR), '') AS opponent,
                    NULLIF(CAST(game_location AS VARCHAR), '') AS game_location,
                    CAST(game_result AS VARCHAR) AS game_result,
                    CAST(home_stathead_id AS VARCHAR) AS home_stathead_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY boxscore_id, team_name_abbr
                        ORDER BY source_page_index, row_index_in_table
                    ) AS rn
                FROM read_parquet('{team_games}')
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(team_name_abbr AS VARCHAR), '') IS NOT NULL
            )
            , home_id_season_counts AS (
                SELECT
                    lower(home_stathead_id) AS home_stathead_id,
                    season,
                    team,
                    COUNT(*) AS n
                FROM raw
                WHERE game_location IS NULL
                  AND lower(home_stathead_id) IS NOT NULL
                  AND season IS NOT NULL
                  AND team IS NOT NULL
                GROUP BY 1, 2, 3
            )
            , home_id_season_map AS (
                SELECT home_stathead_id, season, team AS mapped_home_team
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY home_stathead_id, season
                            ORDER BY n DESC, team
                        ) AS rn_home_id
                    FROM home_id_season_counts
                )
                WHERE rn_home_id = 1
            )
            , home_id_all_counts AS (
                SELECT
                    lower(home_stathead_id) AS home_stathead_id,
                    team,
                    COUNT(*) AS n
                FROM raw
                WHERE game_location IS NULL
                  AND lower(home_stathead_id) IS NOT NULL
                  AND team IS NOT NULL
                GROUP BY 1, 2
            )
            , home_id_all_map AS (
                SELECT home_stathead_id, team AS mapped_home_team
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY home_stathead_id
                            ORDER BY n DESC, team
                        ) AS rn_home_id
                    FROM home_id_all_counts
                )
                WHERE rn_home_id = 1
            )
            SELECT
                r.boxscore_id,
                r.boxscore_url,
                r.game_date,
                r.season,
                r.pfr_week_num,
                r.team,
                r.opponent,
                CASE
                    WHEN r.game_location = '@' THEN 0
                    WHEN r.game_location IS NULL THEN 1
                    WHEN r.game_location = 'N'
                     AND r.team = COALESCE(s.mapped_home_team, a.mapped_home_team) THEN 1
                    WHEN r.game_location = 'N' THEN 0
                    ELSE NULL
                END AS is_home,
                CASE WHEN r.game_location = 'N' THEN 1 ELSE 0 END AS is_neutral,
                lower(r.home_stathead_id) AS home_stathead_id,
                r.game_result
            FROM raw r
            LEFT JOIN home_id_season_map s
              ON lower(r.home_stathead_id) = s.home_stathead_id
             AND r.season = s.season
            LEFT JOIN home_id_all_map a
              ON lower(r.home_stathead_id) = a.home_stathead_id
            WHERE r.rn = 1
            ORDER BY game_date, boxscore_id, team
        ) TO '{sql_path(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return out_path


def build_player_game_fact(con: duckdb.DuckDBPyConnection, base_dir: Path, out_dir: Path) -> Path:
    tables_dir = base_dir / "tables"
    game_dim = sql_path(out_dir / "pfr_game_team_dim.parquet")
    out_path = out_dir / "pfr_player_game_fact.parquet"

    paths = {
        "offense": sql_path(tables_dir / "player_offense" / "_combined.parquet"),
        "defense": sql_path(tables_dir / "player_defense" / "_combined.parquet"),
        "returns": sql_path(tables_dir / "returns" / "_combined.parquet"),
        "kicking": sql_path(tables_dir / "kicking" / "_combined.parquet"),
    }

    con.execute(
        f"""
        COPY (
            WITH offense AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    COALESCE(
                        'pfr:' || NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), ''),
                        'name:' || lower(regexp_replace(CAST(player AS VARCHAR), '[^a-zA-Z0-9]+', '', 'g'))
                    ) AS pfr_player_game_key,
                    NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(pass_cmp AS DOUBLE)) AS completions,
                    SUM(TRY_CAST(pass_att AS DOUBLE)) AS attempts,
                    SUM(TRY_CAST(pass_yds AS DOUBLE)) AS passing_yards,
                    SUM(TRY_CAST(pass_td AS DOUBLE)) AS passing_tds,
                    SUM(TRY_CAST(pass_int AS DOUBLE)) AS passing_interceptions,
                    SUM(TRY_CAST(pass_sacked AS DOUBLE)) AS sacks_suffered,
                    SUM(TRY_CAST(pass_sacked_yds AS DOUBLE)) AS sack_yards_lost,
                    MAX(TRY_CAST(pass_long AS DOUBLE)) AS passing_long,
                    SUM(TRY_CAST(rush_att AS DOUBLE)) AS carries,
                    SUM(TRY_CAST(rush_yds AS DOUBLE)) AS rushing_yards,
                    SUM(TRY_CAST(rush_td AS DOUBLE)) AS rushing_tds,
                    MAX(TRY_CAST(rush_long AS DOUBLE)) AS rushing_long,
                    SUM(TRY_CAST(rec AS DOUBLE)) AS receptions,
                    SUM(TRY_CAST(rec_yds AS DOUBLE)) AS receiving_yards,
                    SUM(TRY_CAST(rec_td AS DOUBLE)) AS receiving_tds,
                    MAX(TRY_CAST(rec_long AS DOUBLE)) AS receiving_long,
                    SUM(TRY_CAST(targets AS DOUBLE)) AS targets,
                    SUM(TRY_CAST(fumbles AS DOUBLE)) AS fumbles,
                    SUM(TRY_CAST(fumbles_lost AS DOUBLE)) AS fumbles_lost
                FROM read_parquet('{paths["offense"]}')
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(player AS VARCHAR), '') IS NOT NULL
                GROUP BY 1,2,3,4,5,6
            ),
            defense AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    COALESCE(
                        'pfr:' || NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), ''),
                        'name:' || lower(regexp_replace(CAST(player AS VARCHAR), '[^a-zA-Z0-9]+', '', 'g'))
                    ) AS pfr_player_game_key,
                    NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(def_int AS DOUBLE)) AS def_interceptions,
                    SUM(TRY_CAST(def_int_yds AS DOUBLE)) AS def_interception_yards,
                    SUM(TRY_CAST(def_int_td AS DOUBLE)) AS def_tds,
                    SUM(TRY_CAST(sacks AS DOUBLE)) AS def_sacks,
                    SUM(TRY_CAST(tackles_combined AS DOUBLE)) AS def_tackles_with_assist,
                    SUM(TRY_CAST(tackles_solo AS DOUBLE)) AS def_tackles_solo,
                    SUM(TRY_CAST(tackles_assists AS DOUBLE)) AS def_tackle_assists,
                    SUM(TRY_CAST(fumbles_rec AS DOUBLE)) AS fum_rec,
                    SUM(TRY_CAST(fumbles_rec_yds AS DOUBLE)) AS fum_rec_yds,
                    SUM(TRY_CAST(fumbles_rec_td AS DOUBLE)) AS fum_ret_td,
                    SUM(TRY_CAST(fumbles_forced AS DOUBLE)) AS def_fumbles_forced,
                    SUM(TRY_CAST(pass_defended AS DOUBLE)) AS def_pass_defended,
                    SUM(TRY_CAST(tackles_loss AS DOUBLE)) AS def_tackles_for_loss,
                    SUM(TRY_CAST(qb_hits AS DOUBLE)) AS def_qb_hits
                FROM read_parquet('{paths["defense"]}')
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(player AS VARCHAR), '') IS NOT NULL
                GROUP BY 1,2,3,4,5,6
            ),
            returns AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    COALESCE(
                        'pfr:' || NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), ''),
                        'name:' || lower(regexp_replace(CAST(player AS VARCHAR), '[^a-zA-Z0-9]+', '', 'g'))
                    ) AS pfr_player_game_key,
                    NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(kick_ret AS DOUBLE)) AS kickoff_returns,
                    SUM(TRY_CAST(kick_ret_yds AS DOUBLE)) AS kickoff_return_yards,
                    SUM(TRY_CAST(kick_ret_td AS DOUBLE)) AS kickoff_return_tds,
                    MAX(TRY_CAST(kick_ret_long AS DOUBLE)) AS kickoff_return_long,
                    SUM(TRY_CAST(punt_ret AS DOUBLE)) AS punt_returns,
                    SUM(TRY_CAST(punt_ret_yds AS DOUBLE)) AS punt_return_yards,
                    SUM(TRY_CAST(punt_ret_td AS DOUBLE)) AS punt_return_tds,
                    MAX(TRY_CAST(punt_ret_long AS DOUBLE)) AS punt_return_long
                FROM read_parquet('{paths["returns"]}')
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(player AS VARCHAR), '') IS NOT NULL
                GROUP BY 1,2,3,4,5,6
            ),
            kicking AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    COALESCE(
                        'pfr:' || NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), ''),
                        'name:' || lower(regexp_replace(CAST(player AS VARCHAR), '[^a-zA-Z0-9]+', '', 'g'))
                    ) AS pfr_player_game_key,
                    NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(xpm AS DOUBLE)) AS pat_made,
                    SUM(TRY_CAST(xpa AS DOUBLE)) AS pat_att,
                    SUM(TRY_CAST(fgm AS DOUBLE)) AS fg_made,
                    SUM(TRY_CAST(fga AS DOUBLE)) AS fg_att,
                    SUM(TRY_CAST(punt AS DOUBLE)) AS punts,
                    SUM(TRY_CAST(punt_yds AS DOUBLE)) AS punt_yards,
                    MAX(TRY_CAST(punt_long AS DOUBLE)) AS punt_long
                FROM read_parquet('{paths["kicking"]}')
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(player AS VARCHAR), '') IS NOT NULL
                GROUP BY 1,2,3,4,5,6
            ),
            keys AS (
                SELECT boxscore_id, team, pfr_player_game_key FROM offense
                UNION SELECT boxscore_id, team, pfr_player_game_key FROM defense
                UNION SELECT boxscore_id, team, pfr_player_game_key FROM returns
                UNION SELECT boxscore_id, team, pfr_player_game_key FROM kicking
            )
            SELECT
                k.boxscore_id,
                COALESCE(o.game_date, d.game_date, r.game_date, pk.game_date) AS game_date,
                COALESCE(o.season, d.season, r.season, pk.season) AS season,
                k.team,
                k.pfr_player_game_key,
                COALESCE(o.pfr_id, d.pfr_id, r.pfr_id, pk.pfr_id) AS pfr_id,
                COALESCE(o.player, d.player, r.player, pk.player) AS player,
                trim(both ';' from
                    (CASE WHEN o.pfr_player_game_key IS NOT NULL THEN 'player_offense;' ELSE '' END) ||
                    (CASE WHEN d.pfr_player_game_key IS NOT NULL THEN 'player_defense;' ELSE '' END) ||
                    (CASE WHEN r.pfr_player_game_key IS NOT NULL THEN 'returns;' ELSE '' END) ||
                    (CASE WHEN pk.pfr_player_game_key IS NOT NULL THEN 'kicking;' ELSE '' END)
                ) AS source_tables,
                COALESCE(o.completions, 0) AS completions,
                COALESCE(o.attempts, 0) AS attempts,
                COALESCE(o.passing_yards, 0) AS passing_yards,
                COALESCE(o.passing_tds, 0) AS passing_tds,
                COALESCE(o.passing_interceptions, 0) AS passing_interceptions,
                COALESCE(o.sacks_suffered, 0) AS sacks_suffered,
                COALESCE(o.sack_yards_lost, 0) AS sack_yards_lost,
                o.passing_long,
                COALESCE(o.carries, 0) AS carries,
                COALESCE(o.rushing_yards, 0) AS rushing_yards,
                COALESCE(o.rushing_tds, 0) AS rushing_tds,
                o.rushing_long,
                COALESCE(o.receptions, 0) AS receptions,
                COALESCE(o.receiving_yards, 0) AS receiving_yards,
                COALESCE(o.receiving_tds, 0) AS receiving_tds,
                o.receiving_long,
                COALESCE(o.targets, 0) AS targets,
                COALESCE(o.fumbles, 0) AS fumbles,
                COALESCE(o.fumbles_lost, 0) AS fumbles_lost,
                COALESCE(d.def_interceptions, 0) AS def_interceptions,
                COALESCE(d.def_interception_yards, 0) AS def_interception_yards,
                COALESCE(d.def_tds, 0) AS def_tds,
                COALESCE(d.def_sacks, 0) AS def_sacks,
                COALESCE(d.def_tackles_with_assist, 0) AS def_tackles_with_assist,
                COALESCE(d.def_tackles_solo, 0) AS def_tackles_solo,
                COALESCE(d.def_tackle_assists, 0) AS def_tackle_assists,
                COALESCE(d.fum_rec, 0) AS fum_rec,
                COALESCE(d.fum_rec_yds, 0) AS fum_rec_yds,
                COALESCE(d.fum_ret_td, 0) AS fum_ret_td,
                COALESCE(d.def_fumbles_forced, 0) AS def_fumbles_forced,
                COALESCE(d.def_pass_defended, 0) AS def_pass_defended,
                COALESCE(d.def_tackles_for_loss, 0) AS def_tackles_for_loss,
                COALESCE(d.def_qb_hits, 0) AS def_qb_hits,
                COALESCE(r.kickoff_returns, 0) AS kickoff_returns,
                COALESCE(r.kickoff_return_yards, 0) AS kickoff_return_yards,
                COALESCE(r.kickoff_return_tds, 0) AS kickoff_return_tds,
                r.kickoff_return_long,
                COALESCE(r.punt_returns, 0) AS punt_returns,
                COALESCE(r.punt_return_yards, 0) AS punt_return_yards,
                COALESCE(r.punt_return_tds, 0) AS punt_return_tds,
                r.punt_return_long,
                COALESCE(pk.pat_made, 0) AS pat_made,
                COALESCE(pk.pat_att, 0) AS pat_att,
                COALESCE(pk.fg_made, 0) AS fg_made,
                COALESCE(pk.fg_att, 0) AS fg_att,
                COALESCE(pk.punts, 0) AS punts,
                COALESCE(pk.punt_yards, 0) AS punt_yards,
                pk.punt_long,
                g.pfr_week_num,
                g.opponent,
                g.is_home,
                g.is_neutral,
                g.game_result,
                CASE WHEN COALESCE(o.pfr_id, d.pfr_id, r.pfr_id, pk.pfr_id) IS NULL THEN 1 ELSE 0 END AS missing_pfr_id
            FROM keys k
            LEFT JOIN offense o USING (boxscore_id, team, pfr_player_game_key)
            LEFT JOIN defense d USING (boxscore_id, team, pfr_player_game_key)
            LEFT JOIN returns r USING (boxscore_id, team, pfr_player_game_key)
            LEFT JOIN kicking pk USING (boxscore_id, team, pfr_player_game_key)
            LEFT JOIN read_parquet('{game_dim}') g
              ON k.boxscore_id = g.boxscore_id
             AND k.team = g.team
            ORDER BY game_date, k.boxscore_id, k.team, player
        ) TO '{sql_path(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return out_path


def build_position_hints(con: duckdb.DuckDBPyConnection, base_dir: Path, out_dir: Path) -> Path:
    tables_dir = base_dir / "tables"
    game_dim = sql_path(out_dir / "pfr_game_team_dim.parquet")
    out_path = out_dir / "pfr_player_game_position_hint.parquet"
    paths = {
        "home_starters": sql_path(tables_dir / "home_starters" / "_combined.parquet"),
        "vis_starters": sql_path(tables_dir / "vis_starters" / "_combined.parquet"),
        "home_snaps": sql_path(tables_dir / "home_snap_counts" / "_combined.parquet"),
        "vis_snaps": sql_path(tables_dir / "vis_snap_counts" / "_combined.parquet"),
    }
    con.execute(
        f"""
        COPY (
            WITH side AS (
                SELECT
                    boxscore_id,
                    max(CASE WHEN is_home = 1 THEN team END) AS home_team,
                    max(CASE WHEN is_home = 0 THEN team END) AS vis_team,
                    max(is_neutral) AS is_neutral
                FROM read_parquet('{game_dim}')
                GROUP BY boxscore_id
            ),
            raw AS (
                SELECT h.boxscore_id, h.game_date, h.season, s.home_team AS side_team, h.player, h.player_urls, h.pos,
                       1 AS started, NULL::DOUBLE AS offense_snaps, NULL::VARCHAR AS offense_pct,
                       NULL::DOUBLE AS defense_snaps, NULL::VARCHAR AS defense_pct,
                       NULL::DOUBLE AS st_snaps, NULL::VARCHAR AS st_pct,
                       CAST(h.table_caption AS VARCHAR) AS table_caption,
                       s.is_neutral,
                       'home_starters' AS source_table
                FROM read_parquet('{paths["home_starters"]}') h
                LEFT JOIN side s USING (boxscore_id)
                UNION ALL
                SELECT v.boxscore_id, v.game_date, v.season, s.vis_team AS side_team, v.player, v.player_urls, v.pos,
                       1, NULL::DOUBLE, NULL::VARCHAR, NULL::DOUBLE, NULL::VARCHAR, NULL::DOUBLE, NULL::VARCHAR,
                       CAST(v.table_caption AS VARCHAR),
                       s.is_neutral,
                       'vis_starters'
                FROM read_parquet('{paths["vis_starters"]}') v
                LEFT JOIN side s USING (boxscore_id)
                UNION ALL
                SELECT h.boxscore_id, h.game_date, h.season, s.home_team AS side_team, h.player, h.player_urls, h.pos,
                       0, TRY_CAST(h.offense AS DOUBLE), CAST(h.off_pct AS VARCHAR),
                       TRY_CAST(h.defense AS DOUBLE), CAST(h.def_pct AS VARCHAR),
                       TRY_CAST(h.special_teams AS DOUBLE), CAST(h.st_pct AS VARCHAR),
                       CAST(h.table_caption AS VARCHAR),
                       s.is_neutral,
                       'home_snap_counts'
                FROM read_parquet('{paths["home_snaps"]}') h
                LEFT JOIN side s USING (boxscore_id)
                UNION ALL
                SELECT v.boxscore_id, v.game_date, v.season, s.vis_team AS side_team, v.player, v.player_urls, v.pos,
                       0, TRY_CAST(v.offense AS DOUBLE), CAST(v.off_pct AS VARCHAR),
                       TRY_CAST(v.defense AS DOUBLE), CAST(v.def_pct AS VARCHAR),
                       TRY_CAST(v.special_teams AS DOUBLE), CAST(v.st_pct AS VARCHAR),
                       CAST(v.table_caption AS VARCHAR),
                       s.is_neutral,
                       'vis_snap_counts'
                FROM read_parquet('{paths["vis_snaps"]}') v
                LEFT JOIN side s USING (boxscore_id)
            ),
            captioned AS (
                SELECT
                    *,
                    lower(NULLIF(regexp_replace(
                        regexp_replace(CAST(table_caption AS VARCHAR), '\\s+Starters\\s+Table$', ''),
                        '\\s+Snap\\s+Counts\\s+Table$',
                        ''
                    ), '')) AS caption_key
                FROM raw
            ),
            caption_team_season_counts AS (
                SELECT
                    season,
                    caption_key,
                    side_team AS team,
                    COUNT(*) AS n
                FROM captioned
                WHERE COALESCE(is_neutral, 0) = 0
                  AND caption_key IS NOT NULL
                  AND side_team IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            caption_team_season_map AS (
                SELECT season, caption_key, team AS mapped_caption_team
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY season, caption_key
                            ORDER BY n DESC, team
                        ) AS rn_caption
                    FROM caption_team_season_counts
                )
                WHERE rn_caption = 1
            ),
            caption_team_all_counts AS (
                SELECT
                    caption_key,
                    side_team AS team,
                    COUNT(*) AS n
                FROM captioned
                WHERE COALESCE(is_neutral, 0) = 0
                  AND caption_key IS NOT NULL
                  AND side_team IS NOT NULL
                GROUP BY 1, 2
            ),
            caption_team_all_map AS (
                SELECT caption_key, team AS mapped_caption_team
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY caption_key
                            ORDER BY n DESC, team
                        ) AS rn_caption
                    FROM caption_team_all_counts
                )
                WHERE rn_caption = 1
            ),
            resolved AS (
                SELECT
                    c.*,
                    COALESCE(sm.mapped_caption_team, am.mapped_caption_team, c.side_team) AS team
                FROM captioned c
                LEFT JOIN caption_team_season_map sm
                  ON c.season = sm.season
                 AND c.caption_key = sm.caption_key
                LEFT JOIN caption_team_all_map am
                  ON c.caption_key = am.caption_key
            ),
            cleaned AS (
                SELECT
                    CAST(boxscore_id AS VARCHAR) AS boxscore_id,
                    CAST(game_date AS DATE) AS game_date,
                    TRY_CAST(season AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    CAST(player AS VARCHAR) AS player,
                    NULLIF(regexp_extract(CAST(player_urls AS VARCHAR), '/players/[^/]+/([^/.]+)\\.htm', 1), '') AS pfr_id,
                    lower(regexp_replace(CAST(player AS VARCHAR), '[^a-zA-Z0-9]+', '', 'g')) AS player_name_norm,
                    NULLIF(CAST(pos AS VARCHAR), '') AS pos,
                    started,
                    offense_snaps,
                    offense_pct,
                    defense_snaps,
                    defense_pct,
                    st_snaps,
                    st_pct,
                    source_table
                FROM resolved
                WHERE NULLIF(CAST(boxscore_id AS VARCHAR), '') IS NOT NULL
                  AND NULLIF(CAST(player AS VARCHAR), '') IS NOT NULL
            )
            SELECT
                boxscore_id,
                any_value(game_date) AS game_date,
                any_value(season) AS season,
                team,
                COALESCE('pfr:' || pfr_id, 'name:' || player_name_norm) AS pfr_player_game_key,
                pfr_id,
                any_value(player) AS player,
                string_agg(DISTINCT pos, ';' ORDER BY pos) FILTER (WHERE pos IS NOT NULL) AS pos_hint,
                max(started) AS started,
                max(offense_snaps) AS offense_snaps,
                any_value(offense_pct) FILTER (WHERE offense_pct IS NOT NULL) AS offense_pct,
                max(defense_snaps) AS defense_snaps,
                any_value(defense_pct) FILTER (WHERE defense_pct IS NOT NULL) AS defense_pct,
                max(st_snaps) AS st_snaps,
                any_value(st_pct) FILTER (WHERE st_pct IS NOT NULL) AS st_pct,
                string_agg(DISTINCT source_table, ';' ORDER BY source_table) AS source_tables
            FROM cleaned
            GROUP BY boxscore_id, team, COALESCE('pfr:' || pfr_id, 'name:' || player_name_norm), pfr_id
            ORDER BY game_date, boxscore_id, team, player
        ) TO '{sql_path(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return out_path


def write_manifest(con: duckdb.DuckDBPyConnection, out_dir: Path, paths: dict[str, Path]) -> Path:
    manifest_path = out_dir / "retabulation_manifest.json"
    manifest = {
        "generated_at_utc": now_stamp(),
        "outputs": {name: str(path) for name, path in paths.items()},
        "counts": {},
    }
    for name, path in paths.items():
        if path.suffix == ".parquet":
            parquet = sql_path(path)
            manifest["counts"][name] = {
                "rows": run_scalar(con, f"SELECT COUNT(*) FROM read_parquet('{parquet}')"),
                "distinct_boxscores": run_scalar(
                    con, f"SELECT COUNT(DISTINCT boxscore_id) FROM read_parquet('{parquet}')"
                ),
            }
    fact = sql_path(paths["player_game_fact"])
    manifest["counts"]["player_game_fact"].update(
        {
            "rows_missing_pfr_id": run_scalar(
                con, f"SELECT COUNT(*) FROM read_parquet('{fact}') WHERE missing_pfr_id = 1"
            ),
            "distinct_pfr_ids": run_scalar(
                con, f"SELECT COUNT(DISTINCT pfr_id) FROM read_parquet('{fact}') WHERE pfr_id IS NOT NULL"
            ),
            "key_collisions": run_scalar(
                con,
                f"""
                SELECT COUNT(*) FROM (
                    SELECT boxscore_id, team, pfr_player_game_key, COUNT(*) AS n
                    FROM read_parquet('{fact}')
                    GROUP BY 1,2,3
                    HAVING COUNT(*) > 1
                )
                """,
            ),
        }
    )
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", type=Path, default=DEFAULT_BASE_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = args.base_dir
    if not base_dir.exists():
        raise FileNotFoundError(base_dir)
    out_dir = args.output_dir or (args.output_root / f"pfr_boxscore_retabs_{now_stamp()}")
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='3GB'")
    con.execute("SET preserve_insertion_order=false")
    temp_dir = out_dir / "_duckdb_tmp"
    temp_dir.mkdir(exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{sql_path(temp_dir)}'")
    con.execute("PRAGMA max_temp_directory_size='2GB'")

    print(f"[retab] base={base_dir}", flush=True)
    print(f"[retab] out={out_dir}", flush=True)
    game_team_dim = build_game_team_dim(con, base_dir, out_dir)
    print(f"[retab] wrote {game_team_dim}", flush=True)
    player_game_fact = build_player_game_fact(con, base_dir, out_dir)
    print(f"[retab] wrote {player_game_fact}", flush=True)
    position_hint = build_position_hints(con, base_dir, out_dir)
    print(f"[retab] wrote {position_hint}", flush=True)
    manifest = write_manifest(
        con,
        out_dir,
        {
            "game_team_dim": game_team_dim,
            "player_game_fact": player_game_fact,
            "position_hint": position_hint,
        },
    )
    print(f"[retab] wrote {manifest}", flush=True)
    print(manifest.read_text(encoding="utf-8"), flush=True)


if __name__ == "__main__":
    main()
