#!/usr/bin/env python
"""Build a Newspapers.com acquisition queue from the local v26 NFL supertable.

The queue is intentionally acquisition-first. It treats the pre-PBP horizon
(default: 1920-1978) as needing newspaper source capture even when older audit
queues marked games as covered.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


RELEASES_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_INVENTORY = Path(
    r"D:\league-history-data\nfl\derived\validation\game_completeness_inventory"
    r"\20260616T043000Z_v22_pbp_parser_rebuild_closed\game_completeness_inventory.csv"
)
DEFAULT_OUTPUT_ROOT = Path(
    r"D:\league-history-data\nfl\derived\validation\newspaper_acquisition_queue"
)


def latest_v26_parquet() -> Path:
    candidates = sorted(
        RELEASES_ROOT.glob("*_v26/tables/nfl_player_stats_all.parquet"),
        key=lambda path: path.stat().st_mtime,
    )
    if not candidates:
        raise FileNotFoundError(f"No v26 nfl_player_stats_all.parquet found under {RELEASES_ROOT}")
    return candidates[-1]


def q(path: Path) -> str:
    return str(path).replace("\\", "/")


def sql_str(path: Path) -> str:
    return "'" + q(path).replace("'", "''") + "'"


def build_sql(
    pre_pbp_end_year: int,
    audit_end_year: int,
    supertable_parquet: Path,
    inventory_csv: Path,
) -> str:
    supertable_sql = sql_str(supertable_parquet)
    inventory_sql = sql_str(inventory_csv)
    return f"""
CREATE OR REPLACE TEMP VIEW inventory_games AS
SELECT
  boxscore_id,
  boxscore_url,
  CAST("year" AS INTEGER) AS year,
  CAST(week AS INTEGER) AS week,
  season_type,
  CAST(game_date AS DATE) AS game_date,
  game_scope,
  home_team,
  away_team,
  LEAST(home_team, away_team) AS team_a,
  GREATEST(home_team, away_team) AS team_b,
  coverage_statuses,
  stathead_has_detailed_boxscore_tables,
  stathead_detailed_table_count,
  stathead_detailed_table_rows,
  has_player_offense_table,
  has_player_defense_table,
  has_kicking_table,
  has_returns_table,
  has_team_stats_table,
  has_pfr_pbp_table,
  has_scoring_table,
  has_game_info_table,
  local_rows,
  local_player_weeks,
  local_player_ids,
  local_team_def_rows,
  local_core_offense_atoms,
  local_kicking_atoms,
  local_return_atoms,
  local_idp_defense_atoms,
  local_team_def_atoms,
  pbp_expectation,
  idp_expectation,
  team_def_expectation,
  completeness_flags,
  completeness_status,
  review_priority,
  source_page_url
FROM read_csv_auto({inventory_sql})
WHERE CAST("year" AS INTEGER) BETWEEN 1920 AND {audit_end_year}
  AND COALESCE(game_scope, '') = 'nfl_game';

CREATE OR REPLACE TEMP VIEW v26_game_stats AS
WITH side_rows AS (
  SELECT
    CAST("year" AS INTEGER) AS year,
    CAST(week AS INTEGER) AS week,
    CAST(game_date AS DATE) AS game_date,
    nfl_team,
    opponent_nfl_team,
    LEAST(nfl_team, opponent_nfl_team) AS team_a,
    GREATEST(nfl_team, opponent_nfl_team) AS team_b,
    position,
    data_source,
    attempts,
    completions,
    passing_yards,
    passing_tds,
    passing_interceptions,
    carries,
    rushing_yards,
    rushing_tds,
    receptions,
    receiving_yards,
    receiving_tds,
    fg_att,
    fg_made,
    pat_att,
    pat_made,
    punts,
    punt_yards,
    kickoff_returns,
    kickoff_return_yards,
    punt_returns,
    punt_return_yards,
    fumbles,
    fumbles_lost,
    def_tackles_solo,
    def_tackle_assists,
    def_sacks,
    def_interceptions,
    points_allowed,
    total_yds_allowed,
    passing_yds_allowed,
    rushing_yds_allowed
  FROM read_parquet({supertable_sql})
  WHERE "year" BETWEEN 1920 AND {audit_end_year}
    AND game_date IS NOT NULL
    AND nfl_team IS NOT NULL
    AND opponent_nfl_team IS NOT NULL
)
SELECT
  year,
  week,
  game_date,
  team_a,
  team_b,
  COUNT(*) AS v26_rows,
  COUNT(*) FILTER (WHERE position <> 'DEF' OR position IS NULL) AS v26_player_rows,
  COUNT(*) FILTER (WHERE position = 'DEF') AS v26_def_rows,
  COUNT(DISTINCT nfl_team) AS v26_team_sides,
  STRING_AGG(DISTINCT COALESCE(data_source, ''), ' | ' ORDER BY COALESCE(data_source, '')) AS v26_data_sources,
  SUM(COALESCE(attempts, 0) + COALESCE(completions, 0) + COALESCE(passing_yards, 0)
      + COALESCE(passing_tds, 0) + COALESCE(passing_interceptions, 0)) AS pass_atom_sum,
  SUM(COALESCE(carries, 0) + COALESCE(rushing_yards, 0) + COALESCE(rushing_tds, 0)) AS rush_atom_sum,
  SUM(COALESCE(receptions, 0) + COALESCE(receiving_yards, 0) + COALESCE(receiving_tds, 0)) AS rec_atom_sum,
  SUM(COALESCE(fg_att, 0) + COALESCE(fg_made, 0) + COALESCE(pat_att, 0) + COALESCE(pat_made, 0)) AS kick_atom_sum,
  SUM(COALESCE(punts, 0) + COALESCE(punt_yards, 0)) AS punt_atom_sum,
  SUM(COALESCE(kickoff_returns, 0) + COALESCE(kickoff_return_yards, 0)
      + COALESCE(punt_returns, 0) + COALESCE(punt_return_yards, 0)) AS return_atom_sum,
  SUM(COALESCE(fumbles, 0) + COALESCE(fumbles_lost, 0)) AS fumble_atom_sum,
  SUM(COALESCE(def_tackles_solo, 0) + COALESCE(def_tackle_assists, 0)
      + COALESCE(def_sacks, 0) + COALESCE(def_interceptions, 0)) AS idp_atom_sum,
  SUM(COALESCE(points_allowed, 0) + COALESCE(total_yds_allowed, 0)
      + COALESCE(passing_yds_allowed, 0) + COALESCE(rushing_yds_allowed, 0)) AS team_def_atom_sum
FROM side_rows
GROUP BY year, week, game_date, team_a, team_b;

CREATE OR REPLACE TEMP VIEW acquisition_audit AS
SELECT
  inv.boxscore_id,
  inv.boxscore_url,
  inv.year,
  inv.week,
  inv.season_type,
  inv.game_date,
  inv.away_team,
  inv.home_team,
  inv.team_a,
  inv.team_b,
  CASE
    WHEN inv.year <= 1931 THEN 'P0_pre_1932_no_official_box_baseline'
    WHEN inv.year <= 1959 THEN 'P1_1932_1959_partial_box_stats'
    WHEN inv.year <= {pre_pbp_end_year} THEN 'P2_1960_1978_pre_pbp_full_box_needed'
    ELSE 'P3_1979_1994_stat_gap_exception'
  END AS acquisition_priority_band,
  CASE
    WHEN inv.year <= {pre_pbp_end_year} THEN TRUE
    WHEN st.v26_rows IS NULL THEN TRUE
    WHEN COALESCE(st.v26_team_sides, 0) < 2 THEN TRUE
    WHEN COALESCE(st.pass_atom_sum, 0) = 0 THEN TRUE
    WHEN COALESCE(st.rush_atom_sum, 0) = 0 THEN TRUE
    WHEN COALESCE(st.rec_atom_sum, 0) = 0 THEN TRUE
    WHEN inv.year >= 1932 AND COALESCE(st.kick_atom_sum, 0) = 0 THEN TRUE
    WHEN inv.year >= 1932 AND COALESCE(st.punt_atom_sum, 0) = 0 THEN TRUE
    WHEN inv.year >= 1941 AND COALESCE(st.return_atom_sum, 0) = 0 THEN TRUE
    ELSE FALSE
  END AS needs_newspapers,
  (
    CASE WHEN inv.year <= {pre_pbp_end_year} THEN 1000 ELSE 0 END
    + CASE WHEN st.v26_rows IS NULL THEN 250 ELSE 0 END
    + CASE WHEN COALESCE(st.v26_team_sides, 0) < 2 THEN 150 ELSE 0 END
    + CASE WHEN COALESCE(st.pass_atom_sum, 0) = 0 THEN 40 ELSE 0 END
    + CASE WHEN COALESCE(st.rush_atom_sum, 0) = 0 THEN 40 ELSE 0 END
    + CASE WHEN COALESCE(st.rec_atom_sum, 0) = 0 THEN 40 ELSE 0 END
    + CASE WHEN inv.year >= 1932 AND COALESCE(st.kick_atom_sum, 0) = 0 THEN 30 ELSE 0 END
    + CASE WHEN inv.year >= 1932 AND COALESCE(st.punt_atom_sum, 0) = 0 THEN 30 ELSE 0 END
    + CASE WHEN inv.year >= 1941 AND COALESCE(st.return_atom_sum, 0) = 0 THEN 25 ELSE 0 END
    + CASE WHEN inv.has_player_offense_table = 0 THEN 20 ELSE 0 END
    + CASE WHEN inv.has_kicking_table = 0 THEN 10 ELSE 0 END
    + CASE WHEN inv.has_returns_table = 0 THEN 10 ELSE 0 END
  ) AS acquisition_score,
  CONCAT_WS(';',
    CASE WHEN inv.year <= {pre_pbp_end_year} THEN 'pre_pbp_horizon_requires_newspaper_source' END,
    CASE WHEN st.v26_rows IS NULL THEN 'no_v26_game_match' END,
    CASE WHEN COALESCE(st.v26_team_sides, 0) < 2 THEN 'missing_one_or_both_team_sides_in_v26' END,
    CASE WHEN COALESCE(st.pass_atom_sum, 0) = 0 THEN 'no_passing_atoms_in_v26' END,
    CASE WHEN COALESCE(st.rush_atom_sum, 0) = 0 THEN 'no_rushing_atoms_in_v26' END,
    CASE WHEN COALESCE(st.rec_atom_sum, 0) = 0 THEN 'no_receiving_atoms_in_v26' END,
    CASE WHEN inv.year >= 1932 AND COALESCE(st.kick_atom_sum, 0) = 0 THEN 'no_kicking_atoms_in_v26' END,
    CASE WHEN inv.year >= 1932 AND COALESCE(st.punt_atom_sum, 0) = 0 THEN 'no_punting_atoms_in_v26' END,
    CASE WHEN inv.year >= 1941 AND COALESCE(st.return_atom_sum, 0) = 0 THEN 'no_return_atoms_in_v26' END,
    CASE WHEN inv.has_player_offense_table = 0 THEN 'inventory_no_player_offense_table' END,
    CASE WHEN inv.has_kicking_table = 0 THEN 'inventory_no_kicking_table' END,
    CASE WHEN inv.has_returns_table = 0 THEN 'inventory_no_returns_table' END
  ) AS acquisition_reasons,
  CASE
    WHEN st.v26_rows IS NULL OR COALESCE(st.v26_team_sides, 0) < 2 THEN 3
    WHEN inv.year <= 1931 THEN 3
    WHEN inv.year <= 1959 THEN 2
    ELSE 2
  END AS suggested_pages_per_game,
  inv.coverage_statuses,
  inv.stathead_has_detailed_boxscore_tables,
  inv.stathead_detailed_table_count,
  inv.stathead_detailed_table_rows,
  inv.has_player_offense_table,
  inv.has_player_defense_table,
  inv.has_kicking_table,
  inv.has_returns_table,
  inv.has_team_stats_table,
  inv.has_pfr_pbp_table,
  inv.local_rows,
  inv.local_player_weeks,
  inv.local_player_ids,
  inv.local_team_def_rows,
  inv.local_core_offense_atoms,
  inv.local_kicking_atoms,
  inv.local_return_atoms,
  inv.local_idp_defense_atoms,
  inv.local_team_def_atoms,
  inv.pbp_expectation,
  inv.idp_expectation,
  inv.team_def_expectation,
  inv.completeness_flags,
  inv.completeness_status,
  inv.review_priority AS old_review_priority,
  st.v26_rows,
  st.v26_player_rows,
  st.v26_def_rows,
  st.v26_team_sides,
  st.v26_data_sources,
  st.pass_atom_sum,
  st.rush_atom_sum,
  st.rec_atom_sum,
  st.kick_atom_sum,
  st.punt_atom_sum,
  st.return_atom_sum,
  st.fumble_atom_sum,
  st.idp_atom_sum,
  st.team_def_atom_sum,
  inv.source_page_url
FROM inventory_games inv
LEFT JOIN v26_game_stats st
  ON inv.game_date = st.game_date
 AND inv.team_a = st.team_a
 AND inv.team_b = st.team_b;

CREATE OR REPLACE TEMP VIEW acquisition_queue AS
SELECT *
FROM acquisition_audit
WHERE needs_newspapers
ORDER BY
  CASE acquisition_priority_band
    WHEN 'P0_pre_1932_no_official_box_baseline' THEN 0
    WHEN 'P1_1932_1959_partial_box_stats' THEN 1
    WHEN 'P2_1960_1978_pre_pbp_full_box_needed' THEN 2
    ELSE 3
  END,
  acquisition_score DESC,
  year,
  game_date,
  boxscore_id;

CREATE OR REPLACE TEMP VIEW year_summary AS
SELECT
  year,
  COUNT(*) AS inventory_games,
  SUM(CASE WHEN needs_newspapers THEN 1 ELSE 0 END) AS needs_newspapers_games,
  SUM(CASE WHEN year <= {pre_pbp_end_year} THEN 1 ELSE 0 END) AS pre_pbp_games,
  SUM(CASE WHEN acquisition_priority_band = 'P0_pre_1932_no_official_box_baseline' THEN 1 ELSE 0 END) AS p0_games,
  SUM(CASE WHEN acquisition_priority_band = 'P1_1932_1959_partial_box_stats' THEN 1 ELSE 0 END) AS p1_games,
  SUM(CASE WHEN acquisition_priority_band = 'P2_1960_1978_pre_pbp_full_box_needed' THEN 1 ELSE 0 END) AS p2_games,
  SUM(CASE WHEN acquisition_priority_band = 'P3_1979_1994_stat_gap_exception' AND needs_newspapers THEN 1 ELSE 0 END) AS p3_gap_games,
  SUM(CASE WHEN v26_rows IS NULL THEN 1 ELSE 0 END) AS no_v26_match_games,
  SUM(CASE WHEN COALESCE(v26_team_sides, 0) < 2 THEN 1 ELSE 0 END) AS missing_team_side_games,
  SUM(CASE WHEN COALESCE(pass_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_passing_atom_games,
  SUM(CASE WHEN COALESCE(rush_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_rushing_atom_games,
  SUM(CASE WHEN COALESCE(rec_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_receiving_atom_games,
  SUM(CASE WHEN year >= 1932 AND COALESCE(kick_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_kicking_atom_games,
  SUM(CASE WHEN year >= 1932 AND COALESCE(punt_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_punting_atom_games,
  SUM(CASE WHEN year >= 1941 AND COALESCE(return_atom_sum, 0) = 0 THEN 1 ELSE 0 END) AS no_return_atom_games
FROM acquisition_audit
GROUP BY year
ORDER BY year;
"""


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    value = con.execute(sql).fetchone()[0]
    return int(value or 0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--supertable-parquet", type=Path, default=None)
    parser.add_argument("--inventory-csv", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--label", default=None)
    parser.add_argument("--pre-pbp-end-year", type=int, default=1978)
    parser.add_argument("--audit-end-year", type=int, default=1994)
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()

    parquet = args.supertable_parquet or latest_v26_parquet()
    if not parquet.exists():
        raise FileNotFoundError(parquet)
    if not args.inventory_csv.exists():
        raise FileNotFoundError(args.inventory_csv)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    label = args.label or f"{stamp}_v26_stat_gap_newspaper_queue"
    output_dir = args.output_root / label
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET progress_bar_time=99999")
    con.execute("SET preserve_insertion_order=false")
    con.execute(build_sql(args.pre_pbp_end_year, args.audit_end_year, parquet, args.inventory_csv))

    all_audit_csv = output_dir / "v26_game_stat_completeness_audit_1920_1994.csv"
    queue_csv = output_dir / "newspaper_acquisition_queue_1920_1994.csv"
    pre_pbp_csv = output_dir / "pre_pbp_newspaper_acquisition_queue_1920_1978.csv"
    year_summary_csv = output_dir / "newspaper_acquisition_year_summary.csv"
    samples_csv = output_dir / "highest_priority_sample_200.csv"
    batch_dir = output_dir / "pre_pbp_batches"
    batch_dir.mkdir(exist_ok=True)

    con.execute(f"COPY acquisition_audit TO '{q(all_audit_csv)}' (HEADER, DELIMITER ',')")
    con.execute(f"COPY acquisition_queue TO '{q(queue_csv)}' (HEADER, DELIMITER ',')")
    con.execute(
        f"""
        COPY (
          SELECT *
          FROM acquisition_queue
          WHERE year <= {args.pre_pbp_end_year}
          ORDER BY
            CASE acquisition_priority_band
              WHEN 'P0_pre_1932_no_official_box_baseline' THEN 0
              WHEN 'P1_1932_1959_partial_box_stats' THEN 1
              ELSE 2
            END,
            acquisition_score DESC,
            year,
            game_date,
            boxscore_id
        ) TO '{q(pre_pbp_csv)}' (HEADER, DELIMITER ',')
        """
    )
    con.execute(f"COPY year_summary TO '{q(year_summary_csv)}' (HEADER, DELIMITER ',')")
    con.execute(
        f"""
        COPY (
          SELECT *
          FROM acquisition_queue
          ORDER BY acquisition_score DESC, year, game_date, boxscore_id
          LIMIT 200
        ) TO '{q(samples_csv)}' (HEADER, DELIMITER ',')
        """
    )

    pre_pbp_total = scalar(
        con, f"SELECT COUNT(*) FROM acquisition_queue WHERE year <= {args.pre_pbp_end_year}"
    )
    batch_paths: list[Path] = []
    for batch_idx, offset in enumerate(range(0, pre_pbp_total, args.batch_size), start=1):
        batch_path = batch_dir / f"pre_pbp_batch_{batch_idx:04d}.csv"
        con.execute(
            f"""
            COPY (
              SELECT *
              FROM acquisition_queue
              WHERE year <= {args.pre_pbp_end_year}
              ORDER BY
                CASE acquisition_priority_band
                  WHEN 'P0_pre_1932_no_official_box_baseline' THEN 0
                  WHEN 'P1_1932_1959_partial_box_stats' THEN 1
                  ELSE 2
                END,
                acquisition_score DESC,
                year,
                game_date,
                boxscore_id
              LIMIT {args.batch_size} OFFSET {offset}
            ) TO '{q(batch_path)}' (HEADER, DELIMITER ',')
            """
        )
        batch_paths.append(batch_path)

    summary = {
        "generated_at_utc": stamp,
        "supertable_parquet": str(parquet),
        "inventory_csv": str(args.inventory_csv),
        "output_dir": str(output_dir),
        "audit_end_year": args.audit_end_year,
        "pre_pbp_end_year": args.pre_pbp_end_year,
        "inventory_games_1920_1994": scalar(con, "SELECT COUNT(*) FROM acquisition_audit"),
        "needs_newspapers_1920_1994": scalar(con, "SELECT COUNT(*) FROM acquisition_queue"),
        "pre_pbp_inventory_games_1920_1978": scalar(
            con, f"SELECT COUNT(*) FROM acquisition_audit WHERE year <= {args.pre_pbp_end_year}"
        ),
        "pre_pbp_needs_newspapers_1920_1978": pre_pbp_total,
        "batch_size": args.batch_size,
        "pre_pbp_batch_count": len(batch_paths),
        "first_pre_pbp_batch": str(batch_paths[0]) if batch_paths else None,
        "p0_games": scalar(
            con,
            "SELECT COUNT(*) FROM acquisition_queue "
            "WHERE acquisition_priority_band = 'P0_pre_1932_no_official_box_baseline'",
        ),
        "p1_games": scalar(
            con,
            "SELECT COUNT(*) FROM acquisition_queue "
            "WHERE acquisition_priority_band = 'P1_1932_1959_partial_box_stats'",
        ),
        "p2_games": scalar(
            con,
            "SELECT COUNT(*) FROM acquisition_queue "
            "WHERE acquisition_priority_band = 'P2_1960_1978_pre_pbp_full_box_needed'",
        ),
        "p3_gap_games": scalar(
            con,
            "SELECT COUNT(*) FROM acquisition_queue "
            "WHERE acquisition_priority_band = 'P3_1979_1994_stat_gap_exception'",
        ),
        "outputs": {
            "all_audit_csv": str(all_audit_csv),
            "queue_csv": str(queue_csv),
            "pre_pbp_csv": str(pre_pbp_csv),
            "year_summary_csv": str(year_summary_csv),
            "samples_csv": str(samples_csv),
            "pre_pbp_batch_dir": str(batch_dir),
        },
    }

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# v26 Newspaper Acquisition Queue",
                "",
                "Authority: local D-drive v26 `nfl_player_stats_all.parquet` joined to the game completeness inventory.",
                "",
                "Core rule: every game through the pre-PBP horizon needs newspaper source capture.",
                f"Current pre-PBP horizon: 1920-{args.pre_pbp_end_year}.",
                "",
                "Priority bands:",
                "- P0: 1920-1931, before official player box-score baseline.",
                "- P1: 1932-1959, partial historical box stats.",
                "- P2: 1960-1978, pre-PBP full box/source verification.",
                "- P3: 1979-1994, stat-gap exceptions only.",
                "",
                "Primary queue file:",
                f"- `{pre_pbp_csv.name}`",
                "",
                "Batch files:",
                f"- `{batch_dir.name}/pre_pbp_batch_0001.csv` onward",
                f"- Batch size: {args.batch_size} games",
                "",
                "Broad audit files:",
                f"- `{queue_csv.name}`",
                f"- `{all_audit_csv.name}`",
                f"- `{year_summary_csv.name}`",
                "",
            ]
        ),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
