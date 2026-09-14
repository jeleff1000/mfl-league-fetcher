"""Regression tests for compact research-matchup player-week cells."""

from __future__ import annotations

from itertools import product

import duckdb
import pytest

from scripts.research_cohorts.compact_matchup_cells import (
    _narrow_rollup_projection,
    _compact_season_requested_cells,
    _compact_weekly_requested_cells,
    materialize_compact_core_selector_indices,
    materialize_compact_grade_selector_indices,
    materialize_career_cohort_grade_cells,
    materialize_career_compact_outer_rows,
    materialize_career_core_cohort_cells,
    materialize_career_outer_rows,
    materialize_narrow_position_year_source,
    materialize_player_id_subset_source,
    materialize_season_core_cohort_cells,
    materialize_season_bracket_grade_cells,
    materialize_season_bracket_grade_position_batch,
    materialize_season_compact_outer_rows,
    materialize_season_outer_rows,
    materialize_season_core_cohort_position_batch,
    materialize_weekly_core_cohort_cells,
    materialize_weekly_outer_rows,
    validate_narrow_position_capacity_allocation,
)


def _all_pool_profile() -> str:
    """A fully split profile makes the sample-floor fallback observable in tests."""
    return """
      struct_pack(
        teams_08tm_split := true, teams_10tm_split := true,
        teams_12tm_split := true, teams_14tm_split := true,
        roster_flx_split := true, roster_sflx_split := true, roster_idp_split := true,
        scoring_std_split := true, scoring_half_split := true, scoring_ppr_split := true,
        pass_td_4pt_split := true, pass_td_6pt_split := true,
        dynasty_redraft_split := true, dynasty_dynasty_split := true,
        best_ball_managed_split := true, best_ball_best_ball_split := true
      )
    """


def test_weekly_adaptive_selection_requires_a_150_league_cohort() -> None:
    connection = duckdb.connect()
    profile = _all_pool_profile()
    connection.execute(
        f"""
        CREATE TABLE compact AS
        SELECT 'kept'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year, 1::INTEGER AS week,
               'RB'::VARCHAR AS position, {profile} AS cohort_profile,
               [
                 struct_pack(q_teams := '12tm', q_roster := 'flx', q_scoring := 'std', q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed', eligible_leagues := 149::BIGINT, rostered_leagues := 500::BIGINT),
                 struct_pack(q_teams := '12tm', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 151::BIGINT, rostered_leagues := 500::BIGINT),
                 struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 900::BIGINT, rostered_leagues := 300::BIGINT)
               ] AS cohort_cells
        UNION ALL
        SELECT 'dropped', 2025, 1, 'RB', {profile},
               [struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 900::BIGINT, rostered_leagues := 149::BIGINT)]
        """
    )

    _compact_weekly_requested_cells(connection, output_table="compact", split_threshold=150)
    materialize_compact_core_selector_indices(connection, output_table="compact")

    assert connection.execute("SELECT NFL_player_id FROM compact").fetchall() == [("kept",)]
    assert connection.execute(
        "SELECT COUNT(*) FROM compact, UNNEST(cohort_cells) u(cell) WHERE cell.eligible_leagues < 150"
    ).fetchone()[0] == 0
    assert connection.execute(
        """
        SELECT list_extract(cohort_cells, list_extract(core_selector_indices, 145)).q_roster,
               list_extract(cohort_cells, list_extract(core_selector_indices, 145)).eligible_leagues
        FROM compact
        """
    ).fetchone() == ("ALL", 151)


def test_season_adaptive_selection_requires_a_150_league_cohort() -> None:
    connection = duckdb.connect()
    profile = _all_pool_profile()
    connection.execute(
        f"""
        CREATE TABLE compact AS
        SELECT 'kept'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year,
               'RB'::VARCHAR AS position, {profile} AS cohort_profile,
               [
                 struct_pack(q_teams := '12tm', q_roster := 'flx', q_scoring := 'std', q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed', eligible_leagues := 149::BIGINT, rostered_league_weeks := 500::BIGINT),
                 struct_pack(q_teams := '12tm', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 151::BIGINT, rostered_league_weeks := 500::BIGINT),
                 struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 900::BIGINT, rostered_league_weeks := 300::BIGINT)
               ] AS cohort_cells
        UNION ALL
        SELECT 'dropped', 2025, 'RB', {profile},
               [struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 900::BIGINT, rostered_league_weeks := 149::BIGINT)]
        """
    )

    _compact_season_requested_cells(connection, output_table="compact", split_threshold=150)
    materialize_compact_core_selector_indices(connection, output_table="compact")

    assert connection.execute("SELECT NFL_player_id FROM compact").fetchall() == [("kept",)]
    assert connection.execute(
        "SELECT COUNT(*) FROM compact, UNNEST(cohort_cells) u(cell) WHERE cell.eligible_leagues < 150"
    ).fetchone()[0] == 0
    assert connection.execute(
        """
        SELECT list_extract(cohort_cells, list_extract(core_selector_indices, 145)).q_roster,
               list_extract(cohort_cells, list_extract(core_selector_indices, 145)).eligible_leagues
        FROM compact
        """
    ).fetchone() == ("ALL", 151)


def test_compact_outer_wrappers_join_core_and_grade_on_one_outer_identity() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_core AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year, 'RB'::VARCHAR AS position,
               [struct_pack(q_teams := 'ALL')] AS cohort_cells,
               struct_pack(teams_10tm_split := false) AS cohort_profile
        """
    )
    connection.execute(
        """
        CREATE TABLE season_grade AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year, 'RB'::VARCHAR AS position,
               [struct_pack(q_playoff_teams := 'ALL', eligible_leagues := 2::BIGINT,
                            playoff_leagues := 1::BIGINT, champ_leagues := 1::BIGINT,
                            playoff_rate_pct := 50.0, champ_rate_pct := 50.0)] AS grade_cells
        """
    )

    materialize_season_compact_outer_rows(
        connection,
        core_outer_table="season_core",
        grade_outer_table="season_grade",
        output_table="season_compact",
    )
    assert connection.execute(
        "SELECT NFL_player_id, year, position, list_count(cohort_cells), list_count(grade_cells) FROM season_compact"
    ).fetchall() == [("p1", 2025, "RB", 1, 1)]

    connection.execute(
        """
        CREATE TABLE career_core AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
               [struct_pack(q_teams := 'ALL')] AS cohort_cells
        """
    )
    connection.execute(
        """
        CREATE TABLE career_grade AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
               [struct_pack(q_playoff_teams := 'ALL', eligible_league_seasons := 2::BIGINT,
                            playoff_credits := 1::BIGINT, champ_credits := 1::BIGINT,
                            playoff_rate_pct := 50.0, champ_rate_pct := 50.0,
                            expected_playoffs := 1.0, expected_champs := 1.0)] AS grade_cells
        """
    )

    materialize_career_compact_outer_rows(
        connection,
        season_compact_table="season_compact",
        output_table="career_compact",
        core_outer_table="career_core",
        grade_outer_table="career_grade",
    )
    assert connection.execute(
        "SELECT NFL_player_id, position, list_count(cohort_cells), list_count(grade_cells) FROM career_compact"
    ).fetchall() == [("p1", "RB", 1, 1)]


def test_career_outer_preserves_the_independent_roster_capacity_map() -> None:
    """Career roster rate must resolve through season roster cells, not start cells."""
    connection = duckdb.connect()
    cell_columns = """
      q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
      q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 200::BIGINT,
      rostered_league_weeks := 150::BIGINT, eligible_league_weeks := 200::BIGINT,
      started_league_weeks := 100::BIGINT, healthy_started_league_weeks := 100::BIGINT,
      healthy_eligible_league_weeks := 120::BIGINT, valid_started_outcomes := 100::BIGINT,
      win_equivalent := 60.0, roster_rate_pct := 75.0, start_rate_pct := 50.0,
      healthy_start_rate_pct := 83.333333, win_rate_pct := 60.0,
      expected_starts := 8.0, expected_wins := 4.8, expected_losses := 3.2,
      clutch_season_sum := 2.0, active_weeks := 12::BIGINT, inactive_weeks := 4::BIGINT
    """
    connection.execute(
        f"""
        CREATE TABLE season_source AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year, 'RB'::VARCHAR AS position,
          [struct_pack({cell_columns.replace('roster_rate_pct := 75.0', 'roster_rate_pct := 10.0')})] AS cohort_cells,
          list_transform(range(1, 289), x -> 1::USMALLINT) AS core_selector_indices,
          [struct_pack({cell_columns})] AS roster_cells,
          list_transform(range(1, 289), x -> 1::USMALLINT) AS roster_selector_indices
        """
    )
    connection.execute(
        """
        CREATE TABLE career_core AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
               [struct_pack(q_teams := 'ALL')] AS cohort_cells
        """
    )
    connection.execute(
        """
        CREATE TABLE career_grade AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
               [struct_pack(q_playoff_teams := 'ALL')] AS grade_cells
        """
    )

    materialize_career_compact_outer_rows(
        connection,
        season_compact_table="season_source",
        output_table="career_output",
        core_outer_table="career_core",
        grade_outer_table="career_grade",
    )

    assert connection.execute(
        "SELECT list_extract(roster_cells, 1).roster_rate_pct, list_count(roster_selector_indices) FROM career_output"
    ).fetchone() == (75.0, 288)


def test_narrow_position_year_source_applies_identity_guards_once() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_rostered INTEGER, is_started INTEGER,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
          team_points DOUBLE, fantasy_points DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1, 'manager', '1', 'Team', 120.0, 23.2),
          ('league_a', 2025, 1, 'old_rb', 'RB', 1, 0, NULL, NULL, NULL, NULL, NULL),
          ('league_a', 2025, 1, 'myles', 'DL', 1, 1, 'manager', '1', 'Team', 120.0, 10.0)
        """
    )

    materialize_narrow_position_year_source(
        connection,
        source_table="facts",
        output_table="rb_2025_facts",
        year=2025,
        position="RB",
    )

    assert connection.execute(
        "SELECT NFL_player_id, position FROM rb_2025_facts"
    ).fetchall() == [("cmc", "RB")]


def test_narrow_rollup_projection_emits_team_fanout_in_source_scan() -> None:
    """Team-size aliases must be materialized without a second full-table UPDATE."""

    projection = _narrow_rollup_projection({"year", "position", "cohort_teams"})

    assert 'CAST(p."cohort_teams" AS VARCHAR) AS "cohort_teams_rostered"' in projection
    assert 'CAST(p."cohort_teams" AS VARCHAR) AS "cohort_teams_started"' in projection


def test_narrow_position_year_source_drops_unneeded_raw_cache_payload_columns() -> None:
    """A large source lane carries only rollup facts, never raw cache payload."""

    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
          is_playoffs INTEGER, champion INTEGER,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
          team_points DOUBLE, fantasy_points DOUBLE,
          unused_raw_payload VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a',2024,1,'wr','WR',1,'12tm','flx','half','4pt','6po','redraft','managed',
           1,1,1.0,2.0,0,0,'manager','team','Team',120.0,20.0,'do-not-copy')
        """
    )

    materialize_narrow_position_year_source(
        connection,
        source_table="facts",
        output_table="wr_2024_facts",
        year=2024,
        position="WR",
    )

    columns = {row[0] for row in connection.execute("DESCRIBE wr_2024_facts").fetchall()}
    assert "unused_raw_payload" not in columns
    assert {
        "db_name", "year", "week", "NFL_player_id", "position",
        "cohort_position_eligible", "cohort_teams", "cohort_roster",
        "cohort_scoring", "cohort_pass_td", "cohort_playoff_teams",
        "cohort_dynasty", "cohort_best_ball", "is_rostered", "is_started",
        "win", "clutch_equity", "is_playoffs", "champion",
        "manager", "team_key", "team_name", "team_points", "fantasy_points",
    } <= columns


def test_narrow_position_year_source_preserves_literal_team_bucket_for_both_metrics() -> None:
    """Team size is literal; roster/start capacity cannot relabel an 8-team league."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts AS
        SELECT
          'league_a'::VARCHAR AS db_name,
          2025::INTEGER AS year,
          1::INTEGER AS week,
          'rb_' || i AS NFL_player_id,
          'RB'::VARCHAR AS position,
          1::INTEGER AS cohort_position_eligible,
          '08tm'::VARCHAR AS cohort_teams,
          'flx'::VARCHAR AS cohort_roster,
          1::INTEGER AS is_rostered,
          CASE WHEN i < 22 THEN 1 ELSE 0 END::INTEGER AS is_started
        FROM range(52) r(i)
        """
    )

    materialize_narrow_position_year_source(
        connection,
        source_table="facts",
        output_table="rb_2025_facts",
        year=2025,
        position="RB",
    )

    assert connection.execute(
        """
        SELECT DISTINCT cohort_teams, cohort_teams_rostered, cohort_teams_started
        FROM rb_2025_facts
        """
    ).fetchall() == [("08tm", "08tm", "08tm")]


def test_narrow_position_year_source_keeps_qb_team_size_identical_in_flx_and_superflex() -> None:
    """FLX/SFLX changes eligibility metrics, never a league's literal team size."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts AS
        SELECT
          'league_' || lane::VARCHAR AS db_name,
          2025::INTEGER AS year,
          1::INTEGER AS week,
          'qb_' || i AS NFL_player_id,
          'QB'::VARCHAR AS position,
          1::INTEGER AS cohort_position_eligible,
          '12tm'::VARCHAR AS cohort_teams,
          lane::VARCHAR AS cohort_roster,
          1::INTEGER AS is_rostered,
          CASE WHEN i < 11 THEN 1 ELSE 0 END::INTEGER AS is_started
        FROM (VALUES ('flx'), ('sflx')) lanes(lane)
        CROSS JOIN range(20) players(i)
        """
    )

    materialize_narrow_position_year_source(
        connection,
        source_table="facts",
        output_table="qb_2025_facts",
        year=2025,
        position="QB",
    )

    assert connection.execute(
        """
        SELECT cohort_roster, cohort_teams_rostered, cohort_teams_started
        FROM qb_2025_facts
        GROUP BY ALL
        ORDER BY cohort_roster
        """
    ).fetchall() == [
        ("flx", "12tm", "12tm"),
        ("sflx", "12tm", "12tm"),
    ]


def test_narrow_position_capacity_allocation_requires_one_valid_tier_per_eligible_league() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE lane AS
        SELECT * FROM (
          VALUES
            ('league_a', 2025, 'flx', 1, '10tm', '12tm'),
            ('league_a', 2025, 'flx', 1, '10tm', '12tm'),
            ('league_b', 2025, 'sflx', 1, '12tm', '12tm'),
            ('historic', 2005, 'flx', 1, 'ALL', 'ALL'),
            ('unknown_team_count', 2014, 'flx', 1, 'ALL', 'ALL'),
            ('ineligible', 2025, 'flx', 0, NULL, NULL)
        ) AS t(db_name, year, cohort_roster, cohort_position_eligible,
                 cohort_teams_rostered, cohort_teams_started)
        """
    )

    result = validate_narrow_position_capacity_allocation(
        connection,
        source_table="lane",
    )
    assert result["eligible_lanes"] == 4
    assert result["visible_eligible_lanes"] == 2
    assert result["historical_all_lanes"] == 1
    assert result["unknown_team_count_pooled_lanes"] == 1
    assert result["rostered_tiers"] == {"10tm": 1, "12tm": 1}
    assert result["started_tiers"] == {"12tm": 2}
    assert result["rostered_10_12_lanes"] == 2
    assert result["rostered_10_12_share"] == 1.0
    assert result["started_10_12_lanes"] == 2
    assert result["started_10_12_share"] == 1.0

    connection.execute(
        "UPDATE lane SET cohort_teams_started='ALL' WHERE db_name='league_b'"
    )
    with pytest.raises(RuntimeError, match="invalid pooled capacity allocation"):
        validate_narrow_position_capacity_allocation(
            connection,
            source_table="lane",
        )

    connection.execute(
        "UPDATE lane SET cohort_teams_started='12tm' WHERE db_name='league_b'"
    )
    connection.execute(
        "UPDATE lane SET cohort_teams_rostered='10tm' WHERE db_name='historic'"
    )
    with pytest.raises(RuntimeError, match="invalid pooled capacity allocation"):
        validate_narrow_position_capacity_allocation(
            connection,
            source_table="lane",
        )


def test_player_id_subset_source_preserves_only_requested_lane_players() -> None:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE lane (NFL_player_id VARCHAR, position VARCHAR, value INTEGER)"
    )
    connection.execute(
        "INSERT INTO lane VALUES ('cmc', 'RB', 1), ('bijan', 'RB', 2), ('myles', 'DL', 3)"
    )

    materialize_player_id_subset_source(
        connection,
        source_table="lane",
        output_table="bucket",
        player_ids=["cmc", "bijan"],
    )

    assert connection.execute(
        "SELECT NFL_player_id, position, value FROM bucket ORDER BY NFL_player_id"
    ).fetchall() == [("bijan", "RB", 2), ("cmc", "RB", 1)]


def test_weekly_outer_row_is_unique_and_null_roster_is_rostered() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, NULL, 1, 1.0, 2.0),
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1,    1, 1.0, 2.0),
          ('league_b', 2025, 1, 'cmc', 'RB', 1, 0,    0, NULL, NULL),
          ('league_a', 2025, 1, 'other_a', 'RB', 1, 0, 0, NULL, NULL),
          ('league_b', 2025, 1, 'other_b', 'RB', 1, 0, 0, NULL, NULL),
          ('league_c', 2025, 1, 'other_c', 'WR', 1, 0, 0, NULL, NULL)
        """
    )

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="cmc",
    )

    rows = connection.execute(
        """
        SELECT NFL_player_id, year, week, position, cell.rostered_leagues,
          cell.started_leagues, cell.eligible_leagues, cell.win_equivalent,
          cell.valid_started_outcomes, cell.roster_rate_pct, cell.start_rate_pct,
          cell.win_rate_pct, cell.expected_starts, cell.expected_wins,
          cell.clutch_weekly_average
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchall()

    assert rows == [
        (
            "cmc", 2025, 1, "RB", 1, 1, 2, 1.0, 1,
            50.0, 50.0, 100.0, 0.5, 0.5, 1.0,
        )
    ]


def test_weekly_clutch_uses_full_eligible_league_population() -> None:
    """A rare start is normalized across every eligible league in its cell."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'rare_rb', 'RB', 1, 1, 1, 1.0, 100.0),
          ('league_b', 2025, 1, 'rare_rb', 'RB', 1, 0, 0, NULL, NULL),
          ('league_a', 2025, 1, 'other_a', 'RB', 1, 0, 0, NULL, NULL),
          ('league_b', 2025, 1, 'other_b', 'RB', 1, 0, 0, NULL, NULL)
        """
    )

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="rare_rb",
    )

    assert connection.execute(
        """
        SELECT cell.clutch_weekly_average
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchone() == (50.0,)


def test_season_core_batch_sums_weekly_population_clutch() -> None:
    """Season Clutch sums weekly values normalized by eligible leagues."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'rare_rb', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 100.0),
          ('b', 2025, 1, 'rare_rb', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL),
          ('a', 2025, 1, 'other_a', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2025, 1, 'other_b', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('a', 2025, 2, 'other_a', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2025, 2, 'other_b', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats AS
        SELECT * FROM (VALUES
          ('rare_rb', 2025, 1, 'REG', 'SF', 20, 0, 0),
          ('rare_rb', 2025, 2, 'REG', 'SF', 20, 0, 0)
        ) AS t(NFL_player_id, year, week, season_type, nfl_team, offense_snaps, defense_snaps, special_teams_snaps)
        """
    )

    materialize_season_core_cohort_position_batch(
        connection, source_table='facts', nfl_stats_table='nfl_stats',
        output_table='season_batch', year=2025, position='RB', split_threshold=2,
    )

    assert connection.execute(
        """
        SELECT cell.eligible_league_weeks, cell.expected_starts, cell.clutch_season_sum
        FROM season_batch, UNNEST(cohort_cells) AS u(cell)
        WHERE NFL_player_id = 'rare_rb'
          AND cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone() == (4, 0.5, 50.0)


def test_weekly_outer_excludes_identityless_zero_point_rows_from_numerator_and_denominator() -> None:
    """An expanded player-pool row is not a roster or eligibility fact."""

    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
          team_points DOUBLE, fantasy_points DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1, 1, 1.0, 2.0, 'A Manager', 'a', 'A Team', 110.0, 20.0),
          ('league_b', 2025, 1, 'cmc', 'RB', 1, 0, 0, NULL, NULL, 'B Manager', 'b', 'B Team', 90.0, 0.0),
          ('league_b', 2025, 1, 'other', 'RB', 1, 0, 0, NULL, NULL, 'B Manager', 'b', 'B Team', 90.0, 0.0),
          ('synthetic_league', 2025, 1, 'old_rb', 'RB', 1, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, 0.0)
        """
    )

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="cmc",
    )

    assert connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_leagues, cell.started_leagues
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchone() == (2, 1, 1)

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="synthetic_outer",
        year=2025,
        week=1,
        player_id="old_rb",
    )

    assert connection.execute("SELECT COUNT(*) FROM synthetic_outer").fetchone() == (0,)


def test_weekly_outer_excludes_inactive_identityless_player_pool_rows_only_when_active_lookup_proves_them_invalid() -> None:
    """Blank team identity is invalid only for players absent from the season's NFL week map."""

    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
          manager VARCHAR, team_key VARCHAR, team_name VARCHAR,
          team_points DOUBLE, fantasy_points DOUBLE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE player_active_week (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER
        )
        """
    )
    connection.execute("INSERT INTO player_active_week VALUES ('active_handcuff', 2025, 2)")
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1, 1, 1.0, 2.0, 'A Manager', 'a', 'A Team', 110.0, 20.0),
          ('artifact', 2025, 1, 'retired_rb', 'RB', 1, 1, 0, NULL, NULL, 'Injected Manager', NULL, NULL, 95.0, NULL),
          ('league_b', 2025, 1, 'active_handcuff', 'RB', 1, 1, 0, NULL, NULL, 'Real Manager', NULL, NULL, 95.0, 0.0)
        """
    )

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="retired_rb",
    )
    assert connection.execute("SELECT COUNT(*) FROM weekly_outer").fetchone() == (0,)

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="active_outer",
        year=2025,
        week=1,
        player_id="active_handcuff",
    )
    assert connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_leagues
        FROM active_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchone() == (2, 1)


def test_weekly_outer_rows_emit_one_row_per_existing_player_week() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1, 1, 1.0, 1.0),
          ('league_b', 2025, 1, 'other_1', 'RB', 1, 0, 0, NULL, NULL),
          ('league_a', 2025, 2, 'cmc', 'RB', 1, 1, 1, 0.0, 2.0),
          ('league_b', 2025, 2, 'other_2', 'RB', 1, 0, 0, NULL, NULL)
        """
    )

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=None,
        player_id="cmc",
    )

    assert connection.execute(
        "SELECT NFL_player_id, year, week, array_length(cohort_cells) FROM weekly_outer ORDER BY week"
    ).fetchall() == [("cmc", 2025, 1, 1), ("cmc", 2025, 2, 1)]


def test_weekly_outer_rows_respect_cached_regular_game_week_map() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, 1, 1, 1.0, 1.0),
          ('league_b', 2025, 1, 'other_1', 'RB', 1, 0, 0, NULL, NULL),
          ('league_a', 2025, 2, 'cmc', 'RB', 1, 1, 0, 0.0, NULL),
          ('league_b', 2025, 2, 'other_2', 'RB', 1, 0, 0, NULL, NULL),
          ('league_a', 2025, 19, 'cmc', 'RB', 1, 1, 1, 1.0, 2.0),
          ('league_b', 2025, 19, 'other_19', 'RB', 1, 0, 0, NULL, NULL)
        """
    )
    connection.execute(
        """
        CREATE TABLE regular_player_weeks (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER
        )
        """
    )
    connection.execute("INSERT INTO regular_player_weeks VALUES ('cmc', 2025, 1), ('cmc', 2025, 2)")

    materialize_weekly_outer_rows(
        connection,
        source_table="facts",
        valid_player_weeks_table="regular_player_weeks",
        output_table="weekly_outer",
        year=2025,
        week=None,
        player_id="cmc",
    )

    assert connection.execute(
        "SELECT NFL_player_id, year, week FROM weekly_outer ORDER BY week"
    ).fetchall() == [("cmc", 2025, 1), ("cmc", 2025, 2)]


def test_season_outer_row_uses_cached_team_games_and_reconciles_metrics() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
          is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc', 'RB', 1, NULL, 1, 1.0, 1.0, 0, 0),
          ('league_b', 2025, 1, 'cmc', 'RB', 1, 0,    0, NULL, NULL, 0, 0),
          ('league_a', 2025, 2, 'cmc', 'RB', 1, 1,    1, 0.0, 3.0, 1, 1),
          ('league_b', 2025, 2, 'cmc', 'RB', 1, 0,    0, NULL, NULL, 0, 0),
          ('league_a', 2025, 3, 'cmc', 'RB', 1, 1,    1, 1.0, 9.0, 1, 0),
          ('league_b', 2025, 3, 'cmc', 'RB', 1, 1,    1, 1.0, 7.0, 1, 0),
          ('league_a', 2025, 1, 'other_a', 'RB', 1, 0, 0, NULL, NULL, 0, 0),
          ('league_b', 2025, 1, 'other_b', 'RB', 1, 0, 0, NULL, NULL, 0, 0),
          ('league_a', 2025, 2, 'other_a', 'RB', 1, 0, 0, NULL, NULL, 0, 0),
          ('league_b', 2025, 2, 'other_b', 'RB', 1, 0, 0, NULL, NULL, 0, 0),
          ('league_a', 2025, 3, 'other_a', 'RB', 1, 0, 0, NULL, NULL, 0, 0),
          ('league_b', 2025, 3, 'other_b', 'RB', 1, 0, 0, NULL, NULL, 0, 0)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR,
          nfl_team VARCHAR, offense_snaps INTEGER, defense_snaps INTEGER,
          special_teams_snaps INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO nfl_stats VALUES
          ('cmc', 2025, 1, 'REG', 'SF', 10, 0, 0),
          ('cmc', 2025, 2, 'REG', 'SF', 10, 0, 0),
          ('other_sf_1', 2025, 1, 'REG', 'SF', 1, 0, 0),
          ('other_sf_2', 2025, 2, 'REG', 'SF', 1, 0, 0),
          ('other_sf_3', 2025, 3, 'REG', 'SF', 1, 0, 0),
          ('cmc', 2025, 19, 'POST', 'SF', 10, 0, 0)
        """
    )

    materialize_season_outer_rows(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_outer",
        year=2025,
        player_id="cmc",
    )

    row = connection.execute(
        """
        SELECT
          NFL_player_id, year, position,
          cell.eligible_leagues, cell.rostered_league_weeks,
          cell.eligible_league_weeks, cell.started_league_weeks,
          cell.valid_started_outcomes, cell.win_equivalent,
          cell.roster_rate_pct, cell.start_rate_pct, cell.healthy_start_rate_pct,
          cell.win_rate_pct, cell.expected_starts, cell.expected_wins,
          cell.expected_losses, cell.clutch_season_sum,
          cell.playoff_leagues, cell.champ_leagues,
          cell.active_weeks, cell.inactive_weeks
        FROM season_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchone()
    assert row == (
        "cmc", 2025, "RB",
        2, 4, 6, 4, 4, 3.0,
        66.66666666666667, 66.66666666666667, 50.0, 75.0,
        2.0, 1.5, 0.5, 10.0,
        2, 1, 2, 1,
    )


def test_career_outer_row_sums_counts_but_averages_annual_playoff_champ_rates() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_outer AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id, 2024::INTEGER AS year, 'RB'::VARCHAR AS position,
          [struct_pack(
            cohort_key := 'ALL', eligible_leagues := 10::BIGINT,
            rostered_league_weeks := 10::BIGINT, eligible_league_weeks := 20::BIGINT,
            started_league_weeks := 10::BIGINT, valid_started_outcomes := 10::BIGINT,
            win_equivalent := 6.0, roster_rate_pct := 50.0, start_rate_pct := 50.0,
            healthy_started_league_weeks := 5::BIGINT, healthy_eligible_league_weeks := 10::BIGINT,
            healthy_start_rate_pct := 50.0, win_rate_pct := 60.0,
            expected_starts := 0.5, expected_wins := 0.3, expected_losses := 0.2,
            clutch_season_sum := 3.0, playoff_leagues := 3::BIGINT, champ_leagues := 1::BIGINT,
            playoff_rate_pct := 30.0, champ_rate_pct := 10.0,
            active_weeks := 2::BIGINT, inactive_weeks := 1::BIGINT
          )] AS cohort_cells
        UNION ALL
        SELECT
          'cmc'::VARCHAR, 2025::INTEGER, 'RB'::VARCHAR,
          [struct_pack(
            cohort_key := 'ALL', eligible_leagues := 10::BIGINT,
            rostered_league_weeks := 30::BIGINT, eligible_league_weeks := 30::BIGINT,
            started_league_weeks := 15::BIGINT, valid_started_outcomes := 15::BIGINT,
            win_equivalent := 9.0, roster_rate_pct := 100.0, start_rate_pct := 50.0,
            healthy_started_league_weeks := 10::BIGINT, healthy_eligible_league_weeks := 20::BIGINT,
            healthy_start_rate_pct := 50.0, win_rate_pct := 60.0,
            expected_starts := 0.5, expected_wins := 0.3, expected_losses := 0.2,
            clutch_season_sum := 7.0, playoff_leagues := 6::BIGINT, champ_leagues := 2::BIGINT,
            playoff_rate_pct := 60.0, champ_rate_pct := 20.0,
            active_weeks := 3::BIGINT, inactive_weeks := 0::BIGINT
          )]
        """
    )

    materialize_career_outer_rows(
        connection,
        season_outer_table="season_outer",
        output_table="career_outer",
        player_id="cmc",
    )

    row = connection.execute(
        """
        SELECT NFL_player_id, position,
          cell.roster_rate_pct, cell.start_rate_pct, cell.healthy_start_rate_pct,
          cell.win_rate_pct, cell.expected_starts, cell.expected_wins,
          cell.expected_losses, cell.clutch_career_sum, cell.champ_rate_pct,
          cell.expected_champs, cell.playoff_rate_pct, cell.expected_playoffs,
          cell.qualifying_seasons, cell.active_weeks, cell.inactive_weeks
        FROM career_outer, UNNEST(cohort_cells) AS u(cell)
        """
    ).fetchone()
    assert row[:11] == (
        "cmc", "RB", 80.0, 50.0, 50.0, 60.0,
        1.0, 0.6, 0.4, 10.0, 15.0,
    )
    assert row[11] == pytest.approx(0.3)
    assert row[12] == 45.0
    assert row[13] == pytest.approx(0.9)
    assert row[14:] == (2, 5, 1)


def test_career_cells_are_keyed_by_requested_cohort_after_annual_pooling() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_outer AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id,
          2025::INTEGER AS year,
          'RB'::VARCHAR AS position,
          struct_pack(
            teams_08tm_split := 0, teams_10tm_split := 0,
            teams_12tm_split := 0, teams_14tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ) AS cohort_profile,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL',
            eligible_leagues := 10::BIGINT,
            rostered_league_weeks := 10::BIGINT, eligible_league_weeks := 20::BIGINT,
            started_league_weeks := 10::BIGINT,
            healthy_started_league_weeks := 5::BIGINT, healthy_eligible_league_weeks := 10::BIGINT,
            valid_started_outcomes := 10::BIGINT, win_equivalent := 6.0,
            expected_starts := 0.5, expected_wins := 0.3, expected_losses := 0.2,
            clutch_season_sum := 3.12345678945, active_weeks := 2::BIGINT, inactive_weeks := 1::BIGINT
          )] AS cohort_cells
        """
    )

    materialize_career_core_cohort_cells(
        connection,
        season_outer_table="season_outer",
        output_table="career_outer",
        player_id="cmc",
    )

    selected = connection.execute(
        """
        SELECT cell.eligible_league_weeks, cell.rostered_league_weeks,
          cell.expected_starts, cell.expected_wins, cell.clutch_career_sum,
          cell.qualifying_seasons
        FROM career_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='12tm' AND cell.q_roster='flx' AND cell.q_scoring='half'
          AND cell.q_pass_td='4pt' AND cell.q_dynasty='redraft'
          AND cell.q_best_ball='managed'
        """
    ).fetchone()
    assert selected == (20, 10, 0.5, 0.3, 3.123456789, 1)


def test_weekly_core_cells_resolve_each_cohort_dimension_independently() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', NULL, 1, 1.0, 2.0),
          ('b', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1,    1, 0.0, 4.0),
          ('c', 2025, 1, 'cmc', 'RB', 1, '10tm', 'flx', 'ppr',  '4pt', '4po', 'dynasty', 'best_ball', 0,    0, NULL, NULL),
          ('a', 2025, 1, 'other', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL)
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="cmc",
        split_threshold=2,
    )

    rows = connection.execute(
        """
        SELECT
          cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
          cell.q_dynasty, cell.q_best_ball, cell.eligible_leagues,
          cell.rostered_leagues, cell.started_leagues, cell.win_equivalent,
          cell.roster_rate_pct, cell.start_rate_pct, cell.win_rate_pct,
          cell.expected_starts, cell.expected_wins, cell.expected_losses,
          cell.clutch_weekly_average
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        ORDER BY 1, 2, 3, 4, 5, 6
        """
    ).fetchall()
    full_pool = connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_leagues,
          cell.started_leagues, cell.win_equivalent,
          cell.roster_rate_pct, cell.start_rate_pct, cell.win_rate_pct
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
          AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
          AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
        """
    ).fetchone()
    assert full_pool == (3, 2, 2, 1.0, pytest.approx(66.66666666666667), pytest.approx(66.66666666666667), 50.0)
    assert len(rows) == 64
    cells = {row[:6]: row[6:] for row in rows}
    for dynasty, best_ball in (("ALL", "ALL"), ("ALL", "managed"), ("redraft", "ALL"), ("redraft", "managed")):
        assert cells[("12tm", "flx", "half", "4pt", dynasty, best_ball)] == (
            2, 2, 2, 1.0, 100.0, 100.0, 50.0, 1.0, 0.5, 0.5, 3.0,
        )
    assert cells[("ALL", "flx", "ALL", "4pt", "ALL", "ALL")] == (
        3, 2, 2, 1.0, pytest.approx(66.66666666666667),
        pytest.approx(66.66666666666667), 50.0,
        pytest.approx(2 / 3), pytest.approx(1 / 3), pytest.approx(1 / 3), 2.0,
    )
    # 10-team and PPR each miss the split floor.  The build must persist the
    # split profile beside the nested metric cells.  The API uses this built
    # profile to select an existing effective cell; it never counts leagues,
    # discovers a fallback, or duplicates metrics under every format key.
    requested_thin_format = connection.execute(
        """
        SELECT
          cohort_profile.teams_10tm_split,
          cohort_profile.roster_flx_split,
          cohort_profile.scoring_ppr_split,
          cohort_profile.pass_td_4pt_split,
          cohort_profile.dynasty_dynasty_split,
          cohort_profile.best_ball_best_ball_split
        FROM weekly_outer
        """
    ).fetchone()
    assert requested_thin_format == (
        0, 1, 0, 1, 0, 0,
    )
    assert connection.execute(
        "SELECT list_count(cohort_cells) FROM weekly_outer"
    ).fetchone()[0] <= 288
    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_batch",
        year=2025,
        week=None,
        player_id=None,
        split_threshold=2,
    )
    # The independent-pooling fixture still exercises CMC's cells; the one-row
    # "other" sample is now correctly outside the serving floor.
    assert connection.execute("SELECT NFL_player_id FROM weekly_batch ORDER BY 1").fetchall() == [
        ("cmc",),
    ]


def test_weekly_core_cells_select_the_literal_team_bucket_for_start_metrics() -> None:
    """Start-rate metrics retain literal team size despite a conflicting legacy tier."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts AS
        SELECT
          'league_a'::VARCHAR AS db_name,
          2025::INTEGER AS year,
          1::INTEGER AS week,
          'cmc'::VARCHAR AS NFL_player_id,
          'RB'::VARCHAR AS position,
          1::INTEGER AS cohort_position_eligible,
          '12tm'::VARCHAR AS cohort_teams,
          '12tm'::VARCHAR AS cohort_teams_rostered,
          '08tm'::VARCHAR AS cohort_teams_started,
          'flx'::VARCHAR AS cohort_roster,
          'half'::VARCHAR AS cohort_scoring,
          '4pt'::VARCHAR AS cohort_pass_td,
          '6po'::VARCHAR AS cohort_playoff_teams,
          'redraft'::VARCHAR AS cohort_dynasty,
          'managed'::VARCHAR AS cohort_best_ball,
          1::INTEGER AS is_rostered,
          1::INTEGER AS is_started,
          1.0::DOUBLE AS win,
          1.0::DOUBLE AS clutch_equity
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        """
        SELECT cell.start_rate_pct
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams = '12tm'
          AND cell.q_roster = 'flx'
          AND cell.q_scoring = 'half'
          AND cell.q_pass_td = '4pt'
          AND cell.q_dynasty = 'redraft'
          AND cell.q_best_ball = 'managed'
        """
    ).fetchone() == (100.0,)


def test_weekly_core_cells_use_the_literal_team_bucket_for_roster_metrics() -> None:
    """Roster rate keeps literal team size even if legacy aliases disagree."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts AS
        SELECT
          'league_a'::VARCHAR AS db_name, 2025::INTEGER AS year, 1::INTEGER AS week,
          'cmc'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
          1::INTEGER AS cohort_position_eligible,
          '12tm'::VARCHAR AS cohort_teams,
          '12tm'::VARCHAR AS cohort_teams_rostered,
          '08tm'::VARCHAR AS cohort_teams_started,
          'flx'::VARCHAR AS cohort_roster, 'half'::VARCHAR AS cohort_scoring,
          '4pt'::VARCHAR AS cohort_pass_td, '6po'::VARCHAR AS cohort_playoff_teams,
          'redraft'::VARCHAR AS cohort_dynasty, 'managed'::VARCHAR AS cohort_best_ball,
          1::INTEGER AS is_rostered, 1::INTEGER AS is_started,
          1.0::DOUBLE AS win, 1.0::DOUBLE AS clutch_equity
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        """
        SELECT cell.roster_rate_pct
        FROM weekly_outer, UNNEST(roster_cells) AS u(cell)
        WHERE cell.q_teams = '12tm'
          AND cell.q_roster = 'flx'
          AND cell.q_scoring = 'half'
          AND cell.q_pass_td = '4pt'
          AND cell.q_dynasty = 'redraft'
          AND cell.q_best_ball = 'managed'
        """
    ).fetchone() == (100.0,)


def test_compact_core_selector_indexes_all_four_visible_team_tiers() -> None:
    """The API access path has one ordinal for every visible core request."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE compact AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id,
          2025::INTEGER AS year,
          1::INTEGER AS week,
          'RB'::VARCHAR AS position,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
            q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL'
          )] AS cohort_cells
        """
    )

    materialize_compact_core_selector_indices(connection, output_table="compact")

    assert connection.execute(
        "SELECT list_count(core_selector_indices) FROM compact"
    ).fetchone() == (288,)



def test_compact_core_selector_regeneration_replaces_stale_ordinals() -> None:
    """Compaction may shorten cells; its second selector pass must overwrite old pointers."""

    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE compact AS
        SELECT 'p1'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year,
          'RB'::VARCHAR AS position,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
            q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL'
          )] AS cohort_cells,
          list_transform(range(1, 289), x -> 999::USMALLINT) AS core_selector_indices
        """
    )

    materialize_compact_core_selector_indices(connection, output_table="compact")

    assert connection.execute(
        """
        SELECT list_extract(core_selector_indices, 89),
               COUNT(*) FILTER (
                 WHERE list_extract(core_selector_indices, request_ordinal)
                   NOT BETWEEN 1 AND list_count(cohort_cells)
               )
        FROM compact
        CROSS JOIN range(1, 289) AS request(request_ordinal)
        GROUP BY ALL
        """
    ).fetchone() == (1, 0)


def test_compact_grade_selector_indexes_all_visible_team_and_bracket_requests() -> None:
    """The API grade access path is the 4 Ã— 3 Ã— 3 Ã— 2 Ã— 2 Ã— 2 Ã— 3 grid."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE compact AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id,
          2025::INTEGER AS year,
          'RB'::VARCHAR AS position,
          struct_pack(
            teams_08tm_split := 0, teams_10tm_split := 0,
            teams_12tm_split := 0, teams_14tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ) AS cohort_profile,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
            q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
            q_playoff_teams := 'ALL', eligible_leagues := 150::BIGINT,
            playoff_leagues := 0::BIGINT, champ_leagues := 0::BIGINT,
            playoff_rate_pct := 0.0, champ_rate_pct := 0.0
          )] AS grade_cells
        """
    )

    materialize_compact_grade_selector_indices(connection, output_table="compact")

    assert connection.execute(
        "SELECT list_count(grade_selector_indices) FROM compact"
    ).fetchone() == (864,)


def test_weekly_core_quantizes_clutch_before_storing_compact_cells() -> None:
    """Tiny floating aggregation order changes must not change a compact artifact."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a',2025,1,'p1','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.1),
          ('b',2025,1,'p1','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.2),
          ('c',2025,1,'p1','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.2)
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="p1",
        split_threshold=1,
    )

    assert connection.execute(
        """
        SELECT cell.clutch_weekly_average
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone() == (0.166666667,)


def test_weekly_core_cells_keep_week_specific_eligibility_denominators() -> None:
    """A league absent in week 2 cannot inflate that week's CMC denominator."""

    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('league_a', 2025, 1, 'cmc',   'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('league_b', 2025, 1, 'other', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('league_a', 2025, 2, 'cmc',   'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0)
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=None,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        """
        SELECT week, cell.eligible_leagues
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        ORDER BY week
        """
    ).fetchall() == [(1, 2), (2, 1)]


def test_weekly_core_cells_exclude_postseason_with_cached_regular_week_map() -> None:
    """Postseason roster facts cannot create weekly UI outer rows."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1,  'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0),
          ('a', 2025, 19, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0)
        """
    )
    connection.execute(
        """
        CREATE TABLE regular_player_weeks (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER
        )
        """
    )
    connection.execute("INSERT INTO regular_player_weeks VALUES ('cmc', 2025, 1)")

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        valid_player_weeks_table="regular_player_weeks",
        output_table="weekly_outer",
        year=2025,
        week=None,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        "SELECT NFL_player_id, year, week FROM weekly_outer ORDER BY week"
    ).fetchall() == [("cmc", 2025, 1)]


def test_weekly_core_cells_canonicalize_idp_subpositions() -> None:
    """DE and DL source facts must serve as one canonical DL player row."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'myles', 'DE', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0),
          ('b', 2025, 1, 'myles', 'DL', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0)
        """
    )

    materialize_weekly_core_cohort_cells(
        connection,
        source_table="facts",
        output_table="weekly_outer",
        year=2025,
        week=1,
        player_id="myles",
        split_threshold=1,
    )

    assert connection.execute(
        "SELECT NFL_player_id, position FROM weekly_outer"
    ).fetchall() == [("myles", "DL")]
    assert connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_leagues, cell.started_leagues
        FROM weekly_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone() == (2, 2, 2)


def test_weekly_core_position_lane_canonicalizes_idp_subpositions() -> None:
    """A position lane must include raw DE and DL facts as canonical DL."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'myles', 'DE', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0),
          ('b', 2025, 1, 'myles', 'DL', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0),
          ('a', 2025, 1, 'other', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0)
        """
    )
    materialize_weekly_core_cohort_cells(
        connection, source_table="facts", output_table="weekly_dl_batch",
        year=2025, week=1, player_id=None, position="DL", split_threshold=1,
    )
    assert connection.execute(
        "SELECT NFL_player_id, position FROM weekly_dl_batch"
    ).fetchall() == [("myles", "DL")]


def test_season_core_cells_emit_full_pool_and_exact_selectors() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', NULL, 1, 1.0, 2.0),
          ('b', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1,    1, 0.0, 4.0),
          ('c', 2025, 1, 'cmc', 'RB', 1, '10tm', 'flx', 'ppr',  '4pt', '4po', 'dynasty', 'best_ball', 0,    0, NULL, NULL),
          ('d', 2025, 1, 'cmc', 'DL', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1,    1, 1.0, 1.0)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO nfl_stats VALUES
          ('cmc', 2025, 1, 'SF', 'REG', 20, 0, 0)
        """
    )

    materialize_season_core_cohort_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_core_outer",
        year=2025,
        player_id="cmc",
        position="RB",
        split_threshold=2,
    )

    assert connection.execute(
        "SELECT DISTINCT position FROM season_core_outer"
    ).fetchall() == [("RB",)]

    full = connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_league_weeks,
          cell.started_league_weeks, cell.valid_started_outcomes,
          cell.win_equivalent, cell.roster_rate_pct, cell.start_rate_pct,
          cell.healthy_start_rate_pct, cell.win_rate_pct,
          cell.expected_starts, cell.expected_wins, cell.expected_losses,
          cell.clutch_season_sum, cell.active_weeks, cell.inactive_weeks
        FROM season_core_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
          AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
          AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
        """
    ).fetchone()
    assert full == (
        3, 2, 2, 2, 1.0,
        pytest.approx(66.66666666666667), pytest.approx(66.66666666666667),
        pytest.approx(66.66666666666667), 50.0,
        pytest.approx(2 / 3), pytest.approx(1 / 3), pytest.approx(1 / 3),
        2.0, 1, 0,
    )
    exact = connection.execute(
        """
        SELECT cell.eligible_leagues, cell.rostered_league_weeks,
          cell.started_league_weeks, cell.win_equivalent
        FROM season_core_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams = '12tm' AND cell.q_roster = 'flx'
          AND cell.q_scoring = 'half' AND cell.q_pass_td = '4pt'
          AND cell.q_dynasty = 'redraft' AND cell.q_best_ball = 'managed'
        """
    ).fetchone()
    assert exact == (2, 2, 2, 1.0)
    profile = connection.execute(
        """
        SELECT
          cohort_profile.teams_10tm_split,
          cohort_profile.roster_flx_split,
          cohort_profile.scoring_ppr_split,
          cohort_profile.pass_td_4pt_split,
          cohort_profile.dynasty_dynasty_split,
          cohort_profile.best_ball_best_ball_split
        FROM season_core_outer
        """
    ).fetchone()
    assert profile == (0, 1, 0, 1, 0, 0)


def test_season_core_cells_keep_only_requested_or_all_audit_keys() -> None:
    """A fully split season remains bounded by the 288 core requests."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts
        SELECT
          'league_' || ROW_NUMBER() OVER (), 2025, 1, 'cmc', 'RB', 1,
          teams.q, roster.q, scoring.q, pass_td.q, '6po', dynasty.q, best_ball.q,
          1, 1, 1.0, 1.0
        FROM (VALUES ('10tm'), ('12tm')) AS teams(q)
        CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) AS roster(q)
        CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) AS scoring(q)
        CROSS JOIN (VALUES ('4pt'), ('6pt')) AS pass_td(q)
        CROSS JOIN (VALUES ('redraft'), ('dynasty')) AS dynasty(q)
        CROSS JOIN (VALUES ('managed'), ('best_ball')) AS best_ball(q)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('cmc', 2025, 1, 'SF', 'REG', 20, 0, 0)")

    materialize_season_core_cohort_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_core_outer",
        year=2025,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        "SELECT list_count(cohort_cells) FROM season_core_outer"
    ).fetchone()[0] <= 288


def test_season_grade_cells_keep_only_requested_or_all_audit_keys() -> None:
    """A fully split grade output is bounded to core requests times brackets."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts
        SELECT
          'league_' || ROW_NUMBER() OVER (), 2025, 1, 'cmc', 'RB', 1,
          teams.q, roster.q, scoring.q, pass_td.q, bracket.q, dynasty.q, best_ball.q,
          1, 0, 0
        FROM (VALUES ('10tm'), ('12tm')) AS teams(q)
        CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) AS roster(q)
        CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) AS scoring(q)
        CROSS JOIN (VALUES ('4pt'), ('6pt')) AS pass_td(q)
        CROSS JOIN (VALUES ('4po'), ('6po'), ('8po')) AS bracket(q)
        CROSS JOIN (VALUES ('redraft'), ('dynasty')) AS dynasty(q)
        CROSS JOIN (VALUES ('managed'), ('best_ball')) AS best_ball(q)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('cmc', 2025, 1, 'SF', 'REG')")

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_grade_outer",
        year=2025,
        player_id="cmc",
        split_threshold=1,
    )

    assert connection.execute(
        "SELECT list_count(grade_cells) FROM season_grade_outer"
    ).fetchone()[0] <= 864


def test_season_grade_selector_pools_the_lowest_dimension_needed_for_150_leagues() -> None:
    """A thin exact grade cell must resolve to its six-dimension >=150 parent."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts
        SELECT 'target_ppr_' || i, 2025, 15, 'p1', 'WR', 1,
               '12tm', 'flx', 'ppr', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1
        FROM range(61) r(i)
        UNION ALL
        SELECT 'other_ppr_' || i, 2025, 15, 'p1', 'WR', 1,
               '10tm', 'flx', 'ppr', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1
        FROM range(100) r(i)
        UNION ALL
        SELECT 'target_std_' || i, 2025, 15, 'p1', 'WR', 1,
               '12tm', 'flx', 'std', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1
        FROM range(96) r(i)
        """
    )
    connection.execute(
        "CREATE TABLE player_active_week AS SELECT 'p1' AS NFL_player_id, 2025 AS year, 15 AS week"
    )

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_regular_weeks_table="player_active_week",
        output_table="season_grade_outer",
        year=2025,
        player_id="p1",
        position="WR",
        split_threshold=150,
    )
    materialize_compact_grade_selector_indices(
        connection,
        output_table="season_grade_outer",
    )

    # Request ordinal 482 is 12tm / FLX / PPR / 4pt / redraft / managed / 6po.
    # Its exact cell has 61 leagues.  Two six-dimension parents clear 150;
    # the selector keeps 12tm and pools scoring because its 157-league parent
    # is narrower than the alternative 161-league PPR parent.
    row = connection.execute(
        """
        SELECT cell.q_scoring, cell.eligible_leagues
        FROM season_grade_outer,
             UNNEST([list_extract(grade_cells, list_extract(grade_selector_indices, 482))]) AS u(cell)
        """
    ).fetchone()
    assert row == ("ALL", 157)


def test_season_grade_cells_use_cached_regular_week_lookup_without_stats_scan() -> None:
    """Playoff/champ credits can be built from the cache's player-week lookup."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1),
          ('b', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, 0, 0)
        """
    )
    connection.execute(
        "CREATE TABLE player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute("INSERT INTO player_active_week VALUES ('cmc', 2025, 15)")

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_regular_weeks_table="player_active_week",
        output_table="season_grade_outer",
        year=2025,
        player_id="cmc",
        position="RB",
        split_threshold=2,
    )

    row = connection.execute(
        """
        SELECT cell.eligible_leagues, cell.playoff_leagues, cell.champ_leagues
        FROM season_grade_outer, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
          AND cell.q_playoff_teams='ALL'
        """
    ).fetchone()
    assert row == (2, 1, 1)


def test_season_grade_position_batch_forwards_cached_regular_week_lookup() -> None:
    """The production position batch must not silently fall back to NFL stats."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts AS
        SELECT * FROM (
          VALUES
            ('a', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1),
            ('b', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, 0, 0)
        ) AS t(db_name, year, week, NFL_player_id, position, cohort_position_eligible,
          cohort_teams, cohort_roster, cohort_scoring, cohort_pass_td, cohort_playoff_teams,
          cohort_dynasty, cohort_best_ball, is_rostered, is_started, is_playoffs, champion)
        """
    )
    connection.execute(
        "CREATE TABLE player_active_week AS SELECT 'cmc'::VARCHAR AS NFL_player_id, 2025 AS year, 15 AS week"
    )

    materialize_season_bracket_grade_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_regular_weeks_table="player_active_week",
        output_table="grade_batch",
        year=2025,
        position="RB",
        split_threshold=2,
    )

    player_id, cell_count = connection.execute(
        "SELECT NFL_player_id, list_count(grade_cells) FROM grade_batch"
    ).fetchone()
    assert player_id == "cmc"
    assert 1 <= cell_count <= 577


def test_season_core_position_batch_rolls_up_multiple_players() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'p1', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('b', 2025, 1, 'p1', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0.0, 4.0),
          ('c', 2025, 1, 'p1', 'RB', 1, '10tm', 'flx', 'ppr',  '4pt', '4po', 'dynasty', 'best_ball', 0, 0, NULL, NULL),
          ('a', 2025, 1, 'p2', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 3.0),
          ('b', 2025, 1, 'p2', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL),
          ('c', 2025, 1, 'p2', 'RB', 1, '10tm', 'flx', 'ppr',  '4pt', '4po', 'dynasty', 'best_ball', 0, 0, NULL, NULL)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('p1', 2025, 1, 'SF', 'REG', 20, 0, 0), ('p2', 2025, 1, 'SF', 'REG', 20, 0, 0)")

    materialize_season_core_cohort_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_batch",
        year=2025,
        position="RB",
        split_threshold=2,
    )

    rows = connection.execute(
        """
        SELECT o.NFL_player_id, cell.eligible_leagues, cell.rostered_league_weeks,
          cell.started_league_weeks, cell.win_equivalent, cell.active_weeks, cell.inactive_weeks
        FROM season_batch o, UNNEST(o.cohort_cells) AS u(cell)
        WHERE cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
          AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
          AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
        ORDER BY 1
        """
    ).fetchall()
    assert rows == [("p1", 3, 2, 2, 1.0, 1, 0), ("p2", 3, 2, 1, 1.0, 1, 0)]
    profile = connection.execute(
        """
        SELECT cohort_profile.teams_12tm_split,
          cohort_profile.roster_flx_split,
          cohort_profile.scoring_half_split,
          cohort_profile.pass_td_4pt_split,
          cohort_profile.dynasty_redraft_split,
          cohort_profile.best_ball_managed_split
        FROM season_batch
        WHERE NFL_player_id = 'p1'
        """
    ).fetchone()
    assert profile == (1, 1, 1, 1, 1, 1)


def test_season_core_position_batch_uses_cached_calendar_lookups_without_stats_scan() -> None:
    """The cache's team-game and active-week tables fully define season availability."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('b', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0.0, 4.0),
          ('a', 2025, 2, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL),
          ('b', 2025, 2, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL)
        """
    )
    connection.execute(
        "CREATE TABLE player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute(
        "INSERT INTO player_team_game_week VALUES ('cmc', 2025, 1), ('cmc', 2025, 2), ('cmc', 2025, 3)"
    )
    connection.execute(
        "CREATE TABLE player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute("INSERT INTO player_active_week VALUES ('cmc', 2025, 1), ('cmc', 2025, 3)")

    materialize_season_core_cohort_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_team_game_week_table="player_team_game_week",
        player_active_week_table="player_active_week",
        output_table="season_batch",
        year=2025,
        position="RB",
        split_threshold=2,
    )

    row = connection.execute(
        """
        SELECT cell.active_weeks, cell.inactive_weeks,
          cell.eligible_league_weeks, cell.started_league_weeks,
          cell.healthy_started_league_weeks, cell.healthy_eligible_league_weeks,
          cell.expected_starts
        FROM season_batch, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone()
    # NFL week 3 is outside this league's eligible fantasy calendar. It cannot
    # inflate active weeks or expected starts.
    assert row == (1, 1, 4, 2, 2, 2, 0.5)


def test_season_core_position_batch_excludes_bye_only_players() -> None:
    """A roster fact occurring only on a bye cannot create a season player row."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 11, 'missing_lb', 'LB', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL),
          ('b', 2025, 11, 'missing_lb', 'LB', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL)
        """
    )
    connection.execute(
        "CREATE TABLE player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute(
        "INSERT INTO player_team_game_week VALUES ('missing_lb', 2025, 1), ('missing_lb', 2025, 2)"
    )
    connection.execute(
        "CREATE TABLE player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute("INSERT INTO player_active_week VALUES ('missing_lb', 2025, 1)")
    materialize_season_core_cohort_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_team_game_week_table="player_team_game_week",
        player_active_week_table="player_active_week",
        output_table="season_batch",
        year=2025,
        position="LB",
        split_threshold=2,
    )

    assert connection.execute("SELECT COUNT(*) FROM season_batch").fetchone()[0] == 0


def test_season_grade_batch_excludes_bye_only_players() -> None:
    """Grades must not manufacture an outer row from a bye-only roster fact."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 11, 'missing_lb', 'LB', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, 0, 0),
          ('b', 2025, 11, 'missing_lb', 'LB', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, 0, 0)
        """
    )
    connection.execute(
        "CREATE TABLE player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute(
        "INSERT INTO player_team_game_week VALUES ('missing_lb', 2025, 1), ('missing_lb', 2025, 2)"
    )

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_regular_weeks_table="player_team_game_week",
        output_table="season_grade_batch",
        year=2025,
        player_id=None,
        position="LB",
        split_threshold=2,
    )

    assert connection.execute("SELECT COUNT(*) FROM season_grade_batch").fetchone()[0] == 0


def test_season_core_position_batch_excludes_bye_rows_from_start_denominator() -> None:
    """A cache roster row in a player bye cannot create a season numerator or denominator."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1,  'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('a', 2025, 7,  'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, NULL, NULL)
        """
    )
    connection.execute(
        "CREATE TABLE player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute("INSERT INTO player_team_game_week VALUES ('cmc', 2025, 1)")
    connection.execute(
        "CREATE TABLE player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    connection.execute("INSERT INTO player_active_week VALUES ('cmc', 2025, 1)")

    materialize_season_core_cohort_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="must_not_be_scanned",
        player_team_game_week_table="player_team_game_week",
        player_active_week_table="player_active_week",
        output_table="season_batch",
        year=2025,
        position="RB",
        split_threshold=1,
    )

    row = connection.execute(
        """
        SELECT cell.rostered_league_weeks, cell.eligible_league_weeks,
          cell.started_league_weeks, cell.healthy_eligible_league_weeks
        FROM season_batch, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone()
    assert row == (1, 1, 1, 1)


def test_season_core_position_batch_excludes_never_rostered_players() -> None:
    """A real but never-rostered player cannot create a compact season row."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 1.0),
          ('a', 2025, 1, 'depth', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL)
        """
    )
    connection.execute(
        """
        CREATE TABLE stats AS
        SELECT * FROM (
          VALUES
            ('cmc', 2025, 1, 'REG', 'SF', 1, 0, 0),
            ('depth', 2025, 1, 'REG', 'SF', 1, 0, 0)
        ) AS t(NFL_player_id, year, week, season_type, nfl_team, offense_snaps, defense_snaps, special_teams_snaps)
        """
    )

    materialize_season_core_cohort_position_batch(
        connection, source_table="facts", nfl_stats_table="stats",
        output_table="season_batch", year=2025, position="RB", split_threshold=1,
    )

    assert connection.execute(
        "SELECT NFL_player_id FROM season_batch ORDER BY NFL_player_id"
    ).fetchall() == [("cmc",)]


def test_season_core_position_batch_canonicalizes_idp_subpositions() -> None:
    """A DL batch must pool raw DE/DT/DL aliases into canonical DL."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 1, 'myles', 'DE', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('b', 2025, 1, 'myles', 'DL', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0.0, 4.0)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('myles', 2025, 1, 'CLE', 'REG', 0, 40, 0)")
    materialize_season_core_cohort_position_batch(
        connection, source_table="facts", nfl_stats_table="nfl_stats",
        output_table="season_batch", year=2025, position="DL", split_threshold=2,
    )
    assert connection.execute(
        """
        SELECT NFL_player_id, position, cell.eligible_leagues,
          cell.rostered_league_weeks, cell.started_league_weeks
        FROM season_batch, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL'
          AND cell.q_scoring='ALL' AND cell.q_pass_td='ALL'
          AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone() == ('myles', 'DL', 2, 2, 2)


def test_season_grade_position_batch_canonicalizes_idp_subpositions() -> None:
    """The bracket batch must use the same DL normalization as core metrics."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR, is_started INTEGER,
          is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 15, 'myles', 'DE', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0),
          ('b', 2025, 15, 'myles', 'DL', 1, '12tm', 'idp', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1)
        """
    )
    connection.execute("CREATE TABLE nfl_stats (NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR)")
    connection.execute("INSERT INTO nfl_stats VALUES ('myles', 2025, 15, 'REG')")
    materialize_season_bracket_grade_position_batch(
        connection, source_table="facts", nfl_stats_table="nfl_stats",
        output_table="grade_batch", year=2025, position="DL", split_threshold=2,
    )
    assert connection.execute(
        """
        SELECT NFL_player_id, position, cell.eligible_leagues,
          cell.playoff_leagues, cell.champ_leagues
        FROM grade_batch, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_playoff_teams='ALL'
        """
    ).fetchone() == ('myles', 'DL', 2, 2, 1)


def test_season_grade_position_batch_excludes_never_rostered_players() -> None:
    """A never-rostered player cannot receive season playoff/champ grades."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR, is_rostered INTEGER,
          is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1, 1),
          ('a', 2025, 15, 'depth', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, 0, 0)
        """
    )
    connection.execute("CREATE TABLE nfl_stats (NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR)")
    connection.execute("INSERT INTO nfl_stats VALUES ('cmc', 2025, 15, 'REG'), ('depth', 2025, 15, 'REG')")

    materialize_season_bracket_grade_position_batch(
        connection, source_table="facts", nfl_stats_table="nfl_stats",
        output_table="grade_batch", year=2025, position="RB", split_threshold=1,
    )

    assert connection.execute(
        "SELECT NFL_player_id FROM grade_batch ORDER BY NFL_player_id"
    ).fetchall() == [("cmc",)]


def test_season_core_position_batch_discards_unreachable_cube_cells() -> None:
    """Fully split batch output stores <=288 reachable core requests."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    variants = list(product(
        ('10tm', '12tm'), ('flx', 'sflx', 'idp'), ('std', 'half', 'ppr'),
        ('4pt', '6pt'), ('redraft', 'dynasty'), ('managed', 'best_ball'),
    ))
    connection.executemany(
        "INSERT INTO facts VALUES (?, 2025, 1, 'p1', 'RB', 1, ?, ?, ?, ?, '6po', ?, ?, 1, 1, 1.0, 1.0)",
        [(f"l{i}", *variant) for i, variant in enumerate(variants)],
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('p1', 2025, 1, 'SF', 'REG', 20, 0, 0)")
    materialize_season_core_cohort_position_batch(
        connection, source_table="facts", nfl_stats_table="nfl_stats",
        output_table="season_batch", year=2025, position="RB", split_threshold=1,
    )
    assert connection.execute(
        "SELECT list_count(cohort_cells) FROM season_batch WHERE NFL_player_id='p1'"
    ).fetchone()[0] <= 288


def test_season_bracket_grades_emit_full_pool_alongside_exact_brackets() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR, is_started INTEGER,
          is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0, 1),
          ('b', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0),
          ('c', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '4po', 'redraft', 'managed', 0, 0, 0),
          ('d', 2025, 19, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '4po', 'redraft', 'managed', 1, 1, 1)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR
        )
        """
    )
    connection.execute(
        """
        INSERT INTO nfl_stats VALUES
          ('cmc', 2025, 15, 'REG'),
          ('cmc', 2025, 19, 'POST')
        """
    )

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_grade_outer",
        year=2025,
        player_id="cmc",
        split_threshold=2,
    )

    rows = connection.execute(
        """
        SELECT cell.q_playoff_teams, cell.eligible_leagues,
          cell.playoff_leagues, cell.champ_leagues,
          cell.playoff_rate_pct, cell.champ_rate_pct
        FROM season_grade_outer, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
          AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
          AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
        ORDER BY 1
        """
    ).fetchall()
    assert rows == [
        ("4po", 2, 0, 0, 0.0, 0.0),
        ("6po", 2, 2, 1, 100.0, 50.0),
        ("ALL", 4, 2, 1, 50.0, 25.0),
    ]
    materialize_season_bracket_grade_position_batch(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_grade_batch",
        year=2025,
        position="RB",
        split_threshold=2,
    )
    batch_rows = connection.execute(
        """
        SELECT cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
          cell.q_dynasty, cell.q_best_ball, cell.q_playoff_teams,
          cell.eligible_leagues, cell.playoff_leagues,
          cell.champ_leagues, cell.playoff_rate_pct, cell.champ_rate_pct
        FROM season_grade_batch, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
          AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
          AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
        ORDER BY 7
        """
    ).fetchall()
    assert batch_rows == [("ALL", "ALL", "ALL", "ALL", "ALL", "ALL", *row) for row in rows]


def test_season_grade_cells_carry_the_full_core_cohort_key() -> None:
    """A playoff grade must vary with the same format selected for core metrics."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_started INTEGER, is_playoffs INTEGER, champion INTEGER
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1),
          ('b', 2025, 15, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 0),
          ('c', 2025, 15, 'cmc', 'RB', 1, '10tm', 'flx', 'ppr',  '4pt', '4po', 'dynasty', 'best_ball', 0, 0, 0)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR
        )
        """
    )
    connection.execute("INSERT INTO nfl_stats VALUES ('cmc', 2025, 15, 'REG')")

    materialize_season_bracket_grade_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_grade_outer",
        year=2025,
        player_id="cmc",
        split_threshold=2,
    )

    exact = connection.execute(
        """
        SELECT
          cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
          cell.q_dynasty, cell.q_best_ball, cell.q_playoff_teams,
          cell.eligible_leagues, cell.playoff_leagues, cell.champ_leagues
        FROM season_grade_outer, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_teams = '12tm' AND cell.q_roster = 'flx'
          AND cell.q_scoring = 'half' AND cell.q_pass_td = '4pt'
          AND cell.q_dynasty = 'redraft' AND cell.q_best_ball = 'managed'
          AND cell.q_playoff_teams = '6po'
        """
    ).fetchone()
    assert exact == ("12tm", "flx", "half", "4pt", "redraft", "managed", "6po", 2, 2, 1)


def test_career_core_rates_are_unweighted_annual_averages() -> None:
    """Career rates must not let a high-exposure season outweigh an annual rate."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_outer AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id, 2024::INTEGER AS year, 'RB'::VARCHAR AS position,
          struct_pack(
            teams_10tm_split := 0, teams_12tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ) AS cohort_profile,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL',
            eligible_leagues := 1::BIGINT,
            rostered_league_weeks := 1::BIGINT, eligible_league_weeks := 10::BIGINT,
            started_league_weeks := 1::BIGINT,
            healthy_started_league_weeks := 1::BIGINT, healthy_eligible_league_weeks := 10::BIGINT,
            valid_started_outcomes := 1::BIGINT, win_equivalent := 0.1,
            expected_starts := 0.3, expected_wins := 0.1, expected_losses := 0.2,
            clutch_season_sum := 2.0, active_weeks := 3::BIGINT, inactive_weeks := 1::BIGINT
          )] AS cohort_cells
        UNION ALL
        SELECT
          'cmc'::VARCHAR, 2025::INTEGER, 'RB'::VARCHAR,
          struct_pack(
            teams_10tm_split := 0, teams_12tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ),
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL',
            eligible_leagues := 1::BIGINT,
            rostered_league_weeks := 90::BIGINT, eligible_league_weeks := 100::BIGINT,
            started_league_weeks := 90::BIGINT,
            healthy_started_league_weeks := 90::BIGINT, healthy_eligible_league_weeks := 100::BIGINT,
            valid_started_outcomes := 90::BIGINT, win_equivalent := 81.0,
            expected_starts := 12.0, expected_wins := 10.8, expected_losses := 1.2,
            clutch_season_sum := 8.0, active_weeks := 12::BIGINT, inactive_weeks := 0::BIGINT
          )]
        """
    )

    materialize_career_core_cohort_cells(
        connection,
        season_outer_table="season_outer",
        output_table="career_outer",
        player_id="cmc",
    )

    rates = connection.execute(
        """
        SELECT cell.roster_rate_pct, cell.start_rate_pct,
          cell.healthy_start_rate_pct, cell.win_rate_pct,
          cell.expected_starts, cell.expected_wins, cell.expected_losses,
          cell.clutch_career_sum, cell.active_weeks, cell.inactive_weeks
        FROM career_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='10tm' AND cell.q_roster='flx'
          AND cell.q_scoring='half' AND cell.q_pass_td='4pt'
          AND cell.q_dynasty='redraft' AND cell.q_best_ball='managed'
        """
    ).fetchone()
    assert rates == (
        50.0, 50.0, 50.0, 50.0,
        pytest.approx(12.3), pytest.approx(10.9), pytest.approx(1.4),
        10.0, 15, 1,
    )


def test_career_grade_cells_are_cohort_aware_and_average_annual_rates() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_outer AS
        SELECT
          'cmc'::VARCHAR AS NFL_player_id, 2024::INTEGER AS year, 'RB'::VARCHAR AS position,
          struct_pack(
            teams_10tm_split := 0, teams_12tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ) AS cohort_profile,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL',
            eligible_leagues := 10::BIGINT, playoff_leagues := 3::BIGINT, champ_leagues := 1::BIGINT,
            playoff_rate_pct := 30.0, champ_rate_pct := 10.0
          )] AS grade_cells
        UNION ALL
        SELECT
          'cmc'::VARCHAR, 2025::INTEGER, 'RB'::VARCHAR,
          struct_pack(
            teams_10tm_split := 0, teams_12tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ),
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL',
            eligible_leagues := 100::BIGINT, playoff_leagues := 60::BIGINT, champ_leagues := 20::BIGINT,
            playoff_rate_pct := 60.0, champ_rate_pct := 20.0
          )]
        """
    )

    materialize_career_cohort_grade_cells(
        connection,
        season_outer_table="season_outer",
        output_table="career_grade_outer",
        player_id="cmc",
    )

    grade = connection.execute(
        """
        SELECT cell.eligible_league_seasons, cell.playoff_credits, cell.champ_credits,
          cell.playoff_rate_pct, cell.champ_rate_pct,
          cell.expected_playoffs, cell.expected_champs
        FROM career_grade_outer, UNNEST(grade_cells) AS u(cell)
        WHERE cell.q_teams='12tm' AND cell.q_roster='flx'
          AND cell.q_scoring='half' AND cell.q_pass_td='4pt'
          AND cell.q_dynasty='redraft' AND cell.q_best_ball='managed'
          AND cell.q_playoff_teams='6po'
        """
    ).fetchone()
    assert grade == (110, 63, 21, 45.0, 15.0, pytest.approx(0.9), pytest.approx(0.3))


def test_career_materializers_build_every_player_set_wise() -> None:
    """The final lane may not loop players or silently omit a zero-credit player."""
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE season_outer AS
        SELECT * FROM (
          VALUES
            ('p1', 2025, 'RB', 1, 1, 1),
            ('p2', 2025, 'RB', 0, 0, 0)
        ) AS t(NFL_player_id, year, position, rostered, playoff, champ)
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TABLE season_outer AS
        SELECT NFL_player_id, year, position,
          struct_pack(
            teams_10tm_split := 0, teams_12tm_split := 0,
            roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
            scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
            pass_td_4pt_split := 0, pass_td_6pt_split := 0,
            dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
            best_ball_managed_split := 0, best_ball_best_ball_split := 0
          ) AS cohort_profile,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL', eligible_leagues := 1::BIGINT,
            rostered_league_weeks := rostered::BIGINT, eligible_league_weeks := 1::BIGINT,
            started_league_weeks := rostered::BIGINT, healthy_started_league_weeks := rostered::BIGINT,
            healthy_eligible_league_weeks := 1::BIGINT, valid_started_outcomes := rostered::BIGINT,
            win_equivalent := rostered::DOUBLE, expected_starts := rostered::DOUBLE,
            expected_wins := rostered::DOUBLE, expected_losses := 0.0,
            clutch_season_sum := 0.0, active_weeks := 1::BIGINT, inactive_weeks := 0::BIGINT
          )] AS cohort_cells,
          [struct_pack(
            q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL',
            q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL',
            eligible_leagues := 1::BIGINT, playoff_leagues := playoff::BIGINT,
            champ_leagues := champ::BIGINT, playoff_rate_pct := 100.0 * playoff,
            champ_rate_pct := 100.0 * champ
          )] AS grade_cells
        FROM season_outer
        """
    )
    materialize_career_core_cohort_cells(
        connection, season_outer_table="season_outer", output_table="career_core", player_id=None,
    )
    materialize_career_cohort_grade_cells(
        connection, season_outer_table="season_outer", output_table="career_grade", player_id=None,
    )
    assert connection.execute("SELECT NFL_player_id FROM career_core ORDER BY 1").fetchall() == [("p1",), ("p2",)]
    assert connection.execute("SELECT NFL_player_id FROM career_grade ORDER BY 1").fetchall() == [("p1",), ("p2",)]


def test_season_expected_starts_uses_all_week_start_rate_times_active_weeks() -> None:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE facts (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER,
          cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
          cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
          cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE
        )
        """
    )
    connection.execute(
        """
        INSERT INTO facts VALUES
          ('a', 2020, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1, 1.0, 2.0),
          ('b', 2020, 1, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('a', 2020, 2, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2020, 2, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('a', 2020, 3, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2020, 3, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('a', 2020, 4, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2020, 4, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('a', 2020, 5, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL),
          ('b', 2020, 5, 'cmc', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 0, 0, NULL, NULL)
        """
    )
    connection.execute(
        """
        CREATE TABLE nfl_stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          season_type VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
          special_teams_snaps BIGINT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO nfl_stats VALUES
          ('cmc', 2020, 1, 'CAR', 'REG', 20, 0, 0),
          ('team_week_2', 2020, 2, 'CAR', 'REG', 1, 0, 0),
          ('team_week_3', 2020, 3, 'CAR', 'REG', 1, 0, 0),
          ('team_week_4', 2020, 4, 'CAR', 'REG', 1, 0, 0),
          ('team_week_5', 2020, 5, 'CAR', 'REG', 1, 0, 0)
        """
    )

    materialize_season_core_cohort_cells(
        connection,
        source_table="facts",
        nfl_stats_table="nfl_stats",
        output_table="season_core_outer",
        year=2020,
        player_id="cmc",
        split_threshold=1,
    )

    cell = connection.execute(
        """
        SELECT cell.start_rate_pct, cell.healthy_start_rate_pct,
          cell.expected_starts, cell.expected_wins, cell.expected_losses,
          cell.active_weeks, cell.inactive_weeks
        FROM season_core_outer, UNNEST(cohort_cells) AS u(cell)
        WHERE cell.q_teams='ALL' AND cell.q_roster='ALL'
          AND cell.q_scoring='ALL' AND cell.q_pass_td='ALL'
          AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone()
    assert cell == (10.0, 50.0, 0.1, 0.1, 0.0, 1, 4)


def test_weekly_compaction_keeps_the_all_pooled_audit_cell_when_every_ui_request_has_a_more_specific_parent() -> None:
    """The universal pool is a required audit/fallback fact, not a UI request."""
    connection = duckdb.connect()
    profile = _all_pool_profile()
    connection.execute(
        f"""
        CREATE TABLE compact AS
        WITH cells AS (
          SELECT q_teams, 'ALL'::VARCHAR q_roster, 'ALL'::VARCHAR q_scoring,
                 'ALL'::VARCHAR q_pass_td, 'ALL'::VARCHAR q_dynasty,
                 'ALL'::VARCHAR q_best_ball
          FROM (VALUES ('08tm'), ('10tm'), ('12tm'), ('14tm')) t(q_teams)
          UNION ALL SELECT 'ALL', q_roster, 'ALL', 'ALL', 'ALL', 'ALL'
          FROM (VALUES ('flx'), ('sflx'), ('idp')) r(q_roster)
          UNION ALL SELECT 'ALL', 'ALL', q_scoring, 'ALL', 'ALL', 'ALL'
          FROM (VALUES ('std'), ('half'), ('ppr')) s(q_scoring)
          UNION ALL SELECT 'ALL', 'ALL', 'ALL', q_pass_td, 'ALL', 'ALL'
          FROM (VALUES ('4pt'), ('6pt')) p(q_pass_td)
          UNION ALL SELECT 'ALL', 'ALL', 'ALL', 'ALL', q_dynasty, 'ALL'
          FROM (VALUES ('redraft'), ('dynasty')) d(q_dynasty)
          UNION ALL SELECT 'ALL', 'ALL', 'ALL', 'ALL', 'ALL', q_best_ball
          FROM (VALUES ('managed'), ('best_ball')) b(q_best_ball)
          UNION ALL SELECT 'ALL', 'ALL', 'ALL', 'ALL', 'ALL', 'ALL'
        )
        SELECT 'kept'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year, 1::INTEGER AS week,
               'RB'::VARCHAR AS position, {profile} AS cohort_profile,
               list(struct_pack(
                 q_teams := q_teams, q_roster := q_roster, q_scoring := q_scoring,
                 q_pass_td := q_pass_td, q_dynasty := q_dynasty, q_best_ball := q_best_ball,
                 eligible_leagues := 900::BIGINT, rostered_leagues := 300::BIGINT
               )) AS cohort_cells
        FROM cells
        """
    )

    _compact_weekly_requested_cells(connection, output_table="compact", split_threshold=150)

    assert connection.execute(
        "SELECT COUNT(*) FROM compact, UNNEST(cohort_cells) u(cell) "
        "WHERE cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL' "
        "AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'"
    ).fetchone() == (1,)
