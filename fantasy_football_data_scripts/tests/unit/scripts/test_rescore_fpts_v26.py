from __future__ import annotations

import duckdb

from scripts.sota_recon import build_rescore_fpts_v26 as rescore


def test_rescore_target_catches_zero_scored_offensive_stat_rows():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE sample AS
        SELECT
          'P,QB' AS position,
          0.0 AS fpts_4pt_half,
          0.0 AS fpts_5pt_half,
          0.0 AS fpts_6pt_half,
          554.0 AS passing_yards,
          5.0 AS passing_tds,
          2.0 AS passing_interceptions,
          -3.0 AS rushing_yards,
          1.0 AS rushing_tds,
          0.0 AS rushing_fumbles_lost,
          0.0 AS sack_fumbles_lost,
          0.0 AS receiving_yards,
          0.0 AS receiving_tds,
          0.0 AS receptions,
          0.0 AS receiving_fumbles_lost,
          0.0 AS passing_2pt_conversions,
          0.0 AS rushing_2pt_conversions,
          0.0 AS receiving_2pt_conversions,
          0.0 AS special_teams_tds,
          0.0 AS fum_ret_td,
          0.0 AS pts_k_std,
          0.0 AS rushing_first_downs,
          0.0 AS receiving_first_downs,
          NULL AS fpts_4pt_ppfd,
          NULL AS fpts_5pt_ppfd,
          NULL AS fpts_6pt_ppfd
        """
    )

    assert con.execute(f"SELECT COUNT(*) FROM sample WHERE {rescore.TARGET}").fetchone()[0] == 1


def test_rescore_target_catches_stale_ppfd_rows_when_base_fpts_are_current():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE sample AS
        SELECT
          'RB' AS position,
          10.0 AS fpts_4pt_half,
          10.0 AS fpts_5pt_half,
          10.0 AS fpts_6pt_half,
          100.0 AS rushing_yards,
          0.0 AS passing_yards,
          0.0 AS passing_tds,
          0.0 AS passing_interceptions,
          0.0 AS rushing_tds,
          0.0 AS rushing_fumbles_lost,
          0.0 AS sack_fumbles_lost,
          0.0 AS receiving_yards,
          0.0 AS receiving_tds,
          0.0 AS receptions,
          0.0 AS receiving_fumbles_lost,
          0.0 AS passing_2pt_conversions,
          0.0 AS rushing_2pt_conversions,
          0.0 AS receiving_2pt_conversions,
          0.0 AS special_teams_tds,
          0.0 AS fum_ret_td,
          0.0 AS pts_k_std,
          0.0 AS rushing_first_downs,
          0.0 AS receiving_first_downs,
          NULL AS fpts_4pt_ppfd,
          NULL AS fpts_5pt_ppfd,
          NULL AS fpts_6pt_ppfd
        """
    )

    assert con.execute(f"SELECT COUNT(*) FROM sample WHERE {rescore.TARGET}").fetchone()[0] == 1


def test_rescore_output_columns_do_not_rewrite_defense_or_idp_lanes():
    assert rescore._is_rescore_output("fpts_4pt_half")
    assert rescore._is_rescore_output("pts_pass_4pt")
    assert rescore._is_rescore_output("pts_k_std")
    assert not rescore._is_rescore_output("pts_idp_std")
    assert not rescore._is_rescore_output("pts_def_std")
    assert not rescore._is_rescore_output("pts_allow_0")
