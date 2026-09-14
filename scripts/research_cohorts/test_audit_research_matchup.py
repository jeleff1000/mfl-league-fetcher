import duckdb

from audit_research_matchup import (
    BOARD_STATS, Report, check_denominator_identities, check_exposure_bounds,
    check_expected_start_contract, check_rate_denominators,
)


def test_board_diagnostic_uses_conditional_win_rate():
    """The displayed Win% is wins divided by starts, not wins per eligible week."""
    assert ("win_rate_pct", "started_weeks") in BOARD_STATS
    assert ("won_pct", "started_weeks") not in BOARD_STATS


def _audit_row(con, *, bad_signal=False):
    con.execute("""
        CREATE TABLE matchup_audit_fixture AS SELECT * FROM (VALUES
          (0.6, 0.4, 1.0, 0.3, 0.2, 0.5, 1, 1, 1, 2, 2, 2, 0, 17, 0)
        ) AS t(
          won_pct, lost_pct, start_rate_pct,
          expected_wins, expected_losses, expected_starts,
          n_champ_leagues, n_champ_start_leagues, champ_elig_leagues,
          n_final_po, n_started_po, playoff_eligible_leagues,
          n_rostered_leagues, n_leagues, year
        )
    """)
    if bad_signal:
        con.execute("UPDATE matchup_audit_fixture SET n_started_po = 3")


def test_denominator_identity_gate_accepts_valid_signal_counts():
    con = duckdb.connect()
    _audit_row(con)
    report = Report()
    check_denominator_identities(con, report, "matchup_audit_fixture", "season")
    assert report.failures == []
    con.close()


def test_denominator_identity_gate_rejects_playoff_numerator_above_population():
    con = duckdb.connect()
    _audit_row(con, bad_signal=True)
    report = Report()
    check_denominator_identities(con, report, "matchup_audit_fixture", "season")
    assert any("playoff as-starter" in failure for failure in report.failures)
    con.close()


def test_exposure_bounds_accept_valid_counts_and_reject_overcounts():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE exposure_fixture AS SELECT * FROM (VALUES
          (2, 10, 3, 10, 2, 8, 2, 1, 0, 1)
        ) AS t(
          rostered_leagues, roster_eligible_leagues,
          started_leagues, team_game_eligible_leagues,
          healthy_started_leagues, healthy_eligible_leagues,
          wins_started, wins_started_active, losses_started, started_active_weeks
        )
    """)
    report = Report()
    check_exposure_bounds(con, report, "exposure_fixture", "season")
    assert report.failures == []
    con.execute("UPDATE exposure_fixture SET started_leagues = 11")
    report = Report()
    check_exposure_bounds(con, report, "exposure_fixture", "season")
    assert any("started <= team-game eligible" in failure for failure in report.failures)
    con.close()


def test_exposure_bounds_rejects_wl_counts_without_started_week_support():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE season_exposure_fixture AS SELECT * FROM (VALUES
          (3, 2, 4, 10, 3, 3)
        ) AS t(
          wins_started, losses_started, started_team_game_weeks,
          team_game_eligible_league_weeks, n_rostered_leagues, rostered_league_weeks
        )
    """)
    report = Report()
    check_exposure_bounds(con, report, "season_exposure_fixture", "season")
    assert report.failures == []
    con.close()


def test_expected_start_contract_uses_weekly_eligible_share():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE expected_start_fixture AS SELECT * FROM (VALUES
          (0.75, 75.0, 0.50, 0.25, 1.0)
        ) AS t(expected_starts, start_rate_pct, expected_wins,
               expected_losses, active_weeks)
    """)
    report = Report()
    check_expected_start_contract(con, report, "expected_start_fixture", "weekly")
    assert report.failures == []
    con.execute("UPDATE expected_start_fixture SET expected_starts = .9")
    report = Report()
    check_expected_start_contract(con, report, "expected_start_fixture", "weekly")
    assert any("Exp Starts uses eligible-league denominator" in failure
               for failure in report.failures)
    con.close()


def test_weekly_clutch_uses_season_champion_population_not_week_champ_rows():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE clutch_denominator_fixture AS SELECT * FROM (VALUES
          (0.1, 10.0, 100.0)
        ) AS t(avg_clutch_started, clutch_sum, clutch_eligible_leagues)
    """)
    report = Report()
    check_rate_denominators(con, report, "clutch_denominator_fixture", "weekly")
    assert report.failures == []
    con.execute("UPDATE clutch_denominator_fixture SET clutch_eligible_leagues = 50")
    report = Report()
    check_rate_denominators(con, report, "clutch_denominator_fixture", "weekly")
    assert any("Clutch denominator" in failure for failure in report.failures)
    con.close()


def test_population_lattice_checks_each_weekly_denominator():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE population_lattice_fixture AS SELECT * FROM (VALUES
          ('p1', '12tm', 2024, 1, 'RB', 100, 100, 100, 90, 4),
          ('p2', '12tm', 2024, 1, 'RB', 100, 100, 100, 90, 4)
        ) AS t(NFL_player_id, teams, year, week, pos_grp, n_leagues,
               roster_eligible_leagues, team_game_eligible_leagues,
               clutch_eligible_leagues, cohort_level)
    """)
    report = Report()
    from audit_research_matchup import check_denominator_is_population
    check_denominator_is_population(con, report, "population_lattice_fixture", "weekly")
    assert report.failures == []
    con.execute("UPDATE population_lattice_fixture SET roster_eligible_leagues = 99 WHERE NFL_player_id = 'p2'")
    report = Report()
    check_denominator_is_population(con, report, "population_lattice_fixture", "weekly")
    assert any("roster_eligible_leagues" in failure for failure in report.failures)
    con.close()
