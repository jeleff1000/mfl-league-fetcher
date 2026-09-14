"""Tests for the reporter module — CLI output + MotherDuck storage."""

from __future__ import annotations

from datetime import datetime, UTC

import duckdb

from multi_league.validation_v2.models import Check, CheckResult
from multi_league.validation_v2.reporter import print_report, store_results


def _make_check(
    name: str,
    page: str = "matchups",
    severity: str = "ERROR",
    fix_action: str | None = None,
) -> Check:
    """Helper to create a minimal Check."""
    return Check(
        name=name,
        page=page,
        table="matchup",
        severity=severity,
        description=f"test check {name}",
        sql_expr="SUM(CASE WHEN 1=0 THEN 1 ELSE 0 END)",
        fix_action=fix_action,
    )


def _make_result(
    check: Check,
    db_name: str,
    fail_count: int = 0,
    passed: bool = True,
    skipped: bool = False,
    suppressed: bool = False,
    suppressed_by: str | None = None,
) -> CheckResult:
    """Helper to create a CheckResult."""
    return CheckResult(
        check=check,
        db_name=db_name,
        fail_count=fail_count,
        passed=passed,
        skipped=skipped,
        suppressed=suppressed,
        suppressed_by=suppressed_by,
    )


# ---------------------------------------------------------------------------
# CLI output tests
# ---------------------------------------------------------------------------


def test_print_report_hides_suppressed(capsys):
    """Suppressed results should not appear in CLI output."""
    visible_check = _make_check("visible_check", "matchups")
    suppressed_check = _make_check("suppressed_check", "matchups")

    results = [
        _make_result(visible_check, "league_a", fail_count=3, passed=False),
        _make_result(
            suppressed_check,
            "league_a",
            fail_count=5,
            passed=False,
            suppressed=True,
            suppressed_by="some_root",
        ),
    ]

    print_report(results, verbose=True)
    output = capsys.readouterr().out

    # Visible check should be in output
    assert "visible_check" in output
    # Suppressed check should NOT be in output
    assert "suppressed_check" not in output
    # Suppressed count should be mentioned in header
    assert "Suppressed: 1" in output


def test_print_report_hides_skipped(capsys):
    """Skipped results should not appear in CLI output."""
    skipped_check = _make_check("skipped_check", "matchups")

    results = [
        _make_result(skipped_check, "league_a", skipped=True),
    ]

    print_report(results, verbose=True)
    output = capsys.readouterr().out

    assert "skipped_check" not in output
    assert "Skipped: 1" in output


def test_print_report_all_pass(capsys):
    """When all checks pass, show success message."""
    check = _make_check("good_check", "matchups")
    results = [
        _make_result(check, "league_a", passed=True),
    ]

    print_report(results)
    output = capsys.readouterr().out

    assert "All checks passed" in output


def test_print_report_verbose_shows_details(capsys):
    """Verbose mode should show individual check failures."""
    check = _make_check("detail_check", "standings", "WARNING")

    results = [
        _make_result(check, "bad_league", fail_count=7, passed=False),
    ]

    print_report(results, verbose=True)
    output = capsys.readouterr().out

    assert "detail_check" in output
    assert "bad_league" in output
    # Verbose now shows "bad_league(7)" format (top 5 per check)
    assert "bad_league(7)" in output
    assert "WARNING" in output


def test_print_report_groups_by_page(capsys):
    """Results should be grouped by page in the output."""
    matchup_check = _make_check("matchup_fail", "matchups", "ERROR")
    standings_check = _make_check("standings_fail", "standings", "WARNING")

    results = [
        _make_result(matchup_check, "league_a", fail_count=1, passed=False),
        _make_result(standings_check, "league_a", fail_count=2, passed=False),
    ]

    print_report(results)
    output = capsys.readouterr().out

    # Both pages should appear
    assert "[matchups]" in output
    assert "[standings]" in output


def test_print_report_info_is_not_counted_as_failure(capsys):
    check = _make_check("identity_note", "identity", "INFO")
    results = [
        _make_result(check, "league_a", fail_count=2, passed=False),
    ]

    print_report(results)
    output = capsys.readouterr().out

    assert "Checks: 1 visible" in output
    assert "0 failed" in output
    assert "Leagues with failures: 0" in output
    assert "Leagues with info: 1" in output
    assert "[identity] 1 INFO" in output


# ---------------------------------------------------------------------------
# Storage tests
# ---------------------------------------------------------------------------


def test_store_results_creates_tables():
    """store_results should create tables and insert rows."""
    conn = duckdb.connect(":memory:")

    check = _make_check("test_check", "matchups", "ERROR", fix_action="reimport")
    results = [
        _make_result(check, "league_a", fail_count=3, passed=False),
        _make_result(check, "league_b", fail_count=0, passed=True),
    ]

    run_id = store_results(
        conn,
        results,
        run_id="test123",
        run_timestamp=datetime(2026, 4, 11, 12, 0, 0, tzinfo=UTC),
        validator_version="test.version",
        table_prefix="fleet_health.",
    )

    assert run_id == "test123"

    # Verify results table
    rows = conn.execute("SELECT * FROM fleet_health.validation_results ORDER BY db_name").fetchall()
    assert len(rows) == 2

    # Check column values for league_a
    cols = [desc[0] for desc in conn.description]
    row_a = dict(zip(cols, rows[0]))
    assert row_a["db_name"] == "league_a"
    assert row_a["check_name"] == "test_check"
    assert row_a["page"] == "matchups"
    assert row_a["severity"] == "ERROR"
    assert row_a["passed"] is False
    assert row_a["fail_count"] == 3
    assert row_a["fix_action"] == "reimport"

    # Verify summary table
    summary = conn.execute("SELECT * FROM fleet_health.validation_summary").fetchall()
    assert len(summary) == 1
    summary_cols = [desc[0] for desc in conn.description]
    summary_row = dict(zip(summary_cols, summary[0]))
    assert summary_row["total_checks"] == 2
    assert summary_row["total_passed"] == 1
    assert summary_row["total_failed"] == 1
    assert summary_row["error_count"] == 1

    conn.close()


def test_store_results_includes_suppressed():
    """Suppressed results should still be stored with their flags."""
    conn = duckdb.connect(":memory:")

    check = _make_check("suppressed_check", "matchups")
    results = [
        _make_result(
            check,
            "league_a",
            fail_count=5,
            passed=False,
            suppressed=True,
            suppressed_by="root_check",
        ),
    ]

    store_results(
        conn,
        results,
        run_id="sup_test",
        table_prefix="fleet_health.",
    )

    rows = conn.execute("SELECT suppressed, suppressed_by FROM fleet_health.validation_results").fetchall()
    assert len(rows) == 1
    assert rows[0][0] is True  # suppressed
    assert rows[0][1] == "root_check"  # suppressed_by

    conn.close()


def test_store_results_includes_skipped():
    """Skipped results should still be stored."""
    conn = duckdb.connect(":memory:")

    check = _make_check("skipped_check", "matchups")
    results = [
        _make_result(check, "league_a", skipped=True),
    ]

    store_results(
        conn,
        results,
        run_id="skip_test",
        table_prefix="fleet_health.",
    )

    rows = conn.execute("SELECT skipped FROM fleet_health.validation_results").fetchall()
    assert len(rows) == 1
    assert rows[0][0] is True

    conn.close()


def test_store_results_generates_run_id():
    """If no run_id provided, one should be generated."""
    conn = duckdb.connect(":memory:")

    results = [
        _make_result(_make_check("c", "p"), "lg", passed=True),
    ]

    run_id = store_results(conn, results, table_prefix="fleet_health.")

    assert run_id is not None
    assert len(run_id) == 8  # uuid4()[:8]

    conn.close()


def test_print_report_shows_errored_count(capsys):
    """Errored results should surface in the summary header and errored section."""
    check = _make_check("bad_batch_check", "players")
    errored_result = CheckResult(
        check=check,
        db_name="league_err",
        fail_count=0,
        passed=False,
        errored=True,
        error_message="Binder Error: column starter_points not found",
    )
    results = [errored_result]

    print_report(results)
    output = capsys.readouterr().out

    assert "0 errored" not in output  # 1 errored, not 0
    assert "1 errored" in output
    assert "Leagues with errors: 1" in output
    assert "[players] 1 ERRORED" in output
    assert "bad_batch_check" in output
    assert "Binder Error" in output


def test_print_report_shows_transient_errors_separately(capsys):
    """Transient Fly errors should be visible but not reported as hard errors."""
    check = _make_check("players_nfl_id_coverage", "players")
    transient_result = CheckResult(
        check=check,
        db_name="giant_eaglers_xiv",
        fail_count=0,
        passed=False,
        errored=True,
        transient_error=True,
        error_message="HTTP Error 503: Service Unavailable",
    )

    print_report([transient_result])
    output = capsys.readouterr().out

    assert "0 errored, 1 transient" in output
    assert "Leagues with errors" not in output
    assert "Leagues with transient validator errors: 1" in output
    assert "[players] 1 TRANSIENT" in output
    assert "players_nfl_id_coverage" in output


def test_store_results_summary_counts_suppressed_correctly():
    """Summary should count suppressed results separately from real failures."""
    conn = duckdb.connect(":memory:")

    check = _make_check("check_a", "matchups", "ERROR")
    results = [
        # Real failure
        _make_result(check, "league_a", fail_count=3, passed=False),
        # Suppressed failure
        _make_result(
            check,
            "league_b",
            fail_count=2,
            passed=False,
            suppressed=True,
            suppressed_by="root",
        ),
        # Pass
        _make_result(check, "league_c", passed=True),
    ]

    store_results(conn, results, run_id="stest", table_prefix="fleet_health.")

    summary = conn.execute(
        "SELECT total_checks, total_passed, total_failed, total_suppressed, "
        "leagues_with_failures, error_count "
        "FROM fleet_health.validation_summary"
    ).fetchone()

    assert summary[0] == 3  # total_checks
    assert summary[1] == 1  # total_passed
    assert summary[2] == 1  # total_failed (suppressed result excluded)
    assert summary[3] == 1  # total_suppressed
    assert summary[4] == 1  # leagues_with_failures (only league_a, not suppressed league_b)
    assert summary[5] == 1  # error_count (only unsuppressed failures)

    conn.close()


def test_store_results_summary_excludes_info_from_failed_counts():
    conn = duckdb.connect(":memory:")

    info_check = _make_check("identity_note", "identity", "INFO")
    results = [
        _make_result(info_check, "league_a", fail_count=2, passed=False),
    ]

    store_results(conn, results, run_id="info_only", table_prefix="fleet_health.")

    summary = conn.execute(
        "SELECT total_checks, total_passed, total_failed, leagues_with_failures, info_count "
        "FROM fleet_health.validation_summary"
    ).fetchone()

    assert summary[0] == 1
    assert summary[1] == 0
    assert summary[2] == 0
    assert summary[3] == 0
    assert summary[4] == 1

    conn.close()
