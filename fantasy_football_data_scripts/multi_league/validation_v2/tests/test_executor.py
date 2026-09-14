"""Tests for the executor module — batch builder + SQL generator."""

from __future__ import annotations


from multi_league.validation_v2.models import Check, CheckResult
from multi_league.validation_v2.executor import (
    group_checks,
    build_batch_sql,
    run_batch,
    run_sql_full,
)


# ---------------------------------------------------------------------------
# Helper checks used across tests
# ---------------------------------------------------------------------------


def _fid_null_check() -> Check:
    """Check that catches NULL franchise_id rows."""
    return Check(
        name="fid_null",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="franchise_id must not be NULL",
        sql_expr="SUM(CASE WHEN m.franchise_id IS NULL THEN 1 ELSE 0 END)",
        batch_group="core",
    )


def _negative_margin_check() -> Check:
    """Check that counts negative margins (expected to exist, just for grouping tests)."""
    return Check(
        name="negative_margin",
        page="standings",
        table="matchup",
        severity="WARNING",
        description="negative margin rows",
        sql_expr="SUM(CASE WHEN m.margin < 0 THEN 1 ELSE 0 END)",
        batch_group="core",
    )


def _quality_check() -> Check:
    """Check in a different batch_group."""
    return Check(
        name="quality_check",
        page="standings",
        table="matchup",
        severity="INFO",
        description="quality group check",
        sql_expr="SUM(CASE WHEN m.team_points = 0 THEN 1 ELSE 0 END)",
        batch_group="quality",
    )


def _median_check() -> Check:
    """Feature-gated check (median only)."""
    return Check(
        name="median_balance",
        page="standings",
        table="matchup",
        severity="WARNING",
        description="median league balance",
        sql_expr="SUM(CASE WHEN m.win + m.loss + m.tie = 0 THEN 1 ELSE 0 END)",
        feature="median",
        batch_group="core",
    )


def _dep_check() -> Check:
    """Check that depends on fid_null."""
    return Check(
        name="dep_on_fid",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="depends on fid_null",
        sql_expr="SUM(CASE WHEN m.margin IS NULL THEN 1 ELSE 0 END)",
        depends_on=["fid_null"],
        batch_group="core",
    )


def _full_query_check() -> Check:
    """Check with sql_full instead of sql_expr."""
    return Check(
        name="full_query_check",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="standalone full query",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table} m "
            "WHERE m.franchise_id IS NULL "
            "AND m.db_name IN ({league_list}) "
            "GROUP BY m.db_name"
        ),
    )


def _threshold_check() -> Check:
    """Check with a threshold of 1 — 1 failure is tolerated."""
    return Check(
        name="threshold_check",
        page="standings",
        table="matchup",
        severity="WARNING",
        description="allows 1 failure",
        sql_expr="SUM(CASE WHEN m.franchise_id IS NULL THEN 1 ELSE 0 END)",
        threshold=1,
        batch_group="core",
    )


# ---------------------------------------------------------------------------
# 1. test_group_checks_by_table_feature_batch
# ---------------------------------------------------------------------------


def test_group_checks_by_table_feature_batch():
    checks = [_fid_null_check(), _negative_margin_check(), _quality_check(), _median_check()]
    groups = group_checks(checks)

    # (table, feature, batch_group) tuples
    assert ("matchup", None, "core") in groups
    assert ("matchup", None, "quality") in groups
    assert ("matchup", "median", "core") in groups

    # core group should have fid_null + negative_margin
    core_checks = groups[("matchup", None, "core")]
    core_names = {c.name for c in core_checks}
    assert core_names == {"fid_null", "negative_margin"}

    # quality group has 1 check
    assert len(groups[("matchup", None, "quality")]) == 1

    # median group has 1 check
    assert len(groups[("matchup", "median", "core")]) == 1


# ---------------------------------------------------------------------------
# 2. test_build_batch_sql_simple
# ---------------------------------------------------------------------------


def test_build_batch_sql_simple(manifest):
    checks = [_fid_null_check()]
    sql = build_batch_sql(
        checks=checks,
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets={},
        table_prefix="public.",
    )

    # Must have SELECT, GROUP BY, the check alias
    assert "SELECT" in sql
    assert "GROUP BY" in sql
    assert "fid_null" in sql
    assert "public.matchup" in sql
    # Must filter by all leagues (no feature gating)
    assert "good_league" in sql
    assert "bad_league" in sql
    assert "median_league" in sql


# ---------------------------------------------------------------------------
# 3. test_build_batch_sql_with_skip_list
# ---------------------------------------------------------------------------


def test_build_batch_sql_with_skip_list(manifest):
    dep = _dep_check()
    skip_sets = {"dep_on_fid": {"bad_league"}}
    sql = build_batch_sql(
        checks=[dep],
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets=skip_sets,
        table_prefix="public.",
    )

    # Should have a CTE for the skip list
    assert "skip_dep_on_fid" in sql
    assert "bad_league" in sql
    assert "NOT IN" in sql or "not in" in sql.lower()


# ---------------------------------------------------------------------------
# 4. test_build_batch_sql_feature_scoped
# ---------------------------------------------------------------------------


def test_build_batch_sql_feature_scoped(manifest):
    checks = [_median_check()]
    sql = build_batch_sql(
        checks=checks,
        table="matchup",
        manifest=manifest,
        feature="median",
        skip_sets={},
        table_prefix="public.",
    )

    # Only median_league should be in the WHERE clause
    assert "median_league" in sql
    # good_league and bad_league should NOT be in the WHERE
    assert "good_league" not in sql
    assert "bad_league" not in sql


# ---------------------------------------------------------------------------
# 5. test_run_batch_finds_failures
# ---------------------------------------------------------------------------


def test_run_batch_finds_failures(local_db, manifest):
    checks = [_fid_null_check()]
    skip_sets = {}
    sql = build_batch_sql(
        checks=checks,
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets=skip_sets,
        table_prefix="public.",
    )
    results = run_batch(local_db, sql, checks, manifest, skip_sets)

    # Should have results for all leagues that have data
    result_map = {r.db_name: r for r in results}

    # bad_league has 1 NULL franchise_id row -> should fail
    assert "bad_league" in result_map
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    # good_league has 0 NULL franchise_id -> should pass
    assert "good_league" in result_map
    assert result_map["good_league"].passed is True
    assert result_map["good_league"].fail_count == 0

    # median_league should pass too
    assert "median_league" in result_map
    assert result_map["median_league"].passed is True


# ---------------------------------------------------------------------------
# 6. test_run_batch_skip_list_marks_skipped
# ---------------------------------------------------------------------------


def test_run_batch_skip_list_marks_skipped(local_db, manifest):
    dep = _dep_check()
    skip_sets = {"dep_on_fid": {"bad_league"}}
    sql = build_batch_sql(
        checks=[dep],
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets=skip_sets,
        table_prefix="public.",
    )
    results = run_batch(local_db, sql, [dep], manifest, skip_sets)

    result_map = {r.db_name: r for r in results}

    # bad_league is in skip set -> should be skipped
    assert "bad_league" in result_map
    assert result_map["bad_league"].skipped is True
    assert result_map["bad_league"].passed is True

    # good_league is not in skip set -> should be evaluated normally
    assert "good_league" in result_map
    assert result_map["good_league"].skipped is False


# ---------------------------------------------------------------------------
# 7. test_run_batch_threshold
# ---------------------------------------------------------------------------


def test_run_batch_threshold(local_db, manifest):
    check = _threshold_check()
    skip_sets = {}
    sql = build_batch_sql(
        checks=[check],
        table="matchup",
        manifest=manifest,
        feature=None,
        skip_sets=skip_sets,
        table_prefix="public.",
    )
    results = run_batch(local_db, sql, [check], manifest, skip_sets)

    result_map = {r.db_name: r for r in results}

    # bad_league has 1 NULL franchise_id, threshold is 1 -> should pass
    assert "bad_league" in result_map
    assert result_map["bad_league"].passed is True
    assert result_map["bad_league"].fail_count == 1


# ---------------------------------------------------------------------------
# 8. test_run_sql_full
# ---------------------------------------------------------------------------


def test_run_sql_full(local_db, manifest):
    check = _full_query_check()
    results = run_sql_full(local_db, check, manifest, table_prefix="public.")

    result_map = {r.db_name: r for r in results}

    # bad_league has 1 NULL franchise_id -> should appear with fail_count=1
    assert "bad_league" in result_map
    assert result_map["bad_league"].passed is False
    assert result_map["bad_league"].fail_count == 1

    # good_league has no NULLs -> should be marked as passed (absent from query results)
    assert "good_league" in result_map
    assert result_map["good_league"].passed is True
    assert result_map["good_league"].fail_count == 0


# ---------------------------------------------------------------------------
# 9. test_run_sql_full_no_failures
# ---------------------------------------------------------------------------


def test_run_sql_full_no_failures(local_db, manifest):
    """When the full query returns no rows, all leagues in manifest pass."""
    check = Check(
        name="always_pass",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="no failures expected",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table} m "
            "WHERE m.team_points < 0 "
            "AND m.db_name IN ({league_list}) "
            "GROUP BY m.db_name"
        ),
    )
    results = run_sql_full(local_db, check, manifest, table_prefix="public.")

    # All leagues should pass
    for r in results:
        assert r.passed is True
        assert r.fail_count == 0

    # Should have a result for each league in the manifest
    assert len(results) == len(manifest.all_leagues)


# ---------------------------------------------------------------------------
# 10. test_batch_error_produces_errored_results_for_all_checks
# ---------------------------------------------------------------------------


def test_batch_error_produces_errored_results_for_all_checks(local_db, manifest):
    """When a batch raises, every check in the batch produces an errored
    CheckResult for every league in scope — not silently vanish."""
    from multi_league.validation_v2.models import Check
    from multi_league.validation_v2.validate import _run_single_batch_with_error_handling

    # Bogus check: references a column that doesn't exist → guaranteed binder error.
    bogus = Check(
        name="bogus_missing_col",
        page="players",
        table="matchup",
        severity="ERROR",
        description="references non-existent column",
        sql_expr="SUM(CASE WHEN nonexistent_column_xyz IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
    )

    results = _run_single_batch_with_error_handling(
        conn=local_db,
        table="matchup",
        feature=None,
        batch_group="quality",
        checks=[bogus],
        manifest=manifest,
        skip_sets={},
        table_prefix="public.",
    )

    # Every league in the manifest's all_leagues scope should produce one errored result.
    # (Depending on how leagues_for_feature handles feature=None, this resolves to all_leagues.)
    assert len(results) == len(
        manifest.all_leagues
    ), f"expected one errored result per league, got {len(results)} for {len(manifest.all_leagues)} leagues"
    for r in results:
        assert r.check.name == "bogus_missing_col"
        assert r.errored is True
        assert r.transient_error is False
        assert r.passed is False
        assert r.error_message is not None
        assert ("nonexistent_column_xyz" in r.error_message) or ("Binder" in r.error_message)


def test_batch_transient_error_is_marked_transient(manifest):
    from multi_league.validation_v2.models import Check
    from multi_league.validation_v2.validate import _run_single_batch_with_error_handling

    class BusyConnection:
        def execute(self, sql):
            raise RuntimeError("HTTP Error 503: Service Unavailable")

    check = Check(
        name="busy_batch_check",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="busy batch",
        sql_expr="SUM(CASE WHEN 1=0 THEN 1 ELSE 0 END)",
    )

    results = _run_single_batch_with_error_handling(
        conn=BusyConnection(),
        table="player_fantasy",
        feature=None,
        batch_group="quality",
        checks=[check],
        manifest=manifest,
        skip_sets={},
        table_prefix="public.",
    )

    assert len(results) == len(manifest.all_leagues)
    assert all(r.errored for r in results)
    assert all(r.transient_error for r in results)


def test_run_checks_updates_sql_full_skip_lists_within_same_pass(local_db, manifest):
    """A sql_full prerequisite should skip dependent sql_full checks later
    in the same run_checks() call."""
    from multi_league.validation_v2.validate import run_checks

    root = Check(
        name="root_full_check",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="root failure",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table} m "
            "WHERE m.franchise_id IS NULL "
            "AND m.db_name IN ({league_list}) "
            "GROUP BY m.db_name"
        ),
    )
    dependent = Check(
        name="dependent_full_check",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="depends on root",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM {table} m "
            "WHERE m.db_name IN ({league_list}) "
            "GROUP BY m.db_name"
        ),
        depends_on=["root_full_check"],
    )

    results = run_checks(local_db, [root, dependent], manifest, table_prefix="public.")
    dependent_results = {r.db_name: r for r in results if r.check.name == "dependent_full_check"}

    assert dependent_results["bad_league"].skipped is True
    assert dependent_results["bad_league"].passed is True
    assert dependent_results["bad_league"].fail_count == 0
    assert dependent_results["good_league"].skipped is False
    assert dependent_results["good_league"].passed is False
    assert manifest.skip_lists["root_full_check"] == {"bad_league"}


def test_run_checks_sql_full_unexpected_error_marks_errored(manifest):
    """Unexpected standalone-query errors should be surfaced as errored
    results instead of synthetic fail_count=-1 data failures."""
    from multi_league.validation_v2.validate import run_checks

    class BrokenConnection:
        def execute(self, sql):
            raise RuntimeError("Binder Error: column starter_points not found")

    check = Check(
        name="fragile_full_check",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="fragile full query",
        sql_full="SELECT db_name, 1 AS fail_count FROM {table} WHERE db_name IN ({league_list})",
    )

    results = run_checks(BrokenConnection(), [check], manifest, table_prefix="public.")

    assert len(results) == len(manifest.all_leagues)
    for result in results:
        assert result.errored is True
        assert result.transient_error is False
        assert result.passed is False
        assert result.fail_count == 0
        assert result.error_message == "Binder Error: column starter_points not found"


def test_run_checks_sql_full_transient_error_marks_transient(manifest):
    from multi_league.validation_v2.validate import run_checks

    class BusyConnection:
        def execute(self, sql):
            raise RuntimeError("DuckDB server unavailable (ops_writing)")

    check = Check(
        name="fragile_full_check",
        page="simulations",
        table="luck_schedule_swap_season",
        severity="ERROR",
        description="fragile full query",
        sql_full="SELECT db_name, 1 AS fail_count FROM {table} WHERE db_name IN ({league_list})",
    )

    results = run_checks(BusyConnection(), [check], manifest, table_prefix="public.")

    assert len(results) == len(manifest.all_leagues)
    assert all(result.errored for result in results)
    assert all(result.transient_error for result in results)


def test_ci_failures_ignore_transient_validator_errors():
    from multi_league.validation_v2.validate import _ci_failure_results

    check = Check(
        name="players_nfl_id_coverage",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="transient check",
        sql_expr="SUM(1)",
    )
    transient = CheckResult(
        check=check,
        db_name="giant_eaglers_xiv",
        fail_count=0,
        passed=False,
        errored=True,
        transient_error=True,
        error_message="HTTP Error 503: Service Unavailable",
    )
    real_failure = CheckResult(check=check, db_name="theta_chi_oe", fail_count=1, passed=False)

    assert _ci_failure_results([transient]) == []
    assert _ci_failure_results([transient, real_failure]) == [real_failure]
