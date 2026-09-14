from multi_league.validation_v2.models import Check, CheckResult, Manifest, VALIDATOR_VERSION


def test_check_defaults():
    c = Check(
        name="test",
        page="standings",
        table="matchup",
        severity="ERROR",
        description="desc",
        sql_expr="SUM(CASE WHEN 1=0 THEN 1 ELSE 0 END)",
    )
    assert c.threshold == 0
    assert c.feature is None
    assert c.batch_group == "core"
    assert c.depends_on is None
    assert c.cost_tier == "cheap"
    assert c.sql_full is None


def test_check_must_have_sql():
    import pytest

    with pytest.raises(ValueError):
        Check(name="bad", page="x", table="y", severity="ERROR", description="no sql").validate()


def test_check_severity_valid():
    import pytest

    with pytest.raises(ValueError):
        Check(name="bad", page="x", table="y", severity="CRITICAL", description="bad sev", sql_expr="1").validate()


def test_check_batch_group_valid():
    import pytest

    with pytest.raises(ValueError):
        Check(
            name="bad", page="x", table="y", severity="ERROR", description="bad bg", sql_expr="1", batch_group="invalid"
        ).validate()


def test_check_cost_tier_valid():
    import pytest

    with pytest.raises(ValueError):
        Check(
            name="bad", page="x", table="y", severity="ERROR", description="bad ct", sql_expr="1", cost_tier="free"
        ).validate()


def test_check_result_defaults():
    c = Check(name="t", page="p", table="t", severity="ERROR", description="d", sql_expr="1")
    r = CheckResult(check=c, db_name="test_league", fail_count=0, passed=True)
    assert r.skipped is False
    assert r.suppressed is False
    assert r.suppressed_by is None


def test_manifest_leagues_for_feature():
    m = Manifest(
        all_leagues=["a", "b", "c"],
        median_leagues=["a"],
        faab_leagues=["b", "c"],
    )
    assert m.leagues_for_feature(None) == ["a", "b", "c"]
    assert m.leagues_for_feature("median") == ["a"]
    assert m.leagues_for_feature("faab") == ["b", "c"]


def test_manifest_skip_set():
    m = Manifest(skip_lists={"check_a": {"league_1", "league_2"}, "check_b": {"league_2", "league_3"}})
    assert m.skip_set_for(None) == set()
    assert m.skip_set_for(["check_a"]) == {"league_1", "league_2"}
    assert m.skip_set_for(["check_a", "check_b"]) == {"league_1", "league_2", "league_3"}


def test_manifest_update_skip_lists():
    c = Check(name="fid_null", page="p", table="matchup", severity="ERROR", description="d", sql_expr="1")
    r1 = CheckResult(check=c, db_name="bad_league", fail_count=3, passed=False)
    r2 = CheckResult(check=c, db_name="good_league", fail_count=0, passed=True)
    m = Manifest()
    m.update_skip_lists([r1, r2])
    assert "fid_null" in m.skip_lists
    assert "bad_league" in m.skip_lists["fid_null"]
    assert "good_league" not in m.skip_lists["fid_null"]


def test_manifest_update_skip_lists_ignores_skipped():
    c = Check(name="dep", page="p", table="t", severity="ERROR", description="d", sql_expr="1")
    r = CheckResult(check=c, db_name="x", fail_count=0, passed=False, skipped=True)
    m = Manifest()
    m.update_skip_lists([r])
    assert "dep" not in m.skip_lists


def test_validator_version_format():
    assert isinstance(VALIDATOR_VERSION, str)
    assert len(VALIDATOR_VERSION) > 0


def test_check_result_supports_errored_state():
    from multi_league.validation_v2.models import Check, CheckResult

    check = Check(
        name="demo",
        page="players",
        table="matchup",
        severity="ERROR",
        description="demo",
        sql_expr="SUM(1)",
    )
    r = CheckResult(
        check=check,
        db_name="kmffl",
        fail_count=0,
        passed=False,
        errored=True,
        error_message="Binder Error: column foo not found",
    )
    assert r.errored is True
    assert r.transient_error is False
    assert r.error_message == "Binder Error: column foo not found"
    assert r.passed is False
    # Default construction must still work (backward compat):
    r2 = CheckResult(check=check, db_name="kmffl", fail_count=0, passed=True)
    assert r2.errored is False
    assert r2.transient_error is False
    assert r2.error_message is None


def test_manifest_update_skip_lists_ignores_errored_results():
    check = Check(name="source_failed", page="p", table="matchup", severity="ERROR", description="d", sql_expr="1")
    result = CheckResult(
        check=check,
        db_name="league_a",
        fail_count=0,
        passed=False,
        errored=True,
        transient_error=True,
        error_message="HTTP Error 503: Service Unavailable",
    )
    manifest = Manifest()
    manifest.update_skip_lists([result])
    assert "source_failed" not in manifest.skip_lists
