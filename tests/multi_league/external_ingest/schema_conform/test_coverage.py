from types import SimpleNamespace
import pandas as pd
from multi_league.external_ingest.schema_conform._coverage import (
    check_manager_guid_coverage,
    check_known_managers_resolve_canonical,
    check_new_managers_stable_synthetic,
    check_cross_table_consistency,
    apply_required_data_null_guard,
)


def _ctx(name_to_guid):
    return SimpleNamespace(name_to_guid=name_to_guid)


def test_manager_guid_coverage_passes():
    df = pd.DataFrame({"manager_guid": ["G_A"] * 98 + [None] * 2})
    failure = check_manager_guid_coverage(df, table="matchup")
    assert failure is None


def test_manager_guid_coverage_fails_below_threshold():
    df = pd.DataFrame({"manager_guid": ["G_A"] * 90 + [None] * 10})
    failure = check_manager_guid_coverage(df, table="matchup")  # threshold 0.98
    assert failure is not None


def test_manager_guid_coverage_transactions_threshold_looser():
    df = pd.DataFrame({"manager_guid": ["G_A"] * 86 + [None] * 14})
    # transactions threshold 0.85 — should pass
    assert check_manager_guid_coverage(df, table="transactions") is None
    # matchup threshold 0.98 — same df should fail
    assert check_manager_guid_coverage(df, table="matchup") is not None


def test_known_manager_must_resolve_to_canonical():
    df = pd.DataFrame(
        {
            "manager": ["Adin"] * 5,
            "manager_guid": ["GUID_ADIN_CANONICAL"] * 5,
        }
    )
    ctx = _ctx({"adin": "GUID_ADIN_CANONICAL"})
    failure = check_known_managers_resolve_canonical(df, ctx)
    assert failure is None


def test_known_manager_resolved_to_wrong_guid_fails():
    df = pd.DataFrame(
        {
            "manager": ["Adin"] * 5,
            "manager_guid": ["GUID_ADIN_CANONICAL"] * 4 + ["DIFFERENT_GUID"],
        }
    )
    ctx = _ctx({"adin": "GUID_ADIN_CANONICAL"})
    failure = check_known_managers_resolve_canonical(df, ctx)
    assert failure is not None


def test_new_manager_stable_synthetic_id():
    df = pd.DataFrame(
        {
            "manager": ["Rubinstein"] * 3,
            "manager_guid": ["external_abc123"] * 3,
        }
    )
    ctx = _ctx({"adin": "GUID_ADIN"})
    failure = check_new_managers_stable_synthetic(df, ctx)
    assert failure is None


def test_new_manager_inconsistent_synthetic_fails():
    df = pd.DataFrame(
        {
            "manager": ["Rubinstein"] * 3,
            "manager_guid": ["external_abc", "external_xyz", "external_abc"],
        }
    )
    ctx = _ctx({"adin": "GUID_ADIN"})
    failure = check_new_managers_stable_synthetic(df, ctx)
    assert failure is not None


def test_cross_table_consistency_passes():
    """All 3 tables share the same team_keys for the same year."""
    pf = pd.DataFrame({"year": [2014] * 3, "team_key": ["T1", "T2", "T3"]})
    dr = pd.DataFrame({"year": [2014] * 2, "team_key": ["T1", "T2"]})
    tx = pd.DataFrame({"year": [2014] * 2, "team_key": ["T1", "T3"]})
    failure = check_cross_table_consistency(
        {"player_fantasy": pf, "draft": dr, "transactions": tx},
        slot="team_key",
    )
    assert failure is None


def test_cross_table_consistency_fails_when_table_has_alien_keys():
    pf = pd.DataFrame({"year": [2014] * 3, "team_key": ["T1", "T2", "T3"]})
    dr = pd.DataFrame({"year": [2014] * 2, "team_key": ["FOREIGN_X", "FOREIGN_Y"]})
    failure = check_cross_table_consistency(
        {"player_fantasy": pf, "draft": dr},
        slot="team_key",
    )
    assert failure is not None


def test_required_data_null_guard_demotes_when_mostly_null():
    df = pd.DataFrame({"team_points": [None] * 8 + [100, 110]})  # 80% null
    out = apply_required_data_null_guard(df, table="matchup")
    assert out["team_points"].isna().all()  # demoted to all-NULL


def test_required_data_null_guard_keeps_when_populated():
    df = pd.DataFrame({"team_points": [100, 110, 120, None, None]})  # 60% populated
    out = apply_required_data_null_guard(df, table="matchup")
    assert out["team_points"].iloc[0] == 100
