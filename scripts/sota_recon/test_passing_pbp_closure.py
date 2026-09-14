from .audit_passing_pbp_closure import TARGETS, build


def test_every_passing_backlog_column_is_pbp_wired():
    receipt = build()
    assert receipt["target_count"] == 17
    assert receipt["fully_pbp_wired"] == len(TARGETS)
    assert receipt["failures"] == []
    assert receipt["independent_root_quorum"] == 14
    assert receipt["pbp_only_week_paths"] == 3
    assert [r["column"] for r in receipt["rows"]
            if r["independent_root_count"] == 1] == [
                "pass_success", "passing_epa", "passing_wpa"
            ]
    assert set(receipt["remaining_gap_reasons"]) == {
        "pass_success", "passing_epa", "passing_wpa"
    }
