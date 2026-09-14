from __future__ import annotations

import json

import pytest

from scripts.sota_recon import facts


COMMON = dict(
    wave_id="test_wave",
    reason="unit test",
    witness="pytest",
    source_snapshot_id="release_20260617_v26",
)


@pytest.fixture()
def con(tmp_path):
    c = facts.connect(str(tmp_path / "facts.duckdb"))
    yield c
    c.close()


def test_cell_override_round_trips(con):
    fid = facts.emit_fact(
        "cell_override", con=con,
        table_name="nfl_player_stats_all", target_key="MastBe20_1935_12",
        column_name="passing_interceptions", old_value="9", new_value="1", **COMMON,
    )
    rows = facts.effective_cell_overrides(con)
    assert len(rows) == 1
    assert rows[0][0] == fid


def test_newest_fact_supersedes_older(con):
    import datetime

    t1 = datetime.datetime(2026, 7, 10, 1, 0, 0)
    t2 = datetime.datetime(2026, 7, 10, 2, 0, 0)
    facts.emit_fact(
        "cell_override", con=con, created_at=t1,
        table_name="t", target_key="k", column_name="c",
        old_value="9", new_value="2", **COMMON,
    )
    facts.emit_fact(
        "cell_override", con=con, created_at=t2,
        table_name="t", target_key="k", column_name="c",
        old_value="9", new_value="1", **COMMON,
    )
    rows = facts.effective_cell_overrides(con)
    assert len(rows) == 1
    new_value = rows[0][10]
    assert new_value == "1"
    assert con.execute("SELECT COUNT(*) FROM cell_override").fetchone()[0] == 2


def test_same_timestamp_collision_is_a_build_error(con):
    import datetime

    t = datetime.datetime(2026, 7, 10, 1, 0, 0)
    for v in ("1", "2"):
        facts.emit_fact(
            "cell_override", con=con, created_at=t,
            table_name="t", target_key="k", column_name="c",
            old_value="9", new_value=v, **COMMON,
        )
    with pytest.raises(facts.FactConflict):
        facts.effective_cell_overrides(con)


def test_stale_guard():
    facts.assert_fact_applies(9, "9")
    with pytest.raises(facts.StaleFact):
        facts.assert_fact_applies(7, "9")


def test_required_fields_enforced(con):
    with pytest.raises(ValueError):
        facts.emit_fact(
            "cell_override", con=con,
            table_name="t", target_key="k", column_name="c", new_value="1",
            wave_id="w", reason="r", witness="", source_snapshot_id="s",
        )
    with pytest.raises(ValueError):
        facts.emit_fact(
            "source_precedence_decision", con=con,
            stat="passing_interceptions", era="1920-1949", grain="player_week",
            ruling="winner", winner=None, **COMMON,
        )


def test_row_split_requires_two_plus_rows(con):
    with pytest.raises(ValueError):
        facts.emit_fact(
            "row_split", con=con,
            table_name="t", target_key="k",
            replacement_rows_json=json.dumps([{"a": 1}]), **COMMON,
        )
    facts.emit_fact(
        "row_split", con=con,
        table_name="t", target_key="k",
        replacement_rows_json=json.dumps([{"a": 1}, {"a": 2}]), **COMMON,
    )
