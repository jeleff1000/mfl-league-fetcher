"""THE POSITION LAW IS ENFORCED, AND THE ENFORCEMENT ITSELF IS TESTED.

Joe 2026-08-05: "make sure rules are fully law and theres no way to edit the
table and break the law. A rule on editing the file itself that cant be
broken by anyone."

Three things have to be true, and each is proven here rather than asserted:

  1  THE LAW HOLDS on the plane as it stands.
  2  THE LAW REFUSES violations -- watched refusing, on poisoned input, so
     it cannot be a gate that only looks like one.
  3  THE LAW CANNOT BE SWITCHED OFF -- neutering the checker makes the
     writer's own self-test fail, so no plane can be written past a
     disabled gate.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=2")
    return con


def test_law_refuses_poison():
    """A gate does not exist until watched refusing."""
    from scripts.sota_recon.witness_gate import position_law as L
    con = _con()
    L.self_test(con)          # the three canonical poisons

    # and a hand-built violating relation must raise, not report
    con.execute("""CREATE OR REPLACE TEMP TABLE poisoned AS
      SELECT * FROM (VALUES ('X', 2000, 1, 'OL,K', 'OL', 'OL'))
      AS t(NFL_player_id, year, week, fantasy_position, position, nfl_position)""")
    with pytest.raises(L.PositionLawViolation):
        L.assert_frame(con, "poisoned", label="poisoned probe")


def test_each_law_article_has_an_independent_poison():
    from scripts.sota_recon.witness_gate import position_law as L
    con = _con()
    L.self_test(con)


def test_content_fingerprint_detects_same_size_same_mtime_mutation(tmp_path):
    from scripts.sota_recon.witness_gate.position_law import content_sha256, input_fingerprint
    path = tmp_path / "declaration.bin"
    path.write_bytes(b"ABCDEF")
    before = content_sha256(path)
    fingerprint_before = input_fingerprint([path])
    stat = path.stat()
    path.write_bytes(b"UVWXYZ")
    import os
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert content_sha256(path) != before
    assert input_fingerprint([path]) != fingerprint_before


def test_mutation_report_exists_and_is_armed():
    import json
    report = Path(__file__).parents[2] / "docs/audits/sota-recon/position/position-law-mutation-report.json"
    assert report.exists()
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["self_test"] == "PASS"
    assert payload["consistency_policy"] == "exact_zero_violations"


def test_law_cannot_be_disabled():
    """Stubbing the checker must break the writer's own arming step."""
    from scripts.sota_recon.witness_gate import position_law as L
    con = _con()
    original = L.check_frame
    L.check_frame = lambda *a, **k: []          # the saboteur
    try:
        with pytest.raises(L.PositionLawViolation):
            L.self_test(con)
    finally:
        L.check_frame = original


def test_combo_order_lock_is_two_sided():
    """The order cannot be reworded in one file only."""
    import hashlib
    import json
    from scripts.sota_recon.witness_gate.position_taxonomy import (
        COMBO_ORDER, COMBO_ORDER_SHA256)
    tax = Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"
    block = json.loads(tax.read_text(encoding="utf-8"))["combo_order"]
    assert block["LOCKED"] is True
    assert tuple(block["order"]) == COMBO_ORDER
    fp = hashlib.sha256(",".join(block["order"]).encode()).hexdigest()
    assert fp == block["fingerprint_sha256"] == COMBO_ORDER_SHA256


def test_law_holds_on_the_plane():
    """Every article, against the plane as it actually stands."""
    from scripts.sota_recon import sources as S
    from scripts.sota_recon.apply_weekly_overlays import _season_plane
    from scripts.sota_recon.witness_gate import position_law as L
    con = _con()
    violations = L.check_frame(
        con, f"read_parquet('{S.weekly_read_path()}')",
        season_plane=_season_plane())
    assert not violations, "POSITION LAW BROKEN:\n  " + "\n  ".join(violations)


def test_stored_equals_the_rules():
    """The plane must hold exactly what the rules produce -- not merely
    something the rules would tolerate. This is what catches a hand-edited
    table: a value can be legal and still not be what the rule derives."""
    from scripts.sota_recon import sources as S
    from scripts.sota_recon.apply_weekly_overlays import (
        _season_plane, fantasy_position_with_usage, position_expr)
    con = _con()
    wk, sp = S.weekly_read_path(), _season_plane()
    n, fp_same, pos_same = con.execute(f"""
    WITH x AS (
      SELECT t.fantasy_position AS stored, ({fantasy_position_with_usage()}) AS rule,
             t.position AS pstored, ({position_expr()}) AS prule
      FROM read_parquet('{wk}') t
      LEFT JOIN (SELECT NFL_player_id AS sp_pid, CAST(year AS INT) AS sp_yr,
                 declared AS sp_pos FROM read_parquet('{sp}')
                 WHERE declared IS NOT NULL) sp
        ON sp.sp_pid = t.NFL_player_id AND sp.sp_yr = CAST(t.year AS INT))
    SELECT COUNT(*), COUNT(*) FILTER (WHERE stored IS NOT DISTINCT FROM rule),
           COUNT(*) FILTER (WHERE pstored IS NOT DISTINCT FROM prule) FROM x""").fetchone()
    assert fp_same == n, f"fantasy_position drifted from its rule on {n - fp_same} rows"
    assert pos_same == n, f"position drifted from its rule on {n - pos_same} rows"
