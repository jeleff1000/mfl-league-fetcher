"""Guards for the NFL.com column audit's MEASUREMENT, not for any particular number.

Every guard here runs on a synthetic fixture. A guard that asserts a real defect is
currently present dies the moment the defect is fixed -- this programme has already lost a
`pat_pct` guard that way -- so these pin the SHAPE of the measurement instead: that a
placeholder is not read as a value, and that a count is scored on equality.
"""
from __future__ import annotations

import duckdb
import pytest

from .nflcom_column_audit import BLOCK_RECOVERY, ERA_SQL, published


@pytest.fixture()
def con():
    c = duckdb.connect()
    # `''` is NFL.com's "this column does not apply to this row" -- the Defense Career
    # table renders a tackle block and an interception block into one physical schema and
    # blanks the half that does not apply. Rows 3-4 are that blank.
    c.execute("""CREATE TABLE src AS SELECT * FROM (VALUES
        ('a', 2015, '5', '1'), ('b', 2015, '0', '2'),
        ('c', 2015, '',  '3'), ('d', 2015, '',  '4')) t(slug, season, tkl, yr_ignored)""")
    return c


def test_published_rejects_the_empty_string_and_keeps_a_real_zero(con):
    """`0` is a measured zero; `''` is the absence of a measurement. COUNT() conflates them."""
    n_published = con.execute(
        f"SELECT COUNT(*) FROM src WHERE {published('tkl')}").fetchone()[0]
    n_notnull = con.execute("SELECT COUNT(*) FROM src WHERE src.tkl IS NOT NULL").fetchone()[0]
    assert n_published == 2, "a blank column must not count as published"
    assert n_notnull == 4, "IS NOT NULL passes the placeholder -- this is the trap"
    assert n_published < n_notnull, (
        "if these are ever equal on this fixture the published filter has stopped working")
    kept = [r[0] for r in con.execute(
        f"SELECT slug FROM src WHERE {published('tkl')} ORDER BY 1").fetchall()]
    assert kept == ["a", "b"], "a real zero must survive the publication filter"


def test_an_unpublished_row_is_not_scored_as_a_disagreement(con):
    """The defect this replaced: SUM(TRY_CAST('')) is NULL and ABS(NULL - v) <= 1 is NULL,
    so a blank row fell out of the AGREE count while staying in the informative base."""
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES "
                "('a', 5.0), ('b', 0.0), ('c', 7.0), ('d', 9.0)) x(slug, v)")

    def measure(where: str) -> tuple[int, int]:
        return con.execute(f"""
            WITH w AS (SELECT slug, SUM(TRY_CAST(src.tkl AS DOUBLE)) sv
                       FROM src WHERE {where} GROUP BY 1),
                 j AS (SELECT w.sv, t.v vv FROM w JOIN t USING (slug))
            SELECT COUNT(*) FILTER (WHERE sv > 0 OR vv > 0),
                   COUNT(*) FILTER (WHERE (sv > 0 OR vv > 0) AND sv = vv) FROM j""").fetchone()

    naive_n, naive_agree = measure("src.tkl IS NOT NULL")
    fixed_n, fixed_agree = measure(published("tkl"))
    assert (naive_n, naive_agree) == (3, 1), "c and d enter the base carrying no source value"
    assert (fixed_n, fixed_agree) == (1, 1), "only the row the source published is evidence"
    assert fixed_agree / fixed_n > naive_agree / naive_n, (
        "counting unpublished rows against us is what depressed opp_fr to 46.9%")


def test_a_count_is_scored_on_equality_not_a_plus_minus_one_band():
    """+/-1 against a median of 1 accepts {0, 1, 2}: it is not a tolerance, it is a smear.
    143 of 143 `sfty` rows that 'agreed' in 1982-93 were src=1 against our 0."""
    c = duckdb.connect()
    c.execute("CREATE TABLE j AS SELECT * FROM (VALUES "
              "(1.0, 0.0), (1.0, 0.0), (1.0, 0.0), (1.0, 1.0)) x(sv, vv)")
    exact, pm1 = c.execute("""SELECT
        100.0 * COUNT(*) FILTER (WHERE sv = vv) / COUNT(*),
        100.0 * COUNT(*) FILTER (WHERE ABS(sv - vv) <= 1) / COUNT(*) FROM j""").fetchone()
    assert exact == 25.0
    assert pm1 == 100.0, "the band accepts every row, including three flat contradictions"
    assert pm1 > exact, "where these diverge the band is reporting agreement that is not there"


def test_era_strata_separate_a_clean_mapping_from_a_coverage_floor():
    """`pdef` is 99.1% on 2010+ and 0.0% before 1982. The blend, 73.2%, describes neither
    and reads as a defect; the strata read as a clean mapping under a coverage floor."""
    c = duckdb.connect()
    c.execute("CREATE TABLE j AS SELECT * FROM (VALUES "
              "(2015, 3.0, 3.0), (2015, 4.0, 4.0), (1975, 2.0, 0.0), (1975, 5.0, 0.0)) x(yr, sv, vv)")
    got = dict(c.execute(f"""SELECT {ERA_SQL} era,
        ROUND(100.0 * COUNT(*) FILTER (WHERE sv = vv) / COUNT(*), 1) FROM j GROUP BY 1""").fetchall())
    assert got == {"2010+": 100.0, "<1982": 0.0}
    blended = c.execute(
        "SELECT ROUND(100.0*COUNT(*) FILTER (WHERE sv=vv)/COUNT(*),1) FROM j").fetchone()[0]
    assert blended == 50.0, "the blend is an average of a clean era and an absent one"


def test_era_boundaries_are_recording_changes_and_stay_put():
    """Each boundary is a season the league changed what it records -- 1982 sacks, 2000
    passes-defended/safeties. Moving one silently re-bases every stratified verdict."""
    c = duckdb.connect()
    years = [(y,) for y in (1970, 1981, 1982, 1993, 1994, 1999, 2000, 2009, 2010, 2025)]
    c.execute("CREATE TABLE j (yr INT)")
    c.executemany("INSERT INTO j VALUES (?)", years)
    got = [r[0] for r in c.execute(f"SELECT {ERA_SQL} FROM j").fetchall()]
    assert got == ["<1982", "<1982", "1982-93", "1982-93", "1994-99",
                   "1994-99", "2000-09", "2000-09", "2010+", "2010+"]


def test_block_recovery_refuses_a_tied_rb_wr_identity():
    """A shared RB/WR schema cannot be assigned from a rate identity that ties.

    `att=rec=1`, `yds=avg=51` satisfies both possible equations. Returning either block
    would manufacture the missing axis and contaminate every downstream witness.
    """
    c = duckdb.connect()
    c.execute("""CREATE TABLE x AS SELECT * FROM (VALUES
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         5.0, 2.0, 25.0, 5.0),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         2.0, 5.0, 25.0, 5.0),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         1.0, 1.0, 51.0, 51.0),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, NULL, NULL, NULL)
    ) t(comp, scky, fgm, fg_att, xpm, punts, net_yds, total, pdef, sfty, g, gs,
        att, rec, yds, avg)""")
    got = [r[0] for r in c.execute(
        f"SELECT {BLOCK_RECOVERY} FROM x").fetchall()]
    assert got == ["RBFB5", "WRTE", None, None]


# ---------------------------------------------------------------------------------------
# AUDIT CAPACITY: the second question. `disposition` says whether a column is OURS;
# `audit_capacity` says whether it can CHECK us. Synthetic fixtures throughout -- these pin
# the shape of the claim, never a live number.
# ---------------------------------------------------------------------------------------
from .column_dossier import _validate_audit_capacity  # noqa: E402


def _entry(**cap):
    return {"key": "s|t|c", "disposition": "EXCLUDED_WITH_REASON",
            "reason": "r", "evidence": "e" * 40, "audit_capacity": cap}


def test_a_column_with_no_capacity_claim_is_not_penalised():
    """Most excluded columns carry no claim at all; absence is silence, not a violation."""
    assert _validate_audit_capacity({"key": "s|t|c"}) == []


def test_receipted_capacity_must_carry_its_measurement_and_its_control():
    """An adjudication without evidence is an opinion, and so is a capacity claim without
    the crossed control -- `avg == yds/int` at 99.98% only means something beside
    `avg == yds/g` at 15.65%."""
    naked = _validate_audit_capacity(_entry(
        kind="EQUATION", status="RECEIPTED", expression="a == b / c",
        audits=["def_interceptions"]))
    assert any("measurement" in p for p in naked)
    assert any("crossed control" in p for p in naked)
    ok = _validate_audit_capacity(_entry(
        kind="EQUATION", status="RECEIPTED", expression="a == b / c",
        audits=["def_interceptions"], receipt="99.98% of 15,251",
        crossed_control="the alternative denominator reads 15.65%"))
    assert ok == []


def test_capacity_must_name_real_canonicals():
    """A capacity that audits nothing real is a mapping to nowhere wearing a new field."""
    bad = _validate_audit_capacity(_entry(
        kind="AGGREGATE", status="DECLARED", expression="g == there_is_no_such_column",
        audits=["there_is_no_such_column"]))
    assert any("not a canonical" in p for p in bad)


def test_blocked_capacity_must_say_what_it_is_blocked_on():
    """`g` has a route and no target -- v26 carries no games column. BLOCKED without a
    stated blocker is indistinguishable from unfinished work."""
    bad = _validate_audit_capacity(_entry(
        kind="AGGREGATE", status="BLOCKED", expression="g == games_played",
        audits=["games_played"]))
    assert any("blocked on" in p for p in bad)


def test_vocabulary_is_closed():
    """A free-text kind or status would let the queue be emptied by inventing a word."""
    assert any("not in vocabulary" in p for p in
               _validate_audit_capacity(_entry(kind="VIBES", status="RECEIPTED")))
    assert any("not in vocabulary" in p for p in
               _validate_audit_capacity(_entry(kind="KEY", status="PROBABLY_FINE")))
