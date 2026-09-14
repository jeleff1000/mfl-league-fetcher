"""Unit tests for _disambiguate_duplicate_display_names (Bug A).

Exercises Case A (franchise_id_short tie-breaker fires) and Case B
(canonical + team_name is sufficient) in a single league-year fixture.
Also asserts criterion 8: Case A does not over-fire when Case B suffices.
"""

import re
import sys
from pathlib import Path

import duckdb

SCRIPTS_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.sql_matchup_enrichments import (
    MatchupEnrichmentsMixin,
)


class _MatchupRunner(MatchupEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def _make_fixture(tmp_path, db_name):
    r"""Build a local DuckDB with a 4-franchise matchup table.

    Franchises (last-4-chars are base36-alphanumeric so the Case A suffix
    regex `\([A-Za-z0-9]{4}\)` matches — matching the production
    franchise_id shape):
        fidryan01 manager='Ryan'  team_name='King Ads'    (Case B)
        fidryan02 manager='Ryan'  team_name='GreyWolf'    (Case B)
        fiddave01 manager='Dave'  team_name='Hot Takes'   (Case A; same team_name)
        fiddave02 manager='Dave'  team_name='Hot Takes'   (Case A; same team_name)
        fidchris1 manager='Chris' team_name='Chrisville'  (no-op)
    """
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    rows = [
        # Two Ryans, distinct team_names → Case B
        ("fidryan01", "Ryan", "King Ads", "Ryan", "fidryan02", 2024, 1, 100.0, 90.0),
        ("fidryan02", "Ryan", "GreyWolf", "Ryan", "fidryan01", 2024, 1, 90.0, 100.0),
        # Two Daves, IDENTICAL team_name → Case A
        ("fiddave01", "Dave", "Hot Takes", "Dave", "fiddave02", 2024, 1, 110.0, 80.0),
        ("fiddave02", "Dave", "Hot Takes", "Dave", "fiddave01", 2024, 1, 80.0, 110.0),
        # Unique Chris → no-op
        ("fidchris1", "Chris", "Chrisville", None, None, 2024, 1, 95.0, None),
    ]
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.close()


def test_disambiguate_duplicate_display_names(tmp_path):
    db_name = "disambig_test"
    _make_fixture(tmp_path, db_name)

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        matchup_t = runner._qualified_name("matchup")
        affected = runner._disambiguate_duplicate_display_names(matchup_t)
        conn = runner._get_connection()

        # Criterion 2: no manager maps to multiple franchise_ids
        collisions = conn.execute(
            """
            SELECT manager, COUNT(DISTINCT franchise_id) AS n
            FROM public.matchup
            WHERE manager IS NOT NULL
            GROUP BY manager HAVING n > 1
            """
        ).fetchall()
        assert collisions == [], f"managers still collide: {collisions}"

        # Criterion 3: no franchise_id has multiple managers
        multi_mgr = conn.execute(
            """
            SELECT franchise_id, COUNT(DISTINCT manager) AS n
            FROM public.matchup GROUP BY franchise_id HAVING n > 1
            """
        ).fetchall()
        assert multi_mgr == [], f"franchise_ids with multiple managers: {multi_mgr}"

        # Criterion 1: two disambiguated Ryan names, both "Ryan - ..."
        ryans = sorted(
            r[0]
            for r in conn.execute("SELECT DISTINCT manager FROM public.matchup WHERE manager LIKE 'Ryan%'").fetchall()
        )
        assert ryans == ["Ryan - GreyWolf", "Ryan - King Ads"], ryans

        # Criterion 8: Case A did NOT fire for the Ryans (distinct team_names)
        ryan_caseA = conn.execute(
            r"""
            SELECT COUNT(*) FROM public.matchup
            WHERE manager LIKE 'Ryan%'
              AND REGEXP_MATCHES(manager, '\([A-Za-z0-9]{4}\)')
            """
        ).fetchone()[0]
        assert ryan_caseA == 0, "Case A fired for Ryans unnecessarily"

        # Case A fired for the Daves (identical team_name)
        daves = sorted(
            r[0]
            for r in conn.execute("SELECT DISTINCT manager FROM public.matchup WHERE manager LIKE 'Dave%'").fetchall()
        )
        assert len(daves) == 2, f"expected 2 distinct Dave names, got: {daves}"
        for d in daves:
            assert re.fullmatch(r"Dave - Hot Takes \([A-Za-z0-9]{4}\)", d), d

        # Chris untouched
        chris = conn.execute("SELECT DISTINCT manager FROM public.matchup WHERE manager LIKE 'Chris%'").fetchall()
        assert chris == [("Chris",)], chris

        # Criterion 6: no double-franchise_id_short suffix anywhere
        double_suffix = conn.execute(
            r"""
            SELECT COUNT(*) FROM public.matchup
            WHERE REGEXP_MATCHES(manager, '\([A-Za-z0-9]{4}\).*\([A-Za-z0-9]{4}\)')
            """
        ).fetchone()[0]
        assert double_suffix == 0, "double franchise_id_short suffix detected"

        # Criterion 7: idempotent re-run does not change any manager value.
        # _execute() cannot report a real DuckDB UPDATE rowcount (always -1),
        # so verify re-entrancy structurally by snapshotting the manager
        # column before and after a second call.
        snapshot_before = sorted(
            conn.execute(
                "SELECT franchise_id, manager FROM public.matchup ORDER BY franchise_id, year, week"
            ).fetchall()
        )
        runner._disambiguate_duplicate_display_names(matchup_t)
        snapshot_after = sorted(
            conn.execute(
                "SELECT franchise_id, manager FROM public.matchup ORDER BY franchise_id, year, week"
            ).fetchall()
        )
        assert snapshot_before == snapshot_after, "second run modified manager values; disambiguation is not idempotent"

        # Sanity: _execute() returned its sentinel value on first run (meaning
        # the SQL ran without raising). Real "first run did something" is
        # covered by the Ryan/Dave assertions above.
        assert affected is not None, "first run did not execute"
    finally:
        if runner._conn is not None:
            runner._conn.close()
