"""Tests for draft type detection and pick-score support behavior."""

import duckdb

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.draft.sql_draft_enrichments import DraftEnrichmentsMixin
from multi_league.transformations.draft.sql_draft_enrichments import _detect_draft_type_for_year


class _DraftRunner(DraftEnrichmentsMixin, SQLEnrichmentsBase):
    pass


class TestDetectDraftTypeForYear:
    """Test _detect_draft_type_for_year() standalone function."""

    def test_auction_from_metadata(self):
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "auction"}'}) == "auction"

    def test_live_is_ambiguous(self):
        """Yahoo 'live' means live online draft — could be snake or auction."""
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "live"}'}) is None

    def test_self_is_ambiguous(self):
        """Yahoo 'self' means offline/commissioner-entered — could be snake or auction."""
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "self"}'}) is None

    def test_linear_is_snake(self):
        """Sleeper 'linear' draft type is a snake variant."""
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "linear"}'}) == "snake"

    def test_offline_snake(self):
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "offline_snake"}'}) == "snake"

    def test_snake_explicit(self):
        assert _detect_draft_type_for_year({"metadata": '{"draft_type": "snake"}'}) == "snake"

    def test_empty_metadata_returns_none(self):
        assert _detect_draft_type_for_year({"metadata": "{}"}) is None

    def test_missing_metadata_returns_none(self):
        assert _detect_draft_type_for_year({}) is None

    def test_malformed_json_returns_none(self):
        assert _detect_draft_type_for_year({"metadata": "not json"}) is None


class TestDetectDraftTypePerYear:
    def test_external_draft_type_resolves_independently_by_year(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.execute(
            """
            CREATE TABLE public.draft (
                year INTEGER,
                cost DOUBLE,
                draft_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.league_settings (
                year INTEGER,
                draft_type VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO public.draft VALUES (?, ?, ?)",
            [
                (2024, 0.0, "snake"),
                (2024, None, "snake"),
                (2025, 1.0, "Auction"),
                (2025, 18.0, "Auction"),
                (2025, 0.0, "Auction"),
            ],
        )
        conn.executemany(
            "INSERT INTO public.league_settings VALUES (?, ?)",
            [
                (2024, "snake"),
                (2025, "snake"),
            ],
        )

        runner = _DraftRunner(db_name="draft_type_test", data_dir=str(tmp_path), conn=conn)
        try:
            assert runner._get_draft_type_per_year() == {2024: "snake", 2025: "auction"}
            runner.normalize_draft_type_per_year()
            assert conn.execute(
                "SELECT year, draft_type FROM public.draft GROUP BY year, draft_type ORDER BY year"
            ).fetchall() == [
                (2024, "snake"),
                (2025, "auction"),
            ]
            assert conn.execute("SELECT year, draft_type FROM public.league_settings ORDER BY year").fetchall() == [
                (2024, "snake"),
                (2025, "auction"),
            ]
        finally:
            conn.close()

    def test_settings_used_when_draft_rows_have_no_explicit_type(self, tmp_path):
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.execute(
            """
            CREATE TABLE public.draft (
                year INTEGER,
                cost DOUBLE,
                draft_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.league_settings (
                year INTEGER,
                draft_type VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO public.draft VALUES (?, ?, ?)",
            [
                (2025, 0.0, None),
                (2025, None, ""),
                (2025, 0.0, "live"),
            ],
        )
        conn.execute("INSERT INTO public.league_settings VALUES (2025, 'auction')")

        runner = _DraftRunner(db_name="draft_type_test", data_dir=str(tmp_path), conn=conn)
        try:
            assert runner._get_draft_type_per_year() == {2025: "auction"}
        finally:
            conn.close()
