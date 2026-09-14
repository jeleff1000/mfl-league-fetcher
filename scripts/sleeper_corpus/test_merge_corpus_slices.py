"""Regression tests for schema-compatible corpus slice merges."""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
import merge_corpus_slices as merger


class MergeSliceTest(unittest.TestCase):
    def test_missing_newer_source_column_is_inserted_as_null(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            central_path = root / "central.duckdb"
            slice_path = root / "old_slice.duckdb"
            central = duckdb.connect(str(central_path))
            central.execute("CREATE SCHEMA public")
            central.execute("CREATE TABLE public.league_settings (db_name VARCHAR, waiver_budget INTEGER)")
            central.execute("CREATE TABLE _sources (db_name VARCHAR, folded_at TIMESTAMP)")

            source = duckdb.connect(str(slice_path))
            source.execute("CREATE SCHEMA public")
            source.execute("CREATE TABLE public.league_settings (db_name VARCHAR)")
            source.execute("INSERT INTO public.league_settings VALUES ('smpl_old')")
            source.execute("CREATE TABLE _sources (db_name VARCHAR, folded_at TIMESTAMP)")
            source.execute("INSERT INTO _sources VALUES ('smpl_old', NULL)")
            source.close()

            original_tables = merger.TABLES
            merger.TABLES = {"league_settings": ([
                ("db_name", "VARCHAR"), ("waiver_budget", "INTEGER")
            ], "")}
            try:
                merged, skipped = merger.merge_slice(central, slice_path, set())
            finally:
                merger.TABLES = original_tables

            self.assertEqual((merged, skipped), (1, 0))
            self.assertEqual(
                central.execute("SELECT db_name, waiver_budget FROM public.league_settings").fetchall(),
                [("smpl_old", None)],
            )
            central.close()

    def test_main_exits_nonzero_when_any_slice_fails(self) -> None:
        class Central:
            def execute(self, _sql):
                return self

            def fetchone(self):
                return (0,)

            def close(self):
                pass

        with tempfile.TemporaryDirectory() as td:
            slice_path = Path(td) / "corpus_slice_bad.duckdb"
            slice_path.touch()
            original_tables = merger.TABLES
            merger.TABLES = {}
            try:
                with (
                    patch.object(sys, "argv", ["merge_corpus_slices.py", td]),
                    patch.object(merger, "open_snapshot", return_value=Central()),
                    patch.object(merger, "folded_set", return_value=set()),
                    patch.object(merger, "merge_slice", side_effect=RuntimeError("bad slice")),
                ):
                    with self.assertRaises(SystemExit):
                        merger.main()
            finally:
                merger.TABLES = original_tables


if __name__ == "__main__":
    unittest.main()
