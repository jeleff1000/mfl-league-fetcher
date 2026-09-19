"""ESPN local import entry points must apply saved identities before rollups."""

import ast
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from multi_league.transformations.sql_enrichments import SQLEnrichments


SOURCE = Path(__file__).resolve().parents[1] / "fantasy_football_data_scripts/espn_initial_import.py"
CALLS = [
    node for node in ast.walk(ast.parse(SOURCE.read_text(encoding="utf-8")))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    and node.func.id == "SQLEnrichments"
]


@pytest.mark.parametrize("call", CALLS, ids=lambda call: f"entry-line-{call.lineno}")
def test_import_entry_point_applies_saved_merge_and_alias(call, tmp_path):
    # Execute the real entry-point constructor without fetching a whole league.
    # Removing either saved-settings argument must break observable SQL output.
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.matchup(year INTEGER, franchise_id VARCHAR, manager VARCHAR, points DOUBLE)")
        conn.execute("INSERT INTO public.matchup VALUES (2025,'old-owner_0','Old',80), (2026,'new-owner_0','New',90), (2026,'other_0','Other',100)")
        ctx = SimpleNamespace(
            franchise_merges=[{"owner_ids": ["old-owner", "new-owner"], "display_name": "Saved Owner"}],
            manager_name_overrides={"Other": "Preferred Alias"},
        )
        scope = {
            "SQLEnrichments": SQLEnrichments, "db_name": "test_league",
            "md_db_name": "test_league", "data_dir": tmp_path,
            "is_quick_import": False, "ctx": ctx,
            "db": SimpleNamespace(connect=lambda: conn),
        }
        runner = eval(compile(ast.Expression(call), str(SOURCE), "eval"), scope)
        runner._apply_saved_franchise_merges()
        runner._apply_saved_manager_name_overrides()
        assert conn.execute("SELECT year,franchise_id,manager,points FROM public.matchup ORDER BY points").fetchall() == [
            (2025, "old-owner_0", "Saved Owner", 80.0),
            (2026, "old-owner_0", "Saved Owner", 90.0),
            (2026, "other_0", "Preferred Alias", 100.0),
        ]
