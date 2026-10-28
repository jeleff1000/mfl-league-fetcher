"""A provider rename must not detach an existing user alias from its franchise."""
import json
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

from multi_league.core import league_update_ownership as ownership


def witnesses():
    return {
        "league_context": pd.DataFrame([{
            "db_name": "league", "manager_name_overrides_json": json.dumps({"Brandon": "Nam"}),
        }]),
        "matchup": pd.DataFrame([{
            "db_name": "league", "year": 2026, "manager": "Nam", "franchise_id": "stable-id",
        }]),
    }


def test_restore_saved_alias_by_franchise_on_all_active_source_surfaces():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    for table in ("matchup", "schedule", "player_fantasy", "draft", "transactions"):
        conn.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, year INTEGER, manager VARCHAR, "
                     "franchise_id VARCHAR, franchise_name VARCHAR, opponent VARCHAR, opponent_franchise_id VARCHAR, "
                     "source_manager VARCHAR, source_franchise_id VARCHAR, destination_manager VARCHAR, destination_franchise_id VARCHAR)")
        for db, year, fid in (("league", 2026, "stable-id"), ("league", 2025, "stable-id"),
                              ("other", 2026, "stable-id"), ("league", 2026, "different-id")):
            conn.execute(f"INSERT INTO public.{table} VALUES (?, ?, 'New Provider Name', ?, "
                         "'New Provider Name', 'New Provider Name', ?, 'New Provider Name', ?, 'New Provider Name', ?)",
                         [db, year, fid, fid, fid, fid])
    local = SimpleNamespace(connect=lambda: conn, table_exists=lambda table: True)

    ownership.restore_active_manager_aliases(local, witnesses(), active_year=2026)

    for table in ("matchup", "schedule", "player_fantasy", "draft", "transactions"):
        names = conn.execute(f"SELECT manager, franchise_name, opponent, source_manager, destination_manager "
                             f"FROM public.{table} WHERE db_name='league' AND year=2026 AND franchise_id='stable-id'").fetchone()
        assert names == ("Nam",) * 5
        assert conn.execute(f"SELECT COUNT(*) FROM public.{table} WHERE manager='New Provider Name'").fetchone()[0] == 3
    ownership._assert_active_aliases_preserved(witnesses(), {"matchup": conn.execute("SELECT * FROM public.matchup").df()}, active_year=2026)


def test_unconfigured_provider_names_are_not_frozen():
    before = witnesses()
    before["matchup"]["manager"] = "Not a saved alias"
    assert ownership._configured_active_aliases(before, active_year=2026) == {}


def test_conflicting_aliases_still_fail_instead_of_choosing_a_name():
    before = witnesses()
    before["league_context"]["manager_name_overrides_json"] = json.dumps({"Brandon": "Nam", "B": "Another alias"})
    before["schedule"] = before["matchup"].assign(manager="Another alias")
    with pytest.raises(ownership.PreservationError, match="conflicting active aliases"):
        ownership._configured_active_aliases(before, active_year=2026)


def test_preservation_still_rejects_an_alias_removed_after_reconciliation():
    before = witnesses()
    with pytest.raises(ownership.PreservationError, match="active alias changed"):
        ownership._assert_active_aliases_preserved(before, {"matchup": before["matchup"].assign(manager="Provider")}, active_year=2026)
