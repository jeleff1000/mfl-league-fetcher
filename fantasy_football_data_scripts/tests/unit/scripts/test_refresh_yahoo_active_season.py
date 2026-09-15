"""Focused safety tests for Yahoo's active-season refresh entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_identity_repair_preserves_hydrated_draft_metadata_and_fills_yahoo_ids():
    """The fast repair may replace identities, never the retained draft payload."""
    from refresh_yahoo_active_season import _patched_yahoo_draft_identities

    hydrated = pd.DataFrame(
        [
            {
                "db_name": "the_league",
                "year": 2026,
                "round": 1,
                "pick": 1,
                "player": "Adam Randall",
                "manager": "Existing Manager",
                "team_name": "Existing Team",
                "yahoo_player_id": None,
                "yahoo_position": "RB",
            }
        ]
    )
    identities = pd.DataFrame(
        [
            {
                "year": 2026,
                "round": 1,
                "pick": 1,
                "player": "Adam Randall",
                "yahoo_player_id": "99999",
                "yahoo_position": "RB",
            }
        ]
    )

    repaired = _patched_yahoo_draft_identities(hydrated, identities)

    assert repaired.loc[0, "yahoo_player_id"] == "99999"
    assert repaired.loc[0, "manager"] == "Existing Manager"
    assert repaired.loc[0, "team_name"] == "Existing Team"


def test_identity_repair_refuses_to_leave_a_named_draft_pick_without_yahoo_id():
    """A partial Yahoo payload must not silently preserve the original defect."""
    import pytest

    from refresh_yahoo_active_season import _patched_yahoo_draft_identities

    hydrated = pd.DataFrame(
        [
            {
                "db_name": "the_league",
                "year": 2026,
                "round": 1,
                "pick": 1,
                "player": "Adam Randall",
                "yahoo_player_id": None,
            }
        ]
    )
    empty = pd.DataFrame(columns=["year", "round", "pick", "player", "yahoo_player_id"])

    with pytest.raises(RuntimeError, match="did not return IDs"):
        _patched_yahoo_draft_identities(hydrated, empty)


def test_frontend_settings_from_hydrated_context_preserve_user_configuration():
    """The active fast path must keep settings instead of re-reading or resetting them."""
    from refresh_yahoo_active_season import _frontend_settings_from_source_context

    source = pd.DataFrame(
        [
            {
                "db_name": "the_league",
                "league_name": "The League",
                "league_ids_json": '{"2025":"461.l.104461","2026":"470.l.164172"}',
                "manager_name_overrides_json": '{"Joe":"Commissioner"}',
                "franchise_merges_json": '["old-franchise"]',
                "keeper_rules_json": '{"round":3}',
                "league_rules_json": '{"ppr":1}',
                "standings_weights_json": '{"wins":1}',
                "is_private": True,
            }
        ]
    )

    settings = _frontend_settings_from_source_context(source, db_name="the_league")

    assert settings == {
        "league_name": "The League",
        "league_ids": {"2025": "461.l.104461", "2026": "470.l.164172"},
        "manager_name_overrides": {"Joe": "Commissioner"},
        "franchise_merges": ["old-franchise"],
        "keeper_rules": {"round": 3},
        "league_rules": {"ppr": 1},
        "standings_weights": {"wins": 1},
        "is_private": True,
    }


def test_active_history_extends_the_saved_chain_instead_of_the_credential_league():
    """A user's OAuth row may name another league; renewal identity comes from the saved chain."""
    from refresh_yahoo_active_season import _active_yahoo_history

    discovered_from: list[str] = []

    def discover(anchor: str, **_kwargs):
        discovered_from.append(anchor)
        return {
            "2025": "461.l.90939",
            "2026": "470.l.164172",
        }

    history = _active_yahoo_history(
        SimpleNamespace(
            league_id="470.l.80971",
            league_ids={
                "2024": "449.l.198278",
                "2025": "461.l.90939",
            },
        ),
        oauth=object(),
        active_year=2026,
        discover=discover,
    )

    assert discovered_from == ["461.l.90939"]
    assert history["2026"] == "470.l.164172"


def test_active_history_rejects_a_discovered_chain_that_rewrites_saved_identity():
    """A provider response may extend a chain, but it may not replace a saved season key."""
    import pytest

    from refresh_yahoo_active_season import _active_yahoo_history

    with pytest.raises(RuntimeError, match="conflicted with the saved chain"):
        _active_yahoo_history(
            SimpleNamespace(
                league_id="470.l.80971",
                league_ids={"2025": "461.l.90939"},
            ),
            oauth=object(),
            active_year=2026,
            discover=lambda *_args, **_kwargs: {
                "2025": "461.l.12345",
                "2026": "470.l.164172",
            },
        )


def test_shared_refresh_pipeline_forwards_saved_manager_identity_settings(monkeypatch, tmp_path):
    """All platform refreshers must reapply aliases and franchise merges during enrichment."""
    import multi_league.core.import_pipeline as import_pipeline
    import multi_league.transformations.sql_enrichments as sql_enrichments
    import refresh_yahoo_active_season as refresh

    captured = {}

    class FakeEnricher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def load_settings_from_db(self):
            return {}, {}

        def run_all(self):
            return {}

        def reapply_saved_identity_settings(self):
            captured["identity_reconciles"] = captured.get("identity_reconciles", 0) + 1
            return 0

        def close(self):
            return None

    class FakeLocalDb:
        class Connection:
            @staticmethod
            def execute(_sql, _params):
                return SimpleNamespace(fetchone=lambda: (0, None))

        _conn = Connection()

        def read_table(self, _table, *, year):
            assert year == 2026
            return pd.DataFrame(
                [{"db_name": "demo_league", "year": 2026, "manager_week": "Joe202601"}]
            )

        def close(self):
            return None

        def connect(self):
            return self._conn

        def table_exists(self, _table):
            return False

        def row_count(self, _table):
            return 0

        def merge_table(self, table, frame, keys, **kwargs):
            captured["restored_table"] = table
            captured["restored_rows"] = len(frame)
            captured["restore_keys"] = keys

    monkeypatch.setattr(import_pipeline, "run_transformation_pipeline", lambda *args, **kwargs: [])
    monkeypatch.setattr(sql_enrichments, "SQLEnrichments", FakeEnricher)
    monkeypatch.setattr(refresh, "_run_refresh_aggregates", lambda *args, **kwargs: None)

    ctx = SimpleNamespace(
        manager_name_overrides={"Eleff": "Joe", "Marc": "Tom"},
        get_league_id_for_year=lambda year: "470.l.1" if year == 2026 else None,
        franchise_merges=[
            {
                "display_name": "Joe",
                "into_franchise_id": "stable-joe",
                "owner_ids": ["seasonal-joe", "stable-joe"],
            }
        ],
    )
    refresh._run_local_pipeline(
        ctx=ctx,
        context_path=tmp_path / "league_context.json",
        local_db=FakeLocalDb(),
        db_name="demo_league",
        active_year=2026,
        work_dir=tmp_path,
    )

    assert captured["manager_name_overrides"] == {"Eleff": "Joe", "Marc": "Tom"}
    assert captured["franchise_merges"] == [
        {
            "display_name": "Joe",
            "into_franchise_id": "stable-joe",
            "owner_ids": ["seasonal-joe", "stable-joe"],
        }
    ]
    assert captured["restored_table"] == "schedule"
    assert captured["restored_rows"] == 1
    assert captured["identity_reconciles"] == 1


def test_active_refresh_inputs_read_independent_fly_scopes_concurrently():
    """Weekly refresh setup must not serialize unrelated ops and league reads."""
    import threading

    from refresh_yahoo_active_season import _load_active_refresh_inputs

    class Reader:
        def __init__(self):
            self.barrier = threading.Barrier(2)

        def query_df(self, _sql, *, database):
            assert database == "___ops"
            try:
                self.barrier.wait(timeout=0.25)
            except threading.BrokenBarrierError as exc:
                raise AssertionError("finalized ops read was serialized behind materialized-week lookup") from exc
            return pd.DataFrame(
                [
                    {"week": 1, "nfl_team": "KC", "opponent_nfl_team": "LAC", "NFL_player_id": "00-a"},
                    {"week": 2, "nfl_team": "KC", "opponent_nfl_team": "DEN", "NFL_player_id": "00-a"},
                ]
            )

        def query_scalar(self, _sql, *, database):
            assert database == "___leagues"
            try:
                self.barrier.wait(timeout=0.25)
            except threading.BrokenBarrierError as exc:
                raise AssertionError("materialized-week lookup was serialized behind finalized ops") from exc
            return 2

    finalized_ops, last_materialized_week = _load_active_refresh_inputs(
        Reader(),
        db_name="the_league",
        year=2026,
        through_week=2,
    )

    assert finalized_ops["week"].tolist() == [1, 2]
    assert last_materialized_week == 2
