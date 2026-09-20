"""Focused safety tests for Yahoo's active-season refresh entrypoint."""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd


def test_yahoo_active_worker_checks_provider_pair_graph_before_staging():
    text = (Path(__file__).resolve().parents[4] / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    block = text.split("for week in refresh_weeks:\n        scoreboards", 1)[1].split("transaction_windows =", 1)[0]
    assert "validate_yahoo_week_matchup_scope(" in block
    assert block.index("validate_yahoo_week_matchup_scope(") < block.index("merge_provider_refresh_table(")


def test_yahoo_roster_gaps_write_an_incomplete_source_receipt_before_failing():
    text = (Path(__file__).resolve().parents[4] / "scripts" / "refresh_yahoo_active_season.py").read_text(
        encoding="utf-8"
    )
    assert "class YahooIncompleteSourceError" in text
    assert 'raise YahooIncompleteSourceError(f"Yahoo roster fetch failed for weeks: {roster_failures}")' in text
    catch = text.split("except YahooIncompleteSourceError as exc:", 1)[1].split("raise", 1)[0]
    assert 'receipt["status"] = "INCOMPLETE_SOURCE"' in catch
    assert 'receipt["error_code"] = "yahoo_rosters_unavailable"' in catch
    assert "write_refresh_receipt(receipt, args.json_out)" in catch


def test_missing_yahoo_oauth_writes_a_reauthentication_receipt_before_failing():
    text = (Path(__file__).resolve().parents[4] / "scripts" / "refresh_yahoo_active_season.py").read_text(
        encoding="utf-8"
    )
    catch = text.split("except YahooCredentialRequiredError as exc:", 1)[1].split("raise", 1)[0]
    assert 'receipt["status"] = "CREDENTIAL_REQUIRED"' in catch
    assert 'receipt["error_code"] = "yahoo_oauth_reauthentication_required"' in catch
    assert "write_refresh_receipt(receipt, args.json_out)" in catch


def test_predraft_yahoo_season_is_a_clean_noop_before_local_hydration():
    from refresh_yahoo_active_season import yahoo_season_is_predraft

    assert yahoo_season_is_predraft({"metadata": {"draft_status": "predraft"}})
    assert not yahoo_season_is_predraft({"metadata": {"draft_status": "postdraft"}})

    source = (Path(__file__).resolve().parents[4] / "scripts" / "refresh_yahoo_active_season.py").read_text(
        encoding="utf-8"
    )
    main = source[source.index("def main(") :]
    predraft_gate = main.index("if yahoo_season_is_predraft(active_raw_settings):")
    homepage_read = main.index("homepage_source_future = start_background_refresh_call(")
    hydration = main.index("hydrate_local_refresh_sources(")
    assert predraft_gate < hydration
    assert predraft_gate < homepage_read
    assert "raw_settings=active_raw_settings" in main


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_weekly_snapshot_reads_each_franchise_identity_row_once():
    from multi_league.core.delta_publish import canonical_table_registry
    from refresh_yahoo_active_season import UPDATE_REFRESH_SOURCE_TABLES, _source_frames

    class LocalReader:
        def __init__(self, conn):
            self.conn = conn
            self.lock = threading.Lock()

        def query(self, sql, *, database):
            assert database == "___leagues"
            # DuckDB connections are not safe for concurrent execute/fetch
            # sequences. Production uses independent Fly HTTP requests; keep
            # this in-memory adapter deterministic while exercising that fanout.
            with self.lock:
                result = self.conn.execute(sql)
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, row)) for row in result.fetchall()]

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        for table_name in set(UPDATE_REFRESH_SOURCE_TABLES):
            assert table_name in canonical_table_registry()
            conn.execute(
                f'CREATE TABLE public."{table_name}" '
                '(db_name VARCHAR, year INTEGER, marker VARCHAR)'
            )
        conn.execute(
            "INSERT INTO public.franchise_identity_audit VALUES "
            "('league_a', 2026, 'audit_once')"
        )
        conn.execute(
            "INSERT INTO public.franchise_identity_registry VALUES "
            "('league_a', 2026, 'registry_once')"
        )
        frames = _source_frames(
            LocalReader(conn), db_name="league_a", active_year=2026,
            tables=UPDATE_REFRESH_SOURCE_TABLES,
        )
        assert frames["franchise_identity_audit"]["marker"].tolist() == ["audit_once"]
        assert frames["franchise_identity_registry"]["marker"].tolist() == ["registry_once"]
    finally:
        conn.close()


def test_weekly_refresh_restores_exact_frontend_owned_alias_rows(tmp_path):
    """Shared enrichment may never leave a rewritten user alias table behind."""
    from multi_league.core.local_db import LocalLeagueDB
    from refresh_yahoo_active_season import _restore_frontend_configuration_rows

    source = pd.DataFrame(
        [
            {
                "id": 1,
                "db_name": "afi_data",
                "operation": "rename",
                "from_name": "Elizabeth",
                "to_name": "Elizabeth + Joe",
                "applied_at": "2026-09-15 03:06:43.85424",
            }
        ]
    )
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        local.ensure_table("manager_overrides")
        local._insert_into_table("manager_overrides", source)
        local.connect().execute(
            "UPDATE public.manager_overrides SET to_name = 'Provider Name' WHERE db_name = 'afi_data'"
        )

        _restore_frontend_configuration_rows(
            local,
            {"manager_overrides": source},
            db_name="afi_data",
        )

        actual = local.read_table("manager_overrides").sort_values("id").reset_index(drop=True)
        assert actual["to_name"].tolist() == ["Elizabeth + Joe"]
        assert actual["from_name"].tolist() == ["Elizabeth"]
    finally:
        local.close()


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


def test_every_active_refresher_compares_final_history_to_the_fly_snapshot():
    """The active-only local input is not a valid historical witness."""
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_sleeper_active_season.py",
        "refresh_espn_active_season.py",
    ):
        text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert "preservation_before = preservation_witnesses" in text
        assert "preservation_before = local_preservation_snapshot" not in text


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


def test_active_yahoo_key_fast_path_retains_saved_multiplatform_lineage():
    from refresh_yahoo_active_season import _active_yahoo_history

    history = _active_yahoo_history(
        SimpleNamespace(league_id="461.l.9", league_ids={
            "2014": "331.l.1", "2025": "461.l.9", "2024": "sleeper-2024",
        }),
        oauth=object(), active_year=2026, source_active_key="470.l.10",
        discover=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fast path must not discover")),
    )
    assert history == {"2014": "331.l.1", "2024": "sleeper-2024", "2025": "461.l.9", "2026": "470.l.10"}


def test_unpersisted_yahoo_context_rediscovers_native_chain_from_active_key():
    """Imported settings are not authority when the context never saved a chain."""
    from refresh_yahoo_active_season import _active_yahoo_history

    discovered_from: list[str] = []

    def discover(anchor: str, **_kwargs):
        discovered_from.append(anchor)
        return {
            "2014": "331.l.1",
            "2015": "348.l.2",
            "2016": "359.l.3",
            "2025": "461.l.9",
            "2026": "470.l.10",
        }

    history = _active_yahoo_history(
        SimpleNamespace(
            league_id="461.l.9",
            # These were inferred from damaged legacy settings, not persisted
            # by the provider-native import chain finder.
            league_ids={
                "2014": "331.l.1",
                "2015": "461.l.9",
                "2016": "461.l.9",
                "2025": "461.l.9",
            },
        ),
        oauth=object(),
        active_year=2026,
        source_active_key="470.l.10",
        has_persisted_chain=False,
        discover=discover,
    )

    assert discovered_from == ["470.l.10"]
    assert history == {
        "2014": "331.l.1",
        "2015": "348.l.2",
        "2016": "359.l.3",
        "2025": "461.l.9",
        "2026": "470.l.10",
    }


def test_unpersisted_yahoo_context_bounds_discovery_when_imported_timeline_is_complete():
    """A unique contiguous imported chain needs only its newest renewal edge."""
    from refresh_yahoo_active_season import _active_yahoo_history

    calls: list[tuple[str, int | None, int | None]] = []

    def discover(anchor: str, **kwargs):
        calls.append((anchor, kwargs.get("start_year"), kwargs.get("end_year")))
        return {
            "2025": "461.l.9",
            "2026": "470.l.10",
        }

    history = _active_yahoo_history(
        SimpleNamespace(
            league_id="461.l.9",
            league_ids={
                "2022": "414.l.6",
                "2023": "423.l.7",
                "2024": "449.l.8",
                "2025": "461.l.9",
            },
        ),
        oauth=object(),
        active_year=2026,
        source_active_key="470.l.10",
        has_persisted_chain=False,
        discover=discover,
    )

    assert calls == [("470.l.10", 2025, 2026)]
    assert history == {
        "2022": "414.l.6",
        "2023": "423.l.7",
        "2024": "449.l.8",
        "2025": "461.l.9",
        "2026": "470.l.10",
    }


def test_captured_yahoo_chain_avoids_legacy_network_discovery():
    """The signed freshness chain is stronger than another Yahoo history walk."""
    from refresh_yahoo_active_season import _active_yahoo_history

    history = _active_yahoo_history(
        SimpleNamespace(
            league_id="461.l.9",
            league_ids={
                "2022": "414.l.6",
                "2023": "423.l.7",
                "2024": "449.l.8",
                "2025": "461.l.9",
            },
        ),
        oauth=object(),
        active_year=2026,
        source_active_key="470.l.10",
        captured_history={
            "2022": "414.l.6",
            "2023": "423.l.7",
            "2024": "449.l.8",
            "2025": "461.l.9",
            "2026": "470.l.10",
        },
        has_persisted_chain=False,
        discover=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("verified captured chain must avoid Yahoo discovery")
        ),
    )

    assert history == {
        "2022": "414.l.6",
        "2023": "423.l.7",
        "2024": "449.l.8",
        "2025": "461.l.9",
        "2026": "470.l.10",
    }


def test_captured_yahoo_chain_rejects_overlap_conflicts_before_fetch():
    import pytest

    from refresh_yahoo_active_season import _active_yahoo_history

    with pytest.raises(RuntimeError, match="captured Yahoo chain conflicted"):
        _active_yahoo_history(
            SimpleNamespace(league_id="461.l.9", league_ids={"2025": "461.l.9"}),
            oauth=object(),
            active_year=2026,
            source_active_key="470.l.10",
            captured_history={"2025": "461.l.999", "2026": "470.l.10"},
            has_persisted_chain=False,
            discover=lambda *_args, **_kwargs: {},
        )


def test_yahoo_native_discovery_stops_at_requested_season_bounds(monkeypatch):
    """A two-season renewal check must not walk the league's entire history."""
    from multi_league.core import yahoo_league_settings

    metadata = {
        "470.l.10": {"season": "2026", "renew": "461_9", "renewed": ""},
        "461.l.9": {"season": "2025", "renew": "449_8", "renewed": "470_10"},
        "449.l.8": {"season": "2024", "renew": "423_7", "renewed": "461_9"},
    }
    fetched: list[str] = []

    def fetch(url: str, _oauth):
        key = url.split("/league/", 1)[1].split("/settings", 1)[0]
        fetched.append(key)
        return key

    monkeypatch.setattr(yahoo_league_settings, "_fetch_url_xml", fetch)
    monkeypatch.setattr(yahoo_league_settings, "_parse_league_metadata", metadata.__getitem__)

    history = yahoo_league_settings.discover_league_history(
        "470.l.10",
        oauth=object(),
        start_year=2025,
        end_year=2026,
    )

    assert history == {"2025": "461.l.9", "2026": "470.l.10"}
    assert fetched == ["470.l.10", "461.l.9"]


def test_native_chain_backfill_happens_after_user_configuration_preservation_gate():
    """The intentional league_ids_json change must not trip the pre-write guard."""
    source = (ROOT / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    main = source[source.index("def main(") :]

    preservation = main.index('receipt["preservation"] = assert_refresh_preservation(')
    persistence = main.index('receipt["renewal_chain_backfilled"] = _persist_yahoo_renewal_chain(')

    assert preservation < persistence


def test_source_frames_select_the_current_multiplatform_leg_before_provider_fetch():
    """A Yahoo worker must stop before fetch when the 2026 leg belongs to Sleeper."""
    import pandas as pd
    import pytest
    from refresh_yahoo_active_season import _active_update_segment_from_source_frames

    frames = {
        "league_context": pd.DataFrame([
            {"db_name": "mixed_league", "platform": "yahoo", "league_id": "461.l.90939"}
        ]),
        "league_settings": pd.DataFrame([
            {"db_name": "mixed_league", "year": 2025, "platform": "yahoo", "league_key": "461.l.90939"},
            {"db_name": "mixed_league", "year": 2026, "platform": "sleeper", "league_key": "1352102370921705472"},
        ]),
    }

    with pytest.raises(RuntimeError, match="belongs to sleeper"):
        _active_update_segment_from_source_frames(
            frames,
            db_name="mixed_league",
            active_year=2026,
            expected_platform="yahoo",
        )


def test_active_yahoo_key_fast_path_rejects_saved_fly_conflict():
    from refresh_yahoo_active_season import _active_yahoo_history
    import pytest

    with pytest.raises(RuntimeError, match="conflicting active Yahoo league keys"):
        _active_yahoo_history(
            SimpleNamespace(league_id="470.l.10", league_ids={"2026": "470.l.10"}),
            oauth=object(), active_year=2026, source_active_key="470.l.11",
            discover=lambda *_args, **_kwargs: {},
        )


def test_active_yahoo_history_rejects_a_non_yahoo_active_id_before_fetch():
    from refresh_yahoo_active_season import _active_yahoo_history
    import pytest

    with pytest.raises(RuntimeError, match="no valid active Yahoo league key"):
        _active_yahoo_history(
            SimpleNamespace(league_id="461.l.9", league_ids={
                "2025": "461.l.9", "2026": "1389710321509232641",
            }),
            oauth=object(), active_year=2026,
            discover=lambda *_args, **_kwargs: {},
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
                return SimpleNamespace(fetchone=lambda: (0, None), fetchall=lambda: [])

        _conn = Connection()

        def read_table(self, _table, *, year):
            assert year == 2026
            return pd.DataFrame([{"db_name": "demo_league", "year": 2026, "manager_week": "Joe202601"}])

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
    monkeypatch.setattr(refresh, "_attach_ops_cache_for_enrichment", lambda _local_db: None)
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

        def query(self, _sql, *, database):
            assert database == "___ops"
            return [
                {"column_name": "NFL_player_id"},
                {"column_name": "nfl_team"},
                {"column_name": "opponent_nfl_team"},
            ]

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


def test_finalized_provider_schedule_rows_are_not_restored_over_canonical_schedule():
    """A retry keeps the live graph but never appends a second final-week identity."""
    from refresh_yahoo_active_season import _unresolved_provider_schedule_rows

    class Local:
        class Connection:
            @staticmethod
            def execute(_sql, _params):
                return SimpleNamespace(fetchall=lambda: [(1,)])

        def connect(self):
            return self.Connection()

    source = pd.DataFrame([
        {"year": 2026, "week": 1, "manager_week": "Gray_2026_1"},
        {"year": 2026, "week": 2, "manager_week": "Gray_2026_2"},
    ])

    remaining = _unresolved_provider_schedule_rows(
        source, local_db=Local(), db_name="league_a", active_year=2026,
    )

    assert remaining["week"].tolist() == [2]
