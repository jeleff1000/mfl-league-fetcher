"""Focused safety tests for Yahoo's active-season refresh entrypoint."""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest


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


def test_active_year_ops_cache_uses_the_short_ui_retry_budget(tmp_path):
    from refresh_yahoo_active_season import _build_active_year_ops_cache

    class Reader:
        calls = []

        def query(self, sql, *, database, **kwargs):
            assert database == "___ops"
            self.calls.append(kwargs)
            if "player_bio" in sql:
                return [
                    {"column_name": "NFL_player_id", "column_type": "VARCHAR"},
                    {"column_name": "headshot_url", "column_type": "VARCHAR"},
                ]
            return [
                {"column_name": name, "column_type": column_type}
                for name, column_type in (
                    ("NFL_player_id", "VARCHAR"),
                    ("year", "INTEGER"),
                    ("week", "INTEGER"),
                    ("season_type", "VARCHAR"),
                    ("nfl_team", "VARCHAR"),
                    ("opponent_nfl_team", "VARCHAR"),
                )
            ]

        def query_df_parquet(self, sql, *, database, **kwargs):
            assert database == "___ops"
            self.calls.append(kwargs)
            if "player_bio" in sql:
                return pd.DataFrame([{"NFL_player_id": "player-1", "headshot_url": None}])
            return pd.DataFrame([{
                "NFL_player_id": "player-1", "year": 2026, "week": 5,
                "season_type": "REG", "nfl_team": "CHI", "opponent_nfl_team": "GB",
            }])

    reader = Reader()
    _build_active_year_ops_cache(
        reader,
        output=tmp_path / "active_ops.duckdb",
        year=2026,
        scoring_info={},
    )

    assert reader.calls == [{"timeout_seconds": 8, "max_retries": 3}] * 4


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
            def execute(_sql, _params=None):
                if "information_schema.columns" in _sql:
                    return SimpleNamespace(fetchall=lambda: [("franchise_id",)])
                return SimpleNamespace(fetchone=lambda: (0, None), fetchall=lambda: [])

        _conn = Connection()

        def read_table(self, _table, *, year):
            assert year == 2026
            if _table == "matchup":
                return pd.DataFrame([
                    {"year": 2026, "manager_guid": "owner-joe", "franchise_id": "stable-joe"},
                    {"year": 2026, "manager_guid": "owner-tom", "franchise_id": "stable-tom"},
                ])
            return pd.DataFrame([{
                "db_name": "demo_league", "year": 2026, "week": 2,
                "manager_week": "Joe202602", "manager_guid": "owner-joe",
                "opponent_guid": "owner-tom", "franchise_id": "seasonal-joe",
            }])

        def close(self):
            return None

        def connect(self):
            return self._conn

        def table_exists(self, _table):
            return _table == "matchup"

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


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("final_mode", ["complete", "partial", "none"])
def test_shared_refresh_schedule_keeps_saved_owner_merges_without_final_duplicates(
    monkeypatch, tmp_path, platform, final_mode,
):
    """Raw owner IDs must align before filtering final team-weeks (178 -> 168)."""
    import multi_league.core.import_pipeline as import_pipeline
    import multi_league.core.league_refresh as league_refresh
    import multi_league.transformations.sql_enrichments as sql_enrichments
    import refresh_yahoo_active_season as refresh

    def row(week, team):
        opponent = team + 1 if team % 2 else team - 1
        return {
            "db_name": "league_a", "year": 2026, "week": week,
            "team_key": str(team), "manager_guid": f"current-{team}",
            "franchise_id": f"current-{team}_0", "manager": f"M{team}",
            "franchise_name": f"M{team}", "opponent": f"M{opponent}",
            "opponent_guid": f"current-{opponent}",
            "opponent_franchise_id": f"current-{opponent}_0",
            "manager_week": f"current-{team}_0_2026_{week}",
            "team_points": 100.0, "opponent_points": 90.0, "is_bye_week": False,
        }

    final_rows = pd.DataFrame([row(week, team)
        for week in range(1, 6) for team in range(1, 13)
        if not (final_mode == "partial" and week == 4 and team in (7, 8))])
    if final_mode == "none":
        final_rows = final_rows.iloc[:0].copy()
    provider = pd.DataFrame([row(week, team)
        for week in range(1, 15) for team in range(1, 13)]).drop(columns="team_key")
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.register("final_fixture", final_rows)
    conn.execute("CREATE TABLE public.matchup AS SELECT * FROM final_fixture")
    conn.register("provider_fixture", provider)
    conn.execute("CREATE TABLE public.schedule AS SELECT * FROM provider_fixture")

    class Local:
        _conn = conn

        def connect(self):
            return conn

        def close(self):
            pass  # The fixture owns this in-memory connection.

        def table_exists(self, table):
            return bool(conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=?", [table]).fetchone()[0])

        def read_table(self, table, *, year):
            return conn.execute(f"SELECT * FROM public.{table} WHERE db_name='league_a' AND year=?", [year]).fetchdf()

        def merge_table(self, table, frame, keys, **kwargs):
            assert table == "schedule" and keys == ["db_name", "manager_week"]
            conn.register("restored_fixture", frame)
            columns = ", ".join(provider.columns)
            conn.execute("DELETE FROM public.schedule WHERE manager_week IN (SELECT manager_week FROM restored_fixture)")
            conn.execute(f"INSERT INTO public.schedule ({columns}) SELECT {columns} FROM restored_fixture")

    # Keep actual saved-merge SQL and identity/filter/merge behavior. Only
    # unrelated NFL enrichment, simulations and aggregate work is stubbed.
    def identity_enrichments(enricher):
        enricher._apply_saved_franchise_merges()
        # Mirror the canonical composite keys observed in the live pipeline.
        conn.execute("UPDATE public.matchup SET manager_week=franchise_id || '_2026_' || CAST(week AS VARCHAR)")
        columns = ", ".join(provider.columns)
        conn.execute(f"CREATE OR REPLACE TABLE public.schedule AS SELECT {columns} FROM public.matchup")
        return {}

    monkeypatch.setattr(import_pipeline, "run_transformation_pipeline", lambda *args, **kwargs: [])
    monkeypatch.setattr(sql_enrichments.SQLEnrichments, "run_all", identity_enrichments)
    monkeypatch.setattr(sql_enrichments.SQLEnrichments, "load_settings_from_db", lambda self: ({}, {}))
    monkeypatch.setattr(sql_enrichments.SQLEnrichments, "_attach_ops", lambda *args: None)
    monkeypatch.setattr(refresh, "_attach_ops_cache_for_enrichment", lambda *args: None)
    monkeypatch.setattr(league_refresh, "resolve_active_player_nfl_ids_from_bio", lambda *args, **kwargs: 0)
    monkeypatch.setattr(refresh, "_run_refresh_simulations", lambda **kwargs: None)
    monkeypatch.setattr(refresh, "_run_refresh_aggregates", lambda *args, **kwargs: None)
    ctx = SimpleNamespace(manager_name_overrides={}, get_league_id_for_year=lambda _: "provider-league",
        franchise_merges=[{"display_name": f"Preferred{team}", "owner_ids": [f"saved-{team}", f"current-{team}"]} for team in (2, 12)])
    try:
        for _ in range(2):
            refresh._run_local_pipeline(ctx=ctx, context_path=tmp_path / "context.json",
                local_db=Local(), db_name="league_a", active_year=2026,
                work_dir=tmp_path, platform=platform)
            assert conn.execute("SELECT COUNT(*) FROM public.schedule").fetchone()[0] == 168
            assert conn.execute("SELECT week FROM public.schedule GROUP BY week HAVING COUNT(*) <> 12 OR COUNT(DISTINCT franchise_id) <> 12").fetchall() == []
            assert conn.execute("SELECT COUNT(*) FROM public.schedule WHERE franchise_id IN ('current-2_0', 'current-12_0')").fetchone()[0] == 0
            assert conn.execute("SELECT DISTINCT manager FROM public.schedule WHERE franchise_id IN ('saved-2_0', 'saved-12_0') ORDER BY manager").fetchall() == [("Preferred12",), ("Preferred2",)]
            if final_mode == "partial":
                assert conn.execute("SELECT COUNT(*) FROM public.schedule WHERE week=4 AND franchise_id IN ('current-7_0', 'current-8_0')").fetchone()[0] == 2
    finally:
        conn.close()


def test_shared_refresh_pipeline_restores_active_derived_values_before_aggregating(
    monkeypatch, tmp_path,
):
    """Season rollups must include source values restored after simulation."""
    import multi_league.core.import_pipeline as import_pipeline
    import multi_league.core.league_update_ownership as ownership
    import multi_league.transformations.sql_enrichments as sql_enrichments
    import refresh_yahoo_active_season as refresh

    events = []

    class FakeEnricher:
        def __init__(self, **_kwargs):
            pass

        def load_settings_from_db(self):
            return {}, {}

        def run_all(self):
            return {}

        def close(self):
            return None

    class FakeLocalDb:
        class Connection:
            @staticmethod
            def execute(sql, _params):
                if "COUNT(*), MAX(week)" in sql:
                    return SimpleNamespace(fetchone=lambda: (12, 3))
                return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [])

        _conn = Connection()

        def read_table(self, _table, *, year):
            assert year == 2026
            return pd.DataFrame()

        def close(self):
            return None

        def connect(self):
            return self._conn

    source_frames = {"matchup": pd.DataFrame([{"year": 2026}])}
    restored = {"matchup": {"p_playoffs": 4, "p_champ": 4}}

    monkeypatch.setattr(import_pipeline, "run_transformation_pipeline", lambda *args, **kwargs: [])
    monkeypatch.setattr(sql_enrichments, "SQLEnrichments", FakeEnricher)
    monkeypatch.setattr(refresh, "_attach_ops_cache_for_enrichment", lambda _local_db: None)
    monkeypatch.setattr(
        refresh,
        "_run_refresh_simulations",
        lambda **_kwargs: events.append("simulations"),
    )
    monkeypatch.setattr(
        ownership,
        "restore_active_derived_source_values",
        lambda *_args, **_kwargs: events.append("restore") or restored,
    )
    monkeypatch.setattr(
        refresh,
        "_run_refresh_aggregates",
        lambda *_args, **_kwargs: events.append("aggregates"),
    )

    result = refresh._run_local_pipeline(
        ctx=SimpleNamespace(manager_name_overrides={}, franchise_merges=[]),
        context_path=tmp_path / "league_context.json",
        local_db=FakeLocalDb(),
        db_name="dom_s_year",
        active_year=2026,
        work_dir=tmp_path,
        active_derived_source_frames=source_frames,
    )

    assert events == ["simulations", "restore", "aggregates"]
    assert result == restored


def test_active_refresh_inputs_read_independent_fly_scopes_concurrently():
    """Weekly refresh setup must not serialize unrelated ops and league reads."""
    import threading

    from refresh_yahoo_active_season import _load_active_refresh_inputs

    class Reader:
        def __init__(self):
            self.barrier = threading.Barrier(2)

        def query(self, _sql, *, database, **kwargs):
            assert database == "___ops"
            assert kwargs == {"timeout_seconds": 8, "max_retries": 3}
            return [
                {"column_name": "NFL_player_id"},
                {"column_name": "nfl_team"},
                {"column_name": "opponent_nfl_team"},
            ]

        def query_df(self, _sql, *, database, **kwargs):
            assert database == "___ops"
            assert kwargs == {"timeout_seconds": 8, "max_retries": 3}
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
            def execute(_sql, _params=None):
                if "information_schema.columns" in _sql:
                    return SimpleNamespace(fetchall=lambda: [("team_key",)])
                return SimpleNamespace(fetchall=lambda: [(1, "team-1")])

        def connect(self):
            return self.Connection()

    source = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "team-1", "manager_week": "Gray_2026_1"},
        {"year": 2026, "week": 2, "team_key": "team-1", "manager_week": "Gray_2026_2"},
    ])

    remaining = _unresolved_provider_schedule_rows(
        source, local_db=Local(), db_name="league_a", active_year=2026,
    )

    assert remaining["week"].tolist() == [2]


@pytest.mark.parametrize("identity_column", ["team_key", "franchise_id"])
def test_partial_final_week_retains_only_unresolved_team_schedule_rows(identity_column):
    """The missing week-4 pair survives without duplicating 38 finalized rows."""
    import duckdb
    from refresh_yahoo_active_season import _unresolved_provider_schedule_rows

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute(f"CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, {identity_column} VARCHAR, team_points DOUBLE, opponent_points DOUBLE, is_bye_week BOOLEAN)")
        finalized = [("league_a", 2026, week, f"team-{team}", 100.0, 90.0, False)
            for week in range(1, 6) for team in range(1, 9)
            if not (week == 4 and team in (7, 8))]
        conn.executemany("INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)", finalized)
        conn.execute("INSERT INTO public.matchup VALUES ('unrelated', 2026, 4, 'team-7', 100, 90, false)")
        source = pd.DataFrame([{"year": 2026, "week": week, identity_column: f"team-{team}", "manager_week": f"team-{team}_2026_{week}"}
            for week in range(1, 16) for team in range(1, 9)])
        remaining = _unresolved_provider_schedule_rows(source,
            local_db=SimpleNamespace(connect=lambda: conn), db_name="league_a", active_year=2026)
        assert len(remaining) == 82
        assert remaining.loc[remaining["week"].eq(4), identity_column].tolist() == ["team-7", "team-8"]
        assert not remaining["week"].isin([1, 2, 3, 5]).any()
    finally:
        conn.close()


@pytest.mark.parametrize("final_native,provider_native,franchise,expected", [
    ("team-a", "team-b", "shared", ["team-b"]),
    (None, "team-b", "shared", []),
    ("team-a", None, "shared", []),
    (None, "team-b", "", ["team-b"]),
])
def test_schedule_native_team_identity_precedes_shared_franchise(final_native, provider_native, franchise, expected):
    import duckdb
    from refresh_yahoo_active_season import _unresolved_provider_schedule_rows

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, team_key VARCHAR, franchise_id VARCHAR, team_points DOUBLE, opponent_points DOUBLE, is_bye_week BOOLEAN)")
        conn.execute("INSERT INTO public.matchup VALUES ('league_a', 2026, 4, ?, ?, 100, 90, false)", [final_native, franchise])
        source = pd.DataFrame([{"week": 4, "team_key": provider_native, "franchise_id": franchise}])
        remaining = _unresolved_provider_schedule_rows(source,
            local_db=SimpleNamespace(connect=lambda: conn), db_name="league_a", active_year=2026)
        assert remaining["team_key"].tolist() == expected
    finally:
        conn.close()


def test_scheduled_demo_mode_is_limited_to_demo_with_kmffl_credentials():
    from refresh_yahoo_active_season import authorize_scheduled_demo

    assert authorize_scheduled_demo(
        requested=True,
        database_name="demo_league",
        credential_database_name="kmffl",
    )

    for database_name, credential_database_name in (
        ("kmffl", "kmffl"),
        ("demo_league", "demo_league"),
        ("another_grandfathered_league", "kmffl"),
    ):
        with pytest.raises(PermissionError, match="scheduled demo"):
            authorize_scheduled_demo(
                requested=True,
                database_name=database_name,
                credential_database_name=credential_database_name,
            )


def test_normal_yahoo_refresh_is_not_a_scheduled_demo_bypass():
    from refresh_yahoo_active_season import authorize_scheduled_demo

    assert not authorize_scheduled_demo(
        requested=False,
        database_name="demo_league",
        credential_database_name="kmffl",
    )
@pytest.mark.parametrize(
    ("metadata", "expected"),
    [({"current_week": "6", "is_finished": "0"}, (6, 7, 8)),
     ({"current_week": "6", "is_finished": "1"}, ()), ({}, ())],
)
def test_future_schedule_boundary_comes_from_yahoo_not_requested_ceiling(metadata, expected):
    from refresh_yahoo_active_season import _yahoo_future_schedule_weeks
    assert _yahoo_future_schedule_weeks({"metadata": metadata}, regular_season_end=8) == expected
