import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import initial_import_v3


def test_require_sql_enrichment_success_blocks_partial_uploads():
    with pytest.raises(RuntimeError, match="resolve_all_nfl_player_ids"):
        initial_import_v3._require_sql_enrichment_success(
            {
                "resolve_all_nfl_player_ids": ("error", 'Catalog "___ops" does not exist'),
                "dedup_player_fantasy": 12,
            }
        )


def test_require_sql_enrichment_success_blocks_failed_aggregation():
    with pytest.raises(RuntimeError, match="Fantasy aggregation"):
        initial_import_v3._require_sql_enrichment_success({}, fantasy_aggregation_ok=False)


def test_require_sql_enrichment_success_accepts_complete_results():
    initial_import_v3._require_sql_enrichment_success(
        {"resolve_all_nfl_player_ids": 100, "optional_step": None},
        fantasy_aggregation_ok=True,
    )


def test_create_local_sql_enricher_forwards_saved_identity_settings():
    captured = {}

    class FakeEnricher:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    ctx = SimpleNamespace(
        is_single_year_import=False,
        manager_name_overrides={"Jeff - Last Place": "Jeff"},
        franchise_merges=[{"into_franchise_id": "canonical", "owner_ids": ["old", "canonical"]}],
    )

    initial_import_v3._create_local_sql_enricher(
        ctx=ctx,
        db_name="monsters_of_the_midway",
        data_dir="/tmp/league",
        conn=object(),
        enricher_cls=FakeEnricher,
    )

    assert captured["manager_name_overrides"] == {"Jeff - Last Place": "Jeff"}
    assert captured["franchise_merges"] == [
        {"into_franchise_id": "canonical", "owner_ids": ["old", "canonical"]}
    ]


class _FakeCtx:
    def __init__(self, data_directory: str):
        self.league_name = "KMFFL"
        self.league_id = "461.l.90939"
        self.league_ids = {"2025": "461.l.90939"}
        self.start_year = 2025
        self.end_year = 2025
        self.data_directory = data_directory
        self.import_mode = "full"

    def has_league_ids_mapping(self):
        return True

    @property
    def is_single_year_import(self):
        return False

    def save(self, _path):
        return None


class _FakeLocalLeagueDB:
    def __init__(self, data_dir, db_name):
        self.data_dir = data_dir
        self.db_name = db_name
        self.db_path = Path(data_dir) / f"{db_name}.duckdb"

    def connect(self):
        return None

    def ensure_table(self, _table):
        return None

    def close(self):
        return None


def test_targeted_fetch_respects_skip_track_2_upload(tmp_path, monkeypatch):
    context_path = tmp_path / "league_context.json"
    context_path.write_text("{}", encoding="utf-8")

    captured = {}
    ctx = _FakeCtx(str(tmp_path))

    monkeypatch.setattr(initial_import_v3, "_load_ctx", lambda _path: ctx)
    monkeypatch.setattr(
        initial_import_v3,
        "runtime_from_source",
        lambda *_args, **_kwargs: SimpleNamespace(data_dir=tmp_path, db_name="kmffl_smoke_local"),
    )
    monkeypatch.setattr(initial_import_v3, "LocalLeagueDB", _FakeLocalLeagueDB)

    def _fake_run_targeted_fetch(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "rows": 162, "failed_weeks": None, "error": None}

    monkeypatch.setattr(initial_import_v3, "run_targeted_fetch", _fake_run_targeted_fetch)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "initial_import_v3.py",
            "--context",
            str(context_path),
            "--fetch",
            "matchups",
            "--year",
            "2025",
            "--skip-track-2-upload",
        ],
    )

    with pytest.raises(SystemExit) as excinfo:
        initial_import_v3.main()

    assert excinfo.value.code == 0
    assert captured["upload_to_fly"] is False


def test_load_frontend_context_settings_preserves_private_flag():
    class _Reader:
        def query(self, sql, database):
            assert database == "___leagues"
            assert "is_private" in sql
            return [
                {
                    "manager_name_overrides_json": None,
                    "franchise_merges_json": "[]",
                    "keeper_rules_json": None,
                    "league_rules_json": None,
                    "standings_weights_json": None,
                    "is_private": True,
                }
            ]

    settings = initial_import_v3._load_frontend_context_settings(_Reader(), "private_league")

    assert settings["is_private"] is True


def test_load_frontend_context_settings_restores_yahoo_renewal_chain():
    """A weekly Yahoo refresh must retain the chain saved by the original import."""

    class _Reader:
        def query(self, _sql, database):
            assert database == "___leagues"
            return [
                {
                    "league_name": "KMFFL",
                    "league_ids_json": '{"2025":"461.l.90939","2026":"470.l.80971"}',
                    "manager_name_overrides_json": None,
                    "franchise_merges_json": "[]",
                    "keeper_rules_json": None,
                    "league_rules_json": None,
                    "standings_weights_json": None,
                    "is_private": False,
                }
            ]

    settings = initial_import_v3._load_frontend_context_settings(_Reader(), "kmffl")

    assert settings["league_ids"] == {"2025": "461.l.90939", "2026": "470.l.80971"}


def test_build_context_from_fly_reuses_supplied_yahoo_reader_and_frontend_settings(tmp_path, monkeypatch):
    """A Yahoo worker must not probe unrelated provider registries it already excluded."""
    from multi_league.utils import credential_store

    class Reader:
        def __init__(self):
            self.queries: list[str] = []

        def query(self, sql, *, database):
            assert database == "___ops"
            self.queries.append(sql)
            if "main.league_credentials" not in sql:
                raise AssertionError("known Yahoo workers must not query another provider registry")
            return [
                {
                    "league_id": "470.l.80971",
                    "league_name": "Registry Name",
                    "encrypted_refresh_token": "encrypted-token",
                }
            ]

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def save(self, path):
            self.saved_path = path
            path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(credential_store, "get_encryption_key", lambda: "test-key")
    monkeypatch.setattr(credential_store, "decrypt_token", lambda _value, _key: "refresh-token")
    monkeypatch.setattr(initial_import_v3, "LeagueContext", Context)
    monkeypatch.setenv("YAHOO_CLIENT_ID", "client-id")
    monkeypatch.setenv("YAHOO_CLIENT_SECRET", "client-secret")
    reader = Reader()

    ctx, context_path = initial_import_v3._build_context_from_fly(
        "the_league",
        data_dir_override=str(tmp_path),
        reader=reader,
        frontend_settings={"league_name": "Canonical Name", "league_ids": {"2026": "470.l.80971"}},
    )

    assert len(reader.queries) == 1
    assert ctx.league_name == "Canonical Name"
    assert ctx.league_ids == {"2026": "470.l.80971"}
    assert context_path.is_file()


def test_build_context_from_fly_can_use_shared_yahoo_credential_without_changing_target(tmp_path, monkeypatch):
    """A demo clone may authenticate as KMFFL while retaining its own identity and aliases."""
    from multi_league.utils import credential_store

    class Reader:
        def __init__(self):
            self.queries: list[str] = []

        def query(self, sql, *, database):
            assert database == "___ops"
            self.queries.append(sql)
            assert "database_name = 'kmffl'" in sql
            return [{
                "league_id": "470.l.80971",
                "league_name": "KMFFL",
                "encrypted_refresh_token": "encrypted-token",
            }]

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def save(self, path):
            path.write_text("{}", encoding="utf-8")

    aliases = {"Eleff": "Joe", "Marc": "Tom"}
    franchise_merges = [{"canonical": "Joe", "members": ["Eleff", "Joseph"]}]
    keeper_rules = {"enabled": True, "max_keepers": 2}
    league_rules = {"playoff_teams": 6}
    standings_weights = {"wins": 1.0, "points": 0.25}
    monkeypatch.setattr(credential_store, "get_encryption_key", lambda: "test-key")
    monkeypatch.setattr(credential_store, "decrypt_token", lambda _value, _key: "refresh-token")
    monkeypatch.setattr(initial_import_v3, "LeagueContext", Context)
    monkeypatch.setenv("YAHOO_CLIENT_ID", "client-id")
    monkeypatch.setenv("YAHOO_CLIENT_SECRET", "client-secret")
    reader = Reader()

    ctx, context_path = initial_import_v3._build_context_from_fly(
        "demo_league",
        data_dir_override=str(tmp_path),
        reader=reader,
        credential_database_name="kmffl",
        frontend_settings={
            "league_name": "Demo League",
            "league_ids": {"2026": "470.l.80971"},
            "manager_name_overrides": aliases,
            "franchise_merges": franchise_merges,
            "keeper_rules": keeper_rules,
            "league_rules": league_rules,
            "standings_weights": standings_weights,
            "is_private": True,
        },
    )

    assert len(reader.queries) == 1
    assert ctx.database_name == "demo_league"
    assert ctx.league_name == "Demo League"
    assert ctx.league_ids == {"2026": "470.l.80971"}
    assert ctx.manager_name_overrides == aliases
    assert ctx.franchise_merges == franchise_merges
    assert ctx.keeper_rules == keeper_rules
    assert ctx.league_rules == league_rules
    assert ctx.standings_weights == standings_weights
    assert ctx.is_private is True
    assert context_path.is_file()


def test_partial_history_source_still_requires_settings_and_players():
    assert initial_import_v3._pre_upload_non_empty_tables(
        allow_empty_quick_startup=False,
        allow_partial_history_source=True,
    ) == ["league_settings", "player_fantasy"]


def test_detect_locally_complete_yahoo_years_requires_all_core_tables():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    for table_name in ("matchup", "player_fantasy", "draft", "transactions", "schedule"):
        conn.execute(f"CREATE TABLE public.{table_name} (year INTEGER)")

    for table_name in ("matchup", "player_fantasy", "draft", "transactions", "schedule"):
        conn.execute(f"INSERT INTO public.{table_name} VALUES (2025)")

    for table_name in ("matchup", "player_fantasy", "draft", "transactions"):
        conn.execute(f"INSERT INTO public.{table_name} VALUES (2024)")

    fake_db = SimpleNamespace(conn=conn, connect=lambda: conn)

    assert initial_import_v3._detect_locally_complete_yahoo_years(fake_db, [2024, 2025]) == [2025]


def test_local_schedule_year_has_no_played_rows_detects_empty_shell():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.schedule (year INTEGER, team_points DOUBLE, opponent_points DOUBLE)")
    conn.execute("INSERT INTO public.schedule VALUES (2018, 0, 0), (2018, NULL, NULL)")
    conn.execute("INSERT INTO public.schedule VALUES (2019, 0, 14)")

    fake_db = SimpleNamespace(connect=lambda: conn)

    assert initial_import_v3._local_schedule_year_has_no_played_rows(fake_db, 2018) is True
    assert initial_import_v3._local_schedule_year_has_no_played_rows(fake_db, 2019) is False


def test_run_v3_fetcher_saves_schedule_when_matchups_are_all_unplayed(tmp_path, monkeypatch):
    saved: list[tuple[str, pd.DataFrame, int]] = []

    class _FakeDb:
        def save_table(self, table_name, df, year, **_kwargs):
            saved.append((table_name, df.copy(), year))

    ctx = SimpleNamespace(
        data_directory=tmp_path,
        league_id="380.l.822676",
        manager_name_overrides={},
    )
    empty_matchups = pd.DataFrame()
    empty_matchups.attrs["schedule_df"] = pd.DataFrame(
        {
            "year": [2018, 2018],
            "week": [1, 1],
            "manager": ["Team A", "Team B"],
            "manager_guid": ["ga", "gb"],
            "team_name": ["A", "B"],
            "opponent": ["Team B", "Team A"],
            "team_points": [0.0, 0.0],
            "opponent_points": [0.0, 0.0],
            "win": [0, 0],
            "loss": [0, 0],
        }
    )

    monkeypatch.setattr(initial_import_v3, "create_oauth_session", lambda _ctx: (object(), None))
    monkeypatch.setattr(initial_import_v3, "ensure_session_healthy", lambda oauth, _ctx: oauth)

    failures = initial_import_v3.run_v3_fetcher(
        ctx=ctx,
        db=_FakeDb(),
        fetcher_name="matchups",
        years=[2018],
        fetch_year_fn=lambda **_kwargs: empty_matchups,
        table_name="matchup",
        sleep_between_years=0,
    )

    assert failures == {}
    assert [(table_name, year) for table_name, _df, year in saved] == [("schedule", 2018)]


def test_run_track_1_initial_uses_shared_cache_aware_verify(monkeypatch):
    captured = {}

    def _fake_shared_verify(start_year, end_year, dry_run=False):
        captured.update({"start_year": start_year, "end_year": end_year, "dry_run": dry_run})
        return True

    monkeypatch.setattr(initial_import_v3, "_shared_track_1_verify", _fake_shared_verify)

    assert initial_import_v3.run_track_1_initial(1999, 2025, dry_run=True) is True
    assert captured == {"start_year": 1999, "end_year": 2025, "dry_run": True}


def test_apply_quick_import_prefers_most_recent_mapped_year(monkeypatch):
    monkeypatch.setattr(initial_import_v3, "get_current_nfl_season_year", lambda: 2025)
    monkeypatch.setattr(initial_import_v3, "get_nfl_state", lambda: {})

    ctx = SimpleNamespace(
        league_id="461.l.current",
        league_ids={"2021": "390.l.old", "2024": "449.l.latest"},
        start_year=2008,
        end_year=2025,
        import_mode="full",
        oauth_file_path=None,
    )

    updated = initial_import_v3._apply_quick_import(ctx)

    assert updated.start_year == 2021
    assert updated.end_year == 2024
    assert updated.quick_import_years == [2021, 2024]
    assert updated.import_mode == "quick"
    assert updated.league_id == "449.l.latest"
    assert updated.league_ids["2024"] == "449.l.latest"


def test_apply_quick_import_explicit_year_keeps_existing_mapping(monkeypatch):
    monkeypatch.setattr(initial_import_v3, "get_current_nfl_season_year", lambda: 2025)
    monkeypatch.setattr(initial_import_v3, "get_nfl_state", lambda: {})

    ctx = SimpleNamespace(
        league_id="461.l.current",
        league_ids={"2024": "449.l.latest"},
        start_year=2008,
        end_year=2025,
        import_mode="full",
        oauth_file_path=None,
    )

    updated = initial_import_v3._apply_quick_import(ctx, year=2024)

    assert updated.start_year == 2024
    assert updated.end_year == 2024
    assert updated.league_id == "449.l.latest"
    assert updated.league_ids["2024"] == "449.l.latest"


def test_apply_quick_import_infers_yahoo_year_from_league_key(monkeypatch):
    monkeypatch.setattr(initial_import_v3, "get_current_nfl_season_year", lambda: 2026)
    monkeypatch.setattr(initial_import_v3, "get_nfl_state", lambda: {})

    def _unexpected_settings_call(*_args, **_kwargs):
        raise AssertionError("settings should not be fetched when league key maps to a season")

    monkeypatch.setattr(initial_import_v3, "fetch_league_settings", _unexpected_settings_call)

    ctx = SimpleNamespace(
        league_id="461.l.70510",
        league_ids={},
        start_year=None,
        end_year=None,
        import_mode="full",
        oauth_file_path=None,
    )

    updated = initial_import_v3._apply_quick_import(ctx)

    assert updated.start_year == 2025
    assert updated.end_year == 2025
    assert updated.quick_import_years == [2025]
    assert updated.league_id == "461.l.70510"
    assert updated.league_ids["2025"] == "461.l.70510"


def test_apply_quick_import_includes_previous_scored_year_for_empty_shell(monkeypatch):
    monkeypatch.setattr(initial_import_v3, "get_current_nfl_season_year", lambda: 2026)
    monkeypatch.setattr(
        initial_import_v3,
        "get_nfl_state",
        lambda: {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
    )

    ctx = SimpleNamespace(
        league_id="470.l.2026",
        league_ids={"2024": "449.l.2024", "2025": "461.l.2025", "2026": "470.l.2026"},
        start_year=2010,
        end_year=2026,
        import_mode="full",
        oauth_file_path=None,
    )

    updated = initial_import_v3._apply_quick_import(ctx)

    assert updated.start_year == 2025
    assert updated.end_year == 2026
    assert updated.quick_import_years == [2025, 2026]
    assert updated.league_id == "470.l.2026"


def test_allow_empty_quick_startup_allows_draft_activity_without_matchups():
    class _FakeDb:
        counts = {
            "matchup": 0,
            "player_fantasy": 0,
            "schedule": 0,
            "draft": 12,
            "transactions": 2,
        }

        def table_exists(self, table_name):
            return table_name in self.counts

        def row_count(self, table_name):
            return self.counts[table_name]

    ctx = SimpleNamespace(import_mode="quick", quick_import_years=[2026], start_year=2026, end_year=2026)

    assert initial_import_v3._allow_empty_quick_startup(ctx, _FakeDb(), {2026}) is True


def test_allow_empty_quick_fallback_target_requires_prior_matchups():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.matchup (year INTEGER)")
    conn.execute("INSERT INTO public.matchup VALUES (2021)")

    db = SimpleNamespace(connect=lambda: conn)
    ctx = SimpleNamespace(import_mode="quick", quick_import_years=[2021, 2022])

    assert initial_import_v3._allow_empty_quick_fallback_target(ctx, db, 2022) is True
    assert initial_import_v3._allow_empty_quick_fallback_target(ctx, db, 2021) is False

    empty_conn = duckdb.connect(":memory:")
    empty_conn.execute("CREATE SCHEMA public")
    empty_conn.execute("CREATE TABLE public.matchup (year INTEGER)")
    empty_db = SimpleNamespace(connect=lambda: empty_conn)

    assert initial_import_v3._allow_empty_quick_fallback_target(ctx, empty_db, 2022) is False
