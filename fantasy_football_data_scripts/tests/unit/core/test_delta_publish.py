import json
import tarfile

import duckdb
import pytest

import multi_league.core.targets.fly_target as fly_target
from multi_league.core.delta_publish import build_delta_bundle
from multi_league.core.franchise_identity_schema import (
    FRANCHISE_IDENTITY_AUDIT_DDL,
    FRANCHISE_IDENTITY_REGISTRY_DDL,
)
from multi_league.core.local_db import LocalLeagueDB


def _seed_minimal_league(conn: duckdb.DuckDBPyConnection, db_name: str = "speed_test") -> None:
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager_week VARCHAR,
            manager VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup
        VALUES
            (?, 2026, 1, 'alice_2026_1', 'Alice', 'Aces', 'Bob'),
            (?, 2026, 1, 'bob_2026_1', 'Bob', 'Bees', 'Alice')
        """,
        [db_name, db_name],
    )
    conn.execute("CREATE TABLE public.scratch_table (db_name VARCHAR, value INTEGER)")
    conn.execute("INSERT INTO public.scratch_table VALUES (?, 99)", [db_name])


def _seed_player_fantasy_with_unrostered_row(
    conn: duckdb.DuckDBPyConnection,
    db_name: str = "speed_test",
) -> None:
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            franchise_id VARCHAR,
            fantasy_position VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        VALUES
            (?, '00-0033873_2026_1', 'alice_0', 'QB', 2026, 1),
            (?, '00-0035228_2026_1', NULL, NULL, 2026, 1)
        """,
        [db_name, db_name],
    )


def _seed_schedule_with_null_opponent(
    conn: duckdb.DuckDBPyConnection,
    db_name: str = "speed_test",
) -> None:
    conn.execute(
        """
        CREATE TABLE public.schedule (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager_week VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.schedule
        VALUES
            (?, 2026, 1, 'alice_2026_1', 'Alice', 'alice_0', 'Bob', 'bob_0'),
            (?, 2026, 2, 'alice_2026_2', 'Alice', 'alice_0', NULL, NULL)
        """,
        [db_name, db_name],
    )


def _seed_transactions_with_shared_transaction_id(
    conn: duckdb.DuckDBPyConnection,
    db_name: str = "speed_test",
) -> None:
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            transaction_id VARCHAR,
            transaction_sequence INTEGER,
            player VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.transactions
        VALUES
            (?, 'txn-1', 0, 'Player A', 2026, 1),
            (?, 'txn-1', 1, 'Player B', 2026, 1)
        """,
        [db_name, db_name],
    )


def _seed_draft_with_multiple_draft_ids_same_pick(
    conn: duckdb.DuckDBPyConnection,
    db_name: str = "speed_test",
) -> None:
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            draft_id VARCHAR,
            round INTEGER,
            pick INTEGER,
            player VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.draft
        VALUES
            (?, 2026, 'startup-1', 1, 1, 'Player A'),
            (?, 2026, 'rookie-1', 1, 1, 'Player B')
        """,
        [db_name, db_name],
    )


def _seed_franchise_identity_tables(
    conn: duckdb.DuckDBPyConnection,
    db_name: str = "speed_test",
) -> None:
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace(
            "franchise_identity_registry",
            "public.franchise_identity_registry",
            1,
        )
    )
    conn.execute(
        FRANCHISE_IDENTITY_AUDIT_DDL.replace(
            "franchise_identity_audit",
            "public.franchise_identity_audit",
            1,
        )
    )
    conn.execute(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        )
        VALUES (
            ?, 'yahoo', 'guid', 'guid', 'team_slot:1', 'team_slot:1',
            'guid_1', 1, 2025, 1, 'Aces', 'Alice', '461.l.1.t.1', '1',
            'Aces', 'Alice', '1', '2025', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """,
        [db_name],
    )
    conn.execute(
        """
        INSERT INTO public.franchise_identity_audit (
            db_name, platform, year, week, manager_guid, base_franchise_id,
            resolved_franchise_id, assignment_key, identity_key, branch_key, team_index,
            team_key, team_slot, team_name, manager, reason, identity_score,
            name_score, slot_score, total_score, created_at
        )
        VALUES (
            ?, 'yahoo', 2025, 1, 'guid', 'guid', 'guid_1',
            'audit-key-1', 'team_slot:1', 'team_slot:1', 1, '461.l.1.t.1',
            '1', 'Aces', 'Alice', 'slot', 0, 100, 30, 130, CURRENT_TIMESTAMP
        )
        """,
        [db_name],
    )


def test_delta_bundle_filters_canonical_tables_and_keeps_logical_hash_stable(tmp_path, monkeypatch):
    monkeypatch.setenv("IMPORT_RUN_ID", "stable-test-run")
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_minimal_league(conn)

    bundle_a = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "a",
    )
    bundle_b = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "b",
    )

    assert bundle_a.bundle_hash == bundle_b.bundle_hash
    assert bundle_a.bundle_id != bundle_b.bundle_id
    assert [entry["table"] for entry in bundle_a.manifest["tables"]] == ["matchup"]
    assert any(entry["table"] == "draft" for entry in bundle_a.manifest["omitted_tables"])
    assert all(entry["table"] != "scratch_table" for entry in bundle_a.manifest["tables"])

    with tarfile.open(bundle_a.path, "r:gz") as archive:
        names = sorted(member.name for member in archive.getmembers() if member.isfile())
    assert names == ["manifest.json", "tables/matchup.parquet"]


def test_delta_bundle_binds_optional_snapshot_generation_into_digest(tmp_path, monkeypatch):
    monkeypatch.setenv("IMPORT_RUN_ID", "snapshot-test-run")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        _seed_minimal_league(conn)
        old = build_delta_bundle(conn, db_name="speed_test", output_dir=tmp_path / "old")
        base_0 = build_delta_bundle(
            conn, db_name="speed_test", base_generation=0, output_dir=tmp_path / "base-0"
        )
        base_1 = build_delta_bundle(
            conn, db_name="speed_test", base_generation=1, output_dir=tmp_path / "base-1"
        )
        assert "base_generation" not in old.manifest
        assert base_0.manifest["base_generation"] == 0
        assert base_1.manifest["base_generation"] == 1
        assert len({old.bundle_hash, base_0.bundle_hash, base_1.bundle_hash}) == 3
        with pytest.raises(ValueError, match="base_generation"):
            build_delta_bundle(
                conn, db_name="speed_test", base_generation=-1, output_dir=tmp_path / "negative"
            )
    finally:
        conn.close()


def test_player_fantasy_delta_identity_allows_unrostered_null_franchise(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_player_fantasy_with_unrostered_row(conn)

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "bundle",
    )

    player_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "player_fantasy")
    assert player_entry["primary_keys"] == ["db_name", "player_week"]
    assert player_entry["identity_keys"] == ["db_name", "player_week"]
    assert player_entry["fingerprints"]["key_null_counts"] == {"db_name": 0, "player_week": 0}


def test_delta_bundle_repairs_blank_player_week_before_manifest(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            yahoo_player_id VARCHAR,
            "NFL_player_id" VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            fantasy_position VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        VALUES
            ('speed_test', NULL, '77777', NULL, 'Placeholder Player', 'Alice', 'BN', 2025, NULL),
            ('speed_test', '', '40875', '00-0039732', 'Bo Nix', 'Alice', 'QB', 2025, 1)
        """
    )

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="yahoo",
        output_dir=tmp_path / "bundle",
    )

    player_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "player_fantasy")
    repaired_rows = conn.execute(
        "SELECT yahoo_player_id, player_week FROM public.player_fantasy ORDER BY yahoo_player_id"
    ).fetchall()

    assert player_entry["fingerprints"]["key_null_counts"] == {"db_name": 0, "player_week": 0}
    assert repaired_rows[0] == ("40875", "00-0039732_2025_1")
    assert repaired_rows[1][0] == "77777"
    assert repaired_rows[1][1].startswith("UNMAPPED_")
    assert repaired_rows[1][1].endswith("_2025_UNKNOWN_WEEK")


def test_delta_bundle_dedupes_duplicate_player_week_before_manifest(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            "NFL_player_id" VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            is_started INTEGER,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        VALUES
            ('speed_test', '00-0000001_2025_1', '00-0000001', 'Unrostered', NULL, 0, 'BN', 8.0, 2025, 1),
            ('speed_test', '00-0000001_2025_1', '00-0000001', 'Alice', 'alice_0', 1, 'RB', 8.0, 2025, 1)
        """
    )

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "bundle",
    )

    player_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "player_fantasy")
    rows = conn.execute("SELECT player_week, manager FROM public.player_fantasy").fetchall()

    assert player_entry["fingerprints"]["duplicate_primary_keys"] == 0
    assert rows == [("00-0000001_2025_1", "Alice")]


def test_delta_bundle_dedupes_duplicate_matchup_manager_week_before_manifest(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager_week VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            matchup_id INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            win INTEGER,
            loss INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup
        VALUES
            ('speed_test', 2025, 1, 'alice_2025_1', 'Alice', 'alice_fid', 'Aces', NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL),
            ('speed_test', 2025, 1, 'alice_2025_1', 'Alice', 'alice_fid', 'Aces', 'Bob', 'bob_fid', 10, 111.4, 98.2, 0, 1, 0),
            ('speed_test', 2025, 1, 'bob_2025_1', 'Bob', 'bob_fid', 'Bees', 'Alice', 'alice_fid', 10, 98.2, 111.4, 0, 0, 1)
        """
    )

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="yahoo",
        output_dir=tmp_path / "bundle",
    )

    matchup_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "matchup")
    rows = conn.execute(
        """
        SELECT manager_week, opponent, team_points, is_bye_week
        FROM public.matchup
        ORDER BY manager_week
        """
    ).fetchall()

    assert matchup_entry["fingerprints"]["duplicate_primary_keys"] == 0
    assert rows == [
        ("alice_2025_1", "Bob", 111.4, 0),
        ("bob_2025_1", "Alice", 98.2, 0),
    ]


def test_schedule_delta_identity_allows_bye_or_unresolved_opponent(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_schedule_with_null_opponent(conn)

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "bundle",
    )

    schedule_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "schedule")
    assert schedule_entry["primary_keys"] == ["db_name", "manager_week"]
    assert schedule_entry["identity_keys"] == ["db_name", "manager_week"]
    assert schedule_entry["fingerprints"]["key_null_counts"] == {"db_name": 0, "manager_week": 0}


def test_transactions_delta_identity_uses_sequence_for_multi_player_transactions(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_transactions_with_shared_transaction_id(conn)

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "bundle",
    )

    transactions_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "transactions")
    assert transactions_entry["primary_keys"] == ["db_name", "transaction_id", "transaction_sequence"]
    assert transactions_entry["identity_keys"] == ["db_name", "transaction_id", "transaction_sequence"]
    assert transactions_entry["fingerprints"]["duplicate_primary_keys"] == 0


def test_draft_delta_identity_includes_draft_id_for_same_pick_slots(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_draft_with_multiple_draft_ids_same_pick(conn)

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="sleeper",
        output_dir=tmp_path / "bundle",
    )

    draft_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "draft")
    assert draft_entry["primary_keys"] == ["db_name", "year", "draft_id", "round", "pick"]
    assert draft_entry["identity_keys"] == ["db_name", "year", "draft_id", "round", "pick"]
    assert draft_entry["fingerprints"]["duplicate_primary_keys"] == 0


def test_franchise_identity_tables_are_included_in_delta_manifest(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    _seed_franchise_identity_tables(conn)

    bundle = build_delta_bundle(
        conn,
        db_name="speed_test",
        import_mode="quick",
        platform="yahoo",
        output_dir=tmp_path / "bundle",
    )

    registry_entry = next(
        entry for entry in bundle.manifest["tables"] if entry["table"] == "franchise_identity_registry"
    )
    audit_entry = next(entry for entry in bundle.manifest["tables"] if entry["table"] == "franchise_identity_audit")

    assert registry_entry["primary_keys"] == ["db_name", "resolved_franchise_id"]
    assert audit_entry["primary_keys"] == ["db_name", "assignment_key"]
    assert registry_entry["fingerprints"]["duplicate_primary_keys"] == 0
    assert audit_entry["fingerprints"]["duplicate_primary_keys"] == 0


def test_upload_to_fly_delta_uses_manifested_bundle(monkeypatch, tmp_path):
    captured = {}

    class FakeFlyTarget:
        def merge_league_delta(self, db_name, bundle_path, *, bundle_id, bundle_hash):
            captured["db_name"] = db_name
            captured["bundle_exists_during_merge"] = bundle_path.exists()
            captured["bundle_id"] = bundle_id
            captured["bundle_hash"] = bundle_hash
            with tarfile.open(bundle_path, "r:gz") as archive:
                manifest_file = archive.extractfile("manifest.json")
                assert manifest_file is not None
                captured["manifest"] = json.loads(manifest_file.read().decode("utf-8"))
            return {"status": "COMMITTED", "tables": {"matchup": 2}}

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = (args, kwargs)
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    _seed_minimal_league(conn)

    db.upload_to_fly(
        "speed_test",
        import_mode="quick",
        platform="sleeper",
        finalize_inventory=True,
        finalize_merge_source=False,
    )
    db.close()

    manifest_copy = tmp_path / "delta_publish_manifest.json"
    copied_manifest = json.loads(manifest_copy.read_text(encoding="utf-8"))

    assert captured["db_name"] == "speed_test"
    assert captured["bundle_exists_during_merge"] is True
    assert captured["bundle_id"] == captured["manifest"]["bundle_id"]
    assert captured["bundle_hash"] == captured["manifest"]["bundle_hash"]
    assert copied_manifest["bundle_hash"] == captured["bundle_hash"]
    assert [entry["table"] for entry in copied_manifest["tables"]] == ["matchup"]
    assert captured["marked"][1] == {"import_mode": "quick", "platform": "sleeper"}


def test_upload_to_fly_delta_skips_inventory_finalization_by_default(monkeypatch, tmp_path):
    captured = {"marked": False}

    class FakeFlyTarget:
        def merge_league_delta(self, db_name, bundle_path, *, bundle_id, bundle_hash):
            return {"status": "COMMITTED", "tables": {"matchup": 2}, "lock_wait_seconds": 0.0, "merge_seconds": 0.1}

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = True
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    _seed_minimal_league(conn)

    db.upload_to_fly("speed_test", import_mode="quick", platform="sleeper", finalize_merge_source=False)
    db.close()

    assert captured["marked"] is False


def test_upload_to_fly_delta_stale_bundle_skips_finalization(monkeypatch, tmp_path, capsys):
    captured = {"marked": False}

    class FakeFlyTarget:
        def merge_league_delta(self, db_name, bundle_path, *, bundle_id, bundle_hash):
            return {
                "status": "STALE_SKIPPED",
                "detail": "Older bundle cannot commit over newer committed state",
            }

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = True
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    _seed_minimal_league(conn)

    db.upload_to_fly(
        "speed_test",
        import_mode="quick",
        platform="sleeper",
        finalize_inventory=True,
        finalize_merge_source=False,
    )
    db.close()

    assert captured["marked"] is False
    assert "merge-league-delta skipped" in capsys.readouterr().out


def test_upload_to_fly_delta_can_skip_inventory_finalization_by_env(monkeypatch, tmp_path):
    captured = {"marked": False}

    class FakeFlyTarget:
        def merge_league_delta(self, db_name, bundle_path, *, bundle_id, bundle_hash):
            return {"status": "COMMITTED", "tables": {"matchup": 2}, "lock_wait_seconds": 0.0, "merge_seconds": 0.1}

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = True
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    _seed_minimal_league(conn)

    db.upload_to_fly("speed_test", import_mode="quick", platform="sleeper", finalize_merge_source=False)
    db.close()

    assert captured["marked"] is False
