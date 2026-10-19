from pathlib import Path
import json
import pytest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import duckdb
import pandas as pd

import multi_league.core.targets.fly_target as fly_target
from multi_league.core.local_db import LocalLeagueDB, _registerable_frame


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_confirmed_publication_survives_inventory_failure_without_another_upload(monkeypatch, tmp_path, platform, mode):
    class Target:
        def merge_fleet_partition(self, *args, **kwargs): return {"status": "COMMITTED"}
        def merge_league_delta(self, *args, **kwargs): return {"status": "COMMITTED"}
        def merge_league(self, *args, **kwargs): pytest.fail("committed data must not be uploaded again")
        def mark_league_imported(self, *args, **kwargs): raise ConnectionError("temporarily unavailable")
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_DELTA_FALLBACK_TO_DUCKDB", "1")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "2")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "1")
    with LocalLeagueDB(tmp_path, "league") as db:
        db.connect().execute("CREATE TABLE public.matchup (db_name VARCHAR,year INTEGER,week INTEGER,manager_week VARCHAR)")
        db.connect().execute("INSERT INTO public.matchup VALUES ('league',2026,1,'owner_2026_1')")
        with pytest.raises(RuntimeError, match="committed.*finalization pending"):
            db.upload_to_fly("league", import_mode=mode, platform=platform, finalize_merge_source=False)
    saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    assert saved["publication"]["finalization_status"] == "pending"
    assert saved["publication"]["bundle_id"] == saved["bundle_id"]


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_failed_merge_never_records_a_commit(monkeypatch, tmp_path, platform):
    class Target:
        def merge_fleet_partition(self, *args, **kwargs): raise TimeoutError("no confirmed response")
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "2")
    with LocalLeagueDB(tmp_path, "league") as db:
        db.connect().execute("CREATE TABLE public.matchup (db_name VARCHAR,year INTEGER,week INTEGER,manager_week VARCHAR)")
        db.connect().execute("INSERT INTO public.matchup VALUES ('league',2026,1,'owner_2026_1')")
        with pytest.raises(TimeoutError):
            db.upload_to_fly("league", import_mode="quick", platform=platform, finalize_merge_source=False)
    saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
    assert "publication" not in saved


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_action_output_failure_does_not_prevent_commit_receipt_or_finalization(monkeypatch, tmp_path, platform):
    marked = []
    class Target:
        def merge_league(self, *args, **kwargs): return {"status": "merged"}
        def mark_league_imported(self, name, **kwargs): marked.append((name, kwargs["platform"]))
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "duckdb")
    output_directory = tmp_path / "unwritable_output_file"
    output_directory.mkdir()
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_directory))
    with LocalLeagueDB(tmp_path, "league") as db:
        db.connect().execute("CREATE TABLE public.matchup (year INTEGER,week INTEGER)")
        db.connect().execute("INSERT INTO public.matchup VALUES (2026,1)")
        db.upload_to_fly("league", import_mode="full", platform=platform, finalize_merge_source=False)
    saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    assert saved["publication"]["finalization_status"] == "complete"
    assert marked == [("league", platform)]


def test_precommit_legacy_fallback_receipt_records_actual_transport(monkeypatch, tmp_path):
    class Target:
        def merge_league_delta(self, *args, **kwargs): raise ConnectionError("connection refused")
        def merge_league(self, *args, **kwargs): return {"status": "merged"}
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_DELTA_FALLBACK_TO_DUCKDB", "1")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "2")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")
    with LocalLeagueDB(tmp_path, "league") as db:
        db.connect().execute("CREATE TABLE public.matchup (db_name VARCHAR,year INTEGER,week INTEGER,manager_week VARCHAR)")
        db.connect().execute("INSERT INTO public.matchup VALUES ('league',2026,1,'owner_2026_1')")
        db.upload_to_fly("league", import_mode="full", platform="yahoo", finalize_merge_source=False)
    assert json.loads((tmp_path / "delta_publish_manifest.json").read_text())["publication"]["transport"] == "duckdb"


def test_registerable_frame_json_encodes_nested_pandas_values_without_numpy_isnan():
    """ESPN roster payloads can retain nested values in object columns."""
    frame = pd.DataFrame({"payload": [{"id": 1}, ["BN", "IR"], None]})

    registered = _registerable_frame(frame)

    assert registered["payload"].tolist() == ['{"id": 1}', '["BN", "IR"]', None]


def test_upload_to_fly_stages_only_canonical_tables(monkeypatch, tmp_path):
    captured = {}

    class FakeFlyTarget:
        def merge_league(self, db_name, local_path):
            captured["db_name"] = db_name
            captured["exists_during_merge"] = Path(local_path).exists()

            conn = duckdb.connect(str(local_path), read_only=True)
            try:
                captured["tables"] = conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                    """
                ).fetchall()
                captured["rows"] = conn.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
                captured["managers"] = conn.execute("SELECT manager FROM public.matchup ORDER BY week").fetchall()
            finally:
                conn.close()

            return {"status": "merged", "tables": {"matchup": captured["rows"]}}

        def mark_league_imported(self, *args, **kwargs):
            captured["marked"] = (args, kwargs)
            return []

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)

    db = LocalLeagueDB(tmp_path, "speed_test")
    conn = db.connect()
    conn.execute("CREATE TABLE public.matchup (year INTEGER, week INTEGER, manager VARCHAR)")
    conn.execute("INSERT INTO public.matchup VALUES (2026, 1, 'Alice'), (2026, 2, 'Bob')")
    conn.execute("CREATE TABLE public.scratch_table (value INTEGER)")
    conn.execute("INSERT INTO public.scratch_table VALUES (99)")

    db.upload_to_fly("speed_test", import_mode="full", platform="sleeper", finalize_merge_source=False)
    db.close()

    assert captured["db_name"] == "speed_test"
    assert captured["exists_during_merge"] is True
    assert captured["tables"] == [("matchup",)]
    assert captured["rows"] == 2
    assert captured["managers"] == [("Alice",), ("Bob",)]
    assert captured["marked"][1] == {"import_mode": "full", "platform": "sleeper"}


def test_delta_upload_uses_generation_captured_before_import(monkeypatch, tmp_path):
    captured = {}

    class FakeFlyTarget:
        def merge_league_delta(self, db_name, path, *, bundle_id, bundle_hash):
            captured["db_name"] = db_name
            captured["bundle_id"] = bundle_id
            captured["bundle_hash"] = bundle_hash
            return {"status": "COMMITTED"}

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "2")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")
    db = LocalLeagueDB(tmp_path, "speed_test")
    try:
        conn = db.connect()
        conn.execute(
            "CREATE TABLE public.matchup "
            "(db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR, manager VARCHAR)"
        )
        conn.execute(
            "INSERT INTO public.matchup VALUES "
            "('speed_test', 2026, 1, 'alice_2026_1', 'Alice')"
        )
        db.upload_to_fly(
            "speed_test", import_mode="full", platform="sleeper", finalize_merge_source=False
        )
        manifest = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
        assert manifest["base_generation"] == 2
        assert captured["db_name"] == "speed_test"
        assert captured["bundle_hash"] == manifest["bundle_hash"]
    finally:
        db.close()


def test_delta_upload_refuses_missing_required_snapshot(monkeypatch, tmp_path):
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("REQUIRE_IMPORT_BASE_GENERATION", "1")
    monkeypatch.delenv("LEAGUE_IMPORT_BASE_GENERATION", raising=False)
    db = LocalLeagueDB(tmp_path, "speed_test")
    try:
        db.connect().execute(
            "CREATE TABLE public.matchup "
            "(db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)"
        )
        with pytest.raises(RuntimeError, match="unfenced league write"):
            db.upload_to_fly("speed_test", import_mode="full", platform="sleeper")
    finally:
        db.close()


def test_same_name_legacy_uploads_own_distinct_staging_files(monkeypatch, tmp_path):
    staged = []
    arrived = Barrier(2, timeout=5)

    class FakeFlyTarget:
        def merge_league(self, db_name, local_path):
            path = Path(local_path)
            assert path.exists()
            staged.append(path)
            arrived.wait()
            assert path.exists()
            return {"status": "merged", "tables": {"matchup": 1}}

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "duckdb")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")

    def upload(label):
        directory = tmp_path / label
        directory.mkdir()
        db = LocalLeagueDB(directory, "speed_test")
        try:
            conn = db.connect()
            conn.execute("CREATE TABLE public.matchup (year INTEGER, week INTEGER, manager VARCHAR)")
            conn.execute("INSERT INTO public.matchup VALUES (2026, 1, 'Alice')")
            db.upload_to_fly(
                "speed_test", import_mode="full", platform="sleeper", finalize_merge_source=False
            )
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(upload, label) for label in ("first", "second")]
        for future in futures:
            future.result(timeout=15)
    assert len(staged) == 2
    assert staged[0] != staged[1]
