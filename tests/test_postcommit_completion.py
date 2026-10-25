"""Regression coverage for real publication versus post-commit diagnostics."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def no_production_credentials(monkeypatch):
    for name in ("DATABASE_SERVER_URL", "DATABASE_READ_TOKEN", "DATABASE_ADMIN_TOKEN", "GITHUB_ACTIONS"):
        monkeypatch.delenv(name, raising=False)


def load_script(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def committed_manifest(tmp_path, platform="yahoo"):
    manifest = {
        "db_name": "target", "platform": platform, "import_mode": "full",
        "bundle_id": "target-777-1-hash", "bundle_hash": "hash", "base_generation": 4,
        "import_run_id": "777", "tables": [],
        "publication": {"status": "COMMITTED", "executed": True,
                        "bundle_id": "target-777-1-hash", "finalization_status": "complete"},
    }
    path = tmp_path / "delta_publish_manifest.json"
    path.write_text(json.dumps(manifest))
    return path


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("failure", ["visibility", "revalidate", "unexpected"])
def test_committed_cache_outage_is_pending_not_failed(monkeypatch, tmp_path, platform, failure):
    warmer = load_script("warm_postcommit", "scripts/warm_vercel_cache.py")
    manifest = committed_manifest(tmp_path, platform)
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr(sys, "argv", ["warmer", "--db", "target", "--manifest", str(manifest),
        "--secret", "test-secret", "--strict", "--soft-warm-failures", "--verify-hot"])
    def unavailable(*_args, **_kwargs):
        raise TimeoutError("temporary checkpoint or connection loss")
    monkeypatch.setattr(warmer, "verify_fly_commit_visible", unavailable if failure == "visibility" else lambda *_args: None)
    monkeypatch.setattr(warmer, "revalidate", unavailable if failure == "revalidate" else lambda *_args: None)
    if failure == "unexpected":
        monkeypatch.setattr(warmer, "_run_warmer", unavailable)
    assert warmer.main() == 0
    saved = json.loads(manifest.read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    assert saved["cache_verification"]["verified"] is False
    assert "completion_status=committed_cache_pending" in output.read_text()


def test_retry_skips_cache_that_is_already_verified(monkeypatch, tmp_path):
    warmer = load_script("warm_verified_retry", "scripts/warm_vercel_cache.py")
    manifest = committed_manifest(tmp_path)
    saved = json.loads(manifest.read_text())
    saved["cache_verification"] = {"verified": True, "status": "verified"}
    manifest.write_text(json.dumps(saved))
    monkeypatch.setattr(sys, "argv", ["warmer", "--db", "target", "--manifest", str(manifest),
        "--secret", "test-secret", "--strict", "--soft-warm-failures", "--verify-hot"])
    monkeypatch.setattr(warmer, "verify_fly_commit_visible", lambda *_args: None)
    def repeated(*_args):
        raise AssertionError("completed cache work must not repeat")
    monkeypatch.setattr(warmer, "revalidate", repeated)
    assert warmer.main() == 0
    assert json.loads(manifest.read_text())["cache_verification"]["verified"] is True


def test_multiplatform_validation_outage_does_not_skip_cache(monkeypatch, tmp_path):
    runner = load_script("multi_postcommit", ".github/scripts/run_multi_platform_import.py")
    from scripts import warm_vercel_cache as warmer
    from multi_league.validation_v2 import validate as _validator_module
    import subprocess
    manifest = committed_manifest(tmp_path, "sleeper")
    calls = []
    monkeypatch.setattr(runner, "run_local_sql_enrichments_after_merge", lambda *_args: None)
    monkeypatch.setattr(runner, "local_matchup_count", lambda *_args: 0)
    monkeypatch.setattr(runner, "upload_local_db", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("REVALIDATION_SECRET", "test-secret")
    monkeypatch.setattr(warmer, "verify_fly_commit_visible", lambda *_args: None)
    monkeypatch.setattr(warmer, "revalidate", lambda *_args: None)
    monkeypatch.setattr(warmer, "iter_warm_targets", lambda *_args: [])
    def validator(*_args, **_kwargs):
        calls.append("VALIDATE MERGED TARGET")
        raise subprocess.TimeoutExpired("validator", 40)
    monkeypatch.setattr(warmer.subprocess, "run", validator)
    def run(name, command, **_kwargs):
        calls.append(name)
        if name == "VALIDATE MERGED TARGET":
            raise SystemExit("temporary validation timeout")
        if name == "REVALIDATE AND WARM VERCEL CACHE":
            monkeypatch.setattr(sys, "argv", command[1:])
            assert warmer.main() == 0
    monkeypatch.setattr(runner, "run_logged", run)
    runner.postprocess_and_upload_target({"database_name": "target", "platform": "sleeper"}, tmp_path)
    assert "REVALIDATE AND WARM VERCEL CACHE" in calls
    assert calls.index("REVALIDATE AND WARM VERCEL CACHE") < calls.index("VALIDATE MERGED TARGET")
    saved = json.loads(manifest.read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    assert saved["validation"]["status"] == "pending"


def test_remote_recovery_uses_exact_receipt_and_keeps_completed_cache(monkeypatch, tmp_path):
    warmer = load_script("warm_remote_resume", "scripts/warm_vercel_cache.py")
    manifest_path = committed_manifest(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("publication")
    result = {"status": "COMMITTED", "bundle_id": manifest["bundle_id"], "bundle_hash": "hash",
              "import_completion": {
                  "publication": {"status": "COMMITTED", "executed": True,
                      "bundle_id": manifest["bundle_id"], "finalization_status": "complete"},
                  "cache_verification": {"verified": True, "status": "verified"},
                  "validation": {"status": "pending"},
              }}
    queries = []
    def query(sql, *_args, **_kwargs):
        queries.append(sql)
        if "league_publish_generations" in sql:
            return [{"generation": 5, "run_id": "777"}]
        return [{"status": "COMMITTED", "db_name": "target", "bundle_id": manifest["bundle_id"],
                 "bundle_hash": "hash", "import_run_id": "777",
                 "manifest_json": json.dumps(manifest), "result_json": json.dumps(result)}]
    monkeypatch.setattr(warmer, "_fly_query", query)
    recovered = warmer.load_committed_publication("target", "777", 5)
    assert recovered["cache_verification"]["verified"] is True
    assert recovered["validation"]["status"] == "pending"
    assert recovered["publication"]["status"] == "COMMITTED"
    assert "import_run_id = '777'" in queries[0]
    assert "db_name = 'target'" in queries[0]


@pytest.mark.parametrize("outcome,want", [
    ("timeout", "pending"), ("data_error", "failed"), ("transient_report", "pending"), ("passed", "passed"),
])
def test_validation_distinguishes_data_errors_from_unavailable_checks(monkeypatch, tmp_path, outcome, want):
    import subprocess
    warmer = load_script("warm_validation_states", "scripts/warm_vercel_cache.py")
    manifest = committed_manifest(tmp_path)
    def run(*_args, **_kwargs):
        if outcome == "timeout":
            raise subprocess.TimeoutExpired("validator", 40)
        if outcome == "data_error":
            return subprocess.CompletedProcess([], 1, stdout="CI FAILURE: 1 check(s) failed", stderr="")
        if outcome == "transient_report":
            return subprocess.CompletedProcess([], 0, stdout="Checks: 1 visible (0 passed, 0 failed, 1 errored, 1 transient)", stderr="")
        return subprocess.CompletedProcess([], 0, stdout="Checks: 2 visible (2 passed, 0 failed, 0 errored, 0 transient)", stderr="")
    monkeypatch.setattr(warmer.subprocess, "run", run)
    warmer.validate_publication(str(manifest), "target")
    assert json.loads(manifest.read_text())["validation"]["status"] == want


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_finalization_retry_never_reuploads_or_changes_history(monkeypatch, tmp_path, platform, mode):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets import fly_target
    from multi_league.core.readers import fly_reader
    published = []
    marks = []
    class Target:
        def merge_league_delta(self, _db, _path, **kwargs):
            published.append(kwargs)
            return {"status": "COMMITTED", **kwargs}
        def merge_fleet_partition(self, _path, **kwargs):
            published.append(kwargs)
            return {"status": "COMMITTED", "bundle_id": "fleet-canonical", "bundle_hash": "canonical-hash"}
        def mark_league_imported(self, *_args, **_kwargs):
            marks.append(1)
            if len(marks) == 1:
                raise TimeoutError("inventory temporarily unavailable")
        def _reconcile_committed_merge(self, state_db, bundle_id, bundle_hash):
            assert state_db == ("___fleet" if mode == "quick" else "target")
            assert bundle_id == ("fleet-canonical" if mode == "quick" else published[0]["bundle_id"])
            assert bundle_hash == ("canonical-hash" if mode == "quick" else published[0]["bundle_hash"])
            return {"status": "COMMITTED", **published[0]}
    class Reader:
        def query_scalar(self, *_args, **_kwargs):
            return 5
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setattr(fly_reader, "FlyReader", Reader)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "1")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "4")
    preferences = tmp_path / "franchise_config.json"
    preferences.write_text('{"manager_name_overrides":{"Old name":"Saved alias"}}')
    saved_preferences = preferences.read_bytes()
    with LocalLeagueDB(tmp_path, "target") as db:
        conn = db.connect()
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR, manager VARCHAR)")
        conn.execute("INSERT INTO public.matchup VALUES ('target',2014,1,'alias_2014_1','Saved alias'),('target',2026,6,'alias_2026_6','Saved alias')")
        before = conn.execute("SELECT year,manager FROM public.matchup ORDER BY year").fetchall()
        db.upload_to_fly("target", import_mode=mode, platform=platform, finalize_merge_source=False, validate_after_publication=True)
        manifest = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
        assert manifest["publication"]["status"] == "COMMITTED"
        assert manifest["publication"]["finalization_status"] == "pending"
        assert manifest["validation"]["status"] == "pending"
        def must_not_connect():
            raise AssertionError("finalization recovery must not open the league database")
        monkeypatch.setattr(db, "connect", must_not_connect)
        db.upload_to_fly("target", import_mode=mode, platform=platform, finalize_merge_source=False, finalize_only=True)
        assert len(published) == 1
        assert len(marks) == 2
        assert conn.execute("SELECT year,manager FROM public.matchup ORDER BY year").fetchall() == before
        assert preferences.read_bytes() == saved_preferences
    assert json.loads((tmp_path / "delta_publish_manifest.json").read_text())["publication"]["finalization_status"] == "complete"


@pytest.mark.parametrize("lane", ["delta", "fleet"])
def test_completion_checkpoint_changes_only_its_fenced_receipt(monkeypatch, tmp_path, lane):
    import duckdb
    warmer = load_script("warm_checkpoint", "scripts/warm_vercel_cache.py")
    manifest_path = committed_manifest(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    state_db = "___fleet" if lane == "fleet" else "target"
    durable_id = "fleet-canonical" if lane == "fleet" else manifest["bundle_id"]
    manifest["publication"].update(state_db_name=state_db, durable_bundle_id=durable_id, durable_bundle_hash="hash")
    manifest["cache_verification"] = {"verified": True, "status": "verified"}
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA merge_admin")
        conn.execute("CREATE TABLE merge_admin.league_publish_generations (db_name VARCHAR, generation INTEGER)")
        conn.execute("INSERT INTO merge_admin.league_publish_generations VALUES ('target',5)")
        conn.execute("CREATE TABLE merge_admin.league_delta_merge_state (db_name VARCHAR,bundle_id VARCHAR,bundle_hash VARCHAR,status VARCHAR,result_json VARCHAR)")
        conn.execute("INSERT INTO merge_admin.league_delta_merge_state VALUES (?,?,?,'COMMITTED',?)", [state_db,durable_id,"hash",'{"status":"COMMITTED","kept":"original"}'])
        conn.execute("INSERT INTO merge_admin.league_delta_merge_state VALUES ('unrelated','other','other','COMMITTED','{}')")
        def query(sql, *_args, **kwargs):
            assert kwargs.get("write") is True
            cursor = conn.execute(sql)
            return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
        monkeypatch.setattr(warmer, "_fly_query", query)
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "fake")
        warmer._persist_completion(manifest, "target")
        result = json.loads(conn.execute("SELECT result_json FROM merge_admin.league_delta_merge_state WHERE bundle_id=?", [durable_id]).fetchone()[0])
        assert result["kept"] == "original"
        assert result["status"] == "COMMITTED"
        assert result["import_completion"]["cache_verification"]["verified"] is True
        assert conn.execute("SELECT result_json FROM merge_admin.league_delta_merge_state WHERE db_name='unrelated'").fetchone()[0] == "{}"
        stale = json.loads(json.dumps(manifest))
        stale["cache_verification"] = {"verified": False, "status": "pending"}
        stale["publication"]["finalization_status"] = "pending"
        with pytest.raises(RuntimeError, match="fence"):
            warmer._persist_completion(stale, "target")
        unchanged = json.loads(conn.execute("SELECT result_json FROM merge_admin.league_delta_merge_state WHERE bundle_id=?", [durable_id]).fetchone()[0])
        assert unchanged["import_completion"]["cache_verification"]["verified"] is True
        # A new, explicitly changed-data revision invalidates the old cache;
        # an older runner cannot put its completion bits back afterward.
        stale["publication"]["completion_revision"] = 1
        warmer._persist_completion(stale, "target")
        with pytest.raises(RuntimeError, match="fence"):
            warmer._persist_completion(manifest, "target")
        conn.execute("UPDATE merge_admin.league_publish_generations SET generation=6")
        with pytest.raises(RuntimeError, match="fence"):
            warmer._persist_completion(manifest, "target")
    finally:
        conn.close()


@pytest.mark.parametrize("completed", ["cache", "validation"])
def test_recovery_runs_only_the_unfinished_postcommit_phase(monkeypatch, tmp_path, completed):
    warmer = load_script("warm_phase_resume", "scripts/warm_vercel_cache.py")
    manifest = committed_manifest(tmp_path)
    saved = json.loads(manifest.read_text())
    saved["cache_verification"] = {"verified": completed == "cache"}
    saved["validation"] = {"status": "passed" if completed == "validation" else "pending"}
    manifest.write_text(json.dumps(saved))
    calls = []
    monkeypatch.setattr(sys, "argv", ["warmer", "--db", "target", "--manifest", str(manifest),
        "--strict", "--soft-warm-failures", "--verify-hot", "--validate"])
    def cache(_args):
        assert completed != "cache", "completed cache work must not repeat"
        calls.append("cache")
        return 0, True
    def validation(*_args, **_kwargs):
        import subprocess
        assert completed != "validation", "completed verification must not repeat"
        calls.append("validation")
        return subprocess.CompletedProcess([], 0, stdout="Checks: 2 visible (2 passed, 0 failed, 0 errored, 0 transient)", stderr="")
    monkeypatch.setattr(warmer, "_run_warmer", cache)
    monkeypatch.setattr(warmer.subprocess, "run", validation)
    assert warmer.main() == 0
    assert calls == (["validation"] if completed == "cache" else ["cache"])
    saved = json.loads(manifest.read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    assert saved["cache_verification"]["verified"] is True
    assert saved["validation"]["status"] == "passed"


@pytest.mark.parametrize("damage", ["hash", "run", "league", "generation"])
def test_remote_resume_rejects_mismatched_or_superseded_publication(monkeypatch, tmp_path, damage):
    warmer = load_script("warm_scope_fence", "scripts/warm_vercel_cache.py")
    manifest = json.loads(committed_manifest(tmp_path).read_text())
    result = {"status": "COMMITTED", "bundle_id": manifest["bundle_id"], "bundle_hash": "hash"}
    if damage == "hash":
        result["bundle_hash"] = "other"
    elif damage == "run":
        manifest["import_run_id"] = "888"
    elif damage == "league":
        manifest["db_name"] = "unrelated"
    monkeypatch.setattr(warmer, "_fly_query", lambda *_args, **_kwargs: [{
        "status": "COMMITTED", "db_name": "target", "bundle_id": result["bundle_id"],
        "bundle_hash": "hash", "import_run_id": "777", "manifest_json": json.dumps(manifest),
        "result_json": json.dumps(result),
    }])
    with pytest.raises(RuntimeError):
        warmer.load_committed_publication("target", "777", 6 if damage == "generation" else 5)


def test_resume_accepts_canonical_single_league_fleet_manifest(monkeypatch, tmp_path):
    warmer = load_script("warm_canonical_fleet", "scripts/warm_vercel_cache.py")
    manifest = json.loads(committed_manifest(tmp_path).read_text())
    manifest.update(db_name="___fleet", db_names=["target"], mode="quick", league_generations={"target": 4})
    manifest.pop("base_generation")
    manifest.pop("import_mode")
    manifest.pop("publication")
    result = {"status": "COMMITTED", "bundle_id": manifest["bundle_id"], "bundle_hash": "hash"}
    monkeypatch.setattr(warmer, "_fly_query", lambda *_args, **_kwargs: [{
        "status": "COMMITTED", "db_name": "___fleet", "bundle_id": result["bundle_id"],
        "bundle_hash": "hash", "import_run_id": "777", "manifest_json": json.dumps(manifest),
        "result_json": json.dumps(result),
    }])
    recovered = warmer.load_committed_publication("target", "777", 5, mode="quick")
    assert recovered["publication"]["state_db_name"] == "___fleet"
    assert recovered["import_mode"] == "quick"
    assert recovered["publication"]["finalization_status"] == "pending"


def test_reporting_failure_never_changes_a_confirmed_multi_publication(monkeypatch, tmp_path):
    runner = load_script("multi_reporting_outage", ".github/scripts/run_multi_platform_import.py")
    from scripts import warm_vercel_cache as warmer
    manifest = committed_manifest(tmp_path)
    def unavailable(*_args, **_kwargs):
        raise OSError("reporting storage unavailable")
    monkeypatch.setattr(runner, "run_logged", unavailable)
    monkeypatch.setattr(warmer, "_record_cache_result", unavailable)
    runner.finalize_published_target({"database_name": "target", "platform": "yahoo"}, tmp_path)
    assert json.loads(manifest.read_text())["publication"]["status"] == "COMMITTED"


def test_cleanup_retry_keeps_completed_inventory_and_merge_checkpoints(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets import fly_target
    from multi_league.core.readers import fly_reader
    from multi_league.data_fetchers.shared import merge_source_copier, staging_reader
    from scripts import warm_vercel_cache as warmer
    calls = []
    checkpoints = []
    class Target:
        def merge_league_delta(self, _db, _path, **kwargs):
            calls.append("upload")
            return {"status": "COMMITTED", **kwargs}
        def mark_league_imported(self, *_args, **_kwargs):
            calls.append("inventory")
        def _reconcile_committed_merge(self, *_args):
            return {"status": "COMMITTED"}
    class Reader:
        def query_scalar(self, *_args, **_kwargs):
            return 5
    def merge(*_args, **_kwargs):
        calls.append("merge")
        return {"status": "copied", "single_year_import": "false"}
    def cleanup(*_args, **_kwargs):
        calls.append("cleanup")
        if calls.count("cleanup") == 1:
            raise TimeoutError("cleanup response lost")
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setattr(fly_reader, "FlyReader", Reader)
    monkeypatch.setattr(merge_source_copier, "copy_merge_source_to_public", merge)
    monkeypatch.setattr(staging_reader, "clear_staging_tables", cleanup)
    monkeypatch.setattr(warmer, "_persist_completion", lambda manifest, _db: checkpoints.append(json.loads(json.dumps(manifest))))
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "1")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "4")
    ctx = {"import_mode": "full", "oauth_token": "MUST_NOT_PERSIST", "merge_sources": [{
        "source_db": "older", "merge_years": [2014], "manager_mapping": {"Old": "Saved alias"},
        "cookie": "MUST_NOT_PERSIST",
    }]}
    with LocalLeagueDB(tmp_path, "target") as db:
        db.connect().execute("CREATE TABLE public.matchup (db_name VARCHAR,year INTEGER,week INTEGER,manager VARCHAR)")
        db.connect().execute("INSERT INTO public.matchup VALUES ('target',2026,6,'Saved alias')")
        db.upload_to_fly("target", import_mode="full", platform="yahoo", merge_source_ctx=ctx)
        saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
        assert saved["publication"]["finalization_steps"] == {"inventory": "complete", "merge_source": "complete"}
        assert "MUST_NOT_PERSIST" not in json.dumps(saved)
        assert checkpoints, "completed phases must survive losing the runner, not just remain local"
        assert checkpoints[-1]["publication"]["finalization_steps"] == saved["publication"]["finalization_steps"]
        db.upload_to_fly("target", finalize_only=True)
    assert calls == ["upload", "inventory", "merge", "cleanup", "cleanup"]
    saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
    assert saved["publication"]["finalization_status"] == "complete"


def test_fresh_runner_uses_the_exact_recovered_receipt_for_finalization(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    warmer = load_script("warm_fresh_runner", "scripts/warm_vercel_cache.py")
    manifest = json.loads(committed_manifest(tmp_path).read_text())
    manifest["publication"]["finalization_status"] = "pending"
    manifest["cache_verification"] = {"verified": True}
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "fresh"))
    monkeypatch.setattr(warmer, "load_committed_publication", lambda *_args, **_kwargs: manifest)
    calls = []
    def finalize(db, _name, **kwargs):
        assert kwargs["finalize_only"] is True
        path = db.data_dir / "delta_publish_manifest.json"
        saved = json.loads(path.read_text())
        saved["publication"]["finalization_status"] = "complete"
        path.write_text(json.dumps(saved))
        calls.append("finalization")
    monkeypatch.setattr(LocalLeagueDB, "upload_to_fly", finalize)
    monkeypatch.setattr(sys, "argv", ["warmer", "--db", "target", "--publication-run-id", "777", "--soft-warm-failures"])
    assert warmer.main() == 0
    assert calls == ["finalization"]


def test_resumed_merge_invalidates_cache_and_keeps_required_qa(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    warmer = load_script("warm_changed_after_cache", "scripts/warm_vercel_cache.py")
    path = committed_manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["publication"].update(finalization_status="pending", completion_revision=0)
    manifest["cache_verification"] = {"verified": True}
    manifest["validation"] = {"status": "pending"}
    path.write_text(json.dumps(manifest))
    calls = []
    def finalize(*_args, **_kwargs):
        saved = json.loads(path.read_text())
        saved["publication"].update(finalization_status="complete", completion_revision=1)
        saved["cache_verification"] = {"verified": False}
        path.write_text(json.dumps(saved))
    monkeypatch.setattr(LocalLeagueDB, "upload_to_fly", finalize)
    monkeypatch.setattr(warmer, "_run_warmer", lambda _args: (calls.append("cache") or 0, True))
    monkeypatch.setattr(warmer, "validate_publication", lambda *_args: calls.append("qa"))
    monkeypatch.setattr(sys, "argv", ["warmer", "--db", "target", "--manifest", str(path), "--soft-warm-failures"])
    assert warmer.main() == 0
    assert calls == ["cache", "qa"]


def test_visibility_accepts_growth_from_the_saved_merge_plan(monkeypatch, tmp_path):
    warmer = load_script("warm_growth_receipt", "scripts/warm_vercel_cache.py")
    path = committed_manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["publication"]["finalization_plan"] = {"merge_source": True,
        "context": {"merge_sources": [{"source_db": "older", "merge_years": [2014]}]}}
    manifest["tables"] = [{"table": "matchup", "row_count": 10}]
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(warmer, "_fly_query", lambda sql, *_args, **_kwargs:
        [{"status": "COMMITTED"}] if "league_delta_merge_state" in sql else [{"cnt": 20}])
    warmer.verify_fly_commit_visible(str(path), "target", 1)


def test_partial_historical_copy_retries_the_frozen_years_and_invalidates_cache(monkeypatch, tmp_path):
    import duckdb
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets import fly_target
    from multi_league.core.readers import fly_reader
    from multi_league.data_fetchers.shared import merge_source_copier as copier
    remote = duckdb.connect(":memory:")
    remote.execute("ATTACH ':memory:' AS ___leagues")
    remote.execute("CREATE SCHEMA ___leagues.public")
    for table in ("matchup", "transactions"):
        remote.execute(f"CREATE TABLE ___leagues.public.{table} (db_name VARCHAR,year INTEGER,manager VARCHAR)")
        remote.execute(f"INSERT INTO ___leagues.public.{table} VALUES ('older',2014,'Saved alias'),('target',2026,'Saved alias')")
    calls = []
    class Reader:
        def query(self, sql, **_kwargs):
            cursor = remote.execute(sql)
            return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
        def query_scalar(self, sql, **_kwargs):
            if "league_publish_generations" in sql:
                return 5
            return remote.execute(sql).fetchone()[0]
    class Writer:
        def execute(self, sql, **_kwargs):
            if "DELETE FROM ___leagues.public.transactions" in sql:
                calls.append("transactions")
                if calls.count("transactions") == 1:
                    raise TimeoutError("one narrow table copy response failed")
            remote.execute(sql)
    class Target:
        def merge_league_delta(self, _db, _path, **kwargs):
            calls.append("upload")
            return {"status": "COMMITTED", **kwargs}
        def _reconcile_committed_merge(self, *_args):
            return {"status": "COMMITTED"}
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setattr(fly_reader, "FlyReader", Reader)
    monkeypatch.setattr(copier, "_query_year_scoped_tables", lambda _reader: ["matchup", "transactions"])
    monkeypatch.setattr(copier, "_query_columns", lambda _reader, _table: ["db_name", "year", "manager"])
    monkeypatch.setattr(copier, "_remap_copied_franchise_ids", lambda **_kwargs: None)
    monkeypatch.setattr(copier, "_repair_copied_trade_mirrors", lambda **_kwargs: {})
    monkeypatch.setattr(copier, "_refresh_merge_source_aggregates", lambda **_kwargs: {})
    actual_copy = copier.copy_merge_source_to_public
    monkeypatch.setattr(copier, "copy_merge_source_to_public", lambda context, name, **kwargs:
        actual_copy(context, name, reader=Reader(), writer=Writer(), **kwargs))
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "4")
    # Avoid unrelated staging cleanup; the copier still executes real per-table SQL.
    context = {"import_mode": "quick", "merge_sources": [{"source_db": "older"}]}
    try:
        with LocalLeagueDB(tmp_path, "target") as db:
            db.connect().execute("CREATE TABLE public.matchup (db_name VARCHAR,year INTEGER,week INTEGER,manager VARCHAR)")
            db.connect().execute("INSERT INTO public.matchup VALUES ('target',2026,6,'Saved alias')")
            db.upload_to_fly("target", import_mode="full", platform="yahoo", merge_source_ctx=context)
            path = tmp_path / "delta_publish_manifest.json"
            saved = json.loads(path.read_text())
            assert saved["publication"]["finalization_status"] == "pending"
            assert saved["publication"]["finalization_plan"]["context"]["merge_sources"][0]["merge_years"] == [2014]
            # Cache happened to verify while historical completion was pending.
            saved["cache_verification"] = {"verified": True}
            saved["validation"] = {"status": "passed"}
            path.write_text(json.dumps(saved))
            db.upload_to_fly("target", finalize_only=True)
            saved = json.loads(path.read_text())
            assert saved["publication"]["finalization_status"] == "complete"
            assert saved["cache_verification"]["verified"] is False
            assert saved["validation"]["status"] == "pending"
            assert calls.count("upload") == 1
            assert remote.execute("SELECT year,manager FROM ___leagues.public.transactions WHERE db_name='target' ORDER BY year").fetchall() == [(2014,"Saved alias"),(2026,"Saved alias")]
    finally:
        remote.close()


def test_superseded_recovery_cannot_start_inventory_writes(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets import fly_target
    from multi_league.core.readers import fly_reader
    path = committed_manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["publication"].update(finalization_status="pending", transport="delta",
        finalization_plan={"inventory": True, "merge_source": False, "context": {}})
    path.write_text(json.dumps(manifest))
    reads = []
    class Reader:
        def query_scalar(self, *_args, **_kwargs):
            reads.append(1)
            return 5 if len(reads) == 1 else 6
    class Target:
        def _reconcile_committed_merge(self, *_args):
            return {"status": "COMMITTED"}
        def mark_league_imported(self, *_args, **_kwargs):
            pytest.fail("superseded completion must not mutate inventory")
    monkeypatch.setattr(fly_reader, "FlyReader", Reader)
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    result = LocalLeagueDB(tmp_path, "target").upload_to_fly("target", finalize_only=True)
    assert result["finalization_status"] == "pending"
