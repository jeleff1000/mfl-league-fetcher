"""Tests for the fleet partition publish path (/merge-fleet-partition).

Bundles are built by the REAL client builder
(multi_league.core.fleet_publish.build_fleet_partition_bundle) so the
client-side manifest contract and the server-side validation/merge are
exercised together. Verification reads go through /query so tests never
open a second handle on the server's database file.
"""

import json
import tarfile

import duckdb
import pytest

import fleet_merge

from tests.test_integration import client  # noqa: F401  (fixture)

ACTIVE_YEAR = 2026
PRIOR_YEAR = 2025

LEAGUES = ["league_alpha", "league_beta", "league_gamma"]
BATCH_LEAGUES = ["league_alpha", "league_beta"]  # league_gamma simulates a fetch failure


@pytest.fixture
def data_dir(tmp_path):
    """Seeded shared ___leagues: 3 leagues x 2 years of matchup + career rollups.

    Overrides the imported client fixture's data_dir so seeding happens
    before the app opens its pool.
    """
    db_path = tmp_path / "___leagues.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager_week VARCHAR, manager VARCHAR, team_points DOUBLE
        )
        """
    )
    conn.execute(
        "CREATE TABLE public.matchup_career (db_name VARCHAR, franchise_id VARCHAR, manager VARCHAR, wins INTEGER)"
    )
    for league in LEAGUES:
        for year in (PRIOR_YEAR, ACTIVE_YEAR):
            for week in (1, 2):
                for manager in ("alice", "bob"):
                    conn.execute(
                        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
                        [league, year, week, f"{manager}_{year}_{week}", manager, 100.0],
                    )
        for manager in ("alice", "bob"):
            conn.execute(
                "INSERT INTO public.matchup_career VALUES (?, ?, ?, ?)", [league, f"fid_{manager}", manager, 5]
            )
    conn.close()
    return tmp_path


def _stage_fleet_rows(*, matchup_points=120.5, career_wins=6, leagues=None, extra_year=None):
    """In-memory staged fleet DB shaped like the compute plane's output."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager_week VARCHAR, manager VARCHAR, team_points DOUBLE
        )
        """
    )
    conn.execute(
        "CREATE TABLE public.matchup_career (db_name VARCHAR, franchise_id VARCHAR, manager VARCHAR, wins INTEGER)"
    )
    for league in leagues or BATCH_LEAGUES:
        for week in (1, 2, 3):  # active season recomputed through the new week
            for manager in ("alice", "bob"):
                conn.execute(
                    "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
                    [league, ACTIVE_YEAR, week, f"{manager}_{ACTIVE_YEAR}_{week}", manager, matchup_points],
                )
        if extra_year:
            conn.execute(
                "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
                [league, extra_year, 1, f"zed_{extra_year}_1", "zed", 1.0],
            )
        for manager in ("alice", "bob"):
            conn.execute(
                "INSERT INTO public.matchup_career VALUES (?, ?, ?, ?)",
                [league, f"fid_{manager}", manager, career_wins],
            )
    return conn


def _build_bundle(tmp_path, *, import_run_id="2000", publish_sequence=1, generations=None, **stage_kwargs):
    from multi_league.core.fleet_publish import build_fleet_partition_bundle

    conn = _stage_fleet_rows(**stage_kwargs)
    try:
        bundle = build_fleet_partition_bundle(
            conn,
            active_year=ACTIVE_YEAR,
            league_generations=generations if generations is not None else {db: 0 for db in BATCH_LEAGUES},
            tables=["matchup", "matchup_career"],
            output_dir=tmp_path / f"bundle_{import_run_id}_{publish_sequence}",
            import_run_id=import_run_id,
            publish_sequence=publish_sequence,
        )
    finally:
        conn.close()
    return bundle


def _post_bundle(client, bundle):  # noqa: F811
    with open(bundle.path, "rb") as fh:
        return client.post(
            "/merge-fleet-partition",
            headers={
                "Authorization": "Bearer test-admin",
                "X-Bundle-Id": bundle.bundle_id,
                "X-Bundle-Hash": bundle.bundle_hash,
            },
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )


def _query(client, sql):  # noqa: F811
    resp = client.post(
        "/query",
        headers={"Authorization": "Bearer test-read"},
        json={"sql": sql, "database": "___leagues"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _fingerprint(client, table, where):  # noqa: F811
    rows = _query(
        client,
        f"""
        SELECT COUNT(*) AS n, COALESCE(sha256(string_agg(h, '' ORDER BY h)), 'empty') AS fp
        FROM (
            SELECT sha256(CAST(t AS VARCHAR)) AS h
            FROM (SELECT * FROM public.{table} WHERE {where}) t
        )
        """,
    )
    return int(rows[0]["n"]), str(rows[0]["fp"])


def test_fleet_commit_cannot_fire_process_kill_watchdog(data_dir, client, monkeypatch):  # noqa: F811
    import threading
    import main as main_mod

    killed = []
    watchdog = threading.Timer(0, lambda: killed.append(True))
    monkeypatch.setattr(main_mod, "_start_merge_hard_exit_timer", lambda *a: watchdog)
    execute = main_mod._interrupting_execute
    commits = []

    def commit_boundary(conn, sql, *args, **kwargs):
        if kwargs.get("step") == "commit fleet partition":
            commits.append(sql)
            watchdog.run()
        return execute(conn, sql, *args, **kwargs)

    monkeypatch.setattr(main_mod, "_interrupting_execute", commit_boundary)
    response = _post_bundle(client, _build_bundle(data_dir))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "COMMITTED"
    assert commits == ["COMMIT"]
    assert killed == [], "COMMIT can checkpoint; the process-kill timer must already be disarmed"


def test_fleet_partition_requires_admin(client):  # noqa: F811
    resp = client.post("/merge-fleet-partition", files={"file": ("x.tar.gz", b"junk", "application/gzip")})
    assert resp.status_code == 401


def test_fleet_commit_owns_aggregate_timestamp_and_replay_is_idempotent(
    data_dir, client, tmp_path
):  # noqa: F811
    from multi_league.core.fleet_publish import build_fleet_partition_bundle

    stage = duckdb.connect(":memory:")
    try:
        stage.execute("CREATE SCHEMA public")
        stage.execute(
            "CREATE TABLE public.homepage_league_summary "
            "(db_name VARCHAR, last_updated TIMESTAMP, data_year INTEGER, "
            "data_week INTEGER, highest_score_points DOUBLE)"
        )
        stage.execute(
            "INSERT INTO public.homepage_league_summary VALUES "
            "('league_alpha', '2000-01-01 00:00:00', 2026, 1, 152.66)"
        )
        first = build_fleet_partition_bundle(
            stage, active_year=ACTIVE_YEAR,
            league_generations={"league_alpha": 0},
            tables=["homepage_league_summary"],
            output_dir=tmp_path / "first-timestamp",
        )
        stage.execute(
            "UPDATE public.homepage_league_summary "
            "SET last_updated = '2001-01-01 00:00:00'"
        )
        replay = build_fleet_partition_bundle(
            stage, active_year=ACTIVE_YEAR,
            league_generations={"league_alpha": 0},
            tables=["homepage_league_summary"],
            output_dir=tmp_path / "replay-timestamp",
        )
    finally:
        stage.close()

    assert first.bundle_id == replay.bundle_id
    first_response = _post_bundle(client, first)
    assert first_response.status_code == 200, first_response.text
    before = _query(
        client,
        "SELECT CAST(last_updated AS VARCHAR) AS ts FROM public.homepage_league_summary "
        "WHERE db_name = 'league_alpha'",
    )[0]["ts"]
    assert before not in {"2000-01-01 00:00:00", "2001-01-01 00:00:00"}

    replay_response = _post_bundle(client, replay)
    assert replay_response.status_code == 200, replay_response.text
    after = _query(
        client,
        "SELECT CAST(last_updated AS VARCHAR) AS ts FROM public.homepage_league_summary "
        "WHERE db_name = 'league_alpha'",
    )[0]["ts"]
    assert after == before


def test_stale_delta_import_cannot_rewind_a_newer_weekly_fleet_commit(
    data_dir, client, tmp_path
):  # noqa: F811
    from multi_league.core.delta_publish import build_delta_bundle

    stale_source = duckdb.connect(":memory:")
    try:
        stale_source.execute("CREATE SCHEMA public")
        stale_source.execute(
            "CREATE TABLE public.matchup "
            "(db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR, "
            "manager VARCHAR, team_points DOUBLE)"
        )
        stale_source.execute(
            "INSERT INTO public.matchup VALUES "
            "('league_alpha', 2025, 1, 'alice_2025_1', 'alice', 50), "
            "('league_alpha', 2026, 1, 'alice_2026_1', 'alice', 50)"
        )
        stale_source.execute(
            "CREATE TABLE public.league_settings "
            "(db_name VARCHAR, year INTEGER, league_name VARCHAR)"
        )
        stale_source.execute(
            "INSERT INTO public.league_settings VALUES "
            "('league_alpha', 2025, 'Alpha'), ('league_alpha', 2026, 'Alpha')"
        )
        stale_source.execute(
            "CREATE TABLE public.player_fantasy "
            "(db_name VARCHAR, year INTEGER, week INTEGER, player_week VARCHAR, "
            "NFL_player_id VARCHAR, manager VARCHAR, fantasy_points DOUBLE)"
        )
        stale_source.execute(
            "INSERT INTO public.player_fantasy VALUES "
            "('league_alpha', 2026, 1, 'nfl_a_2026_1', 'nfl_a', 'alice', 10)"
        )
        # This is the full-import repair lane, not a quick partition publish.
        # Supply its required derived table schemas so validation reaches the
        # stale-generation fence rather than rejecting an incomplete fixture.
        from multi_league.core.delta_publish import canonical_table_registry

        for table, spec in canonical_table_registry().items():
            columns = ', '.join(f'"{name}" {dtype}' for name, dtype in spec['columns'].items())
            stale_source.execute(f'CREATE TABLE IF NOT EXISTS public."{table}" ({columns})')
        stale = build_delta_bundle(
            stale_source, db_name="league_alpha", import_mode="full",
            platform="sleeper", base_generation=0,
            output_dir=tmp_path / "stale-delta",
        )
    finally:
        stale_source.close()

    newer = _build_bundle(tmp_path, import_run_id="2000")
    assert _post_bundle(client, newer).status_code == 200
    before_active = _fingerprint(client, "matchup", "db_name = 'league_alpha' AND year = 2026")
    before_history = _fingerprint(client, "matchup", "db_name = 'league_alpha' AND year = 2025")
    with open(stale.path, "rb") as fh:
        response = client.post(
            "/merge-league-delta",
            headers={
                "Authorization": "Bearer test-admin",
                "x-db-name": "league_alpha",
                "x-bundle-id": stale.bundle_id,
                "x-bundle-hash": stale.bundle_hash,
            },
            files={"file": (stale.path.name, fh, "application/gzip")},
        )
    assert response.status_code == 409, response.text
    assert "generation" in response.text.lower()
    assert _fingerprint(client, "matchup", "db_name = 'league_alpha' AND year = 2026") == before_active
    assert _fingerprint(client, "matchup", "db_name = 'league_alpha' AND year = 2025") == before_history


def test_fleet_partition_scoped_merge_commits(data_dir, client, tmp_path):  # noqa: F811
    gamma_before = _fingerprint(client, "matchup", "db_name = 'league_gamma'")
    prior_before = _fingerprint(client, "matchup", f"year = {PRIOR_YEAR}")
    gamma_career_before = _fingerprint(client, "matchup_career", "db_name = 'league_gamma'")

    bundle = _build_bundle(tmp_path)
    resp = _post_bundle(client, bundle)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "COMMITTED"
    assert body["tables"] == {"matchup": 12, "matchup_career": 4}
    assert body["active_year"] == ACTIVE_YEAR

    # Batched league: active season replaced (2 weeks -> 3 weeks, new points),
    # prior season intact -> 4 prior + 6 active rows.
    rows = _query(
        client,
        "SELECT COUNT(*) AS n, MAX(team_points) AS pts FROM public.matchup "
        f"WHERE db_name = 'league_alpha' AND year = {ACTIVE_YEAR}",
    )
    assert rows[0]["n"] == 6
    assert rows[0]["pts"] == 120.5
    rows = _query(client, "SELECT COUNT(*) AS n FROM public.matchup WHERE db_name = 'league_alpha'")
    assert rows[0]["n"] == 10
    rows = _query(client, "SELECT MAX(wins) AS w FROM public.matchup_career WHERE db_name = 'league_alpha'")
    assert rows[0]["w"] == 6

    # Fetch-failure league and frozen prior seasons are byte-identical.
    assert _fingerprint(client, "matchup", "db_name = 'league_gamma'") == gamma_before
    assert _fingerprint(client, "matchup", f"year = {PRIOR_YEAR}") == prior_before
    assert _fingerprint(client, "matchup_career", "db_name = 'league_gamma'") == gamma_career_before


def test_fleet_partition_idempotent_replay(data_dir, client, tmp_path):  # noqa: F811
    bundle = _build_bundle(tmp_path)
    first = _post_bundle(client, bundle)
    assert first.status_code == 200, first.text
    after_first = _fingerprint(client, "matchup", "db_name = 'league_alpha'")

    replay = _post_bundle(client, bundle)
    assert replay.status_code == 200, replay.text
    assert replay.json().get("idempotent_replay") is True
    assert _fingerprint(client, "matchup", "db_name = 'league_alpha'") == after_first
    same_content_new_run = _build_bundle(tmp_path, import_run_id="2500", publish_sequence=2)
    assert same_content_new_run.bundle_id == bundle.bundle_id
    replay_new_run = _post_bundle(client, same_content_new_run)
    assert replay_new_run.status_code == 200, replay_new_run.text
    assert replay_new_run.json().get("idempotent_replay") is True
    assert _fingerprint(client, "matchup", "db_name = 'league_alpha'") == after_first


def test_fleet_partition_rejects_older_bundle(data_dir, client, tmp_path):  # noqa: F811
    newer = _build_bundle(tmp_path, import_run_id="3000", publish_sequence=2, matchup_points=121.5)
    assert _post_bundle(client, newer).status_code == 200

    older = _build_bundle(tmp_path, import_run_id="2999", publish_sequence=1)
    resp = _post_bundle(client, older)
    assert resp.status_code == 409


def test_fleet_partition_rejects_out_of_scope_year_parquet(data_dir, client, tmp_path):  # noqa: F811
    """A tampered parquet carrying prior-season rows must be rejected before merge."""
    prior_before = _fingerprint(client, "matchup", f"year = {PRIOR_YEAR}")

    bundle = _build_bundle(tmp_path)
    tampered_dir = tmp_path / "tampered"
    tampered_dir.mkdir()
    with tarfile.open(bundle.path, "r:gz") as tar:
        tar.extractall(tampered_dir, filter="data")

    conn = _stage_fleet_rows(extra_year=PRIOR_YEAR)
    conn.execute(
        f"COPY (SELECT db_name, year, week, manager_week, manager, team_points FROM public.matchup) "
        f"TO '{(tampered_dir / 'tables' / 'matchup.parquet').as_posix()}' (FORMAT PARQUET)"
    )
    conn.close()

    archive = tmp_path / "tampered.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tampered_dir / "manifest.json", arcname="manifest.json")
        tar.add(tampered_dir / "tables" / "matchup.parquet", arcname="tables/matchup.parquet")
        tar.add(tampered_dir / "tables" / "matchup_career.parquet", arcname="tables/matchup_career.parquet")

    manifest = json.loads((tampered_dir / "manifest.json").read_text(encoding="utf-8"))
    with open(archive, "rb") as fh:
        resp = client.post(
            "/merge-fleet-partition",
            headers={
                "Authorization": "Bearer test-admin",
                "X-Bundle-Id": manifest["bundle_id"],
                "X-Bundle-Hash": manifest["bundle_hash"],
            },
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert resp.status_code == 400
    assert _fingerprint(client, "matchup", f"year = {PRIOR_YEAR}") == prior_before


def test_fleet_partition_rejects_replace_league_mode(data_dir, client, tmp_path):  # noqa: F811
    bundle = _build_bundle(tmp_path)

    tampered_dir = tmp_path / "mode_tamper"
    tampered_dir.mkdir()
    with tarfile.open(bundle.path, "r:gz") as tar:
        tar.extractall(tampered_dir, filter="data")
    manifest = json.loads((tampered_dir / "manifest.json").read_text(encoding="utf-8"))
    for entry in manifest["tables"]:
        entry["merge_mode"] = "replace_league"
    (tampered_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    archive = tmp_path / "mode_tamper.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tampered_dir / "manifest.json", arcname="manifest.json")
        tar.add(tampered_dir / "tables" / "matchup.parquet", arcname="tables/matchup.parquet")
        tar.add(tampered_dir / "tables" / "matchup_career.parquet", arcname="tables/matchup_career.parquet")

    with open(archive, "rb") as fh:
        resp = client.post(
            "/merge-fleet-partition",
            headers={"Authorization": "Bearer test-admin"},
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert resp.status_code == 400
    assert "replace_scope" in resp.text


def test_fleet_partition_status_via_delta_status_endpoint(data_dir, client, tmp_path):  # noqa: F811
    bundle = _build_bundle(tmp_path)
    assert _post_bundle(client, bundle).status_code == 200

    resp = client.get(
        "/merge-league-delta/status",
        headers={"Authorization": "Bearer test-admin"},
        params={"db_name": fleet_merge.FLEET_DB_SENTINEL, "bundle_id": bundle.bundle_id},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMMITTED"


def test_expected_cadence_matches_worker_registry():
    """Server cadence policy must cover EVERY registry table and agree exactly.

    One-directional coverage would let a new year-bearing table publish as
    league_rollup and widen its delete to whole league histories.
    """
    from multi_league.core.delta_publish import CADENCE_CLASSES, canonical_table_registry

    registry = canonical_table_registry()
    assert set(fleet_merge.EXPECTED_CADENCE) == set(registry)
    for table, spec in registry.items():
        assert fleet_merge.EXPECTED_CADENCE[table] == spec["cadence_class"], table

    emitted = {spec["cadence_class"] for spec in registry.values()}
    assert emitted <= CADENCE_CLASSES
    assert emitted <= fleet_merge.ALLOWED_CADENCE_CLASSES


def test_fleet_partition_rejects_mislabeled_cadence(data_dir, client, tmp_path):  # noqa: F811
    """A bundle labeling a year-bearing table league_rollup must be rejected
    before merge — otherwise its delete scope widens to whole league histories."""
    bundle = _build_bundle(tmp_path)

    tampered_dir = tmp_path / "cadence_tamper"
    tampered_dir.mkdir()
    with tarfile.open(bundle.path, "r:gz") as tar:
        tar.extractall(tampered_dir, filter="data")
    manifest = json.loads((tampered_dir / "manifest.json").read_text(encoding="utf-8"))
    for entry in manifest["tables"]:
        if entry["table"] == "matchup":
            entry["cadence_class"] = "league_rollup"
            entry["scope"] = {}
    (tampered_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    archive = tmp_path / "cadence_tamper.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tampered_dir / "manifest.json", arcname="manifest.json")
        tar.add(tampered_dir / "tables" / "matchup.parquet", arcname="tables/matchup.parquet")
        tar.add(tampered_dir / "tables" / "matchup_career.parquet", arcname="tables/matchup_career.parquet")

    with open(archive, "rb") as fh:
        resp = client.post(
            "/merge-fleet-partition",
            headers={"Authorization": "Bearer test-admin"},
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert resp.status_code == 400
    assert "must publish as active_season" in resp.text


def test_client_scope_guard_blocks_prior_season_rows(tmp_path):
    """The client builder itself refuses to stage prior-season rows."""
    from multi_league.core.fleet_publish import FleetScopeError, build_fleet_partition_bundle

    conn = _stage_fleet_rows(extra_year=PRIOR_YEAR)
    try:
        with pytest.raises(FleetScopeError, match="outside active year"):
            build_fleet_partition_bundle(
                conn,
                active_year=ACTIVE_YEAR,
                league_generations={db: 0 for db in BATCH_LEAGUES},
                tables=["matchup", "matchup_career"],
                output_dir=tmp_path / "guard",
                import_run_id="4000",
                publish_sequence=1,
            )
    finally:
        conn.close()


def test_type_drift_cast_failure_aborts_before_delete(tmp_path):
    """TRY_CAST would silently NULL unconvertible values fleet-wide; the merge
    must detect cast failures on the parquet and abort BEFORE deleting."""
    import duckdb as _duckdb

    server = _duckdb.connect(":memory:")
    server.execute("CREATE SCHEMA IF NOT EXISTS public")
    # Target column is INTEGER; incoming parquet carries VARCHAR garbage.
    server.execute(
        "CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, "
        "manager_week VARCHAR, team_points INTEGER)"
    )
    server.execute(
        "INSERT INTO public.matchup VALUES ('league_alpha', 2026, 1, 'a_2026_1', 100)"
    )

    staged = _duckdb.connect(":memory:")
    staged.execute(
        "CREATE TABLE m (db_name VARCHAR, year INTEGER, week INTEGER, "
        "manager_week VARCHAR, team_points VARCHAR)"
    )
    staged.execute("INSERT INTO m VALUES ('league_alpha', 2026, 1, 'a_2026_1', 'not-a-number')")
    parquet = tmp_path / "matchup.parquet"
    staged.execute(f"COPY m TO '{parquet.as_posix()}' (FORMAT PARQUET)")
    staged.close()

    manifest = {
        "bundle_id": "b1",
        "bundle_hash": "h1",
        "active_year": 2026,
        "tables": [
            {
                "table": "matchup",
                "path": "matchup.parquet",
                "row_count": 1,
                "cadence_class": "active_season",
                "scope": {"year": 2026},
            }
        ],
    }
    with pytest.raises(RuntimeError, match="Type drift in matchup"):
        fleet_merge.apply_fleet_merge(server, manifest, tmp_path)

    # The pre-delete guard fired: existing production rows are untouched.
    row = server.execute(
        "SELECT team_points FROM public.matchup WHERE manager_week = 'a_2026_1'"
    ).fetchone()
    assert row == (100,)
    server.close()


def test_g14_generation_conflict_rejects_bundle(data_dir, client, tmp_path):  # noqa: F811
    """A bundle built at generation 0 must be rejected after a repair-lane
    commit bumped a batched league's generation."""
    first = _build_bundle(tmp_path, import_run_id="5000", publish_sequence=1)
    assert _post_bundle(client, first).status_code == 200  # bumps gens to 1

    stale = _build_bundle(tmp_path, import_run_id="5001", publish_sequence=1, matchup_points=121.5)  # gens still 0
    resp = _post_bundle(client, stale)
    assert resp.status_code == 409
    assert "republished since bundle build" in resp.text

    fresh = _build_bundle(
        tmp_path, import_run_id="5002", publish_sequence=1,
        generations={db: 1 for db in BATCH_LEAGUES},
    )
    assert _post_bundle(client, fresh).status_code == 200


def test_g14_publish_lock_blocks_repair_lane(data_dir, client, tmp_path):  # noqa: F811
    """While the fleet publish window lock is held, repair-lane delta merges
    are rejected with 409 instead of racing the generation check."""
    resp = client.post(
        "/fleet-publish-lock",
        headers={"Authorization": "Bearer test-admin"},
        json={"locked": True, "run_id": "drill"},
    )
    assert resp.status_code == 200, resp.text

    from tests.test_integration import _make_delta_bundle
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(tmp_path, main_mod, db_name="league_alpha")
    with open(archive_path, "rb") as fh:
        blocked = client.post(
            "/merge-league-delta",
            headers={"Authorization": "Bearer test-admin", "X-Db-Name": "league_alpha"},
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert blocked.status_code == 409
    assert "publish window active" in blocked.text.lower() or "Fleet publish window" in blocked.text

    resp = client.post(
        "/fleet-publish-lock",
        headers={"Authorization": "Bearer test-admin"},
        json={"locked": False, "run_id": "drill"},
    )
    assert resp.status_code == 200
    with open(archive_path, "rb") as fh:
        allowed = client.post(
            "/merge-league-delta",
            headers={"Authorization": "Bearer test-admin", "X-Db-Name": "league_alpha"},
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert allowed.status_code == 200, allowed.text


def test_g14_repair_commit_bumps_generation(data_dir, client, tmp_path):  # noqa: F811
    from tests.test_integration import _make_delta_bundle
    import main as main_mod

    archive_path, manifest = _make_delta_bundle(tmp_path, main_mod, db_name="league_beta")
    with open(archive_path, "rb") as fh:
        resp = client.post(
            "/merge-league-delta",
            headers={"Authorization": "Bearer test-admin", "X-Db-Name": "league_beta"},
            files={"file": ("bundle.tar.gz", fh, "application/gzip")},
        )
    assert resp.status_code == 200, resp.text
    rows = _query(
        client,
        "SELECT generation, lane FROM merge_admin.league_publish_generations WHERE db_name = 'league_beta'",
    )
    assert rows == [{"generation": 1, "lane": "repair"}]
