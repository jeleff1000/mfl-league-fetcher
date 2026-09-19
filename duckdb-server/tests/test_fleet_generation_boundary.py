"""Endpoint proofs that fleet freshness is fenced per league, not by GHA order."""

import json
import tarfile
from dataclasses import replace
from io import BytesIO

from tests.test_fleet_partition import (
    BATCH_LEAGUES,
    _build_bundle,
    _fingerprint,
    _post_bundle,
    _query,
    client,  # noqa: F401 -- shared endpoint fixture
    data_dir,  # noqa: F401 -- shared seeded fleet fixture
)


def _scoped_fingerprints(client):
    return {
        (league, table): _fingerprint(client, table, f"db_name = '{league}'")
        for league in BATCH_LEAGUES
        for table in ("matchup", "matchup_career")
    }


def _generations(client):
    return _query(
        client,
        "SELECT db_name, generation FROM merge_admin.league_publish_generations "
        "WHERE db_name IN ('league_alpha', 'league_beta') ORDER BY db_name",
    )


def _with_manifest(bundle, archive_path, manifest):
    # Keep the real builder's Parquet and all other manifest fields intact.
    with tarfile.open(bundle.path, "r:gz") as source, tarfile.open(archive_path, "w:gz") as target:
        for member in source.getmembers():
            if not member.isfile():
                target.addfile(member)
                continue
            payload = source.extractfile(member).read()
            if member.name == "manifest.json":
                payload = json.dumps(manifest).encode("utf-8")
                member.size = len(payload)
            target.addfile(member, BytesIO(payload))
    return replace(bundle, path=archive_path, manifest=manifest)


def test_missing_league_generations_rejected_before_mutation(client, tmp_path):
    bundle = _build_bundle(tmp_path)
    before = _scoped_fingerprints(client)
    manifest = dict(bundle.manifest)
    del manifest["league_generations"]
    malformed = _with_manifest(bundle, tmp_path / "missing_generations.tar.gz", manifest)

    response = _post_bundle(client, malformed)

    assert response.status_code == 400, response.text
    assert "missing league_generations" in response.text
    assert _scoped_fingerprints(client) == before


def test_one_stale_league_rejects_entire_mixed_scope_bundle(client, tmp_path):
    # Only beta advances. Alpha's generation zero is still current.
    beta = _build_bundle(
        tmp_path, import_run_id="8000", leagues=["league_beta"],
        generations={"league_beta": 0}, matchup_points=140.0, career_wins=8,
    )
    committed = _post_bundle(client, beta)
    assert committed.status_code == 200, committed.text
    assert committed.json()["status"] == "COMMITTED"
    assert _generations(client) == [{"db_name": "league_beta", "generation": 1}]
    before = _scoped_fingerprints(client)
    before_generations = _generations(client)

    mixed = _build_bundle(
        tmp_path, import_run_id="8001",
        generations={"league_alpha": 0, "league_beta": 0},
        matchup_points=1.0, career_wins=0,
    )
    response = _post_bundle(client, mixed)

    assert response.status_code == 409, response.text
    assert "republished since bundle build" in response.text
    assert "league_beta" in response.text
    # Neither active facts, prior history, rollups, nor generation may advance.
    assert _scoped_fingerprints(client) == before
    assert _generations(client) == before_generations


def test_current_generation_accepts_lower_run_id_for_same_leagues(client, tmp_path):
    first = _build_bundle(tmp_path, import_run_id="9000", matchup_points=140.0, career_wins=8)
    response = _post_bundle(client, first)
    assert response.status_code == 200, response.text
    assert _generations(client) == [
        {"db_name": "league_alpha", "generation": 1},
        {"db_name": "league_beta", "generation": 1},
    ]
    history_before = _fingerprint(client, "matchup", "year = 2025")
    current = _build_bundle(
        tmp_path, import_run_id="8999",
        generations={"league_alpha": 1, "league_beta": 1},
        matchup_points=150.0, career_wins=9,
    )
    assert current.bundle_id != first.bundle_id

    response = _post_bundle(client, current)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "COMMITTED"
    assert not response.json().get("idempotent_replay", False)
    for league in BATCH_LEAGUES:
        assert _query(client, "SELECT COUNT(*) AS n, MIN(team_points) AS lo, MAX(team_points) AS hi "
                      f"FROM public.matchup WHERE db_name = '{league}' AND year = 2026") == [
            {"n": 6, "lo": 150.0, "hi": 150.0}]
        assert _query(client, "SELECT COUNT(*) AS n, MIN(wins) AS lo, MAX(wins) AS hi "
                      f"FROM public.matchup_career WHERE db_name = '{league}'") == [
            {"n": 2, "lo": 9, "hi": 9}]
    assert _generations(client) == [
        {"db_name": "league_alpha", "generation": 2},
        {"db_name": "league_beta", "generation": 2},
    ]
    assert _fingerprint(client, "matchup", "year = 2025") == history_before


def test_manifest_scope_missing_parquet_league_rolls_back_facts_and_generations(client, tmp_path):
    first = _post_bundle(client, _build_bundle(tmp_path, import_run_id="10000"))
    assert first.status_code == 200, first.text
    before = _scoped_fingerprints(client)
    before_generations = _generations(client)
    bundle = _build_bundle(
        tmp_path, import_run_id="10001",
        generations={"league_alpha": 1, "league_beta": 1},
        matchup_points=1.0, career_wins=0,
    )
    # Valid weekly table payloads still contain alpha AND beta, but only alpha
    # is declared generation-protected. The transaction must roll back both.
    manifest = dict(bundle.manifest)
    manifest["db_names"] = ["league_alpha"]
    manifest["league_generations"] = {"league_alpha": 1}
    malformed = _with_manifest(bundle, tmp_path / "scope_mismatch.tar.gz", manifest)

    response = _post_bundle(client, malformed)

    assert response.status_code == 422, response.text
    assert "Published row scope does not match the generation-protected league scope" in response.text
    assert _scoped_fingerprints(client) == before
    assert _generations(client) == before_generations
