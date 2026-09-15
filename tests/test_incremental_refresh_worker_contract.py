from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = {
    "yahoo": "yahoo_incremental_refresh_worker.yml",
    "espn": "espn_incremental_refresh_worker.yml",
    "sleeper": "sleeper_incremental_refresh_worker.yml",
}


@pytest.mark.parametrize(("platform", "filename"), WORKFLOWS.items())
def test_ui_lifecycle_wraps_existing_september_refresh(platform: str, filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "workflow_dispatch:" in text
    assert "dispatch_token:" in text
    assert "cache_only:" in text
    assert "scripts/recover_league_update_cache.py" in text
    assert f"--platform {platform}" in text
    assert "if: ${{ !inputs.cache_only }}" in text
    assert "if: ${{ !inputs.cache_only && inputs.execute }}" in text
    assert "if: ${{ failure() && !inputs.cache_only" in text
    assert "attempt_id:" in text
    assert "claim_version:" in text
    assert "observed_manifest_digest:" in text
    assert "push:" not in text
    assert "scripts/record_league_update_status.py" in text
    assert "--status running" in text
    assert text.count('--attempt-id "${{ inputs.attempt_id }}"') >= 4
    assert text.count('--claim-version "${{ inputs.claim_version }}"') >= 4
    assert "--require-entitled" in text
    assert f"scripts/refresh_{platform}_active_season.py" in text
    assert '--observed-manifest-digest "${OBSERVED_MANIFEST_DIGEST}"' in text
    assert "scripts/warm_vercel_cache.py" in text
    assert "id: publication" in text
    assert "scripts/league_update_workflow_receipt.py" in text
    assert "--require-publication" in text
    assert "steps.publication.outputs.committed == 'true'" in text
    assert "--status committed" in text
    assert "--failure-status" in text
    assert "committed_cache_pending" in text
    assert "--strict" in text
    assert "--verify-hot" in text
    assert "--status succeeded" in text
    assert '--status "${recovery_status}"' in text
    assert 'recovery_status=$(python scripts/league_update_workflow_receipt.py' in text
    assert text.index("scripts/warm_vercel_cache.py") < text.index("--status succeeded")
    assert "timeout-minutes: 15" in text


def test_shared_active_refresh_publishes_data_and_homepage_in_one_bundle():
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_espn_active_season.py",
        "refresh_sleeper_active_season.py",
    ):
        text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert "prepare_homepage_refresh" in text
        assert "publish_homepage_refresh_bundle" not in text
        assert text.index("homepage = prepare_homepage_refresh(") < text.index("bundle = build_fleet_partition_bundle(")
        assert text.count("merge_fleet_partition(") == 1
        assert 'parser.add_argument("--observed-manifest-digest")' in text
        assert "load_persisted_refresh_plan(" in text
        assert 'receipt["source_manifest_digest"]' in text
        assert 'receipt["source_manifest_json"]' in text


def test_all_platform_updates_publish_against_the_hydrated_source_generation():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert "source_frames, base_generation = _capture_update_source_frames(" in text
        assert "league_generations={args.db: base_generation}" in text
        assert "generation = _publish_generation(reader, args.db)" not in text
    yahoo = (ROOT / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    snapshot = yahoo.split("def _capture_update_source_frames(", 1)[1].split("\ndef ", 1)[0]
    assert snapshot.count("_publish_generation(reader, db_name)") == 2
    assert snapshot.index("frames = _source_frames(") < snapshot.index("if _publish_generation(")


def test_direct_execute_runs_enforce_the_same_paid_entitlement_as_ui_runs():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert "assert_league_update_entitled(reader, database_name=args.db)" in text
        assert text.index("assert_league_update_entitled(reader, database_name=args.db)") < text.index(
            "bundle = build_fleet_partition_bundle("
        )


def test_sleeper_refresh_merges_rosters_through_canonical_ownership_key():
    text = (ROOT / "scripts" / "refresh_sleeper_active_season.py").read_text(encoding="utf-8")
    ownership = (
        ROOT
        / "fantasy_football_data_scripts"
        / "multi_league"
        / "core"
        / "league_update_ownership.py"
    ).read_text(encoding="utf-8")

    assert 'merge_provider_refresh_table(\n            local_db,\n            "player_fantasy"' in text
    assert '"player_fantasy": ("db_name", "player_week")' in ownership
    assert '"sleeper_player_id_original"' not in ownership
    assert "validate_tabular_active_scope(" in text
    assert text.index("validate_tabular_active_scope(") < text.index("bundle = build_fleet_partition_bundle(")
