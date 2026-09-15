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
    assert "attempt_id:" in text
    assert "claim_version:" in text
    assert "observed_manifest_digest:" in text
    assert "push:" not in text
    assert "scripts/record_league_update_status.py" in text
    assert "--status running" in text
    assert text.count('--attempt-id "${{ inputs.attempt_id }}"') == 3
    assert text.count('--claim-version "${{ inputs.claim_version }}"') == 3
    assert "--require-entitled" in text
    assert f"scripts/refresh_{platform}_active_season.py" in text
    assert '--observed-manifest-digest "${OBSERVED_MANIFEST_DIGEST}"' in text
    assert "scripts/warm_vercel_cache.py" in text
    assert "--strict" in text
    assert "--verify-hot" in text
    assert "--status succeeded" in text
    assert "--status failed" in text
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
