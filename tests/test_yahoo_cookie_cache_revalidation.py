from pathlib import Path


WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "yahoo_cookie_import_worker.yml"
)

CACHE_REFRESH_WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "refresh_league_cache.yml"
)


def test_yahoo_cookie_worker_revalidates_and_warms_after_a_successful_import():
    """A completed cookie import must replace a previously cached importing shell."""
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "REVALIDATION_SECRET: ${{ secrets.REVALIDATION_SECRET }}" in source
    run_cookie_track = source.index("- name: Run Yahoo cookie track")
    refresh_cache = source.index("- name: Revalidate and warm Vercel cache")
    assert refresh_cache > run_cookie_track
    assert "python scripts/warm_vercel_cache.py" in source[refresh_cache:]
    assert '--db "${{ inputs.database_name }}"' in source[refresh_cache:]
    assert "--strategy expire" in source[refresh_cache:]
    assert "--strict" in source[refresh_cache:]


def test_cache_refresh_workflow_uses_the_authenticated_shared_warmer():
    """Operations can refresh a completed league without reimporting its data."""
    source = CACHE_REFRESH_WORKFLOW.read_text(encoding="utf-8")

    assert "database_name:" in source
    assert "REVALIDATION_SECRET: ${{ secrets.REVALIDATION_SECRET }}" in source
    assert "python scripts/warm_vercel_cache.py" in source
    assert '--db "${{ inputs.database_name }}"' in source
    assert "--strategy expire" in source
    assert "--strict" in source
