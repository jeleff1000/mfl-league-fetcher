from pathlib import Path


def test_verify_workflow_seeds_public_actions_cache_after_release_fallback() -> None:
    workflow = (
        Path(__file__).resolve().parents[1] / ".github" / "workflows" / "verify_ops_cache.yml"
    ).read_text(encoding="utf-8")

    assert "actions/cache/save@v5" in workflow
    assert "steps.research-lake.outputs.cache-hit != 'true'" in workflow
    assert "key: ${{ env.RESEARCH_LAKE_KEY }}" in workflow
