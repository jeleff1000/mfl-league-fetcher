from pathlib import Path


def test_retired_cache_cleanup_is_manual_not_hourly() -> None:
    workflow = (
        Path(__file__).resolve().parents[1]
        / ".github"
        / "workflows"
        / "guard_legacy_research_cache.yml"
    ).read_text(encoding="utf-8")

    assert "schedule:" not in workflow
    assert "workflow_dispatch:" in workflow
