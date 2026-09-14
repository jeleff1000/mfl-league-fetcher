from pathlib import Path


def test_membership_audit_is_cache_read_only_and_reads_only_compact_output():
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_membership_audit.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "actions/cache/restore@v5" in text
    assert "actions/cache/save@" not in text
    assert "actions/download-artifact@v5" in text
    assert "research_matchup_compact_season" in text
    assert "missing_by_position" in text
    assert "eligible_league_rows" in text
    assert "started_league_rows" in text
    assert "stat_bearing_residuals" in text
