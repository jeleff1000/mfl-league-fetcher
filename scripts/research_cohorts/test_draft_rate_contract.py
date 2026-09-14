from scripts.research_cohorts import build_research_draft_cohort as draft


def test_native_draft_rate_is_null_without_native_draft_sample():
    sql = draft.native_draft_rate_sql()

    assert "j.n_drafted > 0" in sql
    assert "j.n_leagues IS NOT NULL" in sql
    assert "COALESCE(mb.pct_drafted" not in sql
