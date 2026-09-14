from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "research_lineage_policy.yml"


def test_research_lineage_guard_ignores_product_workflow_changes():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert '".github/workflows/**"' not in workflow
    assert workflow.count('".github/workflows/research_*.yml"') == 2
    assert workflow.count('"scripts/research_cohorts/**"') == 2
