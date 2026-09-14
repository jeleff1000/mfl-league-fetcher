from pathlib import Path

import yaml


WORKFLOW = Path(".github/workflows/research_runtime_smoke.yml")


def test_research_runtime_smoke_is_public_read_only_and_representative() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["research-runtime-smoke"]
    rendered = WORKFLOW.read_text(encoding="utf-8")

    assert "restore-research-ops-cache" in rendered
    assert "test_matchup_metric_sql.py" in rendered
    assert "test_source_backfill_readiness.py" in rendered
    assert "test_release_gate_matchup_metrics.py" in rendered
    assert "DATABASE_ADMIN_TOKEN" not in rendered
    assert "DATABASE_WRITE_TOKEN" not in rendered
    assert "actions/upload-artifact" not in rendered
    assert "fantasy_football_data_scripts" in job["env"]["PYTHONPATH"]
    assert "polars==1.35.2" in rendered
    assert any(step.get("uses") == "actions/checkout@v5" for step in job["steps"])
