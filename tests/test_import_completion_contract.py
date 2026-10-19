from pathlib import Path
import re

import pytest
import yaml


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_import_workflows_expose_commit_and_cache_states_separately(platform, mode):
    workflow = Path(__file__).resolve().parents[1] / ".github/workflows" / f"{platform}_{mode}_import_worker.yml"
    jobs = yaml.safe_load(workflow.read_text(encoding="utf-8"))["jobs"].values()
    job = next(job for job in jobs if any(step.get("name") == "Upload league data" for step in job["steps"]))
    steps = job["steps"]
    upload = next(step for step in steps if step.get("name") == "Upload league data")
    cache = next(step for step in steps if step.get("name") == "Revalidate and warm Vercel cache")
    for output in ("data_committed", "cache_verified", "completion_status"):
        expression = job["outputs"][output]
        step_id = re.search(r"steps\.([a-z_]+)\.outputs\.", expression)[1]
        assert any(step.get("id") == step_id for step in steps)
    assert steps.index(upload) < steps.index(cache)
    expression = cache["if"].replace("${{", "").replace("}}", "").strip()
    for committed, cancelled, expected in [(True, False, True), (False, False, False), (True, True, False)]:
        condition = expression.replace("!cancelled()", str(not cancelled)).replace("&&", "and")
        condition = condition.replace("steps.publication.outputs.data_committed", repr(str(committed).lower()))
        assert eval(condition, {"__builtins__": {}}, {}) is expected
