"""A cache-distribution outage must reach the import's live-source recovery."""

import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
GIT_BASH = Path("C:/Program Files/Git/bin/bash.exe")
BASH = str(GIT_BASH) if GIT_BASH.is_file() else shutil.which("bash")
pytestmark = pytest.mark.skipif(not BASH, reason="Bash is needed to execute the Actions shell steps")


def run_step(script, tmp_path, **environment):
    # Actions expressions are substituted by GitHub before the shell runs.
    script = re.sub(r"\$\{\{[^}]+\}\}", "test", script)
    return subprocess.run([BASH, "-e", "-c", script], cwd=tmp_path,
                          env={**os.environ, **environment}, capture_output=True, text=True)


@pytest.mark.parametrize("platform", ["yahoo", "sleeper", "espn"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_import_worker_cache_miss_reaches_python_recovery(tmp_path, platform, mode):
    workflow = yaml.safe_load((ROOT / f".github/workflows/{platform}_{mode}_import_worker.yml").read_text(encoding="utf-8"))
    steps = [step for job in workflow["jobs"].values() for step in job.get("steps", [])]
    restore = next(step for step in steps if step.get("uses") == "./.github/actions/restore-research-ops-cache")
    assert restore["with"].get("allow-missing") == "true"
    for step in steps:
        if step.get("name") in {"Verify ops cache from research lake", "Log cached ops cache"}:
            result = run_step(step["run"], tmp_path, OPS_CACHE_PATH=str(tmp_path / "missing.duckdb"))
            assert result.returncode == 0, result.stdout + result.stderr


def test_cookie_live_import_uses_shared_recovery_while_offline_replay_keeps_its_inputs():
    workflow = yaml.safe_load((ROOT / ".github/workflows/yahoo_cookie_import_worker.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["import"]["steps"]
    restore = next(step for step in steps if "restore-research-ops-cache" in step.get("uses", ""))
    assert restore["with"]["allow-missing"] == "${{ !inputs.cache_only && 'true' || 'false' }}"
    baseline = next(step for step in steps if step.get("name") == "Verify draft baseline")
    assert baseline["if"] == "inputs.cache_only"


@pytest.mark.parametrize("allow_missing", ["true", "false"])
@pytest.mark.parametrize("download_ok", [True, False])
def test_release_cache_restore_handles_outage_without_using_partial_file(tmp_path, allow_missing, download_ok):
    action = yaml.safe_load((ROOT / ".github/actions/restore-research-ops-cache/action.yml").read_text())
    steps = action["runs"]["steps"]
    assert steps[0].get("continue-on-error") is True
    backup = next(step for step in steps if "Restore release backup" in step.get("name", ""))
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    curl = fake_bin / "curl"
    curl.write_text("#!/bin/bash\nwhile [ $# -gt 0 ]; do\n"
                    "  if [ \"$1\" = --output ]; then shift; printf 'download bytes' > \"$1\"; break; fi\n"
                    "  shift\ndone\nexit " + ("0" if download_ok else "22") + "\n", encoding="utf-8", newline="\n")
    curl.chmod(0o755)
    cache = tmp_path / "research_public_lake" / "ops_cache.duckdb"
    assert cache.parent.parent.resolve() == tmp_path.resolve()
    script = backup["run"].replace("${{ inputs.allow-missing }}", allow_missing)
    result = run_step('export PATH="$PWD/bin:$PATH"\n' + script, tmp_path)
    assert (result.returncode == 0) is (download_ok or allow_missing == "true"), result.stdout + result.stderr
    assert cache.exists() is download_ok
    verify = steps[-1]["run"].replace("${{ inputs.allow-missing }}", allow_missing)
    verification = run_step(verify, tmp_path)
    assert (verification.returncode == 0) is (download_ok or allow_missing == "true")
