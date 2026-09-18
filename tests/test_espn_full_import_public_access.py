"""Exercise the actual full-import credential step without contacting Fly."""

import json
from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize("private, expected_exit", [(False, 0), (True, 1), (None, 1), ("false", 1)])
def test_public_import_does_not_require_a_cookie_encryption_key(tmp_path, monkeypatch, private, expected_exit):
    workflow = yaml.safe_load(
        (Path(__file__).parents[1] / ".github/workflows/espn_full_import_worker.yml").read_text(encoding="utf-8")
    )
    step = next(
        step for job in workflow["jobs"].values() for step in job["steps"]
        if step.get("name") == "Retrieve ESPN cookies"
    )
    program = step["run"].split("python - <<'PYEOF'\n", 1)[1].rsplit("PYEOF", 1)[0]
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CREDENTIAL_ENCRYPTION_KEY", raising=False)
    (tmp_path / "league_data_input.json").write_text(json.dumps({"is_private": private}), encoding="utf-8")
    with pytest.raises(SystemExit) as result:
        exec(compile(program, "ESPN full import credential step", "exec"), {})
    assert result.value.code == expected_exit
