"""Exercise the actual full-import credential step without contacting Fly."""

import json
import sys
import types
from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize(
    "payload",
    [{"is_private": False}, {"is_private": True}, {}],
    ids=["visible-app", "hidden-app", "unspecified-visibility"],
)
@pytest.mark.parametrize(
    "key_available, stored_pair, expected_exit",
    [
        (True, "complete", 0),
        (False, "complete", 1),
        (True, "missing", 1),
        (True, "partial", 1),
    ],
    ids=["load-pair", "missing-key", "missing-pair", "partial-pair"],
)
def test_provider_credentials_are_independent_of_app_visibility(
    tmp_path, monkeypatch, capsys, payload, key_available, stored_pair, expected_exit
):
    workflow = yaml.safe_load(
        (Path(__file__).parents[1] / ".github/workflows/espn_full_import_worker.yml").read_text(encoding="utf-8")
    )
    step = next(
        step for job in workflow["jobs"].values() for step in job["steps"]
        if step.get("name") == "Retrieve ESPN cookies"
    )
    program = step["run"].split("python - <<'PYEOF'\n", 1)[1].rsplit("PYEOF", 1)[0]
    program = program.replace("${{ steps.parse.outputs.database_name }}", "go_pats_2021")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CREDENTIAL_ENCRYPTION_KEY", "test-only-key" if key_available else "")
    monkeypatch.setattr(sys, "path", sys.path[:])
    (tmp_path / "league_data_input.json").write_text(json.dumps(payload), encoding="utf-8")

    # Stub only the external credential read; execute the real workflow logic
    # and inspect the cookie files that the importer would consume.
    credential_store = types.ModuleType("multi_league.utils.credential_store")

    def retrieve_credentials(db_name):
        assert db_name == "go_pats_2021"
        if stored_pair == "missing":
            return None
        return {
            "league_id": 53777071,
            "league_name": "Test league",
            "database_name": "go_pats_2021",
            "espn_s2": "test-cookie-s2",
            "swid": "test-cookie-swid" if stored_pair == "complete" else "",
        }

    credential_store.retrieve_espn_credentials = retrieve_credentials
    monkeypatch.setitem(sys.modules, credential_store.__name__, credential_store)
    exit_code = 0
    try:
        exec(compile(program, "ESPN full import credential step", "exec"), {})
    except SystemExit as exc:
        exit_code = exc.code

    assert exit_code == expected_exit
    if expected_exit == 0:
        assert (tmp_path / ".espn_s2").exists()
        assert (tmp_path / ".espn_swid").exists()
        assert (tmp_path / ".espn_s2").read_text() == "test-cookie-s2"
        assert (tmp_path / ".espn_swid").read_text() == "test-cookie-swid"
    else:
        assert not (tmp_path / ".espn_s2").exists()
        assert not (tmp_path / ".espn_swid").exists()
    output = capsys.readouterr()
    for cookie in ("test-cookie-s2", "test-cookie-swid"):
        assert cookie not in output.out + output.err
