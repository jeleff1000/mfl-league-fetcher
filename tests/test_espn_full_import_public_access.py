"""Exercise the actual full-import credential step with no Fly or ESPN traffic."""

import json
import shutil
import subprocess
import sys
import types
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import requests
import yaml

from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient


def public_league(year=2019):
    return {
        "id": 53777071,
        "seasonId": year,
        "settings": {"name": "Public test league", "isPublic": True},
        "teams": [{"id": 1}, {"id": 2}],
    }


@pytest.fixture
def provider(monkeypatch):
    state = {"responses": [], "calls": []}

    def request(session, method, url, **kwargs):
        assert method.upper() == "GET"
        assert urlsplit(url).hostname == "lm-api-reads.fantasy.espn.com"
        assert not session.cookies
        assert not kwargs.get("cookies")
        assert not (kwargs.get("headers") or {}).get("Cookie")
        state["calls"].append((url, kwargs))
        reply = state["responses"].pop(0) if state["responses"] else (401, {})
        if isinstance(reply, Exception):
            raise reply
        status, payload = reply
        response = requests.Response()
        response.status_code = status
        response.url = url
        response._content = json.dumps(payload).encode()
        return response

    monkeypatch.setattr(requests.sessions.Session, "request", request)
    return state


@pytest.fixture
def run_credential_step(tmp_path, monkeypatch, capsys):
    workflow = yaml.safe_load(
        (Path(__file__).parents[1] / ".github/workflows/espn_full_import_worker.yml").read_text(encoding="utf-8")
    )
    step = next(
        step for job in workflow["jobs"].values() for step in job["steps"]
        if step.get("name") == "Retrieve ESPN cookies"
    )
    source = step["run"].split("python - <<'PYEOF'\n", 1)[1].rsplit("PYEOF", 1)[0]
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", sys.path[:])

    def run(payload=None, *, key_available=True, stored_pair="missing", year=2019):
        payload = {"espn_league_id": 53777071, "end_year": year, **(payload or {})}
        (tmp_path / "league_data_input.json").write_text(json.dumps(payload), encoding="utf-8")
        for key, path in (("espn_s2", ".espn_s2"), ("swid", ".espn_swid")):
            if key in payload:
                (tmp_path / path).write_text(payload[key], encoding="utf-8")
        monkeypatch.setenv("CREDENTIAL_ENCRYPTION_KEY", "test-only-key" if key_available else "")
        credential_store = types.ModuleType("multi_league.utils.credential_store")

        def retrieve_credentials(db_name):
            assert key_available
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
        program = source.replace("${{ steps.parse.outputs.database_name }}", "go_pats_2021")
        program = program.replace("${{ steps.parse.outputs.espn_league_id }}", "53777071")
        program = program.replace("${{ steps.parse.outputs.end_year }}", str(year))
        exit_code = 0
        try:
            exec(compile(program, "ESPN full import credential step", "exec"), {})
        except SystemExit as exc:
            exit_code = exc.code
        output = capsys.readouterr()
        for cookie in ("test-cookie-s2", "test-cookie-swid"):
            assert cookie not in output.out + output.err
        files = {
            name: (tmp_path / name).read_text()
            for name in (".espn_s2", ".espn_swid") if (tmp_path / name).exists()
        }
        return exit_code, files

    return run


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
    run_credential_step, provider, payload, key_available, stored_pair, expected_exit
):
    exit_code, files = run_credential_step(payload, key_available=key_available, stored_pair=stored_pair)
    assert exit_code == expected_exit
    if expected_exit == 0:
        assert files == {".espn_s2": "test-cookie-s2", ".espn_swid": "test-cookie-swid"}
        assert provider["calls"] == []  # Stored pair wins even for a visible app.
    else:
        assert files == {}
        assert provider["calls"]  # Fail only after independently testing public access.


@pytest.mark.parametrize("payload", [{"is_private": False}, {"is_private": True}, {}])
@pytest.mark.parametrize("key_available", [False, True])
def test_verified_public_full_needs_no_stored_pair(run_credential_step, provider, payload, key_available):
    provider["responses"] = [(200, public_league())]
    code, files = run_credential_step(payload, key_available=key_available)
    assert code == 0
    assert not any(files.values())
    assert len(provider["calls"]) == 1
    url, kwargs = provider["calls"][0]
    assert "/seasons/2019/segments/0/leagues/53777071" in url
    assert kwargs["params"]["view"] == ["mSettings", "mTeam"]
    assert kwargs["timeout"] == 7


@pytest.mark.parametrize("year", [2016, 2019])
def test_public_proof_uses_exact_requested_year(run_credential_step, provider, year):
    payload = public_league(year)
    provider["responses"] = [(200, [payload] if year < 2018 else payload)]
    assert run_credential_step(year=year)[0] == 0
    assert len(provider["calls"]) == 1
    url, kwargs = provider["calls"][0]
    if year < 2018:
        assert "/leagueHistory/53777071" in url
        assert kwargs["params"]["seasonId"] == str(year)
    else:
        assert "/seasons/2019/" in url


def test_public_archive_reuses_existing_route_switch(run_credential_step, provider):
    provider["responses"] = [(401, {}), (200, [public_league()])]
    assert run_credential_step()[0] == 0
    assert len(provider["calls"]) == 2
    assert "/seasons/2019/" in provider["calls"][0][0]
    assert "/leagueHistory/53777071" in provider["calls"][1][0]
    assert provider["calls"][1][1]["params"]["seasonId"] == "2019"
    assert all(call[1]["timeout"] == 7 for call in provider["calls"])


def test_public_proof_uses_requested_year_league_id(run_credential_step, provider):
    provider["responses"] = [(200, {**public_league(), "id": 12345})]
    code, _ = run_credential_step({"league_ids": {"2019": 12345, "2026": 67890}})
    assert code == 0
    assert len(provider["calls"]) == 1
    assert "/seasons/2019/segments/0/leagues/12345" in provider["calls"][0][0]


@pytest.mark.parametrize("stored_pair", ["missing", "partial"])
def test_verified_public_access_clears_partial_payload(run_credential_step, provider, stored_pair):
    provider["responses"] = [(200, public_league())]
    code, files = run_credential_step({"espn_s2": "test-cookie-s2"}, stored_pair=stored_pair)
    assert code == 0
    assert files == {".espn_s2": "", ".espn_swid": ""}
    assert len(provider["calls"]) == 1


@pytest.mark.parametrize("change", [
    {"id": 123}, {"id": None}, {"seasonId": 2020}, {"seasonId": None},
    {"settings": {}}, {"settings": []}, {"teams": []}, {"teams": {}},
    {"teams": [{}]}, {"teams": ["not-a-team"]},
])
def test_invalid_public_proof_fails_closed(run_credential_step, provider, change):
    provider["responses"] = [(200, {**public_league(), **change})]
    code, files = run_credential_step()
    assert code == 1
    assert files == {}
    assert len(provider["calls"]) == 1


@pytest.mark.parametrize("missing", ["id", "seasonId", "settings", "teams"])
def test_missing_public_proof_field_fails_closed(run_credential_step, provider, missing):
    payload = public_league()
    del payload[missing]
    provider["responses"] = [(200, payload)]
    assert run_credential_step()[0] == 1
    assert len(provider["calls"]) == 1


@pytest.mark.parametrize("reply", [
    (401, {}), (403, {}), (404, {}), (500, {}), (200, []),
    requests.Timeout("test timeout"), requests.ConnectionError("test connection error"),
])
def test_failed_public_probe_never_allows_import(run_credential_step, provider, reply):
    provider["responses"] = [reply]
    code, files = run_credential_step()
    assert code == 1
    assert files == {}
    assert provider["calls"]


@pytest.mark.parametrize("timeout", [None, 7])
def test_raw_client_timeout_default_is_preserved(provider, timeout):
    provider["responses"] = [(200, public_league())]
    kwargs = {} if timeout is None else {"request_timeout": timeout}
    client = ESPNAPIClient(53777071, **kwargs)
    assert client.get_raw_league(2019, ["mSettings", "mTeam"]) == public_league()
    assert provider["calls"][0][1]["timeout"] == (30 if timeout is None else 7)


@pytest.mark.parametrize("s2, swid, expected_exit", [
    ("test-cookie-s2", "test-cookie-swid", 0),
    ("test-cookie-s2", "", 19),
    ("", "test-cookie-swid", 19),
    ("test-cookie-s2", " \n", 19),
])
def test_payload_skip_requires_complete_pair(tmp_path, s2, swid, expected_exit):
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if not bash:
        pytest.skip("Bash is required to exercise the workflow shell guard")
    workflow = yaml.safe_load(
        (Path(__file__).parents[1] / ".github/workflows/espn_full_import_worker.yml").read_text(encoding="utf-8")
    )
    step = next(
        step for job in workflow["jobs"].values() for step in job["steps"]
        if step.get("name") == "Retrieve ESPN cookies"
    )
    guard = step["run"].split("python - <<'PYEOF'", 1)[0]
    (tmp_path / ".espn_s2").write_text(s2)
    (tmp_path / ".espn_swid").write_text(swid)
    result = subprocess.run(
        [bash, "--noprofile", "--norc", "-e"], input=guard + "exit 19\n",
        cwd=tmp_path, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode == expected_exit, result.stderr
    assert "test-cookie" not in result.stdout + result.stderr
