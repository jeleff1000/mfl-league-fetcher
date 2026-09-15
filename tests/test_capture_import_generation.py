import pytest
import json

from scripts.capture_import_generation import parse_generation, read_generation, record_generation


def test_capture_import_generation_is_fail_closed_and_scope_specific(tmp_path):
    assert parse_generation("league_alpha", [{"generation": 0}]) == 0
    assert parse_generation("league_alpha", [{"generation": 42}]) == 42
    for rows in ([], [{"generation": None}], [{"generation": -1}], [{"generation": True}]):
        with pytest.raises(ValueError, match="generation"):
            parse_generation("league_alpha", rows)
    with pytest.raises(ValueError, match="db_name"):
        parse_generation("league_alpha'; DROP TABLE public.matchup; --", [{"generation": 1}])

    github_env = tmp_path / "github_env"
    github_output = tmp_path / "github_output"
    record_generation(42, github_env=github_env, github_output=github_output)
    assert github_env.read_text() == "LEAGUE_IMPORT_BASE_GENERATION=42\n"
    assert github_output.read_text() == "base_generation=42\n"


def test_capture_import_generation_reads_only_one_filtered_fly_league(monkeypatch):
    captured = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *unused):
            return False

        def read(self):
            return b'[{"generation": 2}]'

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data)
        captured["authorization"] = request.get_header("Authorization")
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    assert read_generation("afi_data", url="https://fly.example/", token="test-read") == 2
    assert captured["url"] == "https://fly.example/query"
    assert captured["body"]["database"] == "___leagues"
    assert "WHERE db_name = 'afi_data'" in captured["body"]["sql"]
    assert captured["authorization"] == "Bearer test-read"
    assert captured["timeout"] <= 20
