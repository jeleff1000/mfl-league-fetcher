import json
import time
import urllib.request
from io import BytesIO
from urllib.error import HTTPError

import pandas as pd
import pytest


def _opener_schedule(*, final: bool) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-10",
                "away_team": "NE",
                "away_score": 10 if final else None,
                "home_team": "SEA",
                "home_score": 13 if final else None,
            }
        ]
    )


def test_discovery_cli_emits_week_one_scope_for_september_tenth(monkeypatch, tmp_path):
    from scripts import discover_live_nfl_ops_refresh as discovery

    monkeypatch.setattr(discovery, "load_nflverse_schedule", lambda: _opener_schedule(final=True))
    output = tmp_path / "scope.json"

    assert discovery.main(["--season", "2026", "--game-date", "2026-09-10", "--output", str(output)]) == 0

    assert json.loads(output.read_text(encoding="utf-8")) == {
        "status": "ready",
        "game_date": "2026-09-10",
        "scope": {
            "year": 2026,
            "week": 1,
            "season_type": "REG",
            "game_date": "2026-09-10",
            "game_ids": ["2026_01_NE_SEA"],
        },
    }


def test_discovery_cli_emits_no_op_for_a_date_without_games(monkeypatch, tmp_path):
    from scripts import discover_live_nfl_ops_refresh as discovery

    monkeypatch.setattr(discovery, "load_nflverse_schedule", lambda: _opener_schedule(final=False))
    output = tmp_path / "scope.json"

    assert discovery.main(["--game-date", "2026-09-11", "--output", str(output)]) == 0

    assert json.loads(output.read_text(encoding="utf-8")) == {
        "status": "no-op",
        "game_date": "2026-09-11",
        "scope": None,
    }


@pytest.mark.parametrize(("status", "expected_attempts"), [(504, 2), (404, 1)])
def test_discovery_retries_only_transient_schedule_responses(
    monkeypatch, tmp_path, status, expected_attempts
):
    from scripts import discover_live_nfl_ops_refresh as discovery

    schedule_bytes = _opener_schedule(final=True).to_parquet(index=False)
    attempts = []

    def download(request, **kwargs):
        attempts.append(kwargs.get("timeout"))
        if len(attempts) == 1:
            url = getattr(request, "full_url", request)
            raise HTTPError(url, status, "fixture response", None, None)
        return BytesIO(schedule_bytes)

    monkeypatch.setattr(urllib.request, "urlopen", download)
    monkeypatch.setattr(time, "sleep", lambda _: None)
    output = tmp_path / "scope.json"
    args = ["--season", "2026", "--game-date", "2026-09-10", "--output", str(output)]

    if status == 504:
        assert discovery.main(args) == 0
        assert json.loads(output.read_text(encoding="utf-8"))["scope"]["game_ids"] == [
            "2026_01_NE_SEA"
        ]
        assert attempts == [30, 30]
    else:
        with pytest.raises(HTTPError, match="404"):
            discovery.main(args)
        assert not output.exists()
    assert len(attempts) == expected_attempts
