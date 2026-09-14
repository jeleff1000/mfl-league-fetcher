import json

import pandas as pd


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

    monkeypatch.setattr(discovery.pd, "read_parquet", lambda _url: _opener_schedule(final=True))
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


def test_discovery_cli_emits_no_op_for_a_date_without_a_final_game(monkeypatch, tmp_path):
    from scripts import discover_live_nfl_ops_refresh as discovery

    monkeypatch.setattr(discovery.pd, "read_parquet", lambda _url: _opener_schedule(final=False))
    output = tmp_path / "scope.json"

    assert discovery.main(["--season", "2026", "--game-date", "2026-09-10", "--output", str(output)]) == 0

    assert json.loads(output.read_text(encoding="utf-8")) == {
        "status": "no-op",
        "game_date": "2026-09-10",
        "scope": None,
    }
