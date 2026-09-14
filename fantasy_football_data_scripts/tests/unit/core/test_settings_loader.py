import json

import polars as pl

from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.settings_loader import LeagueSettingsLoader


def test_settings_loader_prefers_local_duckdb_flat_settings_over_json(tmp_path):
    settings_dir = tmp_path / "league_settings"
    settings_dir.mkdir()
    (settings_dir / "league_settings_2025.json").write_text(
        json.dumps(
            {
                "year": 2025,
                "scoring_settings": {"rec": 1.0, "pass_td": 6.0},
                "roster_position_counts": {"QB": 2, "RB": 1, "WR": 2, "TE": 2, "BN": 8},
            }
        ),
        encoding="utf-8",
    )

    db = LocalLeagueDB(tmp_path, "kmffl")
    db.save_table(
        "league_settings",
        pl.DataFrame(
            {
                "year": [2025],
                "platform": ["yahoo"],
                "league_key": ["461.l.90939"],
                "scoring_rec": [0.5],
                "scoring_pass_td": [4.0],
                "roster_QB": [1],
                "roster_RB": [2],
                "roster_WR": [3],
                "roster_TE": [1],
                "roster_FLX": [1],
                "roster_BN": [6],
            }
        ),
        year=2025,
    )
    # Close the writer connection before the settings loader tries to open a
    # read-only connection to the same file (DuckDB file locking on Windows).
    db.close()

    loader = LeagueSettingsLoader(settings_dir, league_id="461.l.90939", db_name="kmffl")
    scoring_rules = loader.load_scoring_rules()
    roster_settings = loader.load_roster_settings()

    assert scoring_rules[2025]["scoring_settings"]["rec"] == 0.5
    assert scoring_rules[2025]["scoring_settings"]["pass_td"] == 4.0
    assert roster_settings[2025]["QB"] == 1
    assert roster_settings[2025]["RB"] == 2
    assert roster_settings[2025]["FLX"] == 1
    assert "BN" not in roster_settings[2025]


def test_settings_loader_falls_back_to_json_files(tmp_path):
    settings_dir = tmp_path / "league_settings"
    settings_dir.mkdir()
    (settings_dir / "league_settings_2024.json").write_text(
        json.dumps(
            {
                "year": 2024,
                "scoring_rec": 1.0,
                "scoring_pass_td": 6.0,
                "roster_position_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "BN": 7},
            }
        ),
        encoding="utf-8",
    )

    loader = LeagueSettingsLoader(settings_dir)
    scoring_rules = loader.load_scoring_rules()
    roster_settings = loader.load_roster_settings()

    assert scoring_rules[2024]["scoring_settings"]["rec"] == 1.0
    assert scoring_rules[2024]["scoring_settings"]["pass_td"] == 6.0
    assert roster_settings[2024]["QB"] == 1
    assert roster_settings[2024]["RB"] == 2
    assert "BN" not in roster_settings[2024]
