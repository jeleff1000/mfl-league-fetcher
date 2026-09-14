import pandas as pd


def test_live_ops_refresh_reads_the_year_scoped_nflverse_roster(monkeypatch):
    """The identity refresh must include active rookies, not only stat rows."""
    from scripts import refresh_live_nfl_ops
    from multi_league.data_fetchers import update_nfl_super_table

    expected_url = "https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_2026.csv"
    monkeypatch.setattr(refresh_live_nfl_ops.pd, "read_parquet", lambda _url: pd.DataFrame())
    monkeypatch.setattr(
        refresh_live_nfl_ops,
        "finalized_game_scope",
        lambda *_args, **_kwargs: pd.DataFrame([{"game_id": "2026_01_NE_SEA"}]),
    )
    monkeypatch.setattr(
        refresh_live_nfl_ops,
        "prepare_weekly_facts",
        lambda *_args, **_kwargs: pd.DataFrame([{"NFL_player_id": "00-0040888"}]),
    )
    monkeypatch.setattr(
        update_nfl_super_table,
        "fetch_and_combine_nfl_data",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    urls: list[str] = []
    monkeypatch.setattr(
        refresh_live_nfl_ops.pd,
        "read_csv",
        lambda url: urls.append(url) or pd.DataFrame([{"gsis_id": "00-0040888"}]),
    )

    _facts, _games, roster = refresh_live_nfl_ops.fetch_live_refresh_inputs(year=2026, week=1)

    assert urls == [expected_url]
    assert roster.to_dict("records") == [{"gsis_id": "00-0040888"}]
