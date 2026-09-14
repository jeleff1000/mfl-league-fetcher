from __future__ import annotations

import duckdb

from scripts.refresh_yahoo_active_season import _active_source_snapshot_frames


class DuckDBReader:
    def __init__(self) -> None:
        self.connection = duckdb.connect(":memory:")
        self.connection.execute("CREATE SCHEMA public")

    def query(self, sql: str, *, database: str):
        assert database == "___leagues"
        cursor = self.connection.execute(sql)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def test_new_season_seeds_latest_settings_without_copying_prior_season_facts():
    reader = DuckDBReader()
    reader.connection.execute(
        "CREATE TABLE public.league_settings "
        "(db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR)"
    )
    reader.connection.execute(
        "CREATE TABLE public.player_fantasy "
        "(db_name VARCHAR, year INTEGER, week INTEGER, player VARCHAR)"
    )
    reader.connection.execute(
        "INSERT INTO public.league_settings VALUES "
        "('kmffl', 2024, 'yahoo', '449.l.1'), "
        "('kmffl', 2025, 'yahoo', '461.l.2')"
    )
    reader.connection.execute(
        "INSERT INTO public.player_fantasy VALUES ('kmffl', 2025, 17, 'Prior Player')"
    )

    frames = _active_source_snapshot_frames(
        reader,
        registry={
            "league_settings": {"columns": {"db_name", "year", "platform", "league_key"}},
            "player_fantasy": {"columns": {"db_name", "year", "week", "player"}},
        },
        db_name="kmffl",
        active_year=2026,
        table_names=("league_settings", "player_fantasy"),
    )

    assert frames["league_settings"][["db_name", "year", "league_key"]].to_dict("records") == [
        {"db_name": "kmffl", "year": 2025, "league_key": "461.l.2"}
    ]
    assert frames["player_fantasy"].empty

