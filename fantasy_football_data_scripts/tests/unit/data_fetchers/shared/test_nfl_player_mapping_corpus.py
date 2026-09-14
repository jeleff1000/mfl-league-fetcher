import duckdb

from multi_league.data_fetchers.shared import nfl_player_mapping


def test_corpus_mode_loads_player_bio_from_local_ops_cache(tmp_path, monkeypatch):
    ops_path = tmp_path / "ops_cache.duckdb"
    conn = duckdb.connect(str(ops_path))
    conn.execute("CREATE SCHEMA nfl_historical")
    conn.execute(
        """
        CREATE TABLE nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            yahoo_player_id BIGINT,
            sleeper_player_id BIGINT,
            espn_id VARCHAR,
            headshot_url VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO nfl_historical.player_bio VALUES "
        "('00-1', 101, 202, '303', 'https://img.test/1.png')"
    )
    conn.close()

    monkeypatch.setenv("CORPUS_MODE", "1")
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_path))
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly-access-must-not-occur.invalid")
    for name in (
        "_BIO_YAHOO_MAP",
        "_BIO_SLEEPER_MAP",
        "_BIO_ESPN_MAP",
        "_BIO_HEADSHOT_MAP",
    ):
        monkeypatch.setattr(nfl_player_mapping, name, None)

    nfl_player_mapping._load_player_bio_maps()

    assert nfl_player_mapping._BIO_YAHOO_MAP == {"101": "00-1"}
    assert nfl_player_mapping._BIO_SLEEPER_MAP == {"202": "00-1"}
    assert nfl_player_mapping._BIO_ESPN_MAP == {"303": "00-1"}
    assert nfl_player_mapping._BIO_HEADSHOT_MAP == {"00-1": "https://img.test/1.png"}
