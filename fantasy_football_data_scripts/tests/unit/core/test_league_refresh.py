from __future__ import annotations

import json

import pandas as pd
import pytest


def test_provider_roster_merge_guard_rejects_silent_player_collapse():
    import duckdb

    from multi_league.core.league_refresh import RefreshScopeError, assert_provider_roster_merge

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.player_fantasy "
        "(db_name VARCHAR, year INTEGER, week INTEGER, sleeper_player_id VARCHAR)"
    )
    conn.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "('league_a', 2026, 1, '10'), ('league_a', 2026, 1, '20')"
    )

    class LocalDB:
        league_name = "league_a"

        @staticmethod
        def connect():
            return conn

    incoming = pd.DataFrame({"sleeper_player_id": ["10", "20", "20"]})
    assert_provider_roster_merge(
        LocalDB(),
        incoming,
        year=2026,
        week=1,
        provider_id_column="sleeper_player_id",
    )

    conn.execute("DELETE FROM public.player_fantasy WHERE sleeper_player_id = '20'")
    with pytest.raises(RefreshScopeError, match="preserved only 1/2 provider player IDs"):
        assert_provider_roster_merge(
            LocalDB(),
            incoming,
            year=2026,
            week=1,
            provider_id_column="sleeper_player_id",
        )
    conn.close()


def test_homepage_source_snapshot_uses_one_tagged_fly_read():
    from multi_league.core.homepage_refresh import _load_homepage_source_frames

    class Reader:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def query(self, sql, *, database):
            self.calls.append((sql, database))
            return [
                {
                    "source_table": "matchup",
                    "payload": json.dumps({"db_name": "league_a", "year": 2025, "week": 1}),
                },
                {
                    "source_table": "league_context",
                    "payload": json.dumps({"db_name": "league_a", "platform": "yahoo"}),
                },
            ]

    reader = Reader()
    frames = _load_homepage_source_frames(reader, "league_a")

    assert len(reader.calls) == 1
    assert reader.calls[0][1] == "___leagues"
    assert "UNION ALL" in reader.calls[0][0]
    assert frames["matchup"].to_dict("records") == [
        {"db_name": "league_a", "year": 2025, "week": 1}
    ]
    assert frames["league_context"].to_dict("records") == [
        {"db_name": "league_a", "platform": "yahoo"}
    ]


def test_homepage_snapshot_overlays_new_active_season_before_atomic_publish():
    import duckdb

    from multi_league.core.homepage_refresh import _overlay_active_source_frames

    frames = {
        "matchup": pd.DataFrame(
            [
                {"db_name": "league_a", "year": 2025, "week": 17, "manager": "Old"},
                {"db_name": "league_a", "year": 2026, "week": 1, "manager": "Stale"},
            ]
        ),
        "league_context": pd.DataFrame(
            [{"db_name": "league_a", "platform": "yahoo"}]
        ),
    }
    local = duckdb.connect(":memory:")
    local.execute("CREATE SCHEMA public")
    local.execute(
        "CREATE TABLE public.matchup AS SELECT * FROM "
        "(VALUES ('league_a', 2026, 1, 'Fresh')) t(db_name, year, week, manager)"
    )
    try:
        actual = _overlay_active_source_frames(
            frames,
            local,
            db_name="league_a",
            active_year=2026,
        )
    finally:
        local.close()

    assert actual["matchup"].sort_values("year").to_dict("records") == [
        {"db_name": "league_a", "year": 2025, "week": 17, "manager": "Old"},
        {"db_name": "league_a", "year": 2026, "week": 1, "manager": "Fresh"},
    ]
    assert actual["league_context"].to_dict("records") == [
        {"db_name": "league_a", "platform": "yahoo"}
    ]


def test_homepage_profile_refresh_replaces_only_active_franchises():
    from multi_league.core.homepage_refresh import _merge_active_manager_profiles

    existing = pd.DataFrame([
        {"db_name": "league_a", "franchise_id": "active", "manager": "Old Active", "wins": 10},
        {"db_name": "league_a", "franchise_id": "retired", "manager": "Retired", "wins": 90},
    ])
    refreshed = pd.DataFrame([
        {"franchise_id": "active", "manager": "Current Active", "wins": 11},
    ])

    actual = _merge_active_manager_profiles(existing, refreshed, {"active"})

    assert actual.sort_values("franchise_id").to_dict("records") == [
        {"franchise_id": "active", "manager": "Current Active", "wins": 11},
        {"franchise_id": "retired", "manager": "Retired", "wins": 90},
    ]


def test_incremental_homepage_refresh_preserves_existing_historical_highlights():
    """A partial active-season source cannot erase established all-time facts."""
    from multi_league.core.homepage_refresh import _preserve_existing_summary_values

    existing = pd.DataFrame([{
        "db_name": "league_a",
        "data_year": 2025,
        "best_game_clutch_player": "Historical Hero",
        "best_game_clutch_value": 31.25,
    }])
    refreshed = pd.DataFrame([{
        "data_year": 2026,
        "best_game_clutch_player": None,
        "best_game_clutch_value": pd.NA,
    }])

    actual = _preserve_existing_summary_values(existing, refreshed)

    assert actual.to_dict("records") == [{
        "data_year": 2026,
        "best_game_clutch_player": "Historical Hero",
        "best_game_clutch_value": 31.25,
    }]


def test_refresh_window_rechecks_partial_current_week_and_collects_all_later_final_weeks():
    """A rerun overlaps the newest published week for stat corrections.

    A league that stopped after Week 1 must pick up Weeks 2--4, while a
    partially published Week 1 remains eligible for the corrected final-game
    snapshot.  This is the incremental boundary used by every platform
    adapter; it must not be a fixed one-week worker.
    """
    from multi_league.core.league_refresh import completed_weeks_to_refresh

    assert completed_weeks_to_refresh(
        finalized_weeks=[1, 2, 3, 4],
        last_materialized_week=1,
    ) == [1, 2, 3, 4]


def test_provider_week_scope_never_expands_a_finalized_game_refresh_to_a_full_season():
    """The active-season adapters receive the exact completed week set."""
    from multi_league.core.league_refresh import provider_weeks_to_fetch

    assert provider_weeks_to_fetch(max_week=18, requested_weeks=[1, "3", 0, 19, 3]) == [1, 3]


def test_finalized_game_filter_keeps_only_roster_rows_from_completed_games():
    """Do not turn unplayed Yahoo roster slots into zero-point final rows."""
    from multi_league.core.league_refresh import filter_rosters_to_finalized_games

    rosters = pd.DataFrame(
        [
            {"player_id": "p_ne", "nfl_team": "NE", "week": 1},
            {"player_id": "p_sea", "nfl_team": "SEA", "week": 1},
            {"player_id": "p_mia", "nfl_team": "MIA", "week": 1},
        ]
    )
    # Ops uses the historical Stathead code NWE; the final-game boundary must
    # normalize it before comparing it to Yahoo's current NE code.
    finalized_ops = pd.DataFrame(
        [
            {"nfl_team": "NWE", "opponent_nfl_team": "SEA"},
            {"nfl_team": "SEA", "opponent_nfl_team": "NWE"},
        ]
    )

    actual = filter_rosters_to_finalized_games(rosters, finalized_ops)

    assert actual["player_id"].tolist() == ["p_ne", "p_sea"]


def test_finalized_game_filter_rejects_an_ops_payload_without_team_identity():
    from multi_league.core.league_refresh import RefreshScopeError, filter_rosters_to_finalized_games

    rosters = pd.DataFrame([{"player_id": "p_ne", "nfl_team": "NE", "week": 1}])
    incomplete_ops = pd.DataFrame([{"nfl_team": "NWE"}])

    with pytest.raises(RefreshScopeError, match="opponent_nfl_team"):
        filter_rosters_to_finalized_games(rosters, incomplete_ops)


def test_finalized_matchup_filter_excludes_live_scoreboard_rows_without_a_winner():
    """Partial Yahoo scores must never become final wins or standings rows."""
    from multi_league.core.league_refresh import filter_matchups_to_final_results

    scoreboards = pd.DataFrame(
        [
            {"manager": "Final winner", "winner_team_key": "470.l.80971.t.1"},
            {"manager": "Final loser", "winner_team_key": "470.l.80971.t.1"},
            {"manager": "Live leader", "winner_team_key": None},
            {"manager": "Live trailer", "winner_team_key": ""},
        ]
    )

    actual = filter_matchups_to_final_results(scoreboards)

    assert actual["manager"].tolist() == ["Final winner", "Final loser"]


def test_finalized_matchup_filter_excludes_espn_undecided_matchups():
    """ESPN sends partial scoring periods as ``UNDECIDED`` outcomes."""
    from multi_league.core.league_refresh import filter_matchups_to_final_results

    matchups = pd.DataFrame(
        [
            {"manager": "Final winner", "winner": "HOME"},
            {"manager": "Final loser", "winner": "HOME"},
            {"manager": "Live leader", "winner": "UNDECIDED"},
            {"manager": "Live trailer", "winner": None},
        ]
    )

    actual = filter_matchups_to_final_results(matchups, winner_column="winner")

    assert actual["manager"].tolist() == ["Final winner", "Final loser"]


def test_espn_schedule_gate_requires_every_matchup_to_have_a_final_outcome():
    """One unresolved ESPN pair means the whole fantasy scoring period is live."""
    from multi_league.core.league_refresh import espn_schedule_is_final

    assert not espn_schedule_is_final([{"winner": "HOME"}, {"winner": "UNDECIDED"}])
    assert espn_schedule_is_final([{"winner": "HOME"}, {"winner": "AWAY"}, {"winner": "TIE"}])


def test_espn_schedule_gate_rejects_a_missing_final_pair_despite_other_winners():
    from multi_league.core.league_refresh import espn_schedule_is_final

    raw = [
        {"home": {"teamId": pair}, "away": {"teamId": pair + 1},
         "winner": "HOME", "matchupPeriodId": 1, "playoffTierType": "NONE"}
        for pair in (1, 3, 5, 7, 9)
    ]
    assert not espn_schedule_is_final(raw, expected_team_ids=tuple(str(i) for i in range(1, 13)))


def test_espn_schedule_gate_rejects_duplicated_final_pair():
    from multi_league.core.league_refresh import espn_schedule_is_final

    pair = {
        "home": {"teamId": 10, "totalPoints": 152.66},
        "away": {"teamId": 3, "totalPoints": 129.66},
        "winner": "HOME", "playoffTierType": "NONE",
    }
    assert not espn_schedule_is_final([pair, pair], expected_team_ids=("10", "3"))


def test_espn_declared_playoff_byes_do_not_hold_a_complete_final_week_forever():
    from multi_league.core.league_refresh import espn_schedule_is_final

    raw = [
        {"home": {"teamId": team}, "away": None, "winner": "UNDECIDED",
         "matchupPeriodId": 15, "playoffTierType": "WINNERS_BRACKET"}
        for team in (3, 12)
    ] + [
        {"home": {"teamId": pair}, "away": {"teamId": pair + 1},
         "winner": "HOME", "matchupPeriodId": 15, "playoffTierType": "WINNERS_BRACKET"}
        for pair in (1, 5, 7, 9, 10)
    ]
    # The real ESPN graph is one row per team, with playoff byes represented
    # by a single home team and no away team.
    raw[-1]["home"]["teamId"] = 4
    raw[-1]["away"]["teamId"] = 11
    assert espn_schedule_is_final(raw, expected_team_ids=tuple(str(i) for i in range(1, 13)))


def test_espn_regular_row_missing_away_team_is_not_a_declared_bye():
    from multi_league.core.league_refresh import espn_schedule_is_final

    assert not espn_schedule_is_final([
        {"home": {"teamId": 1}, "away": None, "winner": "UNDECIDED",
         "matchupPeriodId": 1, "playoffTierType": "NONE"},
    ], expected_team_ids=("1",))

    assert not espn_schedule_is_final([
        {"home": {"teamId": 1}, "away": None, "winner": "UNDECIDED",
         "matchupPeriodId": 1, "playoffTierType": "UNKNOWN"},
    ], expected_team_ids=("1",))


def test_active_player_bio_cache_sync_updates_only_fetched_sleeper_identities(tmp_path):
    """Active workers see new roster identities without reloading the full bio table."""
    import duckdb

    from multi_league.core.league_refresh import active_platform_player_ids, sync_player_bio_cache_from_fly

    league = duckdb.connect()
    league.execute("CREATE TABLE draft (sleeper_player_id VARCHAR)")
    league.execute("INSERT INTO draft VALUES ('13302'), ('13302'), ('not-a-player')")
    assert active_platform_player_ids(league, platform="sleeper") == {"13302"}

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, sleeper_player_id DOUBLE, espn_id VARCHAR)"
    )
    cache.execute("INSERT INTO nfl_historical.player_bio VALUES ('00-old', 'Old Player', 'WR', 1, '1')")
    cache.close()

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query_df(self, sql, *, database):
            assert database == "___ops"
            self.sql.append(sql)
            return pd.DataFrame(
                [
                    {
                        "NFL_player_id": "00-0040888",
                        "player": "Adam Randall",
                        "nfl_position": "WR",
                        "sleeper_player_id": 13302,
                        "espn_id": "4685526",
                    }
                ]
            )

    reader = Reader()
    receipt = sync_player_bio_cache_from_fly(
        reader,
        ops_cache=cache_path,
        platform="sleeper",
        provider_ids={"13302"},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 1}
    assert "TRY_CAST(sleeper_player_id AS BIGINT) IN (13302)" in reader.sql[0]
    updated = duckdb.connect(str(cache_path), read_only=True)
    try:
        assert updated.execute(
            "SELECT NFL_player_id, player, sleeper_player_id FROM nfl_historical.player_bio ORDER BY NFL_player_id"
        ).fetchall() == [("00-0040888", "Adam Randall", 13302.0), ("00-old", "Old Player", 1.0)]
    finally:
        updated.close()


def test_active_player_id_resolution_uses_synced_sleeper_id_despite_name_alias():
    """Current-season provider identity outranks a harmless display-name alias."""
    import duckdb

    from multi_league.core.league_refresh import resolve_active_player_nfl_ids_from_bio

    league = duckdb.connect()
    league.execute("CREATE SCHEMA public")
    league.execute(
        "CREATE TABLE public.player_fantasy ("
        "db_name VARCHAR, year INTEGER, sleeper_player_id VARCHAR, "
        "NFL_player_id VARCHAR, player VARCHAR)"
    )
    league.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "('franchise_mode_fantasy', 2026, '7670', NULL, 'Joshua Palmer'), "
        "('franchise_mode_fantasy', 2025, '7670', NULL, 'Joshua Palmer'), "
        "('franchise_mode_fantasy', 2026, '9999', NULL, 'Reused ID')"
    )
    league.execute("ATTACH ':memory:' AS ___ops")
    league.execute("CREATE SCHEMA ___ops.nfl_historical")
    league.execute(
        "CREATE TABLE ___ops.nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, sleeper_player_id DOUBLE)"
    )
    league.execute(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES "
        "('00-0036988', 'Josh Palmer', 7670), "
        "('00-a', 'First Reuse', 9999), ('00-b', 'Second Reuse', 9999)"
    )

    changed = resolve_active_player_nfl_ids_from_bio(
        league,
        db_name="franchise_mode_fantasy",
        active_year=2026,
        platform="sleeper",
    )

    assert changed == 1
    assert league.execute(
        "SELECT year, sleeper_player_id, NFL_player_id "
        "FROM public.player_fantasy ORDER BY year, sleeper_player_id"
    ).fetchall() == [
        (2025, "7670", None),
        (2026, "7670", "00-0036988"),
        (2026, "9999", None),
    ]


def test_active_player_bio_cache_sync_skips_fly_when_every_provider_id_is_cached(tmp_path):
    """A repeated active refresh must reuse complete local player-bio mappings."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, sleeper_player_id DOUBLE)"
    )
    cache.execute(
        "INSERT INTO nfl_historical.player_bio VALUES ('00-0040888', 'Adam Randall', 'WR', 13302)"
    )
    cache.close()

    class Reader:
        def query_df(self, sql, *, database):
            raise AssertionError("a complete local provider-ID cache must not read Fly")

    receipt = sync_player_bio_cache_from_fly(
        Reader(),
        ops_cache=cache_path,
        platform="sleeper",
        provider_ids={"13302"},
        player_names={"Adam Randall"},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 0}


def test_active_player_bio_cache_sync_fetches_an_active_name_missing_from_cached_ids(tmp_path):
    """A cached roster must not hide an unmapped active player from rank enrichment."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, yahoo_player_id DOUBLE)"
    )
    cache.execute(
        "INSERT INTO nfl_historical.player_bio VALUES ('00-0040888', 'Cached Player', 'WR', 13302)"
    )
    cache.close()

    class Reader:
        def query_df(self, sql, *, database):
            assert database == "___ops"
            assert "LOWER(TRIM(player)) IN ('eli raridon')" in sql
            assert "cached player" not in sql
            return pd.DataFrame(
                [{
                    "NFL_player_id": "00-0041395",
                    "player": "Eli Raridon",
                    "nfl_position": "TE",
                    "yahoo_player_id": None,
                }]
            )

    receipt = sync_player_bio_cache_from_fly(
        Reader(),
        ops_cache=cache_path,
        platform="yahoo",
        provider_ids={"13302"},
        player_names={"Cached Player", "Eli Raridon"},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 1}
    with duckdb.connect(str(cache_path), read_only=True) as check:
        assert check.execute(
            "SELECT nfl_position FROM nfl_historical.player_bio WHERE NFL_player_id = '00-0041395'"
        ).fetchone() == ("TE",)


def test_active_player_bio_cache_sync_fetches_an_active_nfl_id_without_provider_identity(tmp_path):
    """An already-resolved active player must not depend on a platform ID or name column."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, espn_id VARCHAR)"
    )
    cache.close()

    class Reader:
        def query_df(self, sql, *, database):
            assert database == "___ops"
            assert "NFL_player_id IN ('00-0037256')" in sql
            return pd.DataFrame(
                [{
                    "NFL_player_id": "00-0037256",
                    "player": "Rhamondre Stevenson",
                    "nfl_position": "RB",
                    "espn_id": None,
                }]
            )

    receipt = sync_player_bio_cache_from_fly(
        Reader(),
        ops_cache=cache_path,
        platform="espn",
        provider_ids=set(),
        player_names=set(),
        nfl_player_ids={"00-0037256"},
    )

    assert receipt == {"provider_ids": 0, "player_bio_rows": 1}


def test_active_player_bio_cache_sync_normalizes_integer_looking_espn_ids_before_cache_lookup(tmp_path):
    """ESPN's float-shaped frame IDs must hit their integer-shaped bio mappings."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, espn_id VARCHAR)"
    )
    cache.execute(
        "INSERT INTO nfl_historical.player_bio VALUES ('00-0040888', 'Adam Randall', 'WR', '4685526')"
    )
    cache.close()

    class Reader:
        def query_df(self, sql, *, database):
            raise AssertionError("a normalized cached ESPN ID must not read Fly")

    receipt = sync_player_bio_cache_from_fly(
        Reader(),
        ops_cache=cache_path,
        platform="espn",
        provider_ids={"4685526.0"},
        player_names={"Adam Randall"},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 0}


def test_active_player_bio_cache_sync_scopes_name_hints_to_the_missing_provider_id(tmp_path):
    """One new ESPN ID must not re-fetch every already-cached active player name."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, espn_id VARCHAR)"
    )
    cache.execute(
        "INSERT INTO nfl_historical.player_bio VALUES ('00-existing', 'Existing Player', 'WR', '1')"
    )
    cache.close()

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query_df(self, sql, *, database):
            assert database == "___ops"
            self.sql.append(sql)
            return pd.DataFrame(
                [
                    {
                        "NFL_player_id": "00-new",
                        "player": "New Player",
                        "nfl_position": "RB",
                        "espn_id": "2",
                    }
                ]
            )

    reader = Reader()
    receipt = sync_player_bio_cache_from_fly(
        reader,
        ops_cache=cache_path,
        platform="espn",
        provider_ids={"1", "2"},
        player_names={"Existing Player", "New Player"},
        provider_name_hints={"1": {"Existing Player"}, "2": {"New Player"}},
    )

    assert receipt == {"provider_ids": 2, "player_bio_rows": 1}
    assert "espn_id IN ('2')" in reader.sql[0]
    assert "'new player'" in reader.sql[0]
    assert "'existing player'" not in reader.sql[0]
    updated = duckdb.connect(str(cache_path), read_only=True)
    try:
        assert updated.execute(
            "SELECT NFL_player_id, espn_id FROM nfl_historical.player_bio ORDER BY NFL_player_id"
        ).fetchall() == [("00-existing", "1"), ("00-new", "2")]
    finally:
        updated.close()


def test_active_player_bio_cache_sync_fills_an_empty_espn_id_from_one_unique_cached_name(tmp_path):
    """A uniquely mapped player name can safely avoid a redundant Fly round trip."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, espn_id VARCHAR)"
    )
    cache.execute(
        "INSERT INTO nfl_historical.player_bio VALUES ('00-0040878', 'Mike Washington Jr.', 'WR', NULL)"
    )
    cache.close()

    class Reader:
        def query_df(self, sql, *, database):
            raise AssertionError("a unique empty local ESPN mapping must not read Fly")

    receipt = sync_player_bio_cache_from_fly(
        Reader(),
        ops_cache=cache_path,
        platform="espn",
        provider_ids={"4686658"},
        player_names={"Mike Washington Jr."},
        provider_name_hints={"4686658": {"Mike Washington Jr."}},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 0}
    updated = duckdb.connect(str(cache_path), read_only=True)
    try:
        assert updated.execute(
            "SELECT NFL_player_id, espn_id FROM nfl_historical.player_bio"
        ).fetchall() == [("00-0040878", "4686658")]
    finally:
        updated.close()


def test_active_platform_player_name_hints_keep_each_provider_id_paired_to_its_player():
    """A bounded active refresh needs names only for the provider IDs it missed."""
    import duckdb

    from multi_league.core.league_refresh import active_platform_player_name_hints

    league = duckdb.connect()
    league.execute("CREATE TABLE player_fantasy (espn_player_id VARCHAR, player VARCHAR)")
    league.execute(
        "INSERT INTO player_fantasy VALUES ('1.0', 'Existing Player'), ('2', 'New Player'), ('2', 'New Player')"
    )
    league.execute("CREATE TABLE draft (espn_player_id VARCHAR, player VARCHAR)")
    league.execute("INSERT INTO draft VALUES ('3', 'Draft Player')")

    assert active_platform_player_name_hints(league, platform="espn") == {
        "1.0": {"Existing Player"},
        "2": {"New Player"},
        "3": {"Draft Player"},
    }


def test_active_player_bio_cache_sync_uses_name_hints_for_new_yahoo_identities(tmp_path):
    """A new Yahoo ID can resolve from the current roster-backed bio by name."""
    import duckdb

    from multi_league.core.league_refresh import sync_player_bio_cache_from_fly

    cache_path = tmp_path / "ops_cache.duckdb"
    cache = duckdb.connect(str(cache_path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute(
        "CREATE TABLE nfl_historical.player_bio ("
        "NFL_player_id VARCHAR, player VARCHAR, nfl_position VARCHAR, yahoo_player_id DOUBLE)"
    )
    cache.close()

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query_df(self, sql, *, database):
            assert database == "___ops"
            self.sql.append(sql)
            return pd.DataFrame(
                [
                    {
                        "NFL_player_id": "00-0040888",
                        "player": "Adam Randall",
                        "nfl_position": "RB",
                        "yahoo_player_id": None,
                    }
                ]
            )

    reader = Reader()
    receipt = sync_player_bio_cache_from_fly(
        reader,
        ops_cache=cache_path,
        platform="yahoo",
        provider_ids={"99999"},
        player_names={"Adam Randall"},
    )

    assert receipt == {"provider_ids": 1, "player_bio_rows": 1}
    assert "TRY_CAST(yahoo_player_id AS BIGINT) IN (99999)" in reader.sql[0]
    assert "LOWER(TRIM(player)) IN ('adam randall')" in reader.sql[0]


def test_local_refresh_hydration_keeps_existing_history_for_the_target_league(tmp_path):
    """Career rebuilds need the complete, pre-refresh source history locally."""
    from multi_league.core.league_refresh import hydrate_local_refresh_sources
    from multi_league.core.local_db import LocalLeagueDB

    source_frames = {
        "matchup": pd.DataFrame(
            [
                {"db_name": "kmffl", "year": 2025, "week": 17, "manager": "Joe"},
                {"db_name": "kmffl", "year": 2026, "week": 1, "manager": "Joe"},
            ]
        ),
        "league_context": pd.DataFrame(
            [{"db_name": "kmffl", "platform": "yahoo", "league_id": "461.l.90939", "league_name": "KMFFL"}]
        ),
    }
    local = LocalLeagueDB(tmp_path, "kmffl")
    try:
        hydrated = hydrate_local_refresh_sources(local, source_frames, db_name="kmffl")
        assert hydrated == {"league_context": 1, "matchup": 2}
        assert local.connect().execute("SELECT year, week, manager FROM public.matchup ORDER BY year").fetchall() == [
            (2025, 17, "Joe"),
            (2026, 1, "Joe"),
        ]
    finally:
        local.close()


def test_local_refresh_hydration_rejects_a_cross_league_frame(tmp_path):
    """A bad Fly read must not leak another league into a scoped publish bundle."""
    from multi_league.core.league_refresh import RefreshScopeError, hydrate_local_refresh_sources
    from multi_league.core.local_db import LocalLeagueDB

    local = LocalLeagueDB(tmp_path, "kmffl")
    try:
        with pytest.raises(RefreshScopeError, match="unexpected db_name"):
            hydrate_local_refresh_sources(
                local,
                {"matchup": pd.DataFrame([{"db_name": "other", "year": 2026, "week": 1}])},
                db_name="kmffl",
            )
    finally:
        local.close()


def test_local_refresh_hydration_rejects_overlapping_active_platform_rows(tmp_path):
    """An active segment conflict is ambiguous, not permission to erase history."""
    from multi_league.core.league_refresh import RefreshScopeError, hydrate_local_refresh_sources
    from multi_league.core.local_db import LocalLeagueDB

    source_frames = {
        "transactions": pd.DataFrame(
            [
                {"db_name": "the_league", "year": 2025, "platform": "sleeper", "manager": "Old"},
                {"db_name": "the_league", "year": 2026, "platform": "sleeper", "manager": "Stale"},
                {"db_name": "the_league", "year": 2026, "platform": "yahoo", "manager": "Current"},
                {"db_name": "the_league", "year": 2026, "platform": None, "manager": "Derived"},
            ]
        )
    }
    local = LocalLeagueDB(tmp_path, "the_league")
    try:
        with pytest.raises(RefreshScopeError, match="overlapping active platform"):
            hydrate_local_refresh_sources(
                local, source_frames, db_name="the_league",
                active_year=2026, expected_platform="yahoo",
            )
    finally:
        local.close()


def test_local_refresh_hydration_preserves_disjoint_platform_chain_years(tmp_path):
    from multi_league.core.league_refresh import hydrate_local_refresh_sources
    from multi_league.core.local_db import LocalLeagueDB

    source_frames = {"transactions": pd.DataFrame([
        {"db_name": "mixed_league", "year": 2025, "platform": "yahoo", "manager": "Old"},
        {"db_name": "mixed_league", "year": 2026, "platform": "sleeper", "manager": "Current"},
        {"db_name": "mixed_league", "year": 2026, "platform": None, "manager": "Derived"},
    ])}
    local = LocalLeagueDB(tmp_path, "mixed_league")
    try:
        assert hydrate_local_refresh_sources(
            local, source_frames, db_name="mixed_league",
            active_year=2026, expected_platform="sleeper",
        ) == {"transactions": 3}
        assert local.connect().execute(
            "SELECT year, platform, manager FROM public.transactions ORDER BY year, manager"
        ).fetchall() == [
            (2025, "yahoo", "Old"),
            (2026, "sleeper", "Current"),
            (2026, None, "Derived"),
        ]
    finally:
        local.close()


def test_weekly_publish_selects_source_and_rebuilt_homepage_tables():
    """The publish bundle includes provider data and rebuilt homepage output."""
    import duckdb

    from multi_league.core.league_refresh import active_refresh_publish_tables

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, manager_week VARCHAR)")
        conn.execute("CREATE TABLE public.keeper_config (db_name VARCHAR, year INTEGER)")
        conn.execute("CREATE TABLE public.homepage_league_summary (db_name VARCHAR, league_name VARCHAR)")
        conn.execute("CREATE TABLE public.manager_overrides (db_name VARCHAR, id INTEGER)")

        assert active_refresh_publish_tables(conn) == ["homepage_league_summary", "matchup"]
        assert active_refresh_publish_tables(
            conn, publication_schema_version="fleet-partition-v3",
        ) == ["matchup"]
    finally:
        conn.close()


def test_weekly_publish_excludes_scratch_careers_rebuilt_on_fly():
    import duckdb

    from multi_league.core.league_refresh import active_refresh_publish_tables

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.player_fantasy_career (db_name VARCHAR, games_rostered INTEGER)")
        conn.execute("CREATE TABLE public.matchup_career (db_name VARCHAR, games INTEGER)")
        conn.execute("CREATE TABLE public.league_context (db_name VARCHAR, league_name VARCHAR)")
        conn.execute("CREATE TABLE public.manager_overrides (db_name VARCHAR, id INTEGER)")

        assert active_refresh_publish_tables(conn) == []
    finally:
        conn.close()


def test_homepage_frames_are_written_to_a_separate_local_bundle(monkeypatch):
    """Whole-history homepage results are materialized locally for atomic Fleet publish."""
    import duckdb

    from multi_league.core import homepage_refresh

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    frames = {
        "homepage_league_summary": pd.DataFrame(
            [{"db_name": "kmffl", "league_name": "KMFFL"}]
        )
    }
    calls: list[tuple[str, str, int]] = []

    monkeypatch.setattr(
        homepage_refresh,
        "replace_scoped_aggregate_table_from_dataframe",
        lambda target, db_name, table_name, frame: calls.append((db_name, table_name, len(frame))),
    )

    counts = homepage_refresh.write_homepage_frames(conn, "kmffl", frames)

    assert counts == {"homepage_league_summary": 1}
    assert calls == [("kmffl", "homepage_league_summary", 1)]
    conn.close()


def test_homepage_refresh_uses_skinny_player_history_and_preserves_empty_schemas():
    """The homepage pass must not pull the wide weekly-player table or fail on empty sources."""
    from multi_league.core.homepage_refresh import _load_homepage_source_frames

    class EmptyReader:
        sql: list[str] = []

        def query(self, sql, *, database):
            assert database == "___leagues"
            self.sql.append(sql)
            return []

    reader = EmptyReader()
    frames = _load_homepage_source_frames(reader, "empty_league")

    assert len(reader.sql) == 1
    assert "SELECT db_name, player, manager" in reader.sql[0]
    assert "json_group_array" in reader.sql[0]
    for table_name in ("matchup", "draft", "transactions", "player_fantasy_season"):
        assert f"SELECT * FROM public.{table_name}" not in reader.sql[0]
    assert "LOWER(TRIM(COALESCE(managers" in reader.sql[0]
    assert "SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year, franchise_id" in reader.sql[0]
    assert "FROM public.player_fantasy WHERE db_name" in reader.sql[0]
    assert "franchise_id IS NOT NULL" in reader.sql[0]
    assert "FROM (SELECT * FROM public.player_fantasy WHERE" not in reader.sql[0]
    assert set(frames["player_fantasy"].columns) >= {
        "franchise_id",
        "player_week",
        "manager_lamar",
        "is_playoffs",
        "is_consolation",
    }
    assert "db_name" in frames["transactions"].columns
    assert "headshot_url" in frames["player_bio"].columns
    assert list(frames["active_franchises"].columns) == ["db_name", "year", "franchise_id"]


def test_active_profile_scope_includes_every_active_franchise_not_only_started_players():
    """Bench-only managers still need their homepage profile preserved/refreshed."""
    import duckdb

    from multi_league.core.homepage_refresh import _active_franchise_ids

    snapshot = pd.DataFrame(
        [
            {"db_name": "league_a", "year": 2026, "franchise_id": "started"},
            {"db_name": "league_a", "year": 2025, "franchise_id": "retired"},
        ]
    )
    local = duckdb.connect(":memory:")
    local.execute("CREATE SCHEMA public")
    local.execute(
        "CREATE TABLE public.player_fantasy AS SELECT * FROM (VALUES "
        "('league_a', 2026, 'started', 1), "
        "('league_a', 2026, 'bench_only', 0)) "
        "t(db_name, year, franchise_id, is_started)"
    )
    try:
        actual = _active_franchise_ids(
            snapshot,
            active_year=2026,
            active_source=local,
            db_name="league_a",
        )
    finally:
        local.close()

    assert actual == {"started", "bench_only"}


def test_shared_homepage_prepare_writes_into_the_existing_atomic_bundle(monkeypatch):
    """All three platforms prepare homepage rows without a second Fly publish."""
    from multi_league.core import homepage_refresh

    frames = {"homepage_league_summary": pd.DataFrame([{"db_name": "kmffl"}])}
    captured = {}

    class Reader:
        pass

    class Local:
        def connect(self):
            return "active-source"

    def compute(reader, db_name, *, active_source, active_year):
        captured.update(
            reader=reader,
            db_name=db_name,
            active_source=active_source,
            active_year=active_year,
        )
        return frames

    monkeypatch.setattr(homepage_refresh, "compute_homepage_frames_from_fly", compute)
    monkeypatch.setattr(
        homepage_refresh,
        "write_homepage_frames",
        lambda *_args: {"homepage_league_summary": 1},
    )

    reader = Reader()
    result = homepage_refresh.prepare_homepage_refresh(
        reader=reader,
        local_db=Local(),
        db_name="kmffl",
        active_year=2026,
    )

    assert captured.pop("reader") is reader
    assert captured == {
        "db_name": "kmffl",
        "active_source": "active-source",
        "active_year": 2026,
    }
    assert result == {
        "published_tables": ["homepage_league_summary"],
        "rows": {"homepage_league_summary": 1},
    }


def test_weekly_refresh_fetches_a_draft_only_when_the_active_season_has_none():
    """A completed draft is immutable and should not dominate a weekly refresh."""
    from multi_league.core.league_refresh import needs_active_season_draft_fetch

    class _Local:
        def __init__(self, exists: bool, rows: int) -> None:
            self.exists = exists
            self.rows = rows

        def table_exists(self, table: str) -> bool:
            assert table == "draft"
            return self.exists

        def row_count(self, table: str) -> int:
            assert table == "draft"
            return self.rows

    assert needs_active_season_draft_fetch(_Local(False, 0))
    assert needs_active_season_draft_fetch(_Local(True, 0))
    assert not needs_active_season_draft_fetch(_Local(True, 180))


def test_draft_manifest_detects_a_provider_pick_missing_from_hydrated_draft():
    """A nonempty local draft is still incomplete when a provider pick is absent.

    Removing pick 3 from the local frame must cause the refresh to request the
    complete provider payload; treating row count alone as completeness would
    leave that manager's draft score permanently absent.
    """
    from multi_league.core.league_refresh import missing_provider_draft_keys

    hydrated = pd.DataFrame(
        [
            {"draft_id": "primary", "pick": 1},
            {"draft_id": "primary", "pick": 2},
            {"draft_id": "primary", "pick": 4},
        ]
    )
    provider = pd.DataFrame(
        [
            {"draft_id": "primary", "pick": 1},
            {"draft_id": "primary", "pick": 2},
            {"draft_id": "primary", "pick": 3},
            {"draft_id": "primary", "pick": 4},
        ]
    )

    assert missing_provider_draft_keys(
        hydrated,
        provider,
        key_columns=("draft_id", "pick"),
    ) == {("primary", "3")}


def test_draft_manifest_requires_an_exact_local_key_set():
    """Duplicates or stale extra provider keys require authoritative recovery."""
    from multi_league.core.league_refresh import provider_draft_manifest_matches

    provider = pd.DataFrame(
        [
            {"round": 1, "pick": 1},
            {"round": 1, "pick": 2},
        ]
    )
    exact = pd.DataFrame(
        [
            {"round": 1, "pick": 1, "draft_id": "canonical"},
            {"round": 1, "pick": 2, "draft_id": "canonical"},
        ]
    )
    duplicate = pd.concat(
        [
            exact,
            pd.DataFrame([{"round": 1, "pick": 1, "draft_id": "stale"}]),
        ],
        ignore_index=True,
    )
    unexpected = pd.concat(
        [
            exact,
            pd.DataFrame([{"round": 1, "pick": 3, "draft_id": "stale"}]),
        ],
        ignore_index=True,
    )

    assert provider_draft_manifest_matches(exact, provider, key_columns=("round", "pick"))
    assert not provider_draft_manifest_matches(duplicate, provider, key_columns=("round", "pick"))
    assert not provider_draft_manifest_matches(unexpected, provider, key_columns=("round", "pick"))


def test_active_draft_manifest_reuses_complete_current_board_despite_historical_picks():
    from multi_league.core.league_refresh import refresh_authoritative_draft_partition

    current = pd.DataFrame([
        {"year": 2026, "draft_id": "primary", "pick": 1, "round": 1},
        {"year": 2026, "draft_id": "primary", "pick": 2, "round": 1},
    ])
    history = pd.DataFrame([{"year": 2025, "draft_id": "old", "pick": 1, "round": 1}])

    class Local:
        def table_exists(self, table):
            return table == "draft"

        def row_count(self, table):
            return 3

        def read_table(self, table, year=None):
            assert table == "draft"
            return current if year == 2026 else pd.concat([history, current], ignore_index=True)

    manifest = current[["draft_id", "pick"]]
    assert refresh_authoritative_draft_partition(
        Local(), provider_manifest=manifest, key_columns=("draft_id", "pick"),
        fetch_full=lambda: (_ for _ in ()).throw(AssertionError("complete board must not refetch")),
        year=2026, platform="sleeper", league_id="renewed-2026",
    ) == 0


def test_provider_confirmed_no_active_draft_keeps_historical_picks_without_refetch():
    from multi_league.core.league_refresh import refresh_authoritative_draft_partition

    class Local:
        def table_exists(self, table):
            return True

        def row_count(self, table):
            return 100

        def read_table(self, table, year=None):
            return pd.DataFrame() if year == 2026 else pd.DataFrame([{"year": 2025, "pick": 1}])

    assert refresh_authoritative_draft_partition(
        Local(), provider_manifest=pd.DataFrame(columns=["draft_id", "pick"]),
        key_columns=("draft_id", "pick"),
        fetch_full=lambda: (_ for _ in ()).throw(AssertionError("confirmed no draft must not fetch")),
        year=2026, platform="sleeper", league_id="renewed-2026", confirmed_no_draft=True,
    ) == 0


def test_authoritative_draft_refetch_rejects_a_missing_provider_pick():
    """A successful but truncated refetch must not preserve stale draft rows."""
    from multi_league.core.league_refresh import RefreshScopeError, assert_authoritative_draft_refetch

    manifest = pd.DataFrame([{"round": 1, "pick": 1}, {"round": 1, "pick": 2}])
    partial = pd.DataFrame([{"round": 1, "pick": 1, "player": "One"}])
    with pytest.raises(RefreshScopeError, match="authoritative draft"):
        assert_authoritative_draft_refetch(
            partial, manifest, key_columns=("round", "pick")
        )


def test_authoritative_draft_refetch_rejects_empty_completed_draft():
    """An empty endpoint reply is not proof that a known completed draft vanished."""
    from multi_league.core.league_refresh import RefreshScopeError, assert_authoritative_draft_refetch

    manifest = pd.DataFrame([{"draft_id": "d1", "pick": 1}])
    with pytest.raises(RefreshScopeError, match="authoritative draft"):
        assert_authoritative_draft_refetch(
            pd.DataFrame(), manifest, key_columns=("draft_id", "pick")
        )


def test_authoritative_draft_refetch_accepts_exact_keys_or_confirmed_absence():
    """A provider-confirmed no-draft season is valid only without retained picks."""
    from multi_league.core.league_refresh import RefreshScopeError, assert_authoritative_draft_refetch

    manifest = pd.DataFrame([{"pick": 1}, {"pick": 2}])
    fetched = pd.DataFrame([{"pick": 2}, {"pick": 1}])
    assert assert_authoritative_draft_refetch(
        fetched, manifest, key_columns=("pick",)
    )
    empty = pd.DataFrame(columns=["pick"])
    assert not assert_authoritative_draft_refetch(
        empty, empty, key_columns=("pick",), confirmed_no_draft=True
    )
    with pytest.raises(RefreshScopeError, match="no-draft"):
        assert_authoritative_draft_refetch(
            empty, empty, key_columns=("pick",)
        )


def test_refresh_draft_partition_refuses_partial_refetch_before_local_replacement():
    """The actual weekly draft branch cannot silently skip a short provider frame."""
    from multi_league.core.league_refresh import RefreshScopeError, refresh_authoritative_draft_partition

    class Local:
        league_name = "league_a"
        saved = None

        def table_exists(self, _table):
            return False

        def row_count(self, _table):
            return 0

        def _normalize_table_frame(self, _table, frame, **_kwargs):
            return frame.assign(db_name="league_a")

        def save_table(self, table, frame, **kwargs):
            self.saved = (table, frame, kwargs)

    local = Local()
    manifest = pd.DataFrame([{"pick": 1}, {"pick": 2}])
    partial = pd.DataFrame([{"year": 2026, "pick": 1}])
    with pytest.raises(RefreshScopeError, match="authoritative draft"):
        refresh_authoritative_draft_partition(
            local,
            provider_manifest=manifest,
            key_columns=("pick",),
            fetch_full=lambda: partial,
            year=2026,
            platform="espn",
            league_id="2026-1",
        )
    assert local.saved is None


def test_refresh_draft_partition_replaces_exact_picks_and_skips_confirmed_absence():
    """Complete picks replace a year; an evidenced no-draft season is a no-op."""
    from multi_league.core.league_refresh import refresh_authoritative_draft_partition

    class Local:
        league_name = "league_a"
        saved = None

        def table_exists(self, _table):
            return False

        def row_count(self, _table):
            return 0

        def _normalize_table_frame(self, _table, frame, **_kwargs):
            return frame.assign(db_name="league_a")

        def save_table(self, table, frame, **kwargs):
            self.saved = (table, frame, kwargs)

    local = Local()
    manifest = pd.DataFrame([{"pick": 1}, {"pick": 2}])
    complete = pd.DataFrame([
        {"year": 2026, "pick": 2, "round": 1, "draft_id": "d1"},
        {"year": 2026, "pick": 1, "round": 1, "draft_id": "d1"},
    ])
    assert refresh_authoritative_draft_partition(
        local,
        provider_manifest=manifest,
        key_columns=("pick",),
        fetch_full=lambda: complete,
        year=2026,
        platform="espn",
        league_id="2026-1",
    ) == 2
    assert local.saved[0] == "draft"
    untouched = Local()
    assert refresh_authoritative_draft_partition(
        untouched,
        provider_manifest=pd.DataFrame(columns=["pick"]),
        key_columns=("pick",),
        fetch_full=lambda: (_ for _ in ()).throw(AssertionError("no draft fetch expected")),
        year=2026,
        platform="espn",
        league_id="2026-1",
        confirmed_no_draft=True,
    ) == 0
    assert untouched.saved is None


def test_weekly_refresh_refetches_a_nonempty_draft_when_provider_manifest_has_a_missing_pick():
    """A partial draft must take the same fetch branch as an absent draft."""
    from multi_league.core.league_refresh import needs_active_season_draft_fetch

    class _Local:
        def table_exists(self, table: str) -> bool:
            assert table == "draft"
            return True

        def row_count(self, table: str) -> int:
            assert table == "draft"
            return 3

        def read_table(self, table: str) -> pd.DataFrame:
            assert table == "draft"
            return pd.DataFrame(
                [
                    {"draft_id": "primary", "pick": 1},
                    {"draft_id": "primary", "pick": 2},
                    {"draft_id": "primary", "pick": 4},
                ]
            )

    manifest = pd.DataFrame(
        [
            {"draft_id": "primary", "pick": 1},
            {"draft_id": "primary", "pick": 2},
            {"draft_id": "primary", "pick": 3},
            {"draft_id": "primary", "pick": 4},
        ]
    )

    assert needs_active_season_draft_fetch(
        _Local(),
        provider_manifest=manifest,
        manifest_key_columns=("draft_id", "pick"),
    )


def test_replace_active_season_draft_replaces_the_full_authoritative_year():
    """A complete provider draft must evict stale rows, not merge beside them."""
    from multi_league.core.league_refresh import replace_active_season_draft

    class Local:
        league_name = "league_a"
        saved = None

        def _normalize_table_frame(self, _table, frame, **_kwargs):
            return frame.assign(db_name="league_a")

        def table_exists(self, _table):
            return False

        def save_table(self, table, frame, **kwargs):
            self.saved = (table, frame, kwargs)

    local_db = Local()
    provider_draft = pd.DataFrame(
        [
            {"year": 2026, "draft_id": "provider-draft", "round": 1, "pick": 1},
            {"year": 2026, "draft_id": "provider-draft", "round": 1, "pick": 2},
        ]
    )

    assert replace_active_season_draft(
        local_db,
        provider_draft,
        year=2026,
        platform="yahoo",
        league_id="470.l.164172",
    ) == 2

    table, saved, kwargs = local_db.saved
    assert table == "draft"
    assert saved[["year", "draft_id", "round", "pick"]].to_dict("records") == provider_draft.to_dict("records")
    assert kwargs == {"year": 2026, "platform": "yahoo", "league_id": "470.l.164172"}


def test_replace_active_season_draft_skips_empty_provider_payload():
    """An empty provider payload must never erase a hydrated draft year."""
    from unittest.mock import Mock

    from multi_league.core.league_refresh import replace_active_season_draft

    local_db = Mock()

    assert replace_active_season_draft(
        local_db,
        pd.DataFrame(),
        year=2026,
        platform="sleeper",
        league_id="123",
    ) == 0
    local_db.save_table.assert_not_called()


def test_renewed_sleeper_draft_without_db_name_merges_into_the_existing_league(tmp_path):
    """The real Sleeper fetcher omits db_name; LocalDB's save-time fill is too late for ownership."""
    from multi_league.core.league_refresh import replace_active_season_draft
    from multi_league.core.local_db import LocalLeagueDB

    local_db = LocalLeagueDB(tmp_path, "mawhinney_s_vixens")
    draft = pd.DataFrame([
        {"year": 2026, "draft_id": "1389710321509232642", "round": 1, "pick": 1},
        {"year": 2026, "draft_id": "1389710321509232642", "round": 1, "pick": 2},
    ])
    try:
        assert replace_active_season_draft(
            local_db, draft, year=2026, platform="sleeper",
            league_id="1389710321509232641",
        ) == 2
        saved = local_db.read_table("draft")
        assert set(saved["db_name"]) == {"mawhinney_s_vixens"}
        assert set(saved["pick"]) == {1, 2}
    finally:
        local_db.close()


def test_active_draft_rejects_a_foreign_db_name_before_replacing_history(tmp_path):
    from multi_league.core.league_refresh import RefreshScopeError, replace_active_season_draft
    from multi_league.core.local_db import LocalLeagueDB

    local_db = LocalLeagueDB(tmp_path, "mawhinney_s_vixens")
    foreign = pd.DataFrame([{
        "db_name": "another_league", "year": 2026,
        "draft_id": "draft-1", "round": 1, "pick": 1,
    }])
    try:
        with pytest.raises(RefreshScopeError, match="different league"):
            replace_active_season_draft(
                local_db, foreign, year=2026, platform="sleeper",
                league_id="1389710321509232641",
            )
        assert not local_db.table_exists("draft")
    finally:
        local_db.close()


def test_weekly_yahoo_refresh_refetches_a_draft_with_missing_provider_ids():
    """A preexisting draft without Yahoo IDs is incomplete, not immutable."""
    from multi_league.core.league_refresh import needs_active_season_draft_fetch

    class _Local:
        def table_exists(self, table: str) -> bool:
            assert table == "draft"
            return True

        def row_count(self, table: str) -> int:
            assert table == "draft"
            return 2

        def read_table(self, table: str) -> pd.DataFrame:
            assert table == "draft"
            return pd.DataFrame(
                [
                    {"player": "Adam Randall", "yahoo_player_id": None},
                    {"player": "Veteran", "yahoo_player_id": "12345"},
                ]
            )

    assert needs_active_season_draft_fetch(_Local(), platform="yahoo")
    assert not needs_active_season_draft_fetch(_Local(), platform="sleeper")


def test_yahoo_refresh_history_snapshot_uses_bounded_table_year_payloads(monkeypatch):
    """History hydration must not send every wide source row through one Fly query.

    Full league history is still required for shared career/homepage enrichments,
    but one tagged JSON UNION can exceed Fly's response budget.  The manifest
    may be one small query; wide rows must be fetched in bounded table/year
    payloads instead.
    """
    from multi_league.core import delta_publish
    from scripts import refresh_yahoo_active_season

    registry = {
        table: {"columns": {"db_name": "VARCHAR", "year": "INTEGER"}}
        for table in refresh_yahoo_active_season.SOURCE_TABLES
    }
    registry["league_context"] = {"columns": {"db_name": "VARCHAR"}}
    monkeypatch.setattr(delta_publish, "canonical_table_registry", lambda: registry)

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query(self, sql, **_kwargs):
            self.sql.append(sql)
            if "history_source_years" in sql:
                return [
                    {"source_table": "matchup", "year": 2024},
                    {"source_table": "matchup", "year": 2025},
                ]
            if 'public."league_context"' in sql:
                return [{"payload": '{"db_name":"league_a","platform":"yahoo"}'}]
            return [
                {
                    "payload": '{"db_name":"league_a","year":2024}',
                },
                {
                    "payload": '{"db_name":"league_a","year":2025}',
                },
            ]

        def query_df(self, sql, **_kwargs):
            self.sql.append(sql)
            return pd.DataFrame([{"db_name": "league_a", "year": 2025}])

    reader = Reader()
    frames = refresh_yahoo_active_season._source_frames(
        reader,
        db_name="league_a",
        tables=refresh_yahoo_active_season.ACTIVE_REFRESH_SOURCE_TABLES,
    )

    assert set(frames) == set(refresh_yahoo_active_season.ACTIVE_REFRESH_SOURCE_TABLES)
    assert frames["matchup"]["year"].tolist() == [2024, 2025]
    assert len(reader.sql) == 3
    assert "UNION ALL" in reader.sql[0]
    assert "to_json" not in reader.sql[0]
    assert all("UNION ALL" not in sql for sql in reader.sql[1:])
    assert all("to_json" in sql for sql in reader.sql[1:])


def test_yahoo_refresh_reads_only_quick_pipeline_source_tables(monkeypatch):
    """Weekly hydration excludes rollup/config tables already represented by context."""
    from multi_league.core import delta_publish
    from scripts import refresh_yahoo_active_season

    registry = {
        table: {"columns": {"db_name": "VARCHAR", "year": "INTEGER"}}
        for table in refresh_yahoo_active_season.SOURCE_TABLES
    }
    monkeypatch.setattr(delta_publish, "canonical_table_registry", lambda: registry)

    class Reader:
        def query(self, _sql, **_kwargs):
            return []

        def query_df(self, _sql, **_kwargs):
            return pd.DataFrame([{"db_name": "league_a", "year": 2026}])

    tables = ("matchup", "player_fantasy", "league_context")
    frames = refresh_yahoo_active_season._source_frames(
        Reader(),
        db_name="league_a",
        active_year=2026,
        tables=tables,
    )

    assert tuple(frames) == tables


def test_active_snapshot_keeps_keeper_configuration_unscoped(monkeypatch):
    """Keeper rules are league-level user configuration, not a 2026-only row."""
    from multi_league.core import delta_publish
    from scripts import refresh_yahoo_active_season

    registry = {
        table: {"columns": {"db_name": "VARCHAR", "year": "INTEGER"}}
        for table in refresh_yahoo_active_season.SOURCE_TABLES
    }
    registry["league_context"] = {"columns": {"db_name": "VARCHAR"}}
    monkeypatch.setattr(delta_publish, "canonical_table_registry", lambda: registry)

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query(self, sql, **_kwargs):
            self.sql.append(sql)
            return []

    reader = Reader()
    refresh_yahoo_active_season._source_frames(
        reader,
        db_name="league_a",
        active_year=2026,
        tables=refresh_yahoo_active_season.ACTIVE_REFRESH_SOURCE_TABLES,
    )

    assert "keeper_config" in refresh_yahoo_active_season.ACTIVE_REFRESH_SOURCE_TABLES
    keeper_query = next(part for part in reader.sql[0].split("UNION ALL") if "keeper_config" in part)
    assert "year = 2026" not in keeper_query


def test_active_snapshot_drops_legacy_keeper_created_at_metadata():
    """A legacy Fly timestamp is not part of canonical user keeper settings."""
    from scripts import refresh_yahoo_active_season

    registry = {
        "keeper_config": {
            "columns": {
                "db_name": "VARCHAR",
                "year": "INTEGER",
                "updated_at": "TIMESTAMP",
                "enabled": "BOOLEAN",
            }
        }
    }

    class Reader:
        @staticmethod
        def query(_sql, **_kwargs):
            return [{
                "source_table": "keeper_config",
                "payload": {
                    "db_name": "kmffl",
                    "year": 0,
                    "created_at": "2026-04-20 02:47:12",
                    "updated_at": "2026-09-15 11:54:40",
                    "enabled": True,
                },
            }]

    frames = refresh_yahoo_active_season._active_source_snapshot_frames(
        Reader(),
        registry=registry,
        db_name="kmffl",
        active_year=2026,
        table_names=("keeper_config",),
    )

    assert frames["keeper_config"].to_dict("records") == [{
        "db_name": "kmffl",
        "year": 0,
        "updated_at": "2026-09-15 11:54:40",
        "enabled": True,
    }]


def test_yahoo_refresh_uses_the_saved_renewal_chain_before_discovery():
    """Normal weekly runs must not re-walk Yahoo's renewal links every time."""
    from types import SimpleNamespace

    from scripts import refresh_yahoo_active_season

    ctx = SimpleNamespace(league_ids={"2025": "461.l.90939", "2026": "470.l.80971"})

    history = refresh_yahoo_active_season._active_yahoo_history(
        ctx,
        oauth=object(),
        active_year=2026,
        discover=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not rediscover a saved chain")),
    )

    assert history == {"2025": "461.l.90939", "2026": "470.l.80971"}


def test_yahoo_refresh_uses_the_hydrated_active_league_key_before_chain_discovery():
    """A weekly refresh needs only the active key, not a historical Yahoo walk."""
    from types import SimpleNamespace

    from scripts import refresh_yahoo_active_season

    ctx = SimpleNamespace(league_ids={})

    history = refresh_yahoo_active_season._active_yahoo_history(
        ctx,
        oauth=object(),
        active_year=2026,
        source_active_key="470.l.80971",
        discover=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not walk the renewal chain when Fly already has the active key")
        ),
    )

    assert history == {"2026": "470.l.80971"}


def test_yahoo_refresh_reads_a_valid_active_key_from_hydrated_settings():
    """The retained active settings are the fast authoritative weekly source."""
    from scripts import refresh_yahoo_active_season

    source_frames = {
        "league_settings": pd.DataFrame(
            [
                {"db_name": "league_a", "year": 2026, "league_key": "470.l.80971"},
            ]
        )
    }

    assert refresh_yahoo_active_season._active_yahoo_key_from_source_frames(
        source_frames,
        active_year=2026,
    ) == "470.l.80971"


def test_yahoo_refresh_persists_only_a_new_renewal_chain_without_losing_user_settings(tmp_path):
    """A one-time chain backfill must not overwrite aliases or keeper settings."""
    from multi_league.core.local_db import LocalLeagueDB
    from scripts import refresh_yahoo_active_season

    source_context = pd.DataFrame(
        [
            {
                "db_name": "kmffl",
                "platform": "yahoo",
                "league_id": "461.l.90939",
                "league_name": "KMFFL",
                "league_ids_json": None,
                "manager_name_overrides_json": '{"Old name":"Current name"}',
                "keeper_rules_json": '{"enabled":true}',
            }
        ]
    )
    local = LocalLeagueDB(tmp_path, "kmffl")
    try:
        from multi_league.core.league_refresh import hydrate_local_refresh_sources

        hydrate_local_refresh_sources(local, {"league_context": source_context}, db_name="kmffl")
        changed = refresh_yahoo_active_season._persist_yahoo_renewal_chain(
            local,
            source_context=source_context,
            db_name="kmffl",
            history={"2025": "461.l.90939", "2026": "470.l.80971"},
        )

        row = local.connect().execute(
            "SELECT league_ids_json, manager_name_overrides_json, keeper_rules_json "
            "FROM public.league_context WHERE db_name = 'kmffl'"
        ).fetchone()
        assert changed is True
        assert json.loads(row[0]) == {"2025": "461.l.90939", "2026": "470.l.80971"}
        assert row[1] == '{"Old name":"Current name"}'
        assert row[2] == '{"enabled":true}'
    finally:
        local.close()


def test_yahoo_refresh_reads_active_sources_in_one_tagged_snapshot(monkeypatch):
    """The weekly worker avoids serial Fly request latency without changing data shape."""
    from multi_league.core import delta_publish
    from scripts import refresh_yahoo_active_season

    registry = {
        table: {"columns": {"db_name": "VARCHAR", "year": "INTEGER"}}
        for table in refresh_yahoo_active_season.SOURCE_TABLES
    }
    registry["league_context"] = {"columns": {"db_name": "VARCHAR"}}
    monkeypatch.setattr(delta_publish, "canonical_table_registry", lambda: registry)

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query(self, sql, **_kwargs):
            self.sql.append(sql)
            return [
                {
                    "source_table": "matchup",
                    "payload": json.dumps({"db_name": "league_a", "year": 2026, "week": 1}),
                },
                {
                    "source_table": "league_context",
                    "payload": json.dumps({"db_name": "league_a", "platform": "yahoo"}),
                },
            ]

        def query_df(self, *_args, **_kwargs):
            raise AssertionError("active snapshots must use one tagged Fly query")

    reader = Reader()
    frames = refresh_yahoo_active_season._source_frames(
        reader,
        db_name="league_a",
        active_year=2026,
        tables=("matchup", "player_fantasy", "league_context"),
    )

    assert len(reader.sql) == 1
    assert "UNION ALL" in reader.sql[0]
    assert "to_json" in reader.sql[0]
    assert frames["matchup"].to_dict("records") == [{"db_name": "league_a", "year": 2026, "week": 1}]
    assert frames["league_context"].to_dict("records") == [{"db_name": "league_a", "platform": "yahoo"}]
    assert frames["player_fantasy"].empty


def test_yahoo_refresh_reuses_complete_schedule_windows_for_transactions():
    """The transaction fetcher should not rebuild Yahoo's calendar after a weekly matchup fetch."""
    from scripts.refresh_yahoo_active_season import _transaction_matchup_windows

    class Local:
        @staticmethod
        def read_table(table: str, year: int):
            assert (table, year) == ("schedule", 2026)
            return pd.DataFrame(
                [
                    {"year": 2026, "week": 1, "week_start": "2026-09-08", "week_end": "2026-09-14"},
                    {"year": 2026, "week": 2, "week_start": "2026-09-15", "week_end": "2026-09-21"},
                ]
            )

    current_week = pd.DataFrame(
        [{"year": 2026, "week": 3, "week_start": "2026-09-22", "week_end": "2026-09-28"}]
    )

    actual = _transaction_matchup_windows(
        Local(),
        year=2026,
        refresh_weeks=[3],
        current_schedule_frames=[current_week],
    )

    assert actual is not None
    assert actual[["week", "cumulative_week"]].to_dict("records") == [
        {"week": 1, "cumulative_week": 202601},
        {"week": 2, "cumulative_week": 202602},
        {"week": 3, "cumulative_week": 202603},
    ]


def test_yahoo_refresh_rebuilds_windows_when_retained_schedule_has_a_gap():
    """Do not remap old transactions using only a partial active-season calendar."""
    from scripts.refresh_yahoo_active_season import _transaction_matchup_windows

    class Local:
        @staticmethod
        def read_table(_table: str, year: int):
            assert year == 2026
            return pd.DataFrame(
                [{"year": 2026, "week": 1, "week_start": "2026-09-08", "week_end": "2026-09-14"}]
            )

    current_week = pd.DataFrame(
        [{"year": 2026, "week": 3, "week_start": "2026-09-22", "week_end": "2026-09-28"}]
    )

    assert _transaction_matchup_windows(
        Local(),
        year=2026,
        refresh_weeks=[3],
        current_schedule_frames=[current_week],
    ) is None


def test_yahoo_refresh_rebuilds_only_published_season_aggregates_without_subprocess(
    tmp_path,
    monkeypatch,
):
    """Weekly refreshes must not rebuild unpublished career aggregates in child processes."""
    from scripts import refresh_yahoo_active_season
    from multi_league.transformations.aggregation import aggregate_draft_context
    from multi_league.transformations.aggregation import aggregate_fantasy_context
    from multi_league.transformations.aggregation import aggregate_transaction_context
    from multi_league.transformations.aggregation import aggregation_utils
    from multi_league.core import db_utils

    ops_cache = tmp_path / "ops.duckdb"
    ops_cache.touch()
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_cache))
    monkeypatch.setattr(db_utils, "attach_ops_cache", lambda _conn, _path: None)
    # This contract isolates aggregate ordering; its connection double is not DuckDB.
    monkeypatch.setattr(
        refresh_yahoo_active_season,
        "_attach_ops_cache_for_enrichment",
        lambda _local_db: None,
    )

    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    connection = object()

    class Local:
        def connect(self):
            return connection

    def record(name):
        def _record(*args, **kwargs):
            calls.append((name, args, kwargs))

        return _record

    monkeypatch.setattr(aggregation_utils, "configure_table_catalog", record("configure"))
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_season_table", record("create_fantasy_season"))
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_season", record("fantasy_season"))
    monkeypatch.setattr(
        aggregate_fantasy_context,
        "create_fantasy_season_table_all",
        record("create_fantasy_season_all"),
    )
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_season_all", record("fantasy_season_all"))
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_career_table", record("create_fantasy_career"))
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_career", record("fantasy_career"))
    monkeypatch.setattr(
        aggregate_fantasy_context,
        "create_fantasy_career_table_all",
        record("create_fantasy_career_all"),
    )
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_career_all", record("fantasy_career_all"))
    monkeypatch.setattr(aggregate_draft_context, "create_draft_manager_season_table", record("create_draft_manager_season"))
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_manager_season", record("draft_manager_season"))
    monkeypatch.setattr(aggregate_draft_context, "create_draft_manager_career_table", record("create_draft_manager_career"))
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_manager_career", record("draft_manager_career"))
    monkeypatch.setattr(aggregate_draft_context, "create_draft_player_career_table", record("create_draft_player_career"))
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_player_career", record("draft_player_career"))
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_manager_season_table",
        record("create_transaction_manager_season"),
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_manager_season", record("transaction_manager_season"))
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_manager_career_table",
        record("create_transaction_manager_career"),
    )
    monkeypatch.setattr(
        aggregate_transaction_context,
        "aggregate_transaction_manager_career",
        record("transaction_manager_career"),
    )
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_player_career_table",
        record("create_transaction_player_career"),
    )
    monkeypatch.setattr(
        aggregate_transaction_context,
        "aggregate_transaction_player_career",
        record("transaction_player_career"),
    )
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_report_card_table",
        record("create_transaction_report_card"),
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_report_card", record("transaction_report_card"))
    monkeypatch.setattr(
        refresh_yahoo_active_season.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("an incomplete active week must not start an aggregate subprocess"),
    )

    refresh_yahoo_active_season._run_refresh_aggregates(
        Local(),
        db_name="league_a",
        active_year=2026,
        work_dir=tmp_path,
        has_finalized_matchups=False,
    )

    assert calls == [
        ("configure", (connection,), {}),
        ("create_fantasy_season", (connection, "league_a"), {}),
        ("fantasy_season", (connection, "league_a"), {"year": 2026}),
        ("create_fantasy_season_all", (connection, "league_a"), {}),
        ("fantasy_season_all", (connection, "league_a"), {"year": 2026}),
        ("create_fantasy_career", (connection, "league_a"), {}),
        ("fantasy_career", (connection, "league_a"), {}),
        ("create_fantasy_career_all", (connection, "league_a"), {}),
        ("fantasy_career_all", (connection, "league_a"), {}),
        ("create_draft_manager_season", (connection, "league_a"), {}),
        ("draft_manager_season", (connection, "league_a"), {}),
        ("create_draft_manager_career", (connection, "league_a"), {}),
        ("draft_manager_career", (connection, "league_a"), {}),
        ("create_draft_player_career", (connection, "league_a"), {}),
        ("draft_player_career", (connection, "league_a"), {}),
        ("create_transaction_manager_season", (connection, "league_a"), {}),
        ("transaction_manager_season", (connection, "league_a"), {}),
        ("create_transaction_manager_career", (connection, "league_a"), {}),
        ("transaction_manager_career", (connection, "league_a"), {}),
        ("create_transaction_player_career", (connection, "league_a"), {}),
        ("transaction_player_career", (connection, "league_a"), {}),
        ("create_transaction_report_card", (connection, "league_a"), {}),
        ("transaction_report_card", (connection, "league_a"), {}),
    ]


def test_weekly_simulations_are_scoped_to_the_active_season(tmp_path, monkeypatch):
    from scripts import refresh_yahoo_active_season

    commands: list[list[str]] = []

    def capture(command, **_kwargs):
        commands.append(command)

    monkeypatch.setattr(refresh_yahoo_active_season.subprocess, "run", capture)

    refresh_yahoo_active_season._run_refresh_simulations(
        db_name="league_a",
        active_year=2026,
        current_week=4,
        work_dir=tmp_path,
        n_sims=10_000,
    )

    assert len(commands) == 2
    assert commands[0][2].endswith("expected_record_v2")
    assert commands[1][2].endswith("playoff_odds_import")
    for command in commands:
        assert command[command.index("--target-year") + 1] == "2026"
        assert command[command.index("--n-sims") + 1] == "10000"
        assert "--data-dir" in command
    assert commands[0][commands[0].index("--current-week") + 1] == "4"


def test_yahoo_roster_adapter_forwards_the_incremental_week_selection(tmp_path, monkeypatch):
    """The refresh worker must not turn one completed game into 17 weeks of calls."""
    from multi_league.data_fetchers.yahoo import yahoo_rosters

    oauth_file = tmp_path / "oauth.json"
    oauth_file.write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    class _Context:
        oauth_file_path = str(oauth_file)
        oauth_credentials = None
        league_ids = {"2026": "461.l.90939"}
        data_directory = tmp_path
        rate_limit_per_sec = 0.0
        manager_name_overrides = {}

        @staticmethod
        def has_league_ids_mapping():
            return True

    class _Fetcher:
        expected_team_keys = ("461.l.90939.t.1", "461.l.90939.t.2")

        def __init__(self, **_kwargs):
            pass

        def fetch_season_rosters(self, *, year, weeks, end_week):
            captured.update({"year": year, "weeks": weeks, "end_week": end_week})
            return pd.DataFrame([{"week": 1, "player_id": "p_ne"}])

    monkeypatch.setattr(yahoo_rosters, "YahooRosterFetcher", _Fetcher)
    monkeypatch.setattr(yahoo_rosters, "find_league_settings_files", lambda _dir: {})

    rows, failed_weeks = yahoo_rosters.fetch_rosters_for_year(
        _Context(),
        2026,
        weeks=[1],
    )

    assert rows["player_id"].tolist() == ["p_ne"]
    assert rows.attrs["expected_team_keys"] == ("461.l.90939.t.1", "461.l.90939.t.2")
    assert failed_weeks == []
    assert captured == {"year": 2026, "weeks": [1], "end_week": None}


def test_refresh_stage_replaces_only_the_active_season_and_own_league():
    """A refresh bundle must never carry an old season or another league."""
    import duckdb

    from multi_league.core.league_refresh import stage_refresh_partitions

    source = duckdb.connect(":memory:")
    source.execute("CREATE SCHEMA public")
    source.execute(
        "CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)"
    )
    source.execute("CREATE TABLE public.matchup_career (db_name VARCHAR, manager VARCHAR, wins INTEGER)")
    source.execute(
        "INSERT INTO public.matchup VALUES "
        "('kmffl', 2025, 17, 'old'), ('kmffl', 2026, 1, 'current'), ('other', 2026, 1, 'other')"
    )
    source.execute("INSERT INTO public.matchup_career VALUES ('kmffl', 'Joe', 99), ('other', 'Other', 12)")
    try:
        stage = stage_refresh_partitions(
            source,
            db_name="kmffl",
            active_year=2026,
            tables=["matchup", "matchup_career"],
        )
        try:
            assert stage.execute("SELECT db_name, year, week FROM public.matchup").fetchall() == [
                ("kmffl", 2026, 1)
            ]
            assert stage.execute("SELECT db_name, manager, wins FROM public.matchup_career").fetchall() == [
                ("kmffl", "Joe", 99)
            ]
        finally:
            stage.close()
    finally:
        source.close()


def test_history_chunk_concat_preserves_values_without_all_null_dtype_warning():
    """Historical Fly chunks may have old all-null feature columns.

    Concatenation must retain their nulls and the later non-null values without
    relying on pandas' deprecated all-null dtype inference behavior.
    """
    import warnings

    from scripts.refresh_yahoo_active_season import _concat_history_parts

    older = pd.DataFrame(
        {
            "db_name": ["kmffl"],
            "year": [2013],
            "legacy_null": [None],
            "eventually_numeric": [None],
        }
    )
    newer = pd.DataFrame(
        {
            "db_name": ["kmffl"],
            "year": [2025],
            "legacy_null": [None],
            "eventually_numeric": [1.5],
        }
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        actual = _concat_history_parts([older, newer])

    assert actual["year"].tolist() == [2013, 2025]
    assert pd.isna(actual["eventually_numeric"].iloc[0])
    assert actual["eventually_numeric"].iloc[1] == 1.5
    assert actual["legacy_null"].isna().all()


@pytest.mark.parametrize("fetch_fails, output_fails", [(False, False), (False, True), (True, True)])
def test_active_refresh_patches_only_finalized_game_rows_in_research_ops_cache(
    tmp_path, capsys, monkeypatch, fetch_fails, output_fails,
):
    """The reduced research cache cannot use the full-artifact refresh gate.

    Refresh its one joined table from Fly's authoritative finalized rows while
    leaving a not-yet-final Week 1 game untouched.
    """
    import duckdb

    from scripts.refresh_yahoo_active_season import _patch_research_ops_cache_from_fly

    base = tmp_path / "ops_cache.duckdb"
    con = duckdb.connect(str(base))
    try:
        con.execute("CREATE SCHEMA nfl_historical")
        con.execute(
            """
            CREATE TABLE nfl_historical.nfl_player_stats_all (
                NFL_player_id VARCHAR,
                year INTEGER,
                week INTEGER,
                season_type VARCHAR,
                game_date VARCHAR,
                nfl_team VARCHAR,
                opponent_nfl_team VARCHAR,
                fantasy_points DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
              ('ne_player', 2026, 1, 'REG', '2026-09-10', 'NWE', 'SEA', 0.0),
              ('unplayed_player', 2026, 1, 'REG', '2026-09-14', 'DAL', 'NYG', 0.0)
            """
        )
    finally:
        con.close()

    source = pd.DataFrame(
        [
            {
                "NFL_player_id": "ne_player",
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_date": "2026-09-10",
                "nfl_team": "NWE",
                "opponent_nfl_team": "SEA",
                "fantasy_points": 18.25,
            }
        ]
    )

    class _Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query(self, sql, *, database):
            assert database == "___ops"
            assert sql == "DESCRIBE nfl_historical.nfl_player_stats_all"
            return [
                {"column_name": column, "column_type": "VARCHAR" if column.endswith("id") or column in {"season_type", "game_date", "nfl_team", "opponent_nfl_team"} else "INTEGER" if column in {"year", "week"} else "DOUBLE"}
                for column in source.columns
            ]

        def query_df(self, sql, *, database):
            assert database == "___ops"
            self.sql.append(sql)
            if fetch_fails:
                raise RuntimeError("source fetch failed")
            return source.copy()

    reader = _Reader()
    if output_fails:
        def fail_diagnostic(*args, **kwargs):
            raise BrokenPipeError("diagnostic sink closed")
        monkeypatch.setattr("builtins.print", fail_diagnostic)
    if fetch_fails:
        with pytest.raises(RuntimeError, match="source fetch failed"):
            _patch_research_ops_cache_from_fly(
                reader, base=base, finalized_ops=source, year=2026, weeks=[1], work_dir=tmp_path,
            )
        return
    output = _patch_research_ops_cache_from_fly(
        reader,
        base=base,
        finalized_ops=source[["week", "nfl_team", "opponent_nfl_team", "NFL_player_id"]],
        year=2026,
        weeks=[1],
        work_dir=tmp_path,
    )

    assert output != base
    assert all("SELECT *" not in sql.upper() for sql in reader.sql)
    verified = duckdb.connect(str(output), read_only=True)
    try:
        assert verified.execute(
            "SELECT NFL_player_id, fantasy_points FROM nfl_historical.nfl_player_stats_all ORDER BY NFL_player_id"
        ).fetchall() == [("ne_player", 18.25), ("unplayed_player", 0.0)]
    finally:
        verified.close()

    if output_fails:
        return
    timing_line = next(
        line for line in capsys.readouterr().out.splitlines() if line.startswith("[ops-cache-timing] ")
    )
    timing = json.loads(timing_line.removeprefix("[ops-cache-timing] "))
    assert set(timing["phases"]) == {
        "cache_open", "fly_schema", "schema_alignment", "week_1_fetch",
        "week_1_validate", "week_1_replace", "cache_close", "total", "unmarked",
    }
    assert all(value >= 0 for value in timing["phases"].values())
    assert timing["year"] == 2026
    assert timing["weeks"] == [1]


def test_weekly_worker_patches_its_disposable_ops_cache_in_place(tmp_path, monkeypatch):
    """Avoid copying the 739 MB Actions cache before a one-week quick rebuild."""
    from scripts import refresh_yahoo_active_season

    cache = tmp_path / "ops_cache.duckdb"
    cache.write_bytes(b"cache")
    monkeypatch.setenv("OPS_CACHE_PATH", str(cache))
    captured: dict[str, object] = {}

    def _patch(_reader, _facts, **kwargs):
        captured.update(kwargs)
        return kwargs["base"]

    monkeypatch.setattr(refresh_yahoo_active_season, "_patch_research_ops_cache_from_fly", _patch)

    output = refresh_yahoo_active_season._ensure_ops_cache_matches_live(
        object(),
        pd.DataFrame(),
        year=2026,
        weeks=[1],
        work_dir=tmp_path,
    )

    assert output == cache
    assert captured["base"] == cache
    assert captured["in_place"] is True


def test_refresh_aggregate_subprocess_inherits_the_local_package_path(monkeypatch):
    """GitHub invokes refresh_aggregates.py as a child Python process."""
    from scripts import refresh_yahoo_active_season

    monkeypatch.setenv("PYTHONPATH", "existing-path")

    env = refresh_yahoo_active_season._aggregate_subprocess_env()

    entries = env["PYTHONPATH"].split(refresh_yahoo_active_season.os.pathsep)
    assert entries[0] == str(refresh_yahoo_active_season.DATA_SCRIPTS)
    assert entries[1] == "existing-path"


def test_refresh_aggregate_subprocess_releases_the_local_duckdb_lock(monkeypatch, tmp_path):
    """Final-score aggregation releases the local database only for its child process."""
    from scripts import refresh_yahoo_active_season
    from multi_league.transformations.aggregation import aggregate_draft_context
    from multi_league.transformations.aggregation import aggregate_fantasy_context
    from multi_league.transformations.aggregation import aggregate_transaction_context
    from multi_league.transformations.aggregation import aggregation_utils
    from multi_league.core import db_utils

    ops_cache = tmp_path / "ops.duckdb"
    ops_cache.touch()
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_cache))
    monkeypatch.setattr(db_utils, "attach_ops_cache", lambda _conn, _path: None)
    # This contract isolates child-process lock release; its connection double is not DuckDB.
    monkeypatch.setattr(
        refresh_yahoo_active_season,
        "_attach_ops_cache_for_enrichment",
        lambda _local_db: None,
    )

    events: list[str] = []
    commands: list[list[str]] = []
    connection = object()

    class _Local:
        def close(self):
            events.append("close")

        def connect(self):
            events.append("connect")
            return connection

    def _run(command, **_kwargs):
        events.append("run")
        commands.append(command)

    monkeypatch.setattr(aggregation_utils, "configure_table_catalog", lambda _conn: None)
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_season_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_season", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_season_table_all", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_season_all", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_career_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_career", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_fantasy_context, "create_fantasy_career_table_all", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_career_all", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_draft_context, "create_draft_manager_season_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_manager_season", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_draft_context, "create_draft_manager_career_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_manager_career", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(aggregate_draft_context, "create_draft_player_career_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(aggregate_draft_context, "aggregate_draft_player_career", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_manager_season_table",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_manager_season", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_manager_career_table",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_manager_career", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_player_career_table",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_player_career", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        aggregate_transaction_context,
        "create_transaction_report_card_table",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(aggregate_transaction_context, "aggregate_transaction_report_card", lambda *_args, **_kwargs: 0)

    monkeypatch.setattr(refresh_yahoo_active_season.subprocess, "run", _run)

    refresh_yahoo_active_season._run_refresh_aggregates(
        _Local(),
        db_name="kmffl",
        active_year=2026,
        work_dir=tmp_path,
        has_finalized_matchups=True,
    )

    assert events == ["connect", "close", "run", "connect"]
    assert commands[0][commands[0].index("--steps") + 1] == "matchup,standings"


def test_espn_refresh_writes_a_receipt_for_an_early_dry_run_exit(tmp_path):
    """Actions must retain read-only evidence as an artifact too."""
    import json

    from scripts.refresh_espn_active_season import _write_receipt

    path = tmp_path / "receipt.json"
    _write_receipt({"status": "DRY_RUN_READY", "executed": False}, path)

    assert json.loads(path.read_text(encoding="utf-8")) == {"executed": False, "status": "DRY_RUN_READY"}


def test_sleeper_active_renewal_uses_the_persisted_chain_not_league_members():
    """A persisted active ID is validated against the existing chain directly."""
    from scripts.refresh_sleeper_active_season import _resolve_active_renewal

    leagues = {
        "renewed": {"league_id": "renewed", "season": "2026", "previous_league_id": "saved"},
        "unrelated": {"league_id": "unrelated", "season": "2026", "previous_league_id": "other"},
        "other": {"league_id": "other", "season": "2025", "previous_league_id": None},
    }

    class Client:
        def get_league(self, league_id):
            return leagues.get(league_id)

        def get_league_users(self, _league_id):
            raise AssertionError("renewal resolution must not enumerate league members")

        def get_user_leagues(self, _user_id, _sport, _year):
            raise AssertionError("renewal resolution must not enumerate user leagues")

    client = Client()
    renewed = _resolve_active_renewal(
        client,
        seed_league_id="saved",
        active_year=2026,
        known_league_ids={"2025": "saved", "2026": "renewed"},
    )
    assert renewed == leagues["renewed"]
    assert (
        _resolve_active_renewal(
            client,
            seed_league_id="saved",
            active_year=2026,
            known_league_ids={"2025": "saved", "2026": "unrelated"},
        )
        is None
    )


def test_sleeper_first_season_active_id_needs_no_predecessor():
    from scripts.refresh_sleeper_active_season import _resolve_active_renewal

    active = {
        "league_id": "first-season",
        "season": "2026",
        "previous_league_id": None,
    }

    class Client:
        @staticmethod
        def get_league(league_id):
            return active if league_id == "first-season" else None

    assert _resolve_active_renewal(
        Client(),
        seed_league_id=None,
        active_year=2026,
        known_league_ids={"2026": "first-season"},
    ) == active
    assert _resolve_active_renewal(
        Client(),
        seed_league_id=None,
        active_year=2026,
        known_league_ids={},
    ) is None

    class WrongLineage:
        @staticmethod
        def get_league(league_id):
            return {**active, "previous_league_id": "unrelated"}

    assert _resolve_active_renewal(
        WrongLineage(),
        seed_league_id=None,
        active_year=2026,
        known_league_ids={"2026": "first-season"},
    ) is None


def test_sleeper_worker_rejects_caller_id_conflicting_with_saved_active_chain(monkeypatch, tmp_path):
    from scripts import refresh_sleeper_active_season as worker

    monkeypatch.setattr(
        worker, "_load_persisted_sleeper_chain",
        lambda reader, db_name, active_year=None: ({"league_name": "Saved"}, {"2026": "saved-active"}),
    )
    with pytest.raises(RuntimeError, match="conflicting active Sleeper league IDs"):
        worker._build_context(
            reader=object(), db_name="saved", active_year=2026,
            work_dir=tmp_path, active_league_id="caller-fork",
        )


def test_sleeper_missing_successor_is_discovered_from_saved_members_not_one_manager():
    from scripts.refresh_sleeper_active_season import _resolve_active_renewal

    class Client:
        def get_league(self, league_id):
            return {
                "2025": {"league_id": "2025", "season": "2025"},
                "2026": {"league_id": "2026", "season": "2026", "previous_league_id": "2025"},
            }.get(league_id)

        def get_league_users(self, league_id):
            assert league_id == "2025"
            return [{"user_id": "quit"}, {"user_id": "retained"}]

        def get_user_leagues(self, user_id, sport, season):
            assert sport == "nfl" and season == 2026
            return [] if user_id == "quit" else [{"league_id": "2026", "season": "2026"}]

    assert _resolve_active_renewal(
        Client(), seed_league_id="2025", active_year=2026,
        known_league_ids={"2025": "2025"},
    )["league_id"] == "2026"


def test_sleeper_missing_successor_rejects_unrelated_member_league_and_fork():
    from scripts.refresh_sleeper_active_season import _resolve_active_renewal

    class Client:
        def __init__(self, candidates):
            self.candidates = candidates

        def get_league(self, league_id):
            return {
                "saved": {"league_id": "saved", "season": "2025"},
                "other": {"league_id": "other", "season": "2025"},
                "unrelated": {"league_id": "unrelated", "season": "2026", "previous_league_id": "other"},
                "renewed": {"league_id": "renewed", "season": "2026", "previous_league_id": "saved"},
                "fork": {"league_id": "fork", "season": "2026", "previous_league_id": "saved"},
            }.get(league_id)

        def get_league_users(self, _league_id):
            return [{"user_id": "member"}]

        def get_user_leagues(self, _user_id, _sport, _season):
            return [{"league_id": league_id} for league_id in self.candidates]

    for candidates in (["unrelated"], ["renewed", "fork"]):
        assert _resolve_active_renewal(
            Client(candidates), seed_league_id="saved", active_year=2026,
            known_league_ids={"2025": "saved"},
        ) is None


def test_sleeper_weekly_chain_uses_every_saved_segment_year_of_a_multiplatform_league():
    from scripts.refresh_sleeper_active_season import _load_persisted_sleeper_chain

    class Reader:
        def query(self, sql, *, database):
            assert database == "___leagues"
            if "information_schema.columns" in sql:
                return [{"column_name": "league_ids_json"}]
            if "public.league_context" in sql:
                return [{
                    "league_id": "saved-2025", "league_name": "Mixed League",
                    "league_ids_json": None, "manager_name_overrides_json": '{"shared":"Preferred"}',
                }]
            if "public.league_settings" in sql:
                assert "SELECT year, platform, league_key" in sql
                return [
                    {"year": 2014, "platform": "yahoo", "league_key": "331.l.1"},
                    {"year": 2019, "platform": "sleeper", "league_key": "saved-2019"},
                    {"year": 2025, "platform": "sleeper", "league_key": "saved-2025"},
                ]
            raise AssertionError(sql)

    context, chain = _load_persisted_sleeper_chain(Reader(), db_name="mixed_league")
    assert context["manager_name_overrides_json"] == '{"shared":"Preferred"}'
    assert chain == {"2019": "saved-2019", "2025": "saved-2025"}


def test_sleeper_weekly_chain_rejects_two_imported_ids_for_one_season():
    from scripts.refresh_sleeper_active_season import _load_persisted_sleeper_chain

    class Reader:
        def query(self, sql, *, database):
            assert database == "___leagues"
            if "information_schema.columns" in sql:
                return [{"column_name": "league_ids_json"}]
            if "public.league_context" in sql:
                return [{"league_id": "saved-2025", "league_name": "Mixed League",
                         "league_ids_json": '{"2025":"saved-2025"}'}]
            if "public.league_settings" in sql:
                return [
                    {"year": 2025, "platform": "sleeper", "league_key": "saved-2025"},
                    {"year": 2025, "platform": "sleeper", "league_key": "wrong-2025"},
                ]
            raise AssertionError(sql)

    with pytest.raises(RuntimeError, match="conflicting Sleeper league IDs"):
        _load_persisted_sleeper_chain(Reader(), db_name="mixed_league")


def test_shared_refresh_watermark_tracks_played_fantasy_matchups_not_nfl_player_weeks():
    from scripts.refresh_yahoo_active_season import _last_materialized_week

    class Reader:
        def query_scalar(self, sql, *, database):
            assert database == "___leagues"
            assert "FROM public.matchup" in sql
            assert "db_name = 'kmffl'" in sql
            assert "year = 2025" in sql
            return 17

    assert _last_materialized_week(Reader(), db_name="kmffl", year=2025) == 17


def test_manual_week_ceiling_can_revisit_an_older_played_week(monkeypatch):
    import scripts.refresh_yahoo_active_season as worker

    monkeypatch.setattr(worker, "_finalized_ops", lambda reader, **kwargs: pd.DataFrame({"week": [15]}))
    monkeypatch.setattr(worker, "_last_materialized_week", lambda reader, **kwargs: 17)
    frames, watermark = worker._load_active_refresh_inputs(
        object(), db_name="kmffl", year=2025, through_week=15,
    )
    assert frames["week"].tolist() == [15]
    assert watermark == 15


def test_update_source_snapshot_captures_generation_with_fly_frames(monkeypatch):
    import scripts.refresh_yahoo_active_season as worker

    source = {"league_context": pd.DataFrame({"db_name": ["the_league"]})}
    generations = iter([7, 7])
    monkeypatch.setattr(worker, "_publish_generation", lambda reader, db_name: next(generations))
    monkeypatch.setattr(worker, "_source_frames", lambda reader, **kwargs: source)
    frames, generation = worker._capture_update_source_frames(
        object(), db_name="the_league", active_year=2026, tables=("league_context",),
    )
    assert frames is source
    assert generation == 7


def test_update_source_snapshot_is_scoped_to_the_active_season(monkeypatch):
    import scripts.refresh_yahoo_active_season as worker

    source = {"league_context": pd.DataFrame({"db_name": ["the_league"]})}
    generations = iter([7, 7])
    captured = {}
    monkeypatch.setattr(worker, "_publish_generation", lambda reader, db_name: next(generations))

    def capture_source_frames(reader, **kwargs):
        captured.update(kwargs)
        return source

    monkeypatch.setattr(worker, "_source_frames", capture_source_frames)
    frames, generation = worker._capture_update_source_frames(
        object(), db_name="the_league", active_year=2026, tables=("league_context",),
    )

    assert frames is source
    assert generation == 7
    assert captured["active_year"] == 2026


def test_active_transform_split_keeps_history_out_of_transform_input():
    import scripts.refresh_yahoo_active_season as worker

    source = {
        "matchup": pd.DataFrame(
            {
                "db_name": ["the_league", "the_league"],
                "year": [2025, 2026],
                "manager_week": ["manager_2025_1", "manager_2026_1"],
            }
        ),
        "league_context": pd.DataFrame({"db_name": ["the_league"], "payload": ["context"]}),
    }

    transform_input, historical_rows = worker._split_active_transform_source_frames(
        source, active_year=2026,
    )

    assert transform_input["matchup"]["year"].tolist() == [2026]
    assert historical_rows["matchup"]["year"].tolist() == [2025]
    assert transform_input["league_context"].equals(source["league_context"])


def test_historical_dynamic_scoring_rules_restore_after_active_transform():
    """Provider-specific scoring thresholds are historical source facts."""
    import duckdb
    import scripts.refresh_yahoo_active_season as worker

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.league_settings (db_name VARCHAR, year INTEGER)"
    )

    class LocalDB:
        @staticmethod
        def table_exists(table_name):
            return table_name == "league_settings"

        @staticmethod
        def connect():
            return conn

        @staticmethod
        def _insert_into_table(table_name, frame):
            conn.register("_test_restore_rows", frame)
            try:
                target_columns = [
                    row[0]
                    for row in conn.execute(
                        f'DESCRIBE public."{table_name}"'
                    ).fetchall()
                ]
                common = [column for column in frame.columns if column in target_columns]
                quoted = ", ".join(f'"{column}"' for column in common)
                conn.execute(
                    f'INSERT INTO public."{table_name}" ({quoted}) '
                    f'SELECT {quoted} FROM _test_restore_rows'
                )
            finally:
                conn.unregister("_test_restore_rows")

    try:
        worker._restore_historical_source_rows(
            LocalDB(),
            {
                "league_settings": pd.DataFrame(
                    {
                        "db_name": ["the_league"],
                        "year": [2025],
                        "scoring_bonus_pass_yd_325": [3.0],
                    }
                )
            },
        )
        columns = {
            row[0]
            for row in conn.execute("DESCRIBE public.league_settings").fetchall()
        }
        assert "scoring_bonus_pass_yd_325" in columns
        assert conn.execute(
            "SELECT scoring_bonus_pass_yd_325 FROM public.league_settings"
        ).fetchone() == (3.0,)
    finally:
        conn.close()


def test_historical_restore_rejects_unknown_dynamic_source_columns():
    """Dynamic allowances stay limited to scoring-rule threshold columns."""
    import duckdb
    import scripts.refresh_yahoo_active_season as worker

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.league_settings (db_name VARCHAR, year INTEGER)"
    )

    class LocalDB:
        @staticmethod
        def table_exists(table_name):
            return table_name == "league_settings"

        @staticmethod
        def connect():
            return conn

    try:
        with pytest.raises(RuntimeError, match="unregistered columns"):
            worker._restore_historical_source_rows(
                LocalDB(),
                {
                    "league_settings": pd.DataFrame(
                        {"db_name": ["the_league"], "year": [2025], "bogus": [1]}
                    )
                },
            )
    finally:
        conn.close()


def test_update_source_snapshot_rejects_concurrent_publication(monkeypatch):
    import scripts.refresh_yahoo_active_season as worker

    generations = iter([7, 8])
    monkeypatch.setattr(worker, "_publish_generation", lambda reader, db_name: next(generations))
    monkeypatch.setattr(worker, "_source_frames", lambda reader, **kwargs: {})
    with pytest.raises(RuntimeError, match="changed during source snapshot"):
        worker._capture_update_source_frames(
            object(), db_name="the_league", active_year=2026, tables=("league_context",),
        )
