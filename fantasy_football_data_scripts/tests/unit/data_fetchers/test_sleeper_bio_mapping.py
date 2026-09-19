"""Finite stable-ID mapping receipts; never infer identity from display names."""

import duckdb
import pandas as pd
import pytest

from multi_league.data_fetchers import live_nfl_ops_refresh as refresh


def witnesses():
    # Observed NFLverse roster and Sleeper fields: ESPN is absent in Sleeper.
    roster = pd.DataFrame([
        {"gsis_id": "00-0040879", "full_name": "Matthew Hibner",
         "espn_id": "4432260", "rotowire_id": "19364", "sleeper_id": None},
        {"gsis_id": "other", "full_name": "Other Player",
         "espn_id": "999", "rotowire_id": "999", "sleeper_id": "999"},
    ])
    sleeper = pd.DataFrame([
        {"player_id": "13324", "full_name": "Matt Hibner", "gsis_id": None,
         "espn_id": None, "rotowire_id": 19364},
    ])
    return roster, sleeper


def test_finite_crosswalk_resolves_real_hibner_by_rotowire_not_nickname():
    roster, sleeper = witnesses()
    before = roster.copy(deep=True)
    result = refresh.build_sleeper_bio_mapping_rows(roster, sleeper)
    assert result.to_dict("records") == [
        {"NFL_player_id": "00-0040879", "sleeper_player_id": 13324}
    ]
    pd.testing.assert_frame_equal(roster, before)


def test_finite_crosswalk_accepts_unique_espn_without_matching_names():
    roster, sleeper = witnesses()
    sleeper.loc[0, ["espn_id", "rotowire_id", "full_name"]] = [4432260, None, "Different display"]
    assert refresh.build_sleeper_bio_mapping_rows(roster, sleeper).iloc[0].to_dict() == {
        "NFL_player_id": "00-0040879", "sleeper_player_id": 13324
    }


@pytest.mark.parametrize("case", [
    "name_only", "unknown_anchor", "ambiguous_espn", "ambiguous_rotowire",
    "disagreeing_anchors", "existing_sleeper_conflict", "reverse_sleeper_conflict",
    "duplicate_provider", "two_providers_one_gsis", "invalid_provider_id",
])
def test_finite_crosswalk_rejects_unproven_or_non_one_to_one_identity(case):
    roster, sleeper = witnesses()
    if case == "name_only":
        sleeper.loc[0, ["full_name", "rotowire_id"]] = ["Matthew Hibner", None]
    elif case == "unknown_anchor":
        sleeper.loc[0, "rotowire_id"] = 123
    elif case == "ambiguous_espn":
        sleeper.loc[0, "espn_id"] = "4432260"
        roster.loc[1, "espn_id"] = "4432260"
    elif case == "ambiguous_rotowire":
        roster.loc[1, "rotowire_id"] = "19364"
    elif case == "disagreeing_anchors":
        sleeper.loc[0, "espn_id"] = "999"
    elif case == "existing_sleeper_conflict":
        roster.loc[0, "sleeper_id"] = "888"
    elif case == "reverse_sleeper_conflict":
        roster.loc[1, "sleeper_id"] = "13324"
    elif case == "duplicate_provider":
        sleeper = pd.concat([sleeper, sleeper], ignore_index=True)
    elif case == "two_providers_one_gsis":
        duplicate = sleeper.copy()
        duplicate.loc[0, "player_id"] = "777"
        sleeper = pd.concat([sleeper, duplicate], ignore_index=True)
    else:
        sleeper.loc[0, "player_id"] = "13324.5"
    with pytest.raises(refresh.RefreshGateError):
        refresh.build_sleeper_bio_mapping_rows(roster, sleeper)


def bio_connection():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE player_bio (NFL_player_id VARCHAR, player VARCHAR, "
                "sleeper_player_id DOUBLE, espn_id VARCHAR, retained VARCHAR)")
    con.execute("INSERT INTO player_bio VALUES "
                "('00-0040879', 'Matthew Hibner', NULL, '4432260', 'keep'), "
                "('other', 'Other Player', 999, '999', 'untouched')")
    return con


def test_mapping_only_upsert_preserves_bio_and_is_idempotent():
    roster, sleeper = witnesses()
    rows = refresh.build_sleeper_bio_mapping_rows(roster, sleeper)
    con = bio_connection()
    try:
        assert refresh.upsert_player_bio_rows(con, rows, table="player_bio", mapping_only=True) == (0, 1)
        after = con.execute("SELECT * FROM player_bio ORDER BY NFL_player_id").fetchall()
        assert after == [
            ("00-0040879", "Matthew Hibner", 13324.0, "4432260", "keep"),
            ("other", "Other Player", 999.0, "999", "untouched"),
        ]
        assert refresh.upsert_player_bio_rows(con, rows, table="player_bio", mapping_only=True) == (0, 0)
        assert con.execute("SELECT * FROM player_bio ORDER BY NFL_player_id").fetchall() == after
    finally:
        con.close()


@pytest.mark.parametrize("case", [
    "existing_conflict", "reverse_conflict", "duplicate_target", "missing_target",
    "duplicate_source", "two_sources_one_provider", "extra_field", "null_provider",
    "null_reverse_owner",
])
def test_mapping_only_upsert_rejects_entire_batch_without_mutation(case):
    con = bio_connection()
    rows = pd.DataFrame([
        {"NFL_player_id": "00-0040879", "sleeper_player_id": 13324},
        {"NFL_player_id": "other", "sleeper_player_id": 999},
    ])
    try:
        if case == "existing_conflict":
            con.execute("UPDATE player_bio SET sleeper_player_id=888 WHERE NFL_player_id='other'")
        elif case == "reverse_conflict":
            con.execute("INSERT INTO player_bio VALUES ('third','Third',13324,NULL,'keep')")
        elif case == "null_reverse_owner":
            con.execute("INSERT INTO player_bio VALUES (NULL,'Orphan',13324,NULL,'keep')")
            # Both valid targets would be changed if the orphan claim were ignored.
            con.execute("UPDATE player_bio SET sleeper_player_id=NULL WHERE NFL_player_id='other'")
        elif case == "duplicate_target":
            con.execute("INSERT INTO player_bio SELECT * FROM player_bio WHERE NFL_player_id='other'")
        elif case == "missing_target":
            rows.loc[1, "NFL_player_id"] = "missing"
        elif case == "duplicate_source":
            rows.loc[1, "NFL_player_id"] = "00-0040879"
        elif case == "two_sources_one_provider":
            rows.loc[1, "sleeper_player_id"] = 13324
        elif case == "extra_field":
            rows["player"] = "must not overwrite"
        else:
            rows.loc[1, "sleeper_player_id"] = None
        before = con.execute("SELECT * FROM player_bio ORDER BY NFL_player_id").fetchall()
        with pytest.raises(refresh.RefreshGateError):
            refresh.upsert_player_bio_rows(con, rows, table="player_bio", mapping_only=True)
        assert con.execute("SELECT * FROM player_bio ORDER BY NFL_player_id").fetchall() == before
    finally:
        con.close()


def test_finite_mapping_repairs_existing_refresh_guard_without_changing_scores_or_history():
    from multi_league.core.league_refresh import resolve_active_player_nfl_ids_from_bio
    from multi_league.core.league_update_validation import (
        IncompleteSourceError, assert_transformed_active_player_scope,
    )

    con = bio_connection()
    try:
        con.execute("ATTACH ':memory:' AS ___ops")
        con.execute("CREATE SCHEMA ___ops.nfl_historical")
        con.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM player_bio")
        con.execute("CREATE SCHEMA public")
        con.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, "
                    "week INTEGER, sleeper_player_id VARCHAR, NFL_player_id VARCHAR, fantasy_points DOUBLE)")
        con.execute("INSERT INTO public.player_fantasy VALUES "
                    "('scope',2026,1,'13324',NULL,2.3), "
                    "('scope',2025,1,'13324',NULL,5.0), "
                    "('other',2026,1,'13324',NULL,8.0)")
        kwargs = dict(db_name="scope", year=2026, weeks=(1,),
                      provider_id_column="sleeper_player_id", expected_keys={(1, "13324")})
        with pytest.raises(IncompleteSourceError, match="1 scored provider players"):
            assert_transformed_active_player_scope(con, **kwargs)
        roster, sleeper = witnesses()
        rows = refresh.build_sleeper_bio_mapping_rows(roster, sleeper)
        refresh.upsert_player_bio_rows(
            con, rows, table="___ops.nfl_historical.player_bio", mapping_only=True,
        )
        assert resolve_active_player_nfl_ids_from_bio(
            con, db_name="scope", active_year=2026, platform="sleeper",
        ) == 1
        assert assert_transformed_active_player_scope(con, **kwargs) == {
            "provider_player_keys": 1, "mapped_scored_players": 1,
        }
        assert con.execute("SELECT db_name,year,NFL_player_id,fantasy_points "
                           "FROM public.player_fantasy ORDER BY db_name,year").fetchall() == [
            ("other", 2026, None, 8.0),
            ("scope", 2025, None, 5.0),
            ("scope", 2026, "00-0040879", 2.3),
        ]
    finally:
        con.close()


def test_normal_sleeper_bio_sync_resolves_scored_missing_link_from_existing_player_cache(tmp_path, monkeypatch):
    from multi_league.core import league_refresh

    league = duckdb.connect(":memory:")
    league.execute("CREATE SCHEMA public")
    league.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR,year INTEGER,"
                   "NFL_player_id VARCHAR,sleeper_player_id VARCHAR,fantasy_points DOUBLE)")
    league.execute("INSERT INTO public.player_fantasy VALUES ('scope',2026,NULL,'13324',2.3),"
                   "('scope',2026,NULL,'555',0),('scope',2025,NULL,'556',2)")
    # Numeric, unresolved IDs must be excluded by score/year, not TRY_CAST.
    source_before = league.execute("SELECT * FROM public.player_fantasy ORDER BY year,sleeper_player_id").fetchall()
    path = tmp_path / 'ops.duckdb'
    cache = duckdb.connect(str(path))
    cache.execute("CREATE SCHEMA nfl_historical")
    cache.execute("CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR,player VARCHAR,"
                  "sleeper_player_id DOUBLE,espn_id VARCHAR,retained VARCHAR)")
    cache.execute("INSERT INTO nfl_historical.player_bio VALUES ('other','Other',999,'999','keep')")
    cache.close()
    live = bio_connection()
    live.execute("CREATE SCHEMA nfl_historical")
    live.execute("CREATE TABLE nfl_historical.player_bio AS SELECT * FROM player_bio")
    roster, sleeper = witnesses()
    queries = []
    directory_years = []

    class Reader:
        def query_df(self, sql, database):
            assert database == '___ops'
            queries.append(sql)
            return live.execute(sql).fetchdf()

    from multi_league.data_fetchers.sleeper.sleeper_player_cache import SleeperPlayerCache

    class Client:
        def get_all_players(self, sport):
            assert sport == 'nfl'
            return {'13324': sleeper.iloc[0].to_dict()}

    player_cache = SleeperPlayerCache(tmp_path / 'sleeper')
    player_cache.refresh_if_stale(Client())

    def load_directory(year):
        directory_years.append(year)
        return roster.copy()

    monkeypatch.setattr(refresh, 'load_nflverse_identity_roster', load_directory)
    try:
        receipt = league_refresh.sync_sleeper_scored_bio_crosswalk(
            Reader(), league, ops_cache=path, player_cache=player_cache,
            db_name='scope', active_year=2026,
        )
        assert receipt == {"mapped": 1}
        assert directory_years == [2026]
        cache = duckdb.connect(str(path), read_only=True)
        try:
            assert cache.execute("SELECT NFL_player_id,sleeper_player_id FROM nfl_historical.player_bio "
                                 "ORDER BY NFL_player_id").fetchall() == [('00-0040879',13324.0),('other',999.0)]
        finally:
            cache.close()
        assert len(queries) == 1
        assert live.execute("SELECT sleeper_player_id FROM player_bio WHERE NFL_player_id='00-0040879'").fetchone() == (None,)
        def forbidden_roster(_year):
            raise AssertionError('an exact cached link must not refetch the roster')
        monkeypatch.setattr(refresh, 'load_nflverse_identity_roster', forbidden_roster)
        assert league_refresh.sync_sleeper_scored_bio_crosswalk(
            Reader(), league, ops_cache=path, player_cache=player_cache,
            db_name='scope', active_year=2026,
        ) == {"mapped": 0}
        assert len(queries) == 1
        assert directory_years == [2026]
        assert league.execute("SELECT * FROM public.player_fantasy ORDER BY year,sleeper_player_id").fetchall() == source_before
    finally:
        league.close()
        live.close()
