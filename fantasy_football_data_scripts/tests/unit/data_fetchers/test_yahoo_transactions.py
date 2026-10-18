"""Regression tests for Yahoo transaction parsing edge cases."""

import pandas as pd
import pytest
from types import SimpleNamespace

from multi_league.data_fetchers.yahoo.yahoo_transactions import (
    _resolve_transaction_team_keys,
    backfill_unknown_drop_managers,
    process_transactions_chronologically,
    load_matchup_windows,
    map_transaction_to_week,
)


@pytest.mark.parametrize("retry", [False, True])
def test_playerless_audit_entries_do_not_block_real_moves(monkeypatch, retry):
    from datetime import UTC, datetime
    from xml.etree import ElementTree as ET
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher

    last_day = str(int(datetime(2025, 12, 29, 12, tzinfo=UTC).timestamp()))
    audit_day = str(int(datetime(2025, 12, 30, 12, tzinfo=UTC).timestamp()))
    empty = (f"<transaction><transaction_key>273</transaction_key><type>commish</type>"
             f"<status>successful</status><timestamp>{audit_day}</timestamp></transaction>"
             f"<transaction><type>trade</type><timestamp>{audit_day}</timestamp><players count='0'/></transaction>")
    moves = "".join(
        f"<transaction><transaction_key>{index}</transaction_key><type>{kind}</type>"
        f"<status>successful</status><timestamp>{last_day}</timestamp><players count='1'><player>"
        f"<player_key>461.p.{index}</player_key><name><full>Player {index}</full></name>"
        f"<transaction_data><type>{kind}</type><source_type>freeagents</source_type>"
        "<destination_type>team</destination_type><destination_team_key>461.l.236822.t.1</destination_team_key>"
        "</transaction_data></player></players></transaction>"
        for index, kind in enumerate(("add", "drop", "trade", "commish"), 1)
    )
    calls = []

    def response(url, oauth):
        calls.append(url)
        if retry and len(calls) == 1:
            raise fetcher.RecoverableAPIError("injected transport failure")
        return ET.fromstring(f"<fantasy_content><transactions>{empty}{moves}</transactions></fantasy_content>")

    monkeypatch.setattr(fetcher, "fetch_url", response)
    monkeypatch.setattr(fetcher.time, "sleep", lambda _seconds: None)
    windows = pd.DataFrame([{"year": 2025, "week": 17, "week_start": "2025-12-23",
                             "week_end": "2025-12-29", "cumulative_week": 202517}])
    result = fetcher.fetch_transactions_for_year(None, "461.l.236822", 2025,
        {"461.l.236822.t.1": "Manager"}, {"461.l.236822.t.1": "guid"},
        {"461.l.236822.t.1": "Team"}, windows)

    assert [(row.transaction_type, row.week, row.cumulative_week) for row in result] == [
        ("add", 17, 202517), ("drop", 17, 202517), ("trade", 17, 202517), ("commish", 17, 202517)]
    assert [row.player_key for row in result] == ["461.p.1", "461.p.2", "461.p.3", "461.p.4"]
    assert len(calls) == (2 if retry else 1)


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("kind", ["add", "drop", "trade", "commish"])
def test_player_moves_still_require_verified_dates(monkeypatch, retry, kind):
    from datetime import UTC, datetime
    from xml.etree import ElementTree as ET
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
    from multi_league.core.transaction_periods import TransactionWeekError

    stamp = int(datetime(2025, 12, 30, 12, tzinfo=UTC).timestamp())
    calls = []

    def response(url, oauth):
        calls.append(url)
        if retry and len(calls) == 1:
            raise fetcher.RecoverableAPIError("injected transport failure")
        return ET.fromstring(f"<fantasy_content><transactions><transaction><type>{kind}</type>"
            f"<timestamp>{stamp}</timestamp><players><player><player_key>461.p.1</player_key>"
            "</player></players></transaction></transactions></fantasy_content>")

    monkeypatch.setattr(fetcher, "fetch_url", response)
    monkeypatch.setattr(fetcher.time, "sleep", lambda _seconds: None)
    windows = pd.DataFrame([{"year": 2025, "week": 17, "week_start": "2025-12-23", "week_end": "2025-12-29"}])
    with pytest.raises(TransactionWeekError, match="transaction week"):
        fetcher.fetch_transactions_for_year(None, "461.l.236822", 2025, {}, {}, {}, windows)


def test_matchup_windows_prefer_dated_rows_over_null_duplicates(tmp_path):
    class LocalDB:
        def read_table(self, table, year):
            return pd.DataFrame([
                {'year': 2024, 'week': 6, 'week_start': None, 'week_end': None},
                {'year': 2024, 'week': 6, 'week_start': '2024-10-08', 'week_end': '2024-10-14'},
            ])

    windows = load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path), 2024, LocalDB())
    assert map_transaction_to_week('1728520236', 2024, windows) == (6, '2024-10-08', '2024-10-14')


def test_matchup_windows_do_not_hide_conflicting_dates(tmp_path):
    class LocalDB:
        def read_table(self, table, year):
            return pd.DataFrame([
                {"year":2024,"week":6,"week_start":"2024-10-08","week_end":"2024-10-14"},
                {"year":2024,"week":6,"week_start":"2024-10-09","week_end":"2024-10-15"},
            ])
    windows = load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path), 2024, LocalDB())
    with pytest.raises(ValueError, match="transaction week"):
        map_transaction_to_week('1728520236', 2024, windows)


@pytest.mark.parametrize('source', ['local', 'cache'])
def test_active_transaction_calendar_is_not_limited_to_final_games(tmp_path, monkeypatch, source):
    from datetime import UTC, datetime
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
    partial = pd.DataFrame([{'year':2026,'week':4,'week_start':'2026-09-29','week_end':'2026-10-05'}])
    current = pd.DataFrame([
        {'year':2026,'week':5,'week_start':'2026-10-06','week_end':'2026-10-12'},
        {'year':2026,'week':6,'week_start':'2026-10-13','week_end':'2026-10-19'},
    ])
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026,10,11,12,tzinfo=UTC)
    class LocalDB:
        def read_table(self, table, year):
            return partial
    monkeypatch.setattr(fetcher, 'datetime', Clock)
    monkeypatch.setattr(fetcher, 'get_current_nfl_season_year', lambda:2026)
    monkeypatch.setattr(fetcher, 'build_week_windows_from_yahoo', lambda ctx,year:current)
    if source == 'cache':
        partial.to_parquet(tmp_path/'matchup_windows_2026.parquet',index=False)
    windows = fetcher.load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path),2026,
                                           LocalDB() if source == 'local' else None)
    stamp = str(int(datetime(2026,10,11,11,tzinfo=UTC).timestamp()))
    assert fetcher.map_transaction_to_week(stamp,2026,windows) == (5,'2026-10-06','2026-10-12')


def test_yahoo_calendar_uses_one_bulk_request_and_actual_response_weeks(monkeypatch):
    from xml.etree import ElementTree as ET
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
    calls = []
    class Game:
        def __init__(self,*args): pass
        def to_league(self,key):
            assert key == '470.l.40871'
            return SimpleNamespace(settings=lambda:{'start_week':1,'end_week':17})
    def response(url,oauth):
        calls.append(url)
        return ET.fromstring('<fantasy_content><matchups>'
            '<matchup><week>4</week><week_start>2026-09-29</week_start><week_end>2026-10-05</week_end></matchup>'
            '<matchup><week>5</week><week_start>2026-10-06</week_start><week_end>2026-10-12</week_end></matchup>'
            '<matchup><week>5</week><week_start>2026-10-06</week_start><week_end>2026-10-12</week_end></matchup>'
            '<matchup><week>6</week><week_start>2026-10-13</week_start><week_end>2026-10-19</week_end></matchup>'
            '</matchups></fantasy_content>')
    monkeypatch.setattr(fetcher.yfa,'Game',Game)
    monkeypatch.setattr(fetcher,'fetch_url',response)
    ctx = SimpleNamespace(get_oauth_session=lambda:None,game_code='nfl',get_league_id_for_year=lambda year:'470.l.40871')
    windows = fetcher.build_week_windows_from_yahoo(ctx,2026)
    assert windows['week'].tolist() == [4,5,6]
    assert calls == ['https://fantasysports.yahooapis.com/fantasy/v2/league/470.l.40871/scoreboard;week='+','.join(str(w) for w in range(1,18))]


def test_recovery_reuses_retained_calendar_before_final_matchups():
    from multi_league.data_fetchers.yahoo.yahoo_recovery_api import _build_matchup_windows
    class LocalDB:
        def read_table(self, table, year):
            week = 5 if table == '__yahoo_transaction_windows' else 4
            return pd.DataFrame([{'year':2026,'week':week,
                'week_start':'2026-10-06' if week == 5 else '2026-09-29',
                'week_end':'2026-10-12' if week == 5 else '2026-10-05'}])
    windows = _build_matchup_windows(LocalDB(),2026)
    assert windows['week'].tolist() == [4,5]
    assert windows['cumulative_week'].tolist() == [202604,202605]
    assert map_transaction_to_week('1790769600',2026,windows)[0] == 4


@pytest.mark.parametrize('source', ['local','cache'])
@pytest.mark.parametrize('clock', [(2026,8,1),(2027,1,10)])
def test_verified_offseason_calendar_does_not_require_today(tmp_path, monkeypatch, source, clock):
    from datetime import UTC, datetime
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
    windows = pd.DataFrame([{'year':2026,'week':week,
        'week_start':start.strftime('%Y-%m-%d'),
        'week_end':(start+pd.Timedelta(days=6)).strftime('%Y-%m-%d')}
        for week,start in enumerate(pd.date_range('2026-09-08',periods=17,freq='7D'),1)])
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None): return datetime(*clock,tzinfo=UTC)
    class LocalDB:
        def read_table(self,table,year):
            if table == 'league_settings':
                return pd.DataFrame([{'year':2026,'start_week':1,'end_week':17}])
            if source == 'local':
                return windows
            raise KeyError(table)
    monkeypatch.setattr(fetcher,'datetime',Clock)
    monkeypatch.setattr(fetcher,'get_current_nfl_season_year',lambda:2026)
    def no_request(*args): pytest.fail('Verified offseason calendar should not need a Yahoo request')
    monkeypatch.setattr(fetcher,'build_week_windows_from_yahoo',no_request)
    if source == 'cache':
        windows.to_parquet(tmp_path/'matchup_windows_2026.parquet',index=False)
    result = fetcher.load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path),2026,LocalDB())
    stamp = str(int(datetime(2026,9,10,12,tzinfo=UTC).timestamp()))
    assert map_transaction_to_week(stamp,2026,result) == (1,'2026-09-08','2026-09-14')


def test_postseason_last_week_only_calendar_is_not_treated_as_complete(tmp_path, monkeypatch):
    from datetime import UTC, datetime
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
    final = pd.DataFrame([{'year':2026,'week':17,'week_start':'2026-12-29','week_end':'2027-01-04'}])
    native = pd.concat([pd.DataFrame([{'year':2026,'week':1,'week_start':'2026-09-08','week_end':'2026-09-14'}]),final])
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None): return datetime(2027,1,10,tzinfo=UTC)
    class LocalDB:
        def read_table(self,table,year):
            if table == 'league_settings':
                return pd.DataFrame([{'start_week':1,'end_week':17}])
            return final
    monkeypatch.setattr(fetcher,'datetime',Clock)
    monkeypatch.setattr(fetcher,'get_current_nfl_season_year',lambda:2026)
    monkeypatch.setattr(fetcher,'build_week_windows_from_yahoo',lambda *args:native)
    result = fetcher.load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path),2026,LocalDB())
    stamp = str(int(datetime(2026,9,10,12,tzinfo=UTC).timestamp()))
    assert map_transaction_to_week(stamp,2026,result)[0] == 1


@pytest.mark.parametrize('provided', [True, False])
def test_fetch_retains_independent_live_week_calendar(tmp_path, monkeypatch, provided):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher

    windows = pd.DataFrame([
        {'year':2024,'week':5,'week_start':'2024-10-01','week_end':'2024-10-07'},
        {'year':2024,'week':6,'week_start':'2024-10-08','week_end':'2024-10-14'},
    ])
    ctx = SimpleNamespace(get_oauth_session=lambda:None, game_code='nfl',
        get_league_id_for_year=lambda year:'449.l.123', manager_name_overrides={}, data_directory=tmp_path)
    monkeypatch.setattr(fetcher.yfa, 'Game', lambda *args:None)
    monkeypatch.setattr(fetcher, 'fetch_team_mappings', lambda *args,**kwargs:({}, {}, {}))
    monkeypatch.setattr(fetcher, 'load_matchup_windows', lambda *args,**kwargs:windows)
    monkeypatch.setattr(fetcher, 'fetch_transactions_for_year', lambda *args,**kwargs:[])
    with LocalLeagueDB(tmp_path, 'qa') as db:
        result = fetcher.fetch_transactions(ctx, 2024, windows if provided else None, local_db=db)
        assert result.empty
        assert db.connect().execute('SELECT db_name,year,week,week_start::VARCHAR,week_end::VARCHAR '
            'FROM public.__yahoo_transaction_windows ORDER BY week').fetchall() == [
            ('qa',2024,5,'2024-10-01','2024-10-07'), ('qa',2024,6,'2024-10-08','2024-10-14')]
        # The next fetch replaces this year's witness, not other years/leagues.
        fetcher.fetch_transactions(ctx, 2024, windows.iloc[:1], local_db=db)
        assert db.connect().execute('SELECT week FROM public.__yahoo_transaction_windows').fetchall() == [(5,)]


@pytest.mark.parametrize('windows,stamp', [
    ([], '1728520236'),
    ([{'year':2024,'week':6,'week_start':None,'week_end':None}], '1728520236'),
    ([{'year':2024,'week':6,'week_start':'2024-10-08','week_end':'2024-10-14'}], 'invalid'),
    ([{'year':2024,'week':5,'week_start':'2024-10-01','week_end':'2024-10-07'},
      {'year':2024,'week':7,'week_start':'2024-10-15','week_end':'2024-10-21'}], '1728520236'),
    ([{'year':2024,'week':6,'week_start':'2024-10-08','week_end':'2024-10-14'},
      {'year':2024,'week':7,'week_start':'2024-10-08','week_end':'2024-10-14'}], '1728520236'),
])
def test_yahoo_unverified_transaction_week_is_not_fabricated(windows, stamp):
    frame = pd.DataFrame(windows, columns=['year','week','week_start','week_end'])
    with pytest.raises(ValueError, match='transaction week'):
        map_transaction_to_week(stamp, 2024, frame)


@pytest.mark.parametrize("stamp,week", [(1728345599, 5), (1728345600, 6), (1728345601, 6)])
def test_yahoo_week_changes_at_utc_midnight(stamp, week):
    windows = pd.DataFrame([
        {"year":2024, "week":5, "week_start":"2024-10-01", "week_end":"2024-10-07"},
        {"year":2024, "week":6, "week_start":"2024-10-08", "week_end":"2024-10-14"},
    ])
    assert map_transaction_to_week(str(stamp), 2024, windows)[0] == week


def test_resolve_transaction_team_keys_falls_back_to_parent_drop_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="drop",
        destination="waivers",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "461.l.90939.t.7",
            "destination_team_key": "",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.7"
    assert source_key == "461.l.90939.t.7"
    assert destination_key == ""


def test_resolve_transaction_team_keys_falls_back_to_parent_add_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="add",
        destination="team",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "",
            "destination_team_key": "461.l.90939.t.4",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.4"
    assert source_key == ""
    assert destination_key == "461.l.90939.t.4"


def test_backfill_unknown_drop_managers_uses_last_known_owner():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx-1", "tx-2"],
            "year": [2025, 2025],
            "week": [2, 4],
            "timestamp": [1_000, 2_000],
            "transaction_type": ["add", "drop"],
            "manager": ["Alice", "Unknown"],
            "manager_guid": ["guid-a", None],
            "team_name": ["Alpha", None],
            "yahoo_player_id": ["12345", "12345"],
            "player_key": ["461.p.12345", "461.p.12345"],
            "player": ["Player One", "Player One"],
            "destination": ["team", "waivers"],
        }
    )

    ordered = process_transactions_chronologically(df)
    repaired = backfill_unknown_drop_managers(ordered)

    repaired_drop = repaired.loc[repaired["transaction_id"] == "tx-2"].iloc[0]
    assert repaired_drop["manager"] == "Alice"
    assert repaired_drop["manager_guid"] == "guid-a"
    assert repaired_drop["team_name"] == "Alpha"
