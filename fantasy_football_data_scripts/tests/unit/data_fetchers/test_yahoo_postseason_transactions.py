"""Real roster moves can occur after a fantasy league's championship."""
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pandas as pd
import pytest

from multi_league.data_fetchers.yahoo import yahoo_transactions as fetcher
from multi_league.core.transaction_periods import (
    TransactionWeekError, validate_transaction_weeks,
)


def league_calendar():
    return pd.DataFrame([dict(year=2014, week=16, week_start="2014-12-16",
                             week_end="2014-12-22", cumulative_week=201416)])


def transaction_xml(stamp="1419411993", kind="add/drop"):
    # Native QC Waggle record: Dec 24, 2014 09:06:33 UTC, after its week-16 final.
    return ET.fromstring(f"""<fantasy_content><transactions><transaction>
      <transaction_key>331.l.44173.tr.226</transaction_key><type>{kind}</type>
      <status>successful</status><timestamp>{stamp}</timestamp><players>
      <player><player_key>331.p.1</player_key><name><full>Added player</full></name>
        <transaction_data><type>add</type><destination_type>team</destination_type>
        <destination_team_key>331.l.44173.t.1</destination_team_key></transaction_data></player>
      <player><player_key>331.p.2</player_key><name><full>Dropped player</full></name>
        <transaction_data><type>drop</type><source_type>team</source_type>
        <source_team_key>331.l.44173.t.1</source_team_key>
        <destination_type>waivers</destination_type></transaction_data></player>
      </players></transaction></transactions></fantasy_content>""")


def native_calendar(end="2014-12-28"):
    return ET.fromstring(f"""<fantasy_content><game><game_weeks>
      <game_week><week>16</week><start>2014-12-16</start><end>2014-12-22</end></game_week>
      <game_week><week>17</week><start>2014-12-23</start><end>{end}</end></game_week>
      </game_weeks></game></fantasy_content>""")


@pytest.mark.parametrize("retry", [False, True])
def test_real_post_championship_add_and_drop_use_native_week_once(monkeypatch, retry):
    calls = []

    def get(url, oauth):
        calls.append(url)
        if retry and len(calls) == 1:
            raise fetcher.RecoverableAPIError("transport interrupted")
        if url.endswith("game/331/game_weeks"):
            return native_calendar()
        return transaction_xml()

    monkeypatch.setattr(fetcher, "fetch_url", get)
    monkeypatch.setattr(fetcher.time, "sleep", lambda _: None)
    result = fetcher.fetch_transactions_for_year(None, "331.l.44173", 2014,
        {"331.l.44173.t.1": "Manager"}, {"331.l.44173.t.1": "guid"},
        {"331.l.44173.t.1": "Team"}, league_calendar())
    assert [(r.transaction_type, r.player_key, r.week, r.cumulative_week,
             r.week_start, r.week_end) for r in result] == [
        ("add", "331.p.1", 17, 201417, "2014-12-23", "2014-12-28"),
        ("drop", "331.p.2", 17, 201417, "2014-12-23", "2014-12-28")]
    assert sum(url.endswith("game/331/game_weeks") for url in calls) == 1


def test_normal_moves_do_not_request_another_calendar(monkeypatch):
    def get(url, oauth):
        assert "/transactions;" in url, "No additional calendar request for normal moves"
        return transaction_xml("1418982393")  # Dec 19, within week 16.
    monkeypatch.setattr(fetcher, "fetch_url", get)
    result = fetcher.fetch_transactions_for_year(None, "331.l.44173", 2014,
        {"331.l.44173.t.1": "Manager"}, {}, {}, league_calendar())
    assert [row.week for row in result] == [16, 16]


def test_calendar_transport_retry_does_not_duplicate_parsed_page_prefix(monkeypatch):
    page = transaction_xml("1418982393")
    page.find('.//transaction_key').text = '331.l.44173.tr.225'
    page.find('.//transactions').append(transaction_xml().find('.//transaction'))
    calendar_requests = []
    def get(url, oauth):
        if url.endswith('game_weeks'):
            calendar_requests.append(url)
            if len(calendar_requests) == 1:
                raise fetcher.RecoverableAPIError('calendar transport interrupted')
            return native_calendar()
        return page
    monkeypatch.setattr(fetcher, 'fetch_url', get)
    monkeypatch.setattr(fetcher.time, 'sleep', lambda _: None)
    rows = fetcher.fetch_transactions_for_year(None, '331.l.44173', 2014,
        {'331.l.44173.t.1': 'Manager'}, {}, {}, league_calendar())
    assert [(r.transaction_id, r.player_key, r.week) for r in rows] == [
        ('331.l.44173.tr.225', '331.p.1', 16), ('331.l.44173.tr.225', '331.p.2', 16),
        ('331.l.44173.tr.226', '331.p.1', 17), ('331.l.44173.tr.226', '331.p.2', 17)]


def test_calendar_retention_retries_before_rows_are_returned(monkeypatch):
    attempts = []
    retained = []
    def retain(calendar):
        attempts.append(calendar)
        if len(attempts) == 1:
            raise RuntimeError('local retention interrupted')
        retained.append(calendar)
    monkeypatch.setattr(fetcher, 'fetch_url', lambda url, oauth:
        native_calendar() if url.endswith('game_weeks') else transaction_xml())
    monkeypatch.setattr(fetcher.time, 'sleep', lambda _: None)
    rows = fetcher.fetch_transactions_for_year(None, '331.l.44173', 2014,
        {'331.l.44173.t.1': 'Manager'}, {}, {}, league_calendar(), on_calendar_extended=retain)
    assert [r.week for r in rows] == [17, 17]
    assert len(attempts) == 2
    assert len(retained) == 1 and retained[0]['week'].tolist() == [16, 17]


@pytest.mark.parametrize("stamp", ["invalid", "0", "1419930393"])
def test_missing_or_uncovered_dates_are_not_guessed(monkeypatch, stamp):
    def get(url, oauth):
        return native_calendar() if url.endswith("game_weeks") else transaction_xml(stamp)
    monkeypatch.setattr(fetcher, "fetch_url", get)
    with pytest.raises(TransactionWeekError, match="transaction week"):
        fetcher.fetch_transactions_for_year(None, "331.l.44173", 2014, {}, {}, {}, league_calendar())


def test_extended_independent_calendar_is_retained_for_publication(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    ctx = SimpleNamespace(get_oauth_session=lambda: None, game_code="nfl",
        get_league_id_for_year=lambda year: "331.l.44173", manager_name_overrides={},
        data_directory=tmp_path)
    monkeypatch.setattr(fetcher.yfa, "Game", lambda *args: None)
    monkeypatch.setattr(fetcher, "fetch_team_mappings", lambda *args, **kwargs:
        ({"331.l.44173.t.1": "Manager"}, {"331.l.44173.t.1": "guid"},
         {"331.l.44173.t.1": "Team"}))
    monkeypatch.setattr(fetcher, "fetch_url", lambda url, oauth:
        native_calendar() if url.endswith("game_weeks") else transaction_xml())
    with LocalLeagueDB(tmp_path, "qa") as db:
        frame = fetcher.fetch_transactions(ctx, 2014, league_calendar(), local_db=db)
        assert frame["week"].tolist() == [17, 17]
        conn = db.connect()
        assert conn.execute("SELECT week, week_start::VARCHAR, week_end::VARCHAR FROM "
            "public.__yahoo_transaction_windows ORDER BY week").fetchall() == [
                (16, "2014-12-16", "2014-12-22"), (17, "2014-12-23", "2014-12-28")]
        conn.register("real_moves", frame)
        conn.execute("CREATE TABLE public.transactions AS SELECT 'qa' AS db_name, 'yahoo' AS platform, * FROM real_moves")
        validate_transaction_weeks(conn, ["qa"])
        assert conn.execute("SELECT COUNT(*) FROM public.transactions").fetchone()[0] == 2
