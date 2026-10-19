"""Recovery must use the same dated calendar and aliases as normal imports."""

from datetime import UTC, datetime
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pandas as pd
import pytest

from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.transaction_periods import TransactionWeekError
from multi_league.data_fetchers.yahoo import yahoo_recovery as recovery
from multi_league.data_fetchers.yahoo import yahoo_transactions as transactions


@pytest.fixture
def season(tmp_path, monkeypatch):
    league_key = "380.l.249835"
    oauth = object()
    ctx = SimpleNamespace(
        data_directory=tmp_path, matchup_data_directory=tmp_path / "matchup_data",
        league_ids={"2018": league_key}, game_code="nfl", oauth=oauth,
        get_oauth_session=lambda: oauth, get_league_id_for_year=lambda year: league_key,
        manager_name_overrides={"Owner": "Preferred alias"},
    )
    ctx.matchup_data_directory.mkdir()
    monkeypatch.setattr(transactions.yfa, "Game", lambda *_args: SimpleNamespace(
        to_league=lambda key: SimpleNamespace(settings=lambda: {"start_week": 1, "end_week": 17})))
    calls = []
    stamp = str(int(datetime(2018, 10, 11, 12, tzinfo=UTC).timestamp()))

    def response(url, session):
        assert session is oauth
        calls.append(url)
        if "/scoreboard;" in url:
            return ET.fromstring("<fantasy_content><matchups><matchup><week>6</week>"
                "<week_start>2018-10-09</week_start><week_end>2018-10-15</week_end>"
                "</matchup></matchups></fantasy_content>")
        if url.endswith("/teams"):
            return ET.fromstring(f"<fantasy_content><teams><team><team_key>{league_key}.t.1</team_key>"
                "<name>Team</name><managers><manager><nickname>Owner</nickname><guid>owner-guid</guid>"
                "</manager></managers></team></teams></fantasy_content>")
        assert "/transactions" in url
        return ET.fromstring("<fantasy_content><transactions><transaction>"
            f"<transaction_key>{league_key}.tr.1</transaction_key><type>add</type>"
            f"<status>successful</status><timestamp>{stamp}</timestamp><players><player>"
            "<player_key>380.p.123</player_key><name><full>Player</full></name><transaction_data>"
            f"<type>add</type><source_type>freeagents</source_type><destination_type>team</destination_type>"
            f"<destination_team_key>{league_key}.t.1</destination_team_key>"
            "</transaction_data></player></players></transaction></transactions></fantasy_content>")

    monkeypatch.setattr(transactions, "fetch_url", response)
    with LocalLeagueDB(tmp_path, "league") as db:
        for table in ("matchup", "transactions", "league_settings"):
            db.ensure_table(table)
        db.connect().execute("INSERT INTO public.matchup (db_name,year,week,manager,team_points) "
                             "VALUES ('league',2017,1,'Historical alias',123.5)")
        yield ctx, db, calls, response


@pytest.mark.parametrize("calendar_source", ["cache", "provider"])
def test_recovery_fetches_real_transactions_without_final_matchup_dates(season, calendar_source):
    ctx, db, calls, _response = season
    if calendar_source == "cache":
        pd.DataFrame([{"year": 2018, "week": 6, "week_start": "2018-10-09",
                       "week_end": "2018-10-15", "cumulative_week": 201806}]).to_parquet(
            ctx.matchup_data_directory / "matchup_windows_2018.parquet", index=False)
    gap = recovery.Gap("transaction", 2018, "full_year", "manifest")
    assert recovery._resolve_transaction_gap(gap, ctx, ctx.data_directory, local_db=db) is True
    assert db.connect().execute("SELECT manager,year,week,cumulative_week,yahoo_player_id "
                                "FROM public.transactions").fetchall() == [
        ("Preferred alias", 2018, 6, 201806, "123")]
    assert db.connect().execute("SELECT year,week,manager,team_points FROM public.matchup").fetchall() == [
        (2017, 1, "Historical alias", 123.5)]
    assert db.connect().execute("SELECT year,week,week_start::VARCHAR,week_end::VARCHAR "
                                "FROM public.__yahoo_transaction_windows").fetchall() == [
        (2018, 6, "2018-10-09", "2018-10-15")]
    assert sum("/scoreboard;" in url for url in calls) == (calendar_source == "provider")


def test_denied_calendar_defers_recovery_without_erasing_transactions(season, monkeypatch):
    ctx, db, calls, response = season
    def denied_calendar(url, oauth):
        if "/scoreboard;" in url:
            calls.append(url)
            raise RuntimeError("Yahoo API access denied")
        return response(url, oauth)
    monkeypatch.setattr(transactions, "fetch_url", denied_calendar)
    gap = recovery.Gap("transaction", 2018, "full_year", "manifest")
    result = recovery._try_resolve_gap(gap, ctx, ctx.data_directory, local_db=db)
    assert result == "rate_limited"
    assert gap.hard_missing is False
    assert not any("/transactions" in url for url in calls)


def test_recovery_still_rejects_real_transactions_with_invalid_timestamps(season, monkeypatch):
    ctx, db, _calls, response = season
    def invalid_timestamp(url, oauth):
        root = response(url, oauth)
        for stamp in root.findall(".//transaction/timestamp"):
            stamp.text = "invalid"
        return root
    monkeypatch.setattr(transactions, "fetch_url", invalid_timestamp)
    gap = recovery.Gap("transaction", 2018, "full_year", "manifest")
    with pytest.raises(TransactionWeekError, match="invalid timestamp"):
        recovery._resolve_transaction_gap(gap, ctx, ctx.data_directory, local_db=db)
    assert db.connect().execute("SELECT COUNT(*) FROM public.transactions").fetchone() == (0,)
