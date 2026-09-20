from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb
import pandas as pd

from multi_league.data_fetchers.yahoo.yahoo_rosters import (
    YahooRosterFetcher,
    _align_week_count_to_matchups,
    _resolve_roster_weeks,
    get_weeks_from_local_matchup_data,
    get_max_week_from_local_matchup_data,
)


class _FakeLocalDB:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn

    def table_exists(self, table_name: str) -> bool:
        row = self._conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])


def _build_fetcher(monkeypatch, tmp_path) -> YahooRosterFetcher:
    monkeypatch.setattr(YahooRosterFetcher, "_initialize_oauth", lambda self, oauth_file: object())
    return YahooRosterFetcher(Path("Oauth.json"), "414.l.413370", output_dir=tmp_path)


def _player_xml(points_xml: str = "", stats_xml: str = "") -> ET.Element:
    xml = f"""
    <fantasy_content>
      <team>
        <roster>
          <players>
            <player>
              <player_key>414.p.1234</player_key>
              <player_id>1234</player_id>
              <name><full>Chris Olave</full></name>
              <editorial_team_abbr>NO</editorial_team_abbr>
              <display_position>WR</display_position>
              <primary_position>WR</primary_position>
              <selected_position><position>WR</position></selected_position>
              {stats_xml}
              {points_xml}
            </player>
          </players>
        </roster>
      </team>
    </fantasy_content>
    """
    return ET.fromstring(xml)


def test_fetch_roster_for_week_missing_points_stays_null(monkeypatch, tmp_path):
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    monkeypatch.setattr(fetcher, "_fetch_url_xml", lambda url: _player_xml())

    rows = fetcher.fetch_roster_for_week(2024, 6, "414.l.413370.t.1", "Adin")

    assert rows[0]["fantasy_points"] is None


def test_fetch_roster_for_week_zero_points_preserved(monkeypatch, tmp_path):
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    monkeypatch.setattr(
        fetcher,
        "_fetch_url_xml",
        lambda url: _player_xml("<player_points><total>0</total></player_points>"),
    )

    rows = fetcher.fetch_roster_for_week(2024, 6, "414.l.413370.t.1", "Adin")

    assert rows[0]["fantasy_points"] == 0.0
    assert rows[0]["yahoo_official_points"] == 0.0


def test_fetch_roster_for_week_blank_points_stays_null(monkeypatch, tmp_path):
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    monkeypatch.setattr(
        fetcher,
        "_fetch_url_xml",
        lambda url: _player_xml("<player_points><total></total></player_points>"),
    )

    rows = fetcher.fetch_roster_for_week(2024, 6, "414.l.413370.t.1", "Adin")

    assert rows[0]["fantasy_points"] is None


def test_fetch_roster_for_week_preserves_yahoo_stat_ids(monkeypatch, tmp_path):
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    requested_urls = []

    def _fetch_url_xml(url):
        requested_urls.append(url)
        return _player_xml(
            "<player_points><total>16.5</total></player_points>",
            """
            <player_stats>
              <stats>
                <stat><stat_id>11</stat_id><value>5</value></stat>
                <stat><stat_id>12</stat_id><value>40</value></stat>
                <stat><stat_id>13</stat_id><value>1</value></stat>
              </stats>
            </player_stats>
            """,
        )

    monkeypatch.setattr(fetcher, "_fetch_url_xml", _fetch_url_xml)

    rows = fetcher.fetch_roster_for_week(2024, 6, "414.l.413370.t.1", "Adin")

    assert rows[0]["fantasy_points"] == 16.5
    assert rows[0]["yahoo_official_points"] == 16.5
    assert rows[0]["yahoo_stats_available"] is True
    assert rows[0]["yahoo_stat_11"] == 5.0
    assert rows[0]["yahoo_stat_12"] == 40.0
    assert rows[0]["yahoo_stat_13"] == 1.0
    assert requested_urls == [
        "https://fantasysports.yahooapis.com/fantasy/v2/team/414.l.413370.t.1/"
        "roster;week=6/players/stats;type=week;week=6"
    ]


def test_fetch_all_rosters_for_week_uses_native_weekly_team_roster(monkeypatch, tmp_path):
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    fetcher.rate_limit = 0
    requested_urls = []

    def _fetch_url_xml(url):
        requested_urls.append(url)
        return _player_xml()

    monkeypatch.setattr(fetcher, "_fetch_url_xml", _fetch_url_xml)

    df, failures = fetcher.fetch_all_rosters_for_week(
        2024,
        6,
        {
            "414.l.413370.t.1": {
                "manager_name": "Adin",
                "manager_guid": "GUID1234",
            }
        },
    )

    assert failures == []
    assert requested_urls == [
        "https://fantasysports.yahooapis.com/fantasy/v2/team/414.l.413370.t.1/roster;week=6/players"
    ]
    assert len(df) == 1
    row = df.iloc[0]
    assert row["manager_name"] == "Adin"
    assert row["fantasy_position"] == "WR"
    assert row["player_name"] == "Chris Olave"
    assert pd.isna(row["fantasy_points"])


def test_fetch_teams_resolves_hidden_yahoo_guid_to_team_identity(monkeypatch, tmp_path):
    """Hidden Yahoo owners must share the matchup fetcher's fallback identity."""
    fetcher = _build_fetcher(monkeypatch, tmp_path)
    monkeypatch.setattr(
        fetcher,
        "_fetch_url_xml",
        lambda url: ET.fromstring(
            """
            <fantasy_content>
              <league><teams><team>
                <team_key>348.l.727365.t.1</team_key>
                <name>Jabba Juice</name>
                <managers><manager>
                  <guid>--</guid>
                  <nickname>--</nickname>
                  <image_url>https://s.yimg.com/default_user_profile.png</image_url>
                </manager></managers>
              </team></teams></league>
            </fantasy_content>
            """
        ),
    )

    teams = fetcher.fetch_teams()

    assert teams["348.l.727365.t.1"]["manager_guid"] == "yh-team-348.l.727365.t.1"


def test_local_matchup_max_week_ignores_unplayed_zero_score_week():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?)",
        [
            (2024, 1, 101.2, 95.4),
            (2024, 16, 88.0, 90.5),
            (2024, 17, 0.0, 0.0),
            (2025, 17, 120.0, 117.3),
        ],
    )

    local_db = _FakeLocalDB(conn)

    assert get_max_week_from_local_matchup_data(local_db, 2024) == 16
    assert get_max_week_from_local_matchup_data(local_db, 2025) == 17


def test_local_matchup_weeks_preserve_start_week_and_ignore_unplayed_zero_score_week():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?)",
        [
            (2005, 1, 0.0, 0.0),
            (2005, 2, 101.2, 95.4),
            (2005, 3, 88.0, 90.5),
        ],
    )

    assert get_weeks_from_local_matchup_data(_FakeLocalDB(conn), 2005) == [2, 3]


def test_resolve_roster_weeks_honors_yahoo_start_week_without_matchups():
    settings = {"start_week": 2, "end_week": 16}

    assert _resolve_roster_weeks(settings, 2004, None, local_db=None) == list(range(2, 17))


def test_resolve_roster_weeks_prefers_observed_matchup_weeks():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?)",
        [(2024, week, 100.0, 90.0) for week in range(1, 17)],
    )

    settings = {"start_week": 1, "end_week": 17}

    assert _resolve_roster_weeks(settings, 2024, None, local_db=_FakeLocalDB(conn)) == list(range(1, 17))


def test_align_week_count_prefers_observed_local_matchups():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?)",
        [(2024, week, 100.0, 90.0) for week in range(1, 17)],
    )

    assert _align_week_count_to_matchups(17, 2024, None, local_db=_FakeLocalDB(conn)) == 16
    assert _align_week_count_to_matchups(17, 2024, None, local_db=None) == 17
def test_roster_fetcher_uses_an_injected_cookie_session_without_oauth_file(tmp_path):
    """Cookie mode must not materialize a fake OAuth file or prompt for login."""
    from multi_league.data_fetchers.yahoo.yahoo_rosters import YahooRosterFetcher

    cookie_auth = object()
    fetcher = YahooRosterFetcher(
        oauth_file=tmp_path / "not-used.json",
        league_id="359.l.272424",
        oauth_session=cookie_auth,
        output_dir=tmp_path,
    )

    assert fetcher.oauth is cookie_auth
