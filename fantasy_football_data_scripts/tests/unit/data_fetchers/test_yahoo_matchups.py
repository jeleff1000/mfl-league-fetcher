import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.data_fetchers.yahoo import yahoo_matchups
from multi_league.data_fetchers.yahoo.yahoo_matchups import parse_matchups_for_week, weekly_matchup_data


class _FakeSession:
    def __init__(self, text: str):
        self._text = text

    def get(self, _url):
        return _FakeResponse(self._text)


class _RoutingSession:
    def __init__(self, routes: dict[str, str]):
        self.routes = routes
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        for needle, text in self.routes.items():
            if needle in url:
                return _FakeResponse(text)
        raise AssertionError(f"unexpected URL: {url}")


class _FakeResponse:
    def __init__(self, text: str):
        self.status_code = 200
        self.text = text

    def raise_for_status(self):
        return None


def test_parse_matchups_preserves_yahoo_team_key():
    xml = """
    <fantasy_content xmlns="http://fantasysports.yahooapis.com/fantasy/v2/base.rng">
      <league>
        <scoreboard>
          <matchups>
            <matchup>
              <week>1</week>
              <teams>
                <team>
                  <team_key>371.l.2116.t.2</team_key>
                  <name>OPEN TEAM 1</name>
                  <managers>
                    <manager>
                      <guid>owner-guid</guid>
                      <nickname>Mc Bidmaster</nickname>
                    </manager>
                  </managers>
                  <team_points><total>100.0</total></team_points>
                </team>
                <team>
                  <team_key>371.l.2116.t.3</team_key>
                  <name>Bobcats</name>
                  <managers>
                    <manager>
                      <guid>bob-guid</guid>
                      <nickname>Bob</nickname>
                    </manager>
                  </managers>
                  <team_points><total>90.0</total></team_points>
                </team>
              </teams>
            </matchup>
          </matchups>
        </scoreboard>
      </league>
    </fantasy_content>
    """
    oauth = SimpleNamespace(session=_FakeSession(xml))

    rows = parse_matchups_for_week(oauth, "371.l.2116", 2017, 1)

    assert [row["team_key"] for row in rows] == ["371.l.2116.t.2", "371.l.2116.t.3"]


def test_yahoo_scoreboard_declared_matchup_count_rejects_truncated_response():
    oauth = SimpleNamespace(session=_FakeSession(
        _scoreboard_xml(15).replace("<matchups>", '<matchups count="2">')
    ))
    with pytest.raises(RuntimeError, match="declared matchup count"):
        parse_matchups_for_week(oauth, "124.l.644100", 2025, 15)


def test_yahoo_scoreboard_rejects_unparsed_single_team_matchup():
    xml = _scoreboard_xml(15).replace("<matchups>", '<matchups count="1">')
    # The fixture's paired match is intentionally reduced to one team.
    start = xml.index('<team>\n                  <team_key>124.l.644100.t.2')
    end = xml.index('</team>', start) + len('</team>')
    xml = xml[:start] + xml[end:]
    oauth = SimpleNamespace(session=_FakeSession(xml))
    with pytest.raises(RuntimeError, match="team cardinality"):
        parse_matchups_for_week(oauth, "124.l.644100", 2025, 15)


def test_parse_matchups_recovers_hidden_yahoo_team_key_from_url():
    xml = """
    <fantasy_content xmlns="http://fantasysports.yahooapis.com/fantasy/v2/base.rng">
      <league>
        <scoreboard>
          <matchups>
            <matchup>
              <week>1</week>
              <teams>
                <team>
                  <team_key>--</team_key>
                  <url>https://football.fantasysports.yahoo.com/2013/f1/134824/2</url>
                  <name>MoneyBall</name>
                  <managers>
                    <manager>
                      <guid>--</guid>
                      <nickname>--hidden--</nickname>
                    </manager>
                  </managers>
                  <team_points><total>100.0</total></team_points>
                </team>
                <team>
                  <team_key>314.l.134824.t.3</team_key>
                  <url>https://football.fantasysports.yahoo.com/2013/f1/134824/3</url>
                  <name>Paul</name>
                  <managers>
                    <manager>
                      <guid>paul-guid</guid>
                      <nickname>Paul</nickname>
                    </manager>
                  </managers>
                  <team_points><total>90.0</total></team_points>
                </team>
              </teams>
            </matchup>
          </matchups>
        </scoreboard>
      </league>
    </fantasy_content>
    """
    oauth = SimpleNamespace(session=_FakeSession(xml))

    rows = parse_matchups_for_week(oauth, "314.l.134824", 2013, 1)

    assert [row["team_key"] for row in rows] == ["314.l.134824.t.2", "314.l.134824.t.3"]


def _scoreboard_xml(week: int) -> str:
    return f"""
    <fantasy_content xmlns="http://fantasysports.yahooapis.com/fantasy/v2/base.rng">
      <league>
        <scoreboard>
          <matchups>
            <matchup>
              <week>{week}</week>
              <teams>
                <team>
                  <team_key>124.l.644100.t.1</team_key>
                  <name>Team A</name>
                  <managers><manager><guid>a-guid</guid><nickname>Ada</nickname></manager></managers>
                  <team_points><total>100.0</total></team_points>
                </team>
                <team>
                  <team_key>124.l.644100.t.2</team_key>
                  <name>Team B</name>
                  <managers><manager><guid>b-guid</guid><nickname>Ben</nickname></manager></managers>
                  <team_points><total>90.0</total></team_points>
                </team>
              </teams>
            </matchup>
          </matchups>
        </scoreboard>
      </league>
    </fantasy_content>
    """


def test_weekly_matchup_data_falls_back_when_bulk_xml_is_empty(monkeypatch, tmp_path):
    empty_bulk = """
    <fantasy_content xmlns="http://fantasysports.yahooapis.com/fantasy/v2/base.rng">
      <league><scoreboard><matchups count="0"/></scoreboard></league>
    </fantasy_content>
    """
    session = _RoutingSession(
        {
            "week=2,3": empty_bulk,
            "week=2": _scoreboard_xml(2),
            "week=3": _scoreboard_xml(3),
        }
    )
    oauth = SimpleNamespace(session=session)

    class FakeLeague:
        def settings(self):
            return {"start_week": 2, "end_week": 3}

    class FakeGame:
        def __init__(self, *_args, **_kwargs):
            pass

        def to_league(self, league_key):
            assert league_key == "124.l.644100"
            return FakeLeague()

    monkeypatch.setattr(yahoo_matchups, "YFA_AVAILABLE", True)
    monkeypatch.setattr(yahoo_matchups, "RUN_LOGGER_AVAILABLE", False)
    monkeypatch.setattr(yahoo_matchups, "yfa", SimpleNamespace(Game=FakeGame))

    ctx = SimpleNamespace(
        league_id="124.l.644100",
        matchup_data_directory=tmp_path,
        manager_name_overrides={},
        get_oauth_session=lambda: oauth,
        get_league_id_for_year=lambda year: "124.l.644100",
    )

    df, failed_weeks = weekly_matchup_data(ctx=ctx, year=2005)

    assert sorted(df["week"].unique().tolist()) == [2, 3]
    assert len(df) == 4
    assert failed_weeks == []
    assert any("week=2,3" in url for url in session.urls)
    assert any("week=2" in url for url in session.urls)
    assert any("week=3" in url for url in session.urls)
    assert not any("week=1" in url for url in session.urls)


def test_weekly_matchup_data_requests_only_the_selected_week(monkeypatch, tmp_path):
    """A targeted refresh must not replace current-season state with future weeks."""
    session = _RoutingSession({"week=1": _scoreboard_xml(1)})
    oauth = SimpleNamespace(session=session)

    class FakeLeague:
        def settings(self):
            return {"start_week": 1, "end_week": 17}

    class FakeGame:
        def __init__(self, *_args, **_kwargs):
            pass

        def to_league(self, league_key):
            assert league_key == "124.l.644100"
            return FakeLeague()

    monkeypatch.setattr(yahoo_matchups, "YFA_AVAILABLE", True)
    monkeypatch.setattr(yahoo_matchups, "RUN_LOGGER_AVAILABLE", False)
    monkeypatch.setattr(yahoo_matchups, "yfa", SimpleNamespace(Game=FakeGame))
    ctx = SimpleNamespace(
        league_id="124.l.644100",
        matchup_data_directory=tmp_path,
        manager_name_overrides={},
        get_oauth_session=lambda: oauth,
        get_league_id_for_year=lambda year: "124.l.644100",
    )

    df, failed_weeks = weekly_matchup_data(ctx=ctx, year=2005, week=1)

    assert sorted(df["week"].unique().tolist()) == [1]
    assert failed_weeks == []
    assert len(session.urls) == 1
    assert "week=1" in session.urls[0]
    assert "week=1," not in session.urls[0]
