from __future__ import annotations

import importlib.util
import json
import sys
import threading
import time
from argparse import Namespace
from pathlib import Path
import re

import pytest


SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "quick_import_kmffl_2025_web.py"
spec = importlib.util.spec_from_file_location("quick_import_kmffl_2025_web", SCRIPT)
assert spec and spec.loader
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_cookiejar_header_supports_tab_export_without_leaking_values(tmp_path: Path) -> None:
    path = tmp_path / "cookiejar.json"
    path.write_text(
        "# Netscape HTTP Cookie File\n"
        ".yahoo.com\tTRUE\t/\tTRUE\t0\tcrumb\tsecret-crumb\n"
        ".yahoo.com\tTRUE\t/\tTRUE\t0\tfoo\tbar\n",
        encoding="utf-8",
    )

    header = module.cookie_header_from_file(path)

    assert header == "crumb=secret-crumb; foo=bar"
    assert module.redact_cookie_header(header) == "crumb=[REDACTED]; foo=[REDACTED]"


def test_cookiejar_header_supports_browser_name_value_domain_export(tmp_path: Path) -> None:
    path = tmp_path / "cookiejar.json"
    path.write_text("crumb\tsecret-crumb\t.yahoo.com\t/\tTRUE\t0\t0\t0\t0\t0\t0\t0\t0\n", encoding="utf-8")

    assert module.cookie_header_from_file(path) == "crumb=secret-crumb"


def test_cookiejar_header_normalizes_line_wrapped_browser_json_values(tmp_path: Path) -> None:
    path = tmp_path / "cookiejar.json"
    path.write_text(
        '{"cookies":[{"name":"T","value":"t-value\ncontinued","domain":".yahoo.com"},'
        '{"name":"Y","value":"y-value","domain":".yahoo.com"}]}',
        encoding="utf-8",
    )

    assert module.cookie_header_from_file(path) == "T=t-valuecontinued; Y=y-value"


def test_authenticated_session_keeps_t_y_in_the_cookie_jar_for_redirect_handoffs() -> None:
    session = module._authenticated_session("T=t-secret; Y=y-secret")

    assert "Cookie" not in session.headers
    assert session.cookies.get("T") == "t-secret"
    assert session.cookies.get("Y") == "y-secret"


def test_parse_web_timestamp_epoch_uses_season_rollover_and_eastern_time() -> None:
    dec = module.parse_web_timestamp_epoch("Dec 18, 4:52 pm", 2025)
    jan = module.parse_web_timestamp_epoch("Jan 3, 10:05 am", 2025)
    assert dec is not None and jan is not None
    assert module.datetime.fromtimestamp(dec, module.ZoneInfo("America/New_York")).year == 2025
    assert module.datetime.fromtimestamp(jan, module.ZoneInfo("America/New_York")).year == 2026
    assert module.parse_web_timestamp_epoch("not a timestamp", 2025) is None


def test_matchup_detail_coverage_rejects_missing_linked_pages(tmp_path: Path) -> None:
    def page(kind: str) -> module.PageResult:
        return module.PageResult(
            module=kind,
            url="https://example.test",
            final_url="https://example.test",
            status=200,
            content_type="text/html",
            bytes=100,
            sha256="x",
            output_file=str(tmp_path / f"{kind}.html"),
        )

    rows = [
        {"matchup_url": "/matchup?mid1=1&mid2=2"},
        {"matchup_url": "/matchup?mid1=3&mid2=4"},
    ]
    coverage = module.matchup_detail_coverage(
        rows,
        [page("matchup_detail_week_01_1_2"), page("matchup_recap_week_01_1_2")],
    )
    assert coverage["expected_matchup_links"] == 2
    assert coverage["detail_pages_complete"] is False
    assert coverage["recap_pages_complete"] is False

def test_build_page_urls_is_scoped_to_kmffl_2025() -> None:
    urls = module.build_page_urls("461.l.90939", 2025)

    assert len(urls) == len(module.PAGE_MODULES)
    assert all("/2025/f1/90939" in url for url in urls.values())
    assert all(url.startswith("https://football.fantasysports.yahoo.com/") for url in urls.values())
    assert "api" not in " ".join(urls.values())
    assert urls["matchups"].endswith("/matchup")
    assert urls["draft"].endswith("/draftresults")


def test_build_page_urls_preserves_the_profile_archive_game_code() -> None:
    urls = module.build_page_urls("223.l.25022", 2009, game_code="f2")

    assert all("/2009/f2/25022" in url for url in urls.values())
    assert module.build_matchup_week_url("223.l.25022", 2009, 1, game_code="f2").startswith(
        "https://football.fantasysports.yahoo.com/2009/f2/25022/"
    )


def test_parse_yahoo_profile_game_codes_uses_the_canonical_league_url() -> None:
    html = '''
    <script>
      {"game_id":223,"league_id":25022,
       "league_url":"http:\\u002F\\u002Ffootball.fantasysports.yahoo.com\\u002F2009\\u002Ff2\\u002F25022"}
      {"game_id":380,"league_id":62205,
       "league_url":"https://football.fantasysports.yahoo.com/2018/f1/62205"}
    </script>
    '''

    assert module.parse_yahoo_profile_game_codes(html) == {
        "223.l.25022": "f2",
        "380.l.62205": "f1",
    }


def test_parse_standings_html_returns_stable_team_rows() -> None:
    html = """
    <table>
      <tr><th>Rank</th><th>Team</th><th>W-L-T</th><th>PF</th><th>PA</th></tr>
      <tr><td>*1</td><td>Alpha</td><td>8-6-0</td><td>1768.46</td><td>1635.28</td></tr>
      <tr><td>*2</td><td>Beta</td><td>9-5-0</td><td>1548.36</td><td>1635.88</td></tr>
    </table>
    """

    rows = module.parse_standings_html(html)

    assert rows == [
        {"rank": 1, "team": "Alpha", "record": "8-6-0", "points_for": 1768.46, "points_against": 1635.28},
        {"rank": 2, "team": "Beta", "record": "9-5-0", "points_for": 1548.36, "points_against": 1635.88},
    ]


def test_parse_standings_html_uses_row_order_when_yahoo_omits_preseason_ranks() -> None:
    html = """
    <table>
      <tr><th>Rank</th><th>Team</th><th>W-L-T</th><th>Div</th><th>PF</th><th>PA</th></tr>
      <tr><td colspan="6">North Division</td></tr>
      <tr><td><span title=""></span></td><td>Alpha</td><td>0-0-0</td><td>0-0-0</td><td>0.00</td><td>0.00</td></tr>
      <tr><td><span title=""></span></td><td>Beta</td><td>0-0-0</td><td>0-0-0</td><td>0.00</td><td>0.00</td></tr>
    </table>
    """

    rows = module.parse_standings_html(html)

    assert rows == [
        {"rank": 1, "team": "Alpha", "record": "0-0-0", "points_for": 0.0, "points_against": 0.0},
        {"rank": 2, "team": "Beta", "record": "0-0-0", "points_for": 0.0, "points_against": 0.0},
    ]


def test_season_team_count_uses_the_current_season_standings() -> None:
    standings = [{"team": f"Team {number}"} for number in range(12)]

    assert module.season_team_count(standings, configured_team_count=10) == 12


def test_parse_yahoo_auxiliary_matchup_fields() -> None:
    felo = """
    <table><tr><th>Rank</th><th>Manager</th><th>Team</th><th>Rating (+-)</th><th>Level</th><th></th></tr>
    <tr><td>1</td><td>Jason</td><td><a href='/2025/f1/90939/1'>Alpha</a></td><td>940</td><td></td><td>Diamond</td></tr></table>
    """
    standings = """
    <table><tr><th>Rank</th><th>Team</th><th>W-L-T</th><th>PF</th><th>PA</th><th>Waiver Bdgt</th><th>Waiver</th><th>Moves</th></tr>
    <tr><td>1</td><td><a href='/2025/f1/90939/1'>Alpha</a></td><td>8-6-0</td><td>100</td><td>90</td><td>$12</td><td>3</td><td>7</td></tr></table>
    """
    detail = """
    <section id='matchup-header'><a href='/2025/f1/90939/1'>Alpha</a><a href='/2025/f1/90939/2'>Beta</a>
    <table class='M-a'><tr><td>117.08</td><td>Points</td><td>107.14</td></tr><tr><td>118.91</td><td>Orig Proj</td><td>123.84</td></tr></table></section>
    """
    assert module.parse_felo_html(felo)[0]["felo_tier"] == "Diamond"
    assert module.parse_standings_details_html(standings)[0]["faab_balance"] == 12.0
    assert module.parse_standings_details_html(standings)[0]["waiver_priority"] == 3
    assert module.parse_matchup_detail_html(detail, 2025, 1) == [
        {"year": 2025, "week": 1, "team_number": 1, "team_projected_points": 118.91},
        {"year": 2025, "week": 1, "team_number": 2, "team_projected_points": 123.84},
    ]


def test_parse_matchup_recap_metadata_and_grades() -> None:
    html = """
    <meta property='og:title' content='Alpha Beats Beta: A Recap!' />
    <img src='https://s.yimg.com/grades/a-_grade@3x.png' />
    <img src='https://s.yimg.com/grades/c+_grade@3x.png' />
    """
    assert module.parse_matchup_recap_html(
        html, 2025, 1,
        "https://football.fantasysports.yahoo.com/2025/f1/90939/recap?week=1&mid1=1&mid2=2",
    ) == [
        {"year": 2025, "week": 1, "team_number": 1, "grade": "A-", "matchup_recap_title": "Alpha Beats Beta: A Recap!", "matchup_recap_url": "https://football.fantasysports.yahoo.com/2025/f1/90939/recap?week=1&mid1=1&mid2=2"},
        {"year": 2025, "week": 1, "team_number": 2, "grade": "C+", "matchup_recap_title": "Alpha Beats Beta: A Recap!", "matchup_recap_url": "https://football.fantasysports.yahoo.com/2025/f1/90939/recap?week=1&mid1=1&mid2=2"},
    ]


def test_parse_draft_html_returns_pick_player_cost_and_team() -> None:
    html = """
    <table>
      <tr><th>Pick</th><th>Player</th><th>Salary</th><th>Team</th></tr>
      <tr><td>1.</td><td>Brandon Aubrey (Dal - K)</td><td>$2</td><td>BloodBourne</td></tr>
      <tr><td>2.</td><td>Ja'Marr Chase (Cin - WR)</td><td>$69</td><td>BloodBourne</td></tr>
    </table>
    """

    assert module.parse_draft_html(html) == [
        {"pick": 1, "player": "Brandon Aubrey", "nfl_team": "Dal", "position": "K", "cost": 2.0, "team": "BloodBourne"},
        {"pick": 2, "player": "Ja'Marr Chase", "nfl_team": "Cin", "position": "WR", "cost": 69.0, "team": "BloodBourne"},
    ]


def test_parse_settings_html_returns_key_value_rows() -> None:
    html = """
    <table>
      <tr><th>Setting</th><th>Value</th></tr>
      <tr><td>League Name:</td><td>KMFFL</td></tr>
      <tr><td>Max Teams:</td><td>10</td></tr>
    </table>
    """

    assert module.parse_settings_html(html) == [
        {"setting": "League Name", "value": "KMFFL"},
        {"setting": "Max Teams", "value": "10"},
    ]


def test_parse_settings_page_normalizes_league_and_scoring_tables() -> None:
    html = """
    <table>
      <tr><th>Setting</th><th>Value</th></tr>
      <tr><td>Max Teams:</td><td>10</td></tr>
      <tr><td>Draft Type:</td><td>Live Salary Cap Draft</td></tr>
      <tr><td>Playoffs:</td><td>6 teams - Week 15, 16 and 17</td></tr>
      <tr><td>Roster Positions:</td><td>QB, WR, RB, W/R/T, BN, IR</td></tr>
      <tr><td>Fractional Points:</td><td>Yes</td></tr>
    </table>
    <table>
      <tr><th>Offense</th><th>League Value</th><th>Yahoo Default Value</th></tr>
      <tr><td>Passing Yards</td><td>25 yards per point</td><td></td></tr>
      <tr><td>Passing Touchdowns</td><td>4</td><td></td></tr>
      <tr><td>Receptions</td><td>.5</td><td></td></tr>
      <tr><td>Points Allowed 0 points</td><td>0</td><td>0</td></tr>
      <tr><td>Points Allowed 7-13 points</td><td>-1</td><td>4</td></tr>
      <tr><td>Defensive Yards Allowed - Negative</td><td>0</td><td>0</td></tr>
    </table>
    """

    result = module.parse_settings_page(html, league_key="461.l.90939", year=2025)

    assert result["metadata"]["league_key"] == "461.l.90939"
    assert result["metadata"]["num_teams"] == 10
    assert result["metadata"]["draft_type"] == "auction"
    assert result["metadata"]["playoff_start_week"] == 15
    assert result["metadata"]["regular_season_weeks"] == 14
    assert result["metadata"]["uses_fractional_points"] is True
    assert result["roster_positions"] == ["QB", "WR", "RB", "W/R/T", "BN", "IR"]
    assert result["scoring"]["scoring_pass_yd"] == 0.04
    assert result["scoring"]["scoring_pass_td"] == 4.0
    assert result["scoring"]["scoring_rec"] == 0.5
    assert result["scoring"]["scoring_pts_allow_7_13"] == -1.0
    assert result["scoring"]["scoring_pts_allow_0"] == 0.0
    assert result["scoring"]["scoring_yds_allow_neg"] == 0.0


def test_parse_settings_page_accepts_legacy_plural_playoff_weeks() -> None:
    """Older Yahoo settings pages use ``Weeks`` instead of ``Week``."""
    html = """
    <table>
      <tr><th>Setting</th><th>Value</th></tr>
      <tr><td>Max Teams:</td><td>12</td></tr>
      <tr><td>Playoffs:</td><td>6 teams - Weeks 14, 15 and 16</td></tr>
    </table>
    """

    result = module.parse_settings_page(html, league_key="380.l.1316680", year=2018)

    assert result["metadata"]["playoff_teams"] == 6
    assert result["metadata"]["playoff_start_week"] == 14
    assert result["metadata"]["regular_season_weeks"] == 13
    assert result["metadata"]["end_week"] == 16


def test_parse_roster_html_maps_editorial_id_and_bench_status() -> None:
    html = '''
    <script>editorialToFantasyPlayerIds: {"40881":["12345"]}}</script>
    <table><tr class="First">
      <td><span class="pos-label" data-pos="QB">QB</span></td>
      <td><a href="https://sports.yahoo.com/nfl/players/40881" title="Drake Maye">Drake Maye</a></td>
      <td class="pts">15.28</td>
    </tr><tr class="bench">
      <td><span class="pos-label" data-pos="BN">BN</span></td>
      <td><a href="https://sports.yahoo.com/nfl/players/99999">Bench Player</a></td>
      <td class="pts">4.00</td>
    </tr></table>
    '''

    assert module.parse_roster_html(html, week=1, team_number=3) == [
        {
            "week": 1,
            "team_number": 3,
            "yahoo_player_id": "12345",
            "editorial_player_id": "40881",
            "player": "Drake Maye",
            "lineup_position": "QB",
            "is_started": 1,
            "fantasy_points": 15.28,
        },
        {
            "week": 1,
            "team_number": 3,
            "yahoo_player_id": "99999",
            "editorial_player_id": "99999",
            "player": "Bench Player",
            "lineup_position": "BN",
            "is_started": 0,
            "fantasy_points": 4.0,
        },
    ]


def test_parse_matchup_rosters_html_reads_both_teams_from_one_page() -> None:
    html = '''
    <script>editorialToFantasyPlayerIds: {"40881":["12345"]}}</script>
    <table><thead><tr>
      <th><div><span class="user-id">Jason</span></div>
      <a href="https://profiles.sports.yahoo.com/user/guid-jason?sport=football">View Profile</a></th>
      <th>Category</th>
      <th><div><span class="user-id">Adin</span></div>
      <a href="https://profiles.sports.yahoo.com/user/guid-adin?sport=football">View Profile</a></th>
    </tr></thead><tbody>
      <tr>
        <td>Left note</td>
        <td><a href="https://sports.yahoo.com/nfl/players/40881" title="Drake Maye">Drake Maye</a></td>
        <td>17.51</td><td><a class="pps">23.70</a></td>
        <td><span data-pos="QB">QB</span></td><td>QB</td>
        <td><span data-pos="QB">QB</span></td><td><a class="pps">30.88</a></td>
        <td>15.35</td>
        <td><a href="https://sports.yahoo.com/nfl/players/9265" title="Matthew Stafford">Matthew Stafford</a></td>
        <td>Right note</td>
      </tr>
      <tr>
        <td>Left note</td>
        <td><a href="https://sports.yahoo.com/nfl/players/99999" title="Bench Player">Bench Player</a></td>
        <td>1.00</td><td><a class="pps">4.00</a></td>
        <td><span data-pos="WR">WR</span></td><td>BN</td>
        <td><span data-pos="WR">WR</span></td><td><a class="pps">5.00</a></td>
        <td>1.00</td>
        <td><a href="https://sports.yahoo.com/nfl/players/88888" title="Other Bench">Other Bench</a></td>
        <td>Right note</td>
      </tr>
    </tbody></table>
    '''

    rows = module.parse_matchup_rosters_html(html, week=16, team_one=1, team_two=2)

    assert rows == [
        {"week": 16, "team_number": 1, "yahoo_player_id": "12345", "editorial_player_id": "40881", "player": "Drake Maye", "lineup_position": "QB", "is_started": 1, "fantasy_points": 23.7},
        {"week": 16, "team_number": 2, "yahoo_player_id": "9265", "editorial_player_id": "9265", "player": "Matthew Stafford", "lineup_position": "QB", "is_started": 1, "fantasy_points": 30.88},
        {"week": 16, "team_number": 1, "yahoo_player_id": "99999", "editorial_player_id": "99999", "player": "Bench Player", "lineup_position": "BN", "is_started": 0, "fantasy_points": 4.0},
        {"week": 16, "team_number": 2, "yahoo_player_id": "88888", "editorial_player_id": "88888", "player": "Other Bench", "lineup_position": "BN", "is_started": 0, "fantasy_points": 5.0},
    ]
    assert module.parse_matchup_manager_identities(html, team_one=1, team_two=2) == {
        1: {"manager": "Jason", "manager_guid": "guid-jason"},
        2: {"manager": "Adin", "manager_guid": "guid-adin"},
    }


def test_parse_roster_html_recovers_slug_only_defense_links_from_stat_note_id() -> None:
    html = '''
    <table><tr class="bench">
      <td><span class="pos-label" data-pos="BN">BN</span></td>
      <td><a href="https://sports.yahoo.com/nfl/teams/denver/" title="Broncos">Broncos</a></td>
      <td><a class="pps" data-stat-note-id="100007">13.00</a></td>
    </tr><tr>
      <td><span class="pos-label" data-pos="DEF">DEF</span></td>
      <td><a href="https://sports.yahoo.com/nfl/teams/arizona/" title="Cardinals">Cardinals</a></td>
      <td><a class="pps" data-stat-note-id="100022">1.00</a></td>
    </tr></table>
    '''

    rows = module.parse_roster_html(html, week=1, team_number=3)

    assert [(row["yahoo_player_id"], row["player"], row["lineup_position"], row["is_started"])
            for row in rows] == [
        ("100007", "Broncos", "BN", 0),
        ("100022", "Cardinals", "DEF", 1),
    ]


def test_parse_roster_html_recovers_bye_defense_from_watchlist_id() -> None:
    html = '''
    <table><tr class="bench">
      <td><span class="pos-label" data-pos="BN">BN</span></td>
      <td><a href="https://sports.yahoo.com/nfl/teams/la-rams/" title="Rams">Rams</a></td>
      <td><a name="w-90939-100014" href="">Watch list unavailable</a></td>
      <td colspan="2">Bye</td>
    </tr></table>
    '''

    rows = module.parse_roster_html(html, week=8, team_number=1)

    assert rows[0]["yahoo_player_id"] == "100014"
    assert rows[0]["player"] == "Rams"
    assert rows[0]["lineup_position"] == "BN"
    assert rows[0]["fantasy_points"] is None


def test_parse_manager_identity_html_reads_yahoo_guid_and_display_name() -> None:
    html = '''
    <div class="caption default labelMedium">
      <a href="https://profiles.sports.yahoo.com/user/yh-nick-jason?sport=football">Jason</a>
    </div>
    '''

    assert module.parse_manager_identity_html(html) == {
        "manager": "Jason",
        "manager_guid": "yh-nick-jason",
    }


def test_parse_transactions_html_returns_source_observations() -> None:
    html = '''
    <table><tr>
      <td><span title="Added Player">+</span></td>
      <td><a href="https://sports.yahoo.com/nfl/players/41810">Jaxson Dart</a>
        <span class="F-position">NYG - QB</span><h6>Free Agent</h6></td>
      <td><a class="Tst-team-name" href="/2025/f1/90939/1">PukÃ¡mon Go</a>
        <span class="F-timestamp">Dec 18, 2:40 pm</span></td>
    </tr></table>
    '''

    assert module.parse_transactions_html(html, page_start=25) == [
        {
            "page_start": 25,
            "editorial_player_id": "41810",
            "player": "Jaxson Dart",
            "action": "Added Player",
            "position": "NYG - QB",
            "team": "PukÃ¡mon Go",
            "team_number": 1,
            "manager": None,
            "manager_guid": None,
        "timestamp": "Dec 18, 2:40 pm",
        "source_label": "Free Agent",
        }
    ]


def test_parse_transactions_html_emits_both_sides_of_add_drop() -> None:
    html = '''
    <tr>
      <td><span title="Added Player">+</span><span title="Dropped Player">-</span></td>
      <td>
        <div class="Pbot-xs"><a href="https://sports.yahoo.com/nfl/players/1">Added Guy</a><span class="F-position">NYG - WR</span></div>
        <div class="Pbot-xs"><a href="https://sports.yahoo.com/nfl/players/2">Dropped Guy</a><span class="F-position">DAL - TE</span></div>
      </td>
      <td><a class="Tst-team-name" href="/2025/f1/90939/1">Team</a><span class="F-timestamp">Dec 18, 2:40 pm</span></td>
    </tr>
    '''

    rows = module.parse_transactions_html(html, page_start=0)

    assert [(row["editorial_player_id"], row["action"], row["player"]) for row in rows] == [
        ("1", "Added Player", "Added Guy"),
        ("2", "Dropped Player", "Dropped Guy"),
    ]


def test_parse_transactions_html_includes_defense_team_links() -> None:
    html = '''
    <tr>
      <td><span title="Added Player">+</span><span title="Dropped Player">-</span></td>
      <td>
        <div><a href="https://sports.yahoo.com/nfl/teams/atlanta/">Falcons</a><span class="F-position">Atl - DEF</span></div>
        <div><a href="https://sports.yahoo.com/nfl/teams/la-rams/">Rams</a><span class="F-position">LAR - DEF</span></div>
      </td>
      <td><a class="Tst-team-name" href="/2025/f1/90939/1">Team</a><span class="F-timestamp">Dec 18, 2:40 pm</span></td>
    </tr>
    '''

    rows = module.parse_transactions_html(html, page_start=0)

    assert [(row["editorial_player_id"], row["action"], row["player"]) for row in rows] == [
        ("team:atlanta", "Added Player", "Falcons"),
        ("team:la-rams", "Dropped Player", "Rams"),
    ]


def test_parse_transactions_html_captures_transaction_manager_guid() -> None:
    html = """
    <tr>
      <td><a href="https://sports.yahoo.com/nfl/players/123">Example Player</a></td>
      <td><span class="F-position">QB</span></td>
      <td><a class="Tst-team-name" href="/2025/f1/90939/2">Team A</a>
        (<a class="Tst-profile-name" href="https://profiles.sports.yahoo.com/user/guid-a">Alice</a>)
        <span class="F-timestamp">Sep 10, 8:00 pm</span></td>
    </tr>
    """

    rows = module.parse_transactions_html(html, page_start=0)

    assert rows[0]["manager"] == "Alice"
    assert rows[0]["manager_guid"] == "guid-a"


def test_parse_transactions_html_ignores_trading_list_entries() -> None:
    html = """
    <tr>
      <td><span title="Trading List">*</span></td>
      <td><a href="https://sports.yahoo.com/nfl/players/123">Example Player</a>
        <p>Added to Trading List</p></td>
      <td><a href="/2025/f1/90939/2">Team A</a></td>
    </tr>
    """

    assert module.parse_transactions_html(html, page_start=0) == []


def test_parse_faab_html_extracts_winning_offer_and_awarded_team() -> None:
    html = """
    <table><tr>
      <td><a href="https://sports.yahoo.com/nfl/players/33989">Christian Watson</a></td>
      <td><h6>$14 Winning Offer</h6>
        <p><a href="https://football.fantasysports.yahoo.com/2025/f1/90939/3">Other</a> $4 (Lower Offer)</p>
      </td>
      <td>Awarded To: <a href="https://football.fantasysports.yahoo.com/2025/f1/90939/2">Winner</a>
        <span class="F-timestamp">Oct 29,4:52 am</span></td>
    </tr></table>
    """

    assert module.parse_faab_html(html, page_start=0) == [
        {
            "page_start": 0,
            "editorial_player_id": "33989",
            "player": "Christian Watson",
            "team_number": 2,
            "team": "Winner",
            "timestamp": "Oct 29,4:52 am",
            "faab_bid": 14,
            "status": "successful",
        }
    ]


def test_normalize_transaction_observations_maps_source_and_faab() -> None:
    rows = module.normalize_transaction_observations(
        [
            {
                "editorial_player_id": "33989",
                "player": "Christian Watson",
                "action": "Added Player",
                "source_label": "Waiver",
                "team_number": 2,
                "timestamp": "Oct 29,4:52 am",
            }
        ],
        faab_rows=[
            {
                "editorial_player_id": "33989",
                "team_number": 2,
                "timestamp": "Oct 29,4:52 am",
                "faab_bid": 14,
            }
        ],
        year=2025,
    )

    assert rows[0]["transaction_type"] == "add"
    assert rows[0]["source_type"] == "waivers"
    assert rows[0]["destination"] == "team"
    assert rows[0]["status"] == "successful"
    assert rows[0]["faab_bid"] == 14
    assert rows[0]["transaction_id"] == "web-2025-000000"


def test_acquire_transactions_fails_explicitly_at_page_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    html = """
    <tr>
      <td><a href="https://sports.yahoo.com/nfl/players/1">Player</a></td>
      <td><span class="F-position">QB</span></td>
      <td><a class="Tst-team-name" href="/2025/f1/90939/1">Team</a>
        <span class="F-timestamp">Sep 10, 8:00 pm</span></td>
    </tr>
    """

    monkeypatch.setattr(module, "fetch_cached_page", lambda *args, **kwargs: html)

    with pytest.raises(RuntimeError, match="reached the configured page cap"):
        module.acquire_transactions("crumb=x", "461.l.90939", 2025, tmp_path, max_pages=1, team_count=1)


def test_acquire_transactions_uses_league_wide_view(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    requests_seen: list[str] = []

    def fake_fetch(_session: object, url: str, output_file: Path, **_kwargs: object) -> str:
        requests_seen.append(url)
        if "count=0" in url:
            team = "1"
            return f"""
            <tr><td><span title=\"Added Player\"></span>
              <a href=\"https://sports.yahoo.com/nfl/players/{team}\">Player {team}</a>
              <a class=\"Tst-team-name\" href=\"/2025/f1/90939/{team}\">Team {team}</a>
              <span class=\"F-timestamp\">Sep 10, 8:00 pm</span></td></tr>
            """
        return "<tr></tr>"

    monkeypatch.setattr(module, "fetch_cached_page", fake_fetch)
    rows = module.acquire_transactions(
        "crumb=x", "461.l.90939", 2025, tmp_path, max_pages=2, team_count=2, delay_seconds=0
    )

    assert len(rows) == 1
    assert all("transactionsfilter=all" in url and "mid=" not in url for url in requests_seen)


def test_parse_draft_html_supports_historical_snake_round_tables() -> None:
    html = """
    <table><tr><th>Round 1</th></tr><tr><td>1.</td><td>Player One</td><td>Team A</td></tr></table>
    <table><tr><th>Round 2</th></tr><tr><td>1.</td><td>Player Two</td><td>Team B</td></tr></table>
    """

    assert module.parse_draft_html(html) == [
        {"pick": 1, "player": "Player One", "nfl_team": "", "position": "", "cost": None, "team": "Team A"},
        {"pick": 2, "player": "Player Two", "nfl_team": "", "position": "", "cost": None, "team": "Team B"},
    ]


def test_parse_matchups_html_returns_both_team_rows_from_web_card() -> None:
    html = """
    <ul class="List">
      <li class="Linkable Listitem" data-target="/2025/f1/90939/matchup?week=1&mid1=5&mid2=8">
        <div class="Fz-sm"><a href="/2025/f1/90939/5">Alpha</a></div>
        <div class="Fz-lg Fw-b">124.12</div>
        <div class="Fz-sm"><a href="/2025/f1/90939/8">Beta</a></div>
        <div class="Fz-lg Fw-b">101.96</div>
      </li>
    </ul>
    """

    assert module.parse_matchups_html(html, year=2025, week=1) == [
        {
            "year": 2025,
            "week": 1,
            "team_number": 5,
            "opponent_team_number": 8,
            "team": "Alpha",
            "opponent": "Beta",
            "team_points": 124.12,
            "opponent_points": 101.96,
            "matchup_url": "/2025/f1/90939/matchup?week=1&mid1=5&mid2=8",
        },
        {
            "year": 2025,
            "week": 1,
            "team_number": 8,
            "opponent_team_number": 5,
            "team": "Beta",
            "opponent": "Alpha",
            "team_points": 101.96,
            "opponent_points": 124.12,
            "matchup_url": "/2025/f1/90939/matchup?week=1&mid1=5&mid2=8",
        },
    ]


def test_compare_draft_reports_missing_and_field_differences() -> None:
    observed = [{"pick": 1, "player": "A", "team": "Alpha", "cost": 2.0}]
    expected = [
        {"pick": 1, "player": "A", "team": "Alpha", "cost": 3.0},
        {"pick": 2, "player": "B", "team": "Beta", "cost": 4.0},
    ]

    result = module.compare_draft(observed, expected)

    assert not result.matches
    assert result.differences == [
        "pick 1.cost: observed=2.0 expected=3.0",
        "pick 2: missing from observed",
    ]


def test_compare_standings_reports_exact_differences() -> None:
    observed = [
        {"rank": 1, "team": "Alpha", "record": "8-6-0", "points_for": 10.0, "points_against": 9.0}
    ]
    expected = [
        {"rank": 1, "team": "Alpha", "record": "8-6-0", "points_for": 11.0, "points_against": 9.0}
    ]

    result = module.compare_standings(observed, expected)

    assert not result.matches
    assert result.differences == ["Alpha.points_for: observed=10.0 expected=11.0"]


def test_compare_year_with_fly_returns_both_fetcher_checks(monkeypatch) -> None:
    monkeypatch.setattr(module, "_expected_fly_standings", lambda *_: [])
    monkeypatch.setattr(module, "_expected_fly_draft", lambda *_: [])

    result = module.compare_year_with_fly("kmffl", 2025, [], [])

    assert result == {
        "standings": {"matches": True, "differences": []},
        "draft": {"matches": True, "differences": []},
    }


def test_cookiejar_rejects_repository_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="outside the repository"):
        module.validate_cookie_path(tmp_path / "cookiejar.json", tmp_path)


def test_build_year_league_map_contains_explicit_kmffl_history() -> None:
    league_map = module.build_year_league_map(2015, 2025)

    assert list(league_map) == list(range(2015, 2026))
    assert league_map[2015] == "348.l.89552"
    assert league_map[2025] == "461.l.90939"


def test_build_year_league_map_rejects_year_without_known_key() -> None:
    with pytest.raises(ValueError, match="no Yahoo league key supplied"):
        module.build_year_league_map(2014, 2025)


def test_build_year_league_map_accepts_an_explicit_renewal_chain() -> None:
    league_map = module.build_year_league_map(
        2014,
        2016,
        {2014: "342.l.111", 2015: "348.l.222", 2016: "359.l.333"},
    )

    assert league_map == {2014: "342.l.111", 2015: "348.l.222", 2016: "359.l.333"}


def test_build_matchup_week_url_uses_server_rendered_week_submodule() -> None:
    url = module.build_matchup_week_url("461.l.90939", 2025, 17)

    assert url == (
        "https://football.fantasysports.yahoo.com/2025/f1/90939/"
        "?matchup_week=17&module=matchups&lhst=matchups"
    )


def test_throttled_request_raises_after_999_retry_budget(monkeypatch, tmp_path: Path) -> None:
    class Response:
        status_code = 999
        url = "https://football.fantasysports.yahoo.com/"
        headers = {}
        content = b"throttled"
        text = "throttled"

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    sleeps = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    with pytest.raises(module.YahooThrottleError, match="HTTP 999"):
        module.fetch_cached_page(
            Session(),
            "https://football.fantasysports.yahoo.com/",
            tmp_path / "page.html",
            delay_seconds=0,
            throttle_retries=2,
        )
    assert sleeps == [5.0, 15.0]


def test_cookie_preflight_retries_transient_999_before_accepting_session(monkeypatch) -> None:
    class Response:
        def __init__(self, status_code: int):
            self.status_code = status_code
            self.url = "https://football.fantasysports.yahoo.com/2016/f1/272424/settings"
            self.text = "league settings"

    class Session:
        def __init__(self):
            self.responses = iter([Response(200), Response(999), Response(200)])

        def get(self, *args, **kwargs):
            return next(self.responses)

    session = Session()
    sleeps = []
    monkeypatch.setattr(module, "_authenticated_session", lambda header: session)
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    module.preflight_cookie_session("T=test; Y=test", "359.l.272424", 2016, throttle_retries=1)

    assert sleeps == [5.0]


def test_cookie_preflight_primes_the_yahoo_session_before_legacy_settings(monkeypatch) -> None:
    class Response:
        status_code = 200
        url = "https://football.fantasysports.yahoo.com/"
        text = "league settings"

    class Session:
        def __init__(self):
            self.urls: list[str] = []

        def get(self, url, *args, **kwargs):
            self.urls.append(url)
            return Response()

    session = Session()
    monkeypatch.setattr(module, "_authenticated_session", lambda header: session)

    module.preflight_cookie_session("T=test; Y=test", "331.l.492605", 2014)

    assert session.urls == [
        "https://football.fantasysports.yahoo.com/f1/myleagues",
        "https://football.fantasysports.yahoo.com/2014/f1/492605/settings",
    ]


def test_parse_matchups_html_preserves_literal_championship_and_consolation_brackets() -> None:
    html = """
    <p class="Ta-c">Championship Bracket</p>
    <ul class="List">
      <li class="Linkable Listitem" data-target="/2025/f1/90939/matchup?week=16&mid1=1&mid2=2">
        <div class="Fz-sm"><a>Champ One</a></div><div class="Fz-lg">120.0</div>
        <div class="Fz-sm"><a>Champ Two</a></div><div class="Fz-lg">100.0</div>
      </li>
    </ul>
    <p class="Ta-c">Consolation Bracket</p>
    <ul class="List">
      <li class="Linkable Listitem" data-target="/2025/f1/90939/matchup?week=16&mid1=3&mid2=4">
        <div class="Fz-sm"><a>Consolation One</a></div><div class="Fz-lg">90.0</div>
        <div class="Fz-sm"><a>Consolation Two</a></div><div class="Fz-lg">80.0</div>
      </li>
    </ul>
    """

    rows = module.parse_matchups_html(html, year=2025, week=16)

    assert [(row["team"], row["is_playoffs"], row["is_consolation"]) for row in rows] == [
        ("Champ One", 1, 0),
        ("Champ Two", 1, 0),
        ("Consolation One", 1, 1),
        ("Consolation Two", 1, 1),
    ]


def test_prime_cookie_session_retries_a_transient_999(monkeypatch) -> None:
    class Response:
        url = "https://football.fantasysports.yahoo.com/f1/myleagues"
        text = "league list"

        def __init__(self, status_code: int):
            self.status_code = status_code

    class Session:
        def __init__(self):
            self.responses = iter([Response(999), Response(200)])

        def get(self, *args, **kwargs):
            return next(self.responses)

    sleeps: list[float] = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    assert module._prime_authenticated_session(Session(), throttle_retries=1)
    assert sleeps == [5.0]


def test_acquire_pages_uses_a_primed_cookie_session(monkeypatch, tmp_path: Path) -> None:
    primed_session = object()
    fetched_sessions: list[object] = []

    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: primed_session)
    monkeypatch.setattr(
        module,
        "fetch_cached_page",
        lambda session, *args, **kwargs: fetched_sessions.append(session) or "<html>ok</html>",
    )

    module.acquire_pages(
        "T=test; Y=test",
        {"settings": "https://example.test/settings"},
        tmp_path,
        delay_seconds=0,
    )

    assert fetched_sessions == [primed_session]


def test_wait_between_cookie_seasons_applies_explicit_cooldown(monkeypatch, capsys) -> None:
    sleeps = []
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    module.wait_between_cookie_seasons(2015, 2016, 180.0)

    assert sleeps == [180.0]
    assert "2015 -> 2016" in capsys.readouterr().out


def test_run_with_throttle_recovery_retries_cached_operation(monkeypatch) -> None:
    attempts = []
    sleeps = []

    def operation():
        attempts.append(True)
        if len(attempts) == 1:
            raise module.YahooThrottleError("throttled")
        return ["rows"]

    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    result = module.run_with_throttle_recovery(
        operation,
        label="2016 rosters",
        recovery_retries=1,
        recovery_cooldown=300.0,
    )

    assert result == ["rows"]
    assert len(attempts) == 2
    assert sleeps == [300.0]


def test_run_all_years_recovers_a_throttled_matchup_capture(monkeypatch, tmp_path: Path) -> None:
    """A transient matchup 999 must resume the cached season instead of aborting it."""
    page = module.PageResult(
        module="standings",
        url="https://example.test/standings",
        final_url="https://example.test/standings",
        status=200,
        content_type="text/html",
        bytes=1,
        sha256="x",
        output_file=str(tmp_path / "standings.html"),
    )
    matchup_attempts = 0

    monkeypatch.setattr(module, "validate_cookie_path", lambda path: path)
    monkeypatch.setattr(module, "cookie_header_from_file", lambda path: "T=test; Y=test")
    monkeypatch.setattr(module, "resolve_yahoo_profile_game_codes", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "preflight_cookie_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "build_page_urls", lambda *args, **kwargs: {"settings": "settings", "standings": "standings", "draft": "draft"})
    monkeypatch.setattr(module, "acquire_pages", lambda *args, **kwargs: ({"settings": "s", "standings": "s", "draft": "d"}, [page]))
    monkeypatch.setattr(module, "parse_standings_html", lambda html: [{"rank": 1, "team": "Alpha"}])
    monkeypatch.setattr(module, "parse_draft_html", lambda html: [{"pick": 1, "player": "Player"}])
    monkeypatch.setattr(module, "parse_settings_html", lambda html: [{"setting": "playoff_start_week", "value": "15"}])
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    def acquire_matchups(*args, **kwargs):
        nonlocal matchup_attempts
        matchup_attempts += 1
        if matchup_attempts == 1:
            raise module.YahooThrottleError("Yahoo returned HTTP 999")
        return ([{"year": 2025, "week": 1, "team_number": 1, "opponent_team_number": 2}], [])

    monkeypatch.setattr(module, "acquire_matchups", acquire_matchups)

    args = Namespace(
        cookie_jar=str(tmp_path / "cookiejar.json"),
        output_dir=str(tmp_path / "output"),
        start_year=2025,
        end_year=2025,
        league_keys={2025: "461.l.338926"},
        db_name="ms_gang",
        league_name="MS GANG",
        request_delay=0.0,
        throttle_retries=0,
        throttle_recovery_retries=1,
        throttle_recovery_cooldown=0.0,
        year_throttle_cooldown=0.0,
        team_count=10,
        roster_weeks=1,
        transaction_pages=1,
        skip_history=True,
        no_fly_compare=True,
    )

    assert module.run_all_years(args) == 0
    assert matchup_attempts == 2


def test_capture_validation_allows_empty_current_season_transactions() -> None:
    """A preseason Yahoo league can legitimately have no transactions yet."""
    validation = [
        {
            "year": 2025,
            "checks": {"transactions_nonempty": True, "rosters_nonempty": True},
        },
        {
            "year": 2026,
            "checks": {"transactions_nonempty": False, "rosters_nonempty": True},
        },
    ]

    assert module.capture_validation_succeeds(validation, current_year=2026)


def test_run_all_years_recovers_a_throttled_season_metadata_capture(monkeypatch, tmp_path: Path) -> None:
    """Metadata pages use the same resumable recovery contract as history pages."""
    page = module.PageResult(
        module="standings",
        url="https://example.test/standings",
        final_url="https://example.test/standings",
        status=200,
        content_type="text/html",
        bytes=1,
        sha256="x",
        output_file=str(tmp_path / "standings.html"),
    )
    metadata_attempts = 0

    monkeypatch.setattr(module, "validate_cookie_path", lambda path: path)
    monkeypatch.setattr(module, "cookie_header_from_file", lambda path: "T=test; Y=test")
    monkeypatch.setattr(module, "resolve_yahoo_profile_game_codes", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "preflight_cookie_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "build_page_urls", lambda *args, **kwargs: {"settings": "settings", "standings": "standings", "draft": "draft"})
    monkeypatch.setattr(module, "parse_standings_html", lambda html: [{"rank": 1, "team": "Alpha"}])
    monkeypatch.setattr(module, "parse_draft_html", lambda html: [{"pick": 1, "player": "Player"}])
    monkeypatch.setattr(module, "parse_settings_html", lambda html: [{"setting": "playoff_start_week", "value": "15"}])
    monkeypatch.setattr(module, "acquire_matchups", lambda *args, **kwargs: ([{"year": 2025, "week": 1, "team_number": 1, "opponent_team_number": 2}], []))
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    def acquire_pages(*args, **kwargs):
        nonlocal metadata_attempts
        metadata_attempts += 1
        if metadata_attempts == 1:
            raise module.YahooThrottleError("Yahoo returned HTTP 999")
        return {"settings": "s", "standings": "s", "draft": "d"}, [page]

    monkeypatch.setattr(module, "acquire_pages", acquire_pages)

    args = Namespace(
        cookie_jar=str(tmp_path / "cookiejar.json"),
        output_dir=str(tmp_path / "output"),
        start_year=2025,
        end_year=2025,
        league_keys={2025: "461.l.338926"},
        db_name="ms_gang",
        league_name="MS GANG",
        request_delay=0.0,
        throttle_retries=0,
        throttle_recovery_retries=1,
        throttle_recovery_cooldown=0.0,
        year_throttle_cooldown=0.0,
        team_count=10,
        roster_weeks=1,
        transaction_pages=1,
        skip_history=True,
        no_fly_compare=True,
    )

    assert module.run_all_years(args) == 0
    assert metadata_attempts == 2


def test_acquire_rosters_cancels_queued_pages_after_throttle(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    monkeypatch.setattr(module, "ROSTER_FETCH_WORKERS", 1)
    monkeypatch.setattr(module, "_authenticated_session", lambda header: object())
    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: object())
    monkeypatch.setattr(module, "_session_cookie_header", lambda session: "T=test; Y=test")

    def fail_fetch(session, url, output_file, **kwargs):
        calls.append(url)
        raise module.YahooThrottleError("throttled")

    monkeypatch.setattr(module, "fetch_cached_page", fail_fetch)

    with pytest.raises(module.YahooThrottleError, match="throttled"):
        module.acquire_rosters(
            "T=test; Y=test",
            "359.l.272424",
            2016,
            tmp_path,
            team_count=3,
            weeks=1,
            delay_seconds=0,
            throttle_retries=0,
        )

    assert len(calls) < 3


def test_acquire_rosters_does_not_overlap_cookie_requests(monkeypatch, tmp_path: Path) -> None:
    active_requests = 0
    max_active_requests = 0
    active_lock = threading.Lock()

    monkeypatch.setattr(module, "_authenticated_session", lambda header: object())
    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: object())
    monkeypatch.setattr(module, "_session_cookie_header", lambda session: "T=test; Y=test")
    monkeypatch.setattr(module, "parse_roster_html", lambda *args, **kwargs: [])

    def fetch(session, url, output_file, **kwargs):
        nonlocal active_requests, max_active_requests
        with active_lock:
            active_requests += 1
            max_active_requests = max(max_active_requests, active_requests)
        time.sleep(0.05)
        with active_lock:
            active_requests -= 1
        return "<html>ok</html>"

    monkeypatch.setattr(module, "fetch_cached_page", fetch)

    module.acquire_rosters(
        "T=test; Y=test",
        "359.l.272424",
        2016,
        tmp_path,
        team_count=2,
        weeks=1,
        delay_seconds=0,
    )

    assert max_active_requests == 1


def test_acquire_rosters_refetches_an_unusable_cached_roster_page(monkeypatch, tmp_path: Path) -> None:
    fetch_force_refresh: list[bool] = []
    roster_file = tmp_path / "rosters" / "team_01_week_01.html"
    roster_file.parent.mkdir(parents=True)
    roster_file.write_text("cached non-roster response", encoding="utf-8")

    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: object())

    def fetch(session, url, output_file, **kwargs):
        force_refresh = bool(kwargs.get("force_refresh", False))
        fetch_force_refresh.append(force_refresh)
        return "valid roster" if force_refresh else "cached non-roster response"

    def parse(html, **kwargs):
        if html != "valid roster":
            raise ValueError("could not find roster player rows")
        return [{"player": "Valid Player"}]

    monkeypatch.setattr(module, "fetch_cached_page", fetch)
    monkeypatch.setattr(module, "parse_roster_html", parse)

    rows = module.acquire_rosters(
        "T=test; Y=test",
        "359.l.272424",
        2016,
        tmp_path,
        team_count=1,
        weeks=1,
        delay_seconds=0,
    )

    assert fetch_force_refresh == [False, True]
    assert rows[0]["player"] == "Valid Player"


def test_acquire_rosters_uses_discovered_non_contiguous_team_slots(monkeypatch, tmp_path: Path) -> None:
    requested_urls: list[str] = []

    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: object())

    def fetch(session, url, output_file, **kwargs):
        requested_urls.append(url)
        return "valid roster"

    monkeypatch.setattr(module, "fetch_cached_page", fetch)
    monkeypatch.setattr(module, "parse_roster_html", lambda *args, **kwargs: [{"player": "Valid Player"}])

    module.acquire_rosters(
        "T=test; Y=test",
        "359.l.272424",
        2016,
        tmp_path,
        team_count=2,
        team_numbers=[1, 3],
        weeks=1,
        delay_seconds=0,
    )

    assert [url.rsplit("/", 1)[-1].split("?", 1)[0] for url in requested_urls] == ["1", "3"]


def test_acquire_matchup_rosters_fetches_each_pair_once_and_keeps_both_identities(
    monkeypatch, tmp_path: Path,
) -> None:
    requested_urls: list[str] = []
    monkeypatch.setattr(module, "_primed_authenticated_session", lambda header, **_kwargs: object())

    def fetch(_session, url, _output_file, **_kwargs):
        requested_urls.append(url)
        return "valid matchup"

    monkeypatch.setattr(module, "fetch_cached_page", fetch)
    monkeypatch.setattr(
        module,
        "parse_matchup_rosters_html",
        lambda _html, *, week, team_one, team_two: [
            {"week": week, "team_number": team_one, "player": "Alpha"},
            {"week": week, "team_number": team_two, "player": "Beta"},
        ],
    )
    monkeypatch.setattr(
        module,
        "parse_matchup_manager_identities",
        lambda _html, *, team_one, team_two: {
            team_one: {"manager": "Jason", "manager_guid": "guid-jason"},
            team_two: {"manager": "Adin", "manager_guid": "guid-adin"},
        },
    )

    identities: dict[int, dict[str, str]] = {}
    rows = module.acquire_matchup_rosters(
        "T=test; Y=test",
        2025,
        tmp_path,
        [
            {"week": 1, "team_number": 1, "opponent_team_number": 2, "matchup_url": "/2025/f1/90939/matchup?week=1&mid1=1&mid2=2"},
            {"week": 1, "team_number": 2, "opponent_team_number": 1, "matchup_url": "/2025/f1/90939/matchup?week=1&mid1=1&mid2=2"},
        ],
        identity_by_team=identities,
        delay_seconds=0,
    )

    assert requested_urls == [
        "https://football.fantasysports.yahoo.com/2025/f1/90939/matchup?week=1&mid1=1&mid2=2"
    ]
    assert [(row["team_number"], row["manager_guid"]) for row in rows] == [(1, "guid-jason"), (2, "guid-adin")]
    assert identities == {
        1: {"manager": "Jason", "manager_guid": "guid-jason"},
        2: {"manager": "Adin", "manager_guid": "guid-adin"},
    }


def test_write_local_duckdb_uses_distinct_all_year_backup_filename(tmp_path: Path) -> None:
    path = module.write_local_duckdb(
        tmp_path,
        [],
        [],
        [],
        [],
        rosters=[],
        transactions=[],
        matchups=[],
        years=[2015, 2025],
    )

    assert path.name == "kmffl_2015_2025_cookie_backup.duckdb"
    assert path.is_file()
    assert not (tmp_path / "kmffl_2025_web_import.duckdb").exists()


def test_run_captures_matchups_and_manager_identity_for_single_year(monkeypatch, tmp_path: Path) -> None:
    page = module.PageResult(
        module="standings",
        url="https://example.test/standings",
        final_url="https://example.test/standings",
        status=200,
        content_type="text/html",
        bytes=1,
        sha256="x",
        output_file=str(tmp_path / "standings.html"),
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(module, "validate_cookie_path", lambda path: path)
    monkeypatch.setattr(module, "cookie_header_from_file", lambda path: "crumb=test")
    monkeypatch.setattr(module, "resolve_yahoo_profile_game_codes", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        module,
        "build_page_urls",
        lambda league_key, year, **kwargs: {"settings": "settings", "standings": "standings", "draft": "draft", "home": "home", "matchups": "matchups"},
    )
    monkeypatch.setattr(module, "acquire_pages", lambda *args, **kwargs: ({"settings": "s", "standings": "s", "draft": "d"}, [page]))
    monkeypatch.setattr(module, "parse_standings_html", lambda html: [{"rank": 1, "team": "Alpha"}])
    monkeypatch.setattr(module, "parse_draft_html", lambda html: [{"pick": 1, "player": "Player"}])
    monkeypatch.setattr(module, "parse_settings_html", lambda html: [{"setting": "playoff_start_week", "value": "15"}])
    monkeypatch.setattr(
        module,
        "acquire_matchups",
        lambda *args, **kwargs: (
            [{"year": 2025, "week": 1, "team_number": 1, "opponent_team_number": 2}],
            [],
        ),
    )

    def fake_acquire_rosters(*args, **kwargs):
        captured["identity_by_team"] = kwargs["identity_by_team"]
        kwargs["identity_by_team"][1] = {"manager": "Alice", "manager_guid": "guid-alice"}
        return [{"year": 2025, "week": 1, "team_number": 1, "yahoo_player_id": "p1"}]

    monkeypatch.setattr(module, "acquire_rosters", fake_acquire_rosters)
    monkeypatch.setattr(
        module,
        "acquire_transactions",
        lambda *args, **kwargs: [{"year": 2025, "transaction_id": "tx1", "action": "add"}],
    )
    monkeypatch.setattr(module, "acquire_faab_transactions", lambda *args, **kwargs: [])

    args = Namespace(
        cookie_jar=str(tmp_path / "cookiejar.json"),
        output_dir=str(tmp_path / "output"),
        league_key="461.l.90939",
        year=2025,
        db_name="kmffl",
        request_delay=0.0,
        throttle_retries=0,
        team_count=10,
        roster_weeks=1,
        transaction_pages=1,
        skip_history=False,
        no_fly_compare=True,
        materialize_fly_parity=False,
        allow_partial=False,
    )

    assert module.run(args) == 0
    manifest = json.loads((Path(args.output_dir) / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["matchup_rows"] == 1
    assert "matchups" not in manifest["unparsed_page_modules"]
    assert captured["identity_by_team"] == {1: {"manager": "Alice", "manager_guid": "guid-alice"}}

    import duckdb

    db_path = Path(manifest["local_duckdb"])
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM matchups_source").fetchone()[0] == 1
        assert conn.execute("SELECT manager_guid FROM team_identity").fetchone()[0] == "guid-alice"
    finally:
        conn.close()
