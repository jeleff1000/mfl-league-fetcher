"""Unit coverage for the MFL canonical mapping and the shared playoff walk."""

from multi_league.core.canonical_matchup import apply_playoff_outcomes
from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.canonical_settings import flatten_settings
from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIError
from mfl_initial_import import _resolve_league_history
import pandas as pd


def _game(week, fid, opp, win, points, opp_points, playoffs=True, consolation=False):
    return {
        "week": week,
        "franchise_id": fid,
        "opponent_franchise_id": opp,
        "win": win,
        "loss": 0 if win else 1,
        "team_points": points,
        "opponent_points": opp_points,
        "is_playoffs": playoffs,
        "is_consolation": consolation,
    }


def _both_sides(week, fid_a, fid_b, points_a, points_b, **kwargs):
    win_a = 1 if points_a > points_b else 0
    return [
        _game(week, fid_a, fid_b, win_a, points_a, points_b, **kwargs),
        _game(week, fid_b, fid_a, 1 - win_a, points_b, points_a, **kwargs),
    ]


class TestApplyPlayoffOutcomes:
    def test_single_week_final_crowns_winner(self):
        rows = (
            _both_sides(15, "A", "B", 100, 90)
            + _both_sides(15, "C", "D", 80, 85)
            + _both_sides(16, "A", "D", 120, 110)
            + _both_sides(16, "B", "C", 70, 75, consolation=True)
        )
        apply_playoff_outcomes(rows)
        champs = [r for r in rows if r["champion"] == 1]
        assert len(champs) == 1
        assert champs[0]["franchise_id"] == "A"
        final = [r for r in rows if r["is_championship"] == 1]
        assert {r["franchise_id"] for r in final} == {"A", "D"}

    def test_two_week_final_decided_on_aggregate(self):
        # A wins leg 1 by 5, D wins leg 2 by 20 -> D champions on aggregate
        rows = (
            _both_sides(15, "A", "B", 100, 90)
            + _both_sides(15, "C", "D", 80, 85)
            + _both_sides(16, "A", "D", 100, 95)
            + _both_sides(17, "A", "D", 80, 100)
        )
        apply_playoff_outcomes(rows)
        champs = {r["franchise_id"] for r in rows if r["champion"] == 1}
        assert champs == {"D"}

    def test_no_playoff_rows_is_noop(self):
        rows = _both_sides(5, "A", "B", 100, 90, playoffs=False)
        apply_playoff_outcomes(rows)
        assert all(r["champion"] == 0 for r in rows)
        assert all(r["is_championship"] == 0 for r in rows)

    def test_consolation_never_crowned(self):
        rows = _both_sides(15, "A", "B", 100, 90) + _both_sides(16, "C", "D", 150, 90, consolation=True)
        apply_playoff_outcomes(rows)
        champs = {r["franchise_id"] for r in rows if r["champion"] == 1}
        assert champs == {"A"}


def _mfl_raw():
    """Trimmed live payload shapes (probe 2024:63886, 2026-07-17)."""
    league = {
        "name": "Test Dynasty",
        "franchises": {"count": "12", "franchise": [{"id": "0001", "name": "Team One"}]},
        "starters": {
            "count": "10",
            "idp_starters": "0",
            "position": [
                {"name": "QB", "limit": "1-2"},
                {"name": "RB", "limit": "2-4"},
                {"name": "WR", "limit": "2-4"},
                {"name": "TE", "limit": "1-3"},
                {"name": "PK", "limit": "0-1"},
                {"name": "Def", "limit": "1"},
            ],
        },
        "rosterSize": "25",
        "taxiSquad": "4",
        "injuredReserve": "3",
        "lastRegularSeasonWeek": "14",
        "startWeek": "1",
        "endWeek": "17",
        "h2h": "YES",
        "bestLineup": "No",
        "draftPlayerPool": "Rookie",
        "currentWaiverType": "BBID",
        "bbidSeasonLimit": "100",
    }
    rules = {
        "rules": {
            "positionRules": [
                {
                    "positions": "QB|RB|WR|TE",
                    "rule": [
                        {"event": {"$t": "#P"}, "points": {"$t": "*4"}, "range": {"$t": "0-10"}},
                        {"event": {"$t": "PY"}, "points": {"$t": "1/25"}, "range": {"$t": "-50-999"}},
                        {"event": {"$t": "IN"}, "points": {"$t": "*-2"}, "range": {"$t": "0-10"}},
                        {"event": {"$t": "CC"}, "points": {"$t": "*.5"}, "range": {"$t": "0-99"}},
                    ],
                },
                {
                    "positions": "TE",
                    "rule": {"event": {"$t": "CC"}, "points": {"$t": "*1"}, "range": {"$t": "0-99"}},
                },
            ]
        }
    }
    brackets = {
        "playoffBrackets": {
            "playoffBracket": [
                {"name": "Championship", "startWeek": "15", "teamsInvolved": "6",
                 "bracketWinnerTitle": "League Champion", "id": "1"},
                {"name": "Toilet Bowl", "startWeek": "15", "teamsInvolved": "6",
                 "bracketWinnerTitle": "Consolation Winner", "id": "2"},
            ]
        }
    }
    return {"league": league, "rules": rules, "brackets": brackets}


class TestExtractMfl:
    def test_roster_preserves_native_player_id_for_later_resolution(self):
        roster = normalize_roster_df(
            pd.DataFrame([{
                "year": 2024,
                "week": 1,
                "manager": "Manager",
                "player": "Example Player",
                "mfl_player_id": "12345",
                "position": "RB",
                "fantasy_position": "RB",
            }]),
            platform="mfl",
            league_id="63886",
        )

        assert roster.loc[0, "mfl_player_id"] == "12345"
        assert pd.isna(roster.loc[0, "fleaflicker_player_id"])

    def test_cohort_dims_and_gates(self):
        flat = flatten_settings(_mfl_raw(), platform="mfl", year=2024, league_key="63886")
        assert flat["num_teams"] == 12
        assert flat["scoring_rec"] == 0.5
        assert flat["scoring_bonus_rec_te"] == 0.5
        assert flat["scoring_pass_td"] == 4.0
        assert flat["scoring_pass_yd"] == 1 / 25
        assert flat["playoff_start_week"] == 15
        assert flat["regular_season_weeks"] == 14
        assert flat["playoff_teams"] == 6
        assert flat["waiver_budget"] == 100
        assert flat["sleeper_best_ball"] is False

    def test_roster_ranges(self):
        flat = flatten_settings(_mfl_raw(), platform="mfl", year=2024, league_key="63886")
        assert flat["roster_QB"] == 1
        assert flat["roster_SUPER_FLEX"] == 1  # QB "1-2"
        assert flat["roster_K"] == 1  # PK "0-1": CAN start -> gate open
        assert flat["roster_DEF"] == 1
        assert flat["roster_BN"] == 15  # rosterSize 25 - starters 10
        assert flat["roster_TAXI"] == 4
        assert flat["roster_IR"] == 3

    def test_degenerate_last_regular_week_falls_back_to_bracket(self):
        raw = _mfl_raw()
        raw["league"]["lastRegularSeasonWeek"] = "1"
        flat = flatten_settings(raw, platform="mfl", year=2024, league_key="63886")
        assert flat["playoff_start_week"] == 15  # championship bracket startWeek

    def test_missing_rules_leaves_scoring_null_not_wrong(self):
        raw = _mfl_raw()
        raw["rules"] = {"error": {"$t": "restricted"}}
        flat = flatten_settings(raw, platform="mfl", year=2024, league_key="63886")
        assert flat["scoring_pass_td"] is None
        # must stay NULL, not default to 0-PPR: unknown scoring is not zero-reception
        assert flat["scoring_rec"] is None


def test_history_discovery_drops_invalid_historical_ids_instead_of_killing_import():
    class FakeClient:
        def fetch_league(self, league_id, year):
            if league_id == "bad":
                raise MFLAPIError("Invalid league ID")
            if year == 2024:
                return {
                    "history": {
                        "league": [
                            {"url": "https://api.myfantasyleague.com/2012/home/bad", "year": "2012"},
                            {"url": "https://api.myfantasyleague.com/2023/home/good", "year": "2023"},
                        ]
                    }
                }
            return {}

    league_ids, _ = _resolve_league_history(FakeClient(), 2024, "seed")

    assert league_ids == {"2024": "seed", "2023": "good"}
