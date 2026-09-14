from pathlib import Path

from multi_league.data_fetchers.sleeper.playoff_utils import (
    calculate_playoff_rounds,
    calculate_weeks_in_playoffs,
    championship_contenders_for_round,
    playoff_round_for_week,
    playoff_weeks_for_round,
    resolve_playoff_structure,
)
from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext
from multi_league.data_fetchers.sleeper.sleeper_league_settings import fetch_sleeper_settings
from multi_league.data_fetchers.sleeper.sleeper_matchups import SleeperMatchupFetcher
from multi_league.data_fetchers.sleeper.sleeper_schedules import SleeperScheduleFetcher


# Real bracket from dingleberry_derby 2018 Sleeper API (4-team playoff, 2 rounds).
# Round 1: m=1 (1 vs 3, 3 won), m=2 (8 vs 7, 8 won)
# Round 2: m=3 p=1 championship (3 vs 8, 8 won), m=4 p=3 third place (1 vs 7, 1 won)
_DINGLEBERRY_2018_DECIDED_BRACKET = [
    {"m": 1, "r": 1, "l": 1, "w": 3, "t1": 1, "t2": 3},
    {"m": 2, "r": 1, "l": 7, "w": 8, "t1": 8, "t2": 7},
    {"p": 1, "m": 3, "r": 2, "l": 3, "w": 8, "t1": 3, "t2": 8, "t1_from": {"w": 1}, "t2_from": {"w": 2}},
    {"p": 3, "m": 4, "r": 2, "l": 7, "w": 1, "t1": 1, "t2": 7, "t1_from": {"l": 1}, "t2_from": {"l": 2}},
]

# Same bracket structure but championship in progress: m=3 has w=None (not yet decided)
_DINGLEBERRY_2018_CHAMPIONSHIP_LIVE_BRACKET = [
    {"m": 1, "r": 1, "l": 1, "w": 3, "t1": 1, "t2": 3},
    {"m": 2, "r": 1, "l": 7, "w": 8, "t1": 8, "t2": 7},
    {"p": 1, "m": 3, "r": 2, "l": None, "w": None, "t1": 3, "t2": 8, "t1_from": {"w": 1}, "t2_from": {"w": 2}},
    {"p": 3, "m": 4, "r": 2, "l": None, "w": None, "t1": 1, "t2": 7, "t1_from": {"l": 1}, "t2_from": {"l": 2}},
]


def test_championship_contenders_returns_empty_for_round_beyond_decided_bracket():
    """After the championship is decided (p=1 matchup has w set), asking for any
    round beyond the bracket's depth must return an empty set. This prevents the
    Sleeper fetcher from flagging post-championship phantom rows (weeks past the
    last actual round) as still in championship contention.

    Concrete fleet repro: dingleberry_derby 2018 has 4-team bracket (2 rounds,
    last_scored_leg=16 but championship was W15). For target_round=3 (W16
    phantom rows), the function must return empty so the W16 bye rows for
    SellOffForPicks (champion) and HerbertsOlMan (runner-up) are NOT flagged
    as is_playoffs=True by `_determine_playoff_flags`.
    """
    contenders = championship_contenders_for_round(_DINGLEBERRY_2018_DECIDED_BRACKET, target_round=3)

    assert contenders == set(), (
        "After championship has been decided, no roster is in championship "
        f"contention for any round beyond the bracket; got {contenders}"
    )


def test_championship_contenders_returns_empty_for_round_far_beyond_decided_bracket():
    """Same rule applies for any target_round beyond bracket depth (not just +1)."""
    assert championship_contenders_for_round(_DINGLEBERRY_2018_DECIDED_BRACKET, target_round=5) == set()
    assert championship_contenders_for_round(_DINGLEBERRY_2018_DECIDED_BRACKET, target_round=10) == set()


def test_championship_contenders_does_not_short_circuit_when_championship_in_progress():
    """In-season safety: while the championship is being played (p=1 matchup
    has w=None), the function must NOT short-circuit. The existing fallback
    logic should still consider the championship participants as contenders.

    This protects the case where Sleeper's API exposes the bracket but games
    haven't been decided yet — current-round teams must still be flagged as
    is_playoffs=True.
    """
    # During mid-W15 (championship in progress), target_round=2 must return
    # the championship participants {3, 8} via the existing logic, not empty.
    contenders = championship_contenders_for_round(_DINGLEBERRY_2018_CHAMPIONSHIP_LIVE_BRACKET, target_round=2)
    assert contenders == {3, 8}, (
        "While championship is in progress, the bracket-walk fallback must "
        f"still identify both finalists as contenders; got {contenders}"
    )


def test_championship_contenders_returns_correct_results_for_in_bracket_rounds():
    """Regression guard: short-circuit must NOT affect rounds within bracket depth."""
    # Round 1 (W14 semifinals): all 4 wildcard participants
    assert championship_contenders_for_round(_DINGLEBERRY_2018_DECIDED_BRACKET, target_round=1) == {1, 3, 7, 8}
    # Round 2 (W15 championship): both finalists
    assert championship_contenders_for_round(_DINGLEBERRY_2018_DECIDED_BRACKET, target_round=2) == {3, 8}


def test_championship_contenders_handles_empty_bracket():
    """Empty bracket (e.g., before playoffs start) returns empty for all rounds."""
    assert championship_contenders_for_round([], target_round=1) == set()
    assert championship_contenders_for_round([], target_round=3) == set()


class _FakeSleeperClient:
    def __init__(
        self,
        league: dict,
        *,
        winners_bracket: list[dict] | None = None,
        losers_bracket: list[dict] | None = None,
        matchups_by_week: dict[int, list[dict]] | None = None,
        users: list[dict] | None = None,
        rosters: list[dict] | None = None,
    ):
        self._league = league
        self._winners_bracket = winners_bracket or []
        self._losers_bracket = losers_bracket or []
        self._matchups_by_week = matchups_by_week or {}
        self._users = users
        self._rosters = rosters

    def get_league(self, _league_id: str):
        return self._league

    def get_league_users(self, _league_id: str):
        if self._users is not None:
            return self._users
        return [{"user_id": "u1", "display_name": "Alpha", "metadata": {"team_name": "Alpha"}}]

    def get_league_rosters(self, _league_id: str):
        if self._rosters is not None:
            return self._rosters
        return [{"roster_id": 1, "owner_id": "u1"}]

    def get_nfl_state(self):
        return {"season_type": "off", "week": 18, "season": 2024}

    def get_losers_bracket(self, _league_id: str):
        return self._losers_bracket

    def get_winners_bracket(self, _league_id: str):
        return self._winners_bracket

    def get_league_matchups(self, _league_id: str, week: int):
        return self._matchups_by_week.get(week, [])


def _multiweek_championship_league() -> dict:
    return {
        "name": "League Of Throws",
        "season": "2024",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 12,
        "draft_id": "draft_1",
        "settings": {
            "playoff_teams": 6,
            "playoff_week_start": 0,
            "playoff_round_type": 1,
            "last_scored_leg": 18,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 15,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "BN", "BN"],
    }


def test_resolve_playoff_structure_infers_multiweek_championship_start_week():
    structure = resolve_playoff_structure(_multiweek_championship_league()["settings"])

    assert structure["playoff_round_type"] == 2
    assert structure["playoff_rounds"] == 3
    assert structure["weeks_in_playoffs"] == 4
    assert structure["playoff_week_start"] == 15
    assert structure["playoff_week_end"] == 18
    assert structure["playoff_start_source"] == "inferred"


def _no_playoff_league() -> dict:
    return {
        "name": "No Bracket League",
        "season": "2024",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 10,
        "draft_id": "draft_no_playoffs",
        "settings": {
            "playoff_teams": 0,
            "playoff_week_start": 0,
            "playoff_round_type": 0,
            "last_scored_leg": 14,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 15,
        },
        "scoring_settings": {"rec": 0.5, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "BN"],
    }


def test_resolve_playoff_structure_handles_disabled_playoffs():
    structure = resolve_playoff_structure(_no_playoff_league()["settings"])

    assert calculate_playoff_rounds(0) == 0
    assert calculate_weeks_in_playoffs(0, 0) == 0
    assert structure["playoff_teams"] == 0
    assert structure["playoff_rounds"] == 0
    assert structure["weeks_in_playoffs"] == 0
    assert structure["playoff_week_start"] == 15
    assert structure["playoff_week_end"] == 14
    assert structure["playoff_start_source"] == "disabled"


def test_fetch_sleeper_settings_handles_disabled_playoffs():
    settings = fetch_sleeper_settings(_FakeSleeperClient(_no_playoff_league()), "league_no_playoffs", year=2024)

    assert settings["playoff_teams"] == 0
    assert settings["bye_teams"] == 0
    assert settings["playoff_start_week"] == 15
    assert settings["regular_season_weeks"] == 14
    assert settings["end_week"] == 14
    assert settings["has_multiweek_championship"] == 0
    assert settings["metadata"]["num_rounds"] == 0
    assert settings["metadata"]["championship_week"] is None


def test_fetch_sleeper_settings_preserves_multiweek_championship_playoff_start():
    settings = fetch_sleeper_settings(_FakeSleeperClient(_multiweek_championship_league()), "league_1", year=2024)

    assert settings["playoff_start_week"] == 15
    assert settings["regular_season_weeks"] == 14
    assert settings["end_week"] == 18
    assert settings["has_multiweek_championship"] == 1
    assert settings["settings"]["playoff_round_type"] == 2


def test_matchup_fetcher_does_not_flatten_guessed_playoff_flags_without_bracket_api(tmp_path: Path):
    league = {
        "name": "Missing Bracket API",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 2,
        "draft_id": "draft_missing_bracket",
        "settings": {
            "playoff_teams": 2,
            "playoff_week_start": 15,
            "playoff_round_type": 0,
            "last_scored_leg": 15,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }
    users = [
        {"user_id": "u1", "display_name": "Alpha", "metadata": {"team_name": "Alpha"}},
        {"user_id": "u2", "display_name": "Beta", "metadata": {"team_name": "Beta"}},
    ]
    rosters = [{"roster_id": 1, "owner_id": "u1"}, {"roster_id": 2, "owner_id": "u2"}]
    client = _FakeSleeperClient(
        league,
        winners_bracket=[],
        losers_bracket=[],
        matchups_by_week={
            15: [
                {"roster_id": 1, "matchup_id": 1, "points": 120},
                {"roster_id": 2, "matchup_id": 1, "points": 110},
            ]
        },
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="missing_bracket_api",
        league_name="Missing Bracket API",
        username="tester",
        data_directory=tmp_path / "missing_bracket_api",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)
    rows = fetcher.fetch_matchups_for_week(
        "missing_bracket_api", 2025, 15, fetcher._get_roster_mappings("missing_bracket_api")
    )

    assert {row["manager"]: row["is_playoffs"] for row in rows} == {"Alpha": False, "Beta": False}
    assert {row["manager"]: row["is_consolation"] for row in rows} == {"Alpha": False, "Beta": False}


def test_sleeper_schedule_fetcher_uses_shared_playoff_structure(tmp_path: Path):
    ctx = SleeperContext(
        league_id="league_1",
        league_name="League Of Throws",
        username="tester",
        data_directory=tmp_path / "league_of_throws",
    )
    fetcher = SleeperScheduleFetcher(ctx, client=_FakeSleeperClient(_multiweek_championship_league()))

    structure = fetcher._get_playoff_structure("league_1")

    assert structure["playoff_week_start"] == 15
    assert structure["playoff_round_type"] == 2


def test_playoff_round_for_week_reuses_round_for_all_two_week_format():
    assert playoff_round_for_week(14, 14, 1, 2) == 1
    assert playoff_round_for_week(15, 14, 1, 2) == 1
    assert playoff_round_for_week(16, 14, 1, 2) == 2
    assert playoff_round_for_week(17, 14, 1, 2) == 2
    assert playoff_round_for_week(18, 14, 1, 2) == 3


def test_playoff_round_for_week_reuses_final_for_two_week_championship_only():
    assert playoff_round_for_week(15, 15, 2, 3) == 1
    assert playoff_round_for_week(16, 15, 2, 3) == 2
    assert playoff_round_for_week(17, 15, 2, 3) == 3
    assert playoff_round_for_week(18, 15, 2, 3) == 3
    assert playoff_weeks_for_round(15, 3, 2, 3) == (17, 18)


def _all_rounds_multiweek_league() -> dict:
    return {
        "name": "Two Leg Playoffs",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 4,
        "draft_id": "draft_two_leg",
        "settings": {
            "playoff_teams": 4,
            "playoff_week_start": 14,
            "playoff_round_type": 2,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }


def _four_team_users_and_rosters():
    users = [
        {"user_id": "u1", "display_name": "Alpha", "metadata": {"team_name": "Alpha"}},
        {"user_id": "u2", "display_name": "Beta", "metadata": {"team_name": "Beta"}},
        {"user_id": "u3", "display_name": "Gamma", "metadata": {"team_name": "Gamma"}},
        {"user_id": "u4", "display_name": "Delta", "metadata": {"team_name": "Delta"}},
    ]
    rosters = [
        {"roster_id": 1, "owner_id": "u1"},
        {"roster_id": 2, "owner_id": "u2"},
        {"roster_id": 3, "owner_id": "u3"},
        {"roster_id": 4, "owner_id": "u4"},
    ]
    return users, rosters


def _all_rounds_multiweek_bracket():
    return [
        {"m": 1, "r": 1, "w": 1, "t1": 1, "t2": 4},
        {"m": 2, "r": 1, "w": 2, "t1": 2, "t2": 3},
        {"p": 1, "m": 3, "r": 2, "w": 1, "t1": 1, "t2": 2},
        {"p": 3, "m": 4, "r": 2, "w": 3, "t1": 4, "t2": 3},
    ]


def _all_rounds_multiweek_matchups():
    return {
        15: [
            {"roster_id": 1, "matchup_id": 1, "points": 105},
            {"roster_id": 4, "matchup_id": 1, "points": 93},
            {"roster_id": 2, "matchup_id": 2, "points": 111},
            {"roster_id": 3, "matchup_id": 2, "points": 89},
        ],
        16: [
            {"roster_id": 1, "matchup_id": 1, "points": 120},
            {"roster_id": 2, "matchup_id": 1, "points": 110},
            {"roster_id": 4, "matchup_id": 2, "points": 91},
            {"roster_id": 3, "matchup_id": 2, "points": 102},
        ],
        17: [
            {"roster_id": 1, "matchup_id": 1, "points": 118},
            {"roster_id": 2, "matchup_id": 1, "points": 112},
            {"roster_id": 4, "matchup_id": 2, "points": 88},
            {"roster_id": 3, "matchup_id": 2, "points": 99},
        ],
    }


def test_schedule_fetcher_keeps_second_leg_in_same_sleeper_bracket_round(tmp_path: Path):
    users, rosters = _four_team_users_and_rosters()
    client = _FakeSleeperClient(
        _all_rounds_multiweek_league(),
        winners_bracket=_all_rounds_multiweek_bracket(),
        matchups_by_week=_all_rounds_multiweek_matchups(),
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="league_two_leg",
        league_name="Two Leg Playoffs",
        username="tester",
        data_directory=tmp_path / "two_leg_playoffs",
    )
    fetcher = SleeperScheduleFetcher(ctx, client=client)
    roster_map = fetcher._get_roster_mappings("league_two_leg")

    week_15 = fetcher.fetch_schedule_for_week("league_two_leg", 2025, 15, roster_map)
    alpha_week_15 = next(row for row in week_15 if row["manager"] == "Alpha")
    assert alpha_week_15["opponent"] == "Delta"
    assert alpha_week_15["is_playoffs"] is True
    assert alpha_week_15["playoff_round"] == "Semifinal"
    assert alpha_week_15["championship"] is False

    week_17 = fetcher.fetch_schedule_for_week("league_two_leg", 2025, 17, roster_map)
    alpha_week_17 = next(row for row in week_17 if row["manager"] == "Alpha")
    assert alpha_week_17["opponent"] == "Beta"
    assert alpha_week_17["is_playoffs"] is True
    assert alpha_week_17["playoff_round"] == "Championship"
    assert alpha_week_17["championship"] is True


def test_matchup_fetcher_pairs_and_marks_both_legs_of_multiweek_final(tmp_path: Path):
    users, rosters = _four_team_users_and_rosters()
    client = _FakeSleeperClient(
        _all_rounds_multiweek_league(),
        winners_bracket=_all_rounds_multiweek_bracket(),
        matchups_by_week=_all_rounds_multiweek_matchups(),
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="league_two_leg",
        league_name="Two Leg Playoffs",
        username="tester",
        data_directory=tmp_path / "two_leg_playoffs",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)
    roster_map = fetcher._get_roster_mappings("league_two_leg")

    week_15 = fetcher.fetch_matchups_for_week("league_two_leg", 2025, 15, roster_map)
    alpha_week_15 = next(row for row in week_15 if row["manager"] == "Alpha")
    assert alpha_week_15["opponent"] == "Delta"
    assert alpha_week_15["is_playoffs"] is True
    assert alpha_week_15["championship"] == 0

    week_17 = fetcher.fetch_matchups_for_week("league_two_leg", 2025, 17, roster_map)
    alpha_week_17 = next(row for row in week_17 if row["manager"] == "Alpha")
    assert alpha_week_17["opponent"] == "Beta"
    assert alpha_week_17["is_playoffs"] is True
    assert alpha_week_17["championship"] == 1
    assert alpha_week_17["champion"] == 1


def test_matchup_fetcher_uses_sleeper_custom_points_for_completed_championship(tmp_path: Path):
    league = {
        "name": "The Dinosty",
        "season": "2022",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 8,
        "settings": {
            "playoff_teams": 4,
            "playoff_week_start": 16,
            "playoff_round_type": 0,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 2,
            "draft_rounds": 28,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN"],
    }
    users = [
        {"user_id": "u6", "display_name": "MrPool", "metadata": {"team_name": "MrPool"}},
        {"user_id": "u7", "display_name": "Battlecats", "metadata": {"team_name": "Battlecats"}},
    ]
    rosters = [{"roster_id": 6, "owner_id": "u6"}, {"roster_id": 7, "owner_id": "u7"}]
    winners_bracket = [
        {"m": 1, "r": 1, "w": 6, "t1": 6, "t2": 3},
        {"m": 2, "r": 1, "w": 7, "t1": 8, "t2": 7},
        {"p": 1, "m": 3, "r": 2, "w": 6, "l": 7, "t1": 6, "t2": 7},
    ]
    matchups_by_week = {
        17: [
            {"roster_id": 6, "matchup_id": 1, "points": 104.6, "custom_points": 142.19000244140625},
            {"roster_id": 7, "matchup_id": 1, "points": 123.87, "custom_points": None},
        ]
    }
    client = _FakeSleeperClient(
        league,
        winners_bracket=winners_bracket,
        matchups_by_week=matchups_by_week,
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="the_dinosty_2022",
        league_name="The Dinosty",
        username="tester",
        data_directory=tmp_path / "the_dinosty",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)
    roster_map = fetcher._get_roster_mappings("the_dinosty_2022")

    rows = fetcher.fetch_matchups_for_week("the_dinosty_2022", 2022, 17, roster_map)
    mrpool = next(row for row in rows if row["manager"] == "MrPool")
    battlecats = next(row for row in rows if row["manager"] == "Battlecats")

    assert mrpool["team_points"] == 142.19
    assert mrpool["opponent_points"] == 123.87
    assert mrpool["win"] == 1
    assert mrpool["champion"] == 1
    assert battlecats["team_points"] == 123.87
    assert battlecats["loss"] == 1
    assert battlecats["champion"] == 0


def test_matchup_fetcher_keeps_api_pairings_over_playoff_bracket(tmp_path: Path):
    users, rosters = _four_team_users_and_rosters()
    league = _all_rounds_multiweek_league()
    league["settings"]["playoff_week_start"] = 15
    league["settings"]["playoff_round_type"] = 0
    league["settings"]["last_scored_leg"] = 17
    client = _FakeSleeperClient(
        league,
        winners_bracket=[{"m": 1, "r": 1, "w": 1, "t1": 1, "t2": 2}],
        matchups_by_week={
            15: [
                {"roster_id": 1, "matchup_id": 9, "points": 101},
                {"roster_id": 3, "matchup_id": 9, "points": 99},
                {"roster_id": 2, "matchup_id": 10, "points": 88},
                {"roster_id": 4, "matchup_id": 10, "points": 87},
            ]
        },
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="api_pairing",
        league_name="API Pairing",
        username="tester",
        data_directory=tmp_path / "api_pairing",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)
    pairs = fetcher._pair_matchups(client.get_league_matchups("api_pairing", 15), league_id="api_pairing", week=15)
    pair_ids = {frozenset((a.get("roster_id"), b.get("roster_id"))) for a, b in pairs if b is not None}

    assert frozenset((1, 3)) in pair_ids
    assert frozenset((1, 2)) not in pair_ids


def test_matchup_fetcher_does_not_pair_null_matchup_id_rows_without_api_game(tmp_path: Path):
    """Null matchup_id rows can carry points, but they are not an API game.

    Regression for dynast_league 2025: first-round playoff bye/non-bracket
    roster rows had NULL matchup_id values. The fetcher must not pair those
    leftovers with each other after processing the real Sleeper matchup groups.
    """
    league = {
        "name": "Null Side Rows",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 12,
        "draft_id": "draft_null_side_rows",
        "settings": {
            "playoff_teams": 6,
            "playoff_week_start": 15,
            "playoff_round_type": 0,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }
    winners_bracket = [
        {"m": 1, "r": 1, "w": 7, "t1": 7, "t2": 12},
        {"m": 2, "r": 1, "w": 1, "t1": 3, "t2": 1},
        {"m": 3, "r": 2, "w": 7, "t1": 4, "t2": 7},
        {"m": 4, "r": 2, "w": 9, "t1": 9, "t2": 1},
        {"p": 1, "m": 6, "r": 3, "w": 9, "t1": 7, "t2": 9},
    ]
    losers_bracket = [
        {"m": 1, "r": 1, "w": 11, "t1": 11, "t2": 8},
        {"m": 2, "r": 1, "w": 6, "t1": 10, "t2": 6},
    ]
    matchups = [
        {"roster_id": 7, "matchup_id": 1, "points": 143.3},
        {"roster_id": 12, "matchup_id": 1, "points": 142.5},
        {"roster_id": 1, "matchup_id": 2, "points": 117.6},
        {"roster_id": 3, "matchup_id": 2, "points": 93.8},
        {"roster_id": 8, "matchup_id": 4, "points": 82.38},
        {"roster_id": 11, "matchup_id": 4, "points": 61.62},
        {"roster_id": 6, "matchup_id": 5, "points": 63.8},
        {"roster_id": 10, "matchup_id": 5, "points": 113.46},
        {"roster_id": 2, "matchup_id": None, "points": 62.68},
        {"roster_id": 4, "matchup_id": None, "points": 113.96},
        {"roster_id": 5, "matchup_id": None, "points": 113.66},
        {"roster_id": 9, "matchup_id": None, "points": 83.9},
    ]
    client = _FakeSleeperClient(
        league,
        winners_bracket=winners_bracket,
        losers_bracket=losers_bracket,
        matchups_by_week={15: matchups},
    )
    ctx = SleeperContext(
        league_id="null_side_rows",
        league_name="Null Side Rows",
        username="tester",
        data_directory=tmp_path / "null_side_rows",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)

    pairs = fetcher._pair_matchups(matchups, league_id="null_side_rows", week=15)
    pair_ids = {frozenset((a.get("roster_id"), b.get("roster_id"))) for a, b in pairs if b is not None}
    paired_rosters = {roster_id for pair in pair_ids for roster_id in pair}

    assert pair_ids == {
        frozenset((7, 12)),
        frozenset((1, 3)),
        frozenset((8, 11)),
        frozenset((6, 10)),
    }
    assert paired_rosters.isdisjoint({2, 4, 5, 9})


def test_matchup_fetcher_skips_real_sleeper_bye_rows_not_in_round_pairings(tmp_path: Path):
    """Real 2025 shape: top seeds can have NULL matchup rows during byes.

    Sleeper exposes roster scores for those teams, but the weekly API does not
    assign a matchup_id and the bracket API does not list that pair in round 1.
    Those rows must stay unpaired so the bye filler can create neutral byes.
    """
    league = {
        "name": "BB D Lovers Shape",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 12,
        "draft_id": "draft_bbd",
        "settings": {
            "playoff_teams": 6,
            "playoff_week_start": 15,
            "playoff_round_type": 0,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }
    winners_bracket = [
        {"m": 1, "r": 1, "l": 8, "w": 3, "t1": 3, "t2": 8},
        {"m": 2, "r": 1, "l": 5, "w": 7, "t1": 5, "t2": 7},
        {"m": 3, "r": 2, "l": 3, "w": 1, "t1": 1, "t2": 3, "t2_from": {"w": 1}},
        {"m": 4, "r": 2, "l": 7, "w": 6, "t1": 6, "t2": 7, "t2_from": {"w": 2}},
        {"m": 6, "p": 1, "r": 3, "l": 6, "w": 1, "t1": 1, "t2": 6},
    ]
    losers_bracket = [
        {"m": 1, "r": 1, "l": 12, "w": 2, "t1": 2, "t2": 12},
        {"m": 2, "r": 1, "l": 4, "w": 11, "t1": 11, "t2": 4},
        {"m": 6, "p": 1, "r": 3, "l": 2, "w": 9, "t1": 2, "t2": 9},
    ]
    matchups = [
        {"roster_id": 1, "matchup_id": None, "points": 176.98},
        {"roster_id": 2, "matchup_id": 4, "points": 130.78},
        {"roster_id": 3, "matchup_id": 1, "points": 148.78},
        {"roster_id": 4, "matchup_id": 5, "points": 152.2},
        {"roster_id": 5, "matchup_id": 2, "points": 147.9},
        {"roster_id": 6, "matchup_id": None, "points": 178.68},
        {"roster_id": 7, "matchup_id": 2, "points": 173.26},
        {"roster_id": 8, "matchup_id": 1, "points": 117.22},
        {"roster_id": 9, "matchup_id": None, "points": 90.18},
        {"roster_id": 10, "matchup_id": None, "points": 105.1},
        {"roster_id": 11, "matchup_id": 5, "points": 151.68},
        {"roster_id": 12, "matchup_id": 4, "points": 166.22},
    ]
    client = _FakeSleeperClient(
        league,
        winners_bracket=winners_bracket,
        losers_bracket=losers_bracket,
        matchups_by_week={15: matchups},
    )
    ctx = SleeperContext(
        league_id="bbd_shape",
        league_name="BB D Lovers Shape",
        username="tester",
        data_directory=tmp_path / "bbd_shape",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)

    pairs = fetcher._pair_matchups(matchups, league_id="bbd_shape", week=15)
    pair_ids = {frozenset((a.get("roster_id"), b.get("roster_id"))) for a, b in pairs if b is not None}
    paired_rosters = {roster_id for pair in pair_ids for roster_id in pair}

    assert pair_ids == {
        frozenset((3, 8)),
        frozenset((5, 7)),
        frozenset((2, 12)),
        frozenset((4, 11)),
    }
    assert frozenset((1, 6)) not in pair_ids
    assert frozenset((9, 10)) not in pair_ids
    assert paired_rosters.isdisjoint({1, 6, 9, 10})


def test_matchup_fetcher_uses_original_finalists_for_null_championship_rows(tmp_path: Path):
    """Some Sleeper brackets expose original finalists separately from t1/t2.

    Regression for 2025 Sleeper leagues where the p=1 bracket row had
    t1_original/t2_original matching the NULL matchup_id championship rows,
    while t1/t2 pointed at another played placement game. The weekly API still
    owns the actual games; bracket originals only recover the missing pairing.
    """
    league = {
        "name": "Original Finalists",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 10,
        "draft_id": "draft_original_finalists",
        "settings": {
            "playoff_teams": 6,
            "playoff_week_start": 15,
            "playoff_round_type": 0,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }
    users = [
        {"user_id": f"u{i}", "display_name": f"Team {i}", "metadata": {"team_name": f"Team {i}"}} for i in range(1, 11)
    ]
    rosters = [{"roster_id": i, "owner_id": f"u{i}"} for i in range(1, 11)]
    winners_bracket = [
        {"m": 1, "r": 1, "w": 8, "l": 10, "t1": 8, "t2": 10},
        {"m": 2, "r": 1, "w": 9, "l": 1, "t1": 1, "t2": 9},
        {"m": 3, "r": 2, "w": 7, "l": 8, "t1": 7, "t2": 8, "t2_from": {"w": 1}},
        {"m": 4, "r": 2, "w": 9, "l": 2, "t1": 2, "t2": 9, "t2_from": {"w": 2}},
        {"m": 5, "p": 5, "r": 2, "w": 10, "l": 1, "t1": 10, "t2": 1},
        {
            "m": 6,
            "p": 1,
            "r": 3,
            "w": 3,
            "l": 6,
            "t1": 3,
            "t2": 6,
            "t1_original": 7,
            "t2_original": 9,
            "t1_from": {"w": 3},
            "t2_from": {"w": 4},
        },
        {"m": 7, "p": 3, "r": 3, "w": 2, "l": 8, "t1": 8, "t2": 2},
    ]
    matchups = [
        {"roster_id": 3, "matchup_id": 1, "points": 120.48},
        {"roster_id": 6, "matchup_id": 1, "points": 55.08},
        {"roster_id": 2, "matchup_id": 2, "points": 136.42},
        {"roster_id": 8, "matchup_id": 2, "points": 101.96},
        {"roster_id": 7, "matchup_id": None, "points": 172.36},
        {"roster_id": 9, "matchup_id": None, "points": 128.12},
        {"roster_id": 1, "matchup_id": None, "points": 132.78},
        {"roster_id": 4, "matchup_id": None, "points": 80.88},
        {"roster_id": 5, "matchup_id": None, "points": 110.84},
        {"roster_id": 10, "matchup_id": None, "points": 173.48},
    ]
    client = _FakeSleeperClient(
        league,
        winners_bracket=winners_bracket,
        matchups_by_week={17: matchups},
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="original_finalists",
        league_name="Original Finalists",
        username="tester",
        data_directory=tmp_path / "original_finalists",
    )
    fetcher = SleeperMatchupFetcher(ctx, client=client)
    roster_map = fetcher._get_roster_mappings("original_finalists")

    assert championship_contenders_for_round(winners_bracket, target_round=3) == {7, 9}

    pairs = fetcher._pair_matchups(matchups, league_id="original_finalists", week=17)
    pair_ids = {frozenset((a.get("roster_id"), b.get("roster_id"))) for a, b in pairs if b is not None}
    assert frozenset((7, 9)) in pair_ids

    rows = fetcher.fetch_matchups_for_week("original_finalists", 2025, 17, roster_map)
    by_roster = {int(row["team_key"]): row for row in rows}

    assert by_roster[7]["opponent"] == "Team 9"
    assert by_roster[7]["is_playoffs"] is True
    assert by_roster[7]["is_consolation"] is False
    assert by_roster[7]["championship"] == 1
    assert by_roster[7]["champion"] == 1
    assert by_roster[9]["opponent"] == "Team 7"
    assert by_roster[9]["championship"] == 1
    assert by_roster[9]["champion"] == 0
    assert by_roster[3]["is_playoffs"] is False
    assert by_roster[3]["is_consolation"] is True
    assert by_roster[3]["championship"] == 0
    assert by_roster[8]["is_playoffs"] is False
    assert by_roster[8]["is_consolation"] is True
    assert by_roster[2]["champion"] == 0


def test_schedule_fetcher_marks_extra_postseason_pairings_as_consolation(tmp_path: Path):
    league = {
        "name": "Placement Chaos",
        "season": "2025",
        "status": "complete",
        "sport": "nfl",
        "total_rosters": 4,
        "draft_id": "draft_placement",
        "settings": {
            "playoff_teams": 2,
            "playoff_week_start": 15,
            "playoff_round_type": 0,
            "last_scored_leg": 17,
            "start_week": 1,
            "type": 0,
            "draft_rounds": 10,
        },
        "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
        "roster_positions": ["QB", "RB", "WR", "TE", "FLEX", "BN", "BN"],
    }
    users = [
        {"user_id": "u1", "display_name": "Alpha", "metadata": {"team_name": "Alpha"}},
        {"user_id": "u2", "display_name": "Beta", "metadata": {"team_name": "Beta"}},
        {"user_id": "u3", "display_name": "Gamma", "metadata": {"team_name": "Gamma"}},
        {"user_id": "u4", "display_name": "Delta", "metadata": {"team_name": "Delta"}},
    ]
    rosters = [
        {"roster_id": 1, "owner_id": "u1"},
        {"roster_id": 2, "owner_id": "u2"},
        {"roster_id": 3, "owner_id": "u3"},
        {"roster_id": 4, "owner_id": "u4"},
    ]
    winners_bracket = [{"m": 1, "r": 1, "w": 1, "t1": 1, "t2": 2, "p": 1}]
    losers_bracket: list[dict] = []
    matchups_by_week = {
        15: [
            {"roster_id": 1, "matchup_id": 1, "points": 120},
            {"roster_id": 2, "matchup_id": 1, "points": 110},
            # These teams are not in Sleeper's bracket metadata, but they are
            # still playing a real postseason placement game in the weekly API.
            {"roster_id": 3, "matchup_id": 2, "points": 0},
            {"roster_id": 4, "matchup_id": 2, "points": 0},
        ]
    }
    client = _FakeSleeperClient(
        league,
        winners_bracket=winners_bracket,
        losers_bracket=losers_bracket,
        matchups_by_week=matchups_by_week,
        users=users,
        rosters=rosters,
    )
    ctx = SleeperContext(
        league_id="league_placement",
        league_name="Placement Chaos",
        username="tester",
        data_directory=tmp_path / "placement_chaos",
    )
    fetcher = SleeperScheduleFetcher(ctx, client=client)
    roster_map = fetcher._get_roster_mappings("league_placement")

    rows = fetcher.fetch_schedule_for_week("league_placement", 2025, 15, roster_map)
    gamma_row = next(row for row in rows if row["manager"] == "Gamma")
    delta_row = next(row for row in rows if row["manager"] == "Delta")

    assert gamma_row["is_playoffs"] is False
    assert gamma_row["is_consolation"] is True
    assert gamma_row["consolation_round"] == "Consolation Semifinal"
    assert delta_row["is_playoffs"] is False
    assert delta_row["is_consolation"] is True
