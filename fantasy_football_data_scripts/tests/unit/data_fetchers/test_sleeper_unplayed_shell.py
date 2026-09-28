import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import sleeper_initial_import


class _DraftedUnplayedSleeperClient:
    def get_league(self, league_id):
        return {
            "league_id": str(league_id),
            "name": "Late Start League",
            "season": "2026",
            "sport": "nfl",
            "status": "in_season",
            "settings": {"num_teams": 10, "start_week": 4},
        }

    def get_nfl_state(self):
        return {"season": "2026", "week": 3, "season_type": "regular"}

    def get_league_users(self, _league_id):
        return [
            {"user_id": f"user-{index}", "display_name": f"Manager {index}"}
            for index in range(1, 11)
        ]

    def get_league_rosters(self, _league_id):
        return [
            {
                "roster_id": index,
                "owner_id": f"user-{index}",
                "players": [f"player-{index}"],
                "starters": [f"player-{index}"],
                "settings": {
                    "wins": 0,
                    "losses": 0,
                    "ties": 0,
                    "fpts": 0,
                    "fpts_decimal": 0,
                },
            }
            for index in range(1, 11)
        ]

    def get_league_drafts(self, _league_id):
        return [
            {
                "draft_id": "draft-2026",
                "season": "2026",
                "status": "complete",
                "settings": {"teams": 10, "rounds": 16},
            }
        ]

    def get_draft_picks(self, _draft_id):
        return [
            {
                "pick_no": pick_no,
                "round": ((pick_no - 1) // 10) + 1,
                "draft_slot": ((pick_no - 1) % 10) + 1,
                "roster_id": ((pick_no - 1) % 10) + 1,
                "player_id": f"player-{pick_no}",
            }
            for pick_no in range(1, 161)
        ]

    def get_league_matchups(self, _league_id, _week):
        return []


class _LocalShellDb:
    counts = {
        "league_settings": 1,
        "draft": 160,
        "matchup": 0,
        "player_fantasy": 0,
    }

    def table_exists(self, table_name):
        return table_name in self.counts

    def row_count(self, table_name):
        return self.counts[table_name]


def _quick_context():
    return SimpleNamespace(
        import_mode="quick",
        league_id="league-2026",
        league_ids={"2026": "league-2026"},
        get_league_id_for_year=lambda year: "league-2026" if year == 2026 else None,
    )


def test_drafted_midseason_sleeper_league_is_verified_as_unplayed():
    verify = getattr(sleeper_initial_import, "_sleeper_has_verified_unplayed_shell", None)

    assert verify is not None
    assert verify(_DraftedUnplayedSleeperClient(), year=2026, league_id="league-2026") is True


def test_malformed_sleeper_shell_is_rejected():
    class MissingRostersClient(_DraftedUnplayedSleeperClient):
        def get_league_rosters(self, _league_id):
            return []

    verify = getattr(sleeper_initial_import, "_sleeper_has_verified_unplayed_shell", None)

    assert verify is not None
    assert verify(MissingRostersClient(), year=2026, league_id="league-2026") is False


def test_sleeper_shell_api_error_fails_closed():
    class FailedLeagueClient(_DraftedUnplayedSleeperClient):
        def get_league(self, _league_id):
            raise ConnectionError("provider timeout")

    assert (
        sleeper_initial_import._sleeper_has_verified_unplayed_shell(
            FailedLeagueClient(), year=2026, league_id="league-2026"
        )
        is False
    )


def test_scored_sleeper_shell_is_not_treated_as_unplayed():
    class ScoredClient(_DraftedUnplayedSleeperClient):
        def get_league(self, league_id):
            league = super().get_league(league_id)
            league["settings"]["start_week"] = 1
            return league

        def get_league_matchups(self, _league_id, week):
            if week != 1:
                return []
            return [
                {
                    "roster_id": roster_id,
                    "matchup_id": ((roster_id - 1) // 2) + 1,
                    "custom_points": None,
                    "points": 90.0 + roster_id,
                    "players": [f"player-{roster_id}"],
                }
                for roster_id in range(1, 11)
            ]

    verify = getattr(sleeper_initial_import, "_sleeper_has_verified_unplayed_shell", None)

    assert verify is not None
    assert verify(ScoredClient(), year=2026, league_id="league-2026") is False


def test_quick_import_accepts_only_a_verified_local_empty_shell():
    validate = getattr(sleeper_initial_import, "_validate_sleeper_quick_source", None)

    assert validate is not None
    assert (
        validate(
            _quick_context(),
            _DraftedUnplayedSleeperClient(),
            _LocalShellDb(),
            [2026],
        )
        is True
    )


def test_quick_import_rejects_unverified_empty_provider_response():
    class MissingRostersClient(_DraftedUnplayedSleeperClient):
        def get_league_rosters(self, _league_id):
            return []

    validate = getattr(sleeper_initial_import, "_validate_sleeper_quick_source", None)

    assert validate is not None
    with pytest.raises(RuntimeError, match="could not verify an unplayed Sleeper league"):
        validate(_quick_context(), MissingRostersClient(), _LocalShellDb(), [2026])


def test_quick_import_does_not_let_roster_rows_mask_missing_matchups():
    class RosterOnlyDb(_LocalShellDb):
        counts = {**_LocalShellDb.counts, "player_fantasy": 160}

    class MissingRostersClient(_DraftedUnplayedSleeperClient):
        def get_league_rosters(self, _league_id):
            return []

    with pytest.raises(RuntimeError, match="could not verify an unplayed Sleeper league"):
        sleeper_initial_import._validate_sleeper_quick_source(
            _quick_context(), MissingRostersClient(), RosterOnlyDb(), [2026]
        )
