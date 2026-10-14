"""Future redaction must not turn a complete played season into a failed refresh."""
import pandas as pd
import pytest

from multi_league.core.league_refresh import RefreshScopeError, resolve_active_schedule_franchise_ids


class LocalDB:
    league_name = "league_a"

    def table_exists(self, name):
        return name == "matchup"

    def read_table(self, name, year=None):
        return pd.DataFrame({
            "year": [2026, 2026], "week": [1, 1], "team_key": ["1", "2"],
            "manager_guid": ["owner", "owner"], "manager": ["Preferred A", "Preferred B"],
            "team_name": ["Team A", "Team B"], "franchise_id": ["saved-a", "saved-b"],
        })


def redacted(week=14):
    return pd.DataFrame({
        "year": [2026, 2026], "week": [week, week], "platform": ["yahoo", "yahoo"],
        "manager": ["David", "David"], "opponent": ["David", "David"],
        "team_key": ["", ""], "manager_guid": ["--hidden--", "--hidden--"],
    })


def test_redacted_future_pairing_uses_full_native_inventory_and_saved_aliases():
    result = resolve_active_schedule_franchise_ids(
        LocalDB(), redacted(), active_year=2026, future_weeks=(14,), expected_team_keys=("1", "2"),
    )
    assert result.franchise_id.tolist() == ["saved-a", "saved-b"]
    assert result.manager.tolist() == ["Preferred A", "Preferred B"]
    assert result.team_key.tolist() == ["1", "2"]
    assert result.opponent.eq("TBD").all()
    assert result.opponent_franchise_id.isna().all()
    assert result[["win", "loss", "team_points", "opponent_points"]].isna().all().all()


def test_missing_sleeper_future_week_keeps_all_real_teams_without_guessing_pairs():
    frame = redacted().iloc[:0].copy()
    frame["platform"] = "sleeper"
    result = resolve_active_schedule_franchise_ids(
        LocalDB(), frame, active_year=2026, future_weeks=(13, 14), expected_team_keys=("1", "2"),
    )
    assert result.groupby("week").franchise_id.nunique().to_dict() == {13: 2, 14: 2}
    assert result.opponent.eq("TBD").all()


def test_redacted_completed_week_remains_a_hard_failure():
    with pytest.raises(RefreshScopeError, match="unresolved canonical"):
        resolve_active_schedule_franchise_ids(
            LocalDB(), redacted(1), active_year=2026, future_weeks=(14,), expected_team_keys=("1", "2"),
        )


@pytest.mark.parametrize("keys", [("1",), ("1", "3"), ("1", "1")])
def test_future_padding_requires_exact_unique_native_inventory(keys):
    with pytest.raises(RefreshScopeError, match="inventory"):
        resolve_active_schedule_franchise_ids(
            LocalDB(), redacted(), active_year=2026, future_weeks=(14,), expected_team_keys=keys,
        )


def test_unknown_explicit_provider_team_is_not_treated_as_redaction():
    frame = redacted()
    frame["team_key"] = ["1", "alien"]
    with pytest.raises(RefreshScopeError, match="inventory"):
        resolve_active_schedule_franchise_ids(
            LocalDB(), frame, active_year=2026, future_weeks=(14,), expected_team_keys=("1", "2"),
        )


def test_known_future_pairings_are_preserved():
    frame = redacted()
    frame["team_key"] = ["1", "2"]
    frame["opponent_team_key"] = ["2", "1"]
    result = resolve_active_schedule_franchise_ids(
        LocalDB(), frame, active_year=2026, future_weeks=(14,), expected_team_keys=("1", "2"),
    )
    assert result.opponent_franchise_id.tolist() == ["saved-b", "saved-a"]


def test_explicit_alien_opponent_key_cannot_be_overturned_by_display_name():
    frame = redacted()
    frame["team_key"] = ["1", "2"]
    frame["manager"] = ["Preferred A", "Preferred B"]
    frame["opponent"] = ["Preferred B", "Preferred A"]
    frame["opponent_team_key"] = ["alien", "1"]
    with pytest.raises(RefreshScopeError, match="inventory"):
        resolve_active_schedule_franchise_ids(
            LocalDB(), frame, active_year=2026, future_weeks=(14,), expected_team_keys=("1", "2"),
        )


def test_first_season_missing_pairings_use_complete_provider_profiles():
    db = LocalDB()
    db.table_exists = lambda name: False
    profiles = LocalDB().read_table("matchup", 2026)
    actual = resolve_active_schedule_franchise_ids(
        db, redacted().iloc[:0], active_year=2026, future_weeks=(14,),
        expected_team_keys=("1", "2"), provider_identities=profiles,
        allow_provider_guid_initialization=True,
    )
    assert actual.franchise_id.tolist() == ["saved-a", "saved-b"]
    assert actual.opponent.eq("TBD").all()


def test_departed_owner_does_not_override_complete_canonical_team_inventory():
    profiles = LocalDB().read_table("matchup", 2026)
    profiles["manager_guid"] = ""
    actual = resolve_active_schedule_franchise_ids(
        LocalDB(), redacted(), active_year=2026, future_weeks=(14,),
        expected_team_keys=("1", "2"), provider_identities=profiles,
        allow_provider_guid_initialization=True,
    )
    assert actual.franchise_id.tolist() == ["saved-a", "saved-b"]


@pytest.mark.parametrize("provider", ["yahoo", "sleeper"])
def test_explicit_unknown_future_pairing_can_be_rebound_by_shared_pipeline(provider):
    frame = pd.DataFrame({
        "year": [2026, 2026], "week": [14, 14], "platform": [provider, provider],
        "team_key": ["1", "2"], "opponent": ["TBD", "TBD"],
        "opponent_franchise_id": [None, None],
    })
    result = resolve_active_schedule_franchise_ids(LocalDB(), frame, active_year=2026)
    assert result.franchise_id.tolist() == ["saved-a", "saved-b"]
    assert result.opponent_franchise_id.isna().all()
