"""Only a verified captured pair graph can bridge post-enrichment name changes."""
from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_refresh import RefreshScopeError, resolve_active_schedule_franchise_ids


class LocalDB:
    league_name = "league_a"

    def __init__(self, identities):
        self.identities = identities

    def table_exists(self, name):
        return name == "matchup"

    def read_table(self, name, year=None):
        return self.identities.copy()


def captured_schedule():
    from multi_league.data_fetchers.yahoo.yahoo_schedules import _derive_schedule_df_from_matchup_df

    matchup = pd.DataFrame({
        "year": [2026, 2026], "week": [1, 1], "team_key": ["1", "2"],
        "manager": ["Alex", "Blair"], "manager_guid": ["--hidden--", "--hidden--"],
        "opponent": ["Blair", "Alex"], "opponent_guid": ["--hidden--", "--hidden--"],
        "team_name": ["Team A", "Team B"], "franchise_id": ["old-a", "old-b"],
    })
    # The real Yahoo schedule projection applies saved aliases to the own
    # manager only. First resolution can still use the played manager names.
    schedule = _derive_schedule_df_from_matchup_df(
        matchup, 2026, {"Alex": "Preferred A", "Blair": "Preferred B"},
    )
    schedule = resolve_active_schedule_franchise_ids(LocalDB(matchup), schedule, active_year=2026)
    assert schedule.opponent_franchise_id.tolist() == ["old-b", "old-a"]
    schedule["db_name"] = "league_a"
    enriched = matchup.copy()
    enriched["manager"] = ["Preferred A", "Preferred B"]
    enriched["franchise_id"] = ["new-a", "new-b"]
    return schedule, LocalDB(enriched)


def test_captured_yahoo_pairings_survive_aliases_and_canonical_id_changes():
    schedule, db = captured_schedule()
    original = schedule.copy(deep=True)
    result = resolve_active_schedule_franchise_ids(
        db, schedule, active_year=2026, rebind_captured_opponents=True,
    )
    assert result.franchise_id.tolist() == ["new-a", "new-b"]
    assert result.opponent_franchise_id.tolist() == ["new-b", "new-a"]
    pd.testing.assert_frame_equal(schedule, original)


def test_captured_ids_are_not_trusted_by_default():
    schedule, db = captured_schedule()
    with pytest.raises(RefreshScopeError, match="teams=0, opponents=2"):
        resolve_active_schedule_franchise_ids(db, schedule, active_year=2026)


@pytest.mark.parametrize("damage", [
    "missing_reverse", "different_week", "different_year", "different_league",
    "duplicate_edge", "duplicate_own", "unknown_id", "split_old_id", "collapsed_new_id",
    "conflicting_native_opponent", "unknown_native_opponent", "missing_old_id", "missing_week", "self_edge",
])
def test_rebinding_rejects_ambiguous_or_unverified_pair_graphs(damage):
    schedule, db = captured_schedule()
    if damage == "missing_reverse":
        schedule = schedule.iloc[:1].copy()
    elif damage == "different_week":
        schedule.loc[1, "week"] = 2
    elif damage == "different_year":
        schedule.loc[1, "year"] = 2025
    elif damage == "different_league":
        schedule.loc[1, "db_name"] = "league_b"
    elif damage == "duplicate_edge":
        schedule = pd.concat([schedule, schedule.iloc[:1]], ignore_index=True)
    elif damage == "duplicate_own":
        duplicate = schedule.iloc[:1].copy()
        duplicate["opponent_franchise_id"] = "other"
        schedule = pd.concat([schedule, duplicate], ignore_index=True)
    elif damage == "unknown_id":
        schedule.loc[0, "opponent_franchise_id"] = "unmapped"
    elif damage == "split_old_id":
        other_week = schedule.iloc[:1].copy()
        other_week["week"] = 2
        other_week["team_key"] = "2"
        schedule = pd.concat([schedule, other_week], ignore_index=True)
    elif damage == "collapsed_new_id":
        db.identities["franchise_id"] = "merged"
    elif damage == "conflicting_native_opponent":
        schedule.loc[1, "opponent_team_key"] = "2"
    elif damage == "unknown_native_opponent":
        schedule.loc[1, "opponent_team_key"] = "unknown-native-team"
    elif damage == "missing_old_id":
        schedule.loc[1, "franchise_id"] = pd.NA
    elif damage == "missing_week":
        schedule.loc[1, "week"] = float("nan")
    elif damage == "self_edge":
        schedule.loc[0, "opponent_franchise_id"] = "old-a"
    with pytest.raises(RefreshScopeError, match="unresolved canonical schedule identities"):
        resolve_active_schedule_franchise_ids(
            db, schedule, active_year=2026, rebind_captured_opponents=True,
        )


def test_rebinding_does_not_conflate_two_distinct_games_after_a_merge():
    schedule, db = captured_schedule()
    extra = schedule.copy()
    extra["team_key"] = ["3", "4"]
    extra["franchise_id"] = ["old-c", "old-d"]
    extra["opponent_franchise_id"] = ["old-d", "old-c"]
    extra["manager"] = ["Preferred C", "Preferred D"]
    extra["opponent"] = ["Former D", "Former C"]
    schedule = pd.concat([schedule, extra], ignore_index=True)
    identities = db.identities.copy()
    identities["team_key"] = ["3", "4"]
    identities["manager"] = ["Preferred C", "Preferred D"]
    identities["franchise_id"] = ["new-a", "new-d"]
    db.identities = pd.concat([db.identities, identities], ignore_index=True)
    with pytest.raises(RefreshScopeError, match="unresolved canonical schedule identities"):
        resolve_active_schedule_franchise_ids(
            db, schedule, active_year=2026, rebind_captured_opponents=True,
        )


def test_future_tbd_slots_remain_unknown_after_rebinding():
    schedule, db = captured_schedule()
    schedule["week"] = 14
    schedule["opponent"] = "TBD"
    schedule["opponent_franchise_id"] = None
    result = resolve_active_schedule_franchise_ids(
        db, schedule, active_year=2026, rebind_captured_opponents=True,
    )
    assert result.franchise_id.tolist() == ["new-a", "new-b"]
    assert result.opponent_franchise_id.isna().all()


def test_rebinding_preserves_arbitrary_dataframe_indices():
    schedule, db = captured_schedule()
    schedule.index = [42, 42]
    result = resolve_active_schedule_franchise_ids(
        db, schedule, active_year=2026, rebind_captured_opponents=True,
    )
    assert result.opponent_franchise_id.tolist() == ["new-b", "new-a"]
    assert result.index.tolist() == [42, 42]
