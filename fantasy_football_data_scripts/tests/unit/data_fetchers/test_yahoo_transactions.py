"""Regression tests for Yahoo transaction parsing edge cases."""

import pandas as pd
import pytest
from types import SimpleNamespace

from multi_league.data_fetchers.yahoo.yahoo_transactions import (
    _resolve_transaction_team_keys,
    backfill_unknown_drop_managers,
    process_transactions_chronologically,
    load_matchup_windows,
    map_transaction_to_week,
)


def test_matchup_windows_prefer_dated_rows_over_null_duplicates(tmp_path):
    class LocalDB:
        def read_table(self, table, year):
            return pd.DataFrame([
                {'year': 2024, 'week': 6, 'week_start': None, 'week_end': None},
                {'year': 2024, 'week': 6, 'week_start': '2024-10-08', 'week_end': '2024-10-14'},
            ])

    windows = load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path), 2024, LocalDB())
    assert map_transaction_to_week('1728520236', 2024, windows) == (6, '2024-10-08', '2024-10-14')


def test_matchup_windows_do_not_hide_conflicting_dates(tmp_path):
    class LocalDB:
        def read_table(self, table, year):
            return pd.DataFrame([
                {"year":2024,"week":6,"week_start":"2024-10-08","week_end":"2024-10-14"},
                {"year":2024,"week":6,"week_start":"2024-10-09","week_end":"2024-10-15"},
            ])
    windows = load_matchup_windows(SimpleNamespace(matchup_data_directory=tmp_path), 2024, LocalDB())
    with pytest.raises(ValueError, match="transaction week"):
        map_transaction_to_week('1728520236', 2024, windows)


@pytest.mark.parametrize('windows,stamp', [
    ([], '1728520236'),
    ([{'year':2024,'week':6,'week_start':None,'week_end':None}], '1728520236'),
    ([{'year':2024,'week':6,'week_start':'2024-10-08','week_end':'2024-10-14'}], 'invalid'),
    ([{'year':2024,'week':5,'week_start':'2024-10-01','week_end':'2024-10-07'},
      {'year':2024,'week':7,'week_start':'2024-10-15','week_end':'2024-10-21'}], '1728520236'),
    ([{'year':2024,'week':6,'week_start':'2024-10-08','week_end':'2024-10-14'},
      {'year':2024,'week':7,'week_start':'2024-10-08','week_end':'2024-10-14'}], '1728520236'),
])
def test_yahoo_unverified_transaction_week_is_not_fabricated(windows, stamp):
    frame = pd.DataFrame(windows, columns=['year','week','week_start','week_end'])
    with pytest.raises(ValueError, match='transaction week'):
        map_transaction_to_week(stamp, 2024, frame)


@pytest.mark.parametrize("stamp,week", [(1728345599, 5), (1728345600, 6), (1728345601, 6)])
def test_yahoo_week_changes_at_utc_midnight(stamp, week):
    windows = pd.DataFrame([
        {"year":2024, "week":5, "week_start":"2024-10-01", "week_end":"2024-10-07"},
        {"year":2024, "week":6, "week_start":"2024-10-08", "week_end":"2024-10-14"},
    ])
    assert map_transaction_to_week(str(stamp), 2024, windows)[0] == week


def test_resolve_transaction_team_keys_falls_back_to_parent_drop_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="drop",
        destination="waivers",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "461.l.90939.t.7",
            "destination_team_key": "",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.7"
    assert source_key == "461.l.90939.t.7"
    assert destination_key == ""


def test_resolve_transaction_team_keys_falls_back_to_parent_add_team():
    manager_key, source_key, destination_key = _resolve_transaction_team_keys(
        transaction_type="add",
        destination="team",
        player_source_key="",
        player_destination_key="",
        transaction_team_keys={
            "source_team_key": "",
            "destination_team_key": "461.l.90939.t.4",
            "waiver_team_key": "",
            "trader_team_key": "",
            "tradee_team_key": "",
        },
    )

    assert manager_key == "461.l.90939.t.4"
    assert source_key == ""
    assert destination_key == "461.l.90939.t.4"


def test_backfill_unknown_drop_managers_uses_last_known_owner():
    df = pd.DataFrame(
        {
            "transaction_id": ["tx-1", "tx-2"],
            "year": [2025, 2025],
            "week": [2, 4],
            "timestamp": [1_000, 2_000],
            "transaction_type": ["add", "drop"],
            "manager": ["Alice", "Unknown"],
            "manager_guid": ["guid-a", None],
            "team_name": ["Alpha", None],
            "yahoo_player_id": ["12345", "12345"],
            "player_key": ["461.p.12345", "461.p.12345"],
            "player": ["Player One", "Player One"],
            "destination": ["team", "waivers"],
        }
    )

    ordered = process_transactions_chronologically(df)
    repaired = backfill_unknown_drop_managers(ordered)

    repaired_drop = repaired.loc[repaired["transaction_id"] == "tx-2"].iloc[0]
    assert repaired_drop["manager"] == "Alice"
    assert repaired_drop["manager_guid"] == "guid-a"
    assert repaired_drop["team_name"] == "Alpha"
