import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.sleeper.sleeper_roster_identity import _merge_entry


def test_merge_entry_replaces_unknown_placeholder_with_current_api_identity():
    existing = {
        "manager_name": "Unknown",
        "manager_guid": "orp00008",
        "team_name": "Unknown",
    }
    incoming = {
        "manager_name": "Team 8",
        "manager_guid": "orp000800",
        "team_name": "Team 8",
    }

    assert _merge_entry(existing, incoming) == incoming


def test_merge_entry_replaces_stale_guid_when_roster_labels_match():
    existing = {
        "manager_name": "Team 8",
        "manager_guid": "orp00008",
        "team_name": "Team 8",
    }
    incoming = {
        "manager_name": "Team 8",
        "manager_guid": "orp000800",
        "team_name": "Team 8",
    }

    assert _merge_entry(existing, incoming) == incoming


def test_merge_entry_keeps_existing_identity_when_conflicting_labels_disagree():
    existing = {
        "manager_name": "Team 8",
        "manager_guid": "orp000800",
        "team_name": "Team 8",
    }
    incoming = {
        "manager_name": "JLiv10",
        "manager_guid": "862769080858488832",
        "team_name": "The Chubba Missile Crisis",
    }

    assert _merge_entry(existing, incoming) == existing


def test_merge_entry_fills_missing_team_name_when_guid_matches():
    existing = {
        "manager_name": "JLiv10",
        "manager_guid": "862769080858488832",
        "team_name": "",
    }
    incoming = {
        "manager_name": "JLiv10",
        "manager_guid": "862769080858488832",
        "team_name": "The Chubba Missile Crisis",
    }

    assert _merge_entry(existing, incoming) == {
        "manager_name": "JLiv10",
        "manager_guid": "862769080858488832",
        "team_name": "The Chubba Missile Crisis",
    }
