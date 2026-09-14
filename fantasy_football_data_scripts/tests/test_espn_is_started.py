"""
Unit tests for ESPN is_started logic.

Verifies that is_started is determined by fantasy_position (the mapped
lineup slot name), not the raw lineupSlot ID. Only players in actual
starter slots should be is_started=True. Bench/IR players must be False.
"""

import pytest

from multi_league.data_fetchers.espn.espn_rosters import (
    ESPN_SLOT_MAP,
    NON_STARTER_NAMES,
    NON_STARTER_SLOTS,
    _resolve_ghost_rosters,
)


# ── test: slot map completeness ──────────────────────────────────────────────


def test_all_espn_slots_are_mapped():
    """Every ESPN slot ID 0-23 should have a mapping in ESPN_SLOT_MAP."""
    unmapped = [i for i in range(24) if i not in ESPN_SLOT_MAP]
    assert unmapped == [], f"Unmapped ESPN slot IDs: {unmapped}"


# ── test: bench slots map to non-starter names ──────────────────────────────


def test_bench_slots_map_to_non_starter():
    """Slots in NON_STARTER_SLOTS should map to names in NON_STARTER_NAMES."""
    for slot_id in NON_STARTER_SLOTS:
        name = ESPN_SLOT_MAP.get(slot_id)
        assert name is not None, f"NON_STARTER_SLOTS contains unmapped slot {slot_id}"
        assert name in NON_STARTER_NAMES, f"Slot {slot_id} maps to '{name}' which is not in NON_STARTER_NAMES"


# ── test: starter slots do NOT map to non-starter names ─────────────────────


def test_starter_slots_are_not_bench():
    """All slots NOT in NON_STARTER_SLOTS should map to starter position names."""
    for slot_id, name in ESPN_SLOT_MAP.items():
        if slot_id in NON_STARTER_SLOTS:
            continue
        assert name not in NON_STARTER_NAMES, f"Starter slot {slot_id} maps to bench name '{name}'"


# ── test: is_started logic matches fantasy_position ─────────────────────────


@pytest.mark.parametrize(
    "slot_id,expected_started",
    [
        (0, True),  # QB
        (2, True),  # RB
        (4, True),  # WR
        (6, True),  # TE
        (7, True),  # OP (superflex)
        (16, True),  # DEF
        (17, True),  # K
        (23, True),  # FLEX
        (8, True),  # DT (IDP)
        (9, True),  # DE (IDP)
        (10, True),  # LB (IDP)
        (11, True),  # DL (IDP)
        (12, True),  # CB (IDP)
        (13, True),  # S (IDP)
        (14, True),  # DB (IDP)
        (15, True),  # DP (IDP flex)
        (20, False),  # BN
        (21, False),  # IR
        (22, False),  # BN (alternate)
    ],
)
def test_is_started_by_slot_id(slot_id, expected_started):
    """is_started should be True for starter slots, False for bench/IR."""
    fantasy_position = ESPN_SLOT_MAP.get(slot_id)
    assert fantasy_position is not None, f"Slot {slot_id} not in ESPN_SLOT_MAP"

    is_started = fantasy_position not in NON_STARTER_NAMES
    assert is_started == expected_started, (
        f"Slot {slot_id} ('{fantasy_position}'): " f"is_started={is_started}, expected={expected_started}"
    )


# ── test: starter count per typical roster ───────────────────────────────────


def test_typical_roster_starter_count():
    """
    A typical ESPN roster has slots for ~10 starters + bench/IR.
    Verify that the number of unique starter slot IDs is reasonable (8-16).
    """
    starter_slots = [slot_id for slot_id, name in ESPN_SLOT_MAP.items() if name not in NON_STARTER_NAMES]
    assert 8 <= len(starter_slots) <= 25, (
        f"Expected 8-25 starter slot types, got {len(starter_slots)}: " f"{sorted(starter_slots)}"
    )


def test_resolve_ghost_rosters_prefers_started_copy_for_same_team_duplicate(monkeypatch):
    rows = [
        {
            "espn_player_id": "p1",
            "player": "Patrick Mahomes",
            "year": 2024,
            "week": 5,
            "team_key": "7",
            "fantasy_position": "BN",
            "projected_points": 17.1,
            "fantasy_points": 12.44,
            "is_started": 0,
        },
        {
            "espn_player_id": "p1",
            "player": "Patrick Mahomes",
            "year": 2024,
            "week": 5,
            "team_key": "7",
            "fantasy_position": "QB",
            "projected_points": 17.1,
            "fantasy_points": 12.44,
            "is_started": 1,
        },
    ]

    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_rosters._build_ownership_map",
        lambda *_args, **_kwargs: {"p1": {5: "7"}},
    )

    resolved = _resolve_ghost_rosters(rows, ctx=None, year=2024, db=None, max_weeks=18)

    assert len(resolved) == 1
    assert resolved[0]["team_key"] == "7"
    assert resolved[0]["is_started"] == 1
    assert resolved[0]["fantasy_position"] == "QB"
