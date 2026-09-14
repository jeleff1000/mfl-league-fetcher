"""Shared league-name sync: adopt the platform's name without moving the database.

Yahoo and ESPN both seeded the import context with a signup-time name and never
refreshed it, so wrong names stayed wrong (ESPN: 25.6% of leagues showed their
own slug) and correct names went stale as leagues were renamed between seasons.

The dangerous half is the ordering. `fetch_runtime._resolve_db_name()` falls
back to sanitizing `league_name` when `database_name` is unset, so renaming an
unpinned context retargets the import -- and for a hash-suffixed database
(`any_given_sunday_40d008`) the sanitized real name collapses onto a DIFFERENT
league's database. `test_hash_suffixed_database_is_not_retargeted` is that case
and must never regress.
"""
import sys
from pathlib import Path

import pytest

SCRIPTS_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.league_name_sync import (  # noqa: E402
    adopt_api_league_name,
    adopt_from_yearly_settings,
    looks_like_slug,
)


class _Ctx:
    def __init__(self, league_name, database_name=None):
        self.league_name = league_name
        self.database_name = database_name
        self.league_id = "123"


# --- the hazard -------------------------------------------------------------

def test_hash_suffixed_database_is_not_retargeted():
    ctx = _Ctx("any_given_sunday_40d008", "any_given_sunday_40d008")
    changed = adopt_api_league_name(ctx, "Any Given Sunday")
    assert changed is True
    assert ctx.league_name == "Any Given Sunday"
    assert ctx.database_name == "any_given_sunday_40d008", "database followed the rename"


def test_rename_requires_a_pinned_database():
    """If the database cannot be pinned, the name must not change."""
    ctx = _Ctx("a_league", None)
    adopt_api_league_name(ctx, "A League")
    if ctx.league_name != "a_league":
        assert ctx.database_name, "renamed without pinning the database"


# --- adoption rules ---------------------------------------------------------

def test_adopts_a_real_name_over_a_slug():
    ctx = _Ctx("a_league_of_their_own", "a_league_of_their_own")
    assert adopt_api_league_name(ctx, "A League of Their Own") is True
    assert ctx.league_name == "A League of Their Own"


def test_ignores_a_slug_shaped_replacement():
    ctx = _Ctx("the_pigskin_platoon", "the_pigskin_platoon")
    assert adopt_api_league_name(ctx, "the_pigskin_platoon") is False


def test_never_overwrites_a_real_stored_name():
    """The stored name is canonical: Settings owns it, imports must not revert it.

    A league renamed on the platform, or renamed by the user in Settings, keeps
    the name it already has. This only ever fills a gap.
    """
    ctx = _Ctx("Our Chosen Name", "our_chosen_name")
    assert adopt_api_league_name(ctx, "Whatever ESPN Calls It Now") is False
    assert ctx.league_name == "Our Chosen Name"


def test_still_fills_a_missing_name():
    ctx = _Ctx("", "some_db")
    assert adopt_api_league_name(ctx, "A Real Name") is True
    assert ctx.league_name == "A Real Name"


def test_ignores_blank_and_unchanged():
    ctx = _Ctx("Real Name", "real_name")
    assert adopt_api_league_name(ctx, "   ") is False
    assert adopt_api_league_name(ctx, "Real Name") is False
    assert ctx.league_name == "Real Name"


def test_keeps_genuine_lowercase_names():
    """Yahoo and ESPN confirmed real leagues named "xii" and "skankasaurus"."""
    ctx = _Ctx("skankasaurus", "skankasaurus")
    assert adopt_api_league_name(ctx, "skankasaurus") is False
    assert ctx.league_name == "skankasaurus"


# --- per-year selection -----------------------------------------------------

def test_espn_shape_uses_newest_season():
    ctx = _Ctx("old_name", "old_name")
    adopt_from_yearly_settings(
        ctx,
        {2019: {"league_name": "Old Name"}, 2025: {"league_name": "New Name"}},
        key="league_name",
    )
    assert ctx.league_name == "New Name"


def test_yahoo_shape_reads_nested_metadata():
    ctx = _Ctx("u_can_t_handle_this", "u_can_t_handle_this_111c")
    adopt_from_yearly_settings(
        ctx,
        {2025: {"metadata": {"name": "U CaN'T HanDLe THiS Season 23"}}},
        key="name",
        nested="metadata",
    )
    assert ctx.league_name == "U CaN'T HanDLe THiS Season 23"
    assert ctx.database_name == "u_can_t_handle_this_111c"


def test_empty_settings_is_a_noop():
    ctx = _Ctx("x_league", "x_league")
    assert adopt_from_yearly_settings(ctx, {}, key="name") is False


@pytest.mark.parametrize(
    "value,expected",
    [("xii", True), ("a_league_of_their_own", True), ("", True), (None, True),
     ("The Dirty P", False), ("2024 Coo-Coo-Cooler League", False)],
)
def test_looks_like_slug(value, expected):
    assert looks_like_slug(value) is expected
