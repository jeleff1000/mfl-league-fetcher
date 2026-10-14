from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

from multi_league.core.league_update_manifest import LeagueSegment, ResourceRevision, SourceManifest
from multi_league.core.league_update_plan import PersistedRefreshPlan, RefreshPlan


def captured(provider="yahoo", **revision_overrides):
    draft = ResourceRevision(provider, "draft", "2026", "draft-v1", observed_count=2)
    source = SourceManifest(1, "league_a", 2026,
        (LeagueSegment(provider, "active", (2026,), ((2026, "active"),)),), (), (draft,), 4)
    observed = replace(source, provider_revisions=(replace(draft, **revision_overrides),))
    return PersistedRefreshPlan(RefreshPlan("league_a", 2026, (6,), (), (), False), observed, source, "observed", "published")


def local(frame=None):
    if frame is None:
        frame = pd.DataFrame({"db_name": ["league_a"]*2, "league_id": ["active"]*2, "year": [2026]*2,
                              "pick": [1, 2], "provider_player_id": [101, 102]})
    return SimpleNamespace(league_name="league_a", table_exists=lambda name: name == "draft",
                           read_table=lambda name, year: frame.copy())


def retained(db=None, plan=None, provider="yahoo"):
    from multi_league.core.league_refresh import retained_draft_from_captured_revision
    return retained_draft_from_captured_revision(
        db or local(), plan=plan, platform=provider, year=2026, league_id="active",
        key_columns=("pick", "provider_player_id"),
    )


@pytest.mark.parametrize("provider", ["yahoo", "sleeper", "espn"])
def test_unchanged_successfully_published_draft_can_reuse_local_identity_witness(provider):
    result = retained(plan=captured(provider), provider=provider)
    assert result.pick.tolist() == [1, 2]


@pytest.mark.parametrize("change", [{"revision": "new"}, {"status": "incomplete"},
                                   {"observed_count": 3}, {"scope": "2025"}, {"revision": ""}])
def test_new_incomplete_or_different_scope_draft_requires_provider_validation(change):
    assert retained(plan=captured(**change)) is None


def test_manual_run_without_captured_draft_keeps_existing_provider_validation():
    assert retained() is None


@pytest.mark.parametrize("damage", ["empty", "duplicate", "blank", "other_year", "other_league"])
def test_retained_revision_does_not_hide_invalid_hydrated_picks(damage):
    frame = local().read_table("draft", 2026)
    if damage == "empty": frame = frame.iloc[:0]
    if damage == "duplicate": frame.iloc[1] = frame.iloc[0]
    if damage == "blank": frame.loc[0, "provider_player_id"] = None
    if damage == "other_year": frame["year"] = 2025
    if damage == "other_league": frame["db_name"] = "other"
    assert retained(db=local(frame), plan=captured()) is None


def test_retained_revision_cannot_cross_a_renewal_chain_change():
    plan = captured()
    observed = replace(plan.observed_manifest, segments=(LeagueSegment("yahoo", "new", (2026,), ((2026, "new"),)),))
    assert retained(plan=replace(plan, observed_manifest=observed)) is None


def test_espn_without_captured_count_cannot_admit_a_partial_retained_draft():
    plan = captured("espn", observed_count=None)
    plan = replace(plan, published_manifest=plan.observed_manifest)
    assert retained(db=local(local().read_table("draft", 2026).iloc[:1]), plan=plan, provider="espn") is None


def test_retained_draft_must_belong_to_the_same_native_league():
    frame = local().read_table("draft", 2026)
    frame["league_id"] = "other"
    assert retained(db=local(frame), plan=captured()) is None
