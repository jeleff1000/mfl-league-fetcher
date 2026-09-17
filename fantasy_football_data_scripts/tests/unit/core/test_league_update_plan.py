from __future__ import annotations

from dataclasses import replace

import pytest

from multi_league.core.league_update_manifest import (
    LeagueSegment,
    ResourceRevision,
    SourceManifest,
)
from multi_league.core.league_update_manifest import canonical_manifest_json, manifest_digest
from multi_league.core.league_update_plan import (
    PersistedManifestError,
    build_refresh_plan,
    load_persisted_refresh_plan,
)


def resource(provider, kind, scope, revision):
    return ResourceRevision(provider, kind, scope, revision)


def manifest(*, nfl=(), provider=(), segments=None):
    return SourceManifest(
        schema_version=1,
        database_name="league_a",
        active_season=2026,
        segments=segments or (
            LeagueSegment("sleeper", "s26", (2025, 2026), ((2025, "s25"), (2026, "s26"))),
        ),
        nfl_revisions=tuple(nfl),
        provider_revisions=tuple(provider),
        base_generation=4,
    )


@pytest.mark.parametrize(
    ("changed", "expected_weeks", "expected_resources"),
    [
        (resource("nfl", "game", "2026:4:BUF@MIA", "final"), (4,), ("game",)),
        (resource("nfl", "game", "2026:4:BUF@MIA", "corrected"), (4,), ("game",)),
        (resource("nfl", "game", "2026:2:BUF@MIA", "corrected"), (2, 4), ("game",)),
        (resource("sleeper", "settings", "2026", "new"), (1, 2, 3, 4), ("settings",)),
        (resource("sleeper", "transactions", "2026:2", "new"), (2, 4), ("transactions",)),
        (resource("sleeper", "draft", "2026", "new"), (4,), ("draft",)),
        (resource("sleeper", "rosters", "2026:3", "new"), (3, 4), ("rosters",)),
    ],
)
def test_manifest_diff_selects_every_changed_scope_without_latest_week_lower_bound(
    changed,
    expected_weeks,
    expected_resources,
):
    base_nfl = tuple(resource("nfl", "game", f"2026:{week}:A@B", f"n{week}") for week in range(1, 5))
    old_provider = (resource("sleeper", "settings", "2026", "old"),)
    old = manifest(nfl=base_nfl, provider=old_provider)
    if changed.provider == "nfl":
        observed_nfl = tuple(row for row in base_nfl if (row.resource, row.scope) != (changed.resource, changed.scope)) + (changed,)
        observed = manifest(nfl=observed_nfl, provider=old_provider)
    else:
        observed_provider = tuple(row for row in old_provider if (row.resource, row.scope) != (changed.resource, changed.scope)) + (changed,)
        observed = manifest(nfl=base_nfl, provider=observed_provider)

    plan = build_refresh_plan(observed, old, materialized_keys={(2026, 1), (2026, 2), (2026, 3), (2026, 4)})

    assert plan.weeks == expected_weeks
    assert tuple(sorted({row.resource for row in plan.changed_resources})) == expected_resources
    assert plan.requires_refresh is True


def test_missing_materialized_week_is_selected_even_without_a_revision_change():
    nfl = tuple(resource("nfl", "game", f"2026:{week}:A@B", f"n{week}") for week in range(1, 5))
    current = manifest(nfl=nfl)

    plan = build_refresh_plan(
        current,
        current,
        materialized_keys={(2026, 1), (2026, 3), (2026, 4)},
    )

    assert plan.weeks == (2, 4)
    assert plan.reasons == ("missing_materialized_week",)


def test_identical_complete_manifest_is_a_no_op():
    nfl = (resource("nfl", "game", "2026:1:A@B", "n1"),)
    current = manifest(nfl=nfl)
    plan = build_refresh_plan(current, current, materialized_keys={(2026, 1)})
    assert plan.requires_refresh is False
    assert plan.weeks == ()
    assert plan.changed_resources == ()


def test_prior_season_correction_retains_its_year_and_week():
    old = manifest(
        nfl=(resource("nfl", "game", "2026:1:A@B", "same"),),
        provider=(resource("sleeper", "matchups", "2025:17", "old"),),
    )
    observed = replace(old, provider_revisions=(
        resource("sleeper", "matchups", "2025:17", "corrected"),
    ))
    plan = build_refresh_plan(observed, old, {(2025, 17), (2026, 1)})
    assert dict(plan.weeks_by_season)[2025] == (17,)
    assert set(dict(plan.weeks_by_season)) == {2025, 2026}


def test_provider_only_change_remains_a_partition_without_played_weeks():
    old = manifest(provider=(resource("sleeper", "draft", "2026", "old"),))
    observed = replace(old, provider_revisions=(
        resource("sleeper", "draft", "2026", "corrected"),
    ))
    plan = build_refresh_plan(observed, old, set())
    assert plan.requires_refresh is True
    assert plan.weeks_by_season == ((2026, ()),)


def test_historical_settings_change_selects_only_that_seasons_materialized_weeks():
    old = manifest(provider=(resource("sleeper", "settings", "2025", "old"),))
    observed = replace(old, provider_revisions=(
        resource("sleeper", "settings", "2025", "corrected"),
    ))
    plan = build_refresh_plan(observed, old, iter(((2024, 1), (2025, 1), (2025, 17))))
    assert plan.weeks_by_season == ((2025, (1, 17)),)


def test_renewal_segment_change_refreshes_the_active_season():
    nfl = tuple(resource("nfl", "game", f"2026:{week}:A@B", f"n{week}") for week in (1, 2, 3))
    old = manifest(nfl=nfl)
    changed_segment = (
        LeagueSegment("sleeper", "s26-renewed", (2026,), ((2026, "s26-renewed"),)),
    )
    observed = replace(old, segments=changed_segment)
    plan = build_refresh_plan(observed, old, materialized_keys={(2026, 1), (2026, 2), (2026, 3)})
    assert plan.weeks == (1, 2, 3)
    assert "segment_identity_changed" in plan.reasons


def test_removed_resource_is_still_a_refresh_and_never_silently_ignored():
    nfl = (resource("nfl", "game", "2026:1:A@B", "n1"),)
    published = manifest(nfl=nfl, provider=(resource("sleeper", "transactions", "2026:1", "tx"),))
    observed = manifest(nfl=nfl)
    plan = build_refresh_plan(observed, published, materialized_keys={(2026, 1)})
    assert plan.requires_refresh is True
    assert plan.weeks == (1,)
    assert plan.changed_resources[0].status == "removed"


def test_different_database_manifests_are_rejected():
    current = manifest()
    with pytest.raises(ValueError, match="different leagues"):
        build_refresh_plan(replace(current, database_name="other"), current, materialized_keys=set())


class Reader:
    def __init__(self, manifest_row, weeks=(1, 2, 3)):
        self.manifest_row = manifest_row
        self.weeks = weeks

    def query(self, sql, *, database):
        if database == "___ops":
            return [self.manifest_row] if self.manifest_row else []
        assert database == "___leagues"
        return [{"year": 2026, "week": week} for week in self.weeks]


def test_persisted_plan_verifies_the_exact_ui_observation_and_selects_changed_week():
    old = manifest(
        nfl=(
            resource("nfl", "game", "2026:1:A@B", "one"),
            resource("nfl", "game", "2026:2:A@B", "old"),
            resource("nfl", "game", "2026:3:A@B", "three"),
        ),
        provider=(resource("sleeper", "rosters", "2026:2", "old"),),
    )
    observed = replace(
        old,
        nfl_revisions=(
            resource("nfl", "game", "2026:1:A@B", "one"),
            resource("nfl", "game", "2026:2:A@B", "corrected"),
            resource("nfl", "game", "2026:3:A@B", "three"),
        ),
    )
    reader = Reader({
        "observed_manifest_json": canonical_manifest_json(observed),
        "observed_manifest_digest": manifest_digest(observed),
        "published_manifest_json": canonical_manifest_json(old),
        "published_manifest_digest": manifest_digest(old),
    })

    plan = load_persisted_refresh_plan(
        reader,
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=manifest_digest(observed),
    )

    assert plan is not None
    assert plan.weeks == (2, 3)
    assert plan.observed_manifest_digest == manifest_digest(observed)
    assert plan.observed_manifest_json == canonical_manifest_json(observed)


def test_persisted_plan_rejects_a_stale_ui_digest_before_fetching_provider_data():
    observed = manifest()
    reader = Reader({
        "observed_manifest_json": canonical_manifest_json(observed),
        "observed_manifest_digest": manifest_digest(observed),
        "published_manifest_json": None,
        "published_manifest_digest": None,
    })
    with pytest.raises(PersistedManifestError, match="changed after dispatch"):
        load_persisted_refresh_plan(
            reader,
            database_name="league_a",
            active_season=2026,
            expected_observed_digest="stale",
        )


def test_persisted_plan_reads_historical_week_keys_without_other_leagues():
    import duckdb

    old = manifest(provider=(resource("sleeper", "settings", "2025", "old"),))
    observed = replace(old, provider_revisions=(
        resource("sleeper", "settings", "2025", "corrected"),
    ))
    row = {
        "observed_manifest_json": canonical_manifest_json(observed),
        "observed_manifest_digest": manifest_digest(observed),
        "published_manifest_json": canonical_manifest_json(old),
        "published_manifest_digest": manifest_digest(old),
    }
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("INSERT INTO public.player_fantasy VALUES "
                     "('league_a', 2024, 3), ('league_a', 2025, 1), ('league_a', 2025, 17), "
                     "('league_a', 2026, 1), ('other', 2025, 99)")

        class ScopedReader:
            def query(self, sql, *, database):
                if database == "___ops":
                    return [row]
                result = conn.execute(sql)
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, values)) for values in result.fetchall()]

        plan = load_persisted_refresh_plan(
            ScopedReader(), database_name="league_a", active_season=2026,
            expected_observed_digest=manifest_digest(observed),
        )
    assert plan.weeks_by_season == ((2025, (1, 17)),)


def test_manual_run_without_a_persisted_probe_can_use_the_legacy_boundary():
    assert load_persisted_refresh_plan(
        Reader(None),
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=None,
    ) is None


def test_manual_run_with_a_blank_legacy_manifest_can_use_the_legacy_boundary():
    reader = Reader({
        "observed_manifest_json": "",
        "observed_manifest_digest": None,
        "published_manifest_json": None,
        "published_manifest_digest": None,
    })

    assert load_persisted_refresh_plan(
        reader,
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=None,
    ) is None


def test_ui_run_rejects_a_blank_legacy_manifest():
    reader = Reader({
        "observed_manifest_json": "",
        "observed_manifest_digest": None,
        "published_manifest_json": None,
        "published_manifest_digest": None,
    })

    with pytest.raises(PersistedManifestError, match="unavailable"):
        load_persisted_refresh_plan(
            reader,
            database_name="league_a",
            active_season=2026,
            expected_observed_digest="claimed-by-ui",
        )
