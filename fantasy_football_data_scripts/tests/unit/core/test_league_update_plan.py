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
    active_provider_league_id,
    build_refresh_plan,
    load_persisted_refresh_plan,
    provider_renewal_chain,
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


def test_missing_derived_aggregate_forces_latest_materialized_week_refresh():
    nfl = (resource("nfl", "game", "2026:1:A@B", "n1"),)
    current = manifest(nfl=nfl)

    plan = build_refresh_plan(
        current,
        current,
        materialized_keys={(2026, 1)},
        required_reasons={"missing_derived_aggregate"},
    )

    assert plan.requires_refresh is True
    assert plan.weeks == (1,)
    assert plan.reasons == ("missing_derived_aggregate",)


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
    def __init__(self, manifest_row, weeks=(1, 2, 3), missing_derived_years=()):
        self.manifest_row = manifest_row
        self.weeks = weeks
        self.missing_derived_years = missing_derived_years

    def query(self, sql, *, database):
        if database == "___ops":
            return [self.manifest_row] if self.manifest_row else []
        assert database == "___leagues"
        if "missing_derived_years" in sql:
            value = ",".join(str(year) for year in self.missing_derived_years) or None
            return [{"missing_derived_years": value}]
        return [{"year": 2026, "week": week} for week in self.weeks]


def test_persisted_plan_treats_missing_derived_aggregate_as_refresh_work():
    current = manifest(nfl=(resource("nfl", "game", "2026:1:A@B", "one"),))
    row = {
        "observed_manifest_json": canonical_manifest_json(current),
        "observed_manifest_digest": manifest_digest(current),
        "published_manifest_json": canonical_manifest_json(current),
        "published_manifest_digest": manifest_digest(current),
    }
    plan = load_persisted_refresh_plan(
        Reader(row, weeks=(1,), missing_derived_years=(2024, 2025)),
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=manifest_digest(current),
    )

    assert plan is not None
    assert plan.weeks == (1,)
    assert plan.reasons == ("missing_derived_aggregate",)


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
        conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, "
                     "position VARCHAR, fantasy_points DOUBLE, season_ppg DOUBLE, alltime_ppg DOUBLE)")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("CREATE TABLE public.schedule (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("INSERT INTO public.player_fantasy (db_name, year, week) VALUES "
                     "('league_a', 2024, 3), ('league_a', 2025, 1), ('league_a', 2025, 17), "
                     "('league_a', 2026, 1), ('other', 2025, 99)")

        class ScopedReader:
            def query(self, sql, *, database):
                if database == "___ops":
                    return [row]
                if "missing_derived_years" in sql:
                    return [{"missing_derived_years": None}]
                result = conn.execute(sql)
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, values)) for values in result.fetchall()]

        plan = load_persisted_refresh_plan(
            ScopedReader(), database_name="league_a", active_season=2026,
            expected_observed_digest=manifest_digest(observed),
        )
    assert plan.weeks_by_season == ((2025, (1, 17)),)


def test_persisted_plan_replays_active_week_when_only_player_rows_exist():
    import duckdb

    current = manifest(
        nfl=(resource("nfl", "game", "2026:1:A@B", "one"),),
    )

    row = {
        "observed_manifest_json": canonical_manifest_json(current),
        "observed_manifest_digest": manifest_digest(current),
        "published_manifest_json": canonical_manifest_json(current),
        "published_manifest_digest": manifest_digest(current),
    }
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, "
                     "position VARCHAR, fantasy_points DOUBLE, season_ppg DOUBLE, alltime_ppg DOUBLE)")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("CREATE TABLE public.schedule (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("INSERT INTO public.player_fantasy (db_name, year, week) VALUES ('league_a', 2026, 1)")

        class ScopedReader:
            def query(self, sql, *, database):
                if database == "___ops":
                    return [row]
                if "missing_derived_years" in sql:
                    return [{"missing_derived_years": None}]
                result = conn.execute(sql)
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, values)) for values in result.fetchall()]

        plan = load_persisted_refresh_plan(
            ScopedReader(),
            database_name="league_a",
            active_season=2026,
            expected_observed_digest=manifest_digest(current),
        )

    assert plan.weeks == (1,)
    assert plan.reasons == ("missing_materialized_week",)


def test_active_provider_identity_comes_from_the_verified_native_chain():
    current = manifest()
    assert active_provider_league_id(current, provider="sleeper") == "s26"


def test_provider_renewal_chain_returns_the_complete_verified_segment():
    current = manifest()
    assert provider_renewal_chain(current, provider="sleeper") == {
        "2025": "s25",
        "2026": "s26",
    }


@pytest.mark.parametrize(
    "segment",
    [
        LeagueSegment("sleeper", "wrong", (2025, 2026), ((2025, "s25"), (2026, "s26"))),
        LeagueSegment("sleeper", "s26", (2025,), ((2025, "s25"),)),
    ],
)
def test_active_provider_identity_rejects_an_inconsistent_chain(segment):
    with pytest.raises(PersistedManifestError, match="active.*identity|active season"):
        active_provider_league_id(manifest(segments=(segment,)), provider="sleeper")


def test_provider_renewal_chain_rejects_a_missing_season_mapping():
    segment = LeagueSegment(
        "sleeper",
        "s26",
        (2024, 2025, 2026),
        ((2024, "s24"), (2026, "s26")),
    )
    with pytest.raises(PersistedManifestError, match="renewal chain.*seasons"):
        provider_renewal_chain(manifest(segments=(segment,)), provider="sleeper")


def test_provider_renewal_chain_rejects_reused_yahoo_identity_across_seasons():
    segment = LeagueSegment(
        "yahoo",
        "470.l.10",
        (2025, 2026),
        ((2025, "470.l.10"), (2026, "470.l.10")),
    )
    with pytest.raises(PersistedManifestError, match="reused an identity"):
        provider_renewal_chain(manifest(segments=(segment,)), provider="yahoo")


def test_persisted_plan_replays_active_week_when_nonzero_offense_is_missing_ppg():
    import duckdb

    current = manifest(nfl=(resource("nfl", "game", "2026:1:A@B", "one"),))
    row = {
        "observed_manifest_json": canonical_manifest_json(current),
        "observed_manifest_digest": manifest_digest(current),
        "published_manifest_json": canonical_manifest_json(current),
        "published_manifest_digest": manifest_digest(current),
    }
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, "
                     "position VARCHAR, fantasy_points DOUBLE, season_ppg DOUBLE, alltime_ppg DOUBLE)")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("CREATE TABLE public.schedule (db_name VARCHAR, year INTEGER, week INTEGER)")
        conn.execute("INSERT INTO public.player_fantasy VALUES "
                     "('league_a', 2026, 1, 'WR', 16.4, NULL, NULL)")
        conn.execute("INSERT INTO public.matchup VALUES ('league_a', 2026, 1)")
        conn.execute("INSERT INTO public.schedule VALUES ('league_a', 2026, 1)")

        class ScopedReader:
            def query(self, sql, *, database):
                if database == "___ops":
                    return [row]
                if "missing_derived_years" in sql:
                    return [{"missing_derived_years": None}]
                result = conn.execute(sql)
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, values)) for values in result.fetchall()]

        plan = load_persisted_refresh_plan(
            ScopedReader(),
            database_name="league_a",
            active_season=2026,
            expected_observed_digest=manifest_digest(current),
        )

    assert plan.weeks == (1,)
    assert plan.reasons == ("missing_materialized_week",)


def test_persisted_plan_materialization_check_is_one_grouped_bounded_scan():
    current = manifest(nfl=(resource("nfl", "game", "2026:1:A@B", "one"),))
    row = {
        "observed_manifest_json": canonical_manifest_json(current),
        "observed_manifest_digest": manifest_digest(current),
        "published_manifest_json": canonical_manifest_json(current),
        "published_manifest_digest": manifest_digest(current),
    }

    class CapturingReader(Reader):
        league_sql: list[str] = []

        def query(self, sql, *, database):
            if database == "___ops":
                return [row]
            self.league_sql.append(sql)
            if "missing_derived_years" in sql:
                return [{"missing_derived_years": None}]
            return [{"year": 2026, "week": 1}]

    reader = CapturingReader(row)
    load_persisted_refresh_plan(
        reader,
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=manifest_digest(current),
    )

    assert len(reader.league_sql) == 2
    materialization_sql, aggregate_gap_sql = reader.league_sql
    assert "WITH player_weeks AS MATERIALIZED" in materialization_sql
    assert "matchup_weeks AS MATERIALIZED" in materialization_sql
    assert "schedule_weeks AS MATERIALIZED" in materialization_sql
    assert "GROUP BY" in materialization_sql
    assert "EXISTS (SELECT" not in materialization_sql
    assert "player_fantasy_season" in aggregate_gap_sql
    assert "player_fantasy_season_all" in aggregate_gap_sql
    assert "standings_by_year" in aggregate_gap_sql
    assert "BIT_XOR(HASH(key_value))" in aggregate_gap_sql
    assert "EXCEPT" not in aggregate_gap_sql


def test_changed_source_plan_defers_historical_aggregate_audit_to_atomic_publish():
    old = manifest(provider=(resource("yahoo", "matchups", "2026:1", "old"),))
    observed = replace(
        old,
        provider_revisions=(resource("yahoo", "matchups", "2026:1", "new"),),
    )
    row = {
        "observed_manifest_json": canonical_manifest_json(observed),
        "observed_manifest_digest": manifest_digest(observed),
        "published_manifest_json": canonical_manifest_json(old),
        "published_manifest_digest": manifest_digest(old),
    }

    class CapturingReader(Reader):
        league_sql: list[str] = []

        def query(self, sql, *, database):
            if database == "___ops":
                return [row]
            self.league_sql.append(sql)
            assert "missing_derived_years" not in sql
            return [{"year": 2026, "week": 1}]

    reader = CapturingReader(row)
    plan = load_persisted_refresh_plan(
        reader,
        database_name="league_a",
        active_season=2026,
        expected_observed_digest=manifest_digest(observed),
    )

    assert plan is not None
    assert plan.requires_refresh
    assert len(reader.league_sql) == 1


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
