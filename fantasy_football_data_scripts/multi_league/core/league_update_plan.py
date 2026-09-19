"""Deterministic manifest-diff planning for active-season league updates."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
import json

from multi_league.core.league_update_manifest import (
    ManifestDelta,
    ResourceRevision,
    SourceManifest,
    diff_manifests,
    canonical_manifest_json,
    manifest_digest,
    source_manifest_from_mapping,
)


@dataclass(frozen=True, slots=True)
class RefreshPlan:
    database_name: str
    active_season: int
    weeks: tuple[int, ...]
    changed_resources: tuple[ResourceRevision, ...]
    reasons: tuple[str, ...]
    rebuild_full_active_season: bool
    # Empty weeks still represent work (for example a corrected draft).
    # The legacy `weeks` field addresses only `active_season`.
    weeks_by_season: tuple[tuple[int, tuple[int, ...]], ...] = ()

    @property
    def requires_refresh(self) -> bool:
        return bool(self.weeks or self.changed_resources or self.reasons)


@dataclass(frozen=True, slots=True)
class PersistedRefreshPlan:
    plan: RefreshPlan
    observed_manifest: SourceManifest
    published_manifest: SourceManifest
    observed_manifest_digest: str
    published_manifest_digest: str | None

    @property
    def observed_manifest_json(self) -> str:
        return canonical_manifest_json(self.observed_manifest)

    def __getattr__(self, name: str):
        return getattr(self.plan, name)


class PersistedManifestError(RuntimeError):
    """Persisted source manifests cannot prove the dispatched snapshot."""


def active_provider_league_id(
    value: SourceManifest | PersistedRefreshPlan | None,
    *,
    provider: str,
) -> str | None:
    """Return the active ID only when the captured provider chain proves it."""
    if value is None:
        return None
    manifest = value.observed_manifest if isinstance(value, PersistedRefreshPlan) else value
    expected = str(provider).strip().lower()
    segments = [segment for segment in manifest.segments if segment.provider.strip().lower() == expected]
    if len(segments) != 1:
        raise PersistedManifestError(
            f"captured manifest must contain exactly one {expected} active segment"
        )
    segment = segments[0]
    active_year = int(manifest.active_season)
    if active_year not in {int(year) for year in segment.seasons}:
        raise PersistedManifestError("captured provider chain omitted the active season")
    active_ids = {
        str(league_id)
        for year, league_id in segment.renewal_chain
        if int(year) == active_year and str(league_id).strip()
    }
    if active_ids != {str(segment.active_league_id)}:
        raise PersistedManifestError("captured provider chain has an inconsistent active identity")
    return str(segment.active_league_id)


def active_publication_covers_plan(
    plan: RefreshPlan | PersistedRefreshPlan | None,
    *,
    year: int | None,
    weeks: Iterable[int],
) -> bool:
    """Never acknowledge untouched chain partitions as source-current.

    Provider completeness (draft, games and outcomes) is checked by the caller.
    This only compares the captured plan with the active partition it fetched.
    """
    if plan is None or year != plan.active_season or not plan.weeks_by_season:
        return False
    covered_weeks = set(weeks)
    return all(
        season == year and set(required_weeks).issubset(covered_weeks)
        for season, required_weeks in plan.weeks_by_season
    )


def _scope_year_week(scope: str) -> tuple[int | None, int | None]:
    parts = str(scope).split(":")
    try:
        year = int(parts[0])
    except (IndexError, TypeError, ValueError):
        return None, None
    try:
        week = int(parts[1])
    except (IndexError, TypeError, ValueError):
        week = None
    return year, week


def _materialized_weeks(values: Iterable[object], *, season: int) -> set[int]:
    weeks: set[int] = set()
    for value in values:
        raw_year: object = season
        raw_week: object = value
        if isinstance(value, tuple) and len(value) >= 2:
            raw_year, raw_week = value[0], value[1]
        try:
            year = int(raw_year)
            week = int(raw_week)
        except (TypeError, ValueError):
            continue
        if year == season and week > 0:
            weeks.add(week)
    return weeks


def _observed_nfl_weeks(manifest: SourceManifest) -> set[int]:
    weeks: set[int] = set()
    for row in manifest.nfl_revisions:
        year, week = _scope_year_week(row.scope)
        if year == manifest.active_season and week and week > 0:
            weeks.add(week)
    return weeks


def _delta_resources(delta: ManifestDelta) -> tuple[ResourceRevision, ...]:
    present = (
        *delta.nfl_added,
        *delta.nfl_changed,
        *delta.provider_added,
        *delta.provider_changed,
    )
    removed = tuple(
        replace(row, status="removed")
        for row in (*delta.nfl_removed, *delta.provider_removed)
    )
    return tuple(
        sorted(
            (*present, *removed),
            key=lambda row: (row.provider, row.resource, row.scope, row.status),
        )
    )


def build_refresh_plan(
    observed: SourceManifest,
    published: SourceManifest,
    materialized_keys: Iterable[object],
) -> RefreshPlan:
    """Retain changed chain partitions without a newest-week lower bound."""
    delta = diff_manifests(observed, published)
    season = observed.active_season
    resources = _delta_resources(delta)
    materialized_keys = tuple(materialized_keys)
    materialized = _materialized_weeks(materialized_keys, season=season)
    observed_weeks = _observed_nfl_weeks(observed)
    weeks: set[int] = set()
    reasons: set[str] = set()

    full_active = delta.identity_changed or delta.segments_changed
    if full_active:
        reasons.add("segment_identity_changed")
        weeks.update(observed_weeks)

    for row in resources:
        year, week = _scope_year_week(row.scope)
        if year != season:
            continue
        if week and week > 0:
            weeks.add(week)
        if row.resource == "settings":
            full_active = True
            reasons.add("settings_changed")
            weeks.update(observed_weeks | materialized)

    missing_weeks = observed_weeks - materialized
    if missing_weeks:
        reasons.add("missing_materialized_week")
        weeks.update(missing_weeks)

    requires_refresh = bool(resources or reasons or weeks)
    # Retain the latest materialized scope as an overlap witness, but never use
    # it as a lower bound that discards an older correction.
    if requires_refresh and materialized:
        overlap = max(materialized)
        if overlap in observed_weeks:
            weeks.add(overlap)

    partitions: dict[int, set[int]] = {}
    if weeks or full_active:
        partitions[season] = set(weeks)
    for row in resources:
        year, week = _scope_year_week(row.scope)
        if year is None:
            raise PersistedManifestError(f"changed resource has no season: {row.scope!r}")
        selected = partitions.setdefault(year, set())
        if week is not None and week > 0:
            selected.add(week)
        if row.resource == "settings":
            selected.update(_materialized_weeks(materialized_keys, season=year))
            for nfl_row in observed.nfl_revisions:
                nfl_year, nfl_week = _scope_year_week(nfl_row.scope)
                if nfl_year == year and nfl_week is not None and nfl_week > 0:
                    selected.add(nfl_week)

    return RefreshPlan(
        database_name=observed.database_name,
        active_season=season,
        weeks=tuple(sorted(weeks)),
        changed_resources=resources,
        reasons=tuple(sorted(reasons)),
        rebuild_full_active_season=full_active,
        weeks_by_season=tuple(
            (year, tuple(sorted(selected))) for year, selected in sorted(partitions.items())
        ),
    )


def _parse_manifest(raw: object, *, label: str) -> SourceManifest:
    try:
        value = json.loads(str(raw))
    except (TypeError, ValueError) as exc:
        raise PersistedManifestError(f"{label} manifest JSON is invalid") from exc
    if not isinstance(value, dict):
        raise PersistedManifestError(f"{label} manifest JSON is invalid")
    try:
        return source_manifest_from_mapping(value)
    except ValueError as exc:
        raise PersistedManifestError(f"{label} manifest is invalid") from exc


def load_persisted_refresh_plan(
    reader,
    *,
    database_name: str,
    active_season: int,
    expected_observed_digest: str | None,
) -> PersistedRefreshPlan | None:
    """Load the exact UI-observed snapshot and compare it to publication.

    A manual run may have no probe yet and receives ``None`` so its caller can
    materialize a probe through the same supported path. A UI run supplies the
    digest it claimed; absence or drift is a stale-writer error.
    """
    safe_db = str(database_name).replace("'", "''")
    try:
        rows = reader.query(
            "SELECT observed_manifest_json, observed_manifest_digest, "
            "published_manifest_json, published_manifest_digest "
            "FROM accounts.league_update_manifests "
            f"WHERE database_name = '{safe_db}' LIMIT 1",
            database="___ops",
        )
    except Exception as exc:
        if expected_observed_digest:
            raise PersistedManifestError("dispatched source manifest is unavailable") from exc
        return None
    if not rows:
        if expected_observed_digest:
            raise PersistedManifestError("dispatched source manifest is unavailable")
        return None

    row = rows[0]
    raw_observed = row.get("observed_manifest_json")
    if not str(raw_observed or "").strip():
        if expected_observed_digest:
            raise PersistedManifestError("dispatched source manifest is unavailable")
        return None
    observed = _parse_manifest(raw_observed, label="observed")
    observed_digest = manifest_digest(observed)
    stored_observed_digest = str(row.get("observed_manifest_digest") or "").strip()
    if not stored_observed_digest or stored_observed_digest != observed_digest:
        raise PersistedManifestError("observed manifest digest does not match its payload")
    if expected_observed_digest and expected_observed_digest != observed_digest:
        raise PersistedManifestError("source manifest changed after dispatch")
    if observed.database_name != database_name or observed.active_season != int(active_season):
        raise PersistedManifestError("observed manifest identity does not match the requested league season")

    raw_published = row.get("published_manifest_json")
    stored_published_digest = str(row.get("published_manifest_digest") or "").strip() or None
    if raw_published:
        published = _parse_manifest(raw_published, label="published")
        published_digest = manifest_digest(published)
        if stored_published_digest != published_digest:
            raise PersistedManifestError("published manifest digest does not match its payload")
    else:
        # First manifest-aware update: every observed resource is new, while
        # the stable league segment avoids inventing an identity migration.
        published = replace(
            observed,
            nfl_revisions=(),
            provider_revisions=(),
            base_generation=0,
            observed_at=None,
        )
        published_digest = None

    affected_seasons = {int(active_season)}
    for revision in _delta_resources(diff_manifests(observed, published)):
        year, _ = _scope_year_week(revision.scope)
        if year is not None:
            affected_seasons.add(year)
    season_sql = ", ".join(str(year) for year in sorted(affected_seasons))
    materialized_rows = reader.query(
        "WITH player_weeks AS MATERIALIZED ("
        "SELECT TRY_CAST(year AS INTEGER) AS year, TRY_CAST(week AS INTEGER) AS week, "
        "MAX(CASE WHEN UPPER(TRIM(COALESCE(position, ''))) "
        "IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF') "
        "AND ABS(COALESCE(fantasy_points, 0)) > 0 "
        "AND (season_ppg IS NULL OR alltime_ppg IS NULL) THEN 1 ELSE 0 END) AS has_incomplete "
        "FROM public.player_fantasy "
        f"WHERE db_name = '{safe_db}' AND TRY_CAST(year AS INTEGER) IN ({season_sql}) "
        "AND TRY_CAST(week AS INTEGER) > 0 GROUP BY 1, 2), "
        "matchup_weeks AS MATERIALIZED ("
        "SELECT TRY_CAST(year AS INTEGER) AS year, TRY_CAST(week AS INTEGER) AS week, 1 AS present "
        "FROM public.matchup "
        f"WHERE db_name = '{safe_db}' AND TRY_CAST(year AS INTEGER) = {int(active_season)} "
        "AND TRY_CAST(week AS INTEGER) > 0 GROUP BY 1, 2), "
        "schedule_weeks AS MATERIALIZED ("
        "SELECT TRY_CAST(year AS INTEGER) AS year, TRY_CAST(week AS INTEGER) AS week, 1 AS present "
        "FROM public.schedule "
        f"WHERE db_name = '{safe_db}' AND TRY_CAST(year AS INTEGER) = {int(active_season)} "
        "AND TRY_CAST(week AS INTEGER) > 0 GROUP BY 1, 2) "
        "SELECT p.year, p.week FROM player_weeks p "
        "LEFT JOIN matchup_weeks m USING (year, week) "
        "LEFT JOIN schedule_weeks s USING (year, week) "
        f"WHERE p.year <> {int(active_season)} OR "
        "(m.present = 1 AND s.present = 1 AND p.has_incomplete = 0)",
        database="___leagues",
    )
    materialized = {
        (int(item["year"]), int(item["week"]))
        for item in materialized_rows
        if item.get("year") is not None and item.get("week") is not None
    }
    plan = build_refresh_plan(observed, published, materialized)
    return PersistedRefreshPlan(
        plan=plan,
        observed_manifest=observed,
        published_manifest=published,
        observed_manifest_digest=observed_digest,
        published_manifest_digest=published_digest,
    )
