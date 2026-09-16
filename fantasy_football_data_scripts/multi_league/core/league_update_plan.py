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
    observed = _parse_manifest(row.get("observed_manifest_json"), label="observed")
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
        "SELECT DISTINCT TRY_CAST(year AS INTEGER) AS year, TRY_CAST(week AS INTEGER) AS week "
        "FROM public.player_fantasy "
        f"WHERE db_name = '{safe_db}' AND TRY_CAST(year AS INTEGER) IN ({season_sql}) "
        "AND TRY_CAST(week AS INTEGER) > 0",
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
