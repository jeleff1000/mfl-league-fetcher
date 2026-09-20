"""Canonical freshness manifests for active-season league updates.

The source digest deliberately excludes observation timestamps and the Fly
publication generation.  Those values belong to the captured snapshot and
publication fence, but neither is provider/NFL source content.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from typing import Any


MANIFEST_SCHEMA_VERSION = 1

NFL_CANONICAL_IDENTITY_COLUMNS = (
    "NFL_player_id",
    "game_date",
    "week",
    "season_type",
    "nfl_team",
    "opponent_nfl_team",
)

# Inputs that may alter a league scoring result.  Keeping this registry beside
# the revision builder gives the ops materializer and its coverage test one
# source of truth.
NFL_SCORING_INPUT_COLUMNS = (
    "attempts",
    "carries",
    "completions",
    "completions_40plus",
    "def_blk_kick",
    "def_int_ret_td",
    "def_interception_yards",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "passing_2pt_conversions",
    "rushing_yards",
    "rushing_tds",
    "rushing_2pt_conversions",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_2pt_conversions",
    "fumbles_lost",
    "rushing_fumbles",
    "receiving_fumbles",
    "sack_fumbles",
    "rushing_fumbles_lost",
    "receiving_fumbles_lost",
    "sack_fumbles_lost",
    "special_teams_tds",
    "kickoff_return_yards",
    "punt_return_yards",
    "fg_made",
    "fg_att",
    "fg_missed",
    "fg_missed_0_19",
    "fg_missed_20_29",
    "fg_missed_30_39",
    "fg_missed_40_49",
    "fg_missed_50_59",
    "fg_missed_60_",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60_",
    "fg_made_60_plus_canonical",
    "fg_made_distance",
    "fg_yards",
    "fg_yards_canonical",
    "fg_yards_over_30_canonical",
    "fg_yds_over_30",
    "fg_pct",
    "pat_made",
    "pat_att",
    "pat_missed",
    "def_sacks",
    "def_interceptions",
    "def_fumbles_forced",
    "fum_rec",
    "def_tackles_solo",
    "def_tackles_with_assist",
    "def_tackles_for_loss",
    "def_qb_hits",
    "def_pass_defended",
    "def_safeties",
    "def_tds",
    "three_out",
    "fourth_down_stop",
    "fg_blocked",
    "fum_ret_td",
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "passing_first_downs",
    "rushing_first_downs",
    "receiving_first_downs",
    "passing_tds_40plus",
    "passing_tds_50plus",
    "rushing_40plus",
    "rushing_tds_40plus",
    "rushing_tds_50plus",
    "receptions_40plus",
    "receiving_tds_40plus",
    "receiving_tds_50plus",
    "pick6",
    "sacks_suffered",
    "dst_points_allowed",
    "points_allowed",
    "total_yds_allowed",
    "yds_allow_0_99",
    "yds_allow_100_199",
    "yds_allow_200_299",
    "yds_allow_300_349",
    "yds_allow_350_399",
    "yds_allow_400_449",
    "yds_allow_450_499",
    "yds_allow_500_549",
    "yds_allow_550_plus",
    "fantasy_points_ppr",
)

# Rank corrections affect imported enrichments even when scoring stats do not change.
NFL_PRECOMPUTED_SCORING_PREFIXES = ("pts_", "bonus_", "rank_")


def resolve_nfl_scoring_input_columns(schema_columns: Iterable[str]) -> tuple[str, ...]:
    """Resolve an explicit bounded SELECT list from the current source schema."""
    available = {str(column) for column in schema_columns}
    selected = {
        column
        for column in available
        if column in NFL_SCORING_INPUT_COLUMNS
        or column.startswith(NFL_PRECOMPUTED_SCORING_PREFIXES)
    }
    return tuple(sorted(selected))


@dataclass(frozen=True, slots=True)
class LeagueSegment:
    provider: str
    active_league_id: str
    seasons: tuple[int, ...]
    renewal_chain: tuple[tuple[int, str], ...]


@dataclass(frozen=True, slots=True)
class ResourceRevision:
    provider: str
    resource: str
    scope: str
    revision: str
    status: str = "ok"
    expected_count: int | None = None
    observed_count: int | None = None


@dataclass(frozen=True, slots=True)
class SourceManifest:
    schema_version: int
    database_name: str
    active_season: int
    segments: tuple[LeagueSegment, ...]
    nfl_revisions: tuple[ResourceRevision, ...]
    provider_revisions: tuple[ResourceRevision, ...]
    base_generation: int
    observed_at: str | None = None


@dataclass(frozen=True, slots=True)
class ManifestDelta:
    identity_changed: bool
    segments_changed: bool
    nfl_added: tuple[ResourceRevision, ...]
    nfl_changed: tuple[ResourceRevision, ...]
    nfl_removed: tuple[ResourceRevision, ...]
    provider_added: tuple[ResourceRevision, ...]
    provider_changed: tuple[ResourceRevision, ...]
    provider_removed: tuple[ResourceRevision, ...]

    @property
    def is_empty(self) -> bool:
        return not (
            self.identity_changed
            or self.segments_changed
            or self.nfl_added
            or self.nfl_changed
            or self.nfl_removed
            or self.provider_added
            or self.provider_changed
            or self.provider_removed
        )


def _resource_from_mapping(value: Mapping[str, Any]) -> ResourceRevision:
    return ResourceRevision(
        provider=str(value["provider"]),
        resource=str(value["resource"]),
        scope=str(value["scope"]),
        revision=str(value["revision"]),
        status=str(value.get("status") or "ok"),
        expected_count=(
            None if value.get("expected_count") is None else int(value["expected_count"])
        ),
        observed_count=(
            None if value.get("observed_count") is None else int(value["observed_count"])
        ),
    )


def source_manifest_from_mapping(value: Mapping[str, Any]) -> SourceManifest:
    """Parse a persisted manifest without trusting source ordering or scalar types."""
    try:
        segments = tuple(
            LeagueSegment(
                provider=str(segment["provider"]),
                active_league_id=str(segment["active_league_id"]),
                seasons=tuple(int(year) for year in segment.get("seasons", ())),
                renewal_chain=tuple(
                    (int(year), str(league_id))
                    for year, league_id in segment.get("renewal_chain", ())
                ),
            )
            for segment in value.get("segments", ())
        )
        manifest = SourceManifest(
            schema_version=int(value["schema_version"]),
            database_name=str(value["database_name"]),
            active_season=int(value["active_season"]),
            segments=segments,
            nfl_revisions=tuple(
                _resource_from_mapping(resource)
                for resource in value.get("nfl_revisions", ())
            ),
            provider_revisions=tuple(
                _resource_from_mapping(resource)
                for resource in value.get("provider_revisions", ())
            ),
            base_generation=int(value.get("base_generation") or 0),
            observed_at=(
                None if value.get("observed_at") is None else str(value["observed_at"])
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid league update source manifest") from exc
    canonical_manifest_payload(manifest)
    return manifest


def _scope_sort_key(scope: str) -> tuple[tuple[int, int | str], ...]:
    """Sort numeric scope components numerically without changing their text."""
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part)
        for part in str(scope).split(":")
    )


def _resource_sort_key(resource: ResourceRevision) -> tuple[Any, ...]:
    return resource.provider, resource.resource, _scope_sort_key(resource.scope)


def _resource_payload(resource: ResourceRevision) -> dict[str, Any]:
    return {
        "expected_count": resource.expected_count,
        "observed_count": resource.observed_count,
        "provider": resource.provider,
        "resource": resource.resource,
        "revision": resource.revision,
        "scope": resource.scope,
        "status": resource.status,
    }


def _validate_resources(resources: Iterable[ResourceRevision]) -> None:
    seen: set[tuple[str, str, str]] = set()
    for resource in resources:
        key = (resource.provider, resource.resource, resource.scope)
        if key in seen:
            raise ValueError(f"duplicate resource revision: {key!r}")
        seen.add(key)


def canonical_manifest_payload(manifest: SourceManifest) -> dict[str, Any]:
    """Return only source-content fields in deterministic structured order."""
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported league update manifest schema: {manifest.schema_version}")
    if manifest.active_season < 2000:
        raise ValueError(f"invalid active season: {manifest.active_season}")

    all_resources = (*manifest.nfl_revisions, *manifest.provider_revisions)
    _validate_resources(all_resources)
    segments = sorted(manifest.segments, key=lambda value: (value.provider, value.active_league_id))
    return {
        "active_season": int(manifest.active_season),
        "database_name": manifest.database_name,
        "nfl_revisions": [
            _resource_payload(value) for value in sorted(manifest.nfl_revisions, key=_resource_sort_key)
        ],
        "provider_revisions": [
            _resource_payload(value) for value in sorted(manifest.provider_revisions, key=_resource_sort_key)
        ],
        "schema_version": int(manifest.schema_version),
        "segments": [
            {
                "active_league_id": segment.active_league_id,
                "provider": segment.provider,
                "renewal_chain": [
                    [int(year), league_id]
                    for year, league_id in sorted(segment.renewal_chain, key=lambda item: (int(item[0]), item[1]))
                ],
                "seasons": sorted(int(year) for year in segment.seasons),
            }
            for segment in segments
        ],
    }


def canonical_manifest_json(manifest: SourceManifest) -> str:
    return json.dumps(
        canonical_manifest_payload(manifest),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def manifest_digest(manifest: SourceManifest) -> str:
    return hashlib.sha256(canonical_manifest_json(manifest).encode("utf-8")).hexdigest()


def _diff_resource_revisions(
    observed: tuple[ResourceRevision, ...],
    published: tuple[ResourceRevision, ...],
) -> tuple[
    tuple[ResourceRevision, ...],
    tuple[ResourceRevision, ...],
    tuple[ResourceRevision, ...],
]:
    def keyed(resources: tuple[ResourceRevision, ...]) -> dict[tuple[str, str, str], ResourceRevision]:
        return {
            (resource.provider, resource.resource, resource.scope): resource
            for resource in resources
        }

    observed_by_key = keyed(observed)
    published_by_key = keyed(published)
    observed_keys = set(observed_by_key)
    published_keys = set(published_by_key)
    added = tuple(observed_by_key[key] for key in sorted(observed_keys - published_keys))
    changed = tuple(
        observed_by_key[key]
        for key in sorted(observed_keys & published_keys)
        if observed_by_key[key] != published_by_key[key]
    )
    removed = tuple(published_by_key[key] for key in sorted(published_keys - observed_keys))
    return added, changed, removed


def diff_manifests(observed: SourceManifest, published: SourceManifest) -> ManifestDelta:
    """Compare source content while ignoring snapshot and publication metadata."""
    canonical_manifest_payload(observed)
    canonical_manifest_payload(published)
    if observed.database_name != published.database_name:
        raise ValueError("cannot compare source manifests for different leagues")

    nfl_added, nfl_changed, nfl_removed = _diff_resource_revisions(
        observed.nfl_revisions,
        published.nfl_revisions,
    )
    provider_added, provider_changed, provider_removed = _diff_resource_revisions(
        observed.provider_revisions,
        published.provider_revisions,
    )
    observed_segments = canonical_manifest_payload(observed)["segments"]
    published_segments = canonical_manifest_payload(published)["segments"]
    return ManifestDelta(
        identity_changed=observed.active_season != published.active_season,
        segments_changed=observed_segments != published_segments,
        nfl_added=nfl_added,
        nfl_changed=nfl_changed,
        nfl_removed=nfl_removed,
        provider_added=provider_added,
        provider_changed=provider_changed,
        provider_removed=provider_removed,
    )


def _canonical_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        if value == 0:
            return 0
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        return _canonical_scalar(value.item())
    return str(value)


def nfl_game_revision(
    rows: Iterable[Mapping[str, Any]],
    *,
    scoring_columns: Iterable[str] = NFL_SCORING_INPUT_COLUMNS,
) -> str:
    """Hash canonical identity and registered scoring inputs for one game scope."""
    columns = (*NFL_CANONICAL_IDENTITY_COLUMNS, *tuple(scoring_columns))
    canonical_rows = [
        {column: _canonical_scalar(row.get(column)) for column in columns}
        for row in rows
    ]
    canonical_rows.sort(
        key=lambda row: tuple(str(row.get(column) or "") for column in NFL_CANONICAL_IDENTITY_COLUMNS)
    )
    payload = json.dumps(canonical_rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_NFL_TEAM_ALIASES = {
    "GNB": "GB",
    "KAN": "KC",
    "LAR": "LA",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
}


def _canonical_team(value: Any) -> str:
    team = str(value or "").strip().upper()
    return _NFL_TEAM_ALIASES.get(team, team)


def build_nfl_revision_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    schema_version: int = MANIFEST_SCHEMA_VERSION,
    scoring_columns: Iterable[str] = NFL_SCORING_INPUT_COLUMNS,
) -> list[dict[str, Any]]:
    """Group bounded NFL player-game facts into compact game revisions."""
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported league update manifest schema: {schema_version}")

    grouped: dict[tuple[int, int, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        try:
            season = int(row["year"])
            week = int(row["week"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("NFL revision row requires integer year and week") from exc
        left = _canonical_team(row.get("nfl_team"))
        right = _canonical_team(row.get("opponent_nfl_team"))
        if not row.get("NFL_player_id") or not left or not right:
            raise ValueError("NFL revision row requires player and team identity")
        game_key = "@".join(sorted((left, right)))
        grouped.setdefault((season, week, game_key), []).append(row)

    return [
        {
            "season": season,
            "week": week,
            "game_key": game_key,
            "revision": nfl_game_revision(group_rows, scoring_columns=scoring_columns),
            "schema_version": schema_version,
        }
        for (season, week, game_key), group_rows in sorted(grouped.items())
    ]
