"""Small, deterministic provider probes for league-update freshness.

The probes intentionally return hashes and counts, never provider payloads or
credentials.  They use the same client methods as the import fetchers while
remaining narrow enough to run before a weekly update is dispatched.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
from typing import Any

from multi_league.core.league_update_manifest import ResourceRevision


@dataclass(frozen=True, slots=True)
class ProviderProbeResult:
    provider: str
    league_id: str
    active_season: int
    through_week: int
    revisions: tuple[ResourceRevision, ...]
    expected_teams: int
    observed_teams: int
    healthy: bool = True
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class MultiplatformProbeResult:
    segments: tuple[ProviderProbeResult, ...]
    revisions: tuple[ResourceRevision, ...]
    healthy: bool


class ProviderProbeError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        expected_count: int | None = None,
        observed_count: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.expected_count = expected_count
        self.observed_count = observed_count


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_canonical(item) for item in value]
        return sorted(
            items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), default=str),
        )
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _revision(value: Any) -> str:
    encoded = json.dumps(
        _canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _resource(
    provider: str,
    resource: str,
    scope: str,
    value: Any,
    *,
    expected_count: int | None = None,
    observed_count: int | None = None,
) -> ResourceRevision:
    return ResourceRevision(
        provider=provider,
        resource=resource,
        scope=scope,
        revision=_revision(value),
        status="ok",
        expected_count=expected_count,
        observed_count=observed_count,
    )


def _status_code(error: BaseException) -> int | None:
    direct = getattr(error, "status_code", None)
    if isinstance(direct, int):
        return direct
    response = getattr(error, "response", None)
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else None


def _classify(provider: str, error: BaseException) -> ProviderProbeError:
    status = _status_code(error)
    if status in {401, 403}:
        return ProviderProbeError(
            "credential_required",
            f"{provider.title()} authorization must be reconnected",
        )
    if status == 429:
        return ProviderProbeError("rate_limited", f"{provider.title()} is rate limited")
    if status is not None and status >= 500:
        return ProviderProbeError("temporarily_unavailable", f"{provider.title()} is temporarily unavailable")
    if isinstance(error, ProviderProbeError):
        return error
    return ProviderProbeError("provider_error", f"{provider.title()} probe failed")


def _mapping(value: Any, provider: str, resource: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProviderProbeError(
            "malformed_response",
            f"{provider.title()} returned a malformed {resource} response",
        )
    return value


def _rows(value: Any, provider: str, resource: str) -> list[Any]:
    if not isinstance(value, list):
        raise ProviderProbeError(
            "malformed_response",
            f"{provider.title()} returned a malformed {resource} response",
        )
    return value


def _expected_team_count(settings: Mapping[str, Any], *fallbacks: Any) -> int:
    for candidate in (
        settings.get("num_teams"),
        settings.get("size"),
        *fallbacks,
    ):
        try:
            parsed = int(candidate)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    raise ProviderProbeError("malformed_response", "Provider response omitted the expected team count")


def _validate_team_count(provider: str, expected: int, observed: int) -> None:
    if observed != expected:
        raise ProviderProbeError(
            "incomplete_source",
            f"{provider.title()} returned {observed} of {expected} teams",
            expected_count=expected,
            observed_count=observed,
        )


def probe_yahoo_oauth(
    client: Any,
    *,
    league_id: str,
    season: int,
    through_week: int,
) -> ProviderProbeResult:
    provider = "yahoo"
    try:
        league = _mapping(client.get_league(league_id), provider, "league")
        settings = _mapping(league.get("settings"), provider, "settings")
        returned_id = str(league.get("league_id") or league_id)
        if returned_id != str(league_id):
            raise ProviderProbeError("incomplete_source", "Yahoo returned a different league identity")
        teams = _rows(client.get_league_users(league_id), provider, "teams")
        rosters = _rows(client.get_league_rosters(league_id), provider, "rosters")
        expected = _expected_team_count(settings)
        _validate_team_count(provider, expected, len(teams))
        _validate_team_count(provider, expected, len(rosters))

        revisions = [
            _resource(provider, "settings", str(season), league),
            _resource(provider, "teams", str(season), teams, expected_count=expected, observed_count=len(teams)),
            _resource(provider, "rosters", str(season), rosters, expected_count=expected, observed_count=len(rosters)),
        ]
        for week in range(1, int(through_week) + 1):
            matchups = _rows(client.get_week_matchups(league_id, week), provider, "matchups")
            transactions = _rows(client.get_week_transactions(league_id, week), provider, "transactions")
            revisions.extend(
                (
                    _resource(provider, "matchups", f"{season}:{week}", matchups, observed_count=len(matchups)),
                    _resource(
                        provider,
                        "transactions",
                        f"{season}:{week}",
                        transactions,
                        expected_count=0 if not transactions else len(transactions),
                        observed_count=len(transactions),
                    ),
                )
            )
        draft_rows: list[Any] = []
        for draft in _rows(client.get_drafts(league_id), provider, "draft"):
            if not isinstance(draft, Mapping):
                raise ProviderProbeError("malformed_response", "Yahoo returned a malformed draft response")
            draft_id = str(draft.get("draft_id") or "").strip()
            if not draft_id:
                raise ProviderProbeError("malformed_response", "Yahoo returned a draft without an identity")
            draft_rows.extend(_rows(client.get_draft_picks(draft_id), provider, "draft picks"))
        revisions.append(
            _resource(provider, "draft", str(season), draft_rows, observed_count=len(draft_rows))
        )
        return ProviderProbeResult(
            provider=provider,
            league_id=str(league_id),
            active_season=int(season),
            through_week=int(through_week),
            revisions=tuple(sorted(revisions, key=lambda row: (row.provider, row.resource, row.scope))),
            expected_teams=expected,
            observed_teams=len(teams),
        )
    except BaseException as error:
        raise _classify(provider, error) from error


def probe_sleeper(
    client: Any,
    *,
    league_id: str,
    season: int,
    through_week: int,
) -> ProviderProbeResult:
    provider = "sleeper"
    try:
        league = _mapping(client.get_league(league_id), provider, "league")
        if str(league.get("league_id") or "") != str(league_id):
            raise ProviderProbeError("incomplete_source", "Sleeper returned a different league identity")
        if int(league.get("season") or 0) != int(season):
            raise ProviderProbeError("incomplete_source", "Sleeper returned a different active season")
        teams = _rows(client.get_league_users(league_id), provider, "teams")
        rosters = _rows(client.get_league_rosters(league_id), provider, "rosters")
        expected = _expected_team_count(league, league.get("total_rosters"))
        _validate_team_count(provider, expected, len(teams))
        _validate_team_count(provider, expected, len(rosters))
        revisions = [
            _resource(provider, "settings", str(season), league),
            _resource(provider, "teams", str(season), teams, expected_count=expected, observed_count=len(teams)),
            _resource(provider, "rosters", str(season), rosters, expected_count=expected, observed_count=len(rosters)),
        ]
        for week in range(1, int(through_week) + 1):
            matchups = _rows(client.get_league_matchups(league_id, week), provider, "matchups")
            transactions = _rows(client.get_league_transactions(league_id, week), provider, "transactions")
            revisions.extend(
                (
                    _resource(provider, "matchups", f"{season}:{week}", matchups, observed_count=len(matchups)),
                    _resource(
                        provider,
                        "transactions",
                        f"{season}:{week}",
                        transactions,
                        expected_count=0 if not transactions else len(transactions),
                        observed_count=len(transactions),
                    ),
                )
            )
        draft_rows: list[Any] = []
        for draft in _rows(client.get_league_drafts(league_id), provider, "draft"):
            if not isinstance(draft, Mapping):
                raise ProviderProbeError("malformed_response", "Sleeper returned a malformed draft response")
            draft_id = str(draft.get("draft_id") or "").strip()
            if not draft_id:
                raise ProviderProbeError("malformed_response", "Sleeper returned a draft without an identity")
            draft_rows.extend(_rows(client.get_draft_picks(draft_id), provider, "draft picks"))
        revisions.append(_resource(provider, "draft", str(season), draft_rows, observed_count=len(draft_rows)))
        return ProviderProbeResult(
            provider=provider,
            league_id=str(league_id),
            active_season=int(season),
            through_week=int(through_week),
            revisions=tuple(sorted(revisions, key=lambda row: (row.provider, row.resource, row.scope))),
            expected_teams=expected,
            observed_teams=len(teams),
        )
    except BaseException as error:
        raise _classify(provider, error) from error


def probe_espn(
    client: Any,
    *,
    league_id: str,
    season: int,
    through_week: int,
) -> ProviderProbeResult:
    provider = "espn"
    try:
        league = _mapping(
            client.get_raw_league(
                int(season),
                ["mSettings", "mTeam", "mRoster", "mScoreboard", "mMatchupScore", "mTransactions2", "mDraftDetail"],
                scoringPeriodId=int(through_week),
            ),
            provider,
            "league",
        )
        if str(league.get("id") or "") != str(league_id):
            raise ProviderProbeError("incomplete_source", "Espn returned a different league identity")
        if int(league.get("seasonId") or 0) != int(season):
            raise ProviderProbeError("incomplete_source", "Espn returned a different active season")
        settings = _mapping(league.get("settings"), provider, "settings")
        teams = _rows(league.get("teams"), provider, "teams")
        members = _rows(league.get("members", []), provider, "members")
        expected = _expected_team_count(settings, len(teams))
        _validate_team_count(provider, expected, len(teams))
        rosters = [team.get("roster", {}) for team in teams if isinstance(team, Mapping)]
        _validate_team_count(provider, expected, len(rosters))
        schedule = _rows(league.get("schedule", []), provider, "matchups")
        transactions = _rows(league.get("transactions", []), provider, "transactions")
        revisions = [
            _resource(provider, "settings", str(season), settings),
            _resource(provider, "teams", str(season), {"members": members, "teams": teams}, expected_count=expected, observed_count=len(teams)),
            _resource(provider, "rosters", str(season), rosters, expected_count=expected, observed_count=len(rosters)),
            _resource(provider, "matchups", f"{season}:{through_week}", schedule, observed_count=len(schedule)),
            _resource(
                provider,
                "transactions",
                f"{season}:{through_week}",
                transactions,
                expected_count=0 if not transactions else len(transactions),
                observed_count=len(transactions),
            ),
            _resource(provider, "draft", str(season), league.get("draftDetail", {})),
        ]
        return ProviderProbeResult(
            provider=provider,
            league_id=str(league_id),
            active_season=int(season),
            through_week=int(through_week),
            revisions=tuple(sorted(revisions, key=lambda row: (row.provider, row.resource, row.scope))),
            expected_teams=expected,
            observed_teams=len(teams),
        )
    except BaseException as error:
        raise _classify(provider, error) from error


def probe_multiplatform(segments: Sequence[Mapping[str, Any]]) -> MultiplatformProbeResult:
    probes = {
        "yahoo": probe_yahoo_oauth,
        "espn": probe_espn,
        "sleeper": probe_sleeper,
    }
    results: list[ProviderProbeResult] = []
    for segment in segments:
        provider = str(segment.get("provider") or "").strip().lower()
        probe = probes.get(provider)
        if probe is None:
            raise ProviderProbeError("unsupported_provider", f"Unsupported provider segment: {provider}")
        results.append(
            probe(
                segment.get("client"),
                league_id=str(segment.get("league_id") or ""),
                season=int(segment.get("season") or 0),
                through_week=int(segment.get("through_week") or 0),
            )
        )
    ordered = tuple(sorted(results, key=lambda result: (result.provider, result.active_season, result.league_id)))
    revisions = tuple(
        sorted(
            (revision for result in ordered for revision in result.revisions),
            key=lambda row: (row.provider, row.resource, row.scope),
        )
    )
    return MultiplatformProbeResult(
        segments=ordered,
        revisions=revisions,
        healthy=all(result.healthy for result in ordered),
    )
