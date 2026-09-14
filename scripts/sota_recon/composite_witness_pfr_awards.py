"""Fail-closed typed local PFR All-Pro and Pro Bowl membership witnesses."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
import math
import re
from pathlib import Path

import duckdb

from .composite_witness_lane import (
    CompositeSpec,
    EXECUTORS,
    InternalResolution,
    LaneObservation,
    LanePaths,
    LaneResult,
    load_specs,
)


AwardPaths = LanePaths
_SPEC_ID = "pfr-awards-v1"
_SOURCES = ("pfr_all_pro_members", "pfr_pro_bowl_members")
_YEAR = re.compile(r"^\d{4}$")
_FIRST_TEAM = re.compile(r"(?:^|[,;]\s*)[^,:]+:\s*1st\s+Tm(?:\b|\s+All-Conf\.)", re.IGNORECASE)


def _read_parquet(
    path: Path,
    requested: set[str] | None = None,
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    con = duckdb.connect()
    try:
        available = tuple(str(row[0]) for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall())
        columns = tuple(column for column in available if requested is None or column in requested)
        projection = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
        return columns, con.execute(f"SELECT {projection} FROM read_parquet(?)", [str(path)]).fetchall()
    finally:
        con.close()


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    value = str(value).strip()
    return value or None


def _year(value: object) -> str | None:
    value = _text(value)
    if value is None:
        return None
    try:
        numeric = float(value)
    except ValueError:
        numeric = None
    if numeric is not None and numeric.is_integer():
        value = str(int(numeric))
    return value if _YEAR.fullmatch(value) else None


def _integer(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() and number >= 0 else None


def _pfr_id(row: dict[str, object]) -> str | None:
    direct = _text(row.get("pfr_id"))
    if direct is not None:
        return direct
    # Existing retained annual captures preserve the PFR link id.  This is an
    # identity extraction, not a display-name fallback.
    values = [part.strip() for part in str(row.get("player_link_ids", "")).split(";") if part.strip()]
    return values[0] if len(values) == 1 else None


def _registered_spec(spec: CompositeSpec) -> bool:
    enrolled = {item.spec_id: item for item in load_specs()}.get(_SPEC_ID)
    return enrolled == spec and spec.cohort == "pfr_awards" and spec.crosswalk_receipt == "bio_pfr_nflid"


def _crosswalk(path: Path) -> tuple[dict[str, str], list[str]]:
    try:
        columns, records = _read_parquet(path, {"pfr_id", "NFL_player_id"})
    except (OSError, duckdb.Error) as exc:
        return {}, [f"crosswalk: unable to read player_bio: {exc}"]
    if not {"pfr_id", "NFL_player_id"} <= set(columns):
        return {}, ["crosswalk: player_bio requires pfr_id and NFL_player_id"]
    pfr_at, nfl_at = columns.index("pfr_id"), columns.index("NFL_player_id")
    pairs = [(_text(row[pfr_at]), _text(row[nfl_at])) for row in records]
    pairs = [(pfr, nfl) for pfr, nfl in pairs if pfr is not None and nfl is not None]
    pfr_counts = Counter(pfr for pfr, _ in pairs)
    nfl_counts = Counter(nfl for _, nfl in pairs)
    if not pairs or any(count != 1 for count in pfr_counts.values()) or any(count != 1 for count in nfl_counts.values()):
        return {}, ["crosswalk: bio_pfr_nflid must be one-to-one"]
    return dict(pairs), []


def _records(source: str, path: Path) -> tuple[list[tuple[str, str]], list[str]]:
    try:
        columns, rows = _read_parquet(path, {
            "pfr_id", "player_link_ids", "year", "year_id", "first_team", "all_pro_string"
        })
    except (OSError, duckdb.Error) as exc:
        return [], [f"source: {source} cannot be read: {exc}"]
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    values: list[tuple[str, str]] = []
    errors: list[str] = []
    year_column = "year" if "year" in columns else "year_id" if "year_id" in columns else None
    if year_column is None:
        return [], [f"source: {source} missing required year column"]
    for row in records:
        pfr_id, year = _pfr_id(row), _year(row.get(year_column))
        if pfr_id is None or year is None:
            errors.append(f"source: {source} has null or invalid pfr_id/year")
            continue
        if source == "pfr_all_pro_members":
            explicit = _text(row.get("first_team"))
            published = _text(row.get("all_pro_string"))
            first_team = explicit is not None and explicit.lower() in {"1", "true", "yes", "first", "1st"}
            first_team = first_team or (published is not None and _FIRST_TEAM.search(published) is not None)
            if "first_team" not in columns and "all_pro_string" not in columns:
                return [], ["source: pfr_all_pro_members lacks explicit published first-team semantics"]
            if not first_team:
                continue
        values.append((pfr_id, year))
    return values, errors


def _canonical(path: Path) -> tuple[dict[str, tuple[int | None, int | None, int | None]], list[str], bool]:
    try:
        columns, records = _read_parquet(path, {"NFL_player_id", "hof", "allpro", "probowls"})
    except (OSError, duckdb.Error) as exc:
        return {}, [f"canonical: unable to read player_bio target surface: {exc}"], False
    required = {"NFL_player_id", "hof", "allpro", "probowls"}
    if not required <= set(columns):
        return {}, ["canonical: player_bio lacks player-static hof/allpro/probowls reconciliation columns"], True
    positions = {name: columns.index(name) for name in required}
    grouped: defaultdict[str, list[tuple[int | None, int | None, int | None]]] = defaultdict(list)
    errors: list[str] = []
    for row in records:
        nfl_id = _text(row[positions["NFL_player_id"]])
        hof_raw = _text(row[positions["hof"]])
        allpro, probowls = _integer(row[positions["allpro"]]), _integer(row[positions["probowls"]])
        raw_allpro, raw_probowls = row[positions["allpro"]], row[positions["probowls"]]
        if nfl_id is None and hof_raw is None and raw_allpro is None and raw_probowls is None:
            continue
        if nfl_id is None:
            errors.append("canonical: null NFL_player_id on player-static award surface")
            continue
        if hof_raw is not None and hof_raw.lower() not in {"true", "false", "1", "0"}:
            errors.append("canonical: invalid player-static award value")
            continue
        if raw_allpro is not None and allpro is None:
            errors.append("canonical: invalid player-static allpro value")
            continue
        if raw_probowls is not None and probowls is None:
            errors.append("canonical: invalid player-static probowls value")
            continue
        hof = None if hof_raw is None else int(hof_raw.lower() in {"true", "1"})
        grouped[nfl_id].append((hof, allpro, probowls))
    values: dict[str, tuple[int | None, int | None, int | None]] = {}
    for nfl_id, candidates in grouped.items():
        if len(candidates) != 1:
            errors.append(f"canonical: duplicate player-static award triple for NFL_player_id={nfl_id!r}")
        else:
            values[nfl_id] = candidates[0]
    return values, errors, False


def award_scalars(con: duckdb.DuckDBPyConnection, paths: AwardPaths) -> list[tuple[str, str, str]]:
    """Return typed page memberships; callers must compare them before receipt credit."""
    del con  # I/O stays isolated to controlled local Parquet relations.
    facts: list[tuple[str, str, str]] = []
    for source in _SOURCES:
        path = paths.source_paths.get(source)
        if path is None:
            raise ValueError(f"missing registered source path {source}")
        rows, errors = _records(source, path)
        if errors:
            raise ValueError("; ".join(errors))
        target = {"pfr_all_pro_members": "allpro", "pfr_pro_bowl_members": "probowls"}[source]
        facts.extend((target, pfr_id, year) for pfr_id, year in rows)
    return facts


def execute_pfr_awards(spec: CompositeSpec, paths: AwardPaths) -> LaneResult:
    """Compare local typed memberships and resolve Hall status internally."""
    if not _registered_spec(spec):
        return LaneResult((), 0, 1, ("unauthorized spec: requires the exact enrolled PFR awards contract",))
    crosswalk, errors = _crosswalk(paths.player_bio)
    facts: list[tuple[str, str, str]] = []
    for source in _SOURCES:
        path = paths.source_paths.get(source)
        if path is None:
            errors.append(f"source: missing registered source path {source}")
            continue
        rows, source_errors = _records(source, path)
        target = {"pfr_all_pro_members": "allpro", "pfr_pro_bowl_members": "probowls"}[source]
        facts.extend((target, pfr_id, year) for pfr_id, year in rows)
        errors.extend(source_errors)
    # `player_bio` is allowed as the canonical player-static target surface and
    # identity crosswalk, but never as any external evidence relation.
    canonical, canonical_errors, pending = _canonical(paths.player_bio)
    if pending:
        return LaneResult((LaneObservation(spec.spec_id, "pfr_awards", spec.kind, ("allpro", "probowls"), 0, 0, "PENDING", None,
            (("reason", canonical_errors[0]),)),), 0, 0, tuple(canonical_errors))
    errors.extend(canonical_errors)
    if errors:
        return LaneResult((), len(facts), len(errors), tuple(errors))

    by_player: defaultdict[str, dict[str, object]] = defaultdict(
        lambda: {"allpro": set(), "probowls": set()}
    )
    for target, pfr_id, year in facts:
        nfl_id = crosswalk.get(pfr_id)
        if nfl_id is None:
            errors.append(f"unresolved identity: pfr_id={pfr_id!r}")
            continue
        if nfl_id not in canonical:
            errors.append(f"mismatch: no canonical player-static awards for NFL_player_id={nfl_id!r}")
            continue
        by_player[nfl_id][target].add(year)  # type: ignore[union-attr]
    if errors:
        return LaneResult((), len(facts), len(errors), tuple(errors))

    observations: list[LaneObservation] = []
    internal_resolutions: list[InternalResolution] = []
    mismatches = False
    # Membership pages are complete typed universes: absence means the explicit
    # zero value.  Iterate the canonical target universe rather than only the
    # positive external facts, so a missing positive cannot become empty PASS.
    for nfl_id, canonical_value in sorted(canonical.items()):
        values = by_player[nfl_id]
        derived = (len(values["allpro"]), len(values["probowls"]))  # type: ignore[arg-type]
        for target, source_value, expected in zip(("allpro", "probowls"), derived, canonical_value[1:], strict=True):
            # A typed, locally retained membership universe is itself a valid
            # external observation when the canonical target is an explicit
            # gap.  Where canonical data exists, equality remains mandatory.
            status = "PASS" if expected is None or source_value == expected else "MISMATCH"
            mismatches |= status == "MISMATCH"
            evidence = [("NFL_player_id", nfl_id), ("source_value", str(source_value))]
            if expected is not None:
                evidence.append(("canonical_value", str(expected)))
            observations.append(LaneObservation(
                spec.spec_id, "pfr_awards", "SCALAR_OBSERVATION", (target,), 1, 0, status,
                int(source_value), tuple(evidence),
            ))
        if canonical_value[0] is not None:
            internal_resolutions.append(InternalResolution(
                spec.spec_id, "player_bio", ("hof",), 1, 0, "PASS", canonical_value[0],
                (("NFL_player_id", nfl_id),),
            ))
    if errors:
        return LaneResult((), len(facts), len(errors), tuple(errors))
    if mismatches:
        observations = [LaneObservation(**{**item.__dict__, "status": "MISMATCH"}) for item in observations]
        internal_resolutions = [InternalResolution(**{**item.__dict__, "status": "MISMATCH"}) for item in internal_resolutions]
    return LaneResult(tuple(observations), len(canonical), 0, (), tuple(internal_resolutions))


EXECUTORS["pfr_awards"] = execute_pfr_awards
