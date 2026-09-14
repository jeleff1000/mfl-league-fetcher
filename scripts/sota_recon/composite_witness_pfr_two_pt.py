"""Fail-closed local PFR two-point total conservation checks."""

from __future__ import annotations

from collections import Counter, defaultdict
import math
import re
from pathlib import Path

import duckdb

from .composite_witness_lane import CompositeSpec, EXECUTORS, LaneObservation, LanePaths, LaneResult, load_specs


_SPEC_ID = "pfr-two-point-total-v1"
_SOURCES = (("pfr_player_scoring", "REG"), ("pfr_scoring_post", "POST"))
_TARGETS = (
    "passing_2pt_conversions",
    "receiving_2pt_conversions",
    "rushing_2pt_conversions",
)
_YEAR = re.compile(r"^\d{4}$")
_MULTI_TEAM = re.compile(r"^\d+(?:TM|LG)$")


def _read_parquet(
    path: Path,
    requested: set[str] | None = None,
    *,
    distinct: bool = False,
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    con = duckdb.connect()
    try:
        available = tuple(str(row[0]) for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall())
        columns = tuple(column for column in available if requested is None or column in requested)
        projection = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
        qualifier = "DISTINCT " if distinct else ""
        return columns, con.execute(f"SELECT {qualifier}{projection} FROM read_parquet(?)", [str(path)]).fetchall()
    finally:
        con.close()


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    text = str(value).strip()
    return text or None


def _number(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else None


def _authorized(spec: CompositeSpec) -> bool:
    enrolled = {item.spec_id: item for item in load_specs()}.get(_SPEC_ID)
    return (
        enrolled == spec
        and spec.cohort == "pfr_two_point_total"
        and spec.sources == tuple(source for source, _ in _SOURCES)
        and spec.targets == _TARGETS
        and spec.crosswalk_receipt == "bio_pfr_nflid"
    )


def _crosswalk(path: Path) -> tuple[dict[str, str], list[str]]:
    try:
        columns, rows = _read_parquet(path, {"pfr_id", "NFL_player_id"})
    except (OSError, duckdb.Error) as exc:
        return {}, [f"crosswalk: unable to read player_bio: {exc}"]
    if not {"pfr_id", "NFL_player_id"} <= set(columns):
        return {}, ["crosswalk: player_bio requires pfr_id and NFL_player_id"]
    pfr_at, nfl_at = columns.index("pfr_id"), columns.index("NFL_player_id")
    pairs = [(_text(row[pfr_at]), _text(row[nfl_at])) for row in rows]
    pairs = [(pfr_id, nfl_id) for pfr_id, nfl_id in pairs if pfr_id is not None and nfl_id is not None]
    pfr_counts = Counter(pfr_id for pfr_id, _ in pairs)
    nfl_counts = Counter(nfl_id for _, nfl_id in pairs)
    if any(count != 1 for count in pfr_counts.values()) or any(count != 1 for count in nfl_counts.values()):
        return {}, ["crosswalk: bio_pfr_nflid must be one-to-one"]
    return dict(pairs), []


def _source_rows(source: str, season_type: str, path: Path) -> tuple[list[tuple[str, str, str, str, int]], list[str]]:
    required = {"pfr_id", "year_id", "two_pt_md"}
    try:
        columns, records = _read_parquet(path, required | {"team_name_abbr"})
    except (OSError, duckdb.Error) as exc:
        return [], [f"source: {source} cannot be read: {exc}"]
    missing = sorted(required - set(columns))
    if missing:
        return [], [f"source: {source} missing required columns {missing!r}"]
    positions = {column: columns.index(column) for column in required}
    parsed: list[tuple[str | None, str | None, int | None, str | None]] = []
    errors: list[str] = []
    for record in records:
        # Null totals are the common non-member rows in PFR's broad scoring
        # tables.  Non-null malformed totals remain candidates and reject.
        if _text(record[positions["two_pt_md"]]) is None:
            continue
        pfr_id = _text(record[positions["pfr_id"]])
        year = _text(record[positions["year_id"]])
        numeric_year = _number(record[positions["year_id"]])
        if numeric_year is not None:
            year = str(numeric_year)
        total = _number(record[positions["two_pt_md"]])
        if year is not None and not _YEAR.fullmatch(year):
            year = None
        # PFR mixes career/team summaries with annual rows.  The former are
        # outside this player-season contract even when their total is filled.
        if year is None:
            continue
        team = _text(record[columns.index("team_name_abbr")]) if "team_name_abbr" in columns else None
        parsed.append((pfr_id, year, total, team))
        if pfr_id is None:
            errors.append(f"grain: {source} has null pfr_id on a season row")
    grouped: defaultdict[tuple[str, str], list[tuple[int | None, str | None]]] = defaultdict(list)
    for pfr_id, year, total, team in parsed:
        if pfr_id is not None and year is not None:
            grouped[(pfr_id, year)].append((total, team))
    valid: list[tuple[str, str, str, int]] = []
    for grain, candidates in grouped.items():
        if len(candidates) == 1:
            total = candidates[0][0]
        else:
            totals = [total for total, team in candidates if _MULTI_TEAM.fullmatch(team or "")]
            total = totals[0] if len(totals) == 1 else None
        if len(candidates) > 1 and len([1 for _, team in candidates if _MULTI_TEAM.fullmatch(team or "")]) != 1:
            errors.append(f"duplicate grain: {source} {grain!r}")
            continue
        pfr_id, year = grain
        if total is None:
            errors.append(f"null/non-integral source total: {source} {grain!r}")
        elif total < 0:
            errors.append(f"source total below zero: {source} {grain!r}")
        else:
            valid.append((source, season_type, pfr_id, year, total))
    return valid, errors


def _canonical(
    path: Path,
    required_keys: set[tuple[str, str, str]],
) -> tuple[dict[tuple[str, str, str], tuple[int, int]], list[str]]:
    required = {"NFL_player_id", "year", "week", "season_type", *_TARGETS}
    try:
        columns, rows = _read_parquet(path, required, distinct=True)
    except (OSError, duckdb.Error) as exc:
        return {}, [f"canonical: unable to read player-game rows: {exc}"]
    missing = sorted(required - set(columns))
    if missing:
        return {}, [f"canonical: missing required columns {missing!r}"]
    positions = {column: columns.index(column) for column in required}
    normalized: list[tuple[tuple[str | None, str | None, str | None, str | None], int | None]] = []
    grains: Counter[tuple[str | None, str | None, str | None, str | None]] = Counter()
    errors: list[str] = []
    for row in rows:
        nfl_id = _text(row[positions["NFL_player_id"]])
        year_value = _number(row[positions["year"]])
        season_type = _text(row[positions["season_type"]])
        year = str(year_value) if year_value is not None else None
        if (nfl_id, year, season_type) not in required_keys:
            continue
        week_value = _number(row[positions["week"]])
        week = str(week_value) if week_value is not None else None
        grain = (nfl_id, year, season_type, week)
        grains[grain] += 1
        values = [
            0 if _text(row[positions[target]]) is None else _number(row[positions[target]])
            for target in _TARGETS
        ]
        if None in grain or season_type not in {"REG", "POST"}:
            errors.append("canonical: null or invalid player_game grain")
            normalized.append((grain, None))
        elif any(value is None or value < 0 for value in values):
            errors.append(f"canonical: null/non-integral or below-zero two-point component at {grain!r}")
            normalized.append((grain, None))
        else:
            normalized.append((grain, sum(values)))  # type: ignore[arg-type]
    duplicate_grains = {grain for grain, count in grains.items() if None not in grain and count > 1}
    errors.extend(f"duplicate canonical grain: {grain!r}" for grain in sorted(duplicate_grains))
    sums: defaultdict[tuple[str, str, str], int] = defaultdict(int)
    counts: Counter[tuple[str, str, str]] = Counter()
    for grain, total in normalized:
        if total is None or grain in duplicate_grains:
            continue
        nfl_id, year, season_type, _ = grain
        assert nfl_id is not None and year is not None and season_type is not None
        key = (nfl_id, year, season_type)
        sums[key] += total
        counts[key] += 1
    return {key: (total, counts[key]) for key, total in sums.items()}, errors


def execute_pfr_two_point(spec: CompositeSpec, paths: LanePaths) -> LaneResult:
    """Compare exactly the enrolled PFR two-point total, without typed credit."""
    if not _authorized(spec):
        return LaneResult((), 0, 1, ("unauthorized spec: requires the exact enrolled PFR two-point contract",))

    crosswalk, errors = _crosswalk(paths.player_bio)
    source_rows: list[tuple[str, str, str, str, int]] = []
    for source, season_type in _SOURCES:
        source_path = paths.source_paths.get(source)
        if source_path is None:
            errors.append(f"source: missing registered source path {source}")
            continue
        rows, source_errors = _source_rows(source, season_type, source_path)
        source_rows.extend(rows)
        errors.extend(source_errors)
    required_keys: set[tuple[str, str, str]] = set()
    for source, season_type, pfr_id, year, source_total in source_rows:
        nfl_id = crosswalk.get(pfr_id)
        if nfl_id is None:
            if source_total == 0:
                continue
            errors.append(f"unresolved identity: {source} pfr_id={pfr_id!r}")
            continue
        required_keys.add((nfl_id, year, season_type))
    canonical, canonical_errors = _canonical(paths.v26, required_keys)
    errors.extend(canonical_errors)
    if errors:
        return LaneResult((), len(source_rows), len(errors), tuple(errors))

    observations: list[LaneObservation] = []
    for source, season_type, pfr_id, year, source_total in source_rows:
        nfl_id = crosswalk.get(pfr_id)
        if nfl_id is None and source_total == 0:
            continue
        assert nfl_id is not None
        aggregate = canonical.get((nfl_id, year, season_type))
        if aggregate is None:
            if source_total == 0:
                continue
            errors.append(f"mismatch: no canonical player-season aggregate for {source}|{pfr_id}|{year}|{season_type}")
            continue
        canonical_total, game_rows = aggregate
        status = "PASS" if source_total == canonical_total else "MISMATCH"
        observations.append(LaneObservation(
            spec.spec_id,
            source,
            "CONSTRAINT_OBSERVATION",
            _TARGETS,
            game_rows,
            0,
            status,
            source_total,
            (
                ("source", source),
                ("pfr_id", pfr_id),
                ("NFL_player_id", nfl_id),
                ("season", year),
                ("season_type", season_type),
                ("source_value", str(source_total)),
                ("canonical_value", str(canonical_total)),
            ),
        ))
    if errors:
        return LaneResult((), len(source_rows), len(errors), tuple(errors))
    if any(item.status == "MISMATCH" for item in observations):
        observations = [LaneObservation(**{**item.__dict__, "status": "MISMATCH"}) for item in observations]
    return LaneResult(tuple(observations), len(source_rows), 0, ())


EXECUTORS["pfr_two_point_total"] = execute_pfr_two_point
