"""Fail-closed PFR field-goal bucket comparisons at player-season grain.

PFR's kicking tables publish season aggregates.  This executor therefore does
not grant scalar credit from their arithmetic alone: it aggregates canonical
player-game rows to the resolved ``pfr_id/NFL_player_id + season + season
type`` key and requires equality before a scalar observation can pass.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import math
import re
from pathlib import Path

import duckdb

from .composite_witness_lane import CompositeSpec, EXECUTORS, LaneObservation, LanePaths, LaneResult, load_specs


_SCALAR_SPEC_ID = "pfr-field-goals-buckets-1-4-v1"
_CONSTRAINT_SPEC_ID = "pfr-field-goals-50-plus-v1"
_SOURCES = (("pfr_player_kicking", "REG"), ("pfr_kicking_post", "POST"))
_BUCKETS = (
    (1, "0_19"), (2, "20_29"), (3, "30_39"), (4, "40_49"),
)
_YEAR = re.compile(r"^\d{4}$")
_MULTI_TEAM = re.compile(r"^\d+(?:TM|LG)$")


def _read_parquet(
    path: Path,
    requested: set[str] | None = None,
    *,
    distinct: bool = False,
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    connection = duckdb.connect()
    try:
        description = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        available = tuple(str(row[0]) for row in description)
        columns = tuple(column for column in available if requested is None or column in requested)
        projection = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
        qualifier = "DISTINCT " if distinct else ""
        rows = connection.execute(f"SELECT {qualifier}{projection} FROM read_parquet(?)", [str(path)]).fetchall()
        return columns, rows
    finally:
        connection.close()


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    value = str(value).strip()
    return value or None


def _number(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else None


def _registered_specs() -> dict[str, CompositeSpec]:
    specs = {spec.spec_id: spec for spec in load_specs() if spec.spec_id in {_SCALAR_SPEC_ID, _CONSTRAINT_SPEC_ID}}
    if set(specs) != {_SCALAR_SPEC_ID, _CONSTRAINT_SPEC_ID}:
        raise ValueError("closed PFR FG contract must contain both enrolled specs")
    return specs


def _authorized(spec: CompositeSpec) -> bool:
    try:
        registered = _registered_specs()[spec.spec_id]
    except (KeyError, ValueError):
        return False
    return spec == registered and spec.cohort == "pfr_field_goals" and spec.sources == tuple(source for source, _ in _SOURCES) and spec.crosswalk_receipt == "bio_pfr_nflid"


def _crosswalk(path: Path) -> tuple[dict[str, str], list[str]]:
    try:
        columns, rows = _read_parquet(path, {"pfr_id", "NFL_player_id"})
    except (OSError, duckdb.Error) as exc:
        return {}, [f"crosswalk: unable to read player_bio: {exc}"]
    if not {"pfr_id", "NFL_player_id"} <= set(columns):
        return {}, ["crosswalk: player_bio requires pfr_id and NFL_player_id"]
    pfr_at, nfl_at = columns.index("pfr_id"), columns.index("NFL_player_id")
    pairs = [(_text(row[pfr_at]), _text(row[nfl_at])) for row in rows]
    pairs = [(pfr, nfl) for pfr, nfl in pairs if pfr is not None and nfl is not None]
    pfr_counts, nfl_counts = Counter(pfr for pfr, _ in pairs), Counter(nfl for _, nfl in pairs)
    if any(count != 1 for count in pfr_counts.values()) or any(count != 1 for count in nfl_counts.values()):
        return {}, ["crosswalk: bio_pfr_nflid must be one-to-one"]
    return dict(pairs), []


def _source_rows(source: str, season_type: str, path: Path, buckets: tuple[int, ...]) -> tuple[list[dict[str, object]], list[str]]:
    required = {"pfr_id", "year_id"} | {f"fg{kind}{bucket}" for bucket in buckets for kind in ("m", "a")}
    try:
        columns, records = _read_parquet(path, required | {"team_name_abbr"})
    except (OSError, duckdb.Error) as exc:
        return [], [f"source: {source} cannot be read: {exc}"]
    missing = sorted(required - set(columns))
    if missing:
        return [], [f"source: {source} missing required columns {missing!r}"]
    rows = [dict(zip(columns, record, strict=True)) for record in records]
    errors: list[str] = []
    valid: list[dict[str, object]] = []
    parsed: list[tuple[dict[str, object], str | None, str | None]] = []
    for row in rows:
        operands = [row[f"fg{kind}{bucket}"] for bucket in buckets for kind in ("m", "a")]
        # PFR season tables contain many players for whom the enrolled bucket
        # family is wholly unpopulated.  They are outside this spec's candidate
        # surface; a partially populated or malformed candidate still rejects.
        if not any(_text(value) is not None for value in operands):
            continue
        pfr_id, year = _text(row["pfr_id"]), _text(row["year_id"])
        numeric_year = _number(row["year_id"])
        if numeric_year is not None:
            year = str(numeric_year)
        if year is not None and not _YEAR.fullmatch(year):
            year = None
        # Career, team and league summary rows share these broad PFR tables,
        # but are not player-season facts admitted by this contract.
        if year is None:
            continue
        parsed.append((row, pfr_id, year))
        if pfr_id is None:
            errors.append(f"grain: {source} has null pfr_id on a season row")
    grouped: defaultdict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row, pfr_id, year in parsed:
        if pfr_id is not None and year is not None:
            grouped[(pfr_id, year)].append(row)
    admitted: list[tuple[dict[str, object], str, str]] = []
    for grain, candidates in grouped.items():
        if len(candidates) == 1:
            admitted.append((candidates[0], *grain))
            continue
        totals = [row for row in candidates if _MULTI_TEAM.fullmatch(_text(row.get("team_name_abbr")) or "")]
        if len(totals) == 1:
            admitted.append((totals[0], *grain))
        else:
            errors.append(f"duplicate grain: {source} {grain!r}")
    for row, pfr_id, year in admitted:
        if pfr_id is None or year is None:
            continue
        grain = (pfr_id, year)
        operand_error = False
        for bucket in buckets:
            made, attempted = _number(row[f"fgm{bucket}"]), _number(row[f"fga{bucket}"])
            if made is None or attempted is None:
                errors.append(f"null operand: {source} {grain!r} bucket {bucket}")
                operand_error = True
            elif attempted < made:
                errors.append(f"attempts less than made: {source} {grain!r} bucket {bucket}")
                operand_error = True
        if not operand_error:
            row["_source"] = source
            row["_season_type"] = season_type
            row["_season"] = year
            valid.append(row)
    return valid, errors


def _canonical(
    path: Path,
    targets: tuple[str, ...],
    required_keys: set[tuple[str, str, str]],
) -> tuple[dict[tuple[str, str, str], tuple[dict[str, int], int]], list[str]]:
    required = {"NFL_player_id", "year", "week", "season_type", *targets}
    try:
        columns, rows = _read_parquet(path, required, distinct=True)
    except (OSError, duckdb.Error) as exc:
        return {}, [f"canonical: unable to read player-game rows: {exc}"]
    missing = sorted(required - set(columns))
    if missing:
        return {}, [f"canonical: missing required columns {missing!r}"]
    positions = {name: columns.index(name) for name in required}
    errors: list[str] = []
    grains: Counter[tuple[str | None, str | None, str | None, str | None]] = Counter()
    normalized: list[tuple[tuple[str | None, str | None, str | None, str | None], dict[str, int] | None]] = []
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
        values = {
            target: 0 if _text(row[positions[target]]) is None else _number(row[positions[target]])
            for target in targets
        }
        if None in grain or season_type not in {"REG", "POST"}:
            errors.append("canonical: null or invalid player_game grain")
            normalized.append((grain, None))
        elif any(value is None for value in values.values()):
            errors.append(f"canonical: null/non-integral operand at {grain!r}")
            normalized.append((grain, None))
        else:
            normalized.append((grain, values))  # type: ignore[arg-type]
    duplicate_grains = {grain for grain, count in grains.items() if None not in grain and count > 1}
    if duplicate_grains:
        errors.extend(f"duplicate canonical grain: {grain!r}" for grain in sorted(duplicate_grains))
    totals: dict[tuple[str, str, str], tuple[dict[str, int], int]] = {}
    sums: defaultdict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: {target: 0 for target in targets})
    counts: Counter[tuple[str, str, str]] = Counter()
    for grain, values in normalized:
        if values is None or grain in duplicate_grains:
            continue
        nfl_id, year, season_type, _ = grain
        assert nfl_id is not None and year is not None and season_type is not None
        key = (nfl_id, year, season_type)
        counts[key] += 1
        for target, value in values.items():
            sums[key][target] += value
    for key, values in sums.items():
        totals[key] = (values, counts[key])
    return totals, errors


def _evidence(source: str, pfr_id: str, nfl_id: str, season: str, season_type: str, source_value: int, canonical_value: int, **extra: str) -> tuple[tuple[str, str], ...]:
    fields = {
        "source": source, "pfr_id": pfr_id, "NFL_player_id": nfl_id,
        "season": season, "season_type": season_type,
        "source_value": str(source_value), "canonical_value": str(canonical_value), **extra,
    }
    return tuple(fields.items())


def execute_pfr_field_goals(spec: CompositeSpec, paths: LanePaths) -> LaneResult:
    """Compare exactly one closed PFR FG spec against canonical game aggregates."""
    if not _authorized(spec):
        return LaneResult((), 0, 1, ("unauthorized spec: requires an exact enrolled PFR FG contract",))

    buckets = (1, 2, 3, 4) if spec.spec_id == _SCALAR_SPEC_ID else (5,)
    canonical_targets = spec.targets
    crosswalk, errors = _crosswalk(paths.player_bio)
    source_rows: list[dict[str, object]] = []
    for source, season_type in _SOURCES:
        path = paths.source_paths.get(source)
        if path is None:
            errors.append(f"source: missing registered source path {source}")
            continue
        rows, source_errors = _source_rows(source, season_type, path, buckets)
        source_rows.extend(rows)
        errors.extend(source_errors)
    required_keys: set[tuple[str, str, str]] = set()
    for row in source_rows:
        pfr_id = str(row["pfr_id"])
        nfl_id = crosswalk.get(pfr_id)
        if nfl_id is None:
            errors.append(f"unresolved identity: {row['_source']} pfr_id={pfr_id!r}")
            continue
        required_keys.add((nfl_id, str(row["_season"]), str(row["_season_type"])))
    canonical, canonical_errors = _canonical(paths.v26, canonical_targets, required_keys)
    errors.extend(canonical_errors)
    if errors:
        return LaneResult((), len(source_rows), len(errors), tuple(errors))

    observations: list[LaneObservation] = []
    mismatches = False
    for row in source_rows:
        source, season_type = str(row["_source"]), str(row["_season_type"])
        pfr_id, season = str(row["pfr_id"]), str(row["_season"])
        nfl_id = crosswalk.get(pfr_id)
        assert nfl_id is not None
        aggregate = canonical.get((nfl_id, season, season_type))
        if aggregate is None:
            operands = [
                _number(row[f"fg{kind}{bucket}"])
                for bucket in buckets
                for kind in ("m", "a")
            ]
            # An all-zero source row with no canonical player-game rows has no
            # positive fact to reconcile.  Zero rows with canonical coverage
            # are still compared, so a canonical positive cannot be hidden.
            if all(value == 0 for value in operands):
                continue
            errors.append(f"mismatch: no canonical player-season aggregate for {source}|{pfr_id}|{season}|{season_type}")
            continue
        canonical_values, game_rows = aggregate
        if spec.spec_id == _SCALAR_SPEC_ID:
            for bucket, suffix in _BUCKETS:
                made, attempted = _number(row[f"fgm{bucket}"]), _number(row[f"fga{bucket}"])
                assert made is not None and attempted is not None
                for target, source_value in ((f"fg_made_{suffix}", made), (f"fg_missed_{suffix}", attempted - made)):
                    canonical_value = canonical_values[target]
                    status = "PASS" if source_value == canonical_value else "MISMATCH"
                    mismatches |= status == "MISMATCH"
                    observations.append(LaneObservation(spec.spec_id, source, "SCALAR_OBSERVATION", (target,), game_rows, 0, status, source_value,
                        _evidence(source, pfr_id, nfl_id, season, season_type, source_value, canonical_value)))
        else:
            made, attempted = _number(row["fgm5"]), _number(row["fga5"])
            assert made is not None and attempted is not None
            pairs = (
                ("pfr_fg_50_plus_made", ("fg_made_50_59", "fg_made_60_"), made),
                ("pfr_fg_50_plus_missed", ("fg_missed_50_59", "fg_missed_60_"), attempted - made),
            )
            for constraint_id, targets, source_value in pairs:
                canonical_value = sum(canonical_values[target] for target in targets)
                status = "PASS" if source_value == canonical_value else "MISMATCH"
                mismatches |= status == "MISMATCH"
                observations.append(LaneObservation(spec.spec_id, source, "CONSTRAINT_OBSERVATION", targets, game_rows, 0, status, source_value,
                    _evidence(source, pfr_id, nfl_id, season, season_type, source_value, canonical_value, constraint_id=constraint_id)))

    if errors:
        return LaneResult((), len(source_rows), len(errors), tuple(errors))
    if mismatches:
        # One source-row failure must not leave a mix of passing facts that could be
        # mistaken for a partial receipt.
        observations = [LaneObservation(**{**item.__dict__, "status": "MISMATCH"}) for item in observations]
    return LaneResult(tuple(observations), len(source_rows), 0, ())


EXECUTORS["pfr_field_goals"] = execute_pfr_field_goals
