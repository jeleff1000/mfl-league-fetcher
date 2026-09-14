"""Closed executor for NFL.com's L7 made-attempted field-goal cells.

The raw NFL.com relations are player-season split surfaces, not canonical data.
This module emits one scalar observation for each made and derived-miss fact while
retaining the raw value and split-grain locator for a later receipt/dossier writer.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
import re
from pathlib import Path

import duckdb

from .composite_witness_lane import CompositeSpec, EXECUTORS, LaneObservation, LanePaths, LaneResult, load_specs
from .nflcom_splits_column_adjudication import _BUCKETS


MADE_ATTEMPTED = re.compile(r"^(\d+)-(\d+)$")
_SPEC_ID = "nflcom-l7-fg-buckets-v1"
_SOURCES = ("nflcom_player_situational", "nflcom_player_splits")
_TARGETS = tuple(
    target
    for bucket in _BUCKETS.values()
    for target in (f"fg_made_{bucket}", f"fg_missed_{bucket}")
)
_GRAIN = ("nflcom_slug", "season", "_table", "_layout", "split_value")
_L7_LAYOUT = re.compile(r"_L7$")
_NORMALIZED_PENDING = "NORMALIZED_PENDING_GRAIN_RECONCILIATION"
_ALLOWED_L7_COLUMNS = frozenset((
    *_GRAIN,
    "player",
    "_view",
    "_lost_column",
    "g",
    "fgm",
    "fg_att",
    *_BUCKETS,
))


def parse_made_attempted(value: str) -> tuple[int, int]:
    """Parse NFL.com's exact made-attempted form into made and missed counts."""
    match = MADE_ATTEMPTED.fullmatch(value or "")
    if not match:
        raise ValueError(f"malformed made-attempted value: {value!r}")
    made, attempted = map(int, match.groups())
    if attempted < made:
        raise ValueError(f"attempted {attempted} is less than made {made}")
    return made, attempted - made


def _registered_spec() -> CompositeSpec:
    matches = [spec for spec in load_specs() if spec.spec_id == _SPEC_ID]
    if len(matches) != 1:
        raise ValueError(f"closed contract must contain exactly one {_SPEC_ID!r} spec")
    return matches[0]


def _authorized_spec(spec: CompositeSpec) -> bool:
    expected = _registered_spec()
    return (
        spec == expected
        and spec.cohort == "nflcom_l7"
        and spec.sources == _SOURCES
        and spec.derivation == "nflcom_l7_bucket_made_attempted"
        and spec.kind == "SCALAR_OBSERVATION"
        and spec.targets == _TARGETS
        and spec.unit == "count"
        and spec.natural_grain == "player_game"
        and spec.aggregation_class == "SUM"
        and spec.crosswalk_receipt == "nflcom_slug_pfrid"
    )


def _route_keys(spec: CompositeSpec) -> set[tuple[str, str, str, str]]:
    routes: set[tuple[str, str, str, str]] = set()
    for key in spec.row_keys:
        parts = key.split("|", 3)
        if len(parts) != 4:
            raise ValueError(f"malformed nflcom_l7 row key: {key!r}")
        source, table, layout, bucket = parts
        if source not in _SOURCES or bucket not in _BUCKETS:
            raise ValueError(f"unauthorized nflcom_l7 row key: {key!r}")
        routes.add((source, table, layout, bucket))
    if len(routes) != 78:
        raise ValueError(f"nflcom_l7 contract must enumerate 78 routes, found {len(routes)}")
    return routes


def _read_parquet(
    path: Path,
    *,
    candidate_only: bool = False,
) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    connection = duckdb.connect()
    try:
        description = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        columns = tuple(str(row[0]) for row in description)
        where = ""
        if candidate_only:
            terms = []
            if "_layout" in columns:
                terms.append("regexp_matches(COALESCE(CAST(\"_layout\" AS VARCHAR), ''), '_L7$')")
            terms.extend(
                f"NULLIF(CAST(\"{bucket}\" AS VARCHAR), '') IS NOT NULL"
                for bucket in _BUCKETS
                if bucket in columns
            )
            where = " WHERE " + (" OR ".join(terms) if terms else "FALSE")
        distinct = "DISTINCT " if candidate_only else ""
        rows = connection.execute(
            f"SELECT {distinct}* FROM read_parquet(?){where}", [str(path)]
        ).fetchall()
        return columns, rows
    finally:
        connection.close()


def _nonempty(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _crosswalk(paths: LanePaths) -> tuple[dict[str, str], list[str]]:
    path = paths.source_paths.get("nflcom_slug_pfrid")
    if path is None:
        return {}, ["crosswalk: missing nflcom_slug_pfrid source path"]
    try:
        columns, rows = _read_parquet(path)
    except (OSError, duckdb.Error) as exc:
        return {}, [f"crosswalk: unable to read nflcom_slug_pfrid: {exc}"]
    if not {"nflcom_slug", "pfr_id"} <= set(columns):
        return {}, ["crosswalk: requires nflcom_slug and pfr_id columns"]
    positions = {column: columns.index(column) for column in ("nflcom_slug", "pfr_id")}
    pairs = [(_nonempty(row[positions["nflcom_slug"]]), _nonempty(row[positions["pfr_id"]])) for row in rows]
    errors = ["crosswalk: null slug or pfr_id in resolved nflcom_slug_pfrid relation"] if any(
        slug is None or pfr_id is None for slug, pfr_id in pairs
    ) else []
    valid_pairs = [(slug, pfr_id) for slug, pfr_id in pairs if slug is not None and pfr_id is not None]
    slugs = Counter(slug for slug, _ in valid_pairs)
    pfr_ids = Counter(pfr_id for _, pfr_id in valid_pairs)
    if any(count != 1 for count in slugs.values()) or any(count != 1 for count in pfr_ids.values()):
        errors.append("crosswalk: nflcom_slug_pfrid must be one-to-one")
    return dict(valid_pairs), errors


def _evidence(source: str, row: dict[str, object], pfr_id: str, bucket: str, raw_value: object) -> tuple[tuple[str, str], ...]:
    return tuple((key, str(value)) for key, value in (
        ("source", source),
        ("nflcom_slug", row["nflcom_slug"]),
        ("pfr_id", pfr_id),
        ("season", row["season"]),
        ("table", row["_table"]),
        ("layout", row["_layout"]),
        ("split_value", row["split_value"]),
        ("bucket", bucket),
        ("raw_value", raw_value),
    ))


def _source_rows(source: str, path: Path) -> tuple[list[dict[str, object]], list[str]]:
    try:
        columns, records = _read_parquet(path, candidate_only=True)
    except (OSError, duckdb.Error) as exc:
        return [], [f"source: {source} cannot be read: {exc}"]
    required = set(_GRAIN) | set(_BUCKETS)
    missing = sorted(required - set(columns))
    if missing:
        return [], [f"source: {source} missing required columns {missing}"]
    return [dict(zip(columns, record, strict=True)) for record in records], []


def _unlabeled_zero_stadium_aggregate(row: dict[str, object]) -> bool:
    """Identify the retained unlabeled all-zero Stadium summary rows."""
    if _nonempty(row.get("split_value")) is not None or row.get("_table") != "Stadiums":
        return False
    try:
        return all(parse_made_attempted(row.get(column)) == (0, 0) for column in _BUCKETS)  # type: ignore[arg-type]
    except ValueError:
        return False


def execute_nflcom_l7(spec: CompositeSpec, paths: LanePaths) -> LaneResult:
    """Execute only the exact enrolled NFL.com L7 made-attempted contract.

    Any rejected L7 candidate makes the run fail closed: no observation is returned
    with ``PASS`` status. Rows in unrelated non-L7 layouts are intentionally outside
    this closed executor and are ignored rather than treated as contract members.
    """
    if not _authorized_spec(spec):
        return LaneResult((), 0, 1, ("unauthorized spec: requires the exact nflcom_l7 contract",))

    try:
        routes = _route_keys(spec)
    except ValueError as exc:
        return LaneResult((), 0, 1, (f"unauthorized spec: {exc}",))

    crosswalk, errors = _crosswalk(paths)
    rejected = 1 if errors else 0
    candidates: list[tuple[str, dict[str, object]]] = []
    for source in _SOURCES:
        path = paths.source_paths.get(source)
        if path is None:
            errors.append(f"source: missing registered source path {source}")
            rejected += 1
            continue
        rows, source_errors = _source_rows(source, path)
        if source_errors:
            errors.extend(source_errors)
            rejected += 1
            continue
        candidates.extend(
            (source, row)
            for row in rows
            if _L7_LAYOUT.search(str(row.get("_layout") or ""))
            or any(_nonempty(row.get(bucket)) is not None for bucket in _BUCKETS)
        )

    # The licensed identity receipt defines the admitted slug set.  Retained
    # slugs excluded by that bijection are coverage, not rejected identities;
    # no name fallback or observation is permitted.  Likewise, NFL.com's
    # unlabeled all-zero Stadium summaries are not named split-value grains.
    scanned_rows = len(candidates)
    candidates = [
        (source, row)
        for source, row in candidates
        if not _unlabeled_zero_stadium_aggregate(row)
        and (
            _nonempty(row.get("nflcom_slug")) is None
            or _nonempty(row.get("nflcom_slug")) in crosswalk
        )
    ]

    invalid_grain_indexes: set[int] = set()
    for index, (source, row) in enumerate(candidates):
        missing = [column for column in _GRAIN if _nonempty(row.get(column)) is None]
        if missing:
            invalid_grain_indexes.add(index)
            errors.append(f"grain: {source} has null or empty {missing!r}")
    rejected += len(invalid_grain_indexes)

    duplicate_indexes: set[int] = set()
    first_indexes: dict[tuple[str | None, ...], int] = {}
    grains = Counter(
        tuple(_nonempty(row.get(column)) for column in _GRAIN)
        for index, (_, row) in enumerate(candidates)
        if index not in invalid_grain_indexes
    )
    for index, (source, row) in enumerate(candidates):
        if index in invalid_grain_indexes:
            continue
        grain = tuple(_nonempty(row.get(column)) for column in _GRAIN)
        first_indexes.setdefault(grain, index)
        if grains[grain] > 1:
            duplicate_indexes.add(index)
            if index == first_indexes[grain]:
                errors.append(f"duplicate grain: {source} {grain!r}")
    rejected += len(duplicate_indexes)

    observations: list[LaneObservation] = []
    for index, (source, row) in enumerate(candidates):
        if index in invalid_grain_indexes or index in duplicate_indexes:
            continue
        table = _nonempty(row.get("_table"))
        layout = _nonempty(row.get("_layout"))
        slug = _nonempty(row.get("nflcom_slug"))
        if (source, table, layout, "1_19") not in routes:
            errors.append(f"route: {source}|{table}|{layout} is outside nflcom_l7")
            rejected += 1
            continue
        if slug is None or slug not in crosswalk:
            errors.append(f"unresolved identity: {source} nflcom_slug={slug!r}")
            rejected += 1
            continue

        row_errors: list[str] = []
        unexpected_columns = sorted(
            column for column, value in row.items()
            if column not in _ALLOWED_L7_COLUMNS and _nonempty(value) is not None
        )
        if unexpected_columns:
            row_errors.append(
                f"schema: {source}|{table}|{layout} has populated uncontracted columns "
                f"{unexpected_columns!r}"
            )
        for column, bucket in _BUCKETS.items():
            raw_value = row.get(column)
            try:
                parse_made_attempted(raw_value)  # type: ignore[arg-type]
            except ValueError as exc:
                row_errors.append(f"malformed: {source}|{table}|{layout}|{column}: {exc}")
        if row_errors:
            errors.extend(row_errors)
            rejected += 1
            continue

        pfr_id = crosswalk[slug]
        for column, bucket in _BUCKETS.items():
            raw_value = row[column]
            made, missed = parse_made_attempted(raw_value)  # type: ignore[arg-type]
            evidence = _evidence(source, row, pfr_id, bucket, raw_value)
            for target, value in ((f"fg_made_{bucket}", made), (f"fg_missed_{bucket}", missed)):
                observations.append(LaneObservation(
                    spec_id=spec.spec_id,
                    source=source,
                    kind="SCALAR_OBSERVATION",
                    targets=(target,),
                    compared_rows=1,
                    rejected_rows=0,
                    status=_NORMALIZED_PENDING,
                    value=value,
                    evidence=evidence,
                ))

    if errors:
        return LaneResult((), scanned_rows, rejected, tuple(errors))
    return LaneResult(tuple(observations), scanned_rows, 0, ())


EXECUTORS["nflcom_l7"] = execute_nflcom_l7
