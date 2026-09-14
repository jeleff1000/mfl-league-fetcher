"""Behavior tests for the closed NFL.com L7 composite witness executor.

The fixtures deliberately use local Parquet files: the executor's boundary is the
registered unshifted source relation plus the resolved slug-to-PFR-ID crosswalk,
not the lake or a live NFL.com page.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from scripts.sota_recon.composite_witness_lane import (
    EXECUTORS,
    LaneObservation,
    LanePaths,
    artifact_manifest_sha256,
    load_specs,
    write_receipt,
)
from scripts.sota_recon.composite_witness_nflcom import execute_nflcom_l7, parse_made_attempted


BUCKETS = ("1_19", "20_29", "30_39", "40_49", "50_59", "60")
TARGETS = (
    "fg_made_0_19", "fg_missed_0_19",
    "fg_made_20_29", "fg_missed_20_29",
    "fg_made_30_39", "fg_missed_30_39",
    "fg_made_40_49", "fg_missed_40_49",
    "fg_made_50_59", "fg_missed_50_59",
    "fg_made_60_", "fg_missed_60_",
)
ROUTES = {
    "nflcom_player_splits": ("Days", "player_splits_L7"),
    "nflcom_player_situational": ("Field Position", "player_situational_L7"),
}


def nflcom_spec():
    """Return the one literal NFL.com spec; no test derives an alternative contract."""
    return next(spec for spec in load_specs() if spec.spec_id == "nflcom-l7-fg-buckets-v1")


def write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    connection = duckdb.connect()
    try:
        ddl = ", ".join(f'"{column}" VARCHAR' for column in columns)
        connection.execute(f"CREATE TABLE fixture ({ddl})")
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(f"INSERT INTO fixture VALUES ({placeholders})", rows)
        connection.execute(f"COPY fixture TO '{path.as_posix()}' (FORMAT PARQUET)")
    finally:
        connection.close()


def source_row(source: str, *, slug: str = "good-kicker", values: dict[str, object] | None = None,
               table: str | None = None, layout: str | None = None,
               split_value: str = "Sundays") -> dict[str, object]:
    expected_table, expected_layout = ROUTES[source]
    row: dict[str, object] = {
        "nflcom_slug": slug,
        "season": "2025",
        "_table": table or expected_table,
        "_layout": layout or expected_layout,
        "split_value": split_value,
        "player": "A Name That Must Not Be Used For Matching",
        "1_19": "3-4",
        "20_29": "0-2",
        "30_39": "12-12",
        "40_49": "1-3",
        "50_59": "2-5",
        "60": "0-1",
    }
    row.update(values or {})
    return row


def fixture_paths(tmp_path: Path, rows_by_source: dict[str, list[dict[str, object]]] | None = None,
                  crosswalk_rows: list[tuple[object, object]] | None = None) -> LanePaths:
    rows_by_source = rows_by_source or {
        source: [source_row(source)] for source in ROUTES
    }
    source_paths: dict[str, Path] = {}
    for source, rows in rows_by_source.items():
        path = tmp_path / f"{source}.parquet"
        columns = tuple(dict.fromkeys(
            ("nflcom_slug", "season", "_table", "_layout", "split_value", "player", *BUCKETS,
             *(key for row in rows for key in row))
        ))
        write_parquet(path, columns, [tuple(row.get(column) for column in columns) for row in rows])
        source_paths[source] = path

    crosswalk = tmp_path / "nflcom_slug_pfrid.parquet"
    write_parquet(
        crosswalk,
        ("nflcom_slug", "pfr_id"),
        crosswalk_rows if crosswalk_rows is not None else [("good-kicker", "KickerGo00")],
    )
    source_paths["nflcom_slug_pfrid"] = crosswalk
    return LanePaths(
        dossier=tmp_path / "dossier.json",
        v26=tmp_path / "v26.parquet",
        player_bio=tmp_path / "player_bio.parquet",
        receipt=tmp_path / "receipt.json",
        source_paths=source_paths,
    )


@pytest.mark.parametrize(("raw", "made", "missed"), [
    ("3-4", 3, 1),
    ("0-0", 0, 0),
    ("12-12", 12, 0),
])
def test_parse_made_attempted(raw: str, made: int, missed: int) -> None:
    """Catches loss of the attempted-minus-made miss derivation."""
    assert parse_made_attempted(raw) == (made, missed)


@pytest.mark.parametrize("raw", ["", "3", "3/4", "5-4", "x-y", None])
def test_parse_made_attempted_rejects_malformed_or_negative(raw: object) -> None:
    """Catches coercion of malformed values or negative derived misses into valid facts."""
    with pytest.raises(ValueError):
        parse_made_attempted(raw)  # type: ignore[arg-type]


def test_executor_emits_the_twelve_exact_targets_for_both_registered_l7_relations(tmp_path: Path) -> None:
    """Catches a bucket map that drops a made/missed target or routes either L7 relation away."""
    paths = fixture_paths(tmp_path)

    result = execute_nflcom_l7(nflcom_spec(), paths)

    assert result.errors == ()
    assert result.rejected_rows == 0
    assert result.scalar_targets == frozenset(TARGETS)
    assert all(observation.kind == "SCALAR_OBSERVATION" for observation in result.observations)
    assert all(
        observation.status == "NORMALIZED_PENDING_GRAIN_RECONCILIATION"
        for observation in result.observations
    )
    assert {observation.source for observation in result.observations} == set(ROUTES)
    values_by_target = {
        target: sorted(observation.value for observation in result.observations if observation.targets == (target,))
        for target in TARGETS
    }
    assert values_by_target == {
        "fg_made_0_19": [3, 3], "fg_missed_0_19": [1, 1],
        "fg_made_20_29": [0, 0], "fg_missed_20_29": [2, 2],
        "fg_made_30_39": [12, 12], "fg_missed_30_39": [0, 0],
        "fg_made_40_49": [1, 1], "fg_missed_40_49": [2, 2],
        "fg_made_50_59": [2, 2], "fg_missed_50_59": [3, 3],
        "fg_made_60_": [0, 0], "fg_missed_60_": [1, 1],
    }
    evidence = result.observations[0].evidence
    assert dict(evidence)["raw_value"] == "3-4"
    assert dict(evidence)["pfr_id"] == "KickerGo00"


@pytest.mark.parametrize("change", [
    {"derivation": "guessed-made-attempted"},
    {"sources": ("nflcom_player_splits",)},
    {"targets": TARGETS[:-1]},
    {"kind": "CONSTRAINT_OBSERVATION"},
])
def test_executor_refuses_a_near_miss_spec_instead_of_widening_the_closed_contract(
    tmp_path: Path, change: dict[str, object]
) -> None:
    """Catches a generic executor accepting altered derivations, sources, targets, or kind."""
    result = execute_nflcom_l7(replace(nflcom_spec(), **change), fixture_paths(tmp_path))

    assert result.observations == ()
    assert result.rejected_rows == 1
    assert any(error.startswith("unauthorized spec:") for error in result.errors)


@pytest.mark.parametrize(("source", "row", "expected"), [
    ("nflcom_player_splits", source_row("nflcom_player_splits", table="Not A Contract Route"), "route"),
    ("nflcom_player_situational", source_row("nflcom_player_situational", layout="not_authorized_L7"), "route"),
    ("nflcom_player_splits", source_row("nflcom_player_splits", values={"70": "1-1"}), "schema"),
])
def test_executor_rejects_unenumerated_l7_routes_and_buckets(
    tmp_path: Path, source: str, row: dict[str, object], expected: str
) -> None:
    """Catches an L7 table/layout/bucket outside the literal 78-row authorization becoming PASS."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows[source] = [row]
    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.rejected_rows == 1
    assert any(error.startswith(f"{expected}:") for error in result.errors)
    assert not any(observation.status == "PASS" for observation in result.observations)


@pytest.mark.parametrize(("value", "failure"), [
    (None, "malformed:"),
    ("made-four", "malformed:"),
    ("5-4", "malformed:"),
])
def test_executor_reports_null_malformed_and_negative_bucket_values_without_a_pass(
    tmp_path: Path, value: object, failure: str
) -> None:
    """Catches invalid raw L7 values being silently ignored while a receipt-like result passes."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [source_row("nflcom_player_splits", values={"1_19": value})]
    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.rejected_rows == 1
    assert any(error.startswith(failure) for error in result.errors)
    assert not any(observation.status == "PASS" for observation in result.observations)


@pytest.mark.parametrize(("component", "value"), [
    ("nflcom_slug", None),
    ("nflcom_slug", ""),
    ("season", None),
    ("season", ""),
    ("_table", None),
    ("_table", ""),
    ("_layout", None),
    ("_layout", ""),
    ("split_value", None),
    ("split_value", ""),
])
def test_executor_requires_every_source_grain_component_to_be_nonempty(
    tmp_path: Path, component: str, value: object
) -> None:
    """Catches a null or blank locator becoming a non-reconcilable normalized observation."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [
        source_row("nflcom_player_splits", values={component: value})
    ]

    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.observations == ()
    assert result.rejected_rows == 1
    assert any(error.startswith("grain:") for error in result.errors)


def test_executor_rejects_populated_non_bucket_columns_outside_the_explicit_l7_schema(tmp_path: Path) -> None:
    """Catches source-schema drift such as a populated `60_plus` column being silently omitted."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [
        source_row("nflcom_player_splits", values={"60_plus": "1-1"})
    ]

    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.observations == ()
    assert result.rejected_rows == 1
    assert any(error.startswith("schema:") and "60_plus" in error for error in result.errors)


def test_executor_limits_candidates_to_the_resolved_crosswalk_without_name_fallback(tmp_path: Path) -> None:
    """Catches an excluded slug being fused by name or poisoning the admitted identity set."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [source_row("nflcom_player_splits", slug="unresolved-slug")]
    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.rejected_rows == 0
    assert result.errors == ()
    assert len(result.observations) == 12
    assert {dict(item.evidence)["nflcom_slug"] for item in result.observations} == {"good-kicker"}


def test_unlabeled_all_zero_stadium_rows_are_outside_named_split_contract(tmp_path: Path) -> None:
    """Catches an unlabeled aggregate-like Stadium row becoming a fake split grain."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [source_row(
        "nflcom_player_splits",
        table="Stadiums",
        split_value="",
        values={bucket: "0-0" for bucket in BUCKETS},
    )]

    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.rejected_rows == 0
    assert result.errors == ()
    assert len(result.observations) == 12


def test_executor_collapses_exact_duplicate_rows_without_inflating_observations(tmp_path: Path) -> None:
    """Catches retained byte-identical rows inflating normalized pending observations."""
    rows = {name: [source_row(name)] for name in ROUTES}
    duplicate = source_row("nflcom_player_splits")
    rows["nflcom_player_splits"] = [duplicate, duplicate]
    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.errors == ()
    assert result.rejected_rows == 0
    assert len(result.observations) == 24


def test_executor_rejects_conflicting_duplicate_source_grain(tmp_path: Path) -> None:
    """Catches two different bucket claims at one grain being silently deduplicated."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [
        source_row("nflcom_player_splits"),
        source_row("nflcom_player_splits", values={"1_19": "2-2"}),
    ]
    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.observations == ()
    assert result.rejected_rows == 2
    assert any(error.startswith("duplicate grain:") for error in result.errors)


def test_executor_allows_known_l7_context_columns_without_observing_them(tmp_path: Path) -> None:
    """Catches the retained G/FGM/FG_ATT context blocking the six enrolled buckets."""
    rows = {name: [source_row(name)] for name in ROUTES}
    rows["nflcom_player_splits"] = [source_row(
        "nflcom_player_splits", values={"g": "16", "fgm": "18", "fg_att": "25"}
    )]

    result = execute_nflcom_l7(nflcom_spec(), fixture_paths(tmp_path, rows))

    assert result.errors == ()
    assert result.rejected_rows == 0
    assert result.scalar_targets == frozenset(TARGETS)


def test_executor_is_registered_by_its_closed_cohort_name() -> None:
    """Catches the core dispatch map omitting the NFL.com executor after the module exists."""
    assert EXECUTORS["nflcom_l7"] is execute_nflcom_l7


@pytest.mark.parametrize(("observation", "rejected_rows", "message"), [
    (LaneObservation(
        "nflcom-l7", "nflcom", "SCALAR_OBSERVATION", ("fg_made_0_19",),
        1, 0, "NORMALIZED_PENDING_GRAIN_RECONCILIATION",
    ), 0, "non-PASS observation"),
    (LaneObservation(
        "rejected", "fixture", "SCALAR_OBSERVATION", ("fixture_target",), 1, 1, "PASS",
    ), 1, "rejected rows"),
])
def test_receipt_writer_refuses_pending_or_rejected_observations(
    tmp_path: Path, observation: LaneObservation, rejected_rows: int, message: str
) -> None:
    """Catches a pending or rejected lane result minting a PASS licensing receipt."""
    artifact = tmp_path / "source.json"
    artifact.write_text('{"source": "fixture"}', encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        write_receipt(
            tmp_path / "receipt.json",
            contract_hash="a" * 64,
            source_manifest_hash=artifact_manifest_sha256((artifact,)),
            executor_version="fixture-v1",
            crosswalk_receipts={"fixture_crosswalk": "PASS"},
            observations=(observation,),
            compared_rows=1,
            rejected_rows=rejected_rows,
        )
