"""Behavior tests for PFR's constraint-only two-point season total."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from scripts.sota_recon.composite_witness_lane import LanePaths, load_specs
from scripts.sota_recon.composite_witness_pfr_two_pt import execute_pfr_two_point


TARGETS = (
    "passing_2pt_conversions",
    "receiving_2pt_conversions",
    "rushing_2pt_conversions",
)
SOURCE_COLUMNS = ("pfr_id", "year_id", "two_pt_md")
BIO_COLUMNS = ("pfr_id", "NFL_player_id")
CANONICAL_COLUMNS = ("NFL_player_id", "year", "week", "season_type", *TARGETS)


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        definitions = ", ".join(f'"{column}" VARCHAR' for column in columns)
        con.execute(f"CREATE TABLE facts ({definitions})")
        if rows:
            con.executemany(
                f"INSERT INTO facts VALUES ({', '.join('?' for _ in columns)})", rows
            )
        con.execute("COPY facts TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def _source_row(pfr_id: str = "player01", year: str = "2024", total: object = 3) -> tuple[object, ...]:
    return (pfr_id, year, total)


def _canonical_row(
    nfl_id: str = "nfl-player", season_type: str = "REG", week: int = 1, **changes: object
) -> tuple[object, ...]:
    values: dict[str, object] = {
        "NFL_player_id": nfl_id,
        "year": 2024,
        "week": week,
        "season_type": season_type,
        "passing_2pt_conversions": 0,
        "receiving_2pt_conversions": 0,
        "rushing_2pt_conversions": 0,
    }
    values.update(changes)
    return tuple(values[column] for column in CANONICAL_COLUMNS)


def _canonical_rows() -> list[tuple[object, ...]]:
    return [
        _canonical_row(week=1, passing_2pt_conversions=1, receiving_2pt_conversions=1),
        _canonical_row(week=2, rushing_2pt_conversions=1),
        _canonical_row(season_type="POST", week=1, receiving_2pt_conversions=1),
    ]


def _paths(
    tmp_path: Path,
    *,
    regular: list[tuple[object, ...]] | None = None,
    postseason: list[tuple[object, ...]] | None = None,
    bio: list[tuple[object, ...]] | None = None,
    canonical: list[tuple[object, ...]] | None = None,
) -> LanePaths:
    regular_path, postseason_path, bio_path, canonical_path = (
        tmp_path / "regular.parquet",
        tmp_path / "postseason.parquet",
        tmp_path / "bio.parquet",
        tmp_path / "canonical.parquet",
    )
    _write_parquet(regular_path, SOURCE_COLUMNS, regular if regular is not None else [_source_row()])
    _write_parquet(postseason_path, SOURCE_COLUMNS, postseason if postseason is not None else [_source_row(total=1)])
    _write_parquet(bio_path, BIO_COLUMNS, bio if bio is not None else [("player01", "nfl-player")])
    _write_parquet(canonical_path, CANONICAL_COLUMNS, canonical if canonical is not None else _canonical_rows())
    return LanePaths(
        tmp_path / "dossier.json",
        canonical_path,
        bio_path,
        tmp_path / "receipt.json",
        {"pfr_player_scoring": regular_path, "pfr_scoring_post": postseason_path},
    )


@pytest.fixture
def paths(tmp_path: Path) -> LanePaths:
    return _paths(tmp_path)


@pytest.fixture
def spec():
    return next(item for item in load_specs() if item.spec_id == "pfr-two-point-total-v1")


def alter_canonical_total(paths: LanePaths, **changes: object) -> None:
    rows = _canonical_rows()
    rows[0] = _canonical_row(week=1, **changes)
    rows[1] = _canonical_row(week=2)
    _write_parquet(paths.v26, CANONICAL_COLUMNS, rows)


def test_two_point_total_is_constraint_only(paths: LanePaths, spec):
    """Catches a constraint total minting scalar/component witness depth."""
    result = execute_pfr_two_point(spec, paths)

    assert result.errors == ()
    assert result.scalar_targets == frozenset()
    assert [(o.targets, o.kind, o.status) for o in result.observations] == [
        (TARGETS, "CONSTRAINT_OBSERVATION", "PASS"),
        (TARGETS, "CONSTRAINT_OBSERVATION", "PASS"),
    ]
    assert [o.compared_rows for o in result.observations] == [2, 1]


def test_unpopulated_source_and_unrelated_canonical_rows_are_outside_candidate_surface(
    tmp_path: Path, spec
) -> None:
    """Catches null non-candidates in full scoring universes poisoning a typed total."""
    paths = _paths(
        tmp_path,
        regular=[_source_row(), _source_row("non-candidate", total=None)],
        bio=[("player01", "nfl-player"), ("non-candidate", "unrelated")],
        canonical=[*_canonical_rows(), _canonical_row("unrelated", week=1, **{
            target: None for target in TARGETS
        })],
    )

    result = execute_pfr_two_point(spec, paths)

    assert result.errors == ()
    assert [item.status for item in result.observations] == ["PASS", "PASS"]


def test_two_point_mismatch_returns_no_pass_observation(paths: LanePaths, spec):
    """Catches a partial comparison leaving any passing result after mismatch."""
    alter_canonical_total(paths, passing_2pt_conversions=2, receiving_2pt_conversions=0, rushing_2pt_conversions=0)
    result = execute_pfr_two_point(spec, paths)

    assert result.observations[0].status == "MISMATCH"
    assert {item.status for item in result.observations} == {"MISMATCH"}
    assert result.scalar_targets == frozenset()


def test_regular_and_postseason_relations_are_compared_in_isolation(paths: LanePaths, spec):
    """Catches a REG/POST merge or an unauthorized cross-season fallback."""
    result = execute_pfr_two_point(spec, paths)

    assert [(item.source, dict(item.evidence)["season_type"], item.value) for item in result.observations] == [
        ("pfr_player_scoring", "REG", 3),
        ("pfr_scoring_post", "POST", 1),
    ]


def test_malformed_non_null_source_total_blocks_every_observation(tmp_path: Path, spec):
    """Catches treating a malformed published candidate total as zero or dropping it."""
    result = execute_pfr_two_point(spec, _paths(tmp_path, regular=[_source_row(total="not-a-number")]))

    assert any("null/non-integral" in error for error in result.errors)
    assert result.observations == ()


def test_duplicate_pfr_season_grain_blocks_every_observation(tmp_path: Path, spec):
    """Catches duplicate PFR player-season totals being double-counted."""
    result = execute_pfr_two_point(spec, _paths(tmp_path, regular=[_source_row(), _source_row()]))

    assert any("duplicate grain" in error for error in result.errors)
    assert result.observations == ()


def test_explicit_multi_team_total_is_selected_over_component_rows(tmp_path: Path, spec):
    """Catches rejecting or summing PFR's explicit 2TM total with team components."""
    paths = _paths(tmp_path)
    _write_parquet(
        paths.source_paths["pfr_player_scoring"],
        (*SOURCE_COLUMNS, "team_name_abbr"),
        [(*_source_row(total=3), "2TM"), (*_source_row(total=1), "AAA")],
    )

    result = execute_pfr_two_point(spec, paths)

    assert result.errors == ()
    assert [item.status for item in result.observations] == ["PASS", "PASS"]


def test_conflicting_duplicate_canonical_player_game_grain_blocks_every_observation(tmp_path: Path, spec):
    """Catches conflicting canonical player-game rows being summed twice."""
    canonical = _canonical_rows()
    canonical.append(_canonical_row(week=1, passing_2pt_conversions=1, receiving_2pt_conversions=0))
    result = execute_pfr_two_point(spec, _paths(tmp_path, canonical=canonical))

    assert any("duplicate canonical grain" in error for error in result.errors)
    assert result.observations == ()


def test_exact_projected_canonical_duplicates_collapse_without_inflation(tmp_path: Path, spec):
    """Catches byte-equivalent canonical copies being mistaken for conflicting facts."""
    canonical = _canonical_rows()
    canonical.append(canonical[0])

    result = execute_pfr_two_point(spec, _paths(tmp_path, canonical=canonical))

    assert result.errors == ()
    assert [item.status for item in result.observations] == ["PASS", "PASS"]


def test_unresolved_and_non_bijective_identity_block_every_observation(tmp_path: Path, spec):
    """Catches name fallback or accepting an ambiguous PFR identity bridge."""
    unresolved = execute_pfr_two_point(spec, _paths(tmp_path / "unresolved", bio=[]))
    ambiguous = execute_pfr_two_point(
        spec,
        _paths(tmp_path / "ambiguous", bio=[("player01", "nfl-player"), ("player01", "other-player")]),
    )

    assert any("unresolved identity" in error for error in unresolved.errors)
    assert unresolved.observations == ()
    assert any("one-to-one" in error for error in ambiguous.errors)
    assert ambiguous.observations == ()


def test_crosswalk_excluded_zero_total_has_no_positive_constraint_fact(tmp_path: Path, spec):
    """Catches a crosswalk-excluded zero row poisoning the admitted positive surface."""
    paths = _paths(tmp_path, regular=[_source_row(), _source_row("zero-only", total=0)])

    result = execute_pfr_two_point(spec, paths)

    assert result.errors == ()
    assert [item.status for item in result.observations] == ["PASS", "PASS"]


def test_missing_canonical_zero_total_is_outside_positive_constraint_surface(tmp_path: Path, spec):
    """Catches a zero total with no game rows poisoning valid positive comparisons."""
    paths = _paths(
        tmp_path,
        regular=[_source_row(), _source_row("zero-only", total=0)],
        bio=[("player01", "nfl-player"), ("zero-only", "zero-nfl")],
    )

    result = execute_pfr_two_point(spec, paths)

    assert result.errors == ()
    assert [item.status for item in result.observations] == ["PASS", "PASS"]


def test_missing_canonical_positive_total_remains_a_blocker(tmp_path: Path, spec):
    """Catches a positive total being hidden by zero-row coverage scoping."""
    paths = _paths(
        tmp_path,
        regular=[_source_row(), _source_row("positive-only", total=1)],
        bio=[("player01", "nfl-player"), ("positive-only", "positive-nfl")],
    )

    result = execute_pfr_two_point(spec, paths)

    assert any("no canonical player-season aggregate" in error for error in result.errors)
    assert result.observations == ()


def test_negative_source_total_blocks_every_observation(tmp_path: Path, spec):
    """Catches a negative aggregate source total becoming a passing constraint."""
    result = execute_pfr_two_point(spec, _paths(tmp_path, regular=[_source_row(total=-1)]))

    assert any("below zero" in error for error in result.errors)
    assert result.observations == ()
