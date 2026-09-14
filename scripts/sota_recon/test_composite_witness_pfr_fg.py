"""Behavior tests for PFR's paired field-goal bucket witness.

The fixtures are deliberately local Parquet relations.  Expected counts are
hand-written so a mirrored arithmetic helper cannot make these tests pass.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from scripts.sota_recon.composite_witness_lane import LanePaths, load_specs
from scripts.sota_recon.composite_witness_pfr_fg import execute_pfr_field_goals


TARGETS = (
    "fg_made_0_19", "fg_missed_0_19", "fg_made_20_29", "fg_missed_20_29",
    "fg_made_30_39", "fg_missed_30_39", "fg_made_40_49", "fg_missed_40_49",
    "fg_made_50_59", "fg_made_60_", "fg_missed_50_59", "fg_missed_60_",
)


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    con = duckdb.connect()
    try:
        definitions = ", ".join(f'"{column}" VARCHAR' for column in columns)
        con.execute(f"CREATE TABLE facts ({definitions})")
        placeholders = ", ".join("?" for _ in columns)
        con.executemany(f"INSERT INTO facts VALUES ({placeholders})", rows)
        con.execute("COPY facts TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def _source_row(pfr_id: str = "kicker01", year: str = "2024", **changes: object) -> tuple[object, ...]:
    values: dict[str, object] = {
        "pfr_id": pfr_id, "year_id": year,
        "fgm1": 3, "fga1": 5, "fgm2": 4, "fga2": 5,
        "fgm3": 2, "fga3": 4, "fgm4": 1, "fga4": 3,
        "fgm5": 4, "fga5": 6,
    }
    values.update(changes)
    return tuple(values[column] for column in _SOURCE_COLUMNS)


_SOURCE_COLUMNS = ("pfr_id", "year_id", "fgm1", "fga1", "fgm2", "fga2", "fgm3", "fga3", "fgm4", "fga4", "fgm5", "fga5")
_BIO_COLUMNS = ("pfr_id", "NFL_player_id")
_CANONICAL_COLUMNS = ("NFL_player_id", "year", "week", "season_type", *TARGETS)


def _canonical_row(nfl_id: str, season_type: str, week: int, **changes: int) -> tuple[object, ...]:
    values: dict[str, object] = {column: 0 for column in TARGETS}
    values.update({"NFL_player_id": nfl_id, "year": 2024, "week": week, "season_type": season_type})
    values.update(changes)
    return tuple(values[column] for column in _CANONICAL_COLUMNS)


def _default_canonical() -> list[tuple[object, ...]]:
    return [
        # Two game rows independently sum to the regular PFR season row.
        _canonical_row("nfl-kicker", "REG", 1, fg_made_0_19=2, fg_missed_0_19=1, fg_made_20_29=2, fg_missed_20_29=1, fg_made_30_39=1, fg_missed_30_39=1, fg_made_40_49=1, fg_missed_40_49=1, fg_made_50_59=2, fg_made_60_=1, fg_missed_50_59=1, fg_missed_60_=1),
        _canonical_row("nfl-kicker", "REG", 2, fg_made_0_19=1, fg_missed_0_19=1, fg_made_20_29=2, fg_made_30_39=1, fg_missed_30_39=1, fg_missed_40_49=1, fg_made_50_59=1),
        _canonical_row("nfl-kicker", "POST", 1, fg_made_0_19=1, fg_missed_0_19=1, fg_made_20_29=1, fg_made_30_39=1, fg_missed_30_39=1, fg_made_40_49=1, fg_made_50_59=1, fg_missed_50_59=1),
    ]


def _paths(tmp_path: Path, *, regular: list[tuple[object, ...]] | None = None,
           postseason: list[tuple[object, ...]] | None = None,
           bio: list[tuple[object, ...]] | None = None,
           canonical: list[tuple[object, ...]] | None = None) -> LanePaths:
    regular_path, post_path, bio_path, canonical_path = (
        tmp_path / "regular.parquet", tmp_path / "post.parquet", tmp_path / "bio.parquet", tmp_path / "canonical.parquet"
    )
    _write_parquet(regular_path, _SOURCE_COLUMNS, regular or [_source_row()])
    _write_parquet(post_path, _SOURCE_COLUMNS, postseason or [_source_row(fgm1=1, fga1=2, fgm2=1, fga2=1, fgm3=1, fga3=2, fgm4=1, fga4=1, fgm5=1, fga5=2)])
    _write_parquet(bio_path, _BIO_COLUMNS, bio or [("kicker01", "nfl-kicker")])
    canonical_rows = canonical or _default_canonical()
    _write_parquet(canonical_path, _CANONICAL_COLUMNS, canonical_rows)
    return LanePaths(tmp_path / "dossier.json", canonical_path, bio_path, tmp_path / "receipt.json", {
        "pfr_player_kicking": regular_path, "pfr_kicking_post": post_path,
    })


def _specs() -> tuple[object, object]:
    specs = {spec.spec_id: spec for spec in load_specs()}
    return specs["pfr-field-goals-buckets-1-4-v1"], specs["pfr-field-goals-50-plus-v1"]


def _execute(paths: LanePaths):
    scalar, constraint = _specs()
    return execute_pfr_field_goals(scalar, paths), execute_pfr_field_goals(constraint, paths)


def _observation(result, source: str, target: str):
    return next(item for item in result.observations if item.source == source and item.targets == (target,))


def _constraint_ids(result) -> set[str]:
    return {
        dict(item.evidence)["constraint_id"]
        for item in result.observations
        if item.kind == "CONSTRAINT_OBSERVATION"
    }


def test_paired_buckets_compare_all_regular_scalar_targets_to_aggregated_games(tmp_path: Path):
    """Catches arithmetic-only PASS values or a missing bucket 1--4 comparison."""
    scalar, _ = _execute(_paths(tmp_path))

    expected = {"fg_made_0_19": 3, "fg_missed_0_19": 2, "fg_made_20_29": 4, "fg_missed_20_29": 1,
                "fg_made_30_39": 2, "fg_missed_30_39": 2, "fg_made_40_49": 1, "fg_missed_40_49": 2}
    assert {target: _observation(scalar, "pfr_player_kicking", target).value for target in expected} == expected
    assert all(_observation(scalar, "pfr_player_kicking", target).status == "PASS" for target in expected)
    assert _observation(scalar, "pfr_player_kicking", "fg_made_0_19").compared_rows == 2


def test_regular_and_postseason_relations_are_compared_in_isolation(tmp_path: Path):
    """Catches a regular/postseason coalesce or cross-season fallback."""
    scalar, _ = _execute(_paths(tmp_path))

    assert _observation(scalar, "pfr_player_kicking", "fg_made_0_19").value == 3
    assert _observation(scalar, "pfr_kicking_post", "fg_made_0_19").value == 1
    assert {item.status for item in scalar.observations} == {"PASS"}


def test_50_plus_emits_only_two_exact_constraint_comparisons(tmp_path: Path):
    """Catches combined 50+ totals being credited to either scalar component."""
    _, constraints = _execute(_paths(tmp_path))

    assert constraints.scalar_targets.isdisjoint({"fg_made_50_59", "fg_made_60_", "fg_missed_50_59", "fg_missed_60_"})
    assert _constraint_ids(constraints) == {"pfr_fg_50_plus_made", "pfr_fg_50_plus_missed"}
    assert {item.status for item in constraints.observations} == {"PASS"}


def test_null_paired_operand_blocks_all_scalar_passes(tmp_path: Path):
    """Catches treating a null PFR attempt as a zero or silently dropping it."""
    scalar, _ = _execute(_paths(tmp_path, regular=[_source_row(fga3=None)]))

    assert scalar.errors and any("null operand" in error for error in scalar.errors)
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_unpopulated_source_and_unrelated_canonical_rows_are_outside_candidate_surface(tmp_path: Path):
    """Catches null non-candidates in the full local universes poisoning an enrolled kicker."""
    blank = _source_row(pfr_id="non-candidate", **{
        column: None for column in _SOURCE_COLUMNS if column not in {"pfr_id", "year_id"}
    })
    unrelated = _canonical_row("unrelated", "REG", 1, **{column: None for column in TARGETS})
    paths = _paths(
        tmp_path,
        regular=[_source_row(), blank],
        bio=[("kicker01", "nfl-kicker"), ("non-candidate", "unrelated")],
        canonical=[*_default_canonical(), unrelated],
    )

    scalar, constraint = _execute(paths)

    assert scalar.errors == ()
    assert constraint.errors == ()
    assert {item.status for item in (*scalar.observations, *constraint.observations)} == {"PASS"}


def test_missing_canonical_all_zero_row_is_outside_positive_comparison_surface(tmp_path: Path):
    """Catches an all-zero uncovered source season poisoning valid positive comparisons."""
    zero = _source_row("zero-kicker", **{
        column: 0 for column in _SOURCE_COLUMNS if column not in {"pfr_id", "year_id"}
    })
    paths = _paths(
        tmp_path,
        regular=[_source_row(), zero],
        bio=[("kicker01", "nfl-kicker"), ("zero-kicker", "zero-nfl")],
    )

    scalar, constraint = _execute(paths)

    assert scalar.errors == ()
    assert constraint.errors == ()


def test_missing_canonical_positive_row_remains_a_blocker(tmp_path: Path):
    """Catches a positive external operand being hidden by zero-row scoping."""
    positive = _source_row("positive-kicker", fgm4=0, fga4=1)
    paths = _paths(
        tmp_path,
        regular=[_source_row(), positive],
        bio=[("kicker01", "nfl-kicker"), ("positive-kicker", "positive-nfl")],
    )

    scalar, _ = _execute(paths)

    assert any("no canonical player-season aggregate" in error for error in scalar.errors)
    assert scalar.observations == ()


def test_attempts_less_than_makes_blocks_all_scalar_passes(tmp_path: Path):
    """Catches negative missed counts escaping paired-operand validation."""
    scalar, _ = _execute(_paths(tmp_path, regular=[_source_row(fgm4=4, fga4=3)]))

    assert any("less than made" in error for error in scalar.errors)
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_duplicate_pfr_season_grain_blocks_all_scalar_passes(tmp_path: Path):
    """Catches duplicate PFR player-season operands being double-counted."""
    scalar, _ = _execute(_paths(tmp_path, regular=[_source_row(), _source_row()]))

    assert any("duplicate grain" in error for error in scalar.errors)
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_explicit_multi_team_total_is_selected_over_component_rows(tmp_path: Path):
    """Catches rejecting or summing PFR's explicit 2TM total with team components."""
    paths = _paths(tmp_path)
    columns = (*_SOURCE_COLUMNS, "team_name_abbr")
    total = (*_source_row(), "2TM")
    component = (*_source_row(fgm1=1, fga1=1, fgm2=1, fga2=1), "AAA")
    _write_parquet(paths.source_paths["pfr_player_kicking"], columns, [total, component])

    scalar, constraint = _execute(paths)

    assert scalar.errors == ()
    assert constraint.errors == ()
    assert {item.status for item in (*scalar.observations, *constraint.observations)} == {"PASS"}


def test_unresolved_identity_never_falls_back_to_player_name(tmp_path: Path):
    """Catches a tempting display-name join when no passing bio identity exists."""
    scalar, _ = _execute(_paths(tmp_path, regular=[_source_row(pfr_id="same-name")]))

    assert any("unresolved identity" in error for error in scalar.errors)
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_non_bijective_bio_identity_blocks_all_scalar_passes(tmp_path: Path):
    """Catches accepting a PFR id that resolves to multiple NFL player ids."""
    scalar, _ = _execute(_paths(tmp_path, bio=[("kicker01", "nfl-kicker"), ("kicker01", "other-kicker")]))

    assert any("one-to-one" in error for error in scalar.errors)
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_scalar_mismatch_is_reported_and_cannot_mint_a_pass(tmp_path: Path):
    """Catches status PASS being assigned after arithmetic without canonical equality."""
    rows = _default_canonical()
    rows[0] = _canonical_row("nfl-kicker", "REG", 1, fg_made_0_19=99, fg_missed_0_19=1, fg_made_20_29=2, fg_missed_20_29=1, fg_made_30_39=1, fg_missed_30_39=1, fg_made_40_49=1, fg_missed_40_49=1, fg_made_50_59=2, fg_made_60_=1, fg_missed_50_59=1, fg_missed_60_=1)
    scalar, _ = _execute(_paths(tmp_path, canonical=rows))

    assert _observation(scalar, "pfr_player_kicking", "fg_made_0_19").status == "MISMATCH"
    assert not any(item.status == "PASS" for item in scalar.observations)


def test_constraint_mismatch_is_reported_without_scalar_credit(tmp_path: Path):
    """Catches a matching component or a mismatched 50+ total becoming scalar credit."""
    rows = _default_canonical()
    rows[0] = _canonical_row("nfl-kicker", "REG", 1, fg_made_0_19=2, fg_missed_0_19=1, fg_made_20_29=2, fg_missed_20_29=1, fg_made_30_39=1, fg_missed_30_39=1, fg_made_40_49=1, fg_missed_40_49=1, fg_made_50_59=9, fg_made_60_=1, fg_missed_50_59=1, fg_missed_60_=1)
    _, constraints = _execute(_paths(tmp_path, canonical=rows))

    assert any(item.status == "MISMATCH" for item in constraints.observations)
    assert constraints.scalar_targets == frozenset()
