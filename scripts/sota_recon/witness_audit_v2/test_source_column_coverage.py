from scripts.sota_recon.witness_audit_v2.source_column_coverage import (
    density,
    parquet_coverage,
    summarize_counts,
)


def test_density_returns_null_for_zero_denominator():
    assert density(3, 0) is None


def test_density_uses_float_ratio():
    assert density(3, 10) == 0.3


def test_summarize_counts_keeps_zeroes_separate_from_nulls():
    result = summarize_counts([None, 0, 0, 5], eligible=[True, True, True, True])
    assert result["total_rows"] == 4
    assert result["non_null_rows"] == 3
    assert result["explicit_zero_rows"] == 2
    assert result["nonzero_rows"] == 1
    assert result["all_row_density"] == 0.75
    assert result["signal_density"] == 0.25


def test_summarize_counts_uses_eligible_denominator_when_supplied():
    result = summarize_counts([None, 0, 5, 7], eligible=[True, True, True, False])
    assert result["eligible_rows"] == 3
    assert result["eligible_row_density"] == 0.6666666666666666


def test_parquet_coverage_measures_null_zero_signal_and_year(tmp_path):
    import duckdb

    path = tmp_path / "stats.parquet"
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT * FROM (VALUES (2020, 1), (2020, 0), (2021, NULL)) t(year, col1)) TO ? (FORMAT PARQUET)",
        [str(path)],
    )
    con.close()
    source = {"source_id": "fixture", "path": str(path)}
    columns = [{"source_id": "fixture", "raw_column": "col1"}]
    rows = parquet_coverage(source, columns)
    assert len(rows) == 2
    by_year = {row["year"]: row for row in rows}
    assert by_year[2020]["non_null_rows"] == 2
    assert by_year[2020]["explicit_zero_rows"] == 1
    assert by_year[2020]["nonzero_rows"] == 1
    assert by_year[2021]["non_null_rows"] == 0
    assert by_year[2021]["year_start_observed_nonnull"] == 2020


def test_year_to_int_normalizes_double_year_columns():
    from scripts.sota_recon.witness_audit_v2.source_column_coverage import year_to_int

    assert year_to_int(1920) == 1920
    assert year_to_int(1920.0) == 1920
    assert year_to_int("1920") == 1920
    assert year_to_int("1920.0") == 1920
    assert year_to_int(1920.5) is None
    assert year_to_int("n/a") is None
    assert year_to_int(None) is None


def test_annotate_keeps_float_year_coverage_rows():
    # regression: the O.2 146-canonical year-range drift -- DOUBLE year columns (ancient bundle,
    # legacy supertable) produced float years that isinstance(int) silently dropped, collapsing
    # coverage unions to integer-year sources (pbp rollups 1978+).
    from scripts.sota_recon.witness_audit_v2.source_column_coverage import annotate_semantic_coverage

    observations = [
        {"source_id": "ancient", "raw_column": "carries", "canonical_column": "carries"},
        {"source_id": "pbp", "raw_column": "carries", "canonical_column": "carries"},
    ]
    coverage = [
        {"source_id": "ancient", "raw_column": "carries", "year": 1934.0, "non_null_rows": 10, "nonzero_rows": 9},
        {"source_id": "pbp", "raw_column": "carries", "year": 1978, "non_null_rows": 10, "nonzero_rows": 9},
    ]
    rows = annotate_semantic_coverage(observations, coverage)
    union = rows[0]["coverage_union_years"]
    assert union == [1934, 1978]
