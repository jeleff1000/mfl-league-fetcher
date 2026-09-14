"""Density and year-range calculations for source-column observations."""
from __future__ import annotations

from collections.abc import Iterable
import glob


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def year_to_int(value: object) -> int | None:
    """Normalize a year that may arrive as int, integral float (DOUBLE year columns -- ancient
    bundle, legacy supertable), or numeric string. Non-integral / non-numeric -> None.

    Regression receipt (2026-07-25, the O.2 '146-canonical year-range drift'): sources storing
    year as DOUBLE returned 1920.0 from the coverage scan; str(1920.0).isdigit() is False so the
    old normalization left floats, and annotate_semantic_coverage's isinstance(year, int) filter
    dropped every such year -- collapsing e.g. carries' coverage union from 1921+ to 1978+ even
    though the ancient bundle holds 934 pre-1978 nonzero carries rows (data verified intact,
    mtime 2026-07-18). The 07-22 ledger ranges were right; the regenerated ones were wrong."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip()
    try:
        as_float = float(text)
    except ValueError:
        return None
    return int(as_float) if as_float.is_integer() else None


def density(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def summarize_counts(values: Iterable[object], eligible: Iterable[bool] | None = None) -> dict:
    vals = list(values)
    eligibility = list(eligible) if eligible is not None else [True] * len(vals)
    if len(eligibility) != len(vals):
        raise ValueError("eligible mask must match values length")
    total = len(vals)
    eligible_values = [v for v, ok in zip(vals, eligibility) if ok]
    non_null = [v for v in vals if v is not None]
    non_null_eligible = [v for v, ok in zip(vals, eligibility) if ok and v is not None]
    zero = [v for v in vals if v == 0]
    nonzero = [v for v in vals if v is not None and v != 0]
    nonzero_eligible = [v for v, ok in zip(vals, eligibility) if ok and v is not None and v != 0]
    return {
        "total_rows": total,
        "eligible_rows": len(eligible_values),
        "non_null_rows": len(non_null),
        "explicit_zero_rows": len(zero),
        "nonzero_rows": len(nonzero),
        "all_row_density": density(len(non_null), total),
        "eligible_row_density": density(len(non_null_eligible), len(eligible_values)),
        "signal_density": density(len(nonzero_eligible), len(eligible_values)),
    }


def annotate_semantic_coverage(
    observations: list[dict],
    coverage: list[dict],
    declared_year_bounds: dict[tuple[str, str], tuple[int, int]] | None = None,
) -> list[dict]:
    bounds = declared_year_bounds or {}
    by_key = {}
    for row in coverage:
        by_key.setdefault((row.get("source_id"), row.get("raw_column")), []).append(row)
    groups = {}
    for observation in observations:
        semantic = observation.get("canonical_semantic_id") or observation.get("canonical_column")
        groups.setdefault(semantic, []).append(observation)

    output = []
    for semantic, members in groups.items():
        member_years = {}
        for member in members:
            key = (member["source_id"], member["raw_column"])
            member_years[key] = {
                year_to_int(row.get("year"))
                for row in by_key.get(key, [])
                if year_to_int(row.get("year")) is not None
                and not str(row.get("unavailable_reason") or "").startswith("row_scan_error")
                and ((row.get("non_null_rows") or 0) > 0 or (row.get("nonzero_rows") or 0) > 0)
            }
        union = set().union(*member_years.values()) if member_years else set()
        declared = {}
        for key in member_years:
            bounds_value = bounds.get(key)
            if bounds_value and bounds_value[0] <= bounds_value[1]:
                declared[key] = set(range(bounds_value[0], bounds_value[1] + 1))
        expected_union = set().union(*declared.values()) if declared else set(union)
        intersection = set.intersection(*declared.values()) if declared else set()
        true_missing = sorted(expected_union - union)
        for member in members:
            key = (member["source_id"], member["raw_column"])
            expected = declared.get(key, set())
            missing = expected - member_years.get(key, set())
            row = dict(member)
            row.update(
                {
                    "coverage_union_years": sorted(union),
                    "coverage_intersection_years": sorted(intersection),
                    "true_missing_years": true_missing,
                    "source_partial_years": sorted(missing & union),
                    "missing_year_occurrence_count": len(missing),
                }
            )
            output.append(row)
    return output


def contract_coverage(source: dict, column: dict) -> list[dict]:
    by_year = source.get("by_year", {})
    raw = column["raw_column"]
    year_values = {
        int(y): values
        for y, values in by_year.items()
        if str(y).lstrip("-").isdigit() and isinstance(values, dict) and raw in values
    }
    years = sorted(year_values)
    counts = {year: year_values[year].get(raw) for year in years}
    total_rows = source.get("n_rows")
    total_signal = column.get("total_nonzero")
    rows = []
    for year in years:
        signal = counts[year]
        rows.append(
            {
                "source_id": source["source_id"],
                "raw_column": raw,
                "canonical_column": None,
                "year": year,
                "year_start_observed_nonnull": None,
                "year_end_observed_nonnull": None,
                "year_start_observed_nonzero": min((y for y, value in counts.items() if value), default=None),
                "year_end_observed_nonzero": max((y for y, value in counts.items() if value), default=None),
                "total_rows": None,
                "eligible_rows": None,
                "non_null_rows": None,
                "explicit_zero_rows": None,
                "nonzero_rows": signal,
                "all_row_density": None,
                "eligible_row_density": None,
                "signal_density": None,
                "year_coverage_density": None,
                "missing_year": False,
                "sparse_year": signal == 0,
                "unavailable_reason": "contract_signal_counts_only",
            }
        )
    if not rows and total_signal is not None:
        rows.append(
            {
                "source_id": source["source_id"],
                "raw_column": raw,
                "canonical_column": None,
                "year": None,
                "year_start_observed_nonnull": column.get("year_min"),
                "year_end_observed_nonnull": column.get("year_max"),
                "year_start_observed_nonzero": column.get("year_min"),
                "year_end_observed_nonzero": column.get("year_max"),
                "total_rows": total_rows,
                "eligible_rows": None,
                "non_null_rows": None,
                "explicit_zero_rows": None,
                "nonzero_rows": total_signal,
                "all_row_density": None,
                "eligible_row_density": None,
                "signal_density": density(total_signal, total_rows),
                "year_coverage_density": None,
                "missing_year": False,
                "sparse_year": total_signal == 0,
                "unavailable_reason": "contract_signal_counts_only",
            }
        )
    return rows


def parquet_coverage(
    source: dict,
    columns: list[dict],
    connection=None,
) -> list[dict]:
    import duckdb

    path = source.get("path")
    if not path or not str(path).lower().endswith(".parquet"):
        return []
    if not glob.glob(str(path)):
        return []
    wanted = {str(c["raw_column"]): c for c in columns}
    if not wanted:
        return []
    owns_connection = connection is None
    con = connection or duckdb.connect()
    try:
        schema = con.execute("SELECT name, type FROM parquet_schema(?)", [path]).fetchall()
        actual = {str(name): str(dtype) for name, dtype in schema}
        selected = [name for name in wanted if name in actual]
        if not selected:
            return []
        year_name = next((name for name in actual if name.lower() in {"year", "season"}), None)
        group_expr = _quote_identifier(year_name) if year_name else "NULL"
        expressions = ["COUNT(*) AS __total"]
        for index, name in enumerate(selected):
            q = _quote_identifier(name)
            expressions += [
                f"COUNT({q}) AS n_{index}",
                f"SUM(CASE WHEN {q} IS NULL THEN 0 WHEN TRY_CAST({q} AS DOUBLE) = 0 THEN 1 ELSE 0 END) AS z_{index}",
                f"SUM(CASE WHEN TRY_CAST({q} AS DOUBLE) IS NOT NULL AND TRY_CAST({q} AS DOUBLE) <> 0 THEN 1 ELSE 0 END) AS s_{index}",
            ]
        sql = (
            f"SELECT {group_expr} AS __year, {', '.join(expressions)}"
            f" FROM read_parquet(?, union_by_name=true) GROUP BY {group_expr} ORDER BY __year"
        )
        aggregate_rows = con.execute(sql, [path]).fetchall()
    finally:
        if owns_connection:
            con.close()
    output = []
    for row in aggregate_rows:
        year, total = row[0], row[1]
        for index, name in enumerate(selected):
            non_null, zero, signal = row[2 + index * 3 : 5 + index * 3]
            eligible = total
            output.append(
                {
                    "source_id": source["source_id"],
                    "raw_column": name,
                    "canonical_column": None,
                    "year": year_to_int(year) if year_to_int(year) is not None else year,
                    "year_start_observed_nonnull": None,
                    "year_end_observed_nonnull": None,
                    "year_start_observed_nonzero": None,
                    "year_end_observed_nonzero": None,
                    "total_rows": total,
                    "eligible_rows": eligible,
                    "non_null_rows": int(non_null),
                    "explicit_zero_rows": int(zero or 0),
                    "nonzero_rows": int(signal or 0),
                    "all_row_density": density(non_null, total),
                    "eligible_row_density": density(non_null, eligible),
                    "signal_density": density(signal or 0, eligible),
                    "year_coverage_density": density(non_null, total),
                    "missing_year": False,
                    "sparse_year": (signal or 0) == 0,
                    "unavailable_reason": None,
                    "eligibility_basis": "all_source_rows",
                }
            )
    by_column = {}
    for row in output:
        by_column.setdefault(row["raw_column"], []).append(row)
    for name, rows in by_column.items():
        nonnull_years = [r["year"] for r in rows if isinstance(r["year"], int) and r["non_null_rows"]]
        nonzero_years = [r["year"] for r in rows if isinstance(r["year"], int) and r["nonzero_rows"]]
        for row in rows:
            row["year_start_observed_nonnull"] = min(nonnull_years, default=None)
            row["year_end_observed_nonnull"] = max(nonnull_years, default=None)
            row["year_start_observed_nonzero"] = min(nonzero_years, default=None)
            row["year_end_observed_nonzero"] = max(nonzero_years, default=None)
    return output
