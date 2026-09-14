"""Schema constants for the source-column coverage matrix."""

SOURCE_FIELDS = {
    "source_id",
    "display_name",
    "source_family",
    "table_name",
    "path",
    "grain",
    "source_type",
    "lineage_class",
    "authority_class",
    "snapshot_id",
    "newspaper_excluded",
    "discovery_status",
}

COLUMN_FIELDS = {
    "canonical_column",
    "family",
    "dtype",
    "unit",
    "scope",
    "expected_grain",
    "null_semantics",
    "zero_semantics",
    "source_of_truth_status",
    "definition",
    "stat_domain",
    "stat_role",
    "stat_unit",
    "stat_credit_type",
    "canonical_semantic_id",
    "raw_table_context",
    "source_definition_version",
}

OBSERVATION_FIELDS = {
    "source_id",
    "raw_column",
    "canonical_column",
    "relationship_type",
    "alias_family",
    "mapping_confidence",
    "review_status",
    "raw_dtype",
    "normalized_dtype",
    "unit_rule",
    "join_key",
    "identity_requirements",
    "transformation_class",
    "gate_eligible",
    "stat_domain",
    "stat_role",
    "stat_unit",
    "stat_credit_type",
    "canonical_semantic_id",
    "raw_table_context",
    "source_definition_version",
}

COVERAGE_FIELDS = {
    "source_id",
    "raw_column",
    "canonical_column",
    "year",
    "year_start_observed_nonnull",
    "year_end_observed_nonnull",
    "year_start_observed_nonzero",
    "year_end_observed_nonzero",
    "total_rows",
    "eligible_rows",
    "non_null_rows",
    "explicit_zero_rows",
    "nonzero_rows",
    "all_row_density",
    "eligible_row_density",
    "signal_density",
    "year_coverage_density",
    "missing_year",
    "sparse_year",
    "unavailable_reason",
    "coverage_union_years",
    "coverage_intersection_years",
    "true_missing_years",
    "source_partial_years",
    "missing_year_occurrence_count",
}

RELATIONSHIP_FIELDS = {
    "left_source_id",
    "left_column",
    "left_canonical_column",
    "right_source_id",
    "right_column",
    "right_canonical_column",
    "relationship_type",
    "formula",
    "tolerance",
    "overlap_rows",
    "agreement_rows",
    "conflict_rows",
    "agreement_density",
    "conflict_density",
    "systematic_bias",
    "adjudication_status",
}


def validate_fields(record: dict, required: set[str]) -> None:
    missing = required - set(record)
    if missing:
        raise ValueError(f"missing matrix fields: {sorted(missing)}")
