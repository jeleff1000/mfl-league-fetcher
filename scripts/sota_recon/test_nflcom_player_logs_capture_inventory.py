from .nflcom_player_logs_capture_inventory import (
    PLAYER_LOG_BLOCK_RECOVERY,
    mirror_layout_recovery_query,
    classify_capture,
    classify_week_translation,
)


def test_partial_row_recovery_uses_only_signature_fields():
    assert "THEN 'WRTE'" in PLAYER_LOG_BLOCK_RECOVERY
    assert "THEN 'RBFB5'" in PLAYER_LOG_BLOCK_RECOVERY
    assert "THEN 'QB'" in PLAYER_LOG_BLOCK_RECOVERY
    assert "THEN 'DEF_log'" in PLAYER_LOG_BLOCK_RECOVERY
    assert "THEN 'K_log'" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(avg_2 AS DOUBLE) IS NOT NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(lng_2 AS DOUBLE) IS NOT NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(yds AS DOUBLE) IS NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(rec AS DOUBLE) IS NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(avg_2 AS DOUBLE) IS NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "yds_2 AS DOUBLE) / NULLIF(TRY_CAST(rec AS DOUBLE), 0)" in PLAYER_LOG_BLOCK_RECOVERY
    assert "yds_2 AS DOUBLE) / NULLIF(TRY_CAST(att AS DOUBLE), 0)" in PLAYER_LOG_BLOCK_RECOVERY
    assert "COALESCE(ABS(TRY_CAST(avg_2 AS DOUBLE)" in PLAYER_LOG_BLOCK_RECOVERY
    assert "TRY_CAST(avg AS DOUBLE) IS NOT NULL" in PLAYER_LOG_BLOCK_RECOVERY
    assert "NULLIF(TRY_CAST(rec AS DOUBLE), 0)) <= 0.06" in PLAYER_LOG_BLOCK_RECOVERY
    assert "NULLIF(TRY_CAST(att AS DOUBLE), 0)) <= 0.06" in PLAYER_LOG_BLOCK_RECOVERY


def test_mirror_recovery_is_source_only_and_swaps_stat_groups():
    query = mirror_layout_recovery_query("SELECT * FROM source_table")
    assert "MIRRORED_SOURCE_ROW" in query
    assert "a.yds AS DOUBLE) IS NOT DISTINCT FROM TRY_CAST(k.yds_2" in query
    assert "k.direct_layout IN ('RBFB5', 'WRTE')" in query
    assert "NFL_player_id" not in query


def test_raw_logs_are_blocked_when_date_and_layout_axes_are_absent():
    result = classify_capture(total_rows=100, parsed_dates=0, has_layout=False)
    assert result["status"] == "PROOF_PENDING_CAPTURE"
    assert result["blockers"] == ["NO_GAME_DATE", "NO_POSITION_LAYOUT"]


def test_capture_is_not_blocked_when_both_join_axes_survive():
    result = classify_capture(total_rows=100, parsed_dates=100, has_layout=True)
    assert result["status"] == "WITNESSABLE_KEY_SURFACE"
    assert result["blockers"] == []


def test_capture_records_partial_recovered_layout_as_pending():
    result = classify_capture(
        total_rows=100,
        parsed_dates=100,
        has_layout=False,
        recovered_layout_rows=60,
    )
    assert result["status"] == "PROOF_PENDING_CAPTURE"
    assert result["blockers"] == ["PARTIAL_POSITION_LAYOUT"]
    assert result["recovered_layout_rows"] == 60


def test_empty_unclassified_rows_do_not_block_informative_witnesses():
    result = classify_capture(
        total_rows=100,
        parsed_dates=100,
        has_layout=False,
        recovered_layout_rows=60,
        unresolved_numeric_rows=0,
    )
    assert result["status"] == "WITNESSABLE_KEY_SURFACE"
    assert result["blockers"] == []


def test_week_translation_is_pending_when_any_bucket_is_ambiguous():
    result = classify_week_translation(unique_buckets=10, ambiguous_buckets=2)
    assert result["status"] == "PROOF_PENDING_WEEK_TRANSLATION"
    assert result["ambiguous_buckets"] == 2
