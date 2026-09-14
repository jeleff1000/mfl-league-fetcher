r"""Move NFL supertable builder/audit/apply/repair scripts from this repo's
scripts directory to D:\league-history-data\nfl\tools\
organized by verb prefix.

Scripts that stay in the project repo (fantasy app pipeline):
  - league census, reimport, validate leagues, draft tools, Fly ops,
    sleeper/yahoo import, server/cache tools, kmffl scripts,
    aggregate rebuild (Fly-facing), ddl audit, etc.

Run dry_run=True first to review, then set dry_run=False to execute.
"""

import shutil
from pathlib import Path

SRC = Path(__file__).resolve().parent
DST = Path(r"D:\league-history-data\nfl\tools")

# ── Destination bucket → list of filename prefixes/exact names ──────────
BUCKETS: dict[str, list[str]] = {
    "build": [
        "build_nfl_", "build_pfr_", "build_stathead_", "build_pbp_",
        "build_player_", "build_master_", "build_canonical_", "build_unified_",
        "build_publish_", "build_game_stat_",
        "build_supertable_",
        "build_known_stat_",
        "build_draft_dossier_",
    ],
    "apply": [
        "apply_nfl_", "apply_pfr_", "apply_pbp_", "apply_pfa_",
        "apply_dst_", "apply_stathead_", "apply_supertable_",
        "apply_combined_", "apply_exact_",
    ],
    "audit": [
        "audit_nfl_", "audit_pfr_", "audit_super_", "audit_fpts_",
        "audit_pts_", "audit_espn_", "audit_golden_", "audit_live_",
        "audit_franchise_", "audit_nflverse_",
        # these audit scripts don't touch fantasy app:
        "ddl_bracket_results_audit.py", "ddl_sacko_bracket_results_audit.py",
        # not fantasy app:
        "parity_check_", "parity_combo_",
        "scope_super_", "crunch_pbp_", "sample_pbp_",
        "reconcile_pfr_", "reconcile_pbp_",
        "compare_pbp_", "twin_stathead_",
        "find_supertable_", "find_dominant_",
        "investigate_fg_", "investigate_scoring_",
        "probe_nfl_",
        "profile_nfl_",
        "pre_l4_live_",
        "aggregate_merged_pbp_",
        "inventory_nfl_",
    ],
    "validate": [
        "validate_nfl_", "validate_master_",
        "validate_nfl_headshot_", "validate_nfl_pbp_", "validate_nfl_column_",
        "validate_nfl_position_", "validate_nfl_schema_", "validate_nfl_stat_",
        "preflight_validate_", "verify_nfl_", "verify_fantasy_", "verify_optimal_",
        "verify_player_",
        "build_nfl_local_sot_", "build_nfl_local_release_",
        "build_nfl_release_", "build_nfl_rebuild_",
        "build_nfl_validation_",
        "write_nfl_local_", "write_nfl_schema_", "write_nfl_sota_",
        "conform_nfl_schema_", "audit_ddl_compliance.py",
    ],
    "repair": [
        "fix_1946_", "fix_1952_", "fix_dst_", "fix_phase4b", "fix_player_id_",
        "fix_player_bio_", "fix_raiders_", "fix_remaining_", "fix_unified_",
        "fix_deterministic_", "fix_pfa_only_",
        "repair_championship_", "repair_historical_kicker_", "repair_known_",
        "repair_master_", "repair_nfl_", "repair_player_nfl_",
        "repair_pre_l4_", "repair_pre1999_", "repair_super_table_",
        "restore_bio_",
        "correct_postseason_", "correct_super_table_",
        "add_hou_tennessee_",
        "delete_super_table_",
        "insert_sb_lx_",
        "alter_table_pts_k_",
        "drop_pts_def_",
        "rename_nfl_franchise_",
        "enrich_pfr_excel_",
        "populate_super_table_",
        "map_master_to_super_",
        "merge_franchise_history_",
        "merge_super_table_player_",
        "merge_super_table_postseason_",
        "rebuild_player_nfl_rollups_",
        "merge_stathead_nflverse_pbp.py",
        "debug_nfl_team_game_insert.py",
        "nfl_canonical_positions.py",
        "generate_bio_fixups_sql.py",
    ],
    "scrape": [
        "scrape_pfr_", "scrape_stathead_", "scrape_cfb_",
        "retabulate_pfr_", "build_publish_nfl_team_games_",
        "build_stathead_pbp_",
        "combine_stathead_pbp_",
        "parse_stathead_pbp_",
        "merge_stathead_nflverse_pbp.py",
    ],
    "recompute": [
        "recompute_", "backfill_fg_", "backfill_historical_",
        "backfill_pbp_schema_", "backfill_pfr_", "backfill_pre1999_",
        "backfill_pts_", "backfill_sb_lx_",
        "backup_pts_k_",
        "rebuild_nfl_aggregate_table_fly.py",
        "rebuild_nfl_aggregate_tables.py",
        "build_nfl_pbp_", "parse_stathead_pbp_",
        "m15_offense_",
        "build_nfl_atom_delta_",
        "alt_table_pts_k_",
    ],
    "publish": [
        "publish_nfl_", "publish_nfl_pfa_", "publish_nfl_ancient_",
    ],
    "capture": [
        "capture_nfl_", "update_nfl_source_search_", "run_nfl_source_",
        "update_cadence_inventory.py",
        "close_nfl_non_loc_source_horizon.py",
    ],
    "classify": [
        "classify_nfl_", "classify_remaining_", "classify_identity_",
        "stage_combined_", "stage_pbp_", "stage_pfr_", "stage_postseason_",
        "stage_residual_", "stage_special_teams_", "stage_stat_family_",
        "summarize_nfl_schema_",
        "attach_nfl_pfa_",
        "fix_nfl_", "build_nfl_ancient_",
        "build_nfl_pfr_",
        "build_nfl_partial_", "build_nfl_pbp_p0_",
    ],
    "analysis": [
        # _ prefix = temp one-off analysis
        "_audit_fly_only_", "_audit_reader_", "_audit_silent_",
        "_backfill_player_fantasy_", "_build_offense_canonical_",
        "_build_pts_off_", "_check_resolved_", "_check_unmatched_",
        "_check_v22_", "_diag_ncatx_", "_inspect_unmatched_",
        "_read_game_completeness_", "_read_inventory_",
        "_rollback_bad_lane_", "_triage_stat_consistency_",
        # other analysis
        "triage_fantasy_stats_", "build_nfl_all_year_fantasy_",
        "build_nfl_golden_sample_",
        "build_nfl_gap_source_",
        "build_nfl_sparse_week_",
        "bug_j_yahoo_xml_check.py",
        "build_nfl_c_to_d_", "copy_c_to_d_", "plan_c_data_",
        "snapshot_fly_supertable_", "snapshot_ops_to_parquet.py",
        "build_nfl_d_drive_current_state_",
        "build_nfl_recovery_artifact_", "build_nfl_recovery_completion_",
        "build_nfl_recovery_finish_line_", "build_nfl_recovery_pilot_",
        "build_nfl_recovery_ready_", "build_nfl_recovery_review_",
        "build_nfl_residual_", "build_nfl_return_residual_",
        "summarize_draft_cross_table_",
    ],
    "utils": [
        "nfl_data_lake_paths.py",
        "nfl_local_release_utils.py",
        "nfl_pfr_schedule_utils.py",
        "kicker_scoring_canonical_2026_05_02.json",
        "offense_scoring_canonical_2026_05_02.json",
        "idp_scoring_canonical_2026_05_03.json",
        "sleeper_std_def_scoring_2026_04_29.json",
    ],
    "pfa": [
        "build_nfl_ancient_pfa_", "fix_1946_la_sf_pfa_", "fix_1952_dallas_",
        "fix_deterministic_pfa_", "fix_pfa_only_", "fix_remaining_exact_pfa_",
        "publish_nfl_pfa_", "attach_nfl_pfa_",
        "apply_nfl_ancient_", "apply_nfl_cached_gamelog_",
    ],
}

# Scripts that STAY in the project repo (fantasy app)
STAY_NAMES = {
    "league_settings_census.py",
    "backfill_draft_scores.py",
    "bulk_backfill_draft_scores.py",
    "reimport_league.py",
    "yahoo_import_dispatch.py",
    "import_dispatch.py",
    "merge_league_admin.py",
    "draft_cross_table_batch.py",
    "draft_cross_table_fleet.py",
    "draft_optimizer_suite.py",
    "draft_population_ml_census.py",
    "draft_profile_residual_fleet.py",
    "validate_all_sleeper_leagues.py",
    "validate_all_yahoo_leagues.py",
    "validate_common.py",
    "check_no_deprecated_backend.py",
    "check_offseason_drafts.py",
    "check_sleeper_offseason_drafts.py",
    "repair_championship_label_final_flags.py",
    "repair_sacko_loser_path_flags.py",
    "repair_sleeper_null_bracket_byes.py",
    "fix_cache_collision_rows.py",
    "fix_charles_johnson_yahoo_map.py",
    "fix_playoff_week_offset_2026_04_30.py",
    "fix_unjoined_player_ids_2026_04_28.py",
    "local_repro_kmffl.py",
    "verify_kmffl_2013_e2e.py",
    "rerun_kmffl_2013_transforms.py",
    "sim_kmffl_external_merge.py",
    "sim_kmffl_full_pipeline.py",
    "pilot_kmffl_external_ingest.py",
    "refresh_aggregates.py",
    "rebuild_nfl_aggregates_fly.py",
    "rebuild_nfl_career_all_fly.py",
    "rebuild_nfl_aggregate_table_fly.py",
    "rebuild_nfl_aggregate_tables.py",
    "rebuild_player_nfl_rollups_2026_05_04.py",  # touches Fly
    "recompute_player_nfl_rollup_ranks_2026_05_04.py",
    "repair_player_nfl_rollup_consistency_epsilon_2026_05_04.py",
    "repair_super_table_nonfinite_receiving_shares_2026_05_04.py",
    "repair_super_table_postseason_weeks_2026_05_04.py",
    "repair_super_table_def_identity_2026_05_04.py",
    "recompute_l3_ppg_season_career_2026_05_04.py",
    "recompute_player_nfl_rollup_ranks_2026_05_04.py",
    "generate_pfr_usage_gap_buckets.py",
    "audit_api_usage.ps1",
    "audit_frontend_centralized_reads.ps1",
    "audit_league_api_endpoints.py",
    "warm_vercel_cache.py",
    "fly_availability_stress.py",
    "fly_capacity_mode.py",
    "mcp_fly_duckdb.py",
    "seed_fly_ops.py",
    "smoke_test_staging.py",
    "offline_architecture_smoke.py",
    "pro_grade_server_hammer.py",
    "normalize_league_table_refs.ps1",
    "update_sleeper_offseason_draft.py",
    "ddl_bracket_results_audit.py",
    "ddl_sacko_bracket_results_audit.py",
    "migrate_keeper_config_to_ddl.py",
    "apply_pbp_scoring_rollup_to_fly_ops.py",
    "apply_dst_team_margin_schedule_to_fly_ops.py",
    "run_cfb_year_range.ps1",
    "finish_cfb_2025_base.ps1",
    "finish_cfb_players.ps1",
    "scrape_cfb_tables.py",
    "scrape_pfr_context_tables.py",
    "scrape_pfr_boxscore_tables.py",
    "drop_leagues_databases.sql",
    "drop_league_databases.sql",
    "verify_fantasy_points_not_null.py",
    "verify_optimal_fixes.py",
    "repair_known_identity_contamination_20260508.py",
    "repair_known_kicking_corruption_20260508.py",
    "backfill_pbp_schema_columns_20260508.py",
    "apply_combined_identity_promotion_20260509.py",
    "apply_exact_reversed_context_repairs_20260509.py",
    "apply_pbp_exact_near_identity_bridges_20260508.py",
    "apply_pfr_bio_combine_fill.py",
    "apply_pfr_player_bio_pfr_id_bridge.py",
    "apply_pfr_usage_advanced_backfill.py",
    "apply_pfr_usage_gap_insert_candidates.py",
    "apply_pfr_usage_null_delta.py",
    "apply_stathead_pfr_bio_repair_local.py",
    "apply_supertable_raw_atom_packages.py",
    "backfill_pre1999_dst_team_atoms_20260513.py",
    "build_known_stat_family_split_audit.py",
    "build_pbp_supertable_stat_truth_ledger.py",
    "build_pfr_identity_bridge_repairs.py",
    "build_pfr_lastname_context_bridge_repairs.py",
    "build_pfr_remaining_bridge_stubs.py",
    "build_pfr_supertable_update_package.py",
    "build_player_prior_awards.py",
    "build_stathead_pfr_bio_repair.py",
    "build_supertable_holdout_resolution_package.py",
    "build_supertable_prewrite_hardening_audit.py",
    "classify_identity_collision_truth.py",
    "classify_remaining_context_mismatches_20260509.py",
    # this script itself
    "organize_nfl_tools_to_d_drive.py",
}


def match_bucket(name: str) -> str | None:
    """Return the destination bucket for a filename, or None if it stays."""
    if name in STAY_NAMES:
        return None
    for bucket, prefixes in BUCKETS.items():
        for prefix in prefixes:
            if name.startswith(prefix) or name == prefix:
                return bucket
    return None  # no match → stays


def run(dry_run: bool = True) -> None:
    DST.mkdir(parents=True, exist_ok=True)
    for bucket in BUCKETS:
        (DST / bucket).mkdir(exist_ok=True)

    all_scripts = sorted(SRC.iterdir())
    moved: list[tuple[str, str]] = []
    staying: list[str] = []
    unmatched: list[str] = []

    for f in all_scripts:
        if not f.is_file():
            continue
        bucket = match_bucket(f.name)
        if bucket is None:
            staying.append(f.name)
        else:
            dest = DST / bucket / f.name
            moved.append((str(f), str(dest)))

    print(f"Will MOVE:  {len(moved)}")
    print(f"Will STAY:  {len(staying)}")
    print()

    # Show bucket breakdown
    from collections import Counter
    bucket_counts = Counter(Path(d).parent.name for _, d in moved)
    print("Bucket breakdown:")
    for bucket, cnt in sorted(bucket_counts.items(), key=lambda x: -x[1]):
        print(f"  {bucket:15s}: {cnt}")
    print()

    if dry_run:
        print("DRY RUN — no files moved. Set dry_run=False to execute.")
        print()
        print("Sample of files to move:")
        for src, dst in moved[:20]:
            print(f"  {Path(src).name} -> {Path(dst).parent.name}/")
        print(f"  ... and {len(moved)-20} more")
        print()
        print("Files staying in project repo:")
        for name in sorted(staying):
            print(f"  {name}")
        return

    # Execute moves
    errors = []
    for src, dst in moved:
        try:
            shutil.copy2(src, dst)
            Path(src).unlink()
            print(f"MOVED  {Path(src).name} -> {Path(dst).parent.name}/")
        except Exception as e:
            errors.append((src, str(e)))
            print(f"ERROR  {Path(src).name}: {e}")

    print()
    print(f"Moved {len(moved) - len(errors)} files to {DST}")
    if errors:
        print(f"ERRORS ({len(errors)}):")
        for src, err in errors:
            print(f"  {src}: {err}")


if __name__ == "__main__":
    import sys
    dry = "--execute" not in sys.argv
    run(dry_run=dry)
