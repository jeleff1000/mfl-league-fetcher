from pathlib import Path

import pytest


def test_each_worker_fences_provider_player_scope_after_shared_transformations():
    for provider in ("yahoo", "espn", "sleeper"):
        source = Path(f"scripts/refresh_{provider}_active_season.py").read_text(encoding="utf-8")
        assert "capture_active_provider_player_scope(" in source
        assert "assert_transformed_active_player_scope(" in source
        assert "capture_active_final_matchup_scope(" in source
        assert "assert_transformed_active_matchup_scope(" in source
        assert "assert_refresh_derived_output_health(" in source
        assert source.index("capture_active_provider_player_scope(") < source.index("_run_local_pipeline(", source.index("capture_active_provider_player_scope("))
        assert source.index("assert_transformed_active_player_scope(") > source.index("_run_local_pipeline(", source.index("capture_active_provider_player_scope("))
        assert source.index("assert_transformed_active_player_scope(") < source.index("stage_refresh_partitions(", source.index("assert_transformed_active_player_scope("))


def test_each_worker_builds_the_bounded_ops_cache_for_its_active_scoring_variant():
    for provider in ("yahoo", "espn", "sleeper"):
        source = Path(f"scripts/refresh_{provider}_active_season.py").read_text(encoding="utf-8")
        assert "_active_year_scoring_info(" in source
        assert "scoring_info=active_scoring" in source


def test_each_worker_uses_shared_catchup_weeks_and_authoritative_active_draft():
    draft_contract = {
        "yahoo": "needs_active_season_draft_fetch(",
        "espn": "refresh_authoritative_draft_partition(",
        "sleeper": "refresh_authoritative_draft_partition(",
    }
    for provider in ("yahoo", "espn", "sleeper"):
        source = Path(f"scripts/refresh_{provider}_active_season.py").read_text(encoding="utf-8")
        assert "completed_weeks_to_refresh(" in source
        assert draft_contract[provider] in source


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("script_name", [
    "refresh_yahoo_active_season.py", "refresh_espn_active_season.py",
    "refresh_sleeper_active_season.py",
])
def test_weekly_worker_receipt_separates_provider_enrichment_and_fly_times(script_name):
    text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
    for phase in ("source_plan", "source_snapshot", "local_hydration",
                  "provider_fetch", "player_ops_cache", "shared_transformations",
                  "homepage_preservation_stage", "fly_publication", "post_publish_verification"):
        assert f'timer.mark("{phase}")' in text
    assert 'receipt["phase_seconds"] = timer.finish()' in text


@pytest.mark.parametrize("script_name", [
    "refresh_yahoo_active_season.py",
    "refresh_espn_active_season.py",
    "refresh_sleeper_active_season.py",
])
def test_weekly_worker_receipt_breaks_down_preservation_and_staging(script_name):
    text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
    assert 'receipt["homepage_preservation_stage_seconds"] = stage_timer.finish()' in text
    for phase in (
        "preservation_snapshot",
        "preservation_validation",
        "derived_output_validation",
        "stage_partitions",
        "bundle_build",
    ):
        assert f'stage_timer.mark("{phase}")' in text


WORKFLOWS = {
    "yahoo": "yahoo_incremental_refresh_worker.yml",
    "espn": "espn_incremental_refresh_worker.yml",
    "sleeper": "sleeper_incremental_refresh_worker.yml",
}


@pytest.mark.parametrize(
    "filename",
    [
        "yahoo_quick_import_worker.yml",
        "yahoo_full_import_worker.yml",
        "espn_quick_import_worker.yml",
        "espn_full_import_worker.yml",
        "sleeper_quick_import_worker.yml",
        "sleeper_full_import_worker.yml",
        "multi_platform_full_import_worker.yml",
    ],
)
def test_imports_share_the_update_league_execution_boundary(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "import_lock_key:" in text
    assert "required: true" in text.split("import_lock_key:", 1)[1].split("type:", 1)[0]
    assert "group: league-update-${{ github.event.inputs.import_lock_key || github.event.client_payload.import_lock_key }}" in text
    assert "cancel-in-progress: false" in text
    assert "queue: max" in text
    assert "Validate import lock key" in text


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_active_updates_share_the_import_execution_boundary(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    expected_group = (
        "group: league-update-${{ github.event_name == 'schedule' && 'demo_league' || inputs.db_name }}"
        if filename.startswith("yahoo_")
        else "group: league-update-${{ inputs.db_name }}"
    )
    assert expected_group in text
    assert "cancel-in-progress: false" in text
    assert "queue: max" in text


@pytest.mark.parametrize(("platform", "filename"), WORKFLOWS.items())
def test_active_updates_use_bounded_ops_and_runtime_dependencies(platform: str, filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "restore-research-ops-cache" not in text
    assert f"requirements-weekly-update-{platform}.txt" in text
    assert "\n            /.github/\n" not in text
    assert "\n            /requirements.txt\n" not in text
    assert "actions/cache" not in text
    assert "cache-venv" not in text
    assert "hashFiles('requirements.txt')" not in text


def test_espn_update_sparse_checkout_excludes_unrelated_scripts():
    text = (ROOT / ".github" / "workflows" / "espn_incremental_refresh_worker.yml").read_text(
        encoding="utf-8"
    )
    assert "            /scripts/\n" not in text
    for script in (
        "claim_manual_league_update.py",
        "league_update_workflow_receipt.py",
        "probe_league_update_freshness.py",
        "record_league_update_status.py",
        "recover_league_update_cache.py",
        "refresh_espn_active_season.py",
        "refresh_yahoo_active_season.py",
        "warm_vercel_cache.py",
    ):
        assert f"            /scripts/{script}" in text


@pytest.mark.parametrize(("platform", "filename"), WORKFLOWS.items())
def test_active_updates_reserve_time_to_fail_and_exit_after_setup(
    platform: str, filename: str
):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    display_name = {"yahoo": "Yahoo", "espn": "ESPN", "sleeper": "Sleeper"}[platform]
    assert "- name: Set hard refresh deadline" in text
    assert "LEAGUE_UPDATE_DEADLINE_EPOCH=$(( $(date +%s) + 120 ))" in text
    assert 'remaining=$(( LEAGUE_UPDATE_DEADLINE_EPOCH - $(date +%s) ))' in text
    assert 'timeout --signal=KILL "${remaining}s" python scripts/refresh_' in text
    assert 'timeout --signal=KILL 8s python scripts/record_league_update_status.py' in text
    assert text.index("- name: Claim paid manual update") < text.index(
        "- name: Set hard refresh deadline"
    ) < text.index(f"- name: Refresh {display_name} active season")


def test_weekly_runtime_requirements_exclude_non_worker_packages():
    text = (ROOT / "requirements-weekly-update.txt").read_text(encoding="utf-8")
    for package in (
        "streamlit",
        "plotly",
        "matplotlib",
        "scipy",
        "scikit-learn",
        "pulp",
        "pytest",
    ):
        assert package not in text.lower()


def test_weekly_runtime_requirements_do_not_install_other_platforms():
    common = (ROOT / "requirements-weekly-update.txt").read_text(encoding="utf-8").lower()
    yahoo = (ROOT / "requirements-weekly-update-yahoo.txt").read_text(encoding="utf-8").lower()
    espn = (ROOT / "requirements-weekly-update-espn.txt").read_text(encoding="utf-8").lower()
    sleeper = (ROOT / "requirements-weekly-update-sleeper.txt").read_text(encoding="utf-8").lower()

    assert "yahoo-fantasy-api" not in common
    assert "espn-api" not in common
    assert "yahoo-fantasy-api" in yahoo and "espn-api" not in yahoo
    assert "espn-api" in espn and "yahoo-fantasy-api" not in espn
    assert "yahoo-fantasy-api" not in sleeper and "espn-api" not in sleeper


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_executing_manual_or_ui_update_is_main_only_before_checkout(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    guard = text.split("- name: Require canonical public main for publication", 1)[1]
    checkout = text.split("- name: Checkout", 1)[0]
    expected_guard = "if: env.INPUT_EXECUTE == 'true'" if filename.startswith("yahoo_") else "if: inputs.execute"
    assert expected_guard in guard.split("- name:", 1)[0]
    assert "INPUT_WORKFLOW_REF: ${{ github.ref }}" in guard.split("- name:", 1)[0]
    assert 'if [ "${INPUT_WORKFLOW_REF}" != "refs/heads/main" ]; then' in guard.split("- name:", 1)[0]
    assert "Require canonical public main for publication" in checkout


def test_legacy_cookie_import_cannot_race_oauth_update_on_the_same_league():
    text = (ROOT / ".github" / "workflows" / "yahoo_cookie_import_worker.yml").read_text(encoding="utf-8")
    assert "group: league-update-${{ inputs.database_name }}" in text
    assert "cancel-in-progress: false" in text
    assert "queue: max" in text


def test_direct_admin_merge_is_main_only_before_it_can_write():
    text = (ROOT / ".github/workflows/league_admin_merge_worker.yml").read_text(encoding="utf-8")
    first_step = text.split("    steps:\n", 1)[1].split("      - name:", 2)[1]
    assert text.index("Require canonical public main for publication") < text.index(
        "Validate target league lock"
    )
    assert "INPUT_WORKFLOW_REF: ${{ github.ref }}" in first_step
    assert 'if [ "${INPUT_WORKFLOW_REF}" != "refs/heads/main" ]; then' in first_step


@pytest.mark.parametrize(("platform", "filename"), WORKFLOWS.items())
def test_ui_lifecycle_wraps_existing_september_refresh(platform: str, filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "workflow_dispatch:" in text
    assert "dispatch_token:" in text
    assert "cache_only:" in text
    assert "scripts/recover_league_update_cache.py" in text
    assert f"--platform {platform}" in text
    if platform == "yahoo":
        assert "if: ${{ env.INPUT_CACHE_ONLY != 'true' }}" in text
        assert "if: ${{ env.INPUT_CACHE_ONLY != 'true' && env.INPUT_EXECUTE == 'true' }}" in text
        assert "if: ${{ (failure() || cancelled()) && env.INPUT_CACHE_ONLY != 'true'" in text
    else:
        assert "if: ${{ !inputs.cache_only }}" in text
        assert "if: ${{ !inputs.cache_only && inputs.execute }}" in text
        assert "if: ${{ (failure() || cancelled()) && !inputs.cache_only" in text
    assert "attempt_id:" in text
    assert "claim_version:" in text
    assert "observed_manifest_digest:" in text
    assert "push:" not in text
    assert "scripts/record_league_update_status.py" in text
    assert "- name: Claim paid UI update" not in text
    assert "- name: Start paid manual update" not in text
    assert text.count('--attempt-id "${INPUT_ATTEMPT_ID}"') >= 1
    assert text.count('--claim-version "${INPUT_CLAIM_VERSION}"') >= 1
    assert text.count('--attempt-id "${UPDATE_ATTEMPT_ID}"') >= 3
    assert text.count('--claim-version "${UPDATE_CLAIM_VERSION}"') >= 3
    assert f"scripts/refresh_{platform}_active_season.py" in text
    assert '--observed-manifest-digest "${OBSERVED_MANIFEST_DIGEST}"' in text
    assert "scripts/warm_vercel_cache.py" in text
    assert "id: publication" in text
    assert "scripts/league_update_workflow_receipt.py" in text
    assert "--require-publication" in text
    assert "steps.publication.outputs.committed == 'true'" in text
    assert "--status committed" in text
    assert "--failure-status" in text
    assert "--cancelled" in text
    # Status functions are legal in an ``if:`` expression but GitHub rejects
    # them in a step ``env:`` expression.  Keep cancellation classification
    # based on the supported job-status context so dispatch itself is valid.
    assert "WORKFLOW_WAS_CANCELLED: ${{ job.status == 'cancelled' && '1' || '0' }}" in text
    assert "WORKFLOW_WAS_CANCELLED: ${{ cancelled()" not in text
    assert "committed_cache_pending" in text
    assert "--strict" in text
    assert "--verify-hot" in text
    assert "--required-only" in text
    assert "--timeout 5" in text
    assert "--warm-attempts 1" in text
    assert "--hot-verify-attempts 1" in text
    assert "--max-p95-ms 0" in text
    assert "--status succeeded" in text
    assert '--status "${recovery_status}"' in text
    assert 'recovery_status=$(python scripts/league_update_workflow_receipt.py' in text
    assert text.index("scripts/warm_vercel_cache.py") < text.index("--status succeeded")
    # The provider refresh itself has a hard two-minute process deadline. The
    # job gets one extra minute so setup and failure/status cleanup can finish.
    assert "timeout-minutes: 3" in text


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_worker_captures_source_manifest_when_ui_did_not(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "id: manual_probe" in text
    assert "scripts/probe_league_update_freshness.py" in text
    expected_capture = (
        "env.INPUT_EXECUTE == 'true' && env.INPUT_CACHE_ONLY != 'true' && env.INPUT_OBSERVED_MANIFEST_DIGEST == ''"
        if filename.startswith("yahoo_")
        else "inputs.execute && !inputs.cache_only && inputs.observed_manifest_digest == ''"
    )
    assert expected_capture in text
    assert "inputs.dispatch_token == '' && inputs.observed_manifest_digest == ''" not in text
    expected_digest = (
        "steps.manual_probe.outputs.digest || env.INPUT_OBSERVED_MANIFEST_DIGEST"
        if filename.startswith("yahoo_")
        else "steps.manual_probe.outputs.digest || inputs.observed_manifest_digest"
    )
    assert expected_digest in text
    assert text.index("- name: Capture manual source manifest") < text.index(
        "- name: Claim paid manual update"
    )


@pytest.mark.parametrize("platform", ("yahoo", "espn", "sleeper"))
def test_execute_never_falls_back_to_an_uncaptured_week_boundary(platform: str):
    text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
    assert "if args.execute and persisted_plan is None:" in text
    assert "executing update requires a captured source manifest" in text


@pytest.mark.parametrize(("platform", "filename"), WORKFLOWS.items())
def test_paid_manual_execute_uses_the_same_attempt_and_terminal_lifecycle(platform: str, filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "id: manual_claim" in text
    assert "scripts/claim_manual_league_update.py" in text
    assert f"--platform {platform}" in text
    assert text.index("Install dependencies") < text.index("- name: Claim paid manual update")
    assert "Install Fly claim dependency" not in text
    assert "uses: astral-sh/setup-uv@v5" in text
    assert "enable-cache: true" in text
    assert f'cache-dependency-glob: "requirements-weekly-update-{platform}.txt"' in text
    assert f"uv pip install --system --quiet -r requirements-weekly-update-{platform}.txt" in text
    assert "--no-cache-dir" not in text
    assert "steps.manual_claim.outputs.token || inputs.dispatch_token" in text
    assert "steps.manual_claim.outputs.attempt_id || inputs.attempt_id" in text
    assert "steps.manual_claim.outputs.claim_version || inputs.claim_version" in text
    expected_token_guard = (
        "steps.manual_claim.outputs.token != '' || env.INPUT_DISPATCH_TOKEN != ''"
        if platform == "yahoo"
        else "steps.manual_claim.outputs.token != '' || inputs.dispatch_token != ''"
    )
    assert expected_token_guard in text
    assert "--require-entitled" in text
    assert text.count('--dispatch-token "${UPDATE_TOKEN}"') >= 3
    assert text.count('--attempt-id "${UPDATE_ATTEMPT_ID}"') >= 3
    assert text.count('--claim-version "${UPDATE_CLAIM_VERSION}"') >= 3


@pytest.mark.parametrize("platform", ("yahoo", "espn", "sleeper"))
def test_exact_claim_is_rechecked_immediately_before_existing_fleet_publish(platform: str):
    text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
    assert "renew_claim_for_publication(reader, database_name=args.db" in text
    assert text.index("renew_claim_for_publication(reader, database_name=args.db") < text.index(
        "FlyTarget().merge_fleet_partition("
    )
    workflow = (ROOT / ".github/workflows" / WORKFLOWS[platform]).read_text(encoding="utf-8")
    expected_claim = (
        "LEAGUE_UPDATE_REQUIRE_CLAIM: ${{ (github.event_name == 'schedule' || inputs.execute) && '1' || '0' }}"
        if platform == "yahoo"
        else "LEAGUE_UPDATE_REQUIRE_CLAIM: ${{ inputs.execute && '1' || '0' }}"
    )
    assert expected_claim in workflow
    assert "LEAGUE_UPDATE_TOKEN: ${{ steps.manual_claim.outputs.token || inputs.dispatch_token }}" in workflow
    assert "LEAGUE_UPDATE_ATTEMPT_ID: ${{ steps.manual_claim.outputs.attempt_id || inputs.attempt_id }}" in workflow
    assert "LEAGUE_UPDATE_CLAIM_VERSION: ${{ steps.manual_claim.outputs.claim_version || inputs.claim_version }}" in workflow


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_dispatch_inputs_never_expand_as_shell_program_text(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    for name in ("db_name", "dispatch_token", "attempt_id", "claim_version"):
        assert f'--{name.replace("_", "-")} "${{{{ inputs.{name} }}}}"' not in text
    expected_db_input = (
        "INPUT_DB_NAME: ${{ github.event_name == 'schedule' && 'demo_league' || inputs.db_name }}"
        if filename.startswith("yahoo_")
        else "INPUT_DB_NAME: ${{ inputs.db_name }}"
    )
    assert expected_db_input in text
    assert '--db "${INPUT_DB_NAME}"' in text


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_manual_no_change_closes_claim_without_cache_or_commit(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "- name: Settle unchanged paid manual update" in text
    assert "steps.publication.outputs.no_op == 'true'" in text
    assert "--status no_change" in text
    assert "steps.manual_claim.outputs.token != ''" in text
    assert text.index("- name: Settle unchanged paid manual update") < text.index("- name: Upload refresh receipt")


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_partial_manual_claim_failure_has_an_owned_cleanup_step(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    assert "- name: Settle failed partial manual claim" in text
    assert "steps.manual_claim.outcome == 'failure'" in text
    assert "--fail-owned-claim" in text
    assert text.index("- name: Settle failed partial manual claim") < text.index("- name: Fail league update")


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_blank_token_manual_cache_retry_uses_verified_pending_claim(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    recovery = text.split("- name: Recover committed cache publication", 1)[1].split("- name: Refresh", 1)[0]
    expected_recovery = (
        "if: env.INPUT_CACHE_ONLY == 'true' && env.INPUT_EXECUTE == 'true'"
        if filename.startswith("yahoo_")
        else "if: inputs.cache_only && inputs.execute"
    )
    assert expected_recovery in recovery
    assert 'if [ -n "${INPUT_DISPATCH_TOKEN}" ]; then' in recovery
    assert "scripts/recover_league_update_cache.py" in recovery
    validation = text.split("- name: Validate cache-only recovery request", 1)[1].split("- name: Recover", 1)[0]
    assert 'test -n "${UI_DISPATCH_TOKEN}"' not in validation


def test_sleeper_validates_the_actual_active_draft_not_an_empty_placeholder():
    text = (ROOT / "scripts" / "refresh_sleeper_active_season.py").read_text(encoding="utf-8")
    fetch = text.split("def _merge_active_payloads(", 1)[1].split("\ndef main(", 1)[0]
    assert 'draft = local_db.read_table("draft", year=active_year)' in fetch
    assert fetch.index('draft = local_db.read_table("draft", year=active_year)') < fetch.index("validate_tabular_active_scope(")
    assert "draft=draft" in fetch


def test_shared_active_refresh_publishes_bounded_worker_homepage_output_atomically():
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_espn_active_season.py",
        "refresh_sleeper_active_season.py",
    ):
        text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert "prepare_homepage_refresh" in text
        assert "publish_homepage_refresh_bundle" not in text
        bundle_call = text.split("bundle = build_fleet_partition_bundle(", 1)[1].split(")", 1)[0]
        assert "rebuild_career_rollups=True" in bundle_call
        assert "rebuild_homepage_rollups=False" in bundle_call
        assert text.count("merge_fleet_partition(") == 1
        assert 'receipt["homepage_rows"] = receipt["homepage_refresh"]["rows"]' in text
        assert 'receipt["homepage_seconds"] = round(time.monotonic() - homepage_started, 3)' in text
        assert 'parser.add_argument("--observed-manifest-digest")' in text
        assert "load_persisted_refresh_plan(" in text
        assert 'receipt["source_manifest_digest"]' in text
        assert 'receipt["source_manifest_json"]' in text


def test_active_refresh_callers_do_not_select_wire_schema_versions():
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_espn_active_season.py",
        "refresh_sleeper_active_season.py",
    ):
        text = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert "FLEET_CAREER_SCHEMA_VERSION" not in text
        assert "FLEET_HOMEPAGE_SCHEMA_VERSION" not in text
        assert "publication_schema_version" not in text
        assert "server_rebuilds_career_rollups=True" in text
        assert "server_rebuilds_homepage_rollups=False" in text


@pytest.mark.parametrize("filename", WORKFLOWS.values())
def test_post_commit_diagnostics_are_nonfatal_and_separate_from_publication(filename: str):
    text = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    artifact = text.split("- name: Upload refresh receipt", 1)[1].split("- name: Summary", 1)[0]
    summary = text.split("- name: Summary", 1)[1]
    assert "id: diagnostic_artifact" in artifact
    assert "continue-on-error: true" in artifact
    assert "continue-on-error: true" in summary
    assert text.index("--status succeeded") < text.index("- name: Upload refresh receipt")
    assert "data_committed: ${{" in text
    assert "cache_verified: ${{" in text
    assert "diagnostics_uploaded: ${{" in text
    assert "id: cache_publish" in text
    assert "id: cache_recovery" in text
    assert "if-no-files-found: error" in artifact


def test_all_platform_refreshes_start_claim_inside_parallel_preflight():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert "start_league_update_execution(" in text
        preflight = text.split("preflight = run_independent_refresh_preflight({", 1)[1].split("})", 1)[0]
        assert '"entitlement": lambda: start_league_update_execution(' in preflight


def test_all_platform_updates_publish_against_the_hydrated_source_generation():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        capture_marker = (
            "lambda: _capture_update_source_frames("
            if platform == "espn"
            else "source_frames, base_generation = _capture_update_source_frames("
        )
        assert capture_marker in text
        capture_call = text.split(capture_marker, 1)[1].split(")", 1)[0]
        assert "active_year=active_year" in capture_call
        if platform == "espn":
            assert "source_frames, base_generation = source_snapshot_future.result()" in text
        assert "league_generations={args.db: base_generation}" in text
        assert "generation = _publish_generation(reader, args.db)" not in text
    yahoo = (ROOT / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    snapshot = yahoo.split("def _capture_update_source_frames(", 1)[1].split("\ndef ", 1)[0]
    assert snapshot.count("_publish_generation(reader, db_name)") == 2
    assert snapshot.index("frames = _source_frames(") < snapshot.index("if _publish_generation(")


def test_direct_execute_runs_enforce_the_same_paid_entitlement_as_ui_runs():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert "start_league_update_execution(" in text
        assert text.index("start_league_update_execution(") < text.index(
            "bundle = build_fleet_partition_bundle("
        )


def test_all_platforms_overlap_active_ops_cache_with_provider_fetch():
    provider_calls = {
        "yahoo": '_merge_refresh_payloads(',
        "espn": '_merge_active_payloads(',
        "sleeper": '_merge_active_payloads(',
    }
    for platform, provider_call in provider_calls.items():
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        main = text.split("def main(", 1)[1]
        start = main.index("ops_context = background_refresh_call(")
        fetch = main.index(provider_call, start)
        wait = main.index("ops_future.result()", fetch)
        assert start < fetch < wait


def test_espn_overlaps_provider_context_with_local_hydration():
    text = (ROOT / "scripts" / "refresh_espn_active_season.py").read_text(encoding="utf-8")
    main = text.split("def main(", 1)[1]
    start = main.index("context_future = start_background_refresh_call(")
    hydrate = main.index("hydrate_local_refresh_sources(", start)
    wait = main.index("context_future.result()", hydrate)
    fetch = main.index("_merge_active_payloads(", wait)
    assert start < hydrate < wait < fetch


def test_espn_overlaps_source_snapshot_with_refresh_planning():
    text = (ROOT / "scripts" / "refresh_espn_active_season.py").read_text(encoding="utf-8")
    main = text.split("def main(", 1)[1]
    start = main.index("source_snapshot_future = start_background_refresh_call(")
    plan = main.index("persisted_plan = load_persisted_refresh_plan(", start)
    wait = main.index("source_snapshot_future.result()", plan)
    hydrate = main.index("hydrate_local_refresh_sources(", wait)
    assert start < plan < wait < hydrate


def test_espn_overlaps_rosters_with_schedule_and_transaction_reads():
    text = (ROOT / "scripts" / "refresh_espn_active_season.py").read_text(encoding="utf-8")
    payload = text.split("def _merge_active_payloads(", 1)[1].split("\ndef ", 1)[0]
    start = payload.index("secondary_payload_future = start_background_refresh_call(")
    roster = payload.index("fetch_espn_rosters_modern(", start)
    wait = payload.index("secondary_payload_future.result()", roster)
    merge = payload.index('merge_provider_refresh_table(\n            local_db,\n            "transactions"', wait)
    assert start < roster < wait < merge


def test_all_platforms_recheck_ops_cache_after_provider_settings_are_hydrated():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        main = text.split("def main(", 1)[1]
        wait = main.index("ops_future.result()")
        recheck = main.index("_ensure_active_year_ops_cache(", wait)
        pipeline = main.index("_run_local_pipeline(", recheck)
        assert wait < recheck < pipeline
        assert "scoring_info=_active_year_scoring_info(" in main[recheck:pipeline]


def test_all_platforms_overlap_publication_claim_with_local_staging():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        main = text.split("def main(", 1)[1]
        start = main.index("claim_future = start_background_refresh_call(")
        stage = main.index("stage_refresh_partitions(", start)
        wait = main.index("claim_future.result()", stage)
        publish = main.index("merge_fleet_partition(", wait)
        assert start < stage < wait < publish


def test_all_platforms_bound_weekly_fly_merge_to_forty_seconds():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert "merge_timeout_seconds=40" in text


def test_all_platforms_forward_detected_historical_aggregate_repairs():
    for platform in ("yahoo", "espn", "sleeper"):
        text = (ROOT / "scripts" / f"refresh_{platform}_active_season.py").read_text(encoding="utf-8")
        assert '"missing_derived_aggregate" in persisted_plan.reasons' in text
        assert "repair_missing_season_rollups=" in text


def test_sleeper_refresh_merges_rosters_through_canonical_ownership_key():
    text = (ROOT / "scripts" / "refresh_sleeper_active_season.py").read_text(encoding="utf-8")
    ownership = (
        ROOT
        / "fantasy_football_data_scripts"
        / "multi_league"
        / "core"
        / "league_update_ownership.py"
    ).read_text(encoding="utf-8")

    assert 'merge_provider_refresh_table(\n            local_db,\n            "player_fantasy"' in text
    assert '"player_fantasy": ("db_name", "player_week")' in ownership
    assert '"sleeper_player_id_original"' not in ownership
    assert "validate_tabular_active_scope(" in text
    assert text.index("validate_tabular_active_scope(") < text.index("bundle = build_fleet_partition_bundle(")


def test_espn_refresh_validates_raw_roster_team_coverage_before_safe_filtering():
    text = (ROOT / "scripts" / "refresh_espn_active_season.py").read_text(encoding="utf-8")
    assert "validate_active_roster_frame(" in text
    assert "validate_provider_team_inventory(" in text
    assert text.index("validate_active_roster_frame(") < text.index("filter_rosters_to_finalized_games(")
    assert text.index("validate_active_roster_frame(") < text.index("bundle = build_fleet_partition_bundle(")
    assert "espn_schedule_is_final(schedule_rows, expected_team_ids=expected_team_ids)" in text
    assert "validate_espn_final_matchup_frame(" in text
    assert text.index("validate_espn_final_matchup_frame(") < text.index(
        'merge_provider_refresh_table(\n            local_db,\n            "matchup"'
    )


def test_yahoo_refresh_uses_already_fetched_team_keys_to_gate_raw_roster_coverage():
    text = (ROOT / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    assert 'rosters.attrs.get("expected_team_keys")' in text
    assert "validate_provider_team_inventory(" in text
    assert "validate_active_roster_frame(" in text
    assert text.index("validate_active_roster_frame(") < text.index("filter_rosters_to_finalized_games(")
    assert text.index("validate_active_roster_frame(") < text.index("bundle = build_fleet_partition_bundle(")


def test_yahoo_incomplete_source_failure_is_actionable_and_preserves_history():
    workflow = (ROOT / ".github" / "workflows" / "yahoo_incremental_refresh_worker.yml").read_text(
        encoding="utf-8"
    )
    assert 'if [ "${recovery_status}" = "incomplete_source" ]; then' in workflow
    assert "Yahoo did not return complete active-season data; historical data was preserved" in workflow


def test_yahoo_refresh_accepts_only_complete_declared_postseason_pair_graph():
    text = (ROOT / "scripts" / "refresh_yahoo_active_season.py").read_text(encoding="utf-8")
    assert "validate_yahoo_week_matchup_scope(" in text
    assert 'settings_row.iloc[0].get("playoff_start_week")' in text
    assert "final_matchup_weeks += 1" in text
    assert text.index("validate_yahoo_week_matchup_scope(") < text.index(
        'merge_provider_refresh_table(\n                local_db,\n                "matchup"'
    )
