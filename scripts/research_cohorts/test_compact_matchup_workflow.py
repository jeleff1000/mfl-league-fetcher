from pathlib import Path


def test_compact_15_runner_exposes_private_code_to_python_imports():
    """The compact builder imports ``scripts.*`` from the private checkout."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    build = text.split("\n  build:\n", 1)[1].split("\n  merge:\n", 1)[0]
    assert "PYTHONPATH: ${{ github.workspace }}/code" in build


def test_compact_15_runner_merge_exposes_private_code_to_python_imports():
    """The final merger imports the same private ``scripts`` package."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    merge = text.split("\n  merge:\n", 1)[1]
    assert "PYTHONPATH: ${{ github.workspace }}/code" in merge


def test_compact_15_runner_fans_out_career_from_only_the_compact_season_artifact():
    """Career wall time must not serialize independent player hash buckets."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    career = text.split("\n  career:\n", 1)[1].split("\n  merge:\n", 1)[0]
    assert "max-parallel: 15" in career
    assert "compact-matchup-season-source-${{ github.run_id }}" in career
    assert "--season-bundle season-source/research_matchup_compact_season_source.duckdb" in career
    assert "actions/cache/restore" not in career


def test_compact_15_runner_treats_position_as_part_of_the_outer_key():
    """A real two-way player is one compact row per canonical UI position."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "SELECT NFL_player_id,year,week,position FROM research_matchup_compact_weekly" in text
    assert "AND position='RB'" in text


def test_compact_runner_supports_a_single_position_fail_fast_pilot():
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "positions:" in text
    assert "runners:" in text
    assert "--positions '${{ inputs.positions }}'" in text
    assert "--runners '${{ inputs.runners }}'" in text
    assert 'test "${#LANES[@]}" -eq ${{ needs.plan.outputs.lane_count }}' in text
    assert "if 2025 <= int('${{ inputs.year_end }}') and 2025 >= int('${{ inputs.year_start }}') and (not positions or 'RB' in positions):" in text


def test_capacity_runner_emits_the_child_duckdb_error_before_failing_a_lane():
    """A failed capacity task must identify itself and retain stderr evidence."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "capacity_task_failed=" in text
    assert "completed.stderr" in text
    assert "check=False" in text


def test_capacity_summary_rejects_inconsistent_pooled_team_lanes():
    """Unknown team count pools are valid; mixed ALL/literal lanes are not."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "invalid_pooled_lanes" in text


def test_compact_runner_gates_cmc_2019_season_against_its_valid_weekly_cells():
    """A bye week may not be retained in the season denominator."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "cmc_2019_weekly" in text
    assert "cmc_2019_season" in text
    assert "assert cmc_2019_season == cmc_2019_weekly[:5]" in text


def test_compact_runner_gates_mark_ingram_pooling_and_2017_clutch_champ_sanity():
    """Thin PPR must pool, while elite clutch/champ seasons remain plausibly aligned."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "mark_ingram_2017" in text
    assert "00-0027966" in text
    assert "q_scoring == 'ALL'" in text
    assert "clutch_champ_overlap" in text
    assert "assert clutch_champ_overlap >= 3" in text
    assert "champ_rate_pct <= playoff_rate_pct" in text


def test_compact_runner_low_sample_gate_uses_a_nonreserved_sql_alias():
    """The final compact gate must parse in DuckDB before it can protect serving."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_balanced_15.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "SELECT sample_source, COUNT(*)" in text
    assert "'weekly_core' AS sample_source" in text
    assert "SELECT 'weekly_core' source" not in text


def test_compact_merge_replay_reuses_completed_compact_artifacts_without_restoring_cache():
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_merge_existing.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "source_run_id:" in text
    assert "run-id: ${{ inputs.source_run_id }}" in text
    assert "compact-matchup-base-${{ inputs.source_run_id }}" in text
    assert "compact-matchup-season-source-${{ inputs.source_run_id }}" in text
    assert "career_run_id:" in text
    assert "run-id: ${{ inputs.career_run_id || github.run_id }}" in text
    assert "actions/cache/restore" not in text
    assert "research_public_lake" not in text


def test_compact_merge_replay_fans_out_career_from_preserved_season_source():
    """Recovery must parallelize career without reading the cache or raw lanes again."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_merge_existing.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    career = text.split("\n  career:\n", 1)[1].split("\n  merge:\n", 1)[0]
    assert "fail-fast: false" in career
    assert "max-parallel: 15" in career
    assert "compact-matchup-season-source-${{ inputs.source_run_id }}" in career
    assert "--career-buckets 15" in career
    assert "--player-sub-buckets '${{ matrix.bucket == 12 && 4 || 1 }}'" in career
    assert "pattern: compact-matchup-career-*-${{ inputs.career_run_id || github.run_id }}" in text


def test_compact_merge_replay_uses_current_cmc_gate_and_publishable_artifact_name():
    """Replay must use the current golden cell and publish's exact artifact contract."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_merge_existing.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "cmc==(9837,9766,9600,9579,6019.0),(cmc,cmc_candidates)" in text
    assert "cmc==(9980,158761,168520,155348,149718,91526.0,17,0),cmc" not in text
    assert "name: compact-matchup-final-${{ github.run_id }}" in text


def test_compact_reindex_repair_reuses_only_a_completed_compact_artifact():
    """Selector repair must avoid raw-cache restoration and require full preflight."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_reindex_existing.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "source_run_id:" in text
    assert "artifact_name:" in text
    assert "run-id: ${{ inputs.source_run_id }}" in text
    assert "core_selector_indices" in text
    assert "grade_selector_indices" in text
    assert "preflight_bundle(con, require_full_history=True)" in text
    assert "actions/cache/restore" not in text
    assert "research_public_lake" not in text


def test_compact_denominator_probe_is_read_only_and_reports_cmc_2019_week_17():
    """The suspected final-week denominator must be proved from immutable input."""
    workflow = (
        Path(__file__).parents[2]
        / ".github"
        / "workflows"
        / "research_matchup_compact_denominator_probe.yml"
    )
    text = workflow.read_text(encoding="utf-8")

    assert "actions/cache/restore@v5" in text
    assert "CAST(year AS INTEGER)=2019" in text
    assert "CAST(week AS INTEGER)=17" in text
    assert "00-0033280" in text
    assert "player_team_game_week" in text
    assert "player_active_week" in text
    assert "nfl_player_stats_all" in text
    assert "mark ingram" in text
    assert "actions/cache/save" not in text
    assert "fly" not in text.lower()
