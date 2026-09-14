from pathlib import Path

import yaml


def test_weekly_transaction_shards_are_bounded():
    workflow = Path(".github/workflows/research_cohort_shards.yml")
    text = workflow.read_text(encoding="utf-8")
    parsed = yaml.safe_load(text)
    triggers = parsed.get("on", parsed.get(True))
    table_input = triggers["workflow_dispatch"]["inputs"]["tables"]

    assert "transactions_weekly" in table_input["options"]
    # 15 wide since the 2026-07-26 re-shard: matchup fans out across the year_ranges input
    # instead of the hardcoded ten-range matrix.
    assert parsed["jobs"]["shard"]["strategy"]["max-parallel"] == 15
    assert "year_ranges" in triggers["workflow_dispatch"]["inputs"]
    assert (
        "transactions_weekly) "
        "python code/scripts/research_cohorts/build_weekly_txn.py ;;"
    ) in text
    assert (
        "research-${{ matrix.table }}-${{ matrix.range.label }}-${{ github.sha }}"
    ) in text


def test_matchup_rescue_shards_partition_cleanly():
    """The rescue workflow is the only thing that shards a single year, so its bucket
    matrix and RESEARCH_LEAGUE_BUCKETS must agree exactly -- a mismatch would produce a
    partition that covers some leagues twice and others not at all, and the assembler's
    cover check would (correctly) refuse the whole build."""
    workflow = Path(".github/workflows/research_matchup_rescue_shards.yml")
    text = workflow.read_text(encoding="utf-8")
    parsed = yaml.safe_load(text)
    inputs = parsed.get("on", parsed.get(True))["workflow_dispatch"]["inputs"]
    job = parsed["jobs"]["matchup"]
    buckets = yaml.safe_load(inputs["buckets"]["default"])
    declared = int(inputs["bucket_count"]["default"])

    assert sorted(buckets) == list(range(declared)), (
        f"matrix buckets {buckets} do not cover 0..{declared - 1} exactly once")
    assert job["strategy"]["max-parallel"] == "${{ fromJSON(inputs.max_parallel) }}"
    assert int(inputs["max_parallel"]["default"]) <= declared

    # Every 2018-2025 season is a five-bucket population build.  The workflow
    # accepts an explicit JSON list so three years (15 buckets) can run at once.
    assert "years" in inputs
    assert inputs["years"]["default"] == '["2025","2024","2023","2022","2021","2020","2019","2018"]'
    assert "year: ${{ fromJSON(inputs.years) }}" in text


def test_normal_matrix_never_builds_modern_matchup_years_whole():
    """2018-2025 belong exclusively to the five-way matchup partition."""
    parsed = yaml.safe_load(Path(".github/workflows/research_cohort_shards.yml").read_text())
    matrix = parsed["jobs"]["shard"]["strategy"]["matrix"]["range"]
    # This workflow stores the JSON expression as a string, so assert against the
    # default source rather than evaluating an Actions expression.
    assert '"2018"' not in matrix
    assert '"2025"' not in matrix
