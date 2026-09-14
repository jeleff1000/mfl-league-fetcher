from pathlib import Path

import yaml


def _mirrored(private: Path, worker: Path) -> bool:
    """Do the two repos hold the same workflow?

    Compare NEWLINE-NORMALIZED text, not raw bytes. league-history-workers has
    core.autocrlf=true, so git checks its copy out as CRLF while this repo's stays LF --
    the git blobs are identical and `git diff` is empty. Asserting on raw bytes was
    asserting on a checkout artifact, and any `git revert`/re-checkout over there broke
    this test without a single character of content having drifted.
    """
    return (private.read_text(encoding="utf-8").splitlines()
            == worker.read_text(encoding="utf-8").splitlines())


def test_weekly_transaction_shards_are_mirrored_and_bounded():
    private = Path(".github/workflows/research_cohort_shards.yml")
    worker = Path("league-history-workers/.github/workflows/research_cohort_shards.yml")

    assert _mirrored(private, worker)
    text = private.read_text(encoding="utf-8")
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


def test_matchup_rescue_shards_are_mirrored_and_partition_cleanly():
    """The rescue workflow is the only thing that shards a single year, so its bucket
    matrix and RESEARCH_LEAGUE_BUCKETS must agree exactly -- a mismatch would produce a
    partition that covers some leagues twice and others not at all, and the assembler's
    cover check would (correctly) refuse the whole build."""
    private = Path(".github/workflows/research_matchup_rescue_shards.yml")
    worker = Path(
        "league-history-workers/.github/workflows/research_matchup_rescue_shards.yml")

    assert _mirrored(private, worker), "rescue workflow mirror drifted"
    parsed = yaml.safe_load(private.read_text(encoding="utf-8"))
    job = parsed["jobs"]["matchup"]
    buckets = job["strategy"]["matrix"]["bucket"]
    declared = int(job["env"]["RESEARCH_LEAGUE_BUCKETS"])

    assert sorted(buckets) == list(range(declared)), (
        f"matrix buckets {buckets} do not cover 0..{declared - 1} exactly once")
    assert job["strategy"]["max-parallel"] == 15
    assert job["strategy"]["max-parallel"] % declared == 0

    inputs = parsed.get("on", parsed.get(True))["workflow_dispatch"]["inputs"]
    # Every 2018-2025 season is a five-bucket population build.  The workflow
    # accepts an explicit JSON list so three years (15 buckets) can run at once.
    assert "years" in inputs
    assert inputs["years"]["default"] == '["2025","2024","2023","2022","2021","2020","2019","2018"]'
    assert job["strategy"]["max-parallel"] == 15
    assert "year: ${{ fromJSON(inputs.years) }}" in private.read_text(encoding="utf-8")


def test_normal_matrix_never_builds_modern_matchup_years_whole():
    """2018-2025 belong exclusively to the five-way matchup partition."""
    parsed = yaml.safe_load(Path(".github/workflows/research_cohort_shards.yml").read_text())
    matrix = parsed["jobs"]["shard"]["strategy"]["matrix"]["range"]
    # This workflow stores the JSON expression as a string, so assert against the
    # default source rather than evaluating an Actions expression.
    assert '"2018"' not in matrix
    assert '"2025"' not in matrix
