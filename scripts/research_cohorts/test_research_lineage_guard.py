from pathlib import Path

import pytest

from research_lineage_guard import check_workflow_text
from research_lineage_policy import CANONICAL_RESEARCH_MATCHUP_CACHE_KEY


def test_dynamic_output_cache_write_is_rejected(tmp_path: Path):
    text = """
    uses: actions/cache/save@v5
    with:
      key: ${{ inputs.output_cache_key }}
    """
    violations = check_workflow_text(tmp_path / "research_bad.yml", text)
    assert any("output_cache_key" in item for item in violations)


def test_run_specific_cache_write_is_rejected(tmp_path: Path):
    text = f"""
    uses: actions/cache/save@v5
    with:
      key: {CANONICAL_RESEARCH_MATCHUP_CACHE_KEY}-${{{{ github.run_id }}}}
    """
    violations = check_workflow_text(tmp_path / "research_bad.yml", text)
    assert violations


def test_canonical_cache_write_is_accepted(tmp_path: Path):
    text = f"""
    uses: actions/cache/save@v5
    with:
      key: {CANONICAL_RESEARCH_MATCHUP_CACHE_KEY}
    """
    assert check_workflow_text(tmp_path / "research_good.yml", text) == []


def test_restore_only_workflow_is_not_a_lineage_creation_surface(tmp_path: Path):
    text = """
    uses: actions/cache/restore@v5
    with:
      key: an-old-key
    """
    assert check_workflow_text(tmp_path / "restore.yml", text) == []


def test_unrelated_cache_workflow_is_not_a_research_lineage_surface(tmp_path: Path):
    text = "uses: actions/cache/save@v5\nkey: unrelated-cache-${{ github.run_id }}\n"
    assert check_workflow_text(tmp_path / "seed_ops_cache.yml", text) == []
