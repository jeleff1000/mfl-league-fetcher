import ast
from pathlib import Path


SOURCE = Path(__file__).with_name("build_weekly_txn.py")


def test_weekly_transaction_builder_is_import_safe_and_uses_shared_year_contract():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))

    top_level_calls = [
        node for node in tree.body
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
    ]
    assert not any(
        isinstance(node.value.func, ast.Name) and node.value.func.id == "main"
        for node in top_level_calls
    )

    text = SOURCE.read_text(encoding="utf-8")
    assert "configured_years" in text
    assert "for y in YEARS" in text
    assert 'if __name__ == "__main__":' in text


def test_weekly_transaction_builder_preserves_all_format_dimensions():
    text = SOURCE.read_text(encoding="utf-8")

    for dimension in ("league_type", "lineup_mode", "keeper_mode", "format_level"):
        assert dimension in text
    assert "NOT COALESCE(is_dynasty" not in text
    assert "NOT COALESCE(sleeper_best_ball" not in text


def test_weekly_transaction_builder_emits_lossless_merge_components():
    text = SOURCE.read_text(encoding="utf-8")

    for component in (
        "sum_add_lamar", "n_add_lamar", "sum_faab_pct", "n_faab_pct",
        "sum_faab_bid", "n_faab_bid",
    ):
        assert component in text
