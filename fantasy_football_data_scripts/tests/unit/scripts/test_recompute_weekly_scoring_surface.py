from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load_script_module():
    repo_root = Path(__file__).resolve().parents[4]
    script_path = repo_root / "scripts" / "recompute_weekly_scoring_surface_20260513.py"
    spec = importlib.util.spec_from_file_location(
        "recompute_weekly_scoring_surface_20260513_test",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fetch_rows_uses_read_only_fly_reader() -> None:
    mod = load_script_module()
    calls: list[tuple[str, str]] = []

    class Reader:
        def query(self, sql: str, database: str) -> list[dict]:
            calls.append((sql, database))
            return [{"ok": 1}]

    rows = mod.fetch_rows(Reader(), "SELECT 1")

    assert rows == [{"ok": 1}]
    assert calls == [("SELECT 1", "___ops")]


def test_verify_only_main_does_not_construct_writer(monkeypatch) -> None:
    mod = load_script_module()
    reader = object()
    seen: dict[str, object] = {}

    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "FlyReader", lambda: reader)
    monkeypatch.setattr(
        mod,
        "LongFlyWriter",
        lambda: (_ for _ in ()).throw(AssertionError("verify should not create a writer")),
    )
    monkeypatch.setattr(mod, "column_names", lambda actual_reader: {"player_week", mod.SENTINEL_COL})

    def fake_verify(actual_reader, existing, ranges):
        seen["reader"] = actual_reader
        seen["existing"] = existing
        seen["ranges"] = ranges
        return True

    monkeypatch.setattr(mod, "cmd_verify", fake_verify)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recompute_weekly_scoring_surface_20260513.py",
            "--verify",
            "--ranges",
            "2020-2025",
        ],
    )

    assert mod.main() == 0
    assert seen == {
        "reader": reader,
        "existing": {"player_week", mod.SENTINEL_COL},
        "ranges": [(2020, 2025)],
    }


def test_def_rows_zero_generic_misc_return_td_components() -> None:
    mod = load_script_module()
    formula_map = mod.formulas(
        {
            "position",
            "nfl_position",
            "special_teams_tds",
            "fum_ret_td",
            "passing_2pt_conversions",
            "rushing_2pt_conversions",
            "receiving_2pt_conversions",
        }
    )

    for col in ("pts_misc", "pts_st_td_6", "pts_fum_ret_td_6"):
        sql = formula_map[col]
        assert "CASE WHEN UPPER(TRIM" in sql
        assert "= 'DEF' THEN 0 ELSE" in sql


def test_default_dst_formula_excludes_forced_fumbles() -> None:
    mod = load_script_module()
    formula_map = mod.formulas({"def_fumbles_forced"})

    assert "def_fumbles_forced" not in formula_map["pts_def_std"]
    assert "def_fumbles_forced" in formula_map["pts_def_ff"]
