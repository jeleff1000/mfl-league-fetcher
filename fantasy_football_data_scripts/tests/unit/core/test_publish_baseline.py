"""Offline tests for scripts/publish_baseline.py (capture + scope diff)."""

import importlib.util
import json
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(scope="module")
def baseline_mod():
    path = ROOT / "scripts" / "publish_baseline.py"
    spec = importlib.util.spec_from_file_location("publish_baseline_test_mod", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["publish_baseline_test_mod"] = module
    spec.loader.exec_module(module)
    return module


ACTIVE_YEAR = 2026
PRIOR_YEAR = 2025


def _make_db(
    path: Path,
    *,
    bump: float = 0.0,
    active_extra_week: bool = False,
    touch_prior: bool = False,
    touch_excluded: bool = False,
):
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR, team_points DOUBLE
        )
        """
    )
    conn.execute("CREATE TABLE public.matchup_career (db_name VARCHAR, franchise_id VARCHAR, wins INTEGER)")
    for league in ("league_a", "league_b", "league_c"):
        in_batch = league != "league_c"
        for year in (PRIOR_YEAR, ACTIVE_YEAR):
            weeks = [1, 2]
            if active_extra_week and year == ACTIVE_YEAR and in_batch:
                weeks.append(3)
            for week in weeks:
                points = 100.0 + (bump if (in_batch and year == ACTIVE_YEAR) else 0.0)
                if touch_prior and year == PRIOR_YEAR and league == "league_a":
                    points += 999.0  # simulated out-of-scope mutation
                if touch_excluded and year == ACTIVE_YEAR and league == "league_c":
                    points += 555.0  # excluded league wrongly modified
                conn.execute(
                    "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
                    [league, year, week, f"m_{year}_{week}", points],
                )
        conn.execute(
            "INSERT INTO public.matchup_career VALUES (?, ?, ?)",
            [league, "fid_1", 5 + (int(bump) if in_batch else 0)],
        )
    conn.close()


def _capture(baseline_mod, db_path: Path, out: Path, level: str = "full"):
    args = type(
        "Args",
        (),
        {
            "local": str(db_path),
            "fly": False,
            "database": "___leagues",
            "schema": "public",
            "tables": None,
            "level": level,
            "out": str(out),
        },
    )()
    assert baseline_mod.cmd_capture(args) == 0
    return json.loads(out.read_text(encoding="utf-8"))


def _diff(baseline_mod, before: Path, after: Path, manifest: Path | None = None, out: Path | None = None):
    args = type(
        "Args",
        (),
        {
            "before": str(before),
            "after": str(after),
            "manifest": str(manifest) if manifest else None,
            "out": str(out) if out else None,
            "max_changes": 500,
        },
    )()
    return baseline_mod.cmd_diff(args)


def _write_manifest(path: Path):
    manifest = {
        "db_names": ["league_a", "league_b"],
        "tables": [
            {
                "table": "matchup",
                "cadence_class": "active_season",
                "scope": {"year": ACTIVE_YEAR},
                "db_names_hash": "x",
                "db_name_count": 2,
            },
            {
                "table": "matchup_career",
                "cadence_class": "league_rollup",
                "scope": {},
                "db_names_hash": "x",
                "db_name_count": 2,
            },
        ]
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")


def test_identical_databases_diff_clean(baseline_mod, tmp_path):
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb")
    before = _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json")
    after = _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json")
    assert before["tables"]["matchup"]["total_rows"] == 12
    assert _diff(baseline_mod, tmp_path / "before.json", tmp_path / "after.json") == 0


def test_scoped_publish_changes_only_declared_scopes(baseline_mod, tmp_path):
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb", bump=7.0, active_extra_week=True)
    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json")
    _write_manifest(tmp_path / "manifest.json")

    # Without manifest: changes exist -> exit 1.
    assert _diff(baseline_mod, tmp_path / "before.json", tmp_path / "after.json") == 1
    # With manifest: all changes inside declared scope -> exit 0.
    rc = _diff(
        baseline_mod,
        tmp_path / "before.json",
        tmp_path / "after.json",
        manifest=tmp_path / "manifest.json",
        out=tmp_path / "report.json",
    )
    assert rc == 0
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["violation_count"] == 0
    changed_tables = {c["table"] for c in report["changes"]}
    assert changed_tables == {"matchup", "matchup_career"}
    # league_c (excluded from batch) active-season scope must not appear.
    assert not any(
        c.get("scope", {}).get("db_name") == "league_c" and c["table"] == "matchup" for c in report["changes"]
    )


def test_out_of_scope_mutation_is_a_violation(baseline_mod, tmp_path):
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb", bump=7.0, active_extra_week=True, touch_prior=True)
    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json")
    _write_manifest(tmp_path / "manifest.json")

    rc = _diff(
        baseline_mod,
        tmp_path / "before.json",
        tmp_path / "after.json",
        manifest=tmp_path / "manifest.json",
        out=tmp_path / "report.json",
    )
    assert rc == 1
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["violation_count"] == 1
    [violation] = report["violations"]
    assert violation["table"] == "matchup"
    assert violation["scope"] == {"db_name": "league_a", "year": str(PRIOR_YEAR)}


def test_light_capture_detects_row_count_changes_only(baseline_mod, tmp_path):
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb", bump=7.0)  # value change, same row counts
    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json", level="light")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json", level="light")
    # Light level cannot see pure value changes — documents why rehearsals use full.
    assert _diff(baseline_mod, tmp_path / "before.json", tmp_path / "after.json") == 0

    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before_full.json", level="full")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after_full.json", level="full")
    assert _diff(baseline_mod, tmp_path / "before_full.json", tmp_path / "after_full.json") == 1


def test_table_added_is_flagged(baseline_mod, tmp_path):
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb")
    conn = duckdb.connect(str(tmp_path / "b.duckdb"))
    conn.execute("CREATE TABLE public.surprise (db_name VARCHAR)")
    conn.execute("INSERT INTO public.surprise VALUES ('league_a')")
    conn.close()
    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json")
    _write_manifest(tmp_path / "manifest.json")
    rc = _diff(baseline_mod, tmp_path / "before.json", tmp_path / "after.json", manifest=tmp_path / "manifest.json")
    assert rc == 1


def test_excluded_league_mutation_is_a_violation(baseline_mod, tmp_path):
    """The manifest's db_names list bounds membership: a change to a league
    that was not in the batch is flagged even when the year matches."""
    _make_db(tmp_path / "a.duckdb")
    _make_db(tmp_path / "b.duckdb", bump=7.0, touch_excluded=True)
    _capture(baseline_mod, tmp_path / "a.duckdb", tmp_path / "before.json")
    _capture(baseline_mod, tmp_path / "b.duckdb", tmp_path / "after.json")
    _write_manifest(tmp_path / "manifest.json")

    rc = _diff(
        baseline_mod,
        tmp_path / "before.json",
        tmp_path / "after.json",
        manifest=tmp_path / "manifest.json",
        out=tmp_path / "report.json",
    )
    assert rc == 1
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["violation_count"] == 1
    [violation] = report["violations"]
    assert violation["scope"]["db_name"] == "league_c"
