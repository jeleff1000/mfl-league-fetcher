from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb


def test_merge_validation_input_list_excludes_unrelated_sidecars_only_when_requested(tmp_path: Path) -> None:
    from scripts.research_cohorts.validate_research_public_lake import required_input_paths

    full = required_input_paths(tmp_path, require_auxiliary_sidecars=True)
    merge_only = required_input_paths(tmp_path, require_auxiliary_sidecars=False)

    assert tmp_path / "corpus_snapshot.duckdb" in full
    assert tmp_path / "ops_cache.duckdb" in full
    assert tmp_path / "nfl_market_adp.parquet" in full
    assert tmp_path / "ladder_thresholds.json" in full
    assert tmp_path / "nfl_market_adp.parquet" not in merge_only
    assert tmp_path / "ladder_thresholds.json" not in merge_only


def _create_db(path: Path, rows: list[tuple[str, int, str, int]]) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    for table in ("league_settings", "matchup", "player_fantasy"):
        con.execute(
            f"CREATE TABLE public.{table} (db_name VARCHAR, year INTEGER, marker VARCHAR, value INTEGER)"
        )
        con.executemany(f"INSERT INTO public.{table} VALUES (?, ?, ?, ?)", rows)
    con.close()


def _create_mfl_chunk(path: Path, rows: list[tuple[str, int, str, int]]) -> None:
    con = duckdb.connect(str(path))
    for table in ("league_settings", "matchup", "player_fantasy"):
        con.execute(
            f"CREATE TABLE {table} (db_name VARCHAR, year INTEGER, marker VARCHAR, value INTEGER)"
        )
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows)
    con.close()


def _create_wide_mfl_chunk(path: Path, rows: list[tuple[str, int, str, int]]) -> None:
    con = duckdb.connect(str(path))
    for table in ("league_settings", "matchup", "player_fantasy"):
        con.execute(
            f"CREATE TABLE {table} (db_name VARCHAR, year INTEGER, marker VARCHAR, value INTEGER, mfl_only VARCHAR)"
        )
        con.executemany(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, 'mfl-extra')",
            rows,
        )
    con.close()


def _write_mfl_index(path: Path, rows: list[tuple[str, int, str, int]]) -> None:
    path.write_text(
        json.dumps(
            {
                "leagues": [
                    {"season": year, "league_id": db_name.rsplit("_", 1)[-1], "db_name": db_name}
                    for db_name, year, _, _ in rows
                ],
                "accepted_by_year": {
                    str(year): sum(1 for _, row_year, _, _ in rows if row_year == year)
                    for year in sorted({year for _, year, _, _ in rows})
                },
                "league_count": len(rows),
            }
        ),
        encoding="utf-8",
    )


def test_full_mfl_assembler_unions_physical_recovery_and_population_chunks(tmp_path: Path) -> None:
    recovery = tmp_path / "recovery.duckdb"
    recovery_index = tmp_path / "recovery.json"
    population_root = tmp_path / "population"
    batch_dir = population_root / "batch-a"
    (batch_dir / "chunk_state").mkdir(parents=True)
    (batch_dir / "index_state").mkdir()
    output = tmp_path / "full_mfl.duckdb"
    output_index = tmp_path / "full_mfl_index.json"
    recovery_rows = [("smpl_mfl_2004_1", 2004, "recovery", 1)]
    batch_rows = [
        ("smpl_mfl_2004_2", 2004, "batch", 2),
        ("smpl_mfl_2004_3", 2004, "batch", 3),
    ]
    _create_mfl_chunk(recovery, recovery_rows)
    _create_mfl_chunk(batch_dir / "chunk_state" / "mfl_register_chunk.duckdb", batch_rows)
    _write_mfl_index(recovery_index, recovery_rows)
    _write_mfl_index(batch_dir / "index_state" / "mfl_register_all_runs.json", batch_rows)

    script = Path(__file__).with_name("assemble_protected_mfl_sources.py")
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--recovery", str(recovery),
            "--recovery-index", str(recovery_index),
            "--population-root", str(population_root),
            "--out", str(output),
            "--out-index", str(output_index),
            "--expected-ledger", json.dumps({"2004": 3}),
            "--expected-population-total", "2",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    proof = json.loads(output.with_name("full_mfl_proof.json").read_text(encoding="utf-8"))
    assert proof["recovery_league_years"] == 1
    assert proof["population_league_years"] == 2
    assert proof["protected_mfl_league_years"] == 3
    index = json.loads(output_index.read_text(encoding="utf-8"))
    assert index["accepted_by_year"] == {"2004": 3}
    con = duckdb.connect(str(output), read_only=True)
    for table in ("league_settings", "matchup", "player_fantasy"):
        assert con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 3
    con.close()


def test_full_mfl_assembler_skips_equivalent_overlapping_sources(tmp_path: Path) -> None:
    recovery = tmp_path / "recovery.duckdb"
    recovery_index = tmp_path / "recovery.json"
    population_root = tmp_path / "population"
    batch_dir = population_root / "batch-a"
    (batch_dir / "chunk_state").mkdir(parents=True)
    (batch_dir / "index_state").mkdir()
    output = tmp_path / "full_mfl.duckdb"
    output_index = tmp_path / "full_mfl_index.json"
    recovery_rows = [("smpl_mfl_2004_1", 2004, "recovery", 9)]
    batch_rows = [
        ("smpl_mfl_2004_1", 2004, "recovery", 9),
        ("smpl_mfl_2004_2", 2004, "batch", 2),
    ]
    _create_mfl_chunk(recovery, recovery_rows)
    _create_mfl_chunk(batch_dir / "chunk_state" / "mfl_register_chunk.duckdb", batch_rows)
    _write_mfl_index(recovery_index, recovery_rows)
    _write_mfl_index(batch_dir / "index_state" / "mfl_register_all_runs.json", batch_rows)

    script = Path(__file__).with_name("assemble_protected_mfl_sources.py")
    result = subprocess.run(
        [
            sys.executable, str(script),
            "--recovery", str(recovery),
            "--recovery-index", str(recovery_index),
            "--population-root", str(population_root),
            "--out", str(output),
            "--out-index", str(output_index),
            "--expected-ledger", json.dumps({"2004": 2}),
            "--expected-population-total", "2",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    assert "overlapping_league_years=1" in result.stdout
    proof = json.loads(output.with_name("full_mfl_proof.json").read_text(encoding="utf-8"))
    assert proof["overlapping_league_years"] == 1
    con = duckdb.connect(str(output), read_only=True)
    assert con.execute(
        "SELECT marker, value FROM league_settings ORDER BY db_name"
    ).fetchall() == [("recovery", 9), ("batch", 2)]
    con.close()


def test_full_mfl_assembler_rejects_conflicting_overlapping_sources(tmp_path: Path) -> None:
    recovery = tmp_path / "recovery.duckdb"
    recovery_index = tmp_path / "recovery.json"
    population_root = tmp_path / "population"
    batch_dir = population_root / "batch-a"
    (batch_dir / "chunk_state").mkdir(parents=True)
    (batch_dir / "index_state").mkdir()
    output = tmp_path / "full_mfl.duckdb"
    output_index = tmp_path / "full_mfl_index.json"
    recovery_rows = [("smpl_mfl_2004_1", 2004, "recovery", 9)]
    batch_rows = [
        ("smpl_mfl_2004_1", 2004, "conflicting-batch", 1),
        ("smpl_mfl_2004_2", 2004, "batch", 2),
    ]
    _create_mfl_chunk(recovery, recovery_rows)
    _create_mfl_chunk(batch_dir / "chunk_state" / "mfl_register_chunk.duckdb", batch_rows)
    _write_mfl_index(recovery_index, recovery_rows)
    _write_mfl_index(batch_dir / "index_state" / "mfl_register_all_runs.json", batch_rows)

    script = Path(__file__).with_name("assemble_protected_mfl_sources.py")
    result = subprocess.run(
        [
            sys.executable, str(script),
            "--recovery", str(recovery),
            "--recovery-index", str(recovery_index),
            "--population-root", str(population_root),
            "--out", str(output),
            "--out-index", str(output_index),
            "--expected-ledger", json.dumps({"2004": 2}),
            "--expected-population-total", "2",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "conflicting MFL source rows" in result.stderr


def test_full_mfl_assembler_skips_wholly_equivalent_source(tmp_path: Path) -> None:
    recovery = tmp_path / "recovery.duckdb"
    recovery_index = tmp_path / "recovery.json"
    population_root = tmp_path / "population"
    batch_dir = population_root / "batch-a"
    (batch_dir / "chunk_state").mkdir(parents=True)
    (batch_dir / "index_state").mkdir()
    output = tmp_path / "full_mfl.duckdb"
    output_index = tmp_path / "full_mfl_index.json"
    rows = [("smpl_mfl_2004_1", 2004, "same", 9)]
    _create_mfl_chunk(recovery, rows)
    _create_mfl_chunk(batch_dir / "chunk_state" / "mfl_register_chunk.duckdb", rows)
    _write_mfl_index(recovery_index, rows)
    _write_mfl_index(batch_dir / "index_state" / "mfl_register_all_runs.json", rows)

    script = Path(__file__).with_name("assemble_protected_mfl_sources.py")
    result = subprocess.run(
        [
            sys.executable, str(script),
            "--recovery", str(recovery),
            "--recovery-index", str(recovery_index),
            "--population-root", str(population_root),
            "--out", str(output),
            "--out-index", str(output_index),
            "--expected-ledger", json.dumps({"2004": 1}),
            "--expected-population-total", "1",
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    assert "overlapping_league_years=1" in result.stdout
    con = duckdb.connect(str(output), read_only=True)
    assert con.execute("SELECT COUNT(*) FROM player_fantasy").fetchone()[0] == 1
    con.close()


def test_authoritative_mfl_league_year_replaces_only_its_stale_corpus_rows(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    mfl = tmp_path / "mfl.duckdb"
    out = tmp_path / "out.duckdb"
    index = tmp_path / "mfl_register_all_runs.json"
    _create_db(
        base,
        [
            ("smpl_mfl_2004_1", 2004, "old-mfl", 1),
            ("yahoo_2004_1", 2004, "keep-yahoo", 2),
        ],
    )
    _create_mfl_chunk(mfl, [("smpl_mfl_2004_1", 2004, "new-mfl", 9)])
    index.write_text(
        json.dumps(
            {
                "leagues": [{"season": 2004, "league_id": "1", "db_name": "smpl_mfl_2004_1"}],
                "accepted_by_year": {"2004": 1},
                "league_count": 1,
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).with_name("merge_research_with_mfl.py")
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--base",
            str(base),
            "--mfl",
            str(mfl),
            "--mfl-index",
            str(index),
            "--out",
            str(out),
            "--expected-ledger",
            json.dumps({"2004": 1}),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    con = duckdb.connect(str(out), read_only=True)
    for table in ("league_settings", "matchup", "player_fantasy"):
        assert con.execute(f"SELECT marker, value FROM public.{table} ORDER BY db_name").fetchall() == [
            ("new-mfl", 9),
            ("keep-yahoo", 2),
        ]
    con.close()


def test_merge_projects_wider_mfl_schema_into_canonical_base_schema(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    mfl = tmp_path / "mfl.duckdb"
    out = tmp_path / "out.duckdb"
    index = tmp_path / "mfl_register_all_runs.json"
    _create_db(base, [("smpl_mfl_2004_1", 2004, "old", 1)])
    _create_wide_mfl_chunk(mfl, [("smpl_mfl_2004_1", 2004, "new", 9)])
    index.write_text(
        json.dumps(
            {
                "leagues": [{"season": 2004, "league_id": "1", "db_name": "smpl_mfl_2004_1"}],
                "accepted_by_year": {"2004": 1},
                "league_count": 1,
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).with_name("merge_research_with_mfl.py")
    result = subprocess.run(
        [
            sys.executable, str(script), "--base", str(base), "--mfl", str(mfl),
            "--mfl-index", str(index), "--out", str(out), "--expected-ledger", json.dumps({"2004": 1}),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    assert "[mfl-merge] phase=table_complete table=league_settings" in result.stdout
    con = duckdb.connect(str(out), read_only=True)
    assert [row[0] for row in con.execute("DESCRIBE public.league_settings").fetchall()] == [
        "db_name", "year", "marker", "value"
    ]
    assert con.execute("SELECT marker, value FROM public.league_settings").fetchall() == [("new", 9)]
    con.close()


def test_merge_preserves_base_rows_when_one_mfl_table_lacks_a_registered_pair(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    mfl = tmp_path / "mfl.duckdb"
    out = tmp_path / "out.duckdb"
    index = tmp_path / "mfl_register_all_runs.json"
    _create_db(
        base,
        [
            ("smpl_mfl_2004_1", 2004, "old-mfl-1", 1),
            ("smpl_mfl_2004_2", 2004, "old-mfl-2", 2),
            ("yahoo_2004_1", 2004, "keep-yahoo", 3),
        ],
    )
    _create_mfl_chunk(
        mfl,
        [
            ("smpl_mfl_2004_1", 2004, "new-mfl-1", 11),
            ("smpl_mfl_2004_2", 2004, "new-mfl-2", 22),
        ],
    )
    con = duckdb.connect(str(mfl))
    con.execute("DELETE FROM league_settings WHERE db_name='smpl_mfl_2004_2'")
    con.close()
    index.write_text(
        json.dumps(
            {
                "leagues": [
                    {"season": 2004, "league_id": "1", "db_name": "smpl_mfl_2004_1"},
                    {"season": 2004, "league_id": "2", "db_name": "smpl_mfl_2004_2"},
                ],
                "accepted_by_year": {"2004": 2},
                "league_count": 2,
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).with_name("merge_research_with_mfl.py")
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--base",
            str(base),
            "--mfl",
            str(mfl),
            "--mfl-index",
            str(index),
            "--out",
            str(out),
            "--expected-ledger",
            json.dumps({"2004": 2}),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    con = duckdb.connect(str(out), read_only=True)
    assert con.execute(
        "SELECT db_name, marker, value FROM public.league_settings ORDER BY db_name"
    ).fetchall() == [
        ("smpl_mfl_2004_1", "new-mfl-1", 11),
        ("smpl_mfl_2004_2", "old-mfl-2", 2),
        ("yahoo_2004_1", "keep-yahoo", 3),
    ]
    con.close()


def test_merge_fails_when_protected_mfl_ledger_is_incomplete(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    mfl = tmp_path / "mfl.duckdb"
    index = tmp_path / "mfl_register_all_runs.json"
    _create_db(base, [("yahoo_2004_1", 2004, "keep", 1)])
    _create_mfl_chunk(mfl, [("smpl_mfl_2004_1", 2004, "new", 9)])
    index.write_text(
        json.dumps(
            {
                "leagues": [{"season": 2004, "league_id": "1", "db_name": "smpl_mfl_2004_1"}],
                "accepted_by_year": {"2004": 1},
                "league_count": 1,
            }
        ),
        encoding="utf-8",
    )

    script = Path(__file__).with_name("merge_research_with_mfl.py")
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--base",
            str(base),
            "--mfl",
            str(mfl),
            "--mfl-index",
            str(index),
            "--out",
            str(tmp_path / "out.duckdb"),
            "--expected-ledger",
            json.dumps({"2004": 2}),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "protected MFL league-year ledger mismatch" in (result.stdout + result.stderr)


def test_protected_mfl_merge_workflow_cannot_mutate_any_source_cache_or_ops_seed() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_merge_protected_mfl.yml"
    ).read_text(encoding="utf-8")

    assert "mfl-registered-2004-2018-v2-chunk-recover-mass-remaining-v31-r9-20260816-1" in workflow
    assert "mfl-registered-2004-2018-v2-index-recover-mass-remaining-v31-r9-20260816-1" in workflow
    assert "mv chunk_state recovery_chunk_state" in workflow
    assert "mv index_state recovery_index_state" in workflow
    assert '"2004":4780' in workflow
    assert '"2016":9114' in workflow
    assert "39368" in workflow
    assert "mfl_seed_before.sha256" in workflow
    assert "cmp mfl_seed_before.sha256 mfl_seed_after.sha256" in workflow
    assert "ops_seed_before.sha256" in workflow
    assert "cmp ops_seed_before.sha256 ops_seed_after.sha256" in workflow
    assert "--without-auxiliary-sidecars" in workflow
    assert "actions/cache/save@" not in workflow
    assert "gh cache delete" not in workflow
    assert "gh api --method DELETE" not in workflow
    assert "flyctl" not in workflow


def test_protected_mfl_merge_workflow_restores_all_population_chunks_in_15_lanes() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_merge_protected_mfl.yml"
    ).read_text(encoding="utf-8")

    assert "discover-population-cache-matrix" in workflow
    assert "mfl-population-batch-v1-chunk-0-1-allrun-" in workflow
    assert "MFL_POPULATION_EXPECTED_TOTAL: '30156'" in workflow
    assert "max-parallel: 15" in workflow
    assert "assemble_protected_mfl_sources.py" in workflow
    assert r'r"^mfl-population-batch-v1-chunk-0-1-allrun-(\d+)$"' in workflow
    assert "path: chunk_state" in workflow
    assert "mv chunk_state source_cache/${{ matrix.id }}/chunk_state" in workflow


def test_full_mfl_assembler_logs_each_source_and_ledger_boundary() -> None:
    script = Path(__file__).with_name("assemble_protected_mfl_sources.py").read_text(encoding="utf-8")

    assert "[mfl-assemble] phase=source_begin" in script
    assert "[mfl-assemble] phase=source_inventory" in script
    assert "[mfl-assemble] phase=table_append" in script
    assert "[mfl-assemble] phase=ledger_check" in script
