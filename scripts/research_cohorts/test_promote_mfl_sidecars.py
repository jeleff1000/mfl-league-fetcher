from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def _write_base(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            matchup_id VARCHAR,
            win INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
    """)
    con.execute("INSERT INTO public.matchup VALUES ('smpl_mfl_1', 2024, 1, 'team-a', 'base-1', NULL, NULL, NULL)")
    con.execute("""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            NFL_player_id VARCHAR,
            win INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy
        VALUES ('smpl_mfl_1', 2024, 1, 'team-a', 'player-1', NULL, NULL, NULL)
    """)
    con.close()


def _write_conflicting_sidecar(path: Path) -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE source AS
        SELECT 'smpl_mfl_1'::VARCHAR AS db_name,
               2024::INTEGER AS year,
               1::INTEGER AS week,
               'team-a'::VARCHAR AS manager,
               'source-1'::VARCHAR AS matchup_id,
               1::INTEGER AS win,
               101.0::DOUBLE AS team_points,
               99.0::DOUBLE AS opponent_points
        UNION ALL
        SELECT 'smpl_mfl_1'::VARCHAR,
               2024::INTEGER,
               1::INTEGER,
               'team-a'::VARCHAR,
               'source-2'::VARCHAR,
               0::INTEGER,
               99.0::DOUBLE,
               101.0::DOUBLE
    """)
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def _write_authoritative_sidecar(path: Path) -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE source AS
        SELECT 'smpl_mfl_1'::VARCHAR AS db_name,
               2024::INTEGER AS year,
               1::INTEGER AS week,
               'team-a'::VARCHAR AS manager,
               'source-1'::VARCHAR AS matchup_id,
               1::INTEGER AS win,
               101.0::DOUBLE AS team_points,
               99.0::DOUBLE AS opponent_points
    """)
    con.execute("COPY source TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_rejects_conflicting_source_rows_with_the_same_canonical_matchup_key(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    sidecars = tmp_path / "sidecars"
    sidecars.mkdir()
    _write_base(base)
    _write_conflicting_sidecar(sidecars / "mfl_underpopulated_week_rescue_conflict.parquet")

    script = Path(__file__).with_name("promote_mfl_sidecars.py")
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--base", str(base),
            "--sidecars", str(sidecars),
            "--out", str(tmp_path / "out.duckdb"),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "conflicting duplicate source matchup key" in (result.stdout + result.stderr)


def test_authoritative_mfl_values_replace_stale_values_on_matching_keys(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    sidecars = tmp_path / "sidecars"
    sidecars.mkdir()
    _write_base(base)
    con = duckdb.connect(str(base))
    con.execute("UPDATE public.matchup SET win=0, team_points=99.0, opponent_points=101.0")
    con.execute("UPDATE public.player_fantasy SET win=0, team_points=99.0, opponent_points=101.0")
    con.close()
    _write_authoritative_sidecar(sidecars / "mfl_underpopulated_week_rescue_authoritative.parquet")

    out = tmp_path / "out.duckdb"
    script = Path(__file__).with_name("promote_mfl_sidecars.py")
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--base", str(base),
            "--sidecars", str(sidecars),
            "--out", str(out),
        ],
        check=True,
    )

    check = duckdb.connect(str(out), read_only=True)
    assert check.execute(
        "SELECT win, team_points, opponent_points FROM public.matchup"
    ).fetchone() == (1, 101.0, 99.0)
    assert check.execute(
        "SELECT win, team_points, opponent_points FROM public.player_fantasy"
    ).fetchone() == (1, 101.0, 99.0)
    check.close()


def test_promotion_workflow_never_replaces_the_cache_containing_ops() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_promote_mfl_sidecars.yml"
    ).read_text(encoding="utf-8")

    assert "gh api --method DELETE" not in workflow
    assert "actions/cache/save@" not in workflow
    assert "ops_seed_before.sha256" in workflow
    assert "cmp ops_seed_before.sha256 ops_seed_after.sha256" in workflow


def test_promotion_workflow_prefers_a_corpus_only_release_asset() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github"
        / "workflows"
        / "research_promote_mfl_sidecars.yml"
    ).read_text(encoding="utf-8")

    assert "corpus_snapshot.duckdb.part-*" in workflow
    assert "corpus_snapshot.duckdb.sha256" in workflow
    assert "cat corpus_parts/corpus_snapshot.duckdb.part-* > research_public_lake/corpus_snapshot.duckdb" in workflow
