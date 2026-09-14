from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_single_platform_full_workers_have_a_no_publish_verification_mode() -> None:
    for name in (
        "yahoo_full_import_worker.yml",
        "espn_full_import_worker.yml",
        "sleeper_full_import_worker.yml",
    ):
        source = _workflow(name)
        assert "verify_only:" in source
        assert "VERIFY_ONLY:" in source
        assert "env.VERIFY_ONLY != 'true'" in source

    yahoo = _workflow("yahoo_full_import_worker.yml")
    assert "QUICK_ARGS" in yahoo
    assert "--skip-track-2-upload" in yahoo


def test_multiplatform_verify_mode_runs_real_pipeline_without_publication() -> None:
    workflow = _workflow("multi_platform_full_import_worker.yml")
    runner = (ROOT / ".github" / "scripts" / "run_multi_platform_import.py").read_text(
        encoding="utf-8"
    )

    assert "verify_only:" in workflow
    assert "--verify-only" in workflow
    assert 'parser.add_argument("--verify-only"' in runner
    assert "publish=not args.verify_only" in runner
    assert "persist_credentials=not args.verify_only" in runner
