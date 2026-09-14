"""Full local Yahoo import runner using an authenticated web-cookie source.

This runner intentionally mirrors the Yahoo import boundary only:

    Yahoo cookie capture -> canonical Yahoo source tables -> shared pipeline
    -> playoff odds/simulations -> local aggregations -> validation

OAuth is never called and Fly is never contacted.  The downstream stages are
the same local stages used by the normal importer and playoff worker.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
import re
from dataclasses import fields
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
PYTHON = sys.executable
CANONICAL_TABLES = ("league_settings", "matchup", "player_fantasy", "draft", "transactions", "schedule")
AGGREGATION_REFRESH_SCRIPT = SCRIPTS_DIR / "refresh_aggregates.py"
POST_IMPORT_TABLES = (
    "standings_by_year",
    "player_fantasy_season",
    "matchup_season",
    "draft_manager_season",
    "transaction_manager_season",
    "homepage_league_summary",
)


def _run_command(label: str, args: list[str], *, cwd: Path) -> None:
    print(f"\n[{label}] {' '.join(args)}")
    completed = subprocess.run(args, cwd=cwd, env=os.environ.copy(), check=False)
    if completed.returncode:
        raise RuntimeError(f"{label} failed with exit code {completed.returncode}")


def _validate_ops_cache(ops_cache: Path) -> None:
    """Require the same super-table surface used by the shared pipeline."""
    import duckdb

    ops_cache = ops_cache.expanduser().resolve()
    if not ops_cache.is_file():
        raise FileNotFoundError(f"OPS cache does not exist: {ops_cache}")
    conn = duckdb.connect(str(ops_cache), read_only=True)
    try:
        objects = {(row[1], row[2]) for row in conn.execute("SHOW ALL TABLES").fetchall()}
    finally:
        conn.close()
    required = {
        ("nfl_historical", "player_bio"): ("public", "player_bio"),
        ("nfl_historical", "nfl_player_stats_all"): ("public", "nfl_player_stats_all"),
    }
    missing = [
        f"{schema}.{table}"
        for (schema, table), fallback in required.items()
        if (schema, table) not in objects and fallback not in objects
    ]
    if missing:
        raise RuntimeError(
            "OPS cache is incomplete for the shared local pipeline; missing "
            + ", ".join(missing)
            + ". Supply the full local ___ops cache containing the NFL super-table."
        )


def _validate_draft_global_source(source: Path) -> None:
    """Require the offline baseline used by the shared draft-grade pass."""
    if not source.is_file():
        raise FileNotFoundError(
            "The local draft baseline is required for offline draft grades: "
            f"{source}. Supply --draft-global-source."
        )


def _load_league_keys(raw: str | None) -> dict[int, str]:
    """Load an explicit Yahoo renewal-chain mapping."""
    if not raw:
        raise ValueError("cookie imports require explicit Yahoo league keys")
    text = raw
    # Workflow dispatch turns JSON object input into the project's historical
    # unquoted {year:key} spelling.  It is data, never a filesystem path; a
    # path probe can itself fail once a full renewal chain exceeds NAME_MAX.
    if not raw.lstrip().startswith("{"):
        candidate = Path(raw).expanduser()
        try:
            if candidate.is_file():
                text = candidate.read_text(encoding="utf-8")
        except OSError:
            # Treat an invalid/overlong candidate as inline input so the
            # normal parser can return the useful validation error below.
            text = raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        # GitHub workflow inputs and older local invocations may preserve the
        # project's historical {2015: league_key} spelling.  Accept only a
        # literal mapping; never evaluate arbitrary input.
        try:
            payload = ast.literal_eval(text)
        except (SyntaxError, ValueError) as exc:
            if not (text.strip().startswith("{") and text.strip().endswith("}")):
                raise ValueError("league-keys-json must be valid JSON or a simple year:key mapping") from exc
            payload = {}
            for item in text.strip()[1:-1].split(","):
                if not item.strip() or ":" not in item:
                    raise ValueError("league-keys-json contains an invalid year:key mapping") from exc
                year, key = item.split(":", 1)
                payload[int(year.strip().strip("'\""))] = key.strip().strip("'\"")
    if isinstance(payload, dict) and "league_ids" in payload:
        payload = payload["league_ids"]
    if not isinstance(payload, dict):
        raise ValueError("league-keys-json must contain an object mapping years to Yahoo league keys")
    result = {int(year): str(key) for year, key in payload.items()}
    if not result:
        raise ValueError("league-keys-json did not contain any year mappings")
    return result


def _source_prefix(league_name: str) -> str:
    """Create a filesystem-safe prefix for the raw cookie backup."""
    return re.sub(r"[^a-z0-9_-]+", "_", league_name.lower()).strip("_-_") or "yahoo"


def _discard_prior_local_model(output_dir: Path, db_name: str) -> None:
    """Remove a quick-stage model before the full stage rebuilds the same DB.

    Raw cached Yahoo pages and cookie source databases have distinct filenames
    and are intentionally retained. The target is resolved and bounded to
    the worker output directory before removal.
    """
    root = output_dir.expanduser().resolve()
    target = (root / f"{db_name}.duckdb").resolve()
    try:
        target.relative_to(root)
    except ValueError as error:
        raise ValueError("local model target must remain inside the cookie worker output directory") from error
    target.unlink(missing_ok=True)


def _load_context_overrides(raw: str | None) -> dict[str, Any]:
    """Load OAuth-style league context options without accepting its auth fields."""
    if not raw:
        return {}
    candidate = Path(raw).expanduser()
    payload = json.loads(candidate.read_text(encoding="utf-8") if candidate.is_file() else raw)
    if not isinstance(payload, dict):
        raise ValueError("context-json must contain a JSON object")
    from multi_league.core.league_context import LeagueContext

    allowed = {field.name for field in fields(LeagueContext)}
    blocked = {
        "league_id", "league_name", "oauth_file_path", "oauth_credentials",
        "data_directory", "database_name", "require_oauth", "created_at", "updated_at",
    }
    return {key: value for key, value in payload.items() if key in allowed and key not in blocked}


def _write_local_context(
    output_dir: Path,
    db_name: str,
    start_year: int,
    end_year: int,
    league_keys: dict[int, str],
    league_name: str,
    *,
    import_mode: str = "full",
    team_count: int = 10,
    context_overrides: dict[str, Any] | None = None,
    auth_mode: str = "cookie",
    filename: str = "league_context.json",
) -> Path:
    """Create the read-only context consumed by shared local transforms."""
    from multi_league.core.league_context import LeagueContext

    context_payload: dict[str, Any] = dict(context_overrides or {})
    # These values are controlled by the active runner invocation, never by a
    # stale context file. All other OAuth context options are preserved.
    context_payload.update(
        {
            "league_id": league_keys[end_year],
            "league_name": league_name,
            "start_year": start_year,
            "end_year": end_year,
            "num_teams": team_count,
            "data_directory": output_dir,
            "league_ids": {str(year): key for year, key in league_keys.items() if start_year <= year <= end_year},
            "database_name": db_name,
            "import_mode": import_mode,
            "require_oauth": False,
            "yahoo_auth_mode": auth_mode,
            # The decrypted jar is deliberately ephemeral. This is a
            # non-secret credential reference for context consumers, not a
            # filesystem path containing cookie values.
            "cookie_jar_path": f"fly://___ops.main.yahoo_web_credentials/{db_name}"
            if auth_mode == "cookie"
            else None,
        }
    )
    context = LeagueContext(
        **context_payload,
    )
    path = output_dir / filename
    context.save(path)
    return path


def _validate_local_db(output_dir: Path, db_name: str) -> dict[str, Any]:
    import duckdb

    db_path = output_dir / f"{db_name}.duckdb"
    if not db_path.exists():
        raise RuntimeError(f"Local DuckDB was not created: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES FROM public").fetchall()}
        missing = [table for table in CANONICAL_TABLES if table not in tables]
        if missing:
            raise RuntimeError(f"Missing canonical tables: {', '.join(missing)}")
        counts = {table: int(conn.execute(f'SELECT COUNT(*) FROM public."{table}"').fetchone()[0]) for table in tables}
        empty = [table for table in CANONICAL_TABLES if counts[table] == 0]
        if empty:
            raise RuntimeError(f"Canonical tables are empty: {', '.join(empty)}")
        scoped_counts = {
            table: int(
                conn.execute(
                    f'SELECT COUNT(*) FROM public."{table}" WHERE db_name = ?',
                    [db_name],
                ).fetchone()[0]
            )
            for table in CANONICAL_TABLES
        }
        unscoped = [table for table, count in scoped_counts.items() if count == 0]
        if unscoped:
            raise RuntimeError(
                f"Canonical tables contain no rows for db_name={db_name}: "
                + ", ".join(unscoped)
            )
        from multi_league.validation.checks.pipeline_checks import (
            check_pre_upload_sanity,
            check_transform_output_non_empty,
        )

        errors = check_transform_output_non_empty(conn, list(CANONICAL_TABLES))
        errors.extend(check_pre_upload_sanity(conn))
        if errors:
            raise RuntimeError("Local pipeline validation failed: " + "; ".join(errors[:20]))
        return {
            "db_path": str(db_path),
            "tables": sorted(tables),
            "row_counts": counts,
            "scoped_row_counts": scoped_counts,
            "errors": [],
        }
    finally:
        conn.close()


def _local_table_count(output_dir: Path, db_name: str, table: str) -> int:
    """Return a scoped local row count without opening a production catalog."""
    import duckdb

    db_path = output_dir / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return int(
            conn.execute(
                f'SELECT COUNT(*) FROM public."{table}" WHERE db_name = ?',
                [db_name],
            ).fetchone()[0]
        )
    finally:
        conn.close()


def _validate_post_import_outputs(
    output_dir: Path,
    db_name: str,
    *,
    require_simulations: bool,
    require_aggregations: bool,
) -> dict[str, Any]:
    """Check that successful subprocesses actually produced scoped outputs."""
    import duckdb

    db_path = output_dir / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES FROM public").fetchall()}
        result: dict[str, Any] = {"tables": sorted(tables)}
        if require_simulations:
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info('public.matchup')").fetchall()
            }
            required_columns = {"shuffle_avg_wins", "shuffle_avg_playoffs", "p_playoffs", "p_champ"}
            missing_columns = sorted(required_columns - columns)
            if missing_columns:
                raise RuntimeError(
                    "Simulation stages completed without required matchup columns: "
                    + ", ".join(missing_columns)
                )
            result["simulation_rows"] = {
                column: int(
                    conn.execute(
                        f'SELECT COUNT(*) FROM public.matchup WHERE db_name = ? AND "{column}" IS NOT NULL',
                        [db_name],
                    ).fetchone()[0]
                )
                for column in sorted(required_columns)
            }
        if require_aggregations:
            missing_tables = sorted(set(POST_IMPORT_TABLES) - tables)
            if missing_tables:
                raise RuntimeError(
                    "Aggregation stages completed without required tables: "
                    + ", ".join(missing_tables)
                )
            aggregate_counts = {
                table: int(
                    conn.execute(
                        f'SELECT COUNT(*) FROM public."{table}" WHERE db_name = ?',
                        [db_name],
                    ).fetchone()[0]
                )
                for table in POST_IMPORT_TABLES
            }
            empty_tables = [table for table, count in aggregate_counts.items() if count == 0]
            if empty_tables:
                raise RuntimeError(
                    "Aggregation tables contain no rows for the active database: "
                    + ", ".join(empty_tables)
                )
            result["aggregate_counts"] = aggregate_counts
        return result
    finally:
        conn.close()


def run(args: argparse.Namespace) -> int:
    import_mode = str(getattr(args, "import_mode", "full")).strip().lower()
    if import_mode not in {"quick", "full"}:
        raise ValueError(f"Unsupported Yahoo cookie import mode: {import_mode}")
    # Match the OAuth quick contract: a paid/full workflow may know the
    # whole renewal chain, but its first publish must fetch only the live
    # season.  History is acquired by the subsequent full stage.
    capture_start_year = int(args.end_year) if import_mode == "quick" else int(args.start_year)
    capture_end_year = int(args.end_year)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("CORPUS_MODE", "1")
    # This runner is intentionally local-only.  Do not inherit a caller's
    # production backend setting and accidentally route a stage to Fly.
    os.environ["DATABASE_BACKEND"] = "local"
    # A cookie run is local until the worker's explicit scoped Fly upload.
    # Never inherit a legacy MotherDuck token into this stage.
    os.environ.pop("MOTHERDUCK_TOKEN", None)
    ops_cache = Path(args.ops_cache).expanduser().resolve()
    _validate_ops_cache(ops_cache)
    draft_global_source = Path(args.draft_global_source).expanduser().resolve()
    _validate_draft_global_source(draft_global_source)
    league_keys = _load_league_keys(args.league_keys_json)
    missing_keys = [year for year in range(capture_start_year, capture_end_year + 1) if year not in league_keys]
    if missing_keys:
        raise ValueError(f"No Yahoo league key supplied for requested year(s): {missing_keys}")
    os.environ["OPS_CACHE_PATH"] = str(ops_cache)
    os.environ["DRAFT_GLOBAL_SOURCE_PATH"] = str(draft_global_source)
    os.environ["LOCAL_PIPELINE_ATTACH_OPS"] = "1"
    package_root = PROJECT_ROOT / "fantasy_football_data_scripts"
    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath = [str(package_root), str(PROJECT_ROOT)]
    if existing_pythonpath:
        pythonpath.append(existing_pythonpath)
    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath)

    # Import these modules only after the repository root is established.
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.path.insert(0, str(SCRIPTS_DIR))
    from quick_import_kmffl_2025_web import run_all_years
    from build_kmffl_cookie_model import build_frames, write_model, run_local_enrichments
    from multi_league.core.import_pipeline import run_transformation_pipeline
    from multi_league.core.league_context import LeagueContext

    context_overrides = _load_context_overrides(args.context_json)
    effective_team_count = int(context_overrides.get("num_teams", args.team_count))

    capture_args = argparse.Namespace(
        cookie_jar=str(Path(args.cookie_jar).expanduser().resolve()),
        output_dir=str(output_dir),
        start_year=capture_start_year,
        end_year=capture_end_year,
        request_delay=args.request_delay,
        throttle_retries=args.throttle_retries,
        year_throttle_cooldown=args.year_throttle_cooldown,
        throttle_recovery_retries=args.throttle_recovery_retries,
        throttle_recovery_cooldown=args.throttle_recovery_cooldown,
        roster_weeks=args.roster_weeks,
        team_count=effective_team_count,
        transaction_pages=args.transaction_pages,
        skip_history=False,
        no_fly_compare=True,
        db_name=args.db_name,
        league_keys={
            year: league_keys[year]
            for year in range(capture_start_year, capture_end_year + 1)
        },
        league_name=args.league_name,
        source_prefix=_source_prefix(args.league_name),
    )
    if run_all_years(capture_args) != 0:
        raise RuntimeError("Yahoo cookie capture failed")

    source_db = output_dir / (
        f"{_source_prefix(args.league_name)}_{capture_start_year}_{capture_end_year}_cookie_backup.duckdb"
    )
    if not source_db.exists():
        raise RuntimeError(f"Cookie capture did not produce source database: {source_db}")

    # OAuth full imports make a current-season quick artifact before the
    # historical run.  The cookie track has already captured the source once,
    # so reproduce that stage locally from the captured source rather than
    # issuing a second Yahoo request (which would only increase throttling).
    quick_start_year = capture_start_year if import_mode == "quick" else (
        args.end_year if args.start_year >= args.end_year else max(args.start_year, args.end_year - 1)
    )
    quick_years = list(range(quick_start_year, args.end_year + 1))
    quick_year_keys = {year: league_keys[year] for year in quick_years if year in league_keys}
    quick_db_name = args.db_name if import_mode == "quick" else f"{args.db_name}_quick"
    quick_frames = build_frames(
        source_db,
        output_dir,
        quick_start_year,
        args.end_year,
        league_keys=quick_year_keys,
        team_count=effective_team_count,
    )
    quick_model_path = write_model(
        quick_frames,
        output_dir,
        db_name=quick_db_name,
        source_league_id=league_keys[args.end_year],
    )
    quick_context_path = _write_local_context(
        output_dir,
        quick_db_name,
        quick_start_year,
        args.end_year,
        quick_year_keys,
        args.league_name,
        import_mode="quick",
        team_count=effective_team_count,
        context_overrides=context_overrides,
        filename="league_context.json" if import_mode == "quick" else "league_context_quick.json",
    )
    quick_context = LeagueContext.load_readonly(quick_context_path)
    quick_transform_results = run_transformation_pipeline(
        quick_context,
        dry_run=False,
        skip_track_2_upload=True,
        import_mode="quick",
        context_file_path=quick_context_path,
        platform="yahoo",
        db_name=quick_db_name,
        data_dir=str(output_dir),
        quick=True,
    )
    if any(not success for _, success in quick_transform_results):
        raise RuntimeError("Cookie quick-import transformation pipeline failed")
    quick_enrichment_results = run_local_enrichments(output_dir, db_name=quick_db_name, quick=True)
    quick_failures = [name for name, value in quick_enrichment_results.items() if isinstance(value, tuple)]
    if quick_failures:
        raise RuntimeError(f"Cookie quick-import SQL enrichments failed: {', '.join(quick_failures)}")
    quick_validation = _validate_local_db(output_dir, quick_db_name)

    if import_mode == "quick":
        manifest = {
            "runner": "yahoo_cookie_runner",
            "source": "yahoo_web_cookie",
            "league_name": args.league_name,
            "league_keys": {str(year): key for year, key in league_keys.items() if year == args.end_year},
            "oauth_used": False,
            "fly_upload": False,
            "import_mode": "quick",
            "years": [args.end_year],
            "db_name": args.db_name,
            "source_db": str(source_db),
            "model_db": str(quick_model_path),
            "context": str(quick_context_path),
            "transformations": [[name, bool(success)] for name, success in quick_transform_results],
            "enrichments": {
                name: list(value) if isinstance(value, tuple) else value
                for name, value in quick_enrichment_results.items()
            },
            "validation": quick_validation,
            "oauth_stage_contract": [
                {"stage": "oauth_authentication", "status": "replaced", "replacement": "authenticated Yahoo cookie jar"},
                {"stage": "quick_import", "status": "completed", "artifact": args.db_name},
                {"stage": "full_import", "status": "not_applicable_quick_mode"},
                {"stage": "workflow_summary", "status": "completed", "artifact": "cookie_runner_manifest.json"},
            ],
        }
        (output_dir / "cookie_runner_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(json.dumps(manifest, indent=2))
        return 0

    # The workflow has already uploaded the quick artifact under this same
    # database name. Rebuild the local full model cleanly while retaining the
    # captured Yahoo source pages for an idempotent history stage.
    _discard_prior_local_model(output_dir, args.db_name)

    frames = build_frames(
        source_db,
        output_dir,
        args.start_year,
        args.end_year,
        league_keys=league_keys,
        team_count=effective_team_count,
    )
    model_path = write_model(
        frames,
        output_dir,
        db_name=args.db_name,
        source_league_id=league_keys[args.end_year],
    )
    context_path = _write_local_context(
        output_dir,
        args.db_name,
        args.start_year,
        args.end_year,
        league_keys,
        args.league_name,
        import_mode="full",
        team_count=effective_team_count,
        context_overrides=context_overrides,
    )

    # The shared transformation entry point is deliberately invoked even
    # though current pass lists are SQL-backed/empty. This keeps the cookie
    # track aligned with future shared pass additions.
    context = LeagueContext.load_readonly(context_path)
    transform_results = run_transformation_pipeline(
        context,
        dry_run=False,
        skip_track_2_upload=True,
        import_mode="full",
        context_file_path=context_path,
        platform="yahoo",
        db_name=args.db_name,
        data_dir=str(output_dir),
        quick=False,
    )
    if any(not success for _, success in transform_results):
        raise RuntimeError("Shared transformation pipeline failed")

    # Current shared passes are SQL-backed, but this remains a distinct stage
    # so future pass additions retain the OAuth ordering.
    enrichment_results = run_local_enrichments(output_dir, db_name=args.db_name)
    failures = [name for name, value in enrichment_results.items() if isinstance(value, tuple)]
    if failures:
        raise RuntimeError(f"Shared SQL enrichments failed: {', '.join(failures)}")

    matchup_count = _local_table_count(output_dir, args.db_name, "matchup")
    expected_record_ran = False
    playoff_ran = False
    if not args.skip_playoff and matchup_count:
        _run_command(
            "Expected record simulations",
            [
                PYTHON,
                "-m",
                "multi_league.transformations.matchup.expected_record_v2",
                "--db",
                args.db_name,
                "--data-dir",
                str(output_dir),
                "--n-sims",
                str(args.n_sims),
            ],
            cwd=PROJECT_ROOT,
        )
        expected_record_ran = True
        _run_command(
            "Playoff odds and simulations",
            [
                PYTHON,
                "-m",
                "multi_league.transformations.matchup.playoff_odds_import",
                "--db",
                args.db_name,
                "--data-dir",
                str(output_dir),
                "--n-sims",
                str(args.n_sims),
            ],
            cwd=PROJECT_ROOT,
        )
        playoff_ran = True

    if not args.skip_aggregations:
        # Use the same aggregation orchestrator as the OAuth workflow.  The
        # script invokes the six shared modules in their canonical order.
        _run_command(
            "Refresh aggregates",
            [
                PYTHON,
                str(AGGREGATION_REFRESH_SCRIPT),
                "--db",
                args.db_name,
                "--data-dir",
                str(output_dir),
            ],
            cwd=PROJECT_ROOT,
        )

    post_import_validation = _validate_post_import_outputs(
        output_dir,
        args.db_name,
        require_simulations=not args.skip_playoff,
        require_aggregations=not args.skip_aggregations,
    )
    validation = _validate_local_db(output_dir, args.db_name)
    enrichment_manifest = {
        name: list(value) if isinstance(value, tuple) else value
        for name, value in enrichment_results.items()
    }
    manifest = {
        "runner": "yahoo_cookie_runner",
        "source": "yahoo_web_cookie",
        "league_name": args.league_name,
        "league_keys": {str(year): key for year, key in league_keys.items() if args.start_year <= year <= args.end_year},
        "oauth_used": False,
        "fly_upload": False,
        "years": [args.start_year, args.end_year],
        "db_name": args.db_name,
        "source_db": str(source_db),
        "quick_import": {
            "db_name": quick_db_name,
            "years": quick_years,
            "model_db": str(quick_model_path),
            "context": str(quick_context_path),
            "transformations": [[name, bool(success)] for name, success in quick_transform_results],
            "enrichments": {
                name: list(value) if isinstance(value, tuple) else value
                for name, value in quick_enrichment_results.items()
            },
            "validation": quick_validation,
        },
        "model_db": str(model_path),
        "context": str(context_path),
        "ops_cache": str(ops_cache),
        "draft_global_source": str(draft_global_source),
        "context_overrides": context_overrides,
        "transformations": [[name, bool(success)] for name, success in transform_results],
        "enrichments": enrichment_manifest,
        "expected_record_runner": expected_record_ran,
        "playoff_runner": playoff_ran,
        "matchup_rows": matchup_count,
        "import_mode": "full",
        "post_import_aggregations": not args.skip_aggregations,
        "validation": validation,
        "post_import_validation": post_import_validation,
        "oauth_stage_contract": [
            {"stage": "oauth_authentication", "status": "replaced", "replacement": "authenticated Yahoo cookie jar"},
            {"stage": "quick_import", "status": "completed", "artifact": quick_db_name},
            {"stage": "full_import", "status": "completed", "artifact": args.db_name},
            {"stage": "expected_record", "status": "completed" if expected_record_ran else "skipped_by_flag"},
            {"stage": "playoff_odds_and_clutch", "status": "completed" if playoff_ran else "skipped_by_flag"},
            {"stage": "aggregations", "status": "completed" if not args.skip_aggregations else "skipped_by_flag"},
            {"stage": "final_upload", "status": "not_applicable_local_mode", "replacement": "local DuckDB artifact"},
            {"stage": "vercel_cache_warm", "status": "not_applicable_local_mode", "replacement": "local validation"},
            {"stage": "oauth_credential_store", "status": "not_applicable_cookie_mode", "replacement": "external cookie jar supplied by operator"},
            {"stage": "workflow_summary", "status": "completed", "artifact": "cookie_runner_manifest.json"},
        ],
    }
    (output_dir / "cookie_runner_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a full local Yahoo import using cookies.")
    parser.add_argument("--cookie-jar", required=True)
    parser.add_argument("--ops-cache", required=True)
    parser.add_argument(
        "--draft-global-source",
        default=os.environ.get(
            "DRAFT_GLOBAL_SOURCE_PATH",
            str(
                PROJECT_ROOT.parent
                / "league-history-data"
                / "fantasy_leagues"
                / "sampling_corpus"
                / "github_dependencies_v1"
                / "draft_global_source.parquet"
            ),
        ),
        help="Offline global draft baseline used by the shared draft-grade enrichment.",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--db-name", default="kmffl_cookie")
    parser.add_argument("--league-name", default="KMFFL")
    parser.add_argument(
        "--context-json",
        help="Optional OAuth-style league context JSON; preserves aliases, franchise merges, keeper/rule options, and external maps.",
    )
    parser.add_argument(
        "--league-keys-json",
        required=True,
        help="JSON file or inline object mapping each season to its Yahoo league key (renewal chain).",
    )
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--team-count", type=int, default=10)
    parser.add_argument("--roster-weeks", type=int, default=None)
    parser.add_argument("--transaction-pages", type=int, default=200)
    parser.add_argument("--request-delay", type=float, default=0.25)
    parser.add_argument("--throttle-retries", type=int, default=5)
    parser.add_argument("--n-sims", type=int, default=10000)
    parser.add_argument("--import-mode", choices=["quick", "full"], default="full")
    parser.add_argument("--skip-playoff", action="store_true")
    parser.add_argument("--skip-aggregations", action="store_true")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
