"""Run a Yahoo import from encrypted browser credentials.

Cookie authentication changes Yahoo's transport only.  The worker builds a
normal Yahoo context and launches the same importer used by OAuth, whose
fetchers obtain the encrypted cookie session from Fly through that context.
Cookie values never enter the context, command line, or worker output.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "fantasy_football_data_scripts"
SHARED_IMPORTER = PACKAGE_ROOT / "initial_import_v3.py"
PYTHON = sys.executable
sys.path.insert(0, str(PACKAGE_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))


def _write_shared_import_context(args: argparse.Namespace) -> Path:
    """Write the non-secret context consumed by the shared Yahoo importer."""
    from yahoo_cookie_runner import _load_context_overrides, _load_league_keys
    from multi_league.core.league_context import LeagueContext

    import_mode = str(getattr(args, "import_mode", "full")).strip().lower()
    if import_mode not in {"quick", "full"}:
        raise ValueError("--import-mode must be quick or full")
    if not args.league_keys_json:
        raise ValueError(
            "--league-keys-json is required for cookie imports; refusing to fall back "
            "to the KMFFL league-key map"
        )
    league_keys = _load_league_keys(args.league_keys_json)
    start_year = int(args.start_year)
    end_year = int(args.end_year)
    if start_year > end_year:
        raise ValueError("--start-year cannot be after --end-year")
    if end_year not in league_keys:
        raise ValueError(f"--league-keys-json does not contain the end year {end_year}")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = _load_context_overrides(getattr(args, "context_json", None))
    payload.update(
        {
            "league_id": league_keys[end_year],
            "league_name": args.league_name,
            "start_year": start_year,
            "end_year": end_year,
            "num_teams": int(args.team_count),
            "data_directory": output_dir,
            "league_ids": {
                str(year): league_key
                for year, league_key in league_keys.items()
                if start_year <= year <= end_year
            },
            "database_name": args.db_name,
            "import_mode": import_mode,
            "require_oauth": False,
            "yahoo_auth_mode": "cookie",
            # The session is fetched and decrypted in memory only when a
            # shared fetcher calls ctx.get_oauth_session().
            "cookie_jar_path": f"fly://___ops.main.yahoo_web_credentials/{args.db_name}",
        }
    )
    context = LeagueContext(**payload)
    context_path = output_dir / f"league_context_cookie_{import_mode}.json"
    context.save(context_path)
    return context_path


def run_shared_import(args: argparse.Namespace) -> int:
    """Launch the normal Yahoo pipeline with a cookie-backed context."""
    context_path = _write_shared_import_context(args)
    command = [PYTHON, str(SHARED_IMPORTER), "--context", str(context_path)]
    if str(getattr(args, "import_mode", "full")).lower() == "quick":
        command.append("--quick")
    if getattr(args, "skip_track_1", False):
        command.append("--skip-track-1")
    if getattr(args, "skip_upload", False):
        command.append("--skip-track-2-upload")
    completed = subprocess.run(command, cwd=PROJECT_ROOT, env=os.environ.copy(), check=False)
    return int(completed.returncode)


def run_cached_page_replay(args: argparse.Namespace) -> int:
    """Materialize a local model from retained Yahoo HTML without any network access."""
    from yahoo_cookie_runner import run as run_cookie_runner

    placeholder_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            prefix="yahoo_cookie_cache_only_",
            delete=False,
        ) as handle:
            json.dump(
                [{"name": "T", "value": "cache-only", "domain": ".yahoo.com"}],
                handle,
                separators=(",", ":"),
            )
            placeholder_path = Path(handle.name)

        replay_args = argparse.Namespace(
            cookie_jar=str(placeholder_path),
            ops_cache=args.ops_cache,
            draft_global_source=args.draft_global_source,
            output_dir=args.output_dir,
            db_name=args.db_name,
            league_name=args.league_name,
            context_json=getattr(args, "context_json", None),
            league_keys_json=args.league_keys_json,
            start_year=args.start_year,
            end_year=args.end_year,
            team_count=args.team_count,
            roster_weeks=getattr(args, "roster_weeks", None),
            transaction_pages=getattr(args, "transaction_pages", 200),
            request_delay=getattr(args, "request_delay", 0.5),
            throttle_retries=getattr(args, "throttle_retries", 1),
            year_throttle_cooldown=getattr(args, "year_throttle_cooldown", 0.0),
            throttle_recovery_retries=getattr(args, "throttle_recovery_retries", 1),
            throttle_recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
            n_sims=getattr(args, "n_sims", 10000),
            import_mode=getattr(args, "import_mode", "full"),
            # The workflow runs the canonical shared calculation/upload stages
            # after this local reconstruction, so do not calculate them twice.
            skip_playoff=True,
            skip_aggregations=True,
        )
        return int(run_cookie_runner(replay_args))
    finally:
        if placeholder_path is not None:
            placeholder_path.unlink(missing_ok=True)


def run(args: argparse.Namespace) -> int:
    if os.environ.get("COOKIE_BACKUP_CACHE_ONLY") == "1":
        return run_cached_page_replay(args)
    return run_shared_import(args)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a Yahoo import from encrypted Fly cookie credentials")
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--ops-cache", required=True)
    parser.add_argument("--draft-global-source", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--league-name", default="Yahoo League")
    parser.add_argument("--context-json")
    parser.add_argument("--league-keys-json")
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--team-count", type=int, default=10)
    parser.add_argument("--roster-weeks", type=int)
    parser.add_argument("--transaction-pages", type=int, default=200)
    parser.add_argument("--request-delay", type=float, default=0.5)
    parser.add_argument("--throttle-retries", type=int, default=1)
    parser.add_argument("--year-throttle-cooldown", type=float, default=0.0)
    parser.add_argument("--throttle-recovery-retries", type=int, default=1)
    parser.add_argument("--throttle-recovery-cooldown", type=float, default=300.0)
    parser.add_argument("--n-sims", type=int, default=10000)
    parser.add_argument("--import-mode", choices=["quick", "full"], default="full")
    parser.add_argument("--skip-playoff", action="store_true")
    parser.add_argument("--skip-aggregations", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
