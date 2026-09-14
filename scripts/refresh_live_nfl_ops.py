#!/usr/bin/env python3
"""Build and optionally promote the live NFL SuperTable refresh artifact.

This is Track 1 only.  It reads public NFLverse releases, modifies a local
``___ops_nfl`` candidate, then (only with ``--apply``) atomically replaces the
standalone Fly ops file and refreshes its explicit-column views.  It never
opens, reads, or writes ``___leagues``.

Examples:
  python scripts/refresh_live_nfl_ops.py --base ops_nfl_2025.duckdb \
      --output ops_nfl_2026_w01.duckdb --year 2026 --week 1
  python scripts/refresh_live_nfl_ops.py --base ops_nfl_2025.duckdb \
      --output ops_nfl_2026_w01.duckdb --year 2026 --week 1 --apply
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(DATA_SCRIPTS))

from multi_league.data_fetchers.live_nfl_ops_refresh import (  # noqa: E402
    RefreshGateError,
    assert_complete_ops_artifact,
    assert_ops_schema_compatible,
    finalized_game_scope,
    prepare_weekly_facts,
    refresh_local_ops_artifact,
    write_refresh_manifest,
)

SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.parquet"
ROSTER_URL_TEMPLATE = "https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{year}.csv"


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"'))


def fetch_live_refresh_inputs(
    *,
    year: int,
    week: int,
    season_type: str = "REG",
    game_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read a finalized NFLverse scope and return canonical-ready fact inputs."""
    schedule = pd.read_parquet(SCHEDULE_URL)
    normalized_season_type = str(season_type).strip().upper()
    games = finalized_game_scope(
        schedule,
        year=year,
        week=week,
        season_types=(normalized_season_type,),
        game_date=game_date,
    )
    if games.empty:
        suffix = f", game_date={game_date}" if game_date is not None else ""
        raise RefreshGateError(
            f"no scored {normalized_season_type} games are final for year={year}, week={week}{suffix}"
        )

    # This existing fetch plane combines official player and team/DST releases,
    # PBP scoring atoms, scoring variants, and week ranks.  It makes no Fly
    # write; the legacy synthesis branch is disabled by the runner environment.
    from multi_league.data_fetchers.update_nfl_super_table import fetch_and_combine_nfl_data

    fetched = fetch_and_combine_nfl_data(year, week, synthesize_missing=False)
    facts = prepare_weekly_facts(
        fetched,
        games,
        year=year,
        week=week,
        season_type=normalized_season_type,
    )
    roster = pd.read_csv(ROSTER_URL_TEMPLATE.format(year=int(year)))
    return facts, games, roster


def promote_complete_artifact(
    artifact: Path,
    baseline: Path,
    expected_counts: dict[str, int],
    expected_schema_columns: dict[str, int],
) -> None:
    """Perform the explicit, atomic promotion after the local package passes QA."""
    actual_counts = assert_complete_ops_artifact(artifact)
    if actual_counts != expected_counts:
        raise RefreshGateError("artifact changed after the local verification receipt")
    actual_schema_columns = assert_ops_schema_compatible(baseline, artifact)
    if actual_schema_columns != expected_schema_columns:
        raise RefreshGateError("artifact schema changed after the local verification receipt")

    from multi_league.core.targets.fly_target import FlyTarget

    FlyTarget().replace_database("___ops_nfl", artifact)
    cutover = [
        sys.executable,
        str(ROOT / "scripts" / "cutover_ops_views.py"),
        "--apply",
        "--i-understand-this-writes-prod",
    ]
    subprocess.run(cutover, cwd=ROOT, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True, help="complete pre-refresh ___ops_nfl DuckDB artifact")
    parser.add_argument("--output", type=Path, required=True, help="candidate full ___ops_nfl DuckDB artifact")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    parser.add_argument("--season-type", choices=("REG", "POST"), default="REG")
    parser.add_argument("--game-date", help="constrain replacement to one YYYY-MM-DD finalized game date")
    parser.add_argument("--apply", action="store_true", help="atomically promote the locally verified artifact to Fly")
    args = parser.parse_args(argv)

    _load_env()
    # Explicitly protect this worker from any legacy importer path.  The two
    # tables it may contact are the public NFLverse sources and ___ops_nfl.
    os.environ["DATABASE_BACKEND"] = "fly"
    os.environ["LIVE_NFL_OPS_REFRESH"] = "1"
    os.environ["NFL_SUPER_TABLE_SYNTHESIS"] = "off"

    facts, games, players = fetch_live_refresh_inputs(
        year=args.year,
        week=args.week,
        season_type=args.season_type,
        game_date=args.game_date,
    )
    result = refresh_local_ops_artifact(
        base_artifact=args.base,
        output_artifact=args.output,
        facts=facts,
        games=games,
        players=players,
        year=args.year,
        week=args.week,
    )
    manifest = args.output.with_suffix(".manifest.json")
    write_refresh_manifest(result, manifest)
    print(f"[live-nfl-ops] local artifact verified: {args.output}")
    print(f"[live-nfl-ops] manifest: {manifest}")
    print(f"[live-nfl-ops] weekly rows replaced: {result['weekly_rows_replaced']}")
    if args.apply:
        promote_complete_artifact(
            args.output,
            args.base,
            result["table_counts"],
            result["schema_columns"],
        )
        print("[live-nfl-ops] atomic ___ops_nfl promotion and view refresh complete")
    else:
        print("[live-nfl-ops] no Fly write requested; pass --apply only after reviewing this receipt")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
