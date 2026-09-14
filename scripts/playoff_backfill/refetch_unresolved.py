"""Refetch only unresolved playoff evidence for old MFL/Fleaflicker seasons."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.fleaflicker.fleaflicker_api_client import FleaflickerAPIClient
from multi_league.data_fetchers.fleaflicker.fleaflicker_context import FleaflickerContext
from multi_league.data_fetchers.fleaflicker.fleaflicker_rosters import fetch_fleaflicker_rosters
from multi_league.data_fetchers.fleaflicker.fleaflicker_schedules import fetch_fleaflicker_schedule
from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIClient
from multi_league.data_fetchers.mfl.mfl_context import MFLContext
from multi_league.data_fetchers.mfl.mfl_matchups import championship_bracket_pairs
from multi_league.data_fetchers.mfl.mfl_rosters import fetch_mfl_rosters

from scripts.playoff_backfill.apply_evidence_overlay import apply_overlay
from scripts.playoff_backfill.fleaflicker_adapter import build_fleaflicker_evidence
from scripts.playoff_backfill.mfl_adapter import build_mfl_evidence


def unresolved_league_years(con: duckdb.DuckDBPyConnection, platforms: list[str], start: int, end: int) -> list[tuple]:
    placeholders = ",".join("?" for _ in platforms)
    return con.execute(
        f"""
        SELECT s.platform, s.db_name, s.year, s.league_key
        FROM public.league_settings s
        JOIN public.player_fantasy p USING (db_name, year)
        WHERE s.platform IN ({placeholders}) AND s.year BETWEEN ? AND ?
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) FILTER (WHERE p.made_po_bf IS NULL) > 0
        ORDER BY s.platform, s.year, s.db_name
        """,
        [*platforms, start, end],
    ).fetchall()


def _context(cls, db_name: str, key: str, year: int, root: Path):
    return cls(
        league_id=str(key), league_name=db_name, start_year=year, end_year=year,
        league_ids={str(year): str(key)}, data_directory=root / db_name / str(year),
        database_name=db_name,
    )


def fetch_one(platform: str, db_name: str, year: int, league_key: str, context_root: Path) -> pd.DataFrame:
    if platform == "mfl":
        ctx = _context(MFLContext, db_name, league_key, year, context_root)
        client = MFLAPIClient()
        roster = fetch_mfl_rosters(ctx, year, client=client)
        pairs = championship_bracket_pairs(client, str(league_key), year)
        return build_mfl_evidence(roster, pairs, db_name=db_name, year=year)
    ctx = _context(FleaflickerContext, db_name, league_key, year, context_root)
    client = FleaflickerAPIClient()
    roster = fetch_fleaflicker_rosters(ctx, year, client=client)
    schedule = fetch_fleaflicker_schedule(ctx, year, client=client)
    return build_fleaflicker_evidence(roster, schedule, db_name=db_name, year=year)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--platform", choices=("mfl", "fleaflicker"), action="append", required=True)
    parser.add_argument("--start-year", type=int, default=1997)
    parser.add_argument("--end-year", type=int, default=2008)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--context-root", type=Path, default=Path("D:/league-history-data/fantasy_leagues/playoff_refetch"))
    parser.add_argument("--sidecar", type=Path)
    parser.add_argument("--dry-run", action="store_true", help="list unresolved targets without calling platform APIs")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    con = duckdb.connect(args.snapshot, read_only=not args.apply)
    try:
        todo = unresolved_league_years(con, args.platform, args.start_year, args.end_year)
        if args.limit:
            todo = todo[:args.limit]
        print(f"[refetch] unresolved targets: {len(todo):,}", flush=True)
        if not todo or args.dry_run:
            for row in todo[:10]:
                print("  ", row, flush=True)
            return
        frames = []
        for index, (platform, db_name, year, league_key) in enumerate(todo, 1):
            try:
                frame = fetch_one(platform, db_name, int(year), str(league_key), args.context_root)
                frames.append(frame)
                print(f"[refetch] {index}/{len(todo)} {platform} {db_name} {year}: {len(frame):,} rows", flush=True)
            except Exception as exc:
                print(f"[refetch] {index}/{len(todo)} {platform} {db_name} {year}: FAILED {exc}", flush=True)
        evidence = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if args.sidecar:
            args.sidecar.parent.mkdir(parents=True, exist_ok=True)
            evidence.to_parquet(args.sidecar, index=False)
            print(f"[refetch] sidecar -> {args.sidecar}", flush=True)
        if not args.apply:
            return
    finally:
        con.close()
    if not args.no_backup:
        snapshot = Path(args.snapshot)
        backup = snapshot.with_name(f"{snapshot.stem}.pre-refetch-playoff.duckdb")
        if not backup.exists():
            shutil.copy2(snapshot, backup)
    con = duckdb.connect(args.snapshot)
    try:
        print(f"[refetch] applied {apply_overlay(con, evidence)}", flush=True)
    finally:
        con.close()


if __name__ == "__main__":
    main()
