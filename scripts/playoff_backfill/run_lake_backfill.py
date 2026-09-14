"""Build and optionally apply the historical playoff-evidence overlay."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

from scripts.playoff_backfill.apply_evidence_overlay import apply_overlay
from scripts.playoff_backfill.build_lake_evidence import build_evidence


DEFAULT_SNAPSHOT = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=str(DEFAULT_SNAPSHOT))
    parser.add_argument("--platform", choices=("mfl", "fleaflicker", "sleeper"), action="append")
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--sidecar", type=Path, help="write evidence parquet and stop before applying")
    parser.add_argument("--dry-run", action="store_true", help="build and report coverage without updating the snapshot")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    snapshot = Path(args.snapshot)
    con = duckdb.connect(str(snapshot), read_only=args.dry_run)
    try:
        names = None
        if args.platform:
            names = [row[0] for row in con.execute(
                "SELECT DISTINCT db_name FROM public.league_settings WHERE platform IN (" +
                ",".join("?" for _ in args.platform) + ")", args.platform
            ).fetchall()]
        evidence = build_evidence(con, db_names=names, years=args.year)
        bad = evidence.query("is_playoffs_bf == 1 and made_po_bf != 1")
        if not bad.empty:
            raise SystemExit(f"evidence invariant failed: {len(bad):,} playoff rows lack qualification")
        print(f"[backfill] evidence rows: {len(evidence):,}")
        print(evidence.groupby(["source_platform", "year"])["db_name"].nunique().to_string())
        if args.sidecar:
            args.sidecar.parent.mkdir(parents=True, exist_ok=True)
            evidence.to_parquet(args.sidecar, index=False)
            print(f"[backfill] sidecar -> {args.sidecar}")
        if args.dry_run or args.sidecar:
            return
    finally:
        con.close()

    if not args.no_backup:
        backup = snapshot.with_name(f"{snapshot.stem}.pre-playoff-backfill.duckdb")
        print(f"[backfill] backup -> {backup}")
        shutil.copy2(snapshot, backup)
    con = duckdb.connect(str(snapshot))
    try:
        stats = apply_overlay(con, evidence)
        print(f"[backfill] applied: {stats}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
