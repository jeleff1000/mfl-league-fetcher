"""Resumable year-by-year playoff overlay for historical lake rows."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

from scripts.playoff_backfill.apply_evidence_overlay import apply_overlay
from scripts.playoff_backfill.build_lake_evidence import build_evidence


SNAPSHOT = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=str(SNAPSHOT))
    parser.add_argument("--platform", choices=("mfl", "fleaflicker", "sleeper"), action="append", required=True)
    parser.add_argument("--start-year", type=int, default=1997)
    parser.add_argument("--end-year", type=int, default=2018)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()
    if args.start_year > args.end_year:
        raise SystemExit("--start-year must be <= --end-year")

    snapshot = Path(args.snapshot)
    if not args.dry_run and not args.no_backup:
        backup = snapshot.with_name(f"{snapshot.stem}.pre-playoff-backfill.duckdb")
        if not backup.exists():
            print(f"[batch] backup -> {backup}", flush=True)
            shutil.copy2(snapshot, backup)

    con = duckdb.connect(str(snapshot), read_only=args.dry_run)
    try:
        for year in range(args.start_year, args.end_year + 1):
            names = [row[0] for row in con.execute(
                "SELECT DISTINCT db_name FROM public.league_settings "
                "WHERE platform IN (" + ",".join("?" for _ in args.platform) + ") AND year = ?",
                [*args.platform, year],
            ).fetchall()]
            if not names:
                print(f"[batch] {year}: no target leagues", flush=True)
                continue
            evidence = build_evidence(con, db_names=names, years=[year])
            invalid = int(((evidence["is_playoffs_bf"] == 1) & (evidence["made_po_bf"] != 1)).sum())
            if invalid:
                raise SystemExit(f"{year}: {invalid:,} playoff rows lack qualification")
            bracket = int(evidence["is_playoffs_bf"].sum())
            made = int(evidence["made_po_bf"].sum())
            print(f"[batch] {year}: {len(names):,} leagues, {len(evidence):,} rows, made={made:,}, bracket={bracket:,}", flush=True)
            if args.dry_run:
                continue
            con.execute("BEGIN TRANSACTION")
            try:
                stats = apply_overlay(con, evidence)
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            print(f"[batch] {year}: applied {stats}", flush=True)
    finally:
        con.close()


if __name__ == "__main__":
    main()
